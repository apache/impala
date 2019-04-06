// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDbInfoSelector;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.TPartialDbInfo;
import org.apache.impala.util.FunctionUtils;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Internal representation of db-related metadata. Owned by Catalog instance.
 * Not thread safe.
 *
 * Tables are stored in a map from the table name to the table object. They may
 * be loaded 'eagerly' at construction or 'lazily' on first reference.
 * Tables are accessed via getTable which may trigger a metadata read in two cases:
 *  * if the table has never been loaded
 *  * if the table loading failed on the previous attempt
 *
 * Native user added functions are persisted to the parameters map of the hive metastore
 * db object corresponding to this instance. This map's key is the function signature and
 * value is the base64 representation of the thrift serialized function object.
 *
 */
public class Db extends CatalogObjectImpl implements FeDb {
  private static final Logger LOG = LoggerFactory.getLogger(Db.class);
  // TODO: We should have a consistent synchronization model for Db and Table
  // Right now, we synchronize functions and thriftDb_ object in-place and do
  // not take read lock on catalogVersion. See IMPALA-8366 for details
  private final AtomicReference<TDatabase> thriftDb_ = new AtomicReference<>();

  public static final String FUNCTION_INDEX_PREFIX = "impala_registered_function_";

  // Hive metastore imposes a limit of 4000 bytes on the key and value strings
  // in DB parameters map. We need ensure that this limit isn't crossed
  // while serializing functions to the metastore.
  private static final int HIVE_METASTORE_DB_PARAM_LIMIT_BYTES = 4000;

  // Table metadata cache.
  private final CatalogObjectCache<Table> tableCache_;

  // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
  // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
  // This includes both UDFs and UDAs. Updates are made thread safe by synchronizing
  // on this map. When a new Db object is initialized, this list is updated with the
  // UDF/UDAs already persisted, if any, in the metastore DB. Functions are sorted in a
  // canonical order defined by FunctionResolutionOrder.
  private final Map<String, List<Function>> functions_;

  // If true, this database is an Impala system database.
  // (e.g. can't drop it, can't add tables to it, etc).
  private boolean isSystemDb_ = false;

  // maximum number of catalog versions to store for in-flight events for this database
  private static final int MAX_NUMBER_OF_INFLIGHT_EVENTS = 10;

  // FIFO list of versions for all the in-flight metastore events in this database
  // This queue can only grow up to MAX_NUMBER_OF_INFLIGHT_EVENTS size. Anything which
  // is attempted to be added to this list when its at maximum capacity is ignored
  private final LinkedList<Long> versionsForInflightEvents_ = new LinkedList<>();

  public Db(String name, org.apache.hadoop.hive.metastore.api.Database msDb) {
    setMetastoreDb(name, msDb);
    tableCache_ = new CatalogObjectCache<>();
    functions_ = new HashMap<>();
  }

  public void setIsSystemDb(boolean b) { isSystemDb_ = b; }

  /**
   * Creates a Db object with no tables based on the given TDatabase thrift struct.
   */
  public static Db fromTDatabase(TDatabase db) {
    return new Db(db.getDb_name(), db.getMetastore_db());
  }

  /**
   * Updates the hms parameters map by adding the input <k,v> pair.
   */
  private void putToHmsParameters(String k, String v) {
    org.apache.hadoop.hive.metastore.api.Database msDb = thriftDb_.get().metastore_db;
    Preconditions.checkNotNull(msDb);
    Map<String, String> hmsParams = msDb.getParameters();
    if (hmsParams == null) hmsParams = new HashMap<>();
    hmsParams.put(k,v);
    msDb.setParameters(hmsParams);
  }

  /**
   * Updates the hms parameters map by removing the <k,v> pair corresponding to
   * input key <k>. Returns true if the parameters map contains a pair <k,v>
   * corresponding to input k and it is removed, false otherwise.
   */
  private boolean removeFromHmsParameters(String k) {
    org.apache.hadoop.hive.metastore.api.Database msDb = thriftDb_.get().metastore_db;
    Preconditions.checkNotNull(msDb);
    if (msDb.getParameters() == null) return false;
    return msDb.getParameters().remove(k) != null;
  }

  @Override // FeDb
  public boolean isSystemDb() { return isSystemDb_; }
  @Override // FeDb
  public TDatabase toThrift() { return thriftDb_.get(); }
  @Override // FeDb
  public String getName() { return thriftDb_.get().getDb_name(); }
  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.DATABASE; }

  /**
   * Adds a table to the table cache.
   */
  public void addTable(Table table) { tableCache_.add(table); }

  /**
   * Gets all table names in the table cache.
   */
  @Override
  public List<String> getAllTableNames() {
    return Lists.newArrayList(tableCache_.keySet());
  }

  /**
   * Returns the tables in the cache.
   */
  public List<Table> getTables() { return tableCache_.getValues(); }

  @Override
  public boolean containsTable(String tableName) {
    return tableCache_.contains(tableName.toLowerCase());
  }

  /**
   * Returns the Table with the given name if present in the table cache or null if the
   * table does not exist in the cache.
   */
  @Override // FeTable
  public Table getTable(String tblName) { return tableCache_.get(tblName); }

  /**
   * Removes the table name and any cached metadata from the Table cache.
   */
  public Table removeTable(String tableName) {
    return tableCache_.remove(tableName.toLowerCase());
  }

  @Override
  public FeKuduTable createKuduCtasTarget(
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      List<ColumnDef> columnDefs, List<ColumnDef> primaryKeyColumnDefs,
      List<KuduPartitionParam> kuduPartitionParams) {
    return KuduTable.createCtasTarget(this, msTbl, columnDefs, primaryKeyColumnDefs,
        kuduPartitionParams);
  }

  @Override
  public FeFsTable createFsCtasTarget(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws CatalogException {
    return HdfsTable.createCtasTarget(this, msTbl);
  }

  @Override // FeDb
  public org.apache.hadoop.hive.metastore.api.Database getMetaStoreDb() {
    return thriftDb_.get().getMetastore_db();
  }

  @Override // FeDb
  public int numFunctions() {
    synchronized (functions_) {
      return functions_.size();
    }
  }

  @Override // FeDb
  public boolean containsFunction(String name) {
    synchronized (functions_) {
      return functions_.get(name) != null;
    }
  }

  /*
   * See comment in Catalog.
   */
  @Override // FeDb
  public Function getFunction(Function desc, Function.CompareMode mode) {
    synchronized (functions_) {
      List<Function> fns = functions_.get(desc.functionName());
      if (fns == null) return null;
      return FunctionUtils.resolveFunction(fns, desc, mode);
    }
  }

  public Function getFunction(String signatureString) {
    synchronized (functions_) {
      for (List<Function> fns: functions_.values()) {
        for (Function f: fns) {
          if (f.signatureString().equals(signatureString)) return f;
        }
      }
    }
    return null;
  }

  /**
   * Adds the user defined function fn to metastore DB params. fn is
   * serialized to thrift using TBinaryProtocol and then base64-encoded
   * to be compatible with the HMS' representation of params.
   */
  private boolean addFunctionToDbParams(Function fn) {
    Preconditions.checkState(
        fn.getBinaryType() != TFunctionBinaryType.BUILTIN &&
        fn.getBinaryType() != TFunctionBinaryType.JAVA);
    try {
      TSerializer serializer =
          new TSerializer(new TCompactProtocol.Factory());
      byte[] serializedFn = serializer.serialize(fn.toThrift());
      String base64Fn = Base64.getEncoder().encodeToString(serializedFn);
      String fnKey = FUNCTION_INDEX_PREFIX + fn.signatureString();
      if (base64Fn.length() > HIVE_METASTORE_DB_PARAM_LIMIT_BYTES) {
        throw new ImpalaRuntimeException(
            "Serialized function size exceeded HMS 4K byte limit");
      }
      putToHmsParameters(fnKey, base64Fn);
    } catch (ImpalaException | TException  e) {
      LOG.error("Error adding function " + fn.getName() + " to DB params", e);
      return false;
    }
    return true;
  }

  public boolean addFunction(Function fn) {
    // We use the db parameters map to persist native and IR functions.
    boolean addToDbParams =
        (fn.getBinaryType() == TFunctionBinaryType.NATIVE ||
         fn.getBinaryType() == TFunctionBinaryType.IR);
    return addFunction(fn, addToDbParams);
  }

  /**
   * Registers the function fn to this database. If addToDbParams is true,
   * fn is added to the metastore DB params. Returns false if the function
   * fn already exists or when a failure is encountered while adding it to
   * the metastore DB params and true otherwise.
   */
  public boolean addFunction(Function fn, boolean addToDbParams) {
    Preconditions.checkState(fn.dbName().equals(getName()));
    synchronized (functions_) {
      if (getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
        return false;
      }
      List<Function> fns = functions_.get(fn.functionName());
      if (fns == null) {
        fns = new ArrayList<>();
        functions_.put(fn.functionName(), fns);
      }
      if (addToDbParams && !addFunctionToDbParams(fn)) return false;
      fns.add(fn);
      Collections.sort(fns, FunctionUtils.FUNCTION_RESOLUTION_ORDER);
      return true;
    }
  }

  /**
   * See comment in Catalog.
   */
  public Function removeFunction(Function desc) {
    synchronized (functions_) {
      Function fn = getFunction(desc, Function.CompareMode.IS_INDISTINGUISHABLE);
      if (fn == null) return null;
      List<Function> fns = functions_.get(desc.functionName());
      Preconditions.checkNotNull(fns);
      fns.remove(fn);
      if (fns.isEmpty()) functions_.remove(desc.functionName());
      if (fn.getBinaryType() == TFunctionBinaryType.JAVA) return fn;
      // Remove the function from the metastore database parameters
      String fnKey = FUNCTION_INDEX_PREFIX + fn.signatureString();
      boolean removeFn = removeFromHmsParameters(fnKey);
      Preconditions.checkState(removeFn);
      return fn;
    }
  }

  public void removeAllFunctions() {
    synchronized (functions_) {
      functions_.clear();
    }
  }

  /**
   * Removes a Function with the matching signature string. Returns the removed Function
   * if a Function was removed as a result of this call, null otherwise.
   * TODO: Move away from using signature strings and instead use Function IDs.
   */
  public Function removeFunction(String signatureStr) {
    synchronized (functions_) {
      Function targetFn = getFunction(signatureStr);
      if (targetFn != null) return removeFunction(targetFn);
    }
    return null;
  }

  /**
   * Add a builtin with the specified name and signatures to this db.
   * This defaults to not using a Prepare/Close function.
   */
  public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
      boolean varArgs, Type retType, Type ... args) {
    addScalarBuiltin(fnName, symbol, userVisible, null, null, varArgs, retType, args);
  }

  /**
   * Add a builtin with the specified name and signatures to this db.
   */
  public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
      String prepareFnSymbol, String closeFnSymbol, boolean varArgs, Type retType,
      Type ... args) {
    Preconditions.checkState(isSystemDb());
    addBuiltin(ScalarFunction.createBuiltin(
        fnName, Lists.newArrayList(args), varArgs, retType,
        symbol, prepareFnSymbol, closeFnSymbol, userVisible));
  }

  /**
   * Adds a builtin to this database. The function must not already exist.
   */
  public void addBuiltin(Function fn) {
    Preconditions.checkState(isSystemDb());
    Preconditions.checkState(fn != null);
    Preconditions.checkState(getFunction(fn, Function.CompareMode.IS_IDENTICAL) == null);
    addFunction(fn, false);
  }

  /**
   * Returns a map of functionNames to list of (overloaded) functions with that name.
   * This is not thread safe so a higher level lock must be taken while iterating
   * over the returned functions.
   */
  public Map<String, List<Function>> getAllFunctions() {
    return functions_;
  }

  /**
   * Returns a list of transient functions in this Db.
   */
  protected List<Function> getTransientFunctions() {
    List<Function> result = new ArrayList<>();
    synchronized (functions_) {
      for (String fnKey: functions_.keySet()) {
        for (Function fn: functions_.get(fnKey)) {
          if (fn.userVisible() && !fn.isPersistent()) {
            result.add(fn);
          }
        }
      }
    }
    return result;
  }

  /**
   * Returns all functions that match the pattern of 'matcher'.
   */
  @Override
  public List<Function> getFunctions(TFunctionCategory category,
      PatternMatcher matcher) {
    Preconditions.checkNotNull(matcher);
    List<Function> result = new ArrayList<>();
    synchronized (functions_) {
      for (Map.Entry<String, List<Function>> fns: functions_.entrySet()) {
        if (!matcher.matches(fns.getKey())) continue;
        for (Function fn: fns.getValue()) {
          if ((category == null || Function.categoryMatch(fn, category))
              && fn.userVisible()) {
            result.add(fn);
          }
        }
      }
    }
    return result;
  }

  /**
   * Returns all functions with the given name
   */
  @Override // FeDb
  public List<Function> getFunctions(String name) {
    Preconditions.checkNotNull(name);
    synchronized (functions_) {
      List<Function> candidates = functions_.get(name);
      if (candidates == null) return new ArrayList<>();
      return FunctionUtils.getVisibleFunctions(candidates);
    }
  }

  @Override
  public List<Function> getFunctions(TFunctionCategory category, String name) {
    Preconditions.checkNotNull(category);
    Preconditions.checkNotNull(name);
    synchronized (functions_) {
      List<Function> candidates = functions_.get(name);
      if (candidates == null) return new ArrayList<>();
      return FunctionUtils.getVisibleFunctionsInCategory(candidates, category);
    }
  }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setDb(toThrift());
  }

  /**
   * Get partial information about this DB in order to service CatalogdMetaProvider
   * running in a remote impalad.
   */
  public TGetPartialCatalogObjectResponse getPartialInfo(
      TGetPartialCatalogObjectRequest req) {
    TDbInfoSelector selector = Preconditions.checkNotNull(req.db_info_selector,
        "no db_info_selector");

    TGetPartialCatalogObjectResponse resp = new TGetPartialCatalogObjectResponse();
    resp.setObject_version_number(getCatalogVersion());
    resp.db_info = new TPartialDbInfo();
    if (selector.want_hms_database) {
      // TODO(todd): we need to deep-copy here because 'addFunction' other DDLs
      // modify the parameter map in place. We need to change those to copy-on-write
      // instead to avoid this copy.
      resp.db_info.hms_database = getMetaStoreDb().deepCopy();
    }
    if (selector.want_table_names) {
      resp.db_info.table_names = getAllTableNames();
    }
    if (selector.want_function_names) {
      resp.db_info.function_names = ImmutableList.copyOf(functions_.keySet());
    }
    return resp;
  }

  /**
   * Replaces the metastore db object of this Db with the given Metastore Database object
   * @param msDb
   */
  public void setMetastoreDb(String name, Database msDb) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(msDb);
    // create the TDatabase first before atomically replacing setting it in the thriftDb_
    TDatabase tDatabase = new TDatabase(name.toLowerCase());
    tDatabase.setMetastore_db(msDb);
    thriftDb_.set(tDatabase);
  }

  /**
   * Gets the current list of versions for in-flight events for this database
   */
  public List<Long> getVersionsForInflightEvents() {
    return Collections.unmodifiableList(versionsForInflightEvents_);
  }

  /**
   * Removes a given version from the collection of version numbers for in-flight events
   * @param versionNumber version number to remove from the collection
   * @return true if version was successfully removed, false if didn't exist
   */
  public boolean removeFromVersionsForInflightEvents(long versionNumber) {
    return versionsForInflightEvents_.remove(versionNumber);
  }

  /**
   * Adds a version number to the collection of versions for in-flight events. If the
   * collection is already at the max size defined by
   * <code>MAX_NUMBER_OF_INFLIGHT_EVENTS</code>, then it ignores the given version and
   * does not add it
   * @param versionNumber version number to add
   * @return True if version number was added, false if the collection is at its max
   * capacity
   */
  public boolean addToVersionsForInflightEvents(long versionNumber) {
    if (versionsForInflightEvents_.size() >= MAX_NUMBER_OF_INFLIGHT_EVENTS) {
      LOG.warn(String.format("Number of versions to be stored for database %s is at "
              + " its max capacity %d. Ignoring add request for version number %d. This "
              + "could cause unnecessary database invalidation when the event is "
              + "processed",
          getName(), MAX_NUMBER_OF_INFLIGHT_EVENTS, versionNumber));
      return false;
    }
    versionsForInflightEvents_.add(versionNumber);
    return true;
  }
}
