// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TDatabase;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TFunctionCategory;
import com.cloudera.impala.util.PatternMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
public class Db implements CatalogObject {
  private static final Logger LOG = LoggerFactory.getLogger(Db.class);
  private final Catalog parentCatalog_;
  private final TDatabase thriftDb_;
  private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;

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
  private final HashMap<String, List<Function>> functions_;

  // If true, this database is an Impala system database.
  // (e.g. can't drop it, can't add tables to it, etc).
  private boolean isSystemDb_ = false;

  public Db(String name, Catalog catalog,
      org.apache.hadoop.hive.metastore.api.Database msDb) {
    thriftDb_ = new TDatabase(name.toLowerCase());
    parentCatalog_ = catalog;
    thriftDb_.setMetastore_db(msDb);
    tableCache_ = new CatalogObjectCache<Table>();
    functions_ = new HashMap<String, List<Function>>();
  }

  public void setIsSystemDb(boolean b) { isSystemDb_ = b; }

  /**
   * Creates a Db object with no tables based on the given TDatabase thrift struct.
   */
  public static Db fromTDatabase(TDatabase db, Catalog parentCatalog) {
    return new Db(db.getDb_name(), parentCatalog, db.getMetastore_db());
  }

  /**
   * Updates the hms parameters map by adding the input <k,v> pair.
   */
  private void putToHmsParameters(String k, String v) {
    org.apache.hadoop.hive.metastore.api.Database msDb = thriftDb_.metastore_db;
    Preconditions.checkNotNull(msDb);
    Map<String, String> hmsParams = msDb.getParameters();
    if (hmsParams == null) hmsParams = Maps.newHashMap();
    hmsParams.put(k,v);
    msDb.setParameters(hmsParams);
  }

  /**
   * Updates the hms parameters map by removing the <k,v> pair corresponding to
   * input key <k>. Returns true if the parameters map contains a pair <k,v>
   * corresponding to input k and it is removed, false otherwise.
   */
  private boolean removeFromHmsParameters(String k) {
    org.apache.hadoop.hive.metastore.api.Database msDb = thriftDb_.metastore_db;
    Preconditions.checkNotNull(msDb);
    if (msDb.getParameters() == null) return false;
    return msDb.getParameters().remove(k) != null;
  }

  public boolean isSystemDb() { return isSystemDb_; }
  public TDatabase toThrift() { return thriftDb_; }
  @Override
  public String getName() { return thriftDb_.getDb_name(); }
  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.DATABASE;
  }

  /**
   * Adds a table to the table cache.
   */
  public void addTable(Table table) {
    tableCache_.add(table);
  }

  /**
   * Gets all table names in the table cache.
   */
  public List<String> getAllTableNames() {
    return Lists.newArrayList(tableCache_.keySet());
  }

  public boolean containsTable(String tableName) {
    return tableCache_.contains(tableName.toLowerCase());
  }

  /**
   * Returns the Table with the given name if present in the table cache or null if the
   * table does not exist in the cache.
   */
  public Table getTable(String tblName) {
    return tableCache_.get(tblName);
  }

  /**
   * Removes the table name and any cached metadata from the Table cache.
   */
  public Table removeTable(String tableName) {
    return tableCache_.remove(tableName.toLowerCase());
  }

  /**
   * Comparator that sorts function overloads. We want overloads to be always considered
   * in a canonical order so that overload resolution in the case of multiple valid
   * overloads does not depend on the order in which functions are added to the Db. The
   * order is based on the PrimitiveType enum because this was the order used implicitly
   * for builtin operators and functions in earlier versions of Impala.
   */
  private static class FunctionResolutionOrder implements Comparator<Function> {
    @Override
    public int compare(Function f1, Function f2) {
      int numSharedArgs = Math.min(f1.getNumArgs(), f2.getNumArgs());
      for (int i = 0; i < numSharedArgs; ++i) {
        int cmp = typeCompare(f1.getArgs()[i], f2.getArgs()[i]);
        if (cmp < 0) {
          return -1;
        } else if (cmp > 0) {
          return 1;
        }
      }
      // Put alternative with fewer args first.
      if (f1.getNumArgs() < f2.getNumArgs()) {
        return -1;
      } else if (f1.getNumArgs() > f2.getNumArgs()) {
        return 1;
      }
      return 0;
    }

    private int typeCompare(Type t1, Type t2) {
      Preconditions.checkState(!t1.isComplexType());
      Preconditions.checkState(!t2.isComplexType());
      return Integer.compare(t1.getPrimitiveType().ordinal(),
          t2.getPrimitiveType().ordinal());
    }
  }

  private static final FunctionResolutionOrder FUNCTION_RESOLUTION_ORDER =
      new FunctionResolutionOrder();

  /**
   * Returns the metastore.api.Database object this Database was created from.
   * Returns null if it is not related to a hive database such as builtins_db.
   */
  public org.apache.hadoop.hive.metastore.api.Database getMetaStoreDb() {
    return thriftDb_.getMetastore_db();
  }

  /**
   * Returns the number of functions in this database.
   */
  public int numFunctions() {
    synchronized (functions_) {
      return functions_.size();
    }
  }

  /**
   * See comment in Catalog.
   */
  public boolean containsFunction(String name) {
    synchronized (functions_) {
      return functions_.get(name) != null;
    }
  }

  /*
   * See comment in Catalog.
   */
  public Function getFunction(Function desc, Function.CompareMode mode) {
    synchronized (functions_) {
      List<Function> fns = functions_.get(desc.functionName());
      if (fns == null) return null;

      // First check for identical
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) return f;
      }
      if (mode == Function.CompareMode.IS_IDENTICAL) return null;

      // Next check for indistinguishable
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) return f;
      }
      if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) return null;

      // Next check for strict supertypes
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_SUPERTYPE_OF)) return f;
      }
      if (mode == Function.CompareMode.IS_SUPERTYPE_OF) return null;

      // Finally check for non-strict supertypes
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF)) return f;
      }
    }
    return null;
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
      String base64Fn = Base64.encodeBase64String(serializedFn);
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
        fns = Lists.newArrayList();
        functions_.put(fn.functionName(), fns);
      }
      if (addToDbParams && !addFunctionToDbParams(fn)) return false;
      fns.add(fn);
      Collections.sort(fns, FUNCTION_RESOLUTION_ORDER);
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
  protected HashMap<String, List<Function>> getAllFunctions() {
    return functions_;
  }

  /**
   * Returns a list of transient functions in this Db.
   */
  protected List<Function> getTransientFunctions() {
    List<Function> result = Lists.newArrayList();
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
   * Returns all functions that match 'fnPattern'.
   */
  public List<Function> getFunctions(TFunctionCategory category,
      PatternMatcher fnPattern) {
    List<Function> result = Lists.newArrayList();
    synchronized (functions_) {
      for (Map.Entry<String, List<Function>> fns: functions_.entrySet()) {
        if (!fnPattern.matches(fns.getKey())) continue;
        for (Function fn: fns.getValue()) {
          if (fn.userVisible() &&
              (category == null || Function.categoryMatch(fn, category))) {
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
  public List<Function> getFunctions(String name) {
    List<Function> result = Lists.newArrayList();
    Preconditions.checkNotNull(name);
    synchronized (functions_) {
      if (!functions_.containsKey(name)) return result;
      for (Function fn: functions_.get(name)) {
        if (fn.userVisible()) result.add(fn);
      }
    }
    return result;
  }

  /**
   * Returns all functions with the given name and category.
   */
  public List<Function> getFunctions(TFunctionCategory category, String name) {
    List<Function> result = Lists.newArrayList();
    Preconditions.checkNotNull(category);
    Preconditions.checkNotNull(name);
    synchronized (functions_) {
      if (!functions_.containsKey(name)) return result;
      for (Function fn: functions_.get(name)) {
        if (fn.userVisible() && Function.categoryMatch(fn, category)) {
          result.add(fn);
        }
      }
    }
    return result;
  }

  @Override
  public long getCatalogVersion() { return catalogVersion_; }
  @Override
  public void setCatalogVersion(long newVersion) { catalogVersion_ = newVersion; }
  public Catalog getParentCatalog() { return parentCatalog_; }

  @Override
  public boolean isLoaded() { return true; }
}
