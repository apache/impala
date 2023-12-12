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

package org.apache.impala.catalog.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.util.FunctionUtils;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Database instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
public class LocalDb implements FeDb {
  private final static Logger LOG = LoggerFactory.getLogger(LocalDb.class);
  private final LocalCatalog catalog_;
  /** The lower-case name of the database. */
  private final String name_;
  private Database msDb_;

  /**
   * Map from lower-cased table name to table object. Values will be
   * null for tables which have not yet been loaded.
   */
  private Map<String, FeTable> tables_;

  /**
   * Map of function name to list of signatures for that function name.
   */
  private Map<String, List<Function>> functions_;

  public LocalDb(LocalCatalog catalog, String dbName) {
    Preconditions.checkNotNull(catalog);
    Preconditions.checkNotNull(dbName);
    Preconditions.checkArgument(dbName.toLowerCase().equals(dbName));
    this.catalog_ = catalog;
    this.name_ = dbName;
  }

  @Override
  public String getName() {
    return name_;
  }

  @Override
  public Database getMetaStoreDb() {
    if (msDb_ == null) {
      try {
        msDb_ = catalog_.getMetaProvider().loadDb(name_);
      } catch (TException e) {
        throw new LocalCatalogException(String.format(
            "Could not load database '%s' from HMS", name_), e);
      }
    }
    return msDb_;
  }

  @Override
  public boolean containsTable(String tableName) {
    loadTableNames();
    return tables_.containsKey(tableName.toLowerCase());
  }

  @Override
  public FeTable getTableIfCached(String tblName) {
    Preconditions.checkNotNull(tblName);
    tblName = tblName.toLowerCase();
    loadTableNames();
    if (!tables_.containsKey(tblName)) {
      // Table doesn't exist.
      return null;
    }
    FeTable tbl = tables_.get(tblName);
    if (tbl instanceof LocalIncompleteTable && tbl.getMetaStoreTable() == null) {
      // Add msTable if it's cached.
      Pair<Table, MetaProvider.TableMetaRef> tblMeta =
          catalog_.getMetaProvider().getTableIfPresent(name_, tblName);
      if (tblMeta != null) {
        tbl = new LocalIncompleteTable(this, tblMeta.first, tblMeta.second,
            tbl.getTableType(), tbl.getTableComment());
        tables_.put(tblName, tbl);
      }
    }
    return tbl;
  }

  @Override
  public FeTable getTable(String tableName) {
    // the underlying layers of the cache expect all the table name to be in lowercase
    String tblName = Preconditions.checkNotNull(tableName,
        "Received a null table name").toLowerCase();
    FeTable tbl = getTableIfCached(tblName);
    if (tbl instanceof LocalIncompleteTable) {
      // The table exists but hasn't been loaded yet.
      try {
        tbl = LocalTable.load(this, tblName);
      } catch (TableLoadingException tle) {
        // If the table fails to load (eg a Kudu table that doesn't have
        // a backing table, or some other catalogd-side issue), turn it into
        // an IncompleteTable. This allows statements like DROP TABLE to still
        // analyze.
        tbl = new FailedLoadLocalTable(this, tblName, tbl.getTableType(),
            tbl.getTableComment(), tle);
      }
      tables_.put(tblName, tbl);
    }
    return tbl;
  }

  @Override
  public FeKuduTable createKuduCtasTarget(Table msTbl, List<ColumnDef> columnDefs,
      List<ColumnDef> primaryKeyColumnDefs, boolean isPrimaryKeyUnique,
      List<KuduPartitionParam> kuduPartitionParams) throws ImpalaRuntimeException {
    return LocalKuduTable.createCtasTarget(this, msTbl, columnDefs, isPrimaryKeyUnique,
        primaryKeyColumnDefs, kuduPartitionParams);
  }

  @Override
  public FeFsTable createFsCtasTarget(Table msTbl) throws CatalogException {
    return LocalFsTable.createCtasTarget(this, msTbl);
  }

  @Override
  public List<String> getAllTableNames() {
    return getAllTableNames(/*tableTypes*/ Collections.emptySet());
  }

  @Override
  public List<String> getAllTableNames(Set<TImpalaTableType> tableTypes) {
    loadTableNames();
    if (!tableTypes.isEmpty()) {
      return tables_.values().stream()
          .filter(table ->
              tableTypes.stream().anyMatch(type -> type.equals(table.getTableType())))
          .map(table -> table.getName())
          .collect(Collectors.toList());
    }
    return ImmutableList.copyOf(tables_.keySet());
  }

  /**
   * Populate the 'tables_' map if it is not already populated.
   *
   * The map is populated with appropriate keys but null values which
   * will be replaced on-demand.
   */
  private void loadTableNames() {
    if (tables_ != null) return;
    Map<String, FeTable> newMap = new HashMap<>();
    try {
      MetaProvider metaProvider = catalog_.getMetaProvider();
      for (TBriefTableMeta meta : metaProvider.loadTableList(name_)) {
        newMap.put(meta.getName(), new LocalIncompleteTable(this, meta));
      }
    } catch (TException e) {
      throw new LocalCatalogException(String.format(
          "Could not load table names for database '%s' from HMS", name_), e);
    }
    tables_ = newMap;
  }

  @Override
  public boolean isSystemDb() {
    return false;
  }

  @Override
  public Function getFunction(Function desc, CompareMode mode) {
    loadFunction(desc.functionName());
    List<Function> funcs = functions_.get(desc.functionName());
    if (funcs == null) return null;
    return FunctionUtils.resolveFunction(funcs, desc, mode);
  }

  /**
   * Populate the 'functions_' map with the correct set of keys corresponding to
   * the functions in this database. The values will be 'null' to indicate that the
   * functions themselves have not yet been loaded.
   */
  private void loadFunctionNames() {
    if (functions_ != null) return;

    List<String> funcNames;
    try {
      funcNames = catalog_.getMetaProvider().loadFunctionNames(name_);
    } catch (TException e) {
      throw new LocalCatalogException(String.format(
          "Could not load function names for database '%s'", name_), e);
    }

    functions_ = Maps.newHashMapWithExpectedSize(funcNames.size());
    for (String fn : funcNames) functions_.put(fn, null);
  }

  /**
   * Ensure that the given function has been fully loaded.
   * If this function does not exist, this is a no-op.
   */
  private void loadFunction(String functionName) {
    loadFunctionNames();


    // If the function isn't in the map at all, then the function doesn't exist.
    if (!functions_.containsKey(functionName)) return;
    List<Function> overloads = functions_.get(functionName);

    // If it's in the map, it might have a null value, or it might already be loaded.
    // If it's already loaded, we're done.
    if (overloads != null) return;


    try {
      overloads = catalog_.getMetaProvider().loadFunction(name_, functionName);
    } catch (TException e) {
      throw new LocalCatalogException(String.format("Could not load function '%s.%s'",
          name_, functionName), e);
    }
    functions_.put(functionName,  overloads);
  }

  @Override
  public List<Function> getFunctions(String functionName) {
    loadFunction(functionName);
    List<Function> funcs = functions_.get(functionName);
    if (funcs == null) return Collections.emptyList();
    return FunctionUtils.getVisibleFunctions(funcs);
  }

  @Override
  public List<Function> getFunctions(
      TFunctionCategory category, String functionName) {
    loadFunction(functionName);
    List<Function> funcs = functions_.get(functionName);
    if (funcs == null) return Collections.emptyList();
    return FunctionUtils.getVisibleFunctionsInCategory(funcs, category);
  }

  @Override
  public List<Function> getFunctions(
      TFunctionCategory category, PatternMatcher matcher) {
    loadFunctionNames();
    List<Function> result = new ArrayList<>();
    Iterable<String> fnNames = Iterables.filter(functions_.keySet(), matcher);
    for (String fnName : fnNames) {
      result.addAll(getFunctions(category, fnName));
    }
    return result;
  }

  @Override
  public int numFunctions() {
    loadFunctionNames();
    return functions_.size();
  }

  @Override
  public boolean containsFunction(String function) {
    loadFunctionNames();
    // TODO(todd): does this need to be lower-cased here?
    return functions_.containsKey(function);
  }

  @Override
  public TDatabase toThrift() {
    TDatabase tdb = new TDatabase(name_);
    tdb.setMetastore_db(getMetaStoreDb());
    return tdb;
  }

  LocalCatalog getCatalog() {
    return catalog_;
  }

  @Override // FeDb
  public String getOwnerUser() {
    Database db = getMetaStoreDb();
    return db == null ? null :
        (db.getOwnerType() == PrincipalType.USER ? db.getOwnerName() : null);
  }
}
