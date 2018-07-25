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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
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
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.util.FunctionUtils;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Database instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
class LocalDb implements FeDb {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDb.class);

  private final LocalCatalog catalog_;
  /** The lower-case name of the database. */
  private final String name_;
  private Database msDb_;

  /**
   * Map from lower-cased table name to table object. Values will be
   * null for tables which have not yet been loaded.
   */
  private Map<String, LocalTable> tables_;

  /**
   * Map of function name to list of signatures for that function name.
   */
  private Map<String, FunctionOverloads> functions_;

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
  public FeTable getTable(String tblName) {
    Preconditions.checkNotNull(tblName);
    tblName = tblName.toLowerCase();
    loadTableNames();
    if (!tables_.containsKey(tblName)) {
      // Table doesn't exist.
      return null;
    }
    LocalTable tbl = tables_.get(tblName);
    if (tbl == null) {
      // The table exists but hasn't been loaded yet.
      tbl = LocalTable.load(this, tblName);
      tables_.put(tblName, tbl);
    }
    return tbl;
  }

  @Override
  public FeKuduTable createKuduCtasTarget(Table msTbl, List<ColumnDef> columnDefs,
      List<ColumnDef> primaryKeyColumnDefs,
      List<KuduPartitionParam> kuduPartitionParams) {
    return LocalKuduTable.createCtasTarget(this, msTbl, columnDefs, primaryKeyColumnDefs,
        kuduPartitionParams);
  }

  @Override
  public FeFsTable createFsCtasTarget(Table msTbl) throws CatalogException {
    return LocalFsTable.createCtasTarget(this, msTbl);
  }

  @Override
  public List<String> getAllTableNames() {
    loadTableNames();
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
    Map<String, LocalTable> newMap = Maps.newHashMap();
    try {
      List<String> names = catalog_.getMetaProvider().loadTableNames(name_);
      for (String tableName : names) {
        newMap.put(tableName.toLowerCase(), null);
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
    FunctionOverloads funcs = functions_.get(desc.functionName());
    if (funcs == null) return null;
    return FunctionUtils.resolveFunction(funcs, desc, mode);
  }

  /**
   * Populate the 'functions_' map if not already populated.
   * This handles loading persistent native functions, since they're already
   * present in the DB-level metadata, and loading the list of Java function
   * names. The Java function signatures themselves are lazy-loaded as necessary
   * by loadFunction(...).
   */
  private void loadFunctionNames() {
    if (functions_ != null) return;

    // Load the Java functions names. We don't load the actual metadata
    // for them unless they get looked up.
    List<String> javaFuncNames;

    try {
      javaFuncNames = catalog_.getMetaProvider().loadFunctionNames(name_);
    } catch (TException e) {
      throw new LocalCatalogException(String.format(
          "Could not load functions for database '%s' from HMS", name_), e);
    }

    Map<String, FunctionOverloads> newMap = Maps.newHashMap();
    for (String fn : javaFuncNames) {
      newMap.put(fn, new FunctionOverloads(/*javaNeedsLoad=*/true));
    }

    // Load native functions.
    List<Function> nativeFuncs = FunctionUtils.deserializeNativeFunctionsFromDbParams(
        getMetaStoreDb().getParameters());
    for (Function fn : nativeFuncs) {
      String fnName = fn.functionName();
      FunctionOverloads dst = newMap.get(fnName);
      if (dst == null) {
        // We know there were no Java functions by this name since we didn't see
        // this function above in the HMS-derived function list.
        dst = new FunctionOverloads(/*javaNeedsLoad=*/false);
        newMap.put(fnName, dst);
      }
      dst.add(fn);
    }

    functions_ = newMap;
  }

  /**
   * Ensure that the given function has been fully loaded.
   * If this function does not exist, this is a no-op.
   */
  private void loadFunction(String functionName) {
    loadFunctionNames();

    FunctionOverloads overloads = functions_.get(functionName);
    // If the function isn't in the map at all, then it doesn't exist.
    if (overloads == null) return;

    // If it's in the map, the native functions will already have been loaded,
    // since we get that info from the DB params. But, we may still need to
    // load Java info.
    if (!overloads.javaNeedsLoad()) return;

    org.apache.hadoop.hive.metastore.api.Function msFunc;
    try {
      msFunc = catalog_.getMetaProvider().getFunction(name_, functionName);
    } catch (TException e) {
      throw new LocalCatalogException(String.format(
          "Could not load function '%s.%s' from HMS",
          name_, functionName), e);
    }

    try {
      overloads.setJavaFunctions(FunctionUtils.extractFunctions(name_, msFunc,
          BackendConfig.INSTANCE.getBackendCfg().local_library_path));
    } catch (ImpalaRuntimeException e) {
      throw new LocalCatalogException(String.format(
          "Could not load Java function definitions for '%s.%s'",
          name_, functionName), e);
    }
  }

  @Override
  public List<Function> getFunctions(String functionName) {
    loadFunction(functionName);
    FunctionOverloads funcs = functions_.get(functionName);
    if (funcs == null) return Collections.emptyList();
    return FunctionUtils.getVisibleFunctions(funcs);
  }

  @Override
  public List<Function> getFunctions(
      TFunctionCategory category, String functionName) {
    loadFunction(functionName);
    FunctionOverloads funcs = functions_.get(functionName);
    if (funcs == null) return Collections.emptyList();
    return FunctionUtils.getVisibleFunctionsInCategory(funcs, category);
  }

  @Override
  public List<Function> getFunctions(
      TFunctionCategory category, PatternMatcher matcher) {
    loadFunctionNames();
    List<Function> result = Lists.newArrayList();
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

  /**
   * Captures the set of function overloads with a given name. This tracks the
   * lazy-loading state of whether the Java signatures have been loaded yet,
   * and ensures that only a properly-sorted list is exposed.
   */
  private static class FunctionOverloads implements Iterable<Function> {
    /**
     * The loaded functions, or null if no functions have not been loaded.
     */
    private List<Function> functions_;

    /**
     * The function list is lazily sorted only if it gets iterated, so that
     * we don't pay any cost for sorting in the case when function names are
     * loaded but a given function is not actually resolved in a query.
     */
    private boolean needsSort_ = false;

    /**
     * Whether Java functions still need to be loaded.
     */
    private boolean javaNeedsLoad_ = true;

    FunctionOverloads(boolean javaNeedsLoad) {
      this.javaNeedsLoad_ = javaNeedsLoad;
    }

    @Override
    public Iterator<Function> iterator() {
      Preconditions.checkState(!javaNeedsLoad_);
      if (needsSort_) {
        Collections.sort(functions_, FunctionUtils.FUNCTION_RESOLUTION_ORDER);
        needsSort_ = false;
      }
      return functions_.iterator();
    }

    public void add(Function fn) {
      if (functions_ == null) functions_ = Lists.newArrayList();
      functions_.add(fn);
      needsSort_ = true;
    }

    public boolean javaNeedsLoad() {
      return javaNeedsLoad_;
    }

    public void setJavaFunctions(List<Function> fns) {
      Preconditions.checkState(javaNeedsLoad_);
      javaNeedsLoad_ = false;
      needsSort_ |= !fns.isEmpty();
      if (functions_ == null) {
        functions_ = fns;
        return;
      }
      functions_.addAll(fns);
    }
  }
}
