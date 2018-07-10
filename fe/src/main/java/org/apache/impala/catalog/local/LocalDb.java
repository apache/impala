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
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Database instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
class LocalDb implements FeDb {
  private final LocalCatalog catalog_;
  /** The lower-case name of the database. */
  private final String name_;
  private Database msDb_;

  /**
   * Map from lower-cased table name to table object. Values will be
   * null for tables which have not yet been loaded.
   */
  private Map<String, LocalTable> tables_;

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
    // TODO(todd): implement functions
    return null;
  }

  @Override
  public List<Function> getFunctions(String functionName) {
    // TODO(todd): implement functions
    return Collections.emptyList();
  }

  @Override
  public List<Function> getFunctions(
      TFunctionCategory category, String function) {
    // TODO(todd): implement functions
    return Collections.emptyList();
  }

  @Override
  public List<Function> getFunctions(
      TFunctionCategory category, PatternMatcher patternMatcher) {
    // TODO(todd): implement functions
    return Collections.emptyList();
  }

  @Override
  public int numFunctions() {
    // TODO(todd): implement functions
    return 0;
  }

  @Override
  public boolean containsFunction(String function) {
    // TODO(todd): implement functions
    return false;
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
}
