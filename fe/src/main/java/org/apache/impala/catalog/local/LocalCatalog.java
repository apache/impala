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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.AuthorizationPolicy;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Implementation of FeCatalog which runs within the impalad and fetches metadata
 * on-demand using a {@link MetaProvider} instance.
 *
 * This class should be instantiated once per query. As the catalog is queried,
 * it lazy-loads those pieces of metadata into the instance and instantiates
 * appropriate catalog object implementations. This provides caching within a
 * query -- multiple calls to fetch a given database, table, etc, will return
 * the same instance. However, this class does not inherently provide any caching
 * outside the scope of a single query: that caching should be performed at the
 * level of the MetaProvider.
 *
 * This class is not thread-safe, nor are any of the catalog object implementations
 * returned from its methods.
 */
public class LocalCatalog implements FeCatalog {
  private final MetaProvider metaProvider_;
  private Map<String, FeDb> dbs_ = Maps.newHashMap();
  private String nullPartitionKeyValue_;
  private final String defaultKuduMasterHosts_;

  public static LocalCatalog create(String defaultKuduMasterHosts) {
    return new LocalCatalog(new DirectMetaProvider(), defaultKuduMasterHosts);
  }

  private LocalCatalog(MetaProvider metaProvider, String defaultKuduMasterHosts) {
    metaProvider_ = Preconditions.checkNotNull(metaProvider);
    defaultKuduMasterHosts_ = defaultKuduMasterHosts;
  }

  @Override
  public List<? extends FeDb> getDbs(PatternMatcher matcher) {
    loadDbs();
    return Catalog.filterCatalogObjectsByPattern(dbs_.values(), matcher);
  }

  private void loadDbs() {
    if (!dbs_.isEmpty()) return;
    Map<String, FeDb> dbs = Maps.newHashMap();
    List<String> names;
    try {
      names = metaProvider_.loadDbList();
    } catch (TException e) {
      throw new LocalCatalogException("Unable to load database names", e);
    }
    for (String dbName : names) {
      dbName = dbName.toLowerCase();
      if (dbs_.containsKey(dbName)) {
        dbs.put(dbName, dbs_.get(dbName));
      } else {
        dbs.put(dbName, new LocalDb(this, dbName));
      }
    }

    Db bdb = BuiltinsDb.getInstance();
    dbs.put(bdb.getName(), bdb);
    dbs_ = dbs;
  }


  @Override
  public List<String> getTableNames(String dbName, PatternMatcher matcher)
      throws DatabaseNotFoundException {
    return Catalog.filterStringsByPattern(getDbOrThrow(dbName).getAllTableNames(), matcher);
  }

  @Override
  public FeTable getTable(String dbName, String tableName)
      throws DatabaseNotFoundException {
    return getDbOrThrow(dbName).getTable(tableName);
  }

  @Override
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    // TODO(todd): this probably makes the /catalog page not load with an error.
    // We should probably disable that page in local-catalog mode.
    throw new UnsupportedOperationException("LocalCatalog.getTCatalogObject");
  }

  @Override
  public FeDb getDb(String db) {
    loadDbs();
    return dbs_.get(db);
  }

  private FeDb getDbOrThrow(String dbName) throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    FeDb db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return db;
  }

  @Override
  public FeFsPartition getHdfsPartition(
      String db, String tbl, List<TPartitionKeyValue> partition_spec)
      throws CatalogException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public List<? extends FeDataSource> getDataSources(
      PatternMatcher createHivePatternMatcher) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public FeDataSource getDataSource(String dataSourceName) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Function getFunction(Function desc, CompareMode mode) {
    FeDb db = getDb(desc.dbName());
    if (db == null) return null;
    return db.getFunction(desc, mode);
  }

  @Override
  public HdfsCachePool getHdfsCachePool(String poolName) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void prioritizeLoad(Set<TableName> tableNames) {
    // No-op for local catalog.
  }

  @Override
  public void waitForCatalogUpdate(long timeoutMs) {
    // No-op for local catalog.
  }

  @Override
  public Path getTablePath(Table msTbl) {
    // If the table did not have its path set, build the path based on the
    // location property of the parent database.
    if (msTbl.getSd().getLocation() == null || msTbl.getSd().getLocation().isEmpty()) {
      String dbLocation = getDb(msTbl.getDbName()).getMetaStoreDb().getLocationUri();
      return new Path(dbLocation, msTbl.getTableName().toLowerCase());
    } else {
      return new Path(msTbl.getSd().getLocation());
    }
  }

  @Override
  public TUniqueId getCatalogServiceId() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    return null; // TODO(todd): implement auth policy
  }

  @Override
  public String getDefaultKuduMasterHosts() {
    return defaultKuduMasterHosts_;
  }

  public String getNullPartitionKeyValue() {
    if (nullPartitionKeyValue_ == null) {
      try {
        nullPartitionKeyValue_ =
            metaProvider_.loadNullPartitionKeyValue();
      } catch (TException e) {
        throw new LocalCatalogException(
            "Could not load null partition key value", e);
      }
    }
    return nullPartitionKeyValue_;
  }

  @Override
  public boolean isReady() {
    // We are always ready.
    return true;
  }

  @Override
  public void setIsReady(boolean isReady) {
    // No-op for local catalog.
  }

  MetaProvider getMetaProvider() {
    return metaProvider_;
  }
}