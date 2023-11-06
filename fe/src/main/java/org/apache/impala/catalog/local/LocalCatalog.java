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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.PartitionNotFoundException;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
  private final static Logger LOG = LoggerFactory.getLogger(LocalCatalog.class);

  private final MetaProvider metaProvider_;
  private Map<String, FeDb> dbs_ = new HashMap<>();
  private Map<String, HdfsCachePool> hdfsCachePools_ = null;
  private String nullPartitionKeyValue_;
  private final String defaultKuduMasterHosts_;

  public LocalCatalog(MetaProvider metaProvider, String defaultKuduMasterHosts) {
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
    Map<String, FeDb> dbs = new HashMap<>();
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
    return getTableNames(dbName, matcher, /*tableTypes*/ Collections.emptySet());
  }

  @Override
  public List<String> getTableNames(String dbName, PatternMatcher matcher,
      Set<TImpalaTableType> tableTypes)
      throws DatabaseNotFoundException {
    FeDb db = getDbOrThrow(dbName);
    return Catalog.filterStringsByPattern(db.getAllTableNames(tableTypes),
        matcher);
  }

  @Override
  public FeTable getTable(String dbName, String tableName)
      throws DatabaseNotFoundException {
    return getDbOrThrow(dbName).getTable(tableName);
  }

  @Override
  public FeTable getTableNoThrow(String dbName, String tableName) {
    try {
      return getTable(dbName, tableName);
    } catch (Exception e) {
      // pass
    }
    return null;
  }

  @Override
  public FeTable getTableIfCached(String dbName, String tableName)
      throws DatabaseNotFoundException {
    return getDbOrThrow(dbName).getTableIfCached(tableName);
  }

  @Override
  public FeTable getTableIfCachedNoThrow(String dbName, String tableName) {
    try {
      return getTableIfCached(dbName, tableName);
    } catch (Exception e) {
      // pass
    }
    return null;
  }

  @Override
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    // TODO(todd): this probably makes the /catalog page not load with an error.
    // We should probably disable that page in local-catalog mode.
    Preconditions.checkNotNull(objectDesc, "invalid objectDesc");
    if (objectDesc.type == TCatalogObjectType.DATA_SOURCE) {
      // This function could be called by backend function
      // CatalogOpExecutor::HandleDropDataSource() when cleaning jar file of data source.
      TDataSource dsDesc = Preconditions.checkNotNull(objectDesc.data_source);
      String dsName = dsDesc.getName();
      if (dsName != null && !dsName.isEmpty()) {
        try {
          DataSource ds = metaProvider_.loadDataSource(dsName);
          if (ds != null) {
            TCatalogObject resultObj = new TCatalogObject();
            resultObj.setType(TCatalogObjectType.DATA_SOURCE);
            resultObj.setData_source(ds.toThrift());
            return resultObj;
          }
        } catch (Exception e) {
          LOG.info("Data source not found: " + dsName + ", " + e.getMessage());
        }
      }
      throw new CatalogException("Data source not found: " + dsName);
    } else {
      throw new UnsupportedOperationException("LocalCatalog.getTCatalogObject");
    }
  }

  @Override
  public FeDb getDb(String db) {
    loadDbs();
    return dbs_.get(db.toLowerCase());
  }

  private FeDb getDbOrThrow(String dbName) throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    FeDb db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return db;
  }

  private void throwPartitionNotFound(List<TPartitionKeyValue> partitionSpec)
      throws PartitionNotFoundException {
    throw new PartitionNotFoundException(
        "Partition not found: " + Joiner.on(", ").join(partitionSpec));
  }

  @Override
  public FeFsPartition getHdfsPartition(
      String db, String tbl, List<TPartitionKeyValue> partitionSpec)
      throws CatalogException {
    // TODO(todd): somewhat copy-pasted from Catalog.getHdfsPartition

    FeTable table = getTable(db, tbl);
    // This is not an FS table, throw an error.
    if (!(table instanceof FeFsTable)) {
      throwPartitionNotFound(partitionSpec);
    }
    // Get the FeFsPartition object for the given partition spec.
    PrunablePartition partition = FeFsTable.Utils.getPartitionFromThriftPartitionSpec(
        (FeFsTable)table, partitionSpec);
    if (partition == null) throwPartitionNotFound(partitionSpec);
    return FeCatalogUtils.loadPartition((FeFsTable)table, partition.getId());
  }

  @Override
  public List<? extends FeDataSource> getDataSources(PatternMatcher matcher) {
    try {
      List<DataSource> dataSrcs = metaProvider_.loadDataSources();
      return Catalog.filterCatalogObjectsByPattern(dataSrcs, matcher);
    } catch (Exception e) {
      LOG.info("Unable to load DataSource objects, ", e);
      // Return empty list.
      return Lists.newArrayList();
    }
  }

  @Override
  public FeDataSource getDataSource(String dsName) {
    Preconditions.checkNotNull(dsName);
    try {
      return metaProvider_.loadDataSource(dsName);
    } catch (Exception e) {
      LOG.info("DataSource not found: " + dsName, e);
      return null;
    }
  }

  @Override
  public Function getFunction(Function desc, CompareMode mode) {
    FeDb db = getDb(desc.dbName());
    if (db == null) return null;
    return db.getFunction(desc, mode);
  }

  @Override
  public HdfsCachePool getHdfsCachePool(String poolName) {
    loadHdfsCachePools();
    return hdfsCachePools_.get(poolName);
  }

  private void loadHdfsCachePools() {
    if (hdfsCachePools_ != null) return;
    hdfsCachePools_ = new HashMap<>();
    for (HdfsCachePool pool : metaProvider_.getHdfsCachePools()) {
      hdfsCachePools_.put(pool.getName(), pool);
    }
  }

  @Override
  public void prioritizeLoad(Set<TableName> tableNames, TUniqueId queryId) {
    // No-op for local catalog.
  }

  @Override
  public TGetPartitionStatsResponse getPartitionStats(
      TableName table) throws InternalException {
    // TODO(IMPALA-7535) lazy-fetch incremental stats for LocalCatalog
    throw new UnsupportedOperationException("Stats are eagerly fetched in LocalCatalog");
  }

  @Override
  public void waitForCatalogUpdate(long timeoutMs) {
    if (isReady()) return;
    // Sleep here to avoid log spew from the retry loop in Frontend.
    try {
      Thread.sleep(timeoutMs);
    } catch (InterruptedException e) {
      // Ignore
    }
  }

  @Override
  public TUniqueId getCatalogServiceId() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    return metaProvider_.getAuthPolicy();
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
    return metaProvider_.isReady();
  }

  @Override
  public void setIsReady(boolean isReady) {
    // No-op for local catalog.
    // This appears to only be used in some tests.
  }

  public MetaProvider getMetaProvider() {
    return metaProvider_;
  }
}
