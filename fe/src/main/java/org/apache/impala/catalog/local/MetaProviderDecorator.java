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

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

/**
 * A MetaProvider that decorates another MetaProvider. All methods simply delegate to
 * the wrapped MetaProvider. This class can be extended to override specific
 * methods to add additional functionality.
 *
 * This class implements the decorator design pattern.
 */
public abstract class MetaProviderDecorator implements MetaProvider {
  private final MetaProvider decoratedObj_;

  protected MetaProviderDecorator(final MetaProvider decoratedOb) {
    this.decoratedObj_ = decoratedOb;
  }

  public String getURI() {
    return this.decoratedObj_.getURI();
  }

  public AuthorizationPolicy getAuthPolicy() {
    return this.decoratedObj_.getAuthPolicy();
  }

  public boolean isReady() {
    return this.decoratedObj_.isReady();
  }

  public void waitForIsReady(long timeoutMs) {
    this.decoratedObj_.waitForIsReady(timeoutMs);
  }

  public void setIsReady(boolean isReady) {
    this.decoratedObj_.setIsReady(isReady);
  }

  public ImmutableList<String> loadDbList() throws TException {
    return this.decoratedObj_.loadDbList();
  }

  public Database loadDb(String dbName) throws TException {
    return this.decoratedObj_.loadDb(dbName);
  }

  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws MetaException, UnknownDBException, TException {
    return this.decoratedObj_.loadTableList(dbName);
  }

  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException {
    return this.decoratedObj_.loadTable(dbName, tableName);
  }

  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tableName) {
    return this.decoratedObj_.getTableIfPresent(dbName, tableName);
  }

  public String loadNullPartitionKeyValue()
      throws MetaException, TException {
    return this.decoratedObj_.loadNullPartitionKeyValue();
  }

  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException {
    return this.decoratedObj_.loadPartitionList(table);
  }

  public SqlConstraints loadConstraints(TableMetaRef table,
      Table msTbl) throws MetaException, TException {
    return this.decoratedObj_.loadConstraints(table, msTbl);
  }

  public List<String> loadFunctionNames(String dbName) throws TException {
    return this.decoratedObj_.loadFunctionNames(dbName);
  }

  public ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException {
    return this.decoratedObj_.loadFunction(dbName, functionName);
  }

  public ImmutableList<DataSource> loadDataSources() throws TException {
    return this.decoratedObj_.loadDataSources();
  }

  public DataSource loadDataSource(String dsName) throws TException {
    return this.decoratedObj_.loadDataSource(dsName);
  }

  public Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames, ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws MetaException, TException, CatalogException {
    return this.decoratedObj_.loadPartitionsByRefs(table, partitionColumnNames, hostIndex,
        partitionRefs);
  }

  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    return this.decoratedObj_.loadTableColumnStatistics(table, colNames);
  }

  public TPartialTableInfo loadIcebergTable(
      final TableMetaRef table) throws TException {
    return this.decoratedObj_.loadIcebergTable(table);
  }

  public org.apache.iceberg.Table loadIcebergApiTable(
      final TableMetaRef table, TableParams param, Table msTable) throws TException {
    return this.decoratedObj_.loadIcebergApiTable(table, param, msTable);
  }

  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    return this.decoratedObj_.getValidWriteIdList(ref);
  }

  public Iterable<HdfsCachePool> getHdfsCachePools() {
    return this.decoratedObj_.getHdfsCachePools();
  }

}
