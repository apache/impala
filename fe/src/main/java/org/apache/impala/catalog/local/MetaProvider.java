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
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.VirtualColumn;
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
import com.google.errorprone.annotations.Immutable;

/**
 * Interface for loading metadata. See {@link LocalCatalog} for an example.
 *
 * Implementations may directly access the metadata from the source systems
 * or may include caching, etc.
 */
public interface MetaProvider {

  /**
   * Get the authorization policy. This acts as a repository of authorization
   * metadata.
   */
  AuthorizationPolicy getAuthPolicy();

  /**
   * Return true if the metaprovider is ready to service requests.
   */
  boolean isReady();

  ImmutableList<String> loadDbList() throws TException;

  Database loadDb(String dbName) throws TException;

  ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws MetaException, UnknownDBException, TException;

  Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException;

  Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tableName);

  String loadNullPartitionKeyValue()
      throws MetaException, TException;

  /**
   * Load the list of partitions for the given table. Each returned partition
   * acts as a reference which can later be passed to 'loadPartitionsByRefs' in order
   * to fetch more detailed metadata (e.g. after partition pruning has completed).
   */
  List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException;

  /**
   * Load the list of SQL constraints for the given table.
   */
  SqlConstraints loadConstraints(TableMetaRef table,
      Table msTbl) throws MetaException, TException;

  /**
   * Retrieve the list of functions in the given database.
   */
  List<String> loadFunctionNames(String dbName) throws TException;

  /**
   * Retrieve the specified function from the metadata store. A function may have
   * many overloads with the same name.
   */
  ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException;

  /**
   * Retrieve all DataSource objects.
   */
  ImmutableList<DataSource> loadDataSources() throws TException;

  /**
   * Retrieve the DataSource object for the given DataSource name.
   */
  DataSource loadDataSource(String dsName) throws TException;

  /**
   * Load the given partitions from the specified table.
   *
   * If a requested partition does not exist, no exception will be thrown.
   * Instead, the resulting map will contain no entry for that partition.
   */
  Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames, ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws MetaException, TException, CatalogException;

  /**
   * Load statistics for the given columns from the given table.
   *
   * NOTE: Stats should not be returned for the partition columns of FS-backed
   * tables, since it's assumed that these will be computed by the coordinator.
   */
  List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException;

  /**
   * Loads Iceberg related table metadata.
   */
  public TPartialTableInfo loadIcebergTable(
      final TableMetaRef table) throws TException;

  /**
   * Loads the Iceberg API table metadata through the Iceberg library.
   */
  public org.apache.iceberg.Table loadIcebergApiTable(
      final TableMetaRef table, TableParams param, Table msTable) throws TException;

  /**
   * Reference to a table as returned by loadTable(). This reference must be passed
   * back to other functions to fetch more details about the table. Implementations
   * may use this reference to store internal information such as version numbers
   * in order to perform concurrency control checks, etc.
   */
  interface TableMetaRef {
    boolean isMarkedCached();
    List<String> getPartitionPrefixes();
    boolean isPartitioned();
    boolean isTransactional();
    List<VirtualColumn> getVirtualColumns();
  }

  /**
   * Reference to a partition as returned from loadPartitionList(). These references
   * may be passed back into loadPartitionsByRefs() to load detailed partition metadata.
   */
  @Immutable
  interface PartitionRef {
    String getName();
  }

  /**
   * Partition metadata as returned by loadPartitionsByRefs().
   */
  interface PartitionMetadata {
    Map<String, String> getHmsParameters();
    long getWriteId();
    HdfsStorageDescriptor getInputFormatDescriptor();
    HdfsPartitionLocationCompressor.Location getLocation();
    ImmutableList<FileDescriptor> getFileDescriptors();
    ImmutableList<FileDescriptor> getInsertFileDescriptors();
    ImmutableList<FileDescriptor> getDeleteFileDescriptors();
    byte[] getPartitionStats();
    boolean hasIncrementalStats();
    boolean isMarkedCached();
    long getLastCompactionId();
  }

  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref);

  Iterable<HdfsCachePool> getHdfsCachePools();
}
