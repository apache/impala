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

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCatalogInfoSelector;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDbInfoSelector;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.Immutable;

/**
 * MetaProvider which fetches metadata in a granular fashion from the catalogd.
 */
public class CatalogdMetaProvider implements MetaProvider {

  // TODO(todd): currently we haven't implemented catalogd thrift APIs for all pieces
  // of metadata. In order to incrementally build this out, we delegate various calls
  // to the "direct" provider for now and circumvent catalogd.
  private DirectMetaProvider directProvider_ = new DirectMetaProvider();

  /**
   * Send a GetPartialCatalogObject request to catalogd. This handles converting
   * non-OK status responses back to exceptions, performing various generic sanity
   * checks, etc.
   */
  private TGetPartialCatalogObjectResponse sendRequest(
      TGetPartialCatalogObjectRequest req)
      throws TException {
    TGetPartialCatalogObjectResponse resp;
    byte[] ret;
    try {
      ret = FeSupport.GetPartialCatalogObject(new TSerializer().serialize(req));
    } catch (InternalException e) {
      throw new TException(e);
    }
    resp = new TGetPartialCatalogObjectResponse();
    new TDeserializer().deserialize(resp, ret);
    if (resp.status.status_code != TErrorCode.OK) {
      // TODO(todd) do reasonable error handling
      throw new TException(resp.toString());
    }

    // If we requested information about a particular version of an object, but
    // got back a response for a different version, then we have a case of "read skew".
    // For example, we may have fetched the partition list of a table, performed pruning,
    // and then tried to fetch the specific partitions needed for a query, while some
    // concurrent DDL modified the set of partitions. This could result in an unexpected
    // result which violates the snapshot consistency guarantees expected by users.
    if (req.object_desc.isSetCatalog_version() &&
        resp.isSetObject_version_number() &&
        req.object_desc.catalog_version != resp.object_version_number) {
      throw new InconsistentMetadataFetchException(String.format(
          "Catalog object %s changed version from %s to %s while fetching metadata",
          req.object_desc.toString(),
          req.object_desc.catalog_version,
          resp.object_version_number));
    }
    return resp;
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    TGetPartialCatalogObjectRequest req = newReqForCatalog();
    req.catalog_info_selector.want_db_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.catalog_info != null && resp.catalog_info.db_names != null, req,
        "missing table names");
    return ImmutableList.copyOf(resp.catalog_info.db_names);
  }

  private TGetPartialCatalogObjectRequest newReqForCatalog() {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.CATALOG);
    req.catalog_info_selector = new TCatalogInfoSelector();
    return req;
  }

  private TGetPartialCatalogObjectRequest newReqForDb(String dbName) {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.DATABASE);
    req.object_desc.db = new TDatabase(dbName);
    req.db_info_selector = new TDbInfoSelector();
    return req;
  }

  @Override
  public Database loadDb(String dbName) throws TException {
    TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
    req.db_info_selector.want_hms_database = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.db_info != null && resp.db_info.hms_database != null,
        req, "missing expected HMS database");
    return resp.db_info.hms_database;
  }

  @Override
  public ImmutableList<String> loadTableNames(String dbName)
      throws MetaException, UnknownDBException, TException {
    // TODO(todd): do we ever need to fetch the DB without the table names
    // or vice versa?
    TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
    req.db_info_selector.want_table_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.db_info != null && resp.db_info.table_names != null,
        req, "missing expected HMS table");
    return ImmutableList.copyOf(resp.db_info.table_names);
  }

  private TGetPartialCatalogObjectRequest newReqForTable(String dbName,
      String tableName) {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable(dbName, tableName);
    req.table_info_selector = new TTableInfoSelector();
    return req;
  }

  private TGetPartialCatalogObjectRequest newReqForTable(TableMetaRef table) {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl,
        "table ref %s was not created by CatalogdMetaProvider", table);
    TGetPartialCatalogObjectRequest req = newReqForTable(
        ((TableMetaRefImpl)table).dbName_,
        ((TableMetaRefImpl)table).tableName_);
    req.object_desc.setCatalog_version(((TableMetaRefImpl)table).catalogVersion_);
    return req;
  }

  @Override
  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException {
    TGetPartialCatalogObjectRequest req = newReqForTable(dbName, tableName);
    req.table_info_selector.want_hms_table = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.hms_table != null,
        req, "missing expected HMS table");
    TableMetaRef ref = new TableMetaRefImpl(dbName, tableName, resp.table_info.hms_table,
        resp.object_version_number);
    return Pair.create(resp.table_info.hms_table, ref);
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    TGetPartialCatalogObjectRequest req = newReqForTable(table);
    req.table_info_selector.want_stats_for_column_names = colNames;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.column_stats != null,
        req, "missing column stats");
    return resp.table_info.column_stats;
  }

  @Override
  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException {
    TGetPartialCatalogObjectRequest req = newReqForTable(table);
    req.table_info_selector.want_partition_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.partitions != null,
        req, "missing partition list result");
    List<PartitionRef> ret = Lists.newArrayListWithCapacity(
        resp.table_info.partitions.size());
    for (TPartialPartitionInfo p : resp.table_info.partitions) {
      checkResponse(p.isSetId(), req, "response missing partition IDs for partition %s",
          p);
      ret.add(new PartitionRefImpl(p));
    }
    return ret;
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws MetaException, TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl refImpl = (TableMetaRefImpl)table;

    List<Long> ids = Lists.newArrayListWithExpectedSize(partitionRefs.size());
    for (PartitionRef ref: partitionRefs) {
      ids.add(((PartitionRefImpl)ref).getId());
    }

    TGetPartialCatalogObjectRequest req = newReqForTable(table);
    req.table_info_selector.partition_ids = ids;
    req.table_info_selector.want_partition_metadata = true;
    req.table_info_selector.want_partition_files = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.partitions != null,
        req, "missing partition list result");
    checkResponse(resp.table_info.network_addresses != null,
        req, "missing network addresses");
    checkResponse(resp.table_info.partitions.size() == ids.size(),
        req, "returned %d partitions instead of expected %d",
        resp.table_info.partitions.size(), ids.size());

    Map<String, PartitionMetadata> ret = Maps.newHashMapWithExpectedSize(ids.size());
    for (TPartialPartitionInfo part: resp.table_info.partitions) {
      Partition msPart = part.getHms_partition();
      if (msPart == null) {
        checkResponse(refImpl.msTable_.getPartitionKeysSize() == 0, req,
            "Should not return a partition with missing HMS partition unless " +
            "the table is unpartitioned");
        msPart = DirectMetaProvider.msTableToPartition(refImpl.msTable_);
      }
      checkResponse(msPart != null && msPart.getValues() != null, req,
          "malformed partition result: %s", part.toString());
      String partName = FileUtils.makePartName(partitionColumnNames, msPart.getValues());

      checkResponse(part.file_descriptors != null, req, "missing file descriptors");
      List<FileDescriptor> fds = Lists.newArrayListWithCapacity(
          part.file_descriptors.size());
      for (THdfsFileDesc thriftFd: part.file_descriptors) {
        FileDescriptor fd = FileDescriptor.fromThrift(thriftFd);
        // The file descriptors returned via the RPC use host indexes that reference
        // the 'network_addresses' list in the RPC. However, the caller may have already
        // loaded some addresses into 'hostIndex'. So, the returned FDs need to be
        // remapped to point to the caller's 'hostIndex' instead of the list in the
        // RPC response.
        fds.add(fd.cloneWithNewHostIndex(resp.table_info.network_addresses, hostIndex));
      }
      PartitionMetadataImpl metaImpl = new PartitionMetadataImpl(msPart,
          ImmutableList.copyOf(fds));
      PartitionMetadata oldVal = ret.put(partName, metaImpl);
      if (oldVal != null) {
        throw new RuntimeException("catalogd returned partition " + partName +
            " multiple times");
      }
    }
    return ret;
  }

  private static void checkResponse(boolean condition,
      TGetPartialCatalogObjectRequest req, String msg, Object... args) throws TException {
    if (condition) return;
    throw new TException(String.format("Invalid response from catalogd for request " +
        req.toString() + ": " + msg, args));
  }

  @Override
  public String loadNullPartitionKeyValue() throws MetaException, TException {
    return directProvider_.loadNullPartitionKeyValue();
  }

  @Override
  public List<String> loadFunctionNames(String dbName) throws MetaException, TException {
    return directProvider_.loadFunctionNames(dbName);
  }

  @Override
  public Function getFunction(String dbName, String functionName)
      throws MetaException, TException {
    return directProvider_.getFunction(dbName, functionName);
  }

  /**
   * Reference to a partition within a table. We remember the partition's ID and pass
   * that back to the catalog in subsequent requests back to fetch the details of the
   * partition, since the ID is smaller than the name and provides a unique (not-reused)
   * identifier.
   */
  @Immutable
  private static class PartitionRefImpl implements PartitionRef {
    @SuppressWarnings("Immutable") // Thrift objects are mutable, but we won't mutate it.
    private final TPartialPartitionInfo info_;

    public PartitionRefImpl(TPartialPartitionInfo p) {
      this.info_ = p;
    }

    @Override
    public String getName() {
      return info_.getName();
    }

    private long getId() {
      return info_.id;
    }
  }

  public static class PartitionMetadataImpl implements PartitionMetadata {
    private final Partition msPartition_;
    private final ImmutableList<FileDescriptor> fds_;

    public PartitionMetadataImpl(Partition msPartition,
        ImmutableList<FileDescriptor> fds) {
      this.msPartition_ = msPartition;
      this.fds_ = fds;
    }

    @Override
    public Partition getHmsPartition() {
      return msPartition_;
    }

    @Override
    public ImmutableList<FileDescriptor> getFileDescriptors() {
      return fds_;
    }
  }

  /**
   * A reference to a table that has been looked up, allowing callers to fetch further
   * detailed information. This is is more extensive than just the table name so that
   * we can provide a consistency check that the catalog version doesn't change in
   * between calls.
   */
  private static class TableMetaRefImpl implements TableMetaRef {
    private final String dbName_;
    private final String tableName_;

    /**
     * Stash the HMS Table object since we need this in order to handle some strange
     * behavior whereby the catalogd returns a Partition with no HMS partition object
     * in the case of unpartitioned tables.
     */
    private final Table msTable_;

    /**
     * The version of the table when we first loaded it. Subsequent requests about
     * the table are verified against this version.
     */
    private final long catalogVersion_;

    public TableMetaRefImpl(String dbName, String tableName,
        Table msTable, long catalogVersion) {
      this.dbName_ = dbName;
      this.tableName_ = tableName;
      this.msTable_ = msTable;
      this.catalogVersion_ = catalogVersion;
    }
  }
}
