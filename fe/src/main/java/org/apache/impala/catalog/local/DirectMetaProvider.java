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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.FileMetadataLoader;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata provider which calls out directly to the source systems
 * (filesystem, HMS, etc) with no caching.
 */
class DirectMetaProvider implements MetaProvider {
  private final static Logger LOG = LoggerFactory.getLogger(DirectMetaProvider.class);
  private static MetaStoreClientPool msClientPool_;

  DirectMetaProvider() {
    initMsClientPool();
  }

  private static synchronized void initMsClientPool() {
    // Lazy-init the metastore client pool based on the backend configuration.
    // TODO(todd): this should probably be a process-wide singleton.
    if (msClientPool_ == null) {
      TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
      if (MetastoreShim.getMajorVersion() > 2) {
        MetastoreShim.setHiveClientCapabilities();
      }
      msClientPool_ = new MetaStoreClientPool(cfg.num_metadata_loading_threads,
          cfg.initial_hms_cnxn_timeout_s);
    }
  }

  @Override
  public Iterable<HdfsCachePool> getHdfsCachePools() {
    throw new UnsupportedOperationException(
        "HDFSCachePools are not supported in DirectMetaProvider");
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    throw new UnsupportedOperationException(
        "Authorization is not supported in DirectMetaProvider");
  }

  @Override
  public boolean isReady() {
    // Direct provider is always ready since we don't need to wait for
    // an update from any external process.
    return true;
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return ImmutableList.copyOf(c.getHiveClient().getAllDatabases());
    }
  }

  @Override
  public Database loadDb(String dbName) throws TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return c.getHiveClient().getDatabase(dbName);
    }
  }

  @Override
  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      List<TableMeta> tableMetaList = c.getHiveClient().getTableMeta(dbName,
          /*tablePatterns=*/null, /*tableTypes=*/null);
      ImmutableList.Builder<TBriefTableMeta> ret = ImmutableList.builder();
      for (TableMeta meta : tableMetaList) {
        TBriefTableMeta briefMeta = new TBriefTableMeta(meta.getTableName());
        briefMeta.setMsType(meta.getTableType());
        briefMeta.setComment(meta.getComments());
        ret.add(briefMeta);
      }
      return ret.build();
    }
  }

  @Override
  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tblName) {
    try {
      return loadTable(dbName, tblName);
    } catch (TException e) {
      LOG.error("Failed to load table", e);
      return null;
    }
  }

  @Override
  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, TException {
    Table msTable;
    try (MetaStoreClient c = msClientPool_.getClient()) {
      msTable = c.getHiveClient().getTable(dbName, tableName);
    }
    TableMetaRef ref = new TableMetaRefImpl(dbName, tableName, msTable);
    return Pair.create(msTable, ref);
  }

  @Override
  public String loadNullPartitionKeyValue() throws MetaException, TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return MetaStoreUtil.getNullPartitionKeyValue(c.getHiveClient());
    }
  }

  @Override
  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl ref = (TableMetaRefImpl)table;

    // If the table isn't partitioned, just return a single partition with no name.
    // In loadPartitionsByRefs() below, we'll detect this case and load the special
    // unpartitioned table.
    if (!ref.isPartitioned()) {
      return ImmutableList.of((PartitionRef)new PartitionRefImpl(
          PartitionRefImpl.UNPARTITIONED_NAME));
    }

    List<String> partNames;
    try (MetaStoreClient c = msClientPool_.getClient()) {
      partNames = c.getHiveClient().listPartitionNames(
          ref.dbName_, ref.tableName_, /*max_parts=*/(short)-1);
    }
    List<PartitionRef> partRefs = Lists.newArrayListWithCapacity(partNames.size());
    for (String name: partNames) {
      partRefs.add(new PartitionRefImpl(name));
    }
    return partRefs;
  }

  @Override
  public SqlConstraints loadConstraints(
      TableMetaRef table, Table msTbl) throws TException {
    SqlConstraints sqlConstraints;
    try (MetaStoreClient c = msClientPool_.getClient()) {
      sqlConstraints = new SqlConstraints(c.getHiveClient().getPrimaryKeys(
          new PrimaryKeysRequest(msTbl.getDbName(), msTbl.getTableName())),
          c.getHiveClient().getForeignKeys(new ForeignKeysRequest(null,
          null, msTbl.getDbName(), msTbl.getTableName())));
    }
    return sqlConstraints;
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(
      TableMetaRef table, List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs) throws CatalogException, TException {
    Preconditions.checkNotNull(table);
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    Preconditions.checkArgument(!partitionColumnNames.isEmpty());
    Preconditions.checkNotNull(partitionRefs);

    TableMetaRefImpl tableImpl = (TableMetaRefImpl)table;

    String fullTableName = tableImpl.dbName_ + "." + tableImpl.tableName_;

    if (!((TableMetaRefImpl)table).isPartitioned()) {
      return loadUnpartitionedPartition((TableMetaRefImpl)table, partitionRefs,
          hostIndex);
    }

    Map<String, PartitionMetadata> ret = Maps.newHashMapWithExpectedSize(
        partitionRefs.size());
    if (partitionRefs.isEmpty()) return ret;

    // Fetch the partitions.
    List<String> partNames = Lists.newArrayListWithCapacity(partitionRefs.size());
    for (PartitionRef ref: partitionRefs) {
      partNames.add(ref.getName());
    }

    List<Partition> parts;
    try (MetaStoreClient c = msClientPool_.getClient()) {
      parts = MetaStoreUtil.fetchPartitionsByName(
          c.getHiveClient(), partNames, tableImpl.msTable_);
    }

    // HMS may return fewer partition objects than requested, and the
    // returned partition objects don't carry enough information to get their
    // names. So, we map the returned partitions back to the requested names
    // using the passed-in partition column names.
    Set<String> namesSet = ImmutableSet.copyOf(partNames);
    for (Partition p: parts) {
      List<String> vals = p.getValues();
      if (vals.size() != partitionColumnNames.size()) {
        throw new MetaException("Unexpected number of partition values for " +
          "partition " + vals + " (expected " + partitionColumnNames.size() + ")");
      }
      String partName = MetastoreShim.makePartName(partitionColumnNames, p.getValues());
      if (!namesSet.contains(partName)) {
        throw new MetaException("HMS returned unexpected partition " + partName +
            " which was not requested. Requested: " + namesSet);
      }

      FileMetadataLoader loader = loadFileMetadata(fullTableName, partName, p, hostIndex);
      PartitionMetadata existing = ret.put(partName, createPartMetadataImpl(p, loader));
      if (existing != null) {
        throw new MetaException("HMS returned multiple partitions with name " +
            partName);
      }
    }

    return ret;
  }

  private PartitionMetadataImpl createPartMetadataImpl(Partition p,
      FileMetadataLoader loader) {
    List<FileDescriptor> deleteDescriptors = loader.getLoadedDeleteDeltaFds();
    ImmutableList<FileDescriptor> fds;
    ImmutableList<FileDescriptor> insertFds;
    ImmutableList<FileDescriptor> deleteFds;
    if (deleteDescriptors != null && !deleteDescriptors.isEmpty()) {
      fds = ImmutableList.copyOf(Collections.emptyList());
      insertFds = ImmutableList.copyOf(loader.getLoadedInsertDeltaFds());
      deleteFds = ImmutableList.copyOf(loader.getLoadedDeleteDeltaFds());
    } else {
      fds = ImmutableList.copyOf(loader.getLoadedFds());
      insertFds = ImmutableList.copyOf(Collections.emptyList());
      deleteFds = ImmutableList.copyOf(Collections.emptyList());
    }
    return new PartitionMetadataImpl(p, fds, insertFds, deleteFds);
  }

  /**
   * We model partitions slightly differently to Hive. So, in the case of an
   * unpartitioned table, we have to create a fake Partition object which has the
   * metadata of the table.
   */
  private Map<String, PartitionMetadata> loadUnpartitionedPartition(
      TableMetaRefImpl table, List<PartitionRef> partitionRefs,
      ListMap<TNetworkAddress> hostIndex) throws CatalogException {
    //TODO(IMPALA-9042): Remove "throws MetaException"
    Preconditions.checkArgument(partitionRefs.size() == 1,
        "Expected exactly one partition to load for unpartitioned table");
    PartitionRef ref = partitionRefs.get(0);
    Preconditions.checkArgument(ref.getName().isEmpty(),
        "Expected empty partition name for unpartitioned table");
    Partition msPartition = msTableToPartition(table.msTable_);
    String fullName = table.dbName_ + "." + table.tableName_;
    FileMetadataLoader loader = loadFileMetadata(fullName,
        "default",  msPartition, hostIndex);
    return ImmutableMap.of("",
        (PartitionMetadata)createPartMetadataImpl(msPartition, loader));
  }

  static Partition msTableToPartition(Table msTable) {
    Partition msp = new Partition();
    msp.setSd(msTable.getSd());
    msp.setParameters(msTable.getParameters());
    msp.setValues(Collections.<String>emptyList());
    return msp;
  }

  @Override
  public List<String> loadFunctionNames(String dbName) throws TException {
    // NOTE: this is a bit tricky to implement since functions may be stored as
    // HMS functions or serialized functions in the DB parameters themselves. We
    // need to do some refactoring to support this in DirectMetaProvider. Some code
    // used to exist to do this in LocalDb -- look back in git history if you want to
    // revive the usefulness of DirectMetaProvider.
    throw new UnsupportedOperationException(
        "Functions not supported by DirectMetaProvider");
  }


  @Override
  public ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException {
    // See above.
    throw new UnsupportedOperationException(
        "Functions not supported by DirectMetaProvider");
  }

  @Override
  public ImmutableList<DataSource> loadDataSources() throws TException {
    // See above.
    throw new UnsupportedOperationException(
        "DataSource not supported by DirectMetaProvider");
  }

  @Override
  public DataSource loadDataSource(String dsName) throws TException {
    // See above.
    throw new UnsupportedOperationException(
        "DataSource not supported by DirectMetaProvider");
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return MetastoreShim.getTableColumnStatistics(
          c.getHiveClient(),
          ((TableMetaRefImpl)table).dbName_,
          ((TableMetaRefImpl)table).tableName_,
          colNames);
    }
  }

  private FileMetadataLoader loadFileMetadata(String fullTableName,
      String partName, Partition msPartition, ListMap<TNetworkAddress> hostIndex)
        throws CatalogException {
    //TODO(IMPALA-9042): Remove "throws MetaException"
    Path partDir = new Path(msPartition.getSd().getLocation());
    // TODO(todd): The table property to disable recursive loading is not supported
    // by this code path. However, DirectMetaProvider is not yet a supported feature.
    // TODO(todd) this code path would have to change to handle ACID tables -- we don't
    // have the write ID list passed down at this point in the code.
    FileMetadataLoader fml = new FileMetadataLoader(partDir,
        /* recursive= */BackendConfig.INSTANCE.recursivelyListPartitions(),
        /* oldFds= */Collections.emptyList(),
        hostIndex,
        /* validTxnList=*/null,
        /* writeIds=*/null);

    try {
      fml.load();
    } catch (FileNotFoundException fnf) {
      // If the partition directory isn't found, this is treated as having no
      // files.
      return fml;
    } catch (IOException ioe) {
      throw new LocalCatalogException(String.format(
          "Could not load files for partition %s of table %s",
          partName, fullTableName), ioe);
    }

    return fml;
  }

  @Immutable
  private static class PartitionRefImpl implements PartitionRef {
    private static final String UNPARTITIONED_NAME = "";
    private final String name_;

    public PartitionRefImpl(String name) {
      this.name_ = name;
    }

    @Override
    public String getName() {
      return name_;
    }
  }

  private static class PartitionMetadataImpl implements PartitionMetadata {
    private final Partition msPartition_;
    private final ImmutableList<FileDescriptor> fds_;
    private final ImmutableList<FileDescriptor> insertFds_;
    private final ImmutableList<FileDescriptor> deleteFds_;

    public PartitionMetadataImpl(Partition msPartition,
        ImmutableList<FileDescriptor> fds, ImmutableList<FileDescriptor> insertFds,
        ImmutableList<FileDescriptor> deleteFds) {
      this.msPartition_ = msPartition;
      this.fds_ = fds;
      this.insertFds_ = insertFds;
      this.deleteFds_ = deleteFds;
    }

    @Override
    public Map<String, String> getHmsParameters() { return msPartition_.getParameters(); }

    @Override
    public long getWriteId() {
      return MetastoreShim.getWriteIdFromMSPartition(msPartition_);
    }

    @Override
    public HdfsStorageDescriptor getInputFormatDescriptor() {
      String tblName = msPartition_.getDbName() + "." + msPartition_.getTableName();
      try {
        return HdfsStorageDescriptor.fromStorageDescriptor(tblName, msPartition_.getSd());
      } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
        throw new LocalCatalogException(String.format(
            "Invalid input format descriptor for partition (values=%s) of table %s",
            msPartition_.getValues(), tblName), e);
      }
    }

    @Override
    public HdfsPartitionLocationCompressor.Location getLocation() {
      // Treat as no prefix compression.
      return new HdfsPartitionLocationCompressor(0).new Location(
          msPartition_.getSd().getLocation());
    }

    @Override
    public ImmutableList<FileDescriptor> getFileDescriptors() {
      if (insertFds_.isEmpty()) return fds_;
      List<FileDescriptor> ret = Lists.newArrayListWithCapacity(
          insertFds_.size() + deleteFds_.size());
      ret.addAll(insertFds_);
      ret.addAll(deleteFds_);
      return ImmutableList.copyOf(ret);
    }

    @Override
    public ImmutableList<FileDescriptor> getInsertFileDescriptors() {
      return insertFds_;
    }

    @Override
    public ImmutableList<FileDescriptor> getDeleteFileDescriptors() {
      return deleteFds_;
    }

    @Override
    public boolean hasIncrementalStats() {
      return false; /* Incremental stats not supported in direct mode */
    }

    @Override
    public byte[] getPartitionStats() {
      throw new UnsupportedOperationException("Incremental stats not supported with " +
          "DirectMetaProvider implementation.");
    }

    @Override
    public boolean isMarkedCached() {
      throw new UnsupportedOperationException("Hdfs caching not supported with " +
          "DirectMetaProvider implementation");
    }

    @Override
    public long getLastCompactionId() {
      throw new UnsupportedOperationException("Compaction id is not provided with " +
          "DirectMetaProvider implementation");
    }
  }

  private class TableMetaRefImpl implements TableMetaRef {

    private final String dbName_;
    private final String tableName_;
    private final Table msTable_;

    public TableMetaRefImpl(String dbName, String tableName, Table msTable) {
      this.dbName_ = dbName;
      this.tableName_ = tableName;
      this.msTable_ = msTable;
    }

    @Override
    public boolean isPartitioned() {
      return msTable_.getPartitionKeysSize() != 0;
    }

    @Override
    public boolean isMarkedCached() {
      throw new UnsupportedOperationException("Hdfs caching not supported with " +
          "DirectMetaProvider implementation");
    }

    @Override
    public List<String> getPartitionPrefixes() {
      return Collections.emptyList();
    }

    @Override
    public boolean isTransactional() {
      return AcidUtils.isTransactionalTable(msTable_.getParameters());
    }

    @Override
    public List<VirtualColumn> getVirtualColumns() {
      throw new UnsupportedOperationException("Virtual columns are not supported with " +
          "DirectMetaProvider implementation");
    }
  }

  @Override
  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    throw new NotImplementedException(
        "getValidWriteIdList() is not implemented for DirectMetaProvider");
  }

  /**
   * Fetches the latest compaction id from HMS and compares with partition metadata in
   * cache. If a partition is stale due to compaction, removes it from metas.
   */
  public List<PartitionRef> checkLatestCompaction(String dbName, String tableName,
      TableMetaRef table, Map<PartitionRef, PartitionMetadata> metas) throws TException {
    Preconditions.checkNotNull(table, "TableMetaRef must be non-null");
    Preconditions.checkNotNull(metas, "Partition map must be non-null");

    List<PartitionRef> stalePartitions = MetastoreShim.checkLatestCompaction(
        msClientPool_, dbName, tableName, table, metas,
        PartitionRefImpl.UNPARTITIONED_NAME);
    return stalePartitions;
  }

  @Override
  public TPartialTableInfo loadIcebergTable(final TableMetaRef table) throws TException {
    throw new NotImplementedException(
        "loadIcebergTable() is not implemented for DirectMetaProvider");
  }

  @Override
  public org.apache.iceberg.Table loadIcebergApiTable(final TableMetaRef table,
      TableParams param, Table msTable) throws TException {
    throw new NotImplementedException(
        "loadIcebergApiTable() is not implemented for DirectMetaProvider");
  }
}
