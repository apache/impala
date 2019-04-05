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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.FileMetadataLoader;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.Immutable;

/**
 * Metadata provider which calls out directly to the source systems
 * (filesystem, HMS, etc) with no caching.
 */
class DirectMetaProvider implements MetaProvider {
  private static MetaStoreClientPool msClientPool_;

  DirectMetaProvider() {
    initMsClientPool();
  }

  private static synchronized void initMsClientPool() {
    // Lazy-init the metastore client pool based on the backend configuration.
    // TODO(todd): this should probably be a process-wide singleton.
    if (msClientPool_ == null) {
      TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
      msClientPool_ = new MetaStoreClientPool(cfg.num_metadata_loading_threads,
          cfg.initial_hms_cnxn_timeout_s);
    }
  }


  @Override
  public AuthorizationPolicy getAuthPolicy() {
    throw new UnsupportedOperationException("not supported");
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
  public ImmutableList<String> loadTableNames(String dbName)
      throws MetaException, UnknownDBException, TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return ImmutableList.copyOf(c.getHiveClient().getAllTables(dbName));
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
  public Map<String, PartitionMetadata> loadPartitionsByRefs(
      TableMetaRef table, List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs) throws MetaException, TException {
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
          c.getHiveClient(), partNames, tableImpl.dbName_, tableImpl.tableName_);
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
      String partName = FileUtils.makePartName(partitionColumnNames, p.getValues());
      if (!namesSet.contains(partName)) {
        throw new MetaException("HMS returned unexpected partition " + partName +
            " which was not requested. Requested: " + namesSet);
      }

      ImmutableList<FileDescriptor> fds = loadFileMetadata(
          fullTableName, partName, p, hostIndex);

      PartitionMetadata existing = ret.put(partName, new PartitionMetadataImpl(p, fds));
      if (existing != null) {
        throw new MetaException("HMS returned multiple partitions with name " +
            partName);
      }
    }


    return ret;
  }

  /**
   * We model partitions slightly differently to Hive. So, in the case of an
   * unpartitioned table, we have to create a fake Partition object which has the
   * metadata of the table.
   */
  private Map<String, PartitionMetadata> loadUnpartitionedPartition(
      TableMetaRefImpl table, List<PartitionRef> partitionRefs,
      ListMap<TNetworkAddress> hostIndex) {
    Preconditions.checkArgument(partitionRefs.size() == 1,
        "Expected exactly one partition to load for unpartitioned table");
    PartitionRef ref = partitionRefs.get(0);
    Preconditions.checkArgument(ref.getName().isEmpty(),
        "Expected empty partition name for unpartitioned table");
    Partition msPartition = msTableToPartition(table.msTable_);
    String fullName = table.dbName_ + "." + table.tableName_;
    ImmutableList<FileDescriptor> fds = loadFileMetadata(fullName,
        "default",  msPartition, hostIndex);
    return ImmutableMap.of("", (PartitionMetadata)new PartitionMetadataImpl(
        msPartition, fds));
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
  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return c.getHiveClient().getTableColumnStatistics(
          ((TableMetaRefImpl)table).dbName_,
          ((TableMetaRefImpl)table).tableName_,
          colNames);
    }
  }

  private ImmutableList<FileDescriptor> loadFileMetadata(String fullTableName,
      String partName, Partition msPartition, ListMap<TNetworkAddress> hostIndex) {
    Path partDir = new Path(msPartition.getSd().getLocation());
    FileMetadataLoader fml = new FileMetadataLoader(partDir,
        /* oldFds= */Collections.emptyList(),
        hostIndex);

    try {
      fml.load();
    } catch (FileNotFoundException fnf) {
      // If the partition directory isn't found, this is treated as having no
      // files.
      return ImmutableList.of();
    } catch (IOException ioe) {
      throw new LocalCatalogException(String.format(
          "Could not load files for partition %s of table %s",
          partName, fullTableName), ioe);
    }

    return ImmutableList.copyOf(fml.getLoadedFds());
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

    @Override
    public boolean hasIncrementalStats() {
      return false; /* Incremental stats not supported in direct mode */
    }

    @Override
    public byte[] getPartitionStats() {
      throw new UnsupportedOperationException("Incremental stats not supported with " +
          "DirectMetaProvider implementation.");
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

    private boolean isPartitioned() {
      return msTable_.getPartitionKeysSize() != 0;
    }
  }
}
