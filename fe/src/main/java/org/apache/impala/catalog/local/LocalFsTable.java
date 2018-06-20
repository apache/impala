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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class LocalFsTable extends LocalTable implements FeFsTable {
  /**
   * Map from partition ID to partition spec.
   *
   * Set by loadPartitionSpecs().
   */
  ImmutableMap<Long, LocalPartitionSpec> partitionSpecs_;

  /**
   * For each partition column, a map from value to the set of partition IDs
   * having that value.
   *
   * Set by loadPartitionValueMap().
   */
  private ArrayList<TreeMap<LiteralExpr, HashSet<Long>>> partitionValueMap_;

  /**
   * For each partition column, the set of partition IDs having a NULL value
   * for that column.
   *
   * Set by loadPartitionValueMap().
   */
  private ArrayList<HashSet<Long>> nullPartitionIds_;

  /**
   * Map assigning integer indexes for the hosts containing blocks for this table.
   * This is updated as a side effect of LocalFsPartition.loadFileDescriptors().
   */
  private final ListMap<TNetworkAddress> hostIndex_ = new ListMap<>();

  public LocalFsTable(LocalDb db, String tblName, SchemaInfo schemaInfo) {
    super(db, tblName, schemaInfo);
  }

  @Override
  public boolean isCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isLocationCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isMarkedCached() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getLocation() {
    return getMetaStoreTable().getSd().getLocation();
  }

  @Override
  public String getNullPartitionKeyValue() {
    return db_.getCatalog().getNullPartitionKeyValue();
  }

  @Override
  public TResultSet getFiles(List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException {
    // TODO(todd): implement for SHOW FILES.
    return null;
  }

  @Override
  public String getHdfsBaseDir() {
    // TODO(todd): this is redundant with getLocation, it seems.
    return getLocation();
  }

  @Override
  public long getTotalHdfsBytes() {
    // TODO(todd): this is slow because it requires loading all partitions. Remove if possible.
    long size = 0;
    for (FeFsPartition p: loadPartitions(getPartitionIds())) {
      size += p.getSize();
    }
    return size;
  }

  @Override
  public boolean isAvroTable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HdfsFileFormat getMajorityFormat() {
    // Needed by HdfsTableSink.
    throw new UnsupportedOperationException("TODO: implement me");
  }

  @Override
  public long getExtrapolatedNumRows(long totalBytes) {
    // TODO Auto-generated method stub
    return -1;
  }

  @Override
  public boolean isStatsExtrapolationEnabled() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public TResultSet getTableStats() {
    return HdfsTable.getTableStats(this);
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    Preconditions.checkNotNull(referencedPartitions);
    Map<Long, THdfsPartition> idToPartition = Maps.newHashMap();
    List<? extends FeFsPartition> partitions = loadPartitions(referencedPartitions);
    for (FeFsPartition partition : partitions) {
      idToPartition.put(partition.getId(),
          FeCatalogUtils.fsPartitionToThrift(partition,
              /*includeFileDesc=*/false,
              /*includeIncrementalStats=*/false));
    }

    THdfsPartition tPrototypePartition = FeCatalogUtils.fsPartitionToThrift(
        createPrototypePartition(),
        /*includeFileDesc=*/false,
        /*includeIncrementalStats=*/false);

    // TODO(todd): implement avro schema support
    // TODO(todd): set multiple_filesystems member?
    THdfsTable hdfsTable = new THdfsTable(getHdfsBaseDir(), getColumnNames(),
        getNullPartitionKeyValue(), schemaInfo_.getNullColumnValue(), idToPartition,
        tPrototypePartition);

    TTableDescriptor tableDesc = new TTableDescriptor(tableId, TTableType.HDFS_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(), name_, db_.getName());
    tableDesc.setHdfsTable(hdfsTable);
    return tableDesc;
  }

  private LocalFsPartition createPrototypePartition() {
    Partition protoMsPartition = new Partition();
    protoMsPartition.setSd(getMetaStoreTable().getSd());
    protoMsPartition.setParameters(Collections.<String, String>emptyMap());
    LocalPartitionSpec spec = new LocalPartitionSpec(
        this, "", CatalogObjectsConstants.PROTOTYPE_PARTITION_ID);
    LocalFsPartition prototypePartition = new LocalFsPartition(
        this, spec, protoMsPartition);
    return prototypePartition;
  }

  @Override
  public Collection<? extends PrunablePartition> getPartitions() {
    loadPartitionSpecs();
    return partitionSpecs_.values();
  }

  @Override
  public Set<Long> getPartitionIds() {
    loadPartitionSpecs();
    return partitionSpecs_.keySet();
  }

  @Override
  public Map<Long, ? extends PrunablePartition> getPartitionMap() {
    loadPartitionSpecs();
    return partitionSpecs_;
  }

  @Override
  public TreeMap<LiteralExpr, HashSet<Long>> getPartitionValueMap(int col) {
    loadPartitionValueMap();
    return partitionValueMap_.get(col);
  }

  @Override
  public Set<Long> getNullPartitionIds(int colIdx) {
    loadPartitionValueMap();
    return nullPartitionIds_.get(colIdx);
  }

  @Override
  public List<? extends FeFsPartition> loadPartitions(Collection<Long> ids) {
    Preconditions.checkState(partitionSpecs_ != null,
        "Cannot load partitions without having fetched partition IDs " +
        "from the same LocalFsTable instance");

    // Possible in the case that all partitions were pruned.
    if (ids.isEmpty()) return Collections.emptyList();

    List<String> names = Lists.newArrayList();
    for (Long id : ids) {
      LocalPartitionSpec spec = partitionSpecs_.get(id);
      Preconditions.checkArgument(spec != null, "Invalid partition ID for table %s: %s",
          getFullName(), id);
      String name = spec.getName();
      if (name.isEmpty()) {
        // Unpartitioned tables don't need to fetch partitions from the metadata
        // provider. Rather, we just create a partition on the fly.
        Preconditions.checkState(getNumClusteringCols() == 0,
            "Cannot fetch empty partition name from a partitioned table");
        Preconditions.checkArgument(ids.size() == 1,
            "Expected to only fetch one partition for unpartitioned table %s",
            getFullName());
        return Lists.newArrayList(createUnpartitionedPartition(spec));
      } else {
        names.add(name);
      }
    }
    Map<String, Partition> partsByName;
    try {
      partsByName = db_.getCatalog().getMetaProvider().loadPartitionsByNames(
          db_.getName(), name_, getClusteringColumnNames(), names);
    } catch (TException e) {
      throw new LocalCatalogException(
          "Could not load partitions for table " + getFullName(), e);
    }
    List<FeFsPartition> ret = Lists.newArrayListWithCapacity(ids.size());
    for (Long id : ids) {
      LocalPartitionSpec spec = partitionSpecs_.get(id);
      Partition p = partsByName.get(spec.getName());
      if (p == null) {
        // TODO(todd): concurrent drop partition could result in this error.
        // Should we recover in a more graceful way from such an unexpected event?
        throw new LocalCatalogException(
            "Could not load expected partitions for table " + getFullName() +
            ": missing expected partition with name '" + spec.getName() +
            "' (perhaps it was concurrently dropped by another process)");
      }
      ret.add(new LocalFsPartition(this, spec, p));
    }
    return ret;
  }

  private List<String> getClusteringColumnNames() {
    List<String> names = Lists.newArrayListWithCapacity(getNumClusteringCols());
    for (Column c : getClusteringColumns()) {
      names.add(c.getName());
    }
    return names;
  }

  /**
   * Create a partition which represents the main partition of an unpartitioned
   * table.
   */
  private LocalFsPartition createUnpartitionedPartition(LocalPartitionSpec spec) {
    Preconditions.checkArgument(spec.getName().isEmpty());
    Partition msp = new Partition();
    msp.setSd(getMetaStoreTable().getSd());
    msp.setParameters(getMetaStoreTable().getParameters());
    msp.setValues(Collections.<String>emptyList());
    return new LocalFsPartition(this, spec, msp);
  }

  private LocalPartitionSpec createUnpartitionedPartitionSpec() {
    return new LocalPartitionSpec(this, "", /*id=*/0);
  }

  private void loadPartitionValueMap() {
    if (partitionValueMap_ != null) return;

    loadPartitionSpecs();
    ArrayList<TreeMap<LiteralExpr, HashSet<Long>>> valMapByCol =
        new ArrayList<>();
    ArrayList<HashSet<Long>> nullParts = new ArrayList<>();

    for (int i = 0; i < getNumClusteringCols(); i++) {
      valMapByCol.add(new TreeMap<LiteralExpr, HashSet<Long>>());
      nullParts.add(new HashSet<Long>());
    }
    for (LocalPartitionSpec partition : partitionSpecs_.values()) {
      List<LiteralExpr> vals = partition.getPartitionValues();
      for (int i = 0; i < getNumClusteringCols(); i++) {
        LiteralExpr val = vals.get(i);
        if (val instanceof NullLiteral) {
          nullParts.get(i).add(partition.getId());
          continue;
        }

        HashSet<Long> ids = valMapByCol.get(i).get(val);
        if (ids == null) {
          ids = new HashSet<>();
          valMapByCol.get(i).put(val,  ids);
        }
        ids.add(partition.getId());
      }
    }
    partitionValueMap_ = valMapByCol;
    nullPartitionIds_ = nullParts;
  }

  private void loadPartitionSpecs() {
    if (partitionSpecs_ != null) return;

    if (getNumClusteringCols() == 0) {
      // Unpartitioned table.
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      partitionSpecs_ = ImmutableMap.of(0L, createUnpartitionedPartitionSpec());
      return;
    }
    List<String> partNames;
    try {
      partNames = db_.getCatalog().getMetaProvider().loadPartitionNames(
          db_.getName(), name_);
    } catch (TException e) {
      throw new LocalCatalogException("Could not load partition names for table " +
          getFullName(), e);
    }
    ImmutableMap.Builder<Long, LocalPartitionSpec> b = new ImmutableMap.Builder<>();
    long id = 0;
    for (String partName : partNames) {
      b.put(id, new LocalPartitionSpec(this, partName, id));
      id++;
    }
    partitionSpecs_ = b.build();
  }

  /**
   * Override base implementation to populate column stats for
   * clustering columns based on the partition map.
   */
  @Override
  protected void loadColumnStats() {
    super.loadColumnStats();
    // TODO(todd): this is called for all tables even if not necessary,
    // which means we need to load all partition names, even if not
    // necessary.
    loadPartitionValueMap();
    for (int i = 0; i < getNumClusteringCols(); i++) {
      ColumnStats stats = getColumns().get(i).getStats();
      int nonNullParts = partitionValueMap_.get(i).size();
      int nullParts = nullPartitionIds_.get(i).size();
      stats.setNumDistinctValues(nonNullParts + nullParts);
      // TODO(todd): this calculation ends up setting the num_nulls stat
      // to the number of partitions with null rows, not the number of rows.
      // However, it maintains the existing behavior from HdfsTable.
      stats.setNumNulls(nullParts);
    }
  }

  @Override
  public int parseSkipHeaderLineCount(StringBuilder error) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<Long, List<FileDescriptor>> getFilesSample(
      Collection<? extends FeFsPartition> inputParts,
      long percentBytes, long minSampleBytes, long randomSeed) {
    // TODO(todd): implement me
    return Collections.emptyMap();
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() {
    return hostIndex_;
  }
}
