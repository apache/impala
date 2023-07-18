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

package org.apache.impala.catalog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.hive.common.MutableValidReaderWriteIdList;
import org.apache.impala.hive.common.MutableValidWriteIdList;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSqlConstraints;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AvroSchemaConverter;
import org.apache.impala.util.AvroSchemaUtils;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.FsPermissionCache;
import org.apache.impala.util.FsPermissionChecker;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.impala.util.TAccessLevelUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import static org.apache.impala.service.CatalogOpExecutor.FETCHED_LATEST_HMS_EVENT_ID;

/**
 * Internal representation of table-related metadata of a file-resident table on a
 * Hadoop filesystem. The table data can be accessed through libHDFS (which is more of
 * an abstraction over Hadoop's FileSystem class rather than DFS specifically). A
 * partitioned table can even span multiple filesystems.
 *
 * This class is not thread-safe. Clients of this class need to protect against
 * concurrent updates using external locking (see CatalogOpExecutor class).
 *
 * Owned by Catalog instance.
 * The partition keys constitute the clustering columns.
 *
 * Partition metadata are propagated to coordinators in different ways depending on the
 * topic update mode.
 * 1. In v1 mode (topicMode = full), we only send the partitionIds in the thrift table
 * which represents the current list of the partitions. Additionally, for each newly
 * added/removed partition we send a THdfsPartition in the same topic update. However,
 * coordinators detect the removal of any partitions by absence of an id inside
 * partitionIds in the table object.
 * 2. In v2 mode (topicMode = minimal), LocalCatalog coordinators only load what they need
 * and hence we only send deleted partitionIds. Updated partitions are also treated as a
 * special case of deleted partitions by sending the previous partitionId for such
 * partitions so that LocalCatalog coordinators invalidate them proactively.
 *
 * In DDL/REFRESH responses, we are still sending the full thrift tables instead of
 * sending incremental updates as in the topic updates. Because catalogd is not aware of
 * the table states (partitionIds) of each coordinators. Generating incremental table
 * updates requires a base status. This will be improved in IMPALA-9936 and IMPALA-9937.
 */
public class HdfsTable extends Table implements FeFsTable {
  // Name of default partition for unpartitioned tables
  public static final String DEFAULT_PARTITION_NAME = "";

  // Number of times to retry fetching the partitions from the HMS should an error occur.
  private final static int NUM_PARTITION_FETCH_RETRIES = 5;

  // Table property key for overriding the Impalad-wide --enable_stats_extrapolation
  // setting for a specific table. By default, tables do not have the property set and
  // rely on the Impalad-wide --enable_stats_extrapolation flag.
  public static final String TBL_PROP_ENABLE_STATS_EXTRAPOLATION =
      "impala.enable.stats.extrapolation";

  // Similar to above: a table property that overwrites --recursively_list_partitions
  // for a specific table. This is an escape hatch in case it turns out that someone
  // was relying on the previous non-recursive behavior, even though it's known to
  // be inconsistent with modern versions of Spark, Hive, etc.
  public static final String TBL_PROP_DISABLE_RECURSIVE_LISTING =
      "impala.disable.recursive.listing";

  // Average memory requirements (in bytes) for storing the metadata of a partition.
  private static final long PER_PARTITION_MEM_USAGE_BYTES = 2048;

  // Average memory requirements (in bytes) for storing a file descriptor.
  private static final long PER_FD_MEM_USAGE_BYTES = 500;

  // Average memory requirements (in bytes) for storing a block.
  private static final long PER_BLOCK_MEM_USAGE_BYTES = 150;

  // Hdfs table specific metrics
  public static final String CATALOG_UPDATE_DURATION_METRIC = "catalog-update-duration";
  public static final String NUM_PARTITIONS_METRIC = "num-partitions";
  public static final String NUM_FILES_METRIC = "num-files";
  public static final String NUM_BLOCKS_METRIC = "num-blocks";
  public static final String TOTAL_FILE_BYTES_METRIC = "total-file-size-bytes";
  public static final String MEMORY_ESTIMATE_METRIC = "memory-estimate-bytes";
  public static final String HAS_INCREMENTAL_STATS_METRIC = "has-incremental-stats";
  // metrics used to find out the cache hit rate when file-metadata is requested
  // for a given ValidWriteIdList
  public static final String FILEMETADATA_CACHE_MISS_METRIC = "filemetadata-cache-miss";
  public static final String FILEMETADATA_CACHE_HIT_METRIC = "filemetadata-cache-hit";
  // metric used to monitor the number of times method loadFileMetadata is called
  public static final String NUM_LOAD_FILEMETADATA_METRIC = "num-load-filemetadata";

  // Load all partitions time, including fetching all partitions
  // from HMS and loading all partitions. The code path is
  // MetaStoreUtil.fetchAllPartitions() and HdfsTable.loadAllPartitions()
  public static final String LOAD_DURATION_ALL_PARTITIONS =
      "load-duration.all-partitions";

  // The file metadata loading for all all partitions. This is part of
  // LOAD_DURATION_ALL_PARTITIONS
  // Code path: loadFileMetadataForPartitions() inside loadAllPartitions()
  public static final String LOAD_DURATION_FILE_METADATA_ALL_PARTITIONS =
      "load-duration.all-partitions.file-metadata";

  private static final THdfsPartition FAKE_THDFS_PARTITION = new THdfsPartition();

  // string to indicate NULL. set in load() from table properties
  private String nullColumnValue_;

  // hive uses this string for NULL partition keys. Set in load().
  private String nullPartitionKeyValue_;

  // Avro schema of this table if this is an Avro table, otherwise null. Set in load().
  private String avroSchema_ = null;

  // Set to true if any of the partitions have Avro data.
  private boolean hasAvroData_ = false;

  // True if this table's metadata is marked as cached. Does not necessarily mean the
  // data is cached or that all/any partitions are cached.
  private boolean isMarkedCached_ = false;

  // Array of sorted maps storing the association between partition values and
  // partition ids. There is one sorted map per partition key. It is only populated if
  // this table object is stored in ImpaladCatalog.
  // Declared as protected to allow third party extension visibility.
  protected final List<TreeMap<LiteralExpr, Set<Long>>> partitionValuesMap_ =
      new ArrayList<>();

  // Array of partition id sets that correspond to partitions with null values
  // in the partition keys; one set per partition key. It is not populated if the table is
  // stored in the catalog server.
  // Declared as protected to allow third party extension visibility.
  protected final List<Set<Long>> nullPartitionIds_ = new ArrayList<>();

  // Map of partition ids to HdfsPartitions.
  // Declared as protected to allow third party extension visibility.
  protected final Map<Long, HdfsPartition> partitionMap_ = new HashMap<>();

  // The data and delete files of the table, for Iceberg tables only.
  private GroupedContentFiles icebergFiles_;
  // Whether the data and delete files of The Iceberg tables can be outside the location.
  private boolean canDataBeOutsideOfTableLocation_;

  // Map of partition name to HdfsPartition object. Used for speeding up
  // table metadata loading. It is only populated if this table object is stored in
  // catalog server.
  // Declared as protected to allow third party extension visibility.
  protected final Map<String, HdfsPartition> nameToPartitionMap_ = new HashMap<>();

  // The partition used as a prototype when creating new partitions during
  // insertion. New partitions inherit file format and other settings from
  // the prototype.
  @VisibleForTesting
  HdfsPartition prototypePartition_;

  // Empirical estimate (in bytes) of the incremental stats size per column per
  // partition.
  public static final long STATS_SIZE_PER_COLUMN_BYTES = 200;

  // Bi-directional map between an integer index and a unique datanode
  // TNetworkAddresses, each of which contains blocks of 1 or more
  // files in this table. The network addresses are stored using IP
  // address as the host name. Each FileBlock specifies a list of
  // indices within this hostIndex_ to specify which nodes contain
  // replicas of the block.
  private final ListMap<TNetworkAddress> hostIndex_ = new ListMap<TNetworkAddress>();

  // True iff this table has incremental stats in any of its partitions.
  private boolean hasIncrementalStats_ = false;

  // Declared as protected to allow third party extension visibility.
  protected HdfsPartitionLocationCompressor partitionLocationCompressor_;

  // Base Hdfs directory where files of this table are stored.
  // For unpartitioned tables it is simply the path where all files live.
  // For partitioned tables it is the root directory
  // under which partition dirs are placed.
  protected String hdfsBaseDir_;

  // List of FieldSchemas that correspond to the non-partition columns. Used when
  // describing this table and its partitions to the HMS (e.g. as part of an alter table
  // operation), when only non-partition columns are required.
  private final List<FieldSchema> nonPartFieldSchemas_ = new ArrayList<>();

  // Flag to check if the table schema has been loaded. Used as a precondition
  // for setAvroSchema().
  private boolean isSchemaLoaded_ = false;

  // SQL constraints information for the table. Set in load() method.
  private SqlConstraints sqlConstraints_ = new SqlConstraints(new ArrayList<>(),
      new ArrayList<>());

  // Valid write id list for this table.
  // null in the case that this table is not transactional.
  protected MutableValidWriteIdList validWriteIds_ = null;

  // The last committed compaction id in the table level. It will be sent as a filter to
  // retrieve only the latest compaction that is not seen by this instance. This value is
  // updated whenever a partition is added to the table so that it is guaranteed to be
  // up-to-date.
  // -1 means there is no previous compaction event or compaction is not supported.
  private long lastCompactionId_ = -1;

  // Partitions are marked as "dirty" indicating there are in-progress modifications on
  // their metadata. The corresponding partition builder contains the new version of the
  // metadata so represents the in-progress modifications. The modifications will be
  // finalized in the coming incremental metadata refresh (see updatePartitionsFromHms()
  // for more details). This map is only maintained in the catalogd.
  private final Map<Long, HdfsPartition.Builder> dirtyPartitions_ = new HashMap<>();

  // The max id of all partitions of this table sent to coordinators. Partitions with ids
  // larger than this are not known in coordinators.
  private long maxSentPartitionId_ = HdfsPartition.INITIAL_PARTITION_ID - 1;

  // Dropped partitions since last catalog update. These partitions need to be removed
  // in coordinator's cache if there are no updates on them.
  private final Set<HdfsPartition> droppedPartitions_ = new HashSet<>();

  // pendingVersionNumber indicates a version number allocated to this HdfsTable for a
  // ongoing DDL operation. This is mainly used by the topic update thread to skip a
  // table from the topic updates if it cannot acquire lock on this table. The topic
  // update thread bumps up this to a higher value outside the topic window so that
  // the table is considered in the next update. The setCatalogVersion() makes use of this
  // to eventually assign the catalogVersion to the table.
  private long pendingVersionNumber_ = -1;
  // lock protecting access to pendingVersionNumber
  private final Object pendingVersionLock_ = new Object();

  // this field is used to keep track of the last table version which is seen by the
  // topic update thread. It is primarily used to identify distinct locking operations
  // to determine if we can skip the table from the topic update.
  private long lastVersionSeenByTopicUpdate_ = -1;

  public void setIcebergFiles(GroupedContentFiles icebergFiles) {
    icebergFiles_ = icebergFiles;
  }

  public void setCanDataBeOutsideOfTableLocation(
      boolean canDataBeOutsideOfTableLocation) {
    canDataBeOutsideOfTableLocation_ = canDataBeOutsideOfTableLocation;
  }

  // Represents a set of storage-related statistics aggregated at the table or partition
  // level.
  public final static class FileMetadataStats {
    // Number of files in a table/partition.
    public long numFiles;
    // Number of blocks in a table/partition.
    public long numBlocks;
    // Total size (in bytes) of all files in a table/partition.
    public long totalFileBytes;

    // Unsets the storage stats to indicate that their values are unknown.
    public void unset() {
      numFiles = -1;
      numBlocks = -1;
      totalFileBytes = -1;
    }

    // Initializes the values of the storage stats.
    public void init() {
      numFiles = 0;
      numBlocks = 0;
      totalFileBytes = 0;
    }

    public void set(FileMetadataStats stats) {
      numFiles = stats.numFiles;
      numBlocks = stats.numBlocks;
      totalFileBytes = stats.totalFileBytes;
    }
  }

  // Table level storage-related statistics. Depending on whether the table is stored in
  // the catalog server or the impalad catalog cache, these statistics serve different
  // purposes and, hence, are managed differently.
  // Table stored in impalad catalog cache:
  //   - Used in planning.
  //   - Stats are modified real-time by the operations that modify table metadata
  //   (e.g. add partition).
  // Table stored in the the catalog server:
  //   - Used for reporting through catalog web UI.
  //   - Stats are reset whenever the table is loaded (due to a metadata operation) and
  //   are set when the table is serialized to Thrift.
  private final FileMetadataStats fileMetadataStats_ = new FileMetadataStats();

  private final static Logger LOG = LoggerFactory.getLogger(HdfsTable.class);

  public final static long LOADING_WARNING_TIME_NS = 5000000000L;

  public HdfsTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(msTbl, db, name, owner);
    partitionLocationCompressor_ =
        new HdfsPartitionLocationCompressor(numClusteringCols_);
    icebergFiles_ = new GroupedContentFiles();
    canDataBeOutsideOfTableLocation_ = false;
  }

  @Override // FeFsTable
  public boolean isLocationCacheable() {
    return FileSystemUtil.isPathCacheable(new Path(getLocation()));
  }

  @Override // FeFsTable
  public boolean isCacheable() {
    if (!isLocationCacheable()) return false;
    if (!isMarkedCached() && numClusteringCols_ > 0) {
      for (FeFsPartition partition: partitionMap_.values()) {
        if (!partition.isCacheable()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Updates the storage stats of this table based on the partition information.
   * This is used only for the frontend tests that do not spawn a separate Catalog
   * instance.
   */
  public void computeHdfsStatsForTesting() {
    Preconditions.checkState(fileMetadataStats_.numFiles == -1
        && fileMetadataStats_.totalFileBytes == -1);
    fileMetadataStats_.init();
    for (HdfsPartition partition: partitionMap_.values()) {
      fileMetadataStats_.numFiles += partition.getNumFileDescriptors();
      fileMetadataStats_.totalFileBytes += partition.getSize();
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override // FeFsTable
  public boolean isMarkedCached() { return isMarkedCached_; }

  @Override // FeFsTable
  public Collection<? extends PrunablePartition> getPartitions() {
    return partitionMap_.values();
  }

  @Override // FeFsTable
  public Map<Long, ? extends PrunablePartition> getPartitionMap() {
    return partitionMap_;
  }

  @Override // FeFsTable
  public List<FeFsPartition> loadPartitions(Collection<Long> ids) {
    List<FeFsPartition> partitions = Lists.newArrayListWithCapacity(ids.size());
    for (Long id : ids) {
      HdfsPartition partition = partitionMap_.get(id);
      if (partition == null) {
        throw new IllegalArgumentException("no such partition id " + id);
      }
      partitions.add(partition);
    }
    return partitions;
  }

  @Override // FeFsTable
  public Set<Long> getNullPartitionIds(int i) { return nullPartitionIds_.get(i); }

  public HdfsPartitionLocationCompressor getPartitionLocationCompressor() {
    return partitionLocationCompressor_;
  }

  // Returns an unmodifiable set of the partition IDs from partitionMap_.
  @Override // FeFsTable
  public Set<Long> getPartitionIds() {
    return Collections.unmodifiableSet(partitionMap_.keySet());
  }

  @Override // FeFsTable
  public TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int i) {
    return partitionValuesMap_.get(i);
  }

  @Override // FeFsTable
  public String getNullPartitionKeyValue() {
    return nullPartitionKeyValue_; // Set during load.
  }

  @Override // FeFsTable
  public String getLocation() {
    return super.getMetaStoreTable().getSd().getLocation();
  }

  List<FieldSchema> getNonPartitionFieldSchemas() { return nonPartFieldSchemas_; }

  // True if Impala has HDFS write permissions on the hdfsBaseDir
  @Override
  public boolean hasWriteAccessToBaseDir() {
    return TAccessLevelUtil.impliesWriteAccess(accessLevel_);
  }

  /**
   * Returns the first location (HDFS path) that Impala does not have WRITE access
   * to, or an null if none is found. For an unpartitioned table, this just
   * checks the hdfsBaseDir. For a partitioned table it checks the base directory
   * as well as all partition directories.
   */
  @Override
  public String getFirstLocationWithoutWriteAccess() {
    if (!hasWriteAccessToBaseDir()) {
      return hdfsBaseDir_;
    }
    for (HdfsPartition partition: partitionMap_.values()) {
      if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
        return partition.getLocation();
      }
    }
    return null;
  }

  /**
   * Marks a partition dirty by registering the partition builder for its new instance.
   */
  public void markDirtyPartition(HdfsPartition.Builder partBuilder) {
    dirtyPartitions_.put(partBuilder.getOldId(), partBuilder);
  }

  /**
   * Marks partitions dirty by registering the partition builder for its new instance.
   */
  public void markDirtyPartitions(Collection<HdfsPartition.Builder> partBuilders) {
    for (HdfsPartition.Builder b : partBuilders) {
      markDirtyPartition(b);
    }
  }

  /**
   * Pick up the partition builder to continue the in-progress modifications.
   * The builder is then unregistered so the callers should guarantee that the in-progress
   * modifications are finalized (by calling Builder.build() and use the new instance to
   * replace the old one).
   * @return the builder for given partition's new instance.
   */
  public HdfsPartition.Builder pickInprogressPartitionBuilder(HdfsPartition partition) {
    return dirtyPartitions_.remove(partition.getId());
  }

  /**
   * @return true if any partitions are dirty.
   */
  @Override
  public boolean hasInProgressModification() { return !dirtyPartitions_.isEmpty(); }

  /**
   * Clears all the in-progress modifications by clearing all the partition builders.
   */
  @Override
  public void resetInProgressModification() { dirtyPartitions_.clear(); }

  /**
   * Gets the PrunablePartition matching the given partition spec. Returns null if no
   * match was found.
   */
  public static PrunablePartition getPartition(FeFsTable table,
      List<PartitionKeyValue> partitionSpec) {
    List<TPartitionKeyValue> partitionKeyValues = new ArrayList<>();
    for (PartitionKeyValue kv: partitionSpec) {
      Preconditions.checkArgument(kv.isStatic(), "unexpected dynamic partition: %s",
          kv);
      String value = PartitionKeyValue.getPartitionKeyValueString(
          kv.getLiteralValue(), table.getNullPartitionKeyValue());
      partitionKeyValues.add(new TPartitionKeyValue(kv.getColName(), value));
    }
    return Utils.getPartitionFromThriftPartitionSpec(table, partitionKeyValues);
  }

  /**
   * Get the partition by the given list of partValues. Returns null if none of the
   * partitions match the given list of partValues.
   */
  public HdfsPartition getPartition(List<LiteralExpr> partValues) {
    Preconditions.checkNotNull(partValues);
    for (HdfsPartition partition: partitionMap_.values()) {
      if (partValues.equals(partition.getPartitionValues())) return partition;
    }
    return null;
  }

  /**
   * Gets the HdfsPartition matching the Thrift version of the partition spec.
   * Returns null if no match was found.
   */
  public HdfsPartition getPartitionFromThriftPartitionSpec(
      List<TPartitionKeyValue> partitionSpec) {
    return (HdfsPartition)Utils.getPartitionFromThriftPartitionSpec(this, partitionSpec);
  }

  /**
   * Gets hdfs partitions by the given partition set.
   */
  @SuppressWarnings("unchecked")
  public List<HdfsPartition> getPartitionsFromPartitionSet(
      List<List<TPartitionKeyValue>> partitionSet) {
    return (List<HdfsPartition>)Utils.getPartitionsFromPartitionSet(this, partitionSet);
  }

  /**
   * Create columns corresponding to fieldSchemas. Throws a TableLoadingException if the
   * metadata is incompatible with what we support.
   */
  private void addColumnsFromFieldSchemas(List<FieldSchema> fieldSchemas)
      throws TableLoadingException {
    int pos = colsByPos_.size();
    for (FieldSchema s: fieldSchemas) {
      Type type = parseColumnType(s);
      // Check if we support partitioning on columns of such a type.
      if (pos < numClusteringCols_ && !type.supportsTablePartitioning()) {
        throw new TableLoadingException(
            String.format("Failed to load metadata for table '%s' because of " +
                "unsupported partition-column type '%s' in partition column '%s'",
                getFullName(), type.toString(), s.getName()));
      }

      Column col = new Column(s.getName(), type, s.getComment(), pos);
      addColumn(col);
      ++pos;
    }
  }

  /**
   * Adds the synthetic "row__id" column to the table schema. Under "row__id" it adds
   * the ACID hidden columns.
   * Note that this is the exact opposite of the file schema. In an ACID file, the
   * hidden columns are top-level while the user columns are embedded inside a struct
   * typed column called "row". We cheat here because this way we don't need to change
   * column resolution and everything will work seemlessly. We'll only need to generate
   * a different schema path for the columns but that's fairly simple.
   * The hidden columns can be retrieved via 'SELECT row__id.* FROM <table>' which is
   * similar to Hive's 'SELECT row__id FROM <table>'.
   */
  private void addColumnsForFullAcidTable(List<FieldSchema> fieldSchemas)
      throws TableLoadingException {
    addColumn(AcidUtils.getRowIdColumnType(colsByPos_.size()));
    addColumnsFromFieldSchemas(fieldSchemas);
  }

  private void addVirtualColumns() {
    addVirtualColumn(VirtualColumn.INPUT_FILE_NAME);
    addVirtualColumn(VirtualColumn.FILE_POSITION);
  }

  /**
   * Clear the partitions of an HdfsTable and the associated metadata.
   * Declared as protected to allow third party extension visibility.
   */
  protected void resetPartitions() {
    partitionMap_.clear();
    nameToPartitionMap_.clear();
    partitionValuesMap_.clear();
    nullPartitionIds_.clear();
    if (isStoredInImpaladCatalogCache()) {
      // Initialize partitionValuesMap_ and nullPartitionIds_. Also reset column stats.
      for (int i = 0; i < numClusteringCols_; ++i) {
        getColumns().get(i).getStats().setNumNulls(0);
        getColumns().get(i).getStats().setNumDistinctValues(0);
        partitionValuesMap_.add(new TreeMap<>());
        nullPartitionIds_.add(new HashSet<>());
      }
    }
    fileMetadataStats_.init();
  }

  /**
   * Resets any partition metadata, creates the prototype partition and sets the base
   * table directory path as well as the caching info from the HMS table.
   */
  public void initializePartitionMetadata(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws CatalogException {
    Preconditions.checkNotNull(msTbl);
    resetPartitions();
    hdfsBaseDir_ = msTbl.getSd().getLocation();
    setPrototypePartition(msTbl.getSd());

    // We silently ignore cache directives that no longer exist in HDFS, and remove
    // non-existing cache directives from the parameters.
    isMarkedCached_ = HdfsCachingUtil.validateCacheParams(msTbl.getParameters());
  }

  /**
   * Create HdfsPartition objects corresponding to 'msPartitions' and add them to this
   * table's partition list. Any partition metadata will be reset and loaded from
   * scratch. For each partition created, we load the block metadata for each data file
   * under it. Returns time spent loading the filesystem metadata in nanoseconds.
   *
   * If there are no partitions in the Hive metadata, a single partition is added with no
   * partition keys.
   */
  private long loadAllPartitions(IMetaStoreClient client, List<Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl, EventSequence catalogTimeline)
      throws IOException, CatalogException {
    Preconditions.checkNotNull(msTbl);
    final Clock clock = Clock.defaultClock();
    long startTime = clock.getTick();
    initializePartitionMetadata(msTbl);
    FsPermissionCache permCache = preloadPermissionsCache(msPartitions, catalogTimeline);

    Path tblLocation = FileSystemUtil.createFullyQualifiedPath(getHdfsBaseDirPath());
    accessLevel_ = getAvailableAccessLevel(getFullName(), tblLocation, permCache);
    catalogTimeline.markEvent("Got access level");

    List<HdfsPartition.Builder> partBuilders = new ArrayList<>();
    if (msTbl.getPartitionKeysSize() == 0) {
      // Legacy -> Iceberg migrated tables might have HMS partitions (HIVE-25894).
      if (!IcebergTable.isIcebergTable(msTbl)) {
        Preconditions.checkArgument(msPartitions == null || msPartitions.isEmpty());
      }
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      HdfsPartition.Builder partBuilder = createPartitionBuilder(msTbl.getSd(),
          /*msPartition=*/null, permCache);
      partBuilder.setIsMarkedCached(isMarkedCached_);
      setUnpartitionedTableStats(partBuilder);
      partBuilders.add(partBuilder);
    } else {
      for (Partition msPartition: msPartitions) {
        partBuilders.add(createPartitionBuilder(
            msPartition.getSd(), msPartition, permCache));
      }
      // Most of the time spent in getAvailableAccessLevel() for each partition.
      catalogTimeline.markEvent("Created partition builders");
    }
    // Load the file metadata from scratch.
    Timer.Context fileMetadataLdContext = getMetrics().getTimer(
        HdfsTable.LOAD_DURATION_FILE_METADATA_ALL_PARTITIONS).time();
    loadFileMetadataForPartitions(client, partBuilders, /*isRefresh=*/false,
        catalogTimeline);
    fileMetadataLdContext.stop();
    for (HdfsPartition.Builder p : partBuilders) addPartition(p.build());
    return clock.getTick() - startTime;
  }

  /**
   * Loads valid txn list from HMS. Re-throws exceptions as CatalogException.
   */
  private ValidTxnList loadValidTxns(IMetaStoreClient client) throws CatalogException {
    try {
      return MetastoreShim.getValidTxns(client);
    } catch (TException exception) {
      throw new CatalogException(exception.getMessage());
    }
  }

  /**
   * Similar to
   * {@link #loadFileMetadataForPartitions(IMetaStoreClient, Collection, boolean, String,
   * EventSequence)} but without any injecting the debug actions.
   */
  public long loadFileMetadataForPartitions(IMetaStoreClient client,
      Collection<HdfsPartition.Builder> partBuilders, boolean isRefresh,
      EventSequence catalogTimeline) throws CatalogException {
    return loadFileMetadataForPartitions(client, partBuilders, isRefresh, null,
        catalogTimeline);
  }

  /**
   * Helper method to load the block locations for each partition in 'parts'.
   * New file descriptor lists are loaded and the partitions are updated in place.
   *
   * @param isRefresh whether this is a refresh operation or an initial load. This only
   * affects logging.
   * @return time in nanoseconds spent in loading file metadata.
   */
  private long loadFileMetadataForPartitions(IMetaStoreClient client,
      Collection<HdfsPartition.Builder> partBuilders, boolean isRefresh,
      String debugActions, EventSequence catalogTimeline) throws CatalogException {
    getMetrics().getCounter(NUM_LOAD_FILEMETADATA_METRIC).inc();
    final Clock clock = Clock.defaultClock();
    long startTime = clock.getTick();
    catalogTimeline.markEvent(String.format("Start %s file metadata",
        isRefresh ? "refreshing" : "loading"));

    if (DebugUtils.hasDebugAction(debugActions,
        DebugUtils.LOAD_FILE_METADATA_THROW_EXCEPTION)) {
      throw new CatalogException("Threw a catalog exception due to the debug action " +
          "during loading file metadata.");
    }

    //TODO: maybe it'd be better to load the valid txn list in the context of a
    // transaction to have consistent valid write ids and valid transaction ids.
    // Currently tables are loaded when they are first referenced and stay in catalog
    // until certain actions occur (refresh, invalidate, insert, etc.). However,
    // Impala doesn't notice when HMS's cleaner removes old transactional directories,
    // which might lead to FileNotFound exceptions.
    ValidTxnList validTxnList = validWriteIds_ != null ? loadValidTxns(client) : null;
    String logPrefix = String.format(
        "%s file and block metadata for %s paths for table %s",
        isRefresh ? "Refreshing" : "Loading", partBuilders.size(),
        getFullName());

    // Actually load the partitions.
    // TODO(IMPALA-8406): if this fails to load files from one or more partitions, then
    // we'll throw an exception here and end up bailing out of whatever catalog operation
    // we're in the middle of. This could cause a partial metadata update -- eg we may
    // have refreshed the top-level table properties without refreshing the files.
    new ParallelFileMetadataLoader(getFileSystem(), partBuilders, validWriteIds_,
        validTxnList, Utils.shouldRecursivelyListPartitions(this),
        getHostIndex(), debugActions, logPrefix, icebergFiles_,
        canDataBeOutsideOfTableLocation_).load();

    // TODO(todd): would be good to log a summary of the loading process:
    // - how many block locations did we reuse/load individually/load via batch
    // - how many partitions did we read metadata for
    // - etc...
    String partNames = Joiner.on(", ").join(
        Iterables.limit(
            Iterables.transform(partBuilders, HdfsPartition.Builder::getPartitionName),
            3));
    if (partBuilders.size() > 3) {
      partNames += String.format(", and %s others",
          Iterables.size(partBuilders) - 3);
    }

    catalogTimeline.markEvent(String.format("Loaded file metadata for %d partitions",
        partBuilders.size()));
    long duration = clock.getTick() - startTime;
    LOG.info("Loaded file and block metadata for {} partitions: {}. Time taken: {}",
        getFullName(), partNames, PrintUtils.printTimeNs(duration));
    return duration;
  }

  /**
   * Gets the AccessLevel that is available for Impala for this table based on the
   * permissions Impala has on the given path. If the path does not exist, recurses up
   * the path until a existing parent directory is found, and inherit access permissions
   * from that.
   * Always returns READ_WRITE for S3, ADLS, GCS, COS files, and Ranger-enabled HDFS.
   */
  private static TAccessLevel getAvailableAccessLevel(String tableName,
      Path location, FsPermissionCache permCache) throws IOException {
    Preconditions.checkNotNull(location);
    FileSystem fs = location.getFileSystem(CONF);

    if (assumeReadWriteAccess(fs)) return TAccessLevel.READ_WRITE;

    while (location != null) {
      try {
        FsPermissionChecker.Permissions perms = permCache.getPermissions(location);
        if (perms.canReadAndWrite()) {
          return TAccessLevel.READ_WRITE;
        } else if (perms.canRead()) {
          return TAccessLevel.READ_ONLY;
        } else if (perms.canWrite()) {
          return TAccessLevel.WRITE_ONLY;
        }
        return TAccessLevel.NONE;
      } catch (FileNotFoundException e) {
        location = location.getParent();
      }
    }
    // Should never get here.
    throw new NullPointerException("Error determining access level for table " +
        tableName + ": no path ancestor exists for path: " + location);
  }

  /**
   * @return true if we assume read-write access for this filesystem for the purpose of
   *              {@link #getAvailableAccessLevel(String, Path, FsPermissionCache)}
   */
  private static boolean assumeReadWriteAccess(FileSystem fs) {
    // Avoid loading permissions in local catalog mode since they are not used in
    // LocalFsTable. Remove this once we resolve IMPALA-7539.
    if (BackendConfig.INSTANCE.isMinimalTopicMode()) return true;

    // Authorization on Ranger-enabled HDFS can be managed via the HDFS policy
    // repository, and thus it is possible to check the existence of necessary
    // permissions via RPC to the Hadoop NameNode, or Ranger REST API. Before
    // IMPALA-12994 is resolved, we assume the Impala service user has the READ_WRITE
    // permission on all HDFS files in the case when Ranger is enabled in Impala.
    // This also avoids significant overhead when the number of table partitions gets
    // bigger.
    if (FileSystemUtil.isDistributedFileSystem(fs) &&
        BackendConfig.INSTANCE.getAuthorizationProvider().equalsIgnoreCase("ranger")) {
      return true;
    }

    // Avoid calling getPermissions() on file path for S3 files, as that makes a round
    // trip to S3. Also, the S3A connector is currently unable to manage S3 permissions,
    // so for now it is safe to assume that all files(objects) have READ_WRITE
    // permissions, as that's what the S3A connector will always return too.
    // TODO: Revisit if the S3A connector is updated to be able to manage S3 object
    // permissions. (see HADOOP-13892)
    if (FileSystemUtil.isS3AFileSystem(fs)) return true;

    // The ADLS connector currently returns ACLs for files in ADLS, but can only map
    // them to the ADLS client SPI and not the Hadoop users/groups, causing unexpected
    // behavior. So ADLS ACLs are unsupported until the connector is able to map
    // permissions to hadoop users/groups (HADOOP-14437).
    if (FileSystemUtil.isADLFileSystem(fs)) return true;
    if (FileSystemUtil.isABFSFileSystem(fs)) return true;

    // GCS IAM permissions don't map to POSIX permissions. GCS connector presents fake
    // POSIX file permissions configured by the 'fs.gs.reported.permissions' property.
    // So calling getPermissions() on GCS files make no sense. Assume all GCS files have
    // READ_WRITE permissions.
    if (FileSystemUtil.isGCSFileSystem(fs)) return true;

    // COS have different authorization models:
    // - Directory permissions are reported as 777.
    // - File permissions are reported as 666.
    // - File owner is reported as the local current user.
    // - File group is also reported as the local current user.
    // So calling getPermissions() on COS files make no sense. Assume all COS files have
    // READ_WRITE permissions.
    if (FileSystemUtil.isCOSFileSystem(fs)) return true;

    // In OSS, file owner and group are persisted, but the permissions model is not
    // enforced. Authorization occurs at the level of the entire Aliyun account via Aliyun
    // Resource Access Management.
    // The append operation is not supported.
    if (FileSystemUtil.isOSSFileSystem(fs)) return true;
    return false;
  }

  /**
   * Creates new HdfsPartition objects to be added to HdfsTable's partition list.
   * Partitions may be empty, or may not even exist in the filesystem (a partition's
   * location may have been changed to a new path that is about to be created by an
   * INSERT). Also loads the file metadata for this partition. Returns new partition
   * if successful or null if none was created. If the map of Partition name to eventID
   * is not null, it uses it to set the {@code createEventId_} of the
   * HdfsPartition.
   *
   * Throws CatalogException if one of the supplied storage descriptors contains metadata
   * that Impala can't understand.
   */
  public List<HdfsPartition> createAndLoadPartitions(IMetaStoreClient client,
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      @Nullable Map<String, Long> msPartitionsToEventId, EventSequence catalogTimeline)
      throws CatalogException {
    List<HdfsPartition.Builder> addedPartBuilders = new ArrayList<>();
    FsPermissionCache permCache = preloadPermissionsCache(msPartitions, catalogTimeline);
    for (org.apache.hadoop.hive.metastore.api.Partition partition: msPartitions) {
      HdfsPartition.Builder partBuilder = createPartitionBuilder(partition.getSd(),
          partition, permCache);
      Preconditions.checkNotNull(partBuilder);
      long eventId = -1L;
      if (msPartitionsToEventId != null) {
        String partName = FeCatalogUtils.getPartitionName(this, partition.getValues());
        if (!msPartitionsToEventId.containsKey(partName)) {
          LOG.warn("Create event id for partition {} not found. Using -1.", partName);
        }
        eventId = msPartitionsToEventId.getOrDefault(partName, -1L);
      }
      partBuilder.setCreateEventId(eventId);
      addedPartBuilders.add(partBuilder);
    }
    loadFileMetadataForPartitions(client, addedPartBuilders, /*isRefresh=*/false,
        catalogTimeline);
    return addedPartBuilders.stream()
        .map(HdfsPartition.Builder::build)
        .collect(Collectors.toList());
  }

  /**
   * Creates a new HdfsPartition.Builder from a specified StorageDescriptor and an HMS
   * partition object.
   */
  private HdfsPartition.Builder createPartitionBuilder(
      StorageDescriptor storageDescriptor, Partition msPartition,
      FsPermissionCache permCache) throws CatalogException {
    return createOrUpdatePartitionBuilder(
        storageDescriptor, msPartition, permCache, null);
  }

  private HdfsPartition.Builder createOrUpdatePartitionBuilder(
      StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      FsPermissionCache permCache, HdfsPartition.Builder partBuilder)
      throws CatalogException {
    if (partBuilder == null) partBuilder = new HdfsPartition.Builder(this);
    partBuilder
        .setMsPartition(msPartition)
        .setFileFormatDescriptor(
            HdfsStorageDescriptor.fromStorageDescriptor(this.name_, storageDescriptor));
    Path partDirPath = new Path(storageDescriptor.getLocation());
    try {
      if (msPartition != null) {
        // Update the parameters based on validations with hdfs.
        boolean isCached = HdfsCachingUtil.validateCacheParams(
            partBuilder.getParameters());
        partBuilder.setIsMarkedCached(isCached);
      }
      TAccessLevel accessLevel = getAvailableAccessLevel(getFullName(), partDirPath,
          permCache);
      partBuilder.setAccessLevel(accessLevel);
      partBuilder.checkWellFormed();
      if (!TAccessLevelUtil.impliesWriteAccess(accessLevel)) {
          // TODO: READ_ONLY isn't exactly correct because the it's possible the
          // partition does not have READ permissions either. When we start checking
          // whether we can READ from a table, this should be updated to set the
          // table's access level to the "lowest" effective level across all
          // partitions. That is, if one partition has READ_ONLY and another has
          // WRITE_ONLY the table's access level should be NONE.
          accessLevel_ = TAccessLevel.READ_ONLY;
      }
      return partBuilder;
    } catch (IOException e) {
      throw new CatalogException("Error initializing partition", e);
    }
  }

  /**
   * Adds the partition to the HdfsTable. Throws a CatalogException if the partition
   * already exists in this table.
   */
  public void addPartition(HdfsPartition partition) throws CatalogException {
    if (partitionMap_.containsKey(partition.getId())) {
      throw new CatalogException(String.format("Partition %s already exists in table %s",
          partition.getPartitionName(), getFullName()));
    }
    addPartitionNoThrow(partition);
  }

  /**
   * Adds the partition to the HdfsTable. Returns false if a partition with the same
   * partition id already exists.
   */
  public boolean addPartitionNoThrow(HdfsPartition partition) {
    if (partitionMap_.containsKey(partition.getId())) return false;
    if (partition.getFileFormat() == HdfsFileFormat.AVRO) hasAvroData_ = true;
    partitionMap_.put(partition.getId(), partition);
    fileMetadataStats_.totalFileBytes += partition.getSize();
    fileMetadataStats_.numFiles += partition.getNumFileDescriptors();
    updatePartitionMdAndColStats(partition);
    lastCompactionId_ = Math.max(lastCompactionId_, partition.getLastCompactionId());
    return true;
  }

  /**
   * Updates the HdfsTable's partition metadata, i.e. adds the id to the HdfsTable and
   * populates structures used for speeding up partition pruning/lookup. Also updates
   * column stats.
   * Declared as protected to allow third party extension visibility.
   */
  protected void updatePartitionMdAndColStats(HdfsPartition partition) {
    if (partition.getPartitionValues().size() != numClusteringCols_) return;
    nameToPartitionMap_.put(partition.getPartitionName(), partition);
    if (!isStoredInImpaladCatalogCache()) return;
    for (int i = 0; i < partition.getPartitionValues().size(); ++i) {
      ColumnStats stats = getColumns().get(i).getStats();
      LiteralExpr literal = partition.getPartitionValues().get(i);
      // Store partitions with null partition values separately
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        stats.setNumNulls(stats.getNumNulls() + 1);
        if (nullPartitionIds_.get(i).isEmpty()) {
          stats.setNumDistinctValues(stats.getNumDistinctValues() + 1);
        }
        nullPartitionIds_.get(i).add(Long.valueOf(partition.getId()));
        continue;
      }

      // Update the low and high value with 'literal'.
      stats.updateLowAndHighValue(literal);

      Set<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
      if (partitionIds == null) {
        partitionIds = new HashSet<>();
        partitionValuesMap_.get(i).put(literal, partitionIds);
        stats.setNumDistinctValues(stats.getNumDistinctValues() + 1);
      }
      partitionIds.add(Long.valueOf(partition.getId()));
    }
  }

  public void updatePartitions(List<HdfsPartition.Builder> partBuilders)
      throws CatalogException {
    for (HdfsPartition.Builder p : partBuilders) updatePartition(p);
  }

  public void updatePartition(HdfsPartition.Builder partBuilder) throws CatalogException {
    HdfsPartition oldPartition = partBuilder.getOldInstance();
    Preconditions.checkNotNull(oldPartition,
        "Old partition instance should exist for updates");
    Preconditions.checkState(partitionMap_.containsKey(oldPartition.getId()),
        "Updating a non existing partition instance");
    Preconditions.checkState(partitionMap_.get(partBuilder.getOldId()) == oldPartition,
        "Concurrent modification on partitions: old instance changed");
    boolean partitionNotChanged = partBuilder.equalsToOriginal(oldPartition);
    LOG.trace("Partition {} {}", oldPartition.getName(),
        partitionNotChanged ? "changed" : "unchanged");
    if (partitionNotChanged) return;
    HdfsPartition newPartition = partBuilder.build();
    // Partition is reloaded and hence cache directives are not dropped.
    dropPartition(oldPartition, false);
    addPartition(newPartition);
  }

  /**
   * Drops the partition having the given partition spec from HdfsTable. Cleans up its
   * metadata from all the mappings used to speed up partition pruning/lookup.
   * Also updates partition column statistics. Given partitionSpec must match exactly
   * one partition.
   * Returns the HdfsPartition that was dropped. If the partition does not exist, returns
   * null.
   */
  public HdfsPartition dropPartition(List<TPartitionKeyValue> partitionSpec) {
    return dropPartition(getPartitionFromThriftPartitionSpec(partitionSpec));
  }

  /**
   * The same as the above method but specifies the partition using the partition id.
   */
  public HdfsPartition dropPartition(long partitionId) {
    return dropPartition(partitionMap_.get(partitionId));
  }

  /**
   * Drops a partition and updates partition column statistics. Returns the
   * HdfsPartition that was dropped or null if the partition does not exist.
   * If removeCacheDirective = true, any cache directive on the partition is removed.
   */
  private HdfsPartition dropPartition(HdfsPartition partition,
      boolean removeCacheDirective) {
    if (partition == null) return null;
    fileMetadataStats_.totalFileBytes -= partition.getSize();
    fileMetadataStats_.numFiles -= partition.getNumFileDescriptors();
    Preconditions.checkArgument(partition.getPartitionValues().size() ==
        numClusteringCols_);
    Long partitionId = partition.getId();
    partitionMap_.remove(partitionId);
    nameToPartitionMap_.remove(partition.getPartitionName());
    if (removeCacheDirective && partition.isMarkedCached()) {
      try {
        // Partition's parameters map is immutable. Create a temp one for the cleanup.
        HdfsCachingUtil.removePartitionCacheDirective(Maps.newHashMap(
            partition.getParameters()));
      } catch (ImpalaException e) {
        LOG.error("Unable to remove the cache directive on table " + getFullName() +
            ", partition " + partition.getPartitionName() + ": ", e);
      }
    }
    // dirtyPartitions_ and droppedPartitionIds are only maintained in the catalogd.
    // nullPartitionIds_ and partitionValuesMap_ are only maintained in coordinators.
    if (!isStoredInImpaladCatalogCache()) {
      dirtyPartitions_.remove(partitionId);
      // Only tracks the dropped partition instances when we need partition-level updates.
      if (BackendConfig.INSTANCE.isIncrementalMetadataUpdatesEnabled()) {
        droppedPartitions_.add(partition.genMinimalPartition());
      }
      return partition;
    }
    for (int i = 0; i < partition.getPartitionValues().size(); ++i) {
      ColumnStats stats = getColumns().get(i).getStats();
      LiteralExpr literal = partition.getPartitionValues().get(i);
      // Check if this is a null literal.
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        nullPartitionIds_.get(i).remove(partitionId);
        stats.setNumNulls(stats.getNumNulls() - 1);
        if (nullPartitionIds_.get(i).isEmpty()) {
          stats.setNumDistinctValues(stats.getNumDistinctValues() - 1);
        }
        continue;
      }
      Set<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
      // If there are multiple partition ids corresponding to a literal, remove
      // only this id. Otherwise, remove the <literal, id> pair.
      if (partitionIds.size() > 1) partitionIds.remove(partitionId);
      else {
        partitionValuesMap_.get(i).remove(literal);
        stats.setNumDistinctValues(stats.getNumDistinctValues() - 1);
      }
    }
    return partition;
  }

  public HdfsPartition dropPartition(HdfsPartition partition) {
    return dropPartition(partition, true);
  }

  /**
   * Drops the given partitions from this table. Cleans up its metadata from all the
   * mappings used to speed up partition pruning/lookup. Also updates partitions column
   * statistics. Returns the list of partitions that were dropped.
   */
  public List<HdfsPartition> dropPartitions(List<HdfsPartition> partitions,
      boolean removeCacheDirective) {
    List<HdfsPartition> droppedPartitions = new ArrayList<>();
    for (HdfsPartition partition: partitions) {
      HdfsPartition hdfsPartition = dropPartition(partition, removeCacheDirective);
      if (hdfsPartition != null) droppedPartitions.add(hdfsPartition);
    }
    return droppedPartitions;
  }

  public List<HdfsPartition> dropPartitions(List<HdfsPartition> partitions) {
    return dropPartitions(partitions, true);
  }

  /**
   * Update the prototype partition used when creating new partitions for
   * this table. New partitions will inherit storage properties from the
   * provided descriptor.
   */
  public void setPrototypePartition(StorageDescriptor storageDescriptor)
      throws CatalogException {
    HdfsStorageDescriptor hdfsStorageDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name_, storageDescriptor);
    prototypePartition_ = HdfsPartition.prototypePartition(this, hdfsStorageDescriptor);
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    load(reuseMetadata, client, msTbl, /* loadPartitionFileMetadata */
        true, /* loadTableSchema*/true, false,
        /* partitionsToUpdate*/null, null, null, reason, catalogTimeline);
  }

  /**
   * Loads table metadata from the Hive Metastore.
   *
   * If 'reuseMetadata' is false, performs a full metadata load from the Hive Metastore,
   * including partition and file metadata. Otherwise, loads metadata incrementally and
   * updates this HdfsTable in place so that it is in sync with the Hive Metastore.
   *
   * Depending on the operation that triggered the table metadata load, not all the
   * metadata may need to be updated. If 'partitionsToUpdate' is not null, it specifies a
   * list of partitions for which metadata should be updated. Otherwise, all partition
   * metadata will be updated from the Hive Metastore.
   *
   * If 'loadParitionFileMetadata' is true, file metadata of the specified partitions
   * are reloaded from scratch. If 'partitionsToUpdate' is not specified, file metadata
   * of all the partitions are loaded.
   *
   * If 'loadTableSchema' is true, the table schema is loaded from the Hive Metastore.
   *
   * Existing file descriptors might be reused incorrectly if Hdfs rebalancer was
   * executed, as it changes the block locations but doesn't update the mtime (file
   * modification time).
   * If this occurs, user has to execute "invalidate metadata" to invalidate the
   * metadata cache of the table and trigger a fresh load.
   */
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, boolean loadPartitionFileMetadata,
      boolean loadTableSchema, boolean refreshUpdatedPartitions,
      @Nullable Set<String> partitionsToUpdate, @Nullable String debugAction,
      @Nullable Map<String, Long> partitionToEventId, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    String annotation = String.format("%s metadata for %s%s partition(s) of %s.%s (%s)",
        reuseMetadata ? "Reloading" : "Loading",
        loadTableSchema ? "table definition and " : "",
        partitionsToUpdate == null ? "all" : String.valueOf(partitionsToUpdate.size()),
        msTbl.getDbName(), msTbl.getTableName(), reason);
    LOG.info(annotation);
    final Timer storageLdTimer =
        getMetrics().getTimer(Table.LOAD_DURATION_STORAGE_METADATA);
    storageMetadataLoadTime_ = 0;
    Table.LOADING_TABLES.incrementAndGet();
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation)) {
      // turn all exceptions into TableLoadingException
      msTable_ = msTbl;
      try {
        if (loadTableSchema) {
            // set nullPartitionKeyValue from the hive conf.
          nullPartitionKeyValue_ =
            MetaStoreUtil.getNullPartitionKeyValue(client).intern();
          loadSchema(msTbl);
          loadAllColumnStats(client, catalogTimeline);
          loadConstraintsInfo(client, msTbl);
          catalogTimeline.markEvent("Loaded table schema");
        }
        loadValidWriteIdList(client);
        // Set table-level stats first so partition stats can inherit it.
        setTableStats(msTbl);
        // Load partition and file metadata
        if (reuseMetadata) {
          // Incrementally update this table's partitions and file metadata
          Preconditions.checkState(
              partitionsToUpdate == null || loadPartitionFileMetadata);
          storageMetadataLoadTime_ += updateMdFromHmsTable(msTbl);
          if (msTbl.getPartitionKeysSize() == 0) {
            if (loadPartitionFileMetadata) {
              storageMetadataLoadTime_ += updateUnpartitionedTableFileMd(client,
                  debugAction, catalogTimeline);
            } else {  // Update the single partition stats in case table stats changes.
              updateUnpartitionedTableStats();
            }
          } else {
            storageMetadataLoadTime_ += updatePartitionsFromHms(
                client, partitionsToUpdate, loadPartitionFileMetadata,
                refreshUpdatedPartitions, partitionToEventId, debugAction,
                catalogTimeline);
          }
          LOG.info("Incrementally loaded table metadata for: " + getFullName());
        } else {
          LOG.info("Fetching partition metadata from the Metastore: " + getFullName());
          final Timer.Context allPartitionsLdContext =
              getMetrics().getTimer(HdfsTable.LOAD_DURATION_ALL_PARTITIONS).time();
          // Load all partitions from Hive Metastore, including file metadata.
          List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions =
              MetaStoreUtil.fetchAllPartitions(
                  client, msTbl, NUM_PARTITION_FETCH_RETRIES);
          LOG.info("Fetched partition metadata from the Metastore: " + getFullName());
          storageMetadataLoadTime_ = loadAllPartitions(client, msPartitions, msTbl,
              catalogTimeline);
          allPartitionsLdContext.stop();
        }
        if (loadTableSchema) setAvroSchema(client, msTbl, catalogTimeline);
        fileMetadataStats_.unset();
        refreshLastUsedTime();
        // Make sure all the partition modifications are done.
        Preconditions.checkState(dirtyPartitions_.isEmpty());
      } catch (TableLoadingException e) {
        throw e;
      } catch (Exception e) {
        throw new TableLoadingException("Failed to load metadata for table: "
            + getFullName(), e);
      }
    } finally {
      storageLdTimer.update(storageMetadataLoadTime_, TimeUnit.NANOSECONDS);
      long load_time_duration = context.stop();
      if (load_time_duration > LOADING_WARNING_TIME_NS) {
        LOG.warn("Time taken on loading table " + getFullName() + " exceeded " +
            "warning threshold. Time: " + PrintUtils.printTimeNs(load_time_duration));
      }
      updateTableLoadingTime();
      Table.LOADING_TABLES.decrementAndGet();
    }
  }

  /**
   * Load Primary Key and Foreign Key information for table. Throws TableLoadingException
   * if the load fails. Declared as protected to allow third party extensions on this
   * class.
   */
  protected void loadConstraintsInfo(IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException{
    try {
      sqlConstraints_ = new SqlConstraints(client.getPrimaryKeys(
          new PrimaryKeysRequest(msTbl.getDbName(), msTbl.getTableName())),
          client.getForeignKeys(new ForeignKeysRequest(null, null,
          msTbl.getDbName(), msTbl.getTableName())));
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load primary keys/foreign keys for "
          + "table: " + getFullName(), e);
    }
  }

  /**
   * Updates the table metadata, including 'hdfsBaseDir_', 'isMarkedCached_',
   * and 'accessLevel_' from 'msTbl'. Returns time spent accessing file system
   * in nanoseconds. Throws an IOException if there was an error accessing
   * the table location path.
   * Declared as protected to allow third party extension visibility.
   */
  protected long updateMdFromHmsTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws IOException {
    Preconditions.checkNotNull(msTbl);
    final Clock clock = Clock.defaultClock();
    long filesystemAccessTime = 0;
    long startTime = clock.getTick();
    hdfsBaseDir_ = msTbl.getSd().getLocation();
    isMarkedCached_ = HdfsCachingUtil.validateCacheParams(msTbl.getParameters());
    Path location = new Path(hdfsBaseDir_);
    accessLevel_ = getAvailableAccessLevel(getFullName(), location,
        new FsPermissionCache());
    filesystemAccessTime = clock.getTick() - startTime;
    setMetaStoreTable(msTbl);
    return filesystemAccessTime;
  }

  /**
   * Incrementally updates the file metadata of an unpartitioned HdfsTable.
   * Returns time spent updating the file metadata in nanoseconds.
   *
   * This is optimized for the case where few files have changed. See
   * {@link FileMetadataLoader#load} for details.
   */
  private long updateUnpartitionedTableFileMd(IMetaStoreClient client, String debugAction,
      EventSequence catalogTimeline) throws CatalogException {
    Preconditions.checkState(getNumClusteringCols() == 0);
    if (LOG.isTraceEnabled()) {
      LOG.trace("update unpartitioned table: " + getFullName());
    }
    // Step 1: fetch external metadata
    HdfsPartition oldPartition = Iterables.getOnlyElement(partitionMap_.values());
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    Preconditions.checkNotNull(msTbl);
    HdfsPartition.Builder partBuilder = createPartitionBuilder(msTbl.getSd(),
        /*msPartition=*/null, new FsPermissionCache());
    // Copy over the FDs from the old partition to the new one, so that
    // 'refreshPartitionFileMetadata' below can compare modification times and
    // reload the locations only for those that changed.
    partBuilder.setFileDescriptors(oldPartition);
    partBuilder.setIsMarkedCached(isMarkedCached_);
    // Keep track of the previous partition id, so we can send invalidation on the old
    // partition instance to local catalog coordinators.
    partBuilder.setPrevId(oldPartition.getId());
    long fileMdLoadTime = loadFileMetadataForPartitions(client,
        ImmutableList.of(partBuilder), /*isRefresh=*/true, debugAction, catalogTimeline);
    // Step 2: update internal fields
    resetPartitions();
    setPrototypePartition(msTbl.getSd());
    setUnpartitionedTableStats(partBuilder);
    addPartition(partBuilder.build());
    return fileMdLoadTime;
  }

  /**
   * Updates the single partition stats of an unpartitioned HdfsTable.
   */
  private void updateUnpartitionedTableStats() throws CatalogException {
    // Just update the single partition if its #rows is stale.
    HdfsPartition oldPartition = Iterables.getOnlyElement(partitionMap_.values());
    if (oldPartition.getNumRows() != getNumRows()) {
      HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(oldPartition)
          .setNumRows(getNumRows());
      updatePartition(partBuilder);
    }
  }

  /**
   * Updates the partitions of an HdfsTable so that they are in sync with the
   * Hive Metastore. It reloads partitions that were marked 'dirty' by doing a
   * DROP + CREATE. It removes from this table partitions that no longer exist
   * in the Hive Metastore and adds partitions that were added externally (e.g.
   * using Hive) to the Hive Metastore but do not exist in this table. If
   * 'loadParitionFileMetadata' is true, it triggers file/block metadata reload
   * for the partitions specified in 'partitionsToUpdate', if any, or for all
   * the table partitions if 'partitionsToUpdate' is null. Returns time
   * spent loading file metadata in nanoseconds.
   */
  private long updatePartitionsFromHms(IMetaStoreClient client,
      Set<String> partitionsToUpdate, boolean loadPartitionFileMetadata,
      boolean refreshUpdatedPartitions, Map<String, Long> partitionToEventId,
      String debugAction, EventSequence catalogTimeline) throws Exception {
    if (LOG.isTraceEnabled()) LOG.trace("Sync table partitions: " + getFullName());
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    Preconditions.checkNotNull(msTbl);
    Preconditions.checkState(msTbl.getPartitionKeysSize() != 0);
    Preconditions.checkState(loadPartitionFileMetadata || partitionsToUpdate == null);
    PartitionDeltaUpdater deltaUpdater;
    if (refreshUpdatedPartitions) {
      deltaUpdater = new PartBasedDeltaUpdater(client,
          loadPartitionFileMetadata, partitionsToUpdate, partitionToEventId,
          debugAction, catalogTimeline);
    } else {
      deltaUpdater = new PartNameBasedDeltaUpdater(client, loadPartitionFileMetadata,
          partitionsToUpdate, partitionToEventId, debugAction, catalogTimeline);
    }
    deltaUpdater.apply();
    return deltaUpdater.loadTimeForFileMdNs_;
  }

  /**
   * Util class to compute the delta of partitions known to this table and the partitions
   * in Hive Metastore. This is used to incrementally refresh the table. It identifies
   * the partitions which are removed [added] from metastore and removes [adds] them.
   * Additionally, it also updates partitions which are provided or found to be stale.
   */
  private abstract class PartitionDeltaUpdater {
    // flag used to determine if the file-metadata needs to be reloaded for stale
    // partitions
    private final boolean loadFileMd_;
    // total time taken to load file-metadata in nano-seconds.
    private long loadTimeForFileMdNs_;
    // metastore client used to fetch partition information from metastore.
    protected final IMetaStoreClient client_;
    // Nullable set of partition names which when set is used to force load partitions.
    // if loadFileMd_ flag is set, files for these partitions will also be
    // reloaded.
    private final Set<String> partitionsToUpdate_;
    private final String debugAction_;
    protected final Map<String, Long> partitionToEventId_;
    protected final EventSequence catalogTimeline_;

    PartitionDeltaUpdater(IMetaStoreClient client, boolean loadPartitionFileMetadata,
        Set<String> partitionsToUpdate, @Nullable Map<String, Long> partitionToEventId,
        String debugAction, EventSequence catalogTimeline) {
      this.client_ = client;
      this.loadFileMd_ = loadPartitionFileMetadata;
      this.partitionsToUpdate_ = partitionsToUpdate;
      this.debugAction_ = debugAction;
      this.partitionToEventId_ = partitionToEventId;
      this.catalogTimeline_ = catalogTimeline;
    }

    /**
     * This method used to determine if the given HdfsPartition has been removed from
     * hive metastore.
     * @return true if partition does not exist in metastore, else false.
     */
    public abstract boolean isRemoved(HdfsPartition hdfsPartition);

    /**
     * Loads any partitions which are known to metastore but not provided in
     * knownPartitions. All such new partitions will be added in the given
     * {@code addedPartNames} set.
     * @param knownPartitions Known set of partition names to this Table.
     * @param addedPartNames Set of part names which is used to return the newly added
     *                       partNames
     * @return Time taken in nanoseconds for file-metadata loading for new partitions.
     */
    public abstract long loadNewPartitions(Set<String> knownPartitions,
        Set<String> addedPartNames) throws Exception;

    /**
     * Gets a {@link HdfsPartition.Builder} to construct a updated HdfsPartition for
     * the given partition.
     */
    public abstract HdfsPartition.Builder getUpdatedPartition(HdfsPartition partition)
        throws Exception;

    /**
     * Loads both the HMS and file-metadata of the partitions provided by the given
     * map of HdfsPartition.Builders.
     * @param updatedPartitionBuilders The map of partition names and the corresponding
     *                                 HdfsPartition.Builders which need to be loaded.
     * @return Time taken to load file-metadata in nanoseconds.
     */
    public abstract long loadUpdatedPartitions(
        Map<String, HdfsPartition.Builder> updatedPartitionBuilders) throws Exception;

    /**
     * This method applies the partition delta (create new, remove old, update stale)
     * when compared to the current state of partitions in the metastore.
     */
    public void apply() throws Exception {
      List<HdfsPartition> removedPartitions = new ArrayList<>();
      Map<String, HdfsPartition.Builder> updatedPartitions = new HashMap<>();
      List<HdfsPartition.Builder> partitionsToLoadFiles = new ArrayList<>();
      Set<String> partitionNames = new HashSet<>();
      for (HdfsPartition partition: partitionMap_.values()) {
        // Remove partitions that don't exist in the Hive Metastore. These are partitions
        // that were removed from HMS using some external process, e.g. Hive.
        if (isRemoved(partition)) {
          removedPartitions.add(partition);
        } else {
          HdfsPartition.Builder updatedPartBuilder = getUpdatedPartition(partition);
          if (updatedPartBuilder != null) {
            // If there are any self-updated (dirty) or externally updated partitions
            // add them to the list of updatedPartitions so that they are reloaded later.
            updatedPartitions.put(partition.getPartitionName(), updatedPartBuilder);
          } else if (loadFileMd_ && partitionsToUpdate_ == null) {
            partitionsToLoadFiles.add(new HdfsPartition.Builder(partition));
          }
        }
        Preconditions.checkNotNull(partition.getCachedMsPartitionDescriptor());
        partitionNames.add(partition.getPartitionName());
      }
      dropPartitions(removedPartitions);
      // Load dirty partitions from Hive Metastore. File metadata of dirty partitions will
      // always be reloaded (ignore the loadPartitionFileMetadata flag).
      loadTimeForFileMdNs_ = loadUpdatedPartitions(updatedPartitions);
      Preconditions.checkState(!hasInProgressModification());

      Set<String> addedPartitions = new HashSet<>();
      loadTimeForFileMdNs_ += loadNewPartitions(partitionNames, addedPartitions);
      // If a list of modified partitions (old and new) is specified, don't reload file
      // metadata for the new ones as they have already been detected in HMS and have been
      // reloaded by loadNewPartitions().
      if (partitionsToUpdate_ != null) {
        partitionsToUpdate_.removeAll(addedPartitions);
      }
      // Load file metadata. Until we have a notification mechanism for when a
      // file changes in hdfs, it is sometimes required to reload all the file
      // descriptors and block metadata of a table (e.g. REFRESH statement).
      if (loadFileMd_) {
        if (partitionsToUpdate_ != null) {
          Preconditions.checkState(partitionsToLoadFiles.isEmpty());
          // Only reload file metadata of partitions specified in 'partitionsToUpdate'
          List<HdfsPartition> parts = getPartitionsForNames(partitionsToUpdate_);
          partitionsToLoadFiles = parts.stream().map(HdfsPartition.Builder::new)
              .collect(Collectors.toList());
        }
        if (!partitionsToLoadFiles.isEmpty()) {
          loadTimeForFileMdNs_ += loadFileMetadataForPartitions(client_,
              partitionsToLoadFiles, /*isRefresh=*/true, debugAction_, catalogTimeline_);
          updatePartitions(partitionsToLoadFiles);
        }
      }
    }

    /**
     * Returns the total time taken to load file-metadata in nanoseconds. Mostly used
     * for legacy reasons to return to the coordinators the time taken load file-metadata.
     */
    public long getTotalFileMdLoadTime() {
      return loadTimeForFileMdNs_;
    }
  }

  /**
   * Util class which computes the delta of partitions for this table when compared to
   * HMS. This class fetches all the partition objects from metastore and then evaluates
   * the delta with what is known to this HdfsTable. It also detects changed partitions
   * unlike {@link PartNameBasedDeltaUpdater} which only determines change in list
   * of partition names.
   */
  private class PartBasedDeltaUpdater extends PartitionDeltaUpdater {
    private final Map<String, Partition> msPartitions_ = new HashMap<>();
    private final FsPermissionCache permCache_ = new FsPermissionCache();

    public PartBasedDeltaUpdater(
        IMetaStoreClient client, boolean loadPartitionFileMetadata,
        Set<String> partitionsToUpdate, Map<String, Long> partitionToEventId,
        String debugAction, EventSequence catalogTimeline) throws Exception {
      super(client, loadPartitionFileMetadata, partitionsToUpdate, partitionToEventId,
          debugAction, catalogTimeline);
      Stopwatch sw = Stopwatch.createStarted();
      List<Partition> partitionList;
      if (partitionsToUpdate != null) {
        partitionList = MetaStoreUtil
            .fetchPartitionsByName(client, Lists.newArrayList(partitionsToUpdate),
                msTable_);
      } else {
        partitionList =
            MetaStoreUtil.fetchAllPartitions(
                client_, msTable_, NUM_PARTITION_FETCH_RETRIES);
      }
      LOG.debug("Time taken to fetch all partitions of table {}: {} msec", getFullName(),
          sw.stop().elapsed(TimeUnit.MILLISECONDS));
      List<String> partitionColNames = getClusteringColNames();
      for (Partition part : partitionList) {
        msPartitions_
            .put(MetastoreShim.makePartName(partitionColNames, part.getValues()), part);
      }
    }

    @Override
    public boolean isRemoved(HdfsPartition hdfsPartition) {
      return !msPartitions_.containsKey(hdfsPartition.getPartitionName());
    }

    /**
     * In addition to the dirty partitions (representing partitions which are updated
     * via on-going table metadata changes in this Catalog), this also detects staleness
     * by comparing the {@link StorageDescriptor} of the given HdfsPartition with what is
     * present in the HiveMetastore. This is useful to perform "deep" refresh table so
     * that outside changes to existing partitions (eg. location update) are detected.
     */
    @Override
    public HdfsPartition.Builder getUpdatedPartition(HdfsPartition hdfsPartition)
        throws Exception {
      HdfsPartition.Builder updatedPartitionBuilder = pickInprogressPartitionBuilder(
          hdfsPartition);
      Partition msPartition = Preconditions
          .checkNotNull(msPartitions_.get(hdfsPartition.getPartitionName()));
      Preconditions.checkNotNull(msPartition.getSd());
      // we compare the StorageDescriptor from HdfsPartition object to the one
      // from HMS and if they don't match we assume that the partition has been updated
      // in HMS. This would catch the cases where partition fields, locations or
      // file-format are changed from external systems.
      if(!hdfsPartition.compareSd(msPartition.getSd())) {
        // if the updatePartitionBuilder is null, it means that this partition update
        // was not from an in-progress modification in this catalog, but rather from
        // and outside update to the partition.
        if (updatedPartitionBuilder == null) {
          updatedPartitionBuilder = new HdfsPartition.Builder(hdfsPartition);
        }
        // msPartition is different than what we have in HdfsTable
        updatedPartitionBuilder = createOrUpdatePartitionBuilder(msPartition.getSd(),
            msPartition, permCache_, updatedPartitionBuilder);
      }
      return updatedPartitionBuilder;
    }

    @Override
    public long loadNewPartitions(Set<String> knownPartitions, Set<String> addedPartNames)
        throws Exception {
      // get the names of the partitions which present in HMS but not in this table.
      List<Partition> newMsPartitions = new ArrayList<>();
      for (String partNameInMs : msPartitions_.keySet()) {
        if (!knownPartitions.contains(partNameInMs)) {
          newMsPartitions.add(msPartitions_.get(partNameInMs));
          addedPartNames.add(partNameInMs);
        }
      }
      return loadPartitionsFromMetastore(newMsPartitions,
          /*inprogressPartBuilders=*/null, partitionToEventId_, client_,
          catalogTimeline_);
    }

    @Override
    public long loadUpdatedPartitions(
        Map<String, HdfsPartition.Builder> updatedPartBuilders) throws Exception {
      List<Partition> updatedPartitions = new ArrayList<>();
      for (String partName : updatedPartBuilders.keySet()) {
        updatedPartitions.add(Preconditions
            .checkNotNull(msPartitions_.get(partName)));
      }
      // we pass partitionToEventId argument as null below because updated partitions
      // partitions were preexisting before load and just modified from outside.
      return loadPartitionsFromMetastore(updatedPartitions, updatedPartBuilders,
          null, client_, catalogTimeline_);
    }
  }


  /**
   * This DeltaChecker uses partition names to determine the delta between metastore
   * and catalog. As such this is faster than {@link PartBasedDeltaUpdater} but it cannot
   * detect partition updates other than partition names (e.g. outside partition location
   * updates will not be detected).
   */
  private class PartNameBasedDeltaUpdater extends PartitionDeltaUpdater {
    private final Set<String> partitionNamesFromHms_;

    public PartNameBasedDeltaUpdater(
        IMetaStoreClient client, boolean loadPartitionFileMetadata,
        Set<String> partitionsToUpdate, Map<String, Long> partitionToEventId,
        String debugAction, EventSequence catalogTimeline) throws Exception {
      super(client, loadPartitionFileMetadata, partitionsToUpdate, partitionToEventId,
          debugAction, catalogTimeline);
      // Retrieve all the partition names from the Hive Metastore. We need this to
      // identify the delta between partitions of the local HdfsTable and the table entry
      // in the Hive Metastore. Note: This is a relatively "cheap" operation
      // (~.3 secs for 30K partitions).
      partitionNamesFromHms_ = new HashSet<>(client_
          .listPartitionNames(db_.getName(), name_, (short) -1));
    }

    @Override
    public boolean isRemoved(HdfsPartition hdfsPartition) {
      return !partitionNamesFromHms_.contains(hdfsPartition.getPartitionName());
    }

    @Override
    public HdfsPartition.Builder getUpdatedPartition(HdfsPartition hdfsPartition) {
      return pickInprogressPartitionBuilder(hdfsPartition);
    }

    @Override
    public long loadNewPartitions(Set<String> knownPartitionNames,
        Set<String> addedPartNames) throws Exception {
      // Identify and load partitions that were added in the Hive Metastore but don't
      // exist in this table. File metadata of them will be loaded.
      addedPartNames.addAll(Sets
          .difference(partitionNamesFromHms_, knownPartitionNames));
      return loadPartitionsFromMetastore(addedPartNames,
          /*inprogressPartBuilders=*/null, partitionToEventId_, client_,
          catalogTimeline_);
    }

    @Override
    public long loadUpdatedPartitions(
        Map<String, HdfsPartition.Builder> updatedPartitionBuilders) throws Exception {
      // we pass partitionToEventId argument as null below because updated partitions
      // partitions were preexisting before load and just modified from outside.
      return loadPartitionsFromMetastore(updatedPartitionBuilders.keySet(),
          updatedPartitionBuilders, null, client_, catalogTimeline_);
    }
  }

  /**
   * Gets the names of partition columns.
   */
  public List<String> getClusteringColNames() {
    List<String> colNames = new ArrayList<>(getNumClusteringCols());
    for (Column column : getClusteringColumns()) {
      colNames.add(column.name_);
    }
    return colNames;
  }

  /**
   * Given a set of partition names, returns the corresponding HdfsPartition
   * objects.
   */
  public List<HdfsPartition> getPartitionsForNames(Collection<String> partitionNames) {
    List<HdfsPartition> parts = Lists.newArrayListWithCapacity(partitionNames.size());
    for (String partitionName: partitionNames) {
      String partName = DEFAULT_PARTITION_NAME;
      if (partitionName.length() > 0) partName = partitionName;
      HdfsPartition partition = nameToPartitionMap_.get(partName);
      Preconditions.checkNotNull(partition, "Invalid partition name: " + partName);
      parts.add(partition);
    }
    return parts;
  }

  /**
   * Tracks the in-flight INSERT event id in the partition.
   * @return false if the partition doesn't exist. Otherwise returns true.
   */
  public boolean addInflightInsertEventToPartition(String partName, long eventId) {
    HdfsPartition partition = nameToPartitionMap_.get(partName);
    if (partition == null) return false;
    partition.addToVersionsForInflightEvents(/*isInsertEvent*/true, eventId);
    return true;
  }

  private void setUnpartitionedTableStats(HdfsPartition.Builder partBuilder) {
    Preconditions.checkState(numClusteringCols_ == 0);
    // For unpartitioned tables set the numRows in its single partition
    // to the table's numRows.
    partBuilder.setNumRows(getNumRows());
  }

  /**
   * Checks if the table or any of the partitions in the table are stored as Avro.
   * If so, calls setAvroSchemaInternal to set avroSchema_.
   */
  protected void setAvroSchema(IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, EventSequence catalogTimeline)
      throws Exception {
    Preconditions.checkState(isSchemaLoaded_);
    String inputFormat = msTbl.getSd().getInputFormat();
    String serDeLib = msTbl.getSd().getSerdeInfo().getSerializationLib();
    if (HdfsFileFormat.fromJavaClassName(inputFormat, serDeLib) == HdfsFileFormat.AVRO
        || hasAvroData_) {
      setAvroSchemaInternal(client, msTable_, catalogTimeline);
    }
  }

  /**
   * Sets avroSchema_. Additionally, this method also reconciles the schema if the column
   * definitions from the metastore differ from the Avro schema.
   */
  protected void setAvroSchemaInternal(IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, EventSequence catalogTimeline)
      throws Exception {
    // Look for Avro schema in TBLPROPERTIES and in SERDEPROPERTIES, with the latter
    // taking precedence.
    List<Map<String, String>> schemaSearchLocations = new ArrayList<>();
    schemaSearchLocations.add(
        getMetaStoreTable().getSd().getSerdeInfo().getParameters());
    schemaSearchLocations.add(getMetaStoreTable().getParameters());

    avroSchema_ = AvroSchemaUtils.getAvroSchema(schemaSearchLocations);

    if (avroSchema_ == null) {
      // No Avro schema was explicitly set in the table metadata, so infer the Avro
      // schema from the column definitions.
      Schema inferredSchema = AvroSchemaConverter.convertFieldSchemas(
          msTbl.getSd().getCols(), getFullName());
      avroSchema_ = inferredSchema.toString();
      // NOTE: below we reconcile this inferred schema back into the table
      // schema in the case of Avro-formatted tables. This has the side effect
      // of promoting types like TINYINT to INT.
    }
    catalogTimeline.markEvent("Loaded avro schema");
    String serdeLib = msTbl.getSd().getSerdeInfo().getSerializationLib();
    if (serdeLib == null ||
        serdeLib.equals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")) {
      // If the SerDe library is null or set to LazySimpleSerDe or is null, it
      // indicates there is an issue with the table metadata since Avro table need a
      // non-native serde. Instead of failing to load the table, fall back to
      // using the fields from the storage descriptor (same as Hive).
      return;
    } else {
      List<FieldSchema> reconciledFieldSchemas = AvroSchemaUtils.reconcileAvroSchema(
          msTbl, avroSchema_);

      // Reset and update nonPartFieldSchemas_ to the reconciled colDefs.
      nonPartFieldSchemas_.clear();
      nonPartFieldSchemas_.addAll(reconciledFieldSchemas);
      // Update the columns as per the reconciled colDefs and re-load stats.
      clearColumns();
      addColumnsFromFieldSchemas(msTbl.getPartitionKeys());
      addColumnsFromFieldSchemas(nonPartFieldSchemas_);
      addVirtualColumns();
      loadAllColumnStats(client, catalogTimeline);
    }
  }

  /**
   * Loads table schema.
   */
  public void loadSchema(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws TableLoadingException {
    nonPartFieldSchemas_.clear();
    // set NULL indicator string from table properties
    nullColumnValue_ =
        msTbl.getParameters().get(serdeConstants.SERIALIZATION_NULL_FORMAT);
    if (nullColumnValue_ == null) nullColumnValue_ = FeFsTable.DEFAULT_NULL_COLUMN_VALUE;

    // Excludes partition columns.
    nonPartFieldSchemas_.addAll(msTbl.getSd().getCols());

    // The number of clustering columns is the number of partition keys.
    numClusteringCols_ = msTbl.getPartitionKeys().size();
    partitionLocationCompressor_.setClusteringColumns(numClusteringCols_);
    clearColumns();
    // Add all columns to the table. Ordering is important: partition columns first,
    // then all other columns.
    addColumnsFromFieldSchemas(msTbl.getPartitionKeys());
    if (AcidUtils.isFullAcidTable(msTbl.getParameters())) {
      addColumnsForFullAcidTable(nonPartFieldSchemas_);
    } else {
      addColumnsFromFieldSchemas(nonPartFieldSchemas_);
    }
    addVirtualColumns();
    isSchemaLoaded_ = true;
    LOG.info("Loaded {} columns from HMS. Actual columns: {}",
        nonPartFieldSchemas_.size() + numClusteringCols_, colsByPos_.size());
  }

  /**
   * Loads from the Hive Metastore and file system the partitions that correspond to
   * the specified 'partitionNames' and adds/updates them to the internal list of table
   * partitions.
   * If 'inprogressPartBuilders' is null, new partitions will be created.
   * If 'inprogressPartBuilders' is not null, take over the in-progress modifications
   * and finalized them by updating the existing partitions.
   * @return time in nanoseconds spent in loading file metadata.
   */
  private long loadPartitionsFromMetastore(Set<String> partitionNames,
      Map<String, HdfsPartition.Builder> inprogressPartBuilders,
      @Nullable Map<String, Long> partitionToEventId, IMetaStoreClient client,
      EventSequence catalogTimeline) throws Exception {
    Preconditions.checkNotNull(partitionNames);
    if (partitionNames.isEmpty()) return 0;
    // Load partition metadata from Hive Metastore.
    List<Partition> msPartitions = new ArrayList<>(
        MetaStoreUtil.fetchPartitionsByName(
            client, Lists.newArrayList(partitionNames), msTable_));
    return loadPartitionsFromMetastore(msPartitions, inprogressPartBuilders,
        partitionToEventId, client, catalogTimeline);
  }

  private long loadPartitionsFromMetastore(List<Partition> msPartitions,
      Map<String, HdfsPartition.Builder> inprogressPartBuilders,
      @Nullable Map<String, Long> partitionToEventId, IMetaStoreClient client,
      EventSequence catalogTimeline) throws Exception {
    FsPermissionCache permCache = preloadPermissionsCache(msPartitions, catalogTimeline);
    List<HdfsPartition.Builder> partBuilders = new ArrayList<>(msPartitions.size());
    for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
      String partName = FeCatalogUtils.getPartitionName(this, msPartition.getValues());
      HdfsPartition.Builder partBuilder = null;
      if (inprogressPartBuilders != null) {
        // If we have a in-progress partition modification, update the partition builder.
        partBuilder = inprogressPartBuilders.get(partName);
        Preconditions.checkNotNull(partBuilder);
      }
      partBuilder = createOrUpdatePartitionBuilder(
          msPartition.getSd(), msPartition, permCache, partBuilder);
      if (partitionToEventId != null) {
        partBuilder.setCreateEventId(partitionToEventId.getOrDefault(partName, -1L));
      }
      partBuilders.add(partBuilder);
    }
    long latestEventId = MetastoreEventsProcessor.getCurrentEventIdNoThrow(client);
    catalogTimeline.markEvent(FETCHED_LATEST_HMS_EVENT_ID + latestEventId);
    long fileMdLoadTime = loadFileMetadataForPartitions(client, partBuilders,
        /* isRefresh=*/false, catalogTimeline);
    for (HdfsPartition.Builder p : partBuilders) {
      if (inprogressPartBuilders == null) {
        addPartition(p.build());
      } else {
        updatePartition(p);
      }
      if (latestEventId > -1) {
        p.setLastRefreshEventId(latestEventId);
      }
    }
    LOG.info("Setting the latest refresh event id to {} for the loaded partitions for "
        + "the table {}", latestEventId, getFullName());
    return fileMdLoadTime;
  }

  /**
   * For each of the partitions in 'msPartitions' with a location inside the table's
   * base directory, attempt to pre-cache the associated file permissions into the
   * returned cache. This takes advantage of the fact that many partition directories will
   * be in the same parent directories, and we can bulk fetch the permissions with a
   * single round trip to the filesystem instead of individually looking up each.
   */
  private FsPermissionCache preloadPermissionsCache(List<Partition> msPartitions,
      EventSequence catalogTimeline) {
    FsPermissionCache permCache = new FsPermissionCache();
    // Only preload permissions if the number of partitions to be added is
    // large (3x) relative to the number of existing partitions. This covers
    // two common cases:
    //
    // 1) initial load of a table (no existing partition metadata)
    // 2) ALTER TABLE RECOVER PARTITIONS after creating a table pointing to
    // an already-existing partition directory tree
    //
    // Without this heuristic, we would end up using a "listStatus" call to
    // potentially fetch a bunch of irrelevant information about existing
    // partitions when we only want to know about a small number of newly-added
    // partitions.
    // Using '<=' to skip unpartitioned tables, i.e. both sides are 0.
    if (msPartitions.size() <= partitionMap_.size() * 3) return permCache;

    // TODO(todd): when HDFS-13616 (batch listing of multiple directories)
    // is implemented, we could likely implement this with a single round
    // trip.
    Multiset<Path> parentPaths = HashMultiset.create();
    for (Partition p : msPartitions) {
      // We only do this optimization for partitions which are within the table's base
      // directory. Otherwise we risk a case where a user has specified an external
      // partition location that is in a directory containing a high number of irrelevant
      // files, and we'll potentially regress performance compared to just looking up
      // the partition file directly.
      String loc = p.getSd().getLocation();
      if (!loc.startsWith(hdfsBaseDir_)) continue;
      Path parent = new Path(loc).getParent();
      if (parent == null) continue;
      parentPaths.add(parent);
    }

    // For any paths that contain more than one partition, issue a listStatus
    // and pre-cache the resulting permissions.
    for (Multiset.Entry<Path> entry : parentPaths.entrySet()) {
      if (entry.getCount() == 1) continue;
      Path p = entry.getElement();
      try {
        FileSystem fs = p.getFileSystem(CONF);
        // Only attempt to cache permissions for filesystems where we will actually
        // use them.
        if (!assumeReadWriteAccess(fs)) permCache.precacheChildrenOf(fs, p);
      } catch (IOException ioe) {
        // If we fail to pre-warm the cache we'll just wait for later when we
        // try to actually load the individual permissions, at which point
        // we can handle the issue accordingly.
        LOG.debug("Unable to bulk-load permissions for parent path: " + p, ioe);
      }
    }
    catalogTimeline.markEvent(String.format(
        "Preloaded permissions cache for %d partitions", msPartitions.size()));
    return permCache;
  }

  @Override
  protected List<String> getColumnNamesWithHmsStats() {
    List<String> ret = new ArrayList<>();
    // Only non-partition columns have column stats in the HMS.
    for (Column column: getColumns().subList(numClusteringCols_, getColumns().size())) {
      ret.add(column.getName().toLowerCase());
    }
    return ret;
  }

  // TODO: why is there a single synchronized function?
  @Override
  protected synchronized void loadFromThrift(TTable thriftTable)
      throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    THdfsTable hdfsTable = thriftTable.getHdfs_table();
    partitionLocationCompressor_ = new HdfsPartitionLocationCompressor(
        numClusteringCols_, hdfsTable.getPartition_prefixes());
    hdfsBaseDir_ = hdfsTable.getHdfsBaseDir();
    nullColumnValue_ = hdfsTable.nullColumnValue;
    nullPartitionKeyValue_ = hdfsTable.nullPartitionKeyValue;
    hostIndex_.populate(hdfsTable.getNetwork_addresses());
    sqlConstraints_ =  SqlConstraints.fromThrift(hdfsTable.getSql_constraints());
    resetPartitions();
    try {
      if (hdfsTable.has_full_partitions) {
        for (THdfsPartition tPart : hdfsTable.getPartitions().values()) {
          addPartition(new HdfsPartition.Builder(this, tPart.id)
              .fromThrift(tPart)
              .build());
        }
      }
      prototypePartition_ =
          new HdfsPartition.Builder(this, CatalogObjectsConstants.PROTOTYPE_PARTITION_ID)
              .fromThrift(hdfsTable.prototype_partition)
              .build();
    } catch (CatalogException e) {
      throw new TableLoadingException(e.getMessage());
    }
    avroSchema_ = hdfsTable.isSetAvroSchema() ? hdfsTable.getAvroSchema() : null;
    isMarkedCached_ =
        HdfsCachingUtil.validateCacheParams(getMetaStoreTable().getParameters());
    if (hdfsTable.isSetValid_write_ids()) {
      validWriteIds_ =
          new MutableValidReaderWriteIdList(MetastoreShim.getValidWriteIdListFromThrift(
              getFullName(), hdfsTable.getValid_write_ids()));
    }
  }

  /**
   * Validate that all expected partitions are set and not have any stale partitions.
   */
  public void validatePartitions(Set<Long> expectedPartitionIds)
      throws TableLoadingException {
    if (!partitionMap_.keySet().equals(expectedPartitionIds)) {
      Set<Long> missingIds = new HashSet<>(expectedPartitionIds);
      missingIds.removeAll(partitionMap_.keySet());
      Set<Long> staleIds = new HashSet<>(partitionMap_.keySet());
      staleIds.removeAll(expectedPartitionIds);
      throw new TableLoadingException(String.format("Error applying incremental updates" +
              " on table %s. Missing partition ids: %s. Stale partition ids: %s. Total " +
              "partitions: %d.",
          getFullName(), missingIds, staleIds, partitionMap_.size()));
    }
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    // Create thrift descriptors to send to the BE. The BE does not
    // need any information below the THdfsPartition level.
    TTableDescriptor tableDesc = new TTableDescriptor(tableId, TTableType.HDFS_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    tableDesc.setHdfsTable(getTHdfsTable(ThriftObjectType.DESCRIPTOR_ONLY,
        referencedPartitions));
    return tableDesc;
  }

  @Override
  public TTable toThrift() {
    // Send all metadata between the catalog service and the FE.
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HDFS_TABLE);
    table.setHdfs_table(getTHdfsTable(ThriftObjectType.FULL, null));
    return table;
  }

  @Override
  public TTable toHumanReadableThrift() {
    // Same as toThrift, but call getTHdfsTable with lower
    // ThriftObjectType.DESCRIPTOR_ONLY.
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HDFS_TABLE);
    table.setHdfs_table(getTHdfsTable(ThriftObjectType.DESCRIPTOR_ONLY, null));
    return table;
  }

  /**
   * Just like toThrift but unset the full partition metadata in the partition map.
   * So only the partition ids are contained. Used in catalogd to send partition updates
   * individually in catalog topic updates.
   */
  public TTable toThriftWithMinimalPartitions() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HDFS_TABLE);
    // Specify an empty set to exclude all partitions.
    THdfsTable hdfsTable = getTHdfsTable(ThriftObjectType.DESCRIPTOR_ONLY,
        Collections.emptySet());
    // Host indexes are still required by resolving incremental partition updates.
    hdfsTable.setNetwork_addresses(hostIndex_.getList());
    // Coordinators use the partition ids to detect stale and new partitions. Thrift
    // doesn't allow null values in maps. So we still set non-null values here.
    for (long partId : partitionMap_.keySet()) {
      THdfsPartition part = new THdfsPartition();
      part.setId(partId);
      hdfsTable.putToPartitions(partId, part);
    }
    hdfsTable.setHas_full_partitions(false);
    hdfsTable.setHas_partition_names(false);
    table.setHdfs_table(hdfsTable);
    return table;
  }

  /**
   * Just likes super.toMinimalTCatalogObject() but try to add the minimal catalog
   * objects of partitions in the returned result.
   */
  @Override
  public TCatalogObject toMinimalTCatalogObject() {
    TCatalogObject catalogObject = super.toMinimalTCatalogObject();
    if (!BackendConfig.INSTANCE.isIncrementalMetadataUpdatesEnabled()) {
      return catalogObject;
    }
    // Try adding the partition ids and names if we can acquire the read lock.
    // Use tryReadLock() to avoid being blocked by concurrent DDLs. It won't result in
    // correctness issues if we return the result without the partition ids and names.
    // The results are mainly used in two scenarios:
    // 1. The result is added to catalog deleteLog for sending delete updates for
    //    partitions. The delete update for the table is always sent so coordinators are
    //    able to invalidate the HdfsTable object. This enforces the correctness.
    //    However, not sending the deletes causes a leak of the topic values in
    //    statestore if the topic keys (tableName+partName) are not reused anymore.
    //    Note that topic keys are never deleted in the TopicEntryMap of statestore even
    //    if we send the delete updates. So we already have a leak on catalog topic keys.
    //    We can revisit this if we found statestore has memory issues.
    // 2. The result is used in DDL/DML response for a removed/invalidated table.
    //    Coordinators can still invalidate its cache since the table is sent (in the
    //    parent implementation).
    //    However, LocalCatalog coordinators can't immediately invalidate partitions of
    //    a removed table. They will be cleared by the cache eviction policy since the
    //    partitions of deleted tables won't be used anymore.
    // TODO: synchronize the access on the partition map by using a finer-grained lock
    if (!tryReadLock()) {
      LOG.warn("Not returning the partition ids and names of table {} since not " +
          "holding the table read lock", getFullName());
      return catalogObject;
    }
    try {
      catalogObject.getTable().setTable_type(TTableType.HDFS_TABLE);
      // Column names are unused by the consumers so use an empty list here.
      THdfsTable hdfsTable = new THdfsTable(hdfsBaseDir_,
          /*colNames=*/ Collections.emptyList(),
          nullPartitionKeyValue_, nullColumnValue_,
          /*idToPartition=*/ new HashMap<>(),
          /*prototypePartition=*/ FAKE_THDFS_PARTITION);
      for (HdfsPartition part : partitionMap_.values()) {
        hdfsTable.partitions.put(part.getId(), part.toMinimalTHdfsPartition());
      }
      // Adds the recently dropped partitions that are not yet synced to the catalog
      // topic.
      for (HdfsPartition part : droppedPartitions_) {
        hdfsTable.addToDropped_partitions(part.toMinimalTHdfsPartition());
      }
      hdfsTable.setHas_full_partitions(false);
      // The minimal catalog object of partitions contain the partition names.
      hdfsTable.setHas_partition_names(true);
      catalogObject.getTable().setHdfs_table(hdfsTable);
      return catalogObject;
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Gets the max sent partition id in previous catalog topic updates.
   */
  public long getMaxSentPartitionId() { return maxSentPartitionId_; }

  /**
   * Updates the max sent partition id in catalog topic updates.
   */
  public void setMaxSentPartitionId(long maxSentPartitionId) {
    this.maxSentPartitionId_ = maxSentPartitionId;
  }

  /**
   * Resets the max sent partition id in catalog topic updates. Used when statestore
   * resarts and requires a full update from the catalogd.
   */
  public void resetMaxSentPartitionId() {
    maxSentPartitionId_ = Catalog.INITIAL_CATALOG_VERSION - 1;
  }

  /**
   * Gets the deleted/replaced partition instances since last catalog topic update.
   */
  public List<HdfsPartition> getDroppedPartitions() {
    return ImmutableList.copyOf(droppedPartitions_);
  }

  /**
   * Clears the deleted/replaced partition instance set.
   */
  public void resetDroppedPartitions() { droppedPartitions_.clear(); }

  /**
   * Gets catalog objects of new partitions since last catalog update. They are partitions
   * that coordinators are not aware of.
   */
  public List<TCatalogObject> getNewPartitionsSinceLastUpdate() {
    List<TCatalogObject> result = new ArrayList<>();
    int numSkippedParts = 0;
    for (HdfsPartition partition: partitionMap_.values()) {
      if (partition.getId() <= maxSentPartitionId_) {
        numSkippedParts++;
        continue;
      }
      TCatalogObject catalogPart =
          new TCatalogObject(TCatalogObjectType.HDFS_PARTITION, getCatalogVersion());
      partition.setTCatalogObject(catalogPart);
      result.add(catalogPart);
    }
    LOG.info("Skipped {} partitions of table {} in the incremental update",
        numSkippedParts, getFullName());
    return result;
  }

  public TGetPartialCatalogObjectResponse getPartialInfo(
      TGetPartialCatalogObjectRequest req,
      Map<HdfsPartition, TPartialPartitionInfo> missingPartitionInfos)
      throws CatalogException {
    Preconditions.checkNotNull(missingPartitionInfos);
    TGetPartialCatalogObjectResponse resp = super.getPartialInfo(req);
    boolean wantPartitionInfo = req.table_info_selector.want_partition_files ||
        req.table_info_selector.want_partition_metadata ||
        req.table_info_selector.want_partition_names ||
        req.table_info_selector.want_partition_stats;
    if (req.table_info_selector.want_partition_metadata
        && req.table_info_selector.want_hms_partition) {
      LOG.warn("Bad request that has both want_partition_metadata and " +
          "want_hms_partition set to true. Duplicated data will be returned. {}",
          StringUtils.abbreviate(req.toString(), 1000));
    }

    Collection<Long> partIds = req.table_info_selector.partition_ids;
    if (partIds == null && wantPartitionInfo) {
      // Caller specified at least one piece of partition info but didn't specify
      // any partition IDs. That means they want the info for all partitions.
      partIds = partitionMap_.keySet();
    }

    ValidWriteIdList reqWriteIdList = req.table_info_selector.valid_write_ids == null ?
        null : MetastoreShim.getValidWriteIdListFromThrift(getFullName(),
        req.table_info_selector.valid_write_ids);
    Counter misses = metrics_.getCounter(FILEMETADATA_CACHE_MISS_METRIC);
    Counter hits = metrics_.getCounter(FILEMETADATA_CACHE_HIT_METRIC);
    int numFilesFiltered = 0;
    if (partIds != null) {
      resp.table_info.partitions = Lists.newArrayListWithCapacity(partIds.size());
      for (long partId : partIds) {
        HdfsPartition part = partitionMap_.get(partId);
        if (part == null) {
          LOG.warn(String.format("Missing partition ID: %s, Table: %s", partId,
              getFullName()));
          return new TGetPartialCatalogObjectResponse().setLookup_status(
              CatalogLookupStatus.PARTITION_NOT_FOUND);
        }
        TPartialPartitionInfo partInfo = new TPartialPartitionInfo(partId);

        if (req.table_info_selector.want_partition_names) {
          partInfo.setName(part.getPartitionName());
        }

        if (req.table_info_selector.want_partition_metadata) {
          part.setPartitionMetadata(partInfo);
          partInfo.setHas_incremental_stats(part.hasIncrementalStats());
        }
        if (req.table_info_selector.want_hms_partition) {
          partInfo.hms_partition = part.toHmsPartition();
        }

        if (req.table_info_selector.want_partition_files) {
          partInfo.setLast_compaction_id(part.getLastCompactionId());
          try {
            if (!part.getInsertFileDescriptors().isEmpty()) {
              partInfo.file_descriptors = new ArrayList<>();
              partInfo.insert_file_descriptors = new ArrayList<>();
              numFilesFiltered += addFilteredFds(part.getInsertFileDescriptors(),
                  partInfo.insert_file_descriptors, reqWriteIdList);
              partInfo.delete_file_descriptors = new ArrayList<>();
              numFilesFiltered += addFilteredFds(part.getDeleteFileDescriptors(),
                  partInfo.delete_file_descriptors, reqWriteIdList);
            } else {
              partInfo.file_descriptors = new ArrayList<>();
              numFilesFiltered += addFilteredFds(part.getFileDescriptors(),
                  partInfo.file_descriptors, reqWriteIdList);
              partInfo.insert_file_descriptors = new ArrayList<>();
              partInfo.delete_file_descriptors = new ArrayList<>();
            }
            hits.inc();
          } catch (CatalogException ex) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Could not use cached file descriptors of partition {} of table"
                      + " {} for writeIdList {}", part.getPartitionName(), getFullName(),
                  reqWriteIdList, ex);
            }
            misses.inc();
            missingPartitionInfos.put(part, partInfo);
          }
        }

        if (req.table_info_selector.want_partition_stats) {
          partInfo.setPartition_stats(part.getPartitionStatsCompressed());
        }

        partInfo.setIs_marked_cached(part.isMarkedCached());
        resp.table_info.partitions.add(partInfo);
      }
    }
    // In most of the cases, the prefix map only contains one item for the table location.
    // Here we always send it since it's small.
    resp.table_info.setPartition_prefixes(partitionLocationCompressor_.getPrefixes());

    if (reqWriteIdList != null) {
      LOG.debug("{} files filtered out of table {} for {}. Hit rate : {}",
          numFilesFiltered, getFullName(), reqWriteIdList, getFileMetadataCacheHitRate());
    }

    if (req.table_info_selector.want_partition_files) {
      // TODO(todd) we are sending the whole host index even if we returned only
      // one file -- maybe not so efficient, but the alternative is to do a bunch
      // of cloning of file descriptors which might increase memory pressure.
      resp.table_info.setNetwork_addresses(hostIndex_.getList());
    }

    if (req.table_info_selector.want_table_constraints) {
      TSqlConstraints sqlConstraints =
          new TSqlConstraints(sqlConstraints_.getPrimaryKeys(),
          sqlConstraints_.getForeignKeys());
      resp.table_info.setSql_constraints(sqlConstraints);
    }
    // Publish the isMarkedCached_ marker so coordinators don't need to validate
    // it again which requires additional HDFS RPCs.
    resp.table_info.setIs_marked_cached(isMarkedCached_);
    return resp;
  }

  private int addFilteredFds(List<FileDescriptor> fds, List<THdfsFileDesc> thriftFds,
      ValidWriteIdList writeIdList) throws CatalogException {
    List<FileDescriptor> filteredFds = new ArrayList<>(fds);
    int numFilesFiltered = AcidUtils.filterFdsForAcidState(filteredFds, writeIdList);
    for (FileDescriptor fd: filteredFds) {
      thriftFds.add(fd.toThrift());
    }
    return numFilesFiltered;
  }

  private double getFileMetadataCacheHitRate() {
    long hits = metrics_.getCounter(FILEMETADATA_CACHE_HIT_METRIC).getCount();
    long misses = metrics_.getCounter(FILEMETADATA_CACHE_MISS_METRIC).getCount();
    return ((double) hits) / (double) (hits+misses);
  }

  /**
   * Create a THdfsTable corresponding to this HdfsTable. If serializing the "FULL"
   * information, then then all partitions and THdfsFileDescs of each partition should be
   * included. Otherwise, don't include any THdfsFileDescs, and include only those
   * partitions in the refPartitions set (the backend doesn't need metadata for
   * unreferenced partitions). In addition, metadata that is not used by the backend will
   * be omitted.
   *
   * To prevent the catalog from hitting an OOM error while trying to
   * serialize large partition incremental stats, we estimate the stats size and filter
   * the incremental stats data from partition objects if the estimate exceeds
   * --inc_stats_size_limit_bytes. This function also collects storage related statistics
   *  (e.g. number of blocks, files, etc) in order to compute an estimate of the metadata
   *  size of this table.
   */
  public THdfsTable getTHdfsTable(ThriftObjectType type, Set<Long> refPartitions) {
    if (type == ThriftObjectType.FULL) {
      // "full" implies all partitions should be included.
      Preconditions.checkArgument(refPartitions == null);
    }
    long memUsageEstimate = 0;
    int numPartitions =
        (refPartitions == null) ? partitionMap_.values().size() : refPartitions.size();
    memUsageEstimate += numPartitions * PER_PARTITION_MEM_USAGE_BYTES;
    FileMetadataStats stats = new FileMetadataStats();
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    for (HdfsPartition partition: partitionMap_.values()) {
      long id = partition.getId();
      if (refPartitions == null || refPartitions.contains(id)) {
        THdfsPartition tHdfsPartition = FeCatalogUtils.fsPartitionToThrift(
            partition, type);
        if (partition.hasIncrementalStats()) {
          memUsageEstimate += getColumns().size() * STATS_SIZE_PER_COLUMN_BYTES;
          hasIncrementalStats_ = true;
        }
        if (type == ThriftObjectType.FULL) {
          Preconditions.checkState(tHdfsPartition.isSetNum_blocks() &&
              tHdfsPartition.isSetTotal_file_size_bytes());
          stats.numBlocks += tHdfsPartition.getNum_blocks();
          stats.numFiles +=
              tHdfsPartition.isSetFile_desc() ? tHdfsPartition.getFile_desc().size() : 0;
          stats.numFiles += tHdfsPartition.isSetInsert_file_desc() ?
              tHdfsPartition.getInsert_file_desc().size() : 0;
          stats.numFiles += tHdfsPartition.isSetDelete_file_desc() ?
              tHdfsPartition.getDelete_file_desc().size() : 0;
          stats.totalFileBytes += tHdfsPartition.getTotal_file_size_bytes();
        }
        idToPartition.put(id, tHdfsPartition);
      }
    }
    if (type == ThriftObjectType.FULL) fileMetadataStats_.set(stats);

    THdfsPartition prototypePartition = FeCatalogUtils.fsPartitionToThrift(
        prototypePartition_, ThriftObjectType.DESCRIPTOR_ONLY);

    memUsageEstimate += fileMetadataStats_.numFiles * PER_FD_MEM_USAGE_BYTES +
        fileMetadataStats_.numBlocks * PER_BLOCK_MEM_USAGE_BYTES;
    if (type == ThriftObjectType.FULL) {
      // These metrics only make sense when we are collecting a FULL object.
      setEstimatedMetadataSize(memUsageEstimate);
      setNumFiles(fileMetadataStats_.numFiles);
    }
    THdfsTable hdfsTable = new THdfsTable(hdfsBaseDir_, getColumnNames(),
        getNullPartitionKeyValue(), nullColumnValue_, idToPartition, prototypePartition);
    hdfsTable.setAvroSchema(avroSchema_);
    hdfsTable.setSql_constraints(sqlConstraints_.toThrift());
    if (type == ThriftObjectType.FULL) {
      hdfsTable.setHas_full_partitions(true);
      // Network addresses are used only by THdfsFileBlocks which are inside
      // THdfsFileDesc, so include network addreses only when including THdfsFileDesc.
      hdfsTable.setNetwork_addresses(hostIndex_.getList());
    }
    hdfsTable.setPartition_prefixes(partitionLocationCompressor_.getPrefixes());
    if (AcidUtils.isFullAcidTable(getMetaStoreTable().getParameters())) {
      hdfsTable.setIs_full_acid(true);
    }
    if (validWriteIds_ != null) {
      hdfsTable.setValid_write_ids(
          MetastoreShim.convertToTValidWriteIdList(validWriteIds_));
    }
    return hdfsTable;
  }

  @Override // FeFsTable
  public long getTotalHdfsBytes() { return fileMetadataStats_.totalFileBytes; }

  @Override // FeFsTable
  public String getHdfsBaseDir() { return hdfsBaseDir_; }

  public Path getHdfsBaseDirPath() {
    Preconditions.checkNotNull(hdfsBaseDir_, "HdfsTable base dir is null");
    return new Path(hdfsBaseDir_);
  }

  @Override // FeFsTable
  public boolean usesAvroSchemaOverride() { return avroSchema_ != null; }

  @Override // FeFsTable
  public ListMap<TNetworkAddress> getHostIndex() { return hostIndex_; }

  @Override
  public SqlConstraints getSqlConstraints() {
    return sqlConstraints_;
  }

  /**
   * Returns the set of file formats that the partitions are stored in.
   */
  @Override
  public Set<HdfsFileFormat> getFileFormats() {
    // In the case that we have no partitions added to the table yet, it's
    // important to add the "prototype" partition as a fallback.
    Iterable<HdfsPartition> partitionsToConsider = Iterables.concat(
        partitionMap_.values(), Collections.singleton(prototypePartition_));
    return FeCatalogUtils.getFileFormats(partitionsToConsider);
  }

  /**
   * Returns the HDFS paths corresponding to HdfsTable partitions that don't exist in
   * the Hive Metastore. An HDFS path is represented as a list of strings values, one per
   * partition key column.
   * @param debugAction
   */
  public List<List<String>> getPathsWithoutPartitions(@Nullable String debugAction)
      throws CatalogException {
    Set<List<LiteralExpr>> existingPartitions = new HashSet<>();
    // Get the list of partition values of existing partitions in Hive Metastore.
    for (HdfsPartition partition: partitionMap_.values()) {
      existingPartitions.add(partition.getPartitionValues());
    }

    List<String> partitionKeys = new ArrayList<>();
    for (int i = 0; i < numClusteringCols_; ++i) {
      partitionKeys.add(getColumns().get(i).getName());
    }
    Path basePath = new Path(hdfsBaseDir_);
    List<List<String>> partitionsNotInHms = new ArrayList<>();
    try {
      getAllPartitionsNotInHms(basePath, partitionKeys, existingPartitions,
          partitionsNotInHms);
      DebugUtils.executeDebugAction(debugAction, DebugUtils.RECOVER_PARTITIONS_DELAY);
    } catch (Exception e) {
      throw new CatalogException(String.format("Failed to recover partitions for %s " +
          "with exception:%s.", getFullName(), e));
    }
    return partitionsNotInHms;
  }

  /**
   * Returns all partitions which match the partition keys directory structure and pass
   * type compatibility check. Also these partitions are not already part of the table.
   */
  private void getAllPartitionsNotInHms(Path path, List<String> partitionKeys,
      Set<List<LiteralExpr>> existingPartitions,
      List<List<String>> partitionsNotInHms) throws IOException {
    FileSystem fs = path.getFileSystem(CONF);
    List<String> partitionValues = new ArrayList<>();
    List<LiteralExpr> partitionExprs = new ArrayList<>();
    getAllPartitionsNotInHms(path, partitionKeys, 0, fs, partitionValues,
        partitionExprs, existingPartitions, partitionsNotInHms);
  }

  /**
   * Returns all partitions which match the partition keys directory structure and pass
   * the type compatibility check.
   *
   * path e.g. c1=1/c2=2/c3=3
   * partitionKeys The ordered partition keys. e.g.("c1", "c2", "c3")
   * depth. The start position in partitionKeys to match the path name.
   * partitionValues The partition values used to create a partition.
   * partitionExprs The list of LiteralExprs which is used to avoid duplicate partitions.
   * E.g. Having /c1=0001 and /c1=01, we should make sure only one partition
   * will be added.
   * existingPartitions All partitions which exist in Hive Metastore or newly added.
   * partitionsNotInHms Contains all the recovered partitions.
   */
  private void getAllPartitionsNotInHms(Path path, List<String> partitionKeys,
      int depth, FileSystem fs, List<String> partitionValues,
      List<LiteralExpr> partitionExprs, Set<List<LiteralExpr>> existingPartitions,
      List<List<String>> partitionsNotInHms) throws IOException {
    if (depth == partitionKeys.size()) {
      if (existingPartitions.contains(partitionExprs)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Skip recovery of path '%s' because it already "
              + "exists in metastore", path.toString()));
        }
      } else {
        partitionsNotInHms.add(partitionValues);
        existingPartitions.add(partitionExprs);
      }
      return;
    }

    RemoteIterator <? extends FileStatus> statuses = fs.listStatusIterator(path);

    if (statuses == null) return;
    while (statuses.hasNext()) {
      FileStatus status = statuses.next();
      if (!status.isDirectory()) continue;
      Pair<String, LiteralExpr> keyValues =
          getTypeCompatibleValue(status.getPath(), partitionKeys.get(depth));
      if (keyValues == null) continue;

      List<String> currentPartitionValues = Lists.newArrayList(partitionValues);
      List<LiteralExpr> currentPartitionExprs = Lists.newArrayList(partitionExprs);
      currentPartitionValues.add(keyValues.first);
      currentPartitionExprs.add(keyValues.second);
      getAllPartitionsNotInHms(status.getPath(), partitionKeys, depth + 1, fs,
          currentPartitionValues, currentPartitionExprs,
          existingPartitions, partitionsNotInHms);
    }
  }

  /**
   * Returns a list of {@link LiteralExpr} which is type compatible with the partition
   * keys of this table. This is useful to convert the string values which are received
   * from metastore events to {@link LiteralExpr}.
   */
  public List<LiteralExpr> getTypeCompatiblePartValues(List<String> values) {
    List<LiteralExpr> result = new ArrayList<>();
    List<Column> partitionColumns = getClusteringColumns();
    Preconditions.checkState(partitionColumns.size() == values.size());
    for (int i=0; i<partitionColumns.size(); ++i) {
      Pair<String, LiteralExpr> pair = getPartitionExprFromValue(values.get(i),
          partitionColumns.get(i).getType());
      if (pair == null) {
        LOG.error("Could not get a type compatible value for key {} with value {}", i,
            values.get(i));
        return null;
      }
      result.add(pair.second);
    }
    return result;
  }

  /**
   * Checks that the last component of 'path' is of the form "<partitionkey>=<v>"
   * where 'v' is a type-compatible value from the domain of the 'partitionKey' column.
   * If not, returns null, otherwise returns a Pair instance, the first element is the
   * original value, the second element is the LiteralExpr created from the original
   * value.
   */
  private Pair<String, LiteralExpr> getTypeCompatibleValue(
      Path path, String partitionKey) {
    String partName[] = path.getName().split("=");
    if (partName.length != 2 || !partName[0].equals(partitionKey)) return null;

    // Check Type compatibility for Partition value.
    Column column = getColumn(partName[0]);
    Preconditions.checkNotNull(column);
    Type type = column.getType();
    return getPartitionExprFromValue(partName[1], type);
  }

  /**
   * Converts a given partition value to a {@link LiteralExpr} based on the type of the
   * partition column.
   * @param partValue Value of the partition column
   * @param type Type of the partition column
   * @return Pair which contains the partition value and its equivalent
   * {@link LiteralExpr} according to the type provided.
   */
  private Pair<String, LiteralExpr> getPartitionExprFromValue(
      String partValue, Type type) {
    LiteralExpr expr;
    // URL decode the partition value since it may contain encoded URL.
    String value = FileUtils.unescapePathName(partValue);
    if (!value.equals(getNullPartitionKeyValue())) {
      try {
        expr = LiteralExpr.createFromUnescapedStr(value, type);
        // Skip large value which exceeds the MAX VALUE of specified Type.
        if (expr instanceof NumericLiteral) {
          if (NumericLiteral.isOverflow(((NumericLiteral) expr).getValue(), type)) {
            LOG.warn(String.format("Skip the overflow value (%s) for Type (%s).",
                value, type.toSql()));
            return null;
          }
        }
      } catch (Exception ex) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Invalid partition value (%s) for Type (%s).",
              value, type.toSql()));
        }
        return null;
      }
    } else {
      expr = new NullLiteral();
    }
    return new Pair<>(value, expr);
  }

  @Override // FeFsTable
  public TResultSet getTableStats() {
    return getTableStats(this);
  }

  @Override
  public FileSystemUtil.FsType getFsType() {
    Preconditions.checkNotNull(getHdfsBaseDirPath().toUri().getScheme(),
        "Cannot get scheme from path " + getHdfsBaseDirPath());
    return FileSystemUtil.FsType.getFsType(getHdfsBaseDirPath().toUri().getScheme());
  }

  // TODO(todd): move to FeCatalogUtils. Upon moving to Java 8, could be
  // a default method of FeFsTable.
  public static TResultSet getTableStats(FeFsTable table) {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);

    for (int i = 0; i < table.getNumClusteringCols(); ++i) {
      // Add the partition-key values as strings for simplicity.
      Column partCol = table.getColumns().get(i);
      TColumn colDesc = new TColumn(partCol.getName(), Type.STRING.toThrift());
      resultSchema.addToColumns(colDesc);
    }

    boolean statsExtrap = Utils.isStatsExtrapolationEnabled(table);

    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    if (statsExtrap) {
      resultSchema.addToColumns(new TColumn("Extrap #Rows", Type.BIGINT.toThrift()));
    }
    resultSchema.addToColumns(new TColumn("#Files", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Bytes Cached", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Cache Replication", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Format", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Incremental stats", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Location", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("EC Policy", Type.STRING.toThrift()));

    // Pretty print partitions and their stats.
    List<FeFsPartition> orderedPartitions = new ArrayList<>(
        FeCatalogUtils.loadAllPartitions(table));
    Collections.sort(orderedPartitions, HdfsPartition.KV_COMPARATOR);

    long totalCachedBytes = 0L;
    long totalBytes = 0L;
    long totalNumFiles = 0L;
    for (FeFsPartition p: orderedPartitions) {
      int numFiles = p.getNumFileDescriptors();
      long size = p.getSize();
      totalNumFiles += numFiles;
      totalBytes += size;

      TResultRowBuilder rowBuilder = new TResultRowBuilder();

      // Add the partition-key values (as strings for simplicity).
      for (LiteralExpr expr: p.getPartitionValues()) {
        rowBuilder.add(expr.getStringValue());
      }

      // Add rows, extrapolated rows, files, bytes, cache stats, and file format.
      rowBuilder.add(p.getNumRows());
      // Compute and report the extrapolated row count because the set of files could
      // have changed since we last computed stats for this partition. We also follow
      // this policy during scan-cardinality estimation.
      if (statsExtrap) {
        rowBuilder.add(Utils.getExtrapolatedNumRows(table, size));
      }

      rowBuilder.add(numFiles).addBytes(size);
      if (!p.isMarkedCached()) {
        // Helps to differentiate partitions that have 0B cached versus partitions
        // that are not marked as cached.
        rowBuilder.add("NOT CACHED");
        rowBuilder.add("NOT CACHED");
      } else {
        // Calculate the number the number of bytes that are cached.
        long cachedBytes = 0L;
        for (FileDescriptor fd: p.getFileDescriptors()) {
          int numBlocks = fd.getNumFileBlocks();
          for (int i = 0; i < numBlocks; ++i) {
            FbFileBlock block = fd.getFbFileBlock(i);
            if (FileBlock.hasCachedReplica(block)) {
              cachedBytes += FileBlock.getLength(block);
            }
          }
        }
        totalCachedBytes += cachedBytes;
        rowBuilder.addBytes(cachedBytes);

        // Extract cache replication factor from the parameters of the table
        // if the table is not partitioned or directly from the partition.
        Short rep = HdfsCachingUtil.getCachedCacheReplication(
            table.getNumClusteringCols() == 0 ?
            p.getTable().getMetaStoreTable().getParameters() :
            p.getParameters());
        rowBuilder.add(rep.toString());
      }
      rowBuilder.add(p.getFileFormat().toString());
      rowBuilder.add(String.valueOf(p.hasIncrementalStats()));
      rowBuilder.add(p.getLocation());
      rowBuilder.add(FileSystemUtil.getErasureCodingPolicy(p.getLocationPath()));
      result.addToRows(rowBuilder.get());
    }

    // For partitioned tables add a summary row at the bottom.
    if (table.getNumClusteringCols() > 0) {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      int numEmptyCells = table.getNumClusteringCols() - 1;
      rowBuilder.add("Total");
      for (int i = 0; i < numEmptyCells; ++i) {
        rowBuilder.add("");
      }

      // Total rows, extrapolated rows, files, bytes, cache stats.
      // Leave format empty.
      rowBuilder.add(table.getNumRows());
      // Compute and report the extrapolated row count because the set of files could
      // have changed since we last computed stats for this partition. We also follow
      // this policy during scan-cardinality estimation.
      if (statsExtrap) {
        rowBuilder.add(Utils.getExtrapolatedNumRows(
            table, table.getTotalHdfsBytes()));
      }
      rowBuilder.add(totalNumFiles)
          .addBytes(totalBytes)
          .addBytes(totalCachedBytes).add("").add("").add("").add("").add("");
      result.addToRows(rowBuilder.get());
    }
    return result;
  }

  /**
   * Constructs a partition name from a list of TPartitionKeyValue objects.
   */
  public static String constructPartitionName(List<TPartitionKeyValue> partitionSpec) {
    List<String> partitionCols = new ArrayList<>();
    List<String> partitionVals = new ArrayList<>();
    for (TPartitionKeyValue kv: partitionSpec) {
      partitionCols.add(kv.getName());
      partitionVals.add(kv.getValue());
    }
    return org.apache.hadoop.hive.common.FileUtils.makePartName(partitionCols,
        partitionVals);
  }

  /**
   * Generates a debug string useful for logging purposes. It returns a string consisting
   * of names from the given list until the limit and then appends the count of the
   * remaining items.
   */
  private static String generateDebugStr(List<String> items, int limit) {
    String result = Joiner.on(',').join(Iterables.limit(items, limit));
    if (items.size() > limit) {
      result = String.format("%s... and %s others", result, items.size()-limit);
    }
    return result;
  }

  /**
   * Reloads the HdfsPartitions which correspond to the given partNames. Returns the
   * number of partitions which were reloaded.
   * fileMetadataLoadOpts: decides how to reload file metadata for the partitions
   */
  public int reloadPartitionsFromNames(IMetaStoreClient client,
      List<String> partNames, String reason, FileMetadataLoadOpts fileMetadataLoadOpts)
      throws CatalogException {
    Preconditions.checkState(partNames != null && !partNames.isEmpty());
    LOG.info(String.format("Reloading partition metadata: %s %s (%s)",
        getFullName(), generateDebugStr(partNames, 3), reason));
    List<Partition> hmsPartitions;
    Map<Partition, HdfsPartition> hmsPartToHdfsPart = new HashMap<>();
    try {
      hmsPartitions = MetaStoreUtil.fetchPartitionsByName(client, partNames, msTable_);
      for (Partition partition : hmsPartitions) {
        List<LiteralExpr> partExprs = getTypeCompatiblePartValues(partition.getValues());
        HdfsPartition hdfsPartition = getPartition(partExprs);
        if (hdfsPartition != null) {
          hmsPartToHdfsPart.put(partition, hdfsPartition);
        }
      }
      reloadPartitions(client, hmsPartToHdfsPart, fileMetadataLoadOpts,
          NoOpEventSequence.INSTANCE);
      return hmsPartToHdfsPart.size();
    } catch (NoSuchObjectException | InvalidObjectException e) {
      // HMS throws a NoSuchObjectException if the table does not exist
      // in HMS anymore. In case the partitions don't exist in HMS it does not include
      // them in the result of getPartitionsByNames.
      throw new TableLoadingException(
          "Error when reloading partitions for table " + getFullName(), e);
    } catch (TException e2) {
      throw new CatalogException(
          "Unexpected error while retrieving partitions for table " + getFullName(), e2);
    }
  }

  /**
   * Reload the HdfsPartitions which correspond to the given partitions.
   *
   * @param client is the HMS client to be used.
   * @param partsFromEvent Partition objects from the event.
   * @param loadFileMetadata If true, file metadata will be reloaded.
   * @param reason Reason for reloading the partitions for logging purposes.
   * @return the number of partitions which were reloaded.
   */
  public int reloadPartitionsFromEvent(IMetaStoreClient client,
      List<Partition> partsFromEvent, boolean loadFileMetadata, String reason)
      throws CatalogException {
    Preconditions.checkArgument(partsFromEvent != null
        && !partsFromEvent.isEmpty());
    Preconditions.checkState(isWriteLockedByCurrentThread(), "Write Lock should be "
        + "held before reloadPartitionsFromEvent");
    LOG.info("Reloading partition metadata for table: {} ({})", getFullName(), reason);
    Map<Partition, HdfsPartition> hmsPartToHdfsPart = new HashMap<>();
    for (Partition partition : partsFromEvent) {
      List<LiteralExpr> partExprs = getTypeCompatiblePartValues(partition.getValues());
      HdfsPartition hdfsPartition = getPartition(partExprs);
      // only reload partitions that have more recent write id
      if (hdfsPartition != null
          && (!AcidUtils.isTransactionalTable(msTable_.getParameters())
                 || hdfsPartition.getWriteId()
                     <= MetastoreShim.getWriteIdFromMSPartition(partition))) {
        hmsPartToHdfsPart.put(partition, hdfsPartition);
      }
    }
    reloadPartitions(client, hmsPartToHdfsPart, loadFileMetadata,
        NoOpEventSequence.INSTANCE);
    return hmsPartToHdfsPart.size();
  }

  /**
   * Reloads the metadata of partitions given by a map of HMS Partitions to existing (old)
   * HdfsPartitions.
   * @param hmsPartsToHdfsParts The map of HMS partition object to the old HdfsPartition.
   *                            Every key-value in this map represents the HdfsPartition
   *                            which needs to be removed from the table and
   *                            reconstructed from the HMS partition key. If the
   *                            value for a given partition key is null then nothing is
   *                            removed and a new HdfsPartition is simply added.
   * @param loadFileMetadata If true, file metadata for all hmsPartsToHdfsParts will be
   *                        reloaded.
   */
  public void reloadPartitions(IMetaStoreClient client,
      Map<Partition, HdfsPartition> hmsPartsToHdfsParts, boolean loadFileMetadata,
      EventSequence catalogTimeline) throws CatalogException {
    if (loadFileMetadata) {
      reloadPartitions(client, hmsPartsToHdfsParts, FileMetadataLoadOpts.FORCE_LOAD,
          catalogTimeline);
    } else {
      reloadPartitions(client, hmsPartsToHdfsParts, FileMetadataLoadOpts.NO_LOAD,
          catalogTimeline);
    }
  }

  /**
   * Reloads the metadata of partitions given by a map of HMS Partitions to existing (old)
   * HdfsPartitions.
   * @param hmsPartsToHdfsParts The map of HMS partition object to the old HdfsPartition.
   *                            Every key-value in this map represents the HdfsPartition
   *                            which needs to be removed from the table and
   *                            reconstructed from the HMS partition key. If the
   *                            value for a given partition key is null then nothing is
   *                            removed and a new HdfsPartition is simply added.
   * @param fileMetadataLoadOpts describes how to load file metadata
   */
  public void reloadPartitions(IMetaStoreClient client,
      Map<Partition, HdfsPartition> hmsPartsToHdfsParts,
      FileMetadataLoadOpts fileMetadataLoadOpts, EventSequence catalogTimeline)
      throws CatalogException {
    Preconditions.checkState(isWriteLockedByCurrentThread(), "Write Lock should be "
        + "held before reloadPartitions");
    FsPermissionCache permissionCache = new FsPermissionCache();
    Map<HdfsPartition.Builder, HdfsPartition> partBuilderToPartitions = new HashMap<>();
    Set<HdfsPartition.Builder> partBuildersFileMetadataRefresh = new HashSet<>();
    long latestEventId = MetastoreEventsProcessor.getCurrentEventIdNoThrow(client);
    catalogTimeline.markEvent(FETCHED_LATEST_HMS_EVENT_ID + latestEventId);
    for (Map.Entry<Partition, HdfsPartition> entry : hmsPartsToHdfsParts.entrySet()) {
      Partition hmsPartition = entry.getKey();
      HdfsPartition oldPartition = entry.getValue();
      HdfsPartition.Builder partBuilder = createPartitionBuilder(
          hmsPartition.getSd(), hmsPartition, permissionCache);
      Preconditions.checkArgument(oldPartition == null
          || HdfsPartition.comparePartitionKeyValues(
          oldPartition.getPartitionValues(), partBuilder.getPartitionValues()) == 0);
      if (oldPartition != null) {
        partBuilder.setFileDescriptors(oldPartition);
        partBuilder.setCreateEventId(oldPartition.getCreateEventId());
        partBuilder.setLastCompactionId(oldPartition.getLastCompactionId());
      }
      partBuilder.setLastRefreshEventId(latestEventId);
      switch (fileMetadataLoadOpts) {
        case FORCE_LOAD:
          partBuildersFileMetadataRefresh.add(partBuilder);
          break;
        case LOAD_IF_SD_CHANGED:
          if (oldPartition == null || !oldPartition.compareSd(hmsPartition.getSd()) ||
              partBuilder.isMarkedCached()) {
            partBuildersFileMetadataRefresh.add(partBuilder);
          }
          break;
        case NO_LOAD:
          // do nothing
          break;
        default:
          throw new CatalogException("Invalid filemetadataOpts: " +
              fileMetadataLoadOpts.name() + " in reloadPartitions()");
      }
      partBuilderToPartitions.put(partBuilder, oldPartition);
    }
    LOG.info("Setting the latest refresh event id to {} for the reloaded {} partitions",
        latestEventId, partBuilderToPartitions.size());
    if (!partBuildersFileMetadataRefresh.isEmpty()) {
      LOG.info("for table {}, file metadataOps: {}, refreshing file metadata for {}"
              + " out of {} partitions to reload in reloadPartitions()", getFullName(),
          fileMetadataLoadOpts.name(), partBuildersFileMetadataRefresh.size(),
          partBuilderToPartitions.size());
      // load file metadata in parallel
      loadFileMetadataForPartitions(client, partBuildersFileMetadataRefresh,
          /*isRefresh=*/true, catalogTimeline);
    }
    for (Map.Entry<HdfsPartition.Builder, HdfsPartition> entry :
        partBuilderToPartitions.entrySet()) {
      if (entry.getValue() != null) {
        dropPartition(entry.getValue(), false);
      }
      addPartition(entry.getKey().build());
    }
  }

  /**
   * Registers table metrics.
   */
  @Override
  public void initMetrics() {
    super.initMetrics();
    metrics_.addGauge(NUM_PARTITIONS_METRIC, new Gauge<Integer>() {
      @Override
      public Integer getValue() { return partitionMap_.values().size(); }
    });
    metrics_.addGauge(NUM_FILES_METRIC, new Gauge<Long>() {
      @Override
      public Long getValue() { return fileMetadataStats_.numFiles; }
    });
    metrics_.addGauge(NUM_BLOCKS_METRIC, new Gauge<Long>() {
      @Override
      public Long getValue() { return fileMetadataStats_.numBlocks; }
    });
    metrics_.addGauge(TOTAL_FILE_BYTES_METRIC, new Gauge<Long>() {
      @Override
      public Long getValue() { return fileMetadataStats_.totalFileBytes; }
    });
    metrics_.addGauge(MEMORY_ESTIMATE_METRIC, new Gauge<Long>() {
      @Override
      public Long getValue() { return getEstimatedMetadataSize(); }
    });
    metrics_.addGauge(HAS_INCREMENTAL_STATS_METRIC, new Gauge<Boolean>() {
      @Override
      public Boolean getValue() { return hasIncrementalStats_; }
    });
    metrics_.addTimer(CATALOG_UPDATE_DURATION_METRIC);
    metrics_.addCounter(FILEMETADATA_CACHE_HIT_METRIC);
    metrics_.addCounter(FILEMETADATA_CACHE_MISS_METRIC);
    metrics_.addCounter(NUM_LOAD_FILEMETADATA_METRIC);
  }

  /**
   * Creates a temporary HdfsTable object populated with the specified properties.
   * This is used for CTAS statements.
   */
  public static HdfsTable createCtasTarget(Db db,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws CatalogException {
    HdfsTable tmpTable = new HdfsTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    HiveConf hiveConf = new HiveConf(HdfsTable.class);
    // set nullPartitionKeyValue from the hive conf.
    tmpTable.nullPartitionKeyValue_ = hiveConf.get(
        MetaStoreUtil.NULL_PARTITION_KEY_VALUE_CONF_KEY,
        MetaStoreUtil.DEFAULT_NULL_PARTITION_KEY_VALUE);
    tmpTable.loadSchema(msTbl);
    tmpTable.initializePartitionMetadata(msTbl);
    tmpTable.setTableStats(msTbl);
    return tmpTable;
  }

  /**
   * Get valid write ids for the acid table.
   * @param client the client to access HMS
   * @return the list of valid write IDs for the table
   */
  protected ValidWriteIdList fetchValidWriteIds(IMetaStoreClient client)
      throws TableLoadingException {
    String tblFullName = getFullName();
    if (LOG.isTraceEnabled()) LOG.trace("Get valid writeIds for table: " + tblFullName);
    ValidWriteIdList validWriteIds;
    try {
      validWriteIds = MetastoreShim.fetchValidWriteIds(client, tblFullName);
      LOG.info("Valid writeIds of table {}: {}", tblFullName,
          validWriteIds.writeToString());
      return validWriteIds;
    } catch (Exception e) {
      throw new TableLoadingException(String.format("Error loading ValidWriteIds for " +
          "table '%s'", tblFullName), e);
    }
  }

  /**
   * Set ValidWriteIdList with stored writeId
   * @param client the client to access HMS
   */
  protected boolean loadValidWriteIdList(IMetaStoreClient client)
      throws TableLoadingException {
    Stopwatch sw = Stopwatch.createStarted();
    Preconditions.checkState(msTable_ != null && msTable_.getParameters() != null);
    boolean prevWriteIdChanged = false;
    if (MetastoreShim.getMajorVersion() > 2 &&
        AcidUtils.isTransactionalTable(msTable_.getParameters())) {
      ValidWriteIdList writeIdList = fetchValidWriteIds(client);
      prevWriteIdChanged = writeIdList.toString().equals(validWriteIds_);
      validWriteIds_ = new MutableValidReaderWriteIdList(writeIdList);
    } else {
      validWriteIds_ = null;
    }
    LOG.debug("Load Valid Write Id List Done. Time taken: " +
        PrintUtils.printTimeNs(sw.elapsed(TimeUnit.NANOSECONDS)));
    return prevWriteIdChanged;
  }

  @Override
  public ValidWriteIdList getValidWriteIds() {
    if (validWriteIds_ == null) {
      return null;
    }
    // returns a copy to avoid validWriteIds_ is modified outside
    return MetastoreShim.getValidWriteIdListFromString(validWriteIds_.toString());
  }

  /**
   * Sets validWriteIds of this table.
   * @param writeIdList If the writeIdList is {@link MutableValidWriteIdList}, it is set
   *                    as the original instance. Otherwise, a new instance is created.
   */
  public void setValidWriteIds(ValidWriteIdList writeIdList) {
    if (writeIdList != null) {
      validWriteIds_ = new MutableValidReaderWriteIdList(writeIdList);
    } else {
      validWriteIds_ = null;
    }
  }

  /**
   * Add write ids to the validWriteIdList of this table.
   * @param writeIds a list of write ids
   * @param status the status of the writeIds argument
   * @return True if any of writeIds is added and false otherwise
   */
  public boolean addWriteIds(List<Long> writeIds,
      MutableValidWriteIdList.WriteIdStatus status) throws CatalogException {
    Preconditions.checkState(isWriteLockedByCurrentThread(), "Write Lock should be held "
        + "before addWriteIds.");
    Preconditions.checkArgument(writeIds != null, "Cannot add null write ids");
    Preconditions.checkState(validWriteIds_ != null, "Write id list should not be null");
    switch (status) {
      case OPEN:
        return validWriteIds_.addOpenWriteId(Collections.max(writeIds));
      case COMMITTED:
        return validWriteIds_.addCommittedWriteIds(writeIds);
      case ABORTED:
        return validWriteIds_.addAbortedWriteIds(writeIds);
      default:
        throw new CatalogException("Unknown write id status " + status + " for table "
            + getFullName());
    }
  }

  public long getLastCompactionId() {
    return lastCompactionId_;
  }

  /**
   * Updates the pending version of this table if the tbl version matches with the
   * expectedTblVersion.
   * @return true if the pending version was updated. Else, false.
   */
  public boolean updatePendingVersion(long expectedTblVersion, long newPendingVersion) {
    synchronized (pendingVersionLock_) {
      if (expectedTblVersion == getCatalogVersion()) {
        pendingVersionNumber_ = newPendingVersion;
        return true;
      }
      return false;
    }
  }

  /**
   * Sets the version of this table. This makes sure that if there is a
   * pendingVersionNumber which is higher than the given version, it uses the
   * pendingVersionNumber. A pendingVersionNumber which is higher than given version
   * represents that the topic update thread tried to add this table to a update but
   * couldn't. Hence it needs the ongoing update operation (represented by the given
   * version) to use a higher version number so that this table falls within the next
   * topic update window.
   */
  @Override
  public void setCatalogVersion(long version) {
    synchronized (pendingVersionLock_) {
      long versionToBeSet = version;
      if (pendingVersionNumber_ > version) {
        LOG.trace("Pending table version {} is higher than requested version {}",
            pendingVersionNumber_, version);
        versionToBeSet = pendingVersionNumber_;
      }
      LOG.trace("Setting the hdfs table {} version {}", getFullName(), versionToBeSet);
      super.setCatalogVersion(versionToBeSet);
    }
  }

  /**
   * Returns the last version of this table which was seen by the topic update thread
   * when it could not acquire the table lock. This is used to determine if the topic
   * update thread has skipped this table enough number of times that we should now
   * block the topic updates until we add this table. Note that
   * this method is not thread-safe and assumes that this only called from the
   * topic-update thread.
   */
  public long getLastVersionSeenByTopicUpdate() {
    return lastVersionSeenByTopicUpdate_;
  }

  /**
   * Sets the version as seen by the topic update thread if it skips the table. Note that
   * this method is not thread-safe and assumes that this only called from the
   * topic-update thread.
   */
  public void setLastVersionSeenByTopicUpdate(long version) {
    lastVersionSeenByTopicUpdate_ = version;
  }

  public boolean isParquetTable() {
    for (FeFsPartition partition: partitionMap_.values()) {
      if (!partition.getFileFormat().isParquetBased()) {
        return false;
      }
    }
    return true;
  }
}
