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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.AvroSchemaConverter;
import org.apache.impala.util.AvroSchemaUtils;
import org.apache.impala.util.FsPermissionCache;
import org.apache.impala.util.FsPermissionChecker;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.TAccessLevelUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

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
 */
public class HdfsTable extends Table implements FeFsTable {
  // Name of default partition for unpartitioned tables
  private static final String DEFAULT_PARTITION_NAME = "";

  // Number of times to retry fetching the partitions from the HMS should an error occur.
  private final static int NUM_PARTITION_FETCH_RETRIES = 5;

  // Table property key for skip.header.line.count
  public static final String TBL_PROP_SKIP_HEADER_LINE_COUNT = "skip.header.line.count";

  // Table property key for overriding the Impalad-wide --enable_stats_extrapolation
  // setting for a specific table. By default, tables do not have the property set and
  // rely on the Impalad-wide --enable_stats_extrapolation flag.
  public static final String TBL_PROP_ENABLE_STATS_EXTRAPOLATION =
      "impala.enable.stats.extrapolation";

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
  private final List<TreeMap<LiteralExpr, Set<Long>>> partitionValuesMap_ =
      new ArrayList<>();

  // Array of partition id sets that correspond to partitions with null values
  // in the partition keys; one set per partition key. It is not populated if the table is
  // stored in the catalog server.
  private final List<Set<Long>> nullPartitionIds_ = new ArrayList<>();

  // Map of partition ids to HdfsPartitions.
  private final Map<Long, HdfsPartition> partitionMap_ = new HashMap<>();

  // Map of partition name to HdfsPartition object. Used for speeding up
  // table metadata loading. It is only populated if this table object is stored in
  // catalog server.
  private final Map<String, HdfsPartition> nameToPartitionMap_ = new HashMap<>();

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

  private HdfsPartitionLocationCompressor partitionLocationCompressor_;

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

  // Represents a set of storage-related statistics aggregated at the table or partition
  // level.
  public final static class FileMetadataStats {
    // Nuber of files in a table/partition.
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

  // Caching this configuration object makes calls to getFileSystem much quicker
  // (saves ~50ms on a standard plan)
  // TODO(henry): confirm that this is thread safe - cursory inspection of the class
  // and its usage in getFileSystem suggests it should be.
  private static final Configuration CONF = new Configuration();

  public HdfsTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(msTbl, db, name, owner);
    partitionLocationCompressor_ =
        new HdfsPartitionLocationCompressor(numClusteringCols_);
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
   * Gets the HdfsPartition matching the given partition spec. Returns null if no match
   * was found.
   */
  public HdfsPartition getPartition(List<PartitionKeyValue> partitionSpec) {
    return (HdfsPartition)getPartition(this, partitionSpec);
  }

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
   * Clear the partitions of an HdfsTable and the associated metadata.
   */
  private void resetPartitions() {
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
   * under it.
   *
   * If there are no partitions in the Hive metadata, a single partition is added with no
   * partition keys.
   */
  private void loadAllPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws IOException,
      CatalogException {
    Preconditions.checkNotNull(msTbl);
    initializePartitionMetadata(msTbl);
    FsPermissionCache permCache = preloadPermissionsCache(msPartitions);

    Path tblLocation = FileSystemUtil.createFullyQualifiedPath(getHdfsBaseDirPath());
    accessLevel_ = getAvailableAccessLevel(getFullName(), tblLocation, permCache);

    if (msTbl.getPartitionKeysSize() == 0) {
      Preconditions.checkArgument(msPartitions == null || msPartitions.isEmpty());
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      HdfsPartition part = createPartition(msTbl.getSd(), null, permCache);
      if (isMarkedCached_) part.markCached();
      addPartition(part);
    } else {
      for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
        HdfsPartition partition = createPartition(msPartition.getSd(), msPartition,
            permCache);
        addPartition(partition);
        // If the partition is null, its HDFS path does not exist, and it was not added
        // to this table's partition list. Skip the partition.
        if (partition == null) continue;
      }
    }
    // Load the file metadata from scratch.
    loadFileMetadataForPartitions(partitionMap_.values(), /*isRefresh=*/false);
  }


  /**
   * Helper method to load the block locations for each partition in 'parts'.
   * New file descriptor lists are loaded and the partitions are updated in place.
   *
   * @param isRefresh whether this is a refresh operation or an initial load. This only
   * affects logging.
   */
  private void loadFileMetadataForPartitions(Iterable<HdfsPartition> parts,
      boolean isRefresh) throws CatalogException {
    // Group the partitions by their path (multiple partitions may point to the same
    // path).
    Map<Path, List<HdfsPartition>> partsByPath = Maps.newHashMap();
    for (HdfsPartition p : parts) {
      Path partPath = FileSystemUtil.createFullyQualifiedPath(new Path(p.getLocation()));
      partsByPath.computeIfAbsent(partPath, (path) -> new ArrayList<HdfsPartition>())
          .add(p);
    }

    // Create a FileMetadataLoader for each path.
    Map<Path, FileMetadataLoader> loadersByPath = Maps.newHashMap();
    for (Map.Entry<Path, List<HdfsPartition>> e : partsByPath.entrySet()) {
      List<FileDescriptor> oldFds = e.getValue().get(0).getFileDescriptors();
      FileMetadataLoader loader = new FileMetadataLoader(e.getKey(), /*recursive=*/false,
          oldFds, hostIndex_);
      // If there is a cached partition mapped to this path, we recompute the block
      // locations even if the underlying files have not changed.
      // This is done to keep the cached block metadata up to date.
      boolean hasCachedPartition = Iterables.any(e.getValue(),
          HdfsPartition::isMarkedCached);
      loader.setForceRefreshBlockLocations(hasCachedPartition);
      loadersByPath.put(e.getKey(), loader);
    }

    String logPrefix = String.format(
        "%s file and block metadata for %s paths for table %s",
        isRefresh ? "Refreshing" : "Loading", partsByPath.size(),
        getFullName());
    FileSystem tableFs;
    try {
      tableFs = (new Path(getLocation())).getFileSystem(CONF);
    } catch (IOException e) {
      throw new CatalogException("Invalid table path for table: " + getFullName(), e);
    }

    // Actually load the partitions.
    // TODO(IMPALA-8406): if this fails to load files from one or more partitions, then
    // we'll throw an exception here and end up bailing out of whatever catalog operation
    // we're in the middle of. This could cause a partial metadata update -- eg we may
    // have refreshed the top-level table properties without refreshing the files.
    new ParallelFileMetadataLoader(logPrefix, tableFs, loadersByPath.values())
        .load();

    // Store the loaded FDs into the partitions.
    for (Map.Entry<Path, List<HdfsPartition>> e : partsByPath.entrySet()) {
      Path p = e.getKey();
      FileMetadataLoader loader = loadersByPath.get(p);

      for (HdfsPartition part : e.getValue()) {
        part.setFileDescriptors(loader.getLoadedFds());
      }
    }

    // TODO(todd): would be good to log a summary of the loading process:
    // - how long did it take?
    // - how many block locations did we reuse/load individually/load via batch
    // - how many partitions did we read metadata for
    // - etc...
    LOG.info("Loaded file and block metadata for {}", getFullName());
  }

  /**
   * Gets the AccessLevel that is available for Impala for this table based on the
   * permissions Impala has on the given path. If the path does not exist, recurses up
   * the path until a existing parent directory is found, and inherit access permissions
   * from that.
   * Always returns READ_WRITE for S3 and ADLS files.
   */
  private static TAccessLevel getAvailableAccessLevel(String tableName,
      Path location, FsPermissionCache permCache) throws IOException {
    Preconditions.checkNotNull(location);
    FileSystem fs = location.getFileSystem(CONF);
    // Avoid calling getPermissions() on file path for S3 files, as that makes a round
    // trip to S3. Also, the S3A connector is currently unable to manage S3 permissions,
    // so for now it is safe to assume that all files(objects) have READ_WRITE
    // permissions, as that's what the S3A connector will always return too.
    // TODO: Revisit if the S3A connector is updated to be able to manage S3 object
    // permissions. (see HADOOP-13892)
    if (FileSystemUtil.isS3AFileSystem(fs)) return TAccessLevel.READ_WRITE;

    // The ADLS connector currently returns ACLs for files in ADLS, but can only map
    // them to the ADLS client SPI and not the Hadoop users/groups, causing unexpected
    // behavior. So ADLS ACLs are unsupported until the connector is able to map
    // permissions to hadoop users/groups (HADOOP-14437).
    if (FileSystemUtil.isADLFileSystem(fs)) return TAccessLevel.READ_WRITE;
    if (FileSystemUtil.isABFSFileSystem(fs)) return TAccessLevel.READ_WRITE;

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
   * Creates new HdfsPartition objects to be added to HdfsTable's partition list.
   * Partitions may be empty, or may not even exist in the filesystem (a partition's
   * location may have been changed to a new path that is about to be created by an
   * INSERT). Also loads the file metadata for this partition. Returns new partition
   * if successful or null if none was created.
   *
   * Throws CatalogException if one of the supplied storage descriptors contains metadata
   * that Impala can't understand.
   */
  public List<HdfsPartition> createAndLoadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions)
      throws CatalogException {
    List<HdfsPartition> addedParts = new ArrayList<>();
    FsPermissionCache permCache = preloadPermissionsCache(msPartitions);
    for (org.apache.hadoop.hive.metastore.api.Partition partition: msPartitions) {
      HdfsPartition hdfsPartition = createPartition(partition.getSd(), partition,
          permCache);
      Preconditions.checkNotNull(hdfsPartition);
      addedParts.add(hdfsPartition);
    }
    loadFileMetadataForPartitions(addedParts, /* isRefresh = */ false);
    return addedParts;
  }

  /**
   * Creates a new HdfsPartition from a specified StorageDescriptor and an HMS partition
   * object.
   */
  private HdfsPartition createPartition(StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      FsPermissionCache permCache) throws CatalogException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name_, storageDescriptor);
    List<LiteralExpr> keyValues;
    if (msPartition != null) {
      keyValues = FeCatalogUtils.parsePartitionKeyValues(this, msPartition.getValues());
    } else {
      keyValues = Collections.emptyList();
    }
    Path partDirPath = new Path(storageDescriptor.getLocation());
    try {
      if (msPartition != null) {
        HdfsCachingUtil.validateCacheParams(msPartition.getParameters());
      }
      TAccessLevel accessLevel = getAvailableAccessLevel(getFullName(), partDirPath,
          permCache);
      HdfsPartition partition =
          new HdfsPartition(this, msPartition, keyValues, fileFormatDescriptor,
          new ArrayList<FileDescriptor>(), accessLevel);
      partition.checkWellFormed();
      // Set the partition's #rows.
      if (msPartition != null && msPartition.getParameters() != null) {
         partition.setNumRows(FeCatalogUtils.getRowCount(msPartition.getParameters()));
      }
      if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
          // TODO: READ_ONLY isn't exactly correct because the it's possible the
          // partition does not have READ permissions either. When we start checking
          // whether we can READ from a table, this should be updated to set the
          // table's access level to the "lowest" effective level across all
          // partitions. That is, if one partition has READ_ONLY and another has
          // WRITE_ONLY the table's access level should be NONE.
          accessLevel_ = TAccessLevel.READ_ONLY;
      }
      return partition;
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
    if (partition.getFileFormat() == HdfsFileFormat.AVRO) hasAvroData_ = true;
    partitionMap_.put(partition.getId(), partition);
    fileMetadataStats_.totalFileBytes += partition.getSize();
    fileMetadataStats_.numFiles += partition.getNumFileDescriptors();
    updatePartitionMdAndColStats(partition);
  }

  /**
   * Updates the HdfsTable's partition metadata, i.e. adds the id to the HdfsTable and
   * populates structures used for speeding up partition pruning/lookup. Also updates
   * column stats.
   */
  private void updatePartitionMdAndColStats(HdfsPartition partition) {
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
      Set<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
      if (partitionIds == null) {
        partitionIds = new HashSet<>();
        partitionValuesMap_.get(i).put(literal, partitionIds);
        stats.setNumDistinctValues(stats.getNumDistinctValues() + 1);
      }
      partitionIds.add(Long.valueOf(partition.getId()));
    }
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
        HdfsCachingUtil.removePartitionCacheDirective(partition);
      } catch (ImpalaException e) {
        LOG.error("Unable to remove the cache directive on table " + getFullName() +
            ", partition " + partition.getPartitionName() + ": ", e);
      }
    }
    if (!isStoredInImpaladCatalogCache()) return partition;
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

  private HdfsPartition dropPartition(HdfsPartition partition) {
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
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    load(reuseMetadata, client, msTbl, true, true, null);
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
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean loadParitionFileMetadata, boolean loadTableSchema,
      Set<String> partitionsToUpdate) throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    String annotation = String.format("%s metadata for %s partition(s) of %s.%s",
        reuseMetadata ? "Reloading" : "Loading",
        partitionsToUpdate == null ? "all" : String.valueOf(partitionsToUpdate.size()),
        msTbl.getDbName(), msTbl.getTableName());
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation)) {
      // turn all exceptions into TableLoadingException
      msTable_ = msTbl;
      try {
        if (loadTableSchema) {
            // set nullPartitionKeyValue from the hive conf.
            nullPartitionKeyValue_ =
                MetaStoreUtil.getNullPartitionKeyValue(client).intern();
            loadSchema(msTbl);
            loadAllColumnStats(client);
        }
        // Load partition and file metadata
        if (reuseMetadata) {
          // Incrementally update this table's partitions and file metadata
          LOG.info("Incrementally loading table metadata for: " + getFullName());
          Preconditions.checkState(
              partitionsToUpdate == null || loadParitionFileMetadata);
          updateMdFromHmsTable(msTbl);
          if (msTbl.getPartitionKeysSize() == 0) {
            if (loadParitionFileMetadata) updateUnpartitionedTableFileMd();
          } else {
            updatePartitionsFromHms(
                client, partitionsToUpdate, loadParitionFileMetadata);
          }
          LOG.info("Incrementally loaded table metadata for: " + getFullName());
        } else {
          // Load all partitions from Hive Metastore, including file metadata.
          LOG.info("Fetching partition metadata from the Metastore: " + getFullName());
          List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions =
              MetaStoreUtil.fetchAllPartitions(
                  client, db_.getName(), name_, NUM_PARTITION_FETCH_RETRIES);
          LOG.info("Fetched partition metadata from the Metastore: " + getFullName());
          loadAllPartitions(msPartitions, msTbl);
        }
        if (loadTableSchema) setAvroSchema(client, msTbl);
        setTableStats(msTbl);
        fileMetadataStats_.unset();
        refreshLastUsedTime();
      } catch (TableLoadingException e) {
        throw e;
      } catch (Exception e) {
        throw new TableLoadingException("Failed to load metadata for table: "
            + getFullName(), e);
      }
    } finally {
      context.stop();
    }
  }

  /**
   * Updates the table metadata, including 'hdfsBaseDir_', 'isMarkedCached_',
   * and 'accessLevel_' from 'msTbl'. Throws an IOException if there was an error
   * accessing the table location path.
   */
  private void updateMdFromHmsTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws IOException {
    Preconditions.checkNotNull(msTbl);
    hdfsBaseDir_ = msTbl.getSd().getLocation();
    isMarkedCached_ = HdfsCachingUtil.validateCacheParams(msTbl.getParameters());
    Path location = new Path(hdfsBaseDir_);
    accessLevel_ = getAvailableAccessLevel(getFullName(), location,
        new FsPermissionCache());
    setMetaStoreTable(msTbl);
  }

  /**
   * Incrementally updates the file metadata of an unpartitioned HdfsTable.
   *
   * This is optimized for the case where few files have changed. See
   * {@link #refreshFileMetadata(Path, List)} above for details.
   */
  private void updateUnpartitionedTableFileMd() throws CatalogException {
    Preconditions.checkState(getNumClusteringCols() == 0);
    if (LOG.isTraceEnabled()) {
      LOG.trace("update unpartitioned table: " + getFullName());
    }
    HdfsPartition oldPartition = Iterables.getOnlyElement(partitionMap_.values());

    // Instead of updating the existing partition in place, we create a new one
    // so that we reflect any changes in the msTbl object and also assign a new
    // ID. This is one step towards eventually implementing IMPALA-7533.
    resetPartitions();
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    Preconditions.checkNotNull(msTbl);
    setPrototypePartition(msTbl.getSd());
    HdfsPartition part = createPartition(msTbl.getSd(), null, new FsPermissionCache());
    // Copy over the FDs from the old partition to the new one, so that
    // 'refreshPartitionFileMetadata' below can compare modification times and
    // reload the locations only for those that changed.
    part.setFileDescriptors(oldPartition.getFileDescriptors());
    addPartition(part);
    if (isMarkedCached_) part.markCached();
    loadFileMetadataForPartitions(ImmutableList.of(part), /*isRefresh=*/true);
  }

  /**
   * Updates the partitions of an HdfsTable so that they are in sync with the Hive
   * Metastore. It reloads partitions that were marked 'dirty' by doing a DROP + CREATE.
   * It removes from this table partitions that no longer exist in the Hive Metastore and
   * adds partitions that were added externally (e.g. using Hive) to the Hive Metastore
   * but do not exist in this table. If 'loadParitionFileMetadata' is true, it triggers
   * file/block metadata reload for the partitions specified in 'partitionsToUpdate', if
   * any, or for all the table partitions if 'partitionsToUpdate' is null.
   */
  private void updatePartitionsFromHms(IMetaStoreClient client,
      Set<String> partitionsToUpdate, boolean loadPartitionFileMetadata)
      throws Exception {
    if (LOG.isTraceEnabled()) LOG.trace("Sync table partitions: " + getFullName());
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    Preconditions.checkNotNull(msTbl);
    Preconditions.checkState(msTbl.getPartitionKeysSize() != 0);
    Preconditions.checkState(loadPartitionFileMetadata || partitionsToUpdate == null);

    // Retrieve all the partition names from the Hive Metastore. We need this to
    // identify the delta between partitions of the local HdfsTable and the table entry
    // in the Hive Metastore. Note: This is a relatively "cheap" operation
    // (~.3 secs for 30K partitions).
    Set<String> msPartitionNames = new HashSet<>();
    msPartitionNames.addAll(client.listPartitionNames(db_.getName(), name_, (short) -1));
    // Names of loaded partitions in this table
    Set<String> partitionNames = new HashSet<>();
    // Partitions for which file metadata must be loaded
    List<HdfsPartition> partitionsToLoadFiles = Lists.newArrayList();
    // Partitions that need to be dropped and recreated from scratch
    List<HdfsPartition> dirtyPartitions = new ArrayList<>();
    // Partitions removed from the Hive Metastore.
    List<HdfsPartition> removedPartitions = new ArrayList<>();
    // Identify dirty partitions that need to be loaded from the Hive Metastore and
    // partitions that no longer exist in the Hive Metastore.
    for (HdfsPartition partition: partitionMap_.values()) {
      // Remove partitions that don't exist in the Hive Metastore. These are partitions
      // that were removed from HMS using some external process, e.g. Hive.
      if (!msPartitionNames.contains(partition.getPartitionName())) {
        removedPartitions.add(partition);
      }
      if (partition.isDirty()) {
        // Dirty partitions are updated by removing them from table's partition
        // list and loading them from the Hive Metastore.
        dirtyPartitions.add(partition);
      } else {
        if (partitionsToUpdate == null && loadPartitionFileMetadata) {
          partitionsToLoadFiles.add(partition);
        }
      }
      Preconditions.checkNotNull(partition.getCachedMsPartitionDescriptor());
      partitionNames.add(partition.getPartitionName());
    }
    dropPartitions(removedPartitions);
    // dirtyPartitions are reloaded and hence cache directives are not dropped.
    dropPartitions(dirtyPartitions, false);
    // Load dirty partitions from Hive Metastore
    // TODO(todd): the logic around "dirty partitions" is highly suspicious.
    loadPartitionsFromMetastore(dirtyPartitions, client);

    // Identify and load partitions that were added in the Hive Metastore but don't
    // exist in this table.
    Set<String> newPartitionsInHms = Sets.difference(msPartitionNames, partitionNames);
    loadPartitionsFromMetastore(newPartitionsInHms, client);
    // If a list of modified partitions (old and new) is specified, don't reload file
    // metadata for the new ones as they have already been detected in HMS and have been
    // reloaded by loadPartitionsFromMetastore().
    if (partitionsToUpdate != null) {
      partitionsToUpdate.removeAll(newPartitionsInHms);
    }

    // Load file metadata. Until we have a notification mechanism for when a
    // file changes in hdfs, it is sometimes required to reload all the file
    // descriptors and block metadata of a table (e.g. REFRESH statement).
    if (loadPartitionFileMetadata) {
      if (partitionsToUpdate != null) {
        Preconditions.checkState(partitionsToLoadFiles.isEmpty());
        // Only reload file metadata of partitions specified in 'partitionsToUpdate'
        partitionsToLoadFiles = getPartitionsForNames(partitionsToUpdate);
      }
      loadFileMetadataForPartitions(partitionsToLoadFiles, /* isRefresh=*/true);
    }
  }

  /**
   * Given a set of partition names, returns the corresponding HdfsPartition
   * objects.
   */
  private List<HdfsPartition> getPartitionsForNames(Collection<String> partitionNames) {
    List<HdfsPartition> parts = Lists.newArrayListWithCapacity(partitionNames.size());
    for (String partitionName: partitionNames) {
      String partName = DEFAULT_PARTITION_NAME;
      if (partitionName.length() > 0) {
        // Trim the last trailing char '/' from each partition name
        partName = partitionName.substring(0, partitionName.length()-1);
      }
      HdfsPartition partition = nameToPartitionMap_.get(partName);
      Preconditions.checkNotNull(partition, "Invalid partition name: " + partName);
      parts.add(partition);
    }
    return parts;
  }

  @Override
  public void setTableStats(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    super.setTableStats(msTbl);
    // For unpartitioned tables set the numRows in its single partition
    // to the table's numRows.
    if (numClusteringCols_ == 0 && !partitionMap_.isEmpty()) {
      // Unpartitioned tables have a default partition.
      Preconditions.checkState(partitionMap_.size() == 1);
      for (HdfsPartition p: partitionMap_.values()) {
        p.setNumRows(getNumRows());
      }
    }
  }

  /**
   * Returns whether the table has the 'skip.header.line.count' property set.
   */
  private boolean hasSkipHeaderLineCount() {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    if (msTbl == null) return false;
    return msTbl.getParameters().containsKey(TBL_PROP_SKIP_HEADER_LINE_COUNT);
  }

  @Override // FeTable
  public int parseSkipHeaderLineCount(StringBuilder error) {
    if (!hasSkipHeaderLineCount()) return 0;
    return parseSkipHeaderLineCount(getMetaStoreTable().getParameters(), error);
  }

  /**
   * Parses and returns the value of the 'skip.header.line.count' table property. The
   * caller must ensure that the property is contained in the 'tblProperties' map. If
   * parsing fails or a value < 0 is found, the error parameter is updated to contain an
   * error message.
   */
  public static int parseSkipHeaderLineCount(Map<String, String> tblProperties,
      StringBuilder error) {
    Preconditions.checkState(tblProperties != null);
    Preconditions.checkState(tblProperties.containsKey(TBL_PROP_SKIP_HEADER_LINE_COUNT));
    // Try to parse.
    String string_value = tblProperties.get(TBL_PROP_SKIP_HEADER_LINE_COUNT);
    int skipHeaderLineCount = 0;
    String error_msg = String.format("Invalid value for table property %s: %s (value " +
        "must be an integer >= 0)", TBL_PROP_SKIP_HEADER_LINE_COUNT, string_value);
    try {
      skipHeaderLineCount = Integer.parseInt(string_value);
    } catch (NumberFormatException exc) {
      error.append(error_msg);
    }
    if (skipHeaderLineCount < 0) error.append(error_msg);
    return skipHeaderLineCount;
  }

  /**
   * Sets avroSchema_ if the table or any of the partitions in the table are stored
   * as Avro. Additionally, this method also reconciles the schema if the column
   * definitions from the metastore differ from the Avro schema.
   */
  private void setAvroSchema(IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws Exception {
    Preconditions.checkState(isSchemaLoaded_);
    String inputFormat = msTbl.getSd().getInputFormat();
    if (HdfsFileFormat.fromJavaClassName(inputFormat) == HdfsFileFormat.AVRO
        || hasAvroData_) {
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
        loadAllColumnStats(client);
      }
    }
  }

  /**
   * Loads table schema.
   */
  private void loadSchema(org.apache.hadoop.hive.metastore.api.Table msTbl)
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
    addColumnsFromFieldSchemas(nonPartFieldSchemas_);
    isSchemaLoaded_ = true;
  }

  /**
   * Loads partitions from the Hive Metastore and adds them to the internal list of
   * table partitions.
   */
  private void loadPartitionsFromMetastore(List<HdfsPartition> partitions,
      IMetaStoreClient client) throws Exception {
    Preconditions.checkNotNull(partitions);
    if (partitions.isEmpty()) return;
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Incrementally updating %d/%d partitions.",
          partitions.size(), partitionMap_.size()));
    }
    Set<String> partitionNames = new HashSet<>();
    for (HdfsPartition part: partitions) {
      partitionNames.add(part.getPartitionName());
    }
    loadPartitionsFromMetastore(partitionNames, client);
  }

  /**
   * Loads from the Hive Metastore the partitions that correspond to the specified
   * 'partitionNames' and adds them to the internal list of table partitions.
   */
  private void loadPartitionsFromMetastore(Set<String> partitionNames,
      IMetaStoreClient client) throws Exception {
    Preconditions.checkNotNull(partitionNames);
    if (partitionNames.isEmpty()) return;
    // Load partition metadata from Hive Metastore.
    List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions =
        new ArrayList<>();
    msPartitions.addAll(MetaStoreUtil.fetchPartitionsByName(client,
        Lists.newArrayList(partitionNames), db_.getName(), name_));

    FsPermissionCache permCache = preloadPermissionsCache(msPartitions);
    List<HdfsPartition> partitions = new ArrayList<>(msPartitions.size());
    for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
      HdfsPartition partition = createPartition(msPartition.getSd(), msPartition,
          permCache);
      // If the partition is null, its HDFS path does not exist, and it was not added to
      // this table's partition list. Skip the partition.
      if (partition == null) continue;
      partitions.add(partition);
    }
    loadFileMetadataForPartitions(partitions, /* isRefresh=*/false);
    for (HdfsPartition partition : partitions) addPartition(partition);
  }

  /**
   * For each of the partitions in 'msPartitions' with a location inside the table's
   * base directory, attempt to pre-cache the associated file permissions into the
   * returned cache. This takes advantage of the fact that many partition directories will
   * be in the same parent directories, and we can bulk fetch the permissions with a
   * single round trip to the filesystem instead of individually looking up each.
   */
  private FsPermissionCache preloadPermissionsCache(List<Partition> msPartitions) {
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
    if (msPartitions.size() < partitionMap_.size() * 3) return permCache;

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
        permCache.precacheChildrenOf(fs, p);
      } catch (IOException ioe) {
        // If we fail to pre-warm the cache we'll just wait for later when we
        // try to actually load the individual permissions, at which point
        // we can handle the issue accordingly.
        LOG.debug("Unable to bulk-load permissions for parent path: " + p, ioe);
      }
    }
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
    resetPartitions();
    try {
      for (Map.Entry<Long, THdfsPartition> part: hdfsTable.getPartitions().entrySet()) {
        HdfsPartition hdfsPart =
            HdfsPartition.fromThrift(this, part.getKey(), part.getValue());
        addPartition(hdfsPart);
      }
      prototypePartition_ = HdfsPartition.fromThrift(this,
          CatalogObjectsConstants.PROTOTYPE_PARTITION_ID,
          hdfsTable.prototype_partition);
    } catch (CatalogException e) {
      throw new TableLoadingException(e.getMessage());
    }
    avroSchema_ = hdfsTable.isSetAvroSchema() ? hdfsTable.getAvroSchema() : null;
    isMarkedCached_ =
        HdfsCachingUtil.validateCacheParams(getMetaStoreTable().getParameters());
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
  public TGetPartialCatalogObjectResponse getPartialInfo(
      TGetPartialCatalogObjectRequest req) throws TableLoadingException {
    TGetPartialCatalogObjectResponse resp = super.getPartialInfo(req);

    boolean wantPartitionInfo = req.table_info_selector.want_partition_files ||
        req.table_info_selector.want_partition_metadata ||
        req.table_info_selector.want_partition_names ||
        req.table_info_selector.want_partition_stats;

    Collection<Long> partIds = req.table_info_selector.partition_ids;
    if (partIds == null && wantPartitionInfo) {
      // Caller specified at least one piece of partition info but didn't specify
      // any partition IDs. That means they want the info for all partitions.
      partIds = partitionMap_.keySet();
    }

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
          partInfo.hms_partition = part.toHmsPartition();
          partInfo.setHas_incremental_stats(part.hasIncrementalStats());
        }

        if (req.table_info_selector.want_partition_files) {
          List<FileDescriptor> fds = part.getFileDescriptors();
          partInfo.file_descriptors = Lists.newArrayListWithCapacity(fds.size());
          for (FileDescriptor fd: fds) {
            partInfo.file_descriptors.add(fd.toThrift());
          }
        }

        if (req.table_info_selector.want_partition_stats) {
          partInfo.setPartition_stats(part.getPartitionStatsCompressed());
        }

        resp.table_info.partitions.add(partInfo);
      }
    }

    if (req.table_info_selector.want_partition_files) {
      // TODO(todd) we are sending the whole host index even if we returned only
      // one file -- maybe not so efficient, but the alternative is to do a bunch
      // of cloning of file descriptors which might increase memory pressure.
      resp.table_info.setNetwork_addresses(hostIndex_.getList());
    }
    return resp;
  }

  /**
   * Determines whether incremental stats should be sent from the catalogd to impalad.
   * Incremental stats will be sent if their size is less than the configured limit
   * (function of numPartitions) and they are sent via statestore (and not a direct
   * fetch from catalogd).
   */
  private boolean shouldSendIncrementalStats(int numPartitions) {
    long statsSizeEstimate =
        numPartitions * getColumns().size() * STATS_SIZE_PER_COLUMN_BYTES;
    return statsSizeEstimate < BackendConfig.INSTANCE.getIncStatsMaxSize()
        && !BackendConfig.INSTANCE.pullIncrementalStatistics();
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
  private THdfsTable getTHdfsTable(ThriftObjectType type, Set<Long> refPartitions) {
    if (type == ThriftObjectType.FULL) {
      // "full" implies all partitions should be included.
      Preconditions.checkArgument(refPartitions == null);
    }
    long memUsageEstimate = 0;
    int numPartitions =
        (refPartitions == null) ? partitionMap_.values().size() : refPartitions.size();
    memUsageEstimate += numPartitions * PER_PARTITION_MEM_USAGE_BYTES;
    boolean includeIncrementalStats = shouldSendIncrementalStats(numPartitions);
    FileMetadataStats stats = new FileMetadataStats();
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    for (HdfsPartition partition: partitionMap_.values()) {
      long id = partition.getId();
      if (refPartitions == null || refPartitions.contains(id)) {
        THdfsPartition tHdfsPartition = FeCatalogUtils.fsPartitionToThrift(
            partition, type, includeIncrementalStats);
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
          stats.totalFileBytes += tHdfsPartition.getTotal_file_size_bytes();
        }
        idToPartition.put(id, tHdfsPartition);
      }
    }
    if (type == ThriftObjectType.FULL) fileMetadataStats_.set(stats);

    THdfsPartition prototypePartition = FeCatalogUtils.fsPartitionToThrift(
        prototypePartition_, ThriftObjectType.DESCRIPTOR_ONLY, false);

    memUsageEstimate += fileMetadataStats_.numFiles * PER_FD_MEM_USAGE_BYTES +
        fileMetadataStats_.numBlocks * PER_BLOCK_MEM_USAGE_BYTES;
    setEstimatedMetadataSize(memUsageEstimate);
    setNumFiles(fileMetadataStats_.numFiles);
    THdfsTable hdfsTable = new THdfsTable(hdfsBaseDir_, getColumnNames(),
        nullPartitionKeyValue_, nullColumnValue_, idToPartition, prototypePartition);
    hdfsTable.setAvroSchema(avroSchema_);
    if (type == ThriftObjectType.FULL) {
      // Network addresses are used only by THdfsFileBlocks which are inside
      // THdfsFileDesc, so include network addreses only when including THdfsFileDesc.
      hdfsTable.setNetwork_addresses(hostIndex_.getList());
    }
    hdfsTable.setPartition_prefixes(partitionLocationCompressor_.getPrefixes());
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
   */
  public List<List<String>> getPathsWithoutPartitions() throws CatalogException {
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

    RemoteIterator<? extends FileStatus> statuses = FileSystemUtil.listStatus(fs, path,
        /*recursive=*/false);
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
   * Checks that the last component of 'path' is of the form "<partitionkey>=<v>"
   * where 'v' is a type-compatible value from the domain of the 'partitionKey' column.
   * If not, returns null, otherwise returns a Pair instance, the first element is the
   * original value, the second element is the LiteralExpr created from the original
   * value.
   */
  private Pair<String, LiteralExpr> getTypeCompatibleValue(Path path,
      String partitionKey) throws UnsupportedEncodingException {
    String partName[] = path.getName().split("=");
    if (partName.length != 2 || !partName[0].equals(partitionKey)) return null;

    // Check Type compatibility for Partition value.
    Column column = getColumn(partName[0]);
    Preconditions.checkNotNull(column);
    Type type = column.getType();
    LiteralExpr expr = null;
    // URL decode the partition value since it may contain encoded URL.
    String value = URLDecoder.decode(partName[1], StandardCharsets.UTF_8.name());
    if (!value.equals(getNullPartitionKeyValue())) {
      try {
        expr = LiteralExpr.create(value, type);
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
    return new Pair<String, LiteralExpr>(value, expr);
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

    // Pretty print partitions and their stats.
    List<FeFsPartition> orderedPartitions = new ArrayList<>(
        FeCatalogUtils.loadAllPartitions(table));
    Collections.sort(orderedPartitions, HdfsPartition.KV_COMPARATOR);

    long totalCachedBytes = 0L;
    long totalBytes = 0L;
    long totalNumFiles = 0L;
    for (FeFsPartition p: orderedPartitions) {
      int numFiles = p.getFileDescriptors().size();
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
      rowBuilder.add(p.getInputFormatDescriptor().getFileFormat().toString());
      rowBuilder.add(String.valueOf(p.hasIncrementalStats()));
      rowBuilder.add(p.getLocation());
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
          .addBytes(totalCachedBytes).add("").add("").add("").add("");
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
   * Reloads the metadata of partition 'oldPartition' by removing
   * it from the table and reconstructing it from the HMS partition object
   * 'hmsPartition'. If old partition is null then nothing is removed and
   * and partition constructed from 'hmsPartition' is simply added.
   */
  public void reloadPartition(HdfsPartition oldPartition, Partition hmsPartition)
      throws CatalogException {
    // Instead of updating the existing partition in place, we create a new one
    // so that we reflect any changes in the hmsPartition object and also assign a new
    // ID. This is one step towards eventually implementing IMPALA-7533.
    HdfsPartition refreshedPartition = createPartition(
        hmsPartition.getSd(), hmsPartition, new FsPermissionCache());
    Preconditions.checkArgument(oldPartition == null
        || HdfsPartition.KV_COMPARATOR.compare(oldPartition, refreshedPartition) == 0);
    if (oldPartition != null) {
      refreshedPartition.setFileDescriptors(oldPartition.getFileDescriptors());
    }
    loadFileMetadataForPartitions(ImmutableList.of(refreshedPartition),
        /*isRefresh=*/true);
    dropPartition(oldPartition, false);
    addPartition(refreshedPartition);
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
}
