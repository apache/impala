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
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.Reference;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.ImpalaInternalServiceConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.AvroSchemaConverter;
import org.apache.impala.util.AvroSchemaParser;
import org.apache.impala.util.AvroSchemaUtils;
import org.apache.impala.util.FsPermissionChecker;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.TAccessLevelUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
public class HdfsTable extends Table {
  // hive's default value for table property 'serialization.null.format'
  private static final String DEFAULT_NULL_COLUMN_VALUE = "\\N";

  // Name of default partition for unpartitioned tables
  private static final String DEFAULT_PARTITION_NAME = "";

  // Number of times to retry fetching the partitions from the HMS should an error occur.
  private final static int NUM_PARTITION_FETCH_RETRIES = 5;

  // Maximum number of errors logged when loading partitioned tables.
  private static final int MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG = 100;

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
  private final ArrayList<TreeMap<LiteralExpr, HashSet<Long>>> partitionValuesMap_ =
      Lists.newArrayList();

  // Array of partition id sets that correspond to partitions with null values
  // in the partition keys; one set per partition key. It is not populated if the table is
  // stored in the catalog server.
  private final ArrayList<HashSet<Long>> nullPartitionIds_ = Lists.newArrayList();

  // Map of partition ids to HdfsPartitions.
  private final HashMap<Long, HdfsPartition> partitionMap_ = Maps.newHashMap();

  // Map of partition name to HdfsPartition object. Used for speeding up
  // table metadata loading.
  private final HashMap<String, HdfsPartition> nameToPartitionMap_ = Maps.newHashMap();

  // Store all the partition ids of an HdfsTable.
  private final HashSet<Long> partitionIds_ = Sets.newHashSet();

  // Estimate (in bytes) of the incremental stats size per column per partition
  public static final long STATS_SIZE_PER_COLUMN_BYTES = 400;

  // Bi-directional map between an integer index and a unique datanode
  // TNetworkAddresses, each of which contains blocks of 1 or more
  // files in this table. The network addresses are stored using IP
  // address as the host name. Each FileBlock specifies a list of
  // indices within this hostIndex_ to specify which nodes contain
  // replicas of the block.
  private final ListMap<TNetworkAddress> hostIndex_ = new ListMap<TNetworkAddress>();

  // True iff this table has incremental stats in any of its partitions.
  private boolean hasIncrementalStats_ = false;

  // True iff the table's partitions are located on more than one filesystem.
  private boolean multipleFileSystems_ = false;

  private HdfsPartitionLocationCompressor partitionLocationCompressor_;

  // Base Hdfs directory where files of this table are stored.
  // For unpartitioned tables it is simply the path where all files live.
  // For partitioned tables it is the root directory
  // under which partition dirs are placed.
  protected String hdfsBaseDir_;

  // List of FieldSchemas that correspond to the non-partition columns. Used when
  // describing this table and its partitions to the HMS (e.g. as part of an alter table
  // operation), when only non-partition columns are required.
  private final List<FieldSchema> nonPartFieldSchemas_ = Lists.newArrayList();

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

  private static final int MAX_HDFS_PARTITIONS_PARALLEL_LOAD =
      BackendConfig.INSTANCE.maxHdfsPartsParallelLoad();

  private static final int MAX_NON_HDFS_PARTITIONS_PARALLEL_LOAD =
      BackendConfig.INSTANCE.maxNonHdfsPartsParallelLoad();

  // File/Block metadata loading stats for a single HDFS path.
  private class FileMetadataLoadStats {
    // Path corresponding to this metadata load request.
    private final Path hdfsPath;

    // Number of files for which the metadata was loaded.
    public int loadedFiles = 0;

    // Number of hidden files excluded from file metadata loading. More details at
    // isValidDataFile().
    public int hiddenFiles = 0;

    // Number of files skipped from file metadata loading because the files have not
    // changed since the last load. More details at hasFileChanged().
    public int skippedFiles = 0;

    // Number of unknown disk IDs encountered while loading block
    // metadata for this path.
    public long unknownDiskIds = 0;

    public FileMetadataLoadStats(Path path) { hdfsPath = path; }

    public String debugString() {
      Preconditions.checkNotNull(hdfsPath);
      return String.format("Path: %s: Loaded files: %s Hidden files: %s " +
          "Skipped files: %s Unknown diskIDs: %s", hdfsPath, loadedFiles, hiddenFiles,
          skippedFiles, unknownDiskIds);
    }
  }

  // A callable implementation of file metadata loading request for a given
  // HDFS path.
  public class FileMetadataLoadRequest
      implements Callable<FileMetadataLoadStats> {
    private final Path hdfsPath_;
    // All the partitions mapped to the above path
    private final List<HdfsPartition> partitionList_;
    // If set to true, reloads the file metadata only when the files in this path
    // have changed since last load (more details in hasFileChanged()).
    private final boolean reuseFileMd_;

    public FileMetadataLoadRequest(Path path, List<HdfsPartition> partitions,
       boolean reuseFileMd) {
      hdfsPath_ = path;
      partitionList_ = partitions;
      reuseFileMd_ = reuseFileMd;
    }

    @Override
    public FileMetadataLoadStats call() throws IOException {
      FileMetadataLoadStats loadingStats =
          reuseFileMd_ ? refreshFileMetadata(hdfsPath_, partitionList_) :
          resetAndLoadFileMetadata(hdfsPath_, partitionList_);
      return loadingStats;
    }

    public String debugString() {
      String loadType = reuseFileMd_? "Refreshed" : "Loaded";
      return String.format("%s file metadata for path: %s", loadType,
          hdfsPath_.toString());
    }
  }

  public HdfsTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(msTbl, db, name, owner);
    partitionLocationCompressor_ =
        new HdfsPartitionLocationCompressor(numClusteringCols_);
  }

  /**
   * Returns true if the table resides at a location which supports caching (e.g. HDFS).
   */
  public boolean isLocationCacheable() {
    return FileSystemUtil.isPathCacheable(new Path(getLocation()));
  }

  /**
   * Returns true if the table and all its partitions reside at locations which
   * support caching (e.g. HDFS).
   */
  public boolean isCacheable() {
    if (!isLocationCacheable()) return false;
    if (!isMarkedCached() && numClusteringCols_ > 0) {
      for (HdfsPartition partition: getPartitions()) {
        if (partition.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
          continue;
        }
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

  /**
   * Drops and re-loads the file metadata of all the partitions in 'partitions' that
   * map to the path 'partDir'. 'partDir' may belong to any file system that
   * implements the hadoop's FileSystem interface (like HDFS, S3, ADLS etc.). It involves
   * the following steps:
   * - Clear the current file metadata of the partitions.
   * - Call FileSystem.listFiles() on 'partDir' to fetch the FileStatus and BlockLocations
   *   for each file under it.
   * - For every valid data file, enumerate all its blocks and their corresponding hosts
   *   and disk IDs if the underlying file system supports the block locations API
   *   (for ex: HDFS). For other file systems (like S3) we synthesize the file metadata
   *   manually by splitting the file ranges into fixed size blocks.
   * For filesystems that don't support BlockLocation API, synthesize file blocks
   * by manually splitting the file range into fixed-size blocks.  That way, scan
   * ranges can be derived from file blocks as usual.  All synthesized blocks are given
   * an invalid network address so that the scheduler will treat them as remote.
   */
  private FileMetadataLoadStats resetAndLoadFileMetadata(
      Path partDir, List<HdfsPartition> partitions) throws IOException {
    FileMetadataLoadStats loadStats = new FileMetadataLoadStats(partDir);
    // No need to load blocks for empty partitions list.
    if (partitions == null || partitions.isEmpty()) return loadStats;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Loading block md for " + getFullName() + " path: " + partDir.toString());
    }

    FileSystem fs = partDir.getFileSystem(CONF);
    boolean synthesizeFileMd = !FileSystemUtil.supportsStorageIds(fs);
    RemoteIterator<LocatedFileStatus> fileStatusIter =
        FileSystemUtil.listFiles(fs, partDir, false);
    if (fileStatusIter == null) return loadStats;
    Reference<Long> numUnknownDiskIds = new Reference<Long>(Long.valueOf(0));
    List<FileDescriptor> newFileDescs = Lists.newArrayList();
    while (fileStatusIter.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIter.next();
      if (!FileSystemUtil.isValidDataFile(fileStatus)) {
        ++loadStats.hiddenFiles;
        continue;
      }
      FileDescriptor fd;
      // Block locations are manually synthesized if the underlying fs does not support
      // the block location API.
      if (synthesizeFileMd) {
        fd = FileDescriptor.createWithSynthesizedBlockMd(fileStatus,
            partitions.get(0).getFileFormat(), hostIndex_);
      } else {
        fd = FileDescriptor.create(fileStatus,
            fileStatus.getBlockLocations(), fs, hostIndex_, numUnknownDiskIds);
      }
      newFileDescs.add(fd);
      ++loadStats.loadedFiles;
    }
    for (HdfsPartition partition: partitions) partition.setFileDescriptors(newFileDescs);
    loadStats.unknownDiskIds += numUnknownDiskIds.getRef();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Loaded file metadata for " + getFullName() + " " +
          loadStats.debugString());
    }
    return loadStats;
  }

  /**
   * Refreshes file metadata information for 'path'. This method is optimized for
   * the case where the files in the partition have not changed dramatically. It first
   * uses a listStatus() call on the partition directory to detect the modified files
   * (look at hasFileChanged()) and fetches their block locations using the
   * getFileBlockLocations() method. Our benchmarks suggest that the listStatus() call
   * is much faster then the listFiles() (up to ~40x faster in some cases).
   */
  private FileMetadataLoadStats refreshFileMetadata(
      Path partDir, List<HdfsPartition> partitions) throws IOException {
    FileMetadataLoadStats loadStats = new FileMetadataLoadStats(partDir);
    // No need to load blocks for empty partitions list.
    if (partitions == null || partitions.isEmpty()) return loadStats;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Refreshing block md for " + getFullName() + " path: " +
          partDir.toString());
    }

    // Index the partition file descriptors by their file names for O(1) look ups.
    // We just pick the first partition to generate the fileDescByName lookup table
    // since all the partitions map to the same partDir.
    ImmutableMap<String, FileDescriptor> fileDescsByName = Maps.uniqueIndex(
          partitions.get(0).getFileDescriptors(), new Function<FileDescriptor, String>() {
            @Override
            public String apply(FileDescriptor desc) { return desc.getFileName(); }
          });

    FileSystem fs = partDir.getFileSystem(CONF);
    FileStatus[] fileStatuses = FileSystemUtil.listStatus(fs, partDir);
    if (fileStatuses == null) return loadStats;
    boolean synthesizeFileMd = !FileSystemUtil.supportsStorageIds(fs);
    Reference<Long> numUnknownDiskIds = new Reference<Long>(Long.valueOf(0));
    List<FileDescriptor> newFileDescs = Lists.newArrayList();
    HdfsFileFormat fileFormat = partitions.get(0).getFileFormat();
    // If there is a cached partition mapped to this path, we recompute the block
    // locations even if the underlying files have not changed (hasFileChanged()).
    // This is done to keep the cached block metadata up to date.
    boolean isPartitionMarkedCached = false;
    for (HdfsPartition partition: partitions) {
      if (partition.isMarkedCached()) {
        isPartitionMarkedCached = true;
        break;
      }
    }
    for (FileStatus fileStatus: fileStatuses) {
      if (!FileSystemUtil.isValidDataFile(fileStatus)) {
        ++loadStats.hiddenFiles;
        continue;
      }
      String fileName = fileStatus.getPath().getName().toString();
      FileDescriptor fd = fileDescsByName.get(fileName);
      if (isPartitionMarkedCached || hasFileChanged(fd, fileStatus)) {
        if (synthesizeFileMd) {
          fd = FileDescriptor.createWithSynthesizedBlockMd(fileStatus,
              fileFormat, hostIndex_);
        } else {
          BlockLocation[] locations =
            fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
          fd = FileDescriptor.create(fileStatus, locations, fs, hostIndex_,
              numUnknownDiskIds);
        }
        ++loadStats.loadedFiles;
      } else {
        ++loadStats.skippedFiles;
      }
      Preconditions.checkNotNull(fd);
      newFileDescs.add(fd);
    }
    loadStats.unknownDiskIds += numUnknownDiskIds.getRef();
    for (HdfsPartition partition: partitions) partition.setFileDescriptors(newFileDescs);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Refreshed file metadata for " + getFullName() + " "
          + loadStats.debugString());
    }
    return loadStats;
  }

  /**
   * Compares the modification time and file size between the FileDescriptor and the
   * FileStatus to determine if the file has changed. Returns true if the file has changed
   * and false otherwise.
   */
  private static boolean hasFileChanged(FileDescriptor fd, FileStatus status) {
    return (fd == null) || (fd.getFileLength() != status.getLen()) ||
      (fd.getModificationTime() != status.getModificationTime());
  }

  /**
   * Helper method to reload the file metadata of a single partition.
   */
  private void refreshPartitionFileMetadata(HdfsPartition partition)
      throws CatalogException {
    try {
      Path partDir = partition.getLocationPath();
      FileMetadataLoadStats stats = refreshFileMetadata(partDir,
          Lists.newArrayList(partition));
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Refreshed file metadata for %s %s",
              getFullName(), stats.debugString()));
      }
    } catch (IOException e) {
      throw new CatalogException("Encountered invalid partition path", e);
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }
  public boolean isMarkedCached() { return isMarkedCached_; }

  public Collection<HdfsPartition> getPartitions() { return partitionMap_.values(); }
  public Map<Long, HdfsPartition> getPartitionMap() { return partitionMap_; }
  public Set<Long> getNullPartitionIds(int i) { return nullPartitionIds_.get(i); }
  public HdfsPartitionLocationCompressor getPartitionLocationCompressor() {
    return partitionLocationCompressor_;
  }
  public Set<Long> getPartitionIds() { return partitionIds_; }
  public TreeMap<LiteralExpr, HashSet<Long>> getPartitionValueMap(int i) {
    return partitionValuesMap_.get(i);
  }

  /**
   * Returns the value Hive is configured to use for NULL partition key values.
   * Set during load.
   */
  public String getNullPartitionKeyValue() { return nullPartitionKeyValue_; }

  /*
   * Returns the storage location (HDFS path) of this table.
   */
  public String getLocation() {
    return super.getMetaStoreTable().getSd().getLocation();
  }

  List<FieldSchema> getNonPartitionFieldSchemas() { return nonPartFieldSchemas_; }

  // True if Impala has HDFS write permissions on the hdfsBaseDir (for an unpartitioned
  // table) or if Impala has write permissions on all partition directories (for
  // a partitioned table).
  public boolean hasWriteAccess() {
    return TAccessLevelUtil.impliesWriteAccess(accessLevel_);
  }

  /**
   * Returns the first location (HDFS path) that Impala does not have WRITE access
   * to, or an null if none is found. For an unpartitioned table, this just
   * checks the hdfsBaseDir. For a partitioned table it checks all partition directories.
   */
  public String getFirstLocationWithoutWriteAccess() {
    if (getMetaStoreTable() == null) return null;

    if (getMetaStoreTable().getPartitionKeysSize() == 0) {
      if (!TAccessLevelUtil.impliesWriteAccess(accessLevel_)) {
        return hdfsBaseDir_;
      }
    } else {
      for (HdfsPartition partition: partitionMap_.values()) {
        if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
          return partition.getLocation();
        }
      }
    }
    return null;
  }

  /**
   * Gets the HdfsPartition matching the given partition spec. Returns null if no match
   * was found.
   */
  public HdfsPartition getPartition(
      List<PartitionKeyValue> partitionSpec) {
    List<TPartitionKeyValue> partitionKeyValues = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec) {
      String value = PartitionKeyValue.getPartitionKeyValueString(
          kv.getLiteralValue(), getNullPartitionKeyValue());
      partitionKeyValues.add(new TPartitionKeyValue(kv.getColName(), value));
    }
    return getPartitionFromThriftPartitionSpec(partitionKeyValues);
  }

  /**
   * Gets the HdfsPartition matching the Thrift version of the partition spec.
   * Returns null if no match was found.
   */
  public HdfsPartition getPartitionFromThriftPartitionSpec(
      List<TPartitionKeyValue> partitionSpec) {
    // First, build a list of the partition values to search for in the same order they
    // are defined in the table.
    List<String> targetValues = Lists.newArrayList();
    Set<String> keys = Sets.newHashSet();
    for (FieldSchema fs: getMetaStoreTable().getPartitionKeys()) {
      for (TPartitionKeyValue kv: partitionSpec) {
        if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
          targetValues.add(kv.getValue());
          // Same key was specified twice
          if (!keys.add(kv.getName().toLowerCase())) {
            return null;
          }
        }
      }
    }

    // Make sure the number of values match up and that some values were found.
    if (targetValues.size() == 0 ||
        (targetValues.size() != getMetaStoreTable().getPartitionKeysSize())) {
      return null;
    }

    // Search through all the partitions and check if their partition key values
    // match the values being searched for.
    for (HdfsPartition partition: partitionMap_.values()) {
      if (partition.isDefaultPartition()) continue;
      List<LiteralExpr> partitionValues = partition.getPartitionValues();
      Preconditions.checkState(partitionValues.size() == targetValues.size());
      boolean matchFound = true;
      for (int i = 0; i < targetValues.size(); ++i) {
        String value;
        if (partitionValues.get(i) instanceof NullLiteral) {
          value = getNullPartitionKeyValue();
        } else {
          value = partitionValues.get(i).getStringValue();
          Preconditions.checkNotNull(value);
          // See IMPALA-252: we deliberately map empty strings on to
          // NULL when they're in partition columns. This is for
          // backwards compatibility with Hive, and is clearly broken.
          if (value.isEmpty()) value = getNullPartitionKeyValue();
        }
        if (!targetValues.get(i).equals(value)) {
          matchFound = false;
          break;
        }
      }
      if (matchFound) {
        return partition;
      }
    }
    return null;
  }

  /**
   * Gets hdfs partitions by the given partition set.
   */
  public List<HdfsPartition> getPartitionsFromPartitionSet(
      List<List<TPartitionKeyValue>> partitionSet) {
    List<HdfsPartition> partitions = Lists.newArrayList();
    for (List<TPartitionKeyValue> kv : partitionSet) {
      HdfsPartition partition =
          getPartitionFromThriftPartitionSpec(kv);
      if (partition != null) partitions.add(partition);
    }
    return partitions;
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
    partitionIds_.clear();
    partitionMap_.clear();
    nameToPartitionMap_.clear();
    partitionValuesMap_.clear();
    nullPartitionIds_.clear();
    if (isStoredInImpaladCatalogCache()) {
      // Initialize partitionValuesMap_ and nullPartitionIds_. Also reset column stats.
      for (int i = 0; i < numClusteringCols_; ++i) {
        getColumns().get(i).getStats().setNumNulls(0);
        getColumns().get(i).getStats().setNumDistinctValues(0);
        partitionValuesMap_.add(Maps.<LiteralExpr, HashSet<Long>>newTreeMap());
        nullPartitionIds_.add(Sets.<Long>newHashSet());
      }
    }
    fileMetadataStats_.init();
  }

  /**
   * Resets any partition metadata, creates the default partition and sets the base
   * table directory path as well as the caching info from the HMS table.
   */
  private void initializePartitionMetadata(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws CatalogException {
    Preconditions.checkNotNull(msTbl);
    resetPartitions();
    hdfsBaseDir_ = msTbl.getSd().getLocation();
    // INSERT statements need to refer to this if they try to write to new partitions
    // Scans don't refer to this because by definition all partitions they refer to
    // exist.
    addDefaultPartition(msTbl.getSd());

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
    // Map of partition paths to their corresponding HdfsPartition objects. Populated
    // using createPartition() calls. A single partition path can correspond to multiple
    // partitions.
    HashMap<Path, List<HdfsPartition>> partsByPath = Maps.newHashMap();

    if (msTbl.getPartitionKeysSize() == 0) {
      Preconditions.checkArgument(msPartitions == null || msPartitions.isEmpty());
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      Path tblLocation = FileSystemUtil.createFullyQualifiedPath(getHdfsBaseDirPath());
      HdfsPartition part = createPartition(msTbl.getSd(), null);
      partsByPath.put(tblLocation, Lists.newArrayList(part));
      if (isMarkedCached_) part.markCached();
      addPartition(part);
      FileSystem fs = tblLocation.getFileSystem(CONF);
      accessLevel_ = getAvailableAccessLevel(fs, tblLocation);
    } else {
      for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
        HdfsPartition partition = createPartition(msPartition.getSd(), msPartition);
        addPartition(partition);
        // If the partition is null, its HDFS path does not exist, and it was not added
        // to this table's partition list. Skip the partition.
        if (partition == null) continue;
        if (msPartition.getParameters() != null) {
          partition.setNumRows(getRowCount(msPartition.getParameters()));
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

        Path partDir = FileSystemUtil.createFullyQualifiedPath(
            new Path(msPartition.getSd().getLocation()));
        List<HdfsPartition> parts = partsByPath.get(partDir);
        if (parts == null) {
          partsByPath.put(partDir, Lists.newArrayList(partition));
        } else {
          parts.add(partition);
        }
      }
    }
    // Load the file metadata from scratch.
    loadMetadataAndDiskIds(partsByPath, false);
  }

  /**
   * Helper method to load the partition file metadata from scratch. This method is
   * optimized for loading newly added partitions. For refreshing existing partitions
   * use refreshPartitionFileMetadata(HdfsPartition).
   */
  private void resetAndLoadPartitionFileMetadata(HdfsPartition partition) {
    Path partDir = partition.getLocationPath();
    try {
      FileMetadataLoadStats stats =
          resetAndLoadFileMetadata(partDir, Lists.newArrayList(partition));
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Loaded file metadata for %s %s", getFullName(),
            stats.debugString()));
      }
    } catch (Exception e) {
        LOG.error("Error loading file metadata for path: " + partDir.toString() +
            ". Partitions file metadata could be partially loaded.", e);
    }
  }

  /**
   * Returns the thread pool size to load the file metadata of this table.
   * 'numPaths' is the number of paths for which the file metadata should be loaded.
   *
   * We use different thread pool sizes for HDFS and non-HDFS tables since the latter
   * supports much higher throughput of RPC calls for listStatus/listFiles. For
   * simplicity, the filesystem type is determined based on the table's root path and
   * not for each partition individually. Based on our experiments, S3 showed a linear
   * speed up (up to ~100x) with increasing number of loading threads where as the HDFS
   * throughput was limited to ~5x in un-secure clusters and up to ~3.7x in secure
   * clusters. We narrowed it down to scalability bottlenecks in HDFS RPC implementation
   * (HADOOP-14558) on both the server and the client side.
   */
  private int getLoadingThreadPoolSize(int numPaths) throws CatalogException {
    Preconditions.checkState(numPaths > 0);
    FileSystem tableFs;
    try {
      tableFs  = (new Path(getLocation())).getFileSystem(CONF);
    } catch (IOException e) {
      throw new CatalogException("Invalid table path for table: " + getFullName(), e);
    }
    int threadPoolSize = FileSystemUtil.supportsStorageIds(tableFs) ?
        MAX_HDFS_PARTITIONS_PARALLEL_LOAD : MAX_NON_HDFS_PARTITIONS_PARALLEL_LOAD;
    // Thread pool size need not exceed the number of paths to be loaded.
    return Math.min(numPaths, threadPoolSize);
  }

  /**
   * Helper method to load the block locations for each partition directory in
   * partsByPath using a thread pool. 'partsByPath' maps each partition directory to
   * the corresponding HdfsPartition objects. If 'reuseFileMd' is true, the block
   * metadata is incrementally refreshed, else it is reloaded from scratch.
   */
  private void loadMetadataAndDiskIds(Map<Path, List<HdfsPartition>> partsByPath,
      boolean reuseFileMd) throws CatalogException {
    int numPathsToLoad = partsByPath.size();
    // For tables without partitions we have no file metadata to load.
    if (numPathsToLoad == 0)  return;

    int threadPoolSize = getLoadingThreadPoolSize(numPathsToLoad);
    LOG.info(String.format("Loading file and block metadata for %s paths for table %s " +
        "using a thread pool of size %s", numPathsToLoad, getFullName(),
        threadPoolSize));
    ExecutorService partitionLoadingPool = Executors.newFixedThreadPool(threadPoolSize);
    try {
      List<Future<FileMetadataLoadStats>> pendingMdLoadTasks = Lists.newArrayList();
      for (Path p: partsByPath.keySet()) {
        FileMetadataLoadRequest blockMdLoadReq =
            new FileMetadataLoadRequest(p, partsByPath.get(p), reuseFileMd);
        pendingMdLoadTasks.add(partitionLoadingPool.submit(blockMdLoadReq));
      }
      // Wait for the partition load tasks to finish.
      int failedLoadTasks = 0;
      for (Future<FileMetadataLoadStats> task: pendingMdLoadTasks) {
        try {
          FileMetadataLoadStats loadStats = task.get();
          if (LOG.isTraceEnabled()) LOG.trace(loadStats.debugString());
        } catch (ExecutionException | InterruptedException e) {
          ++failedLoadTasks;
          if (failedLoadTasks <= MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG) {
            LOG.error("Encountered an error loading block metadata for table: " +
                getFullName(), e);
          }
        }
      }
      if (failedLoadTasks > 0) {
        int errorsNotLogged = failedLoadTasks - MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG;
        if (errorsNotLogged > 0) {
          LOG.error(String.format("Error loading file metadata for %s paths for table " +
              "%s. Only the first %s errors were logged.", failedLoadTasks, getFullName(),
              MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG));
        }
        throw new TableLoadingException(String.format("Failed to load file metadata "
            + "for %s paths for table %s. Table's file metadata could be partially "
            + "loaded. Check the Catalog server log for more details.", failedLoadTasks,
            getFullName()));
      }
    } finally {
      partitionLoadingPool.shutdown();
    }
    LOG.info(String.format("Loaded file and block metadata for %s", getFullName()));
  }

  /**
   * Gets the AccessLevel that is available for Impala for this table based on the
   * permissions Impala has on the given path. If the path does not exist, recurses up
   * the path until a existing parent directory is found, and inherit access permissions
   * from that.
   * Always returns READ_WRITE for S3 and ADLS files.
   */
  private TAccessLevel getAvailableAccessLevel(FileSystem fs, Path location)
      throws IOException {

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

    FsPermissionChecker permissionChecker = FsPermissionChecker.getInstance();
    while (location != null) {
      try {
        FsPermissionChecker.Permissions perms =
            permissionChecker.getPermissions(fs, location);
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
    Preconditions.checkNotNull(location, "Error: no path ancestor exists");
    return TAccessLevel.NONE;
  }

  /**
   * Creates a new HdfsPartition object to be added to HdfsTable's partition list.
   * Partitions may be empty, or may not even exist in the filesystem (a partition's
   * location may have been changed to a new path that is about to be created by an
   * INSERT). Also loads the file metadata for this partition. Returns new partition
   * if successful or null if none was created.
   *
   * Throws CatalogException if the supplied storage descriptor contains metadata that
   * Impala can't understand.
   */
  public HdfsPartition createAndLoadPartition(
      org.apache.hadoop.hive.metastore.api.Partition msPartition)
      throws CatalogException {
    HdfsPartition hdfsPartition = createPartition(msPartition.getSd(), msPartition);
    resetAndLoadPartitionFileMetadata(hdfsPartition);
    return hdfsPartition;
  }

  /**
   * Same as createAndLoadPartition() but is optimized for loading file metadata of
   * newly created HdfsPartitions in parallel.
   */
  public List<HdfsPartition> createAndLoadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions)
      throws CatalogException {
    HashMap<Path, List<HdfsPartition>> partsByPath = Maps.newHashMap();
    List<HdfsPartition> addedParts = Lists.newArrayList();
    for (org.apache.hadoop.hive.metastore.api.Partition partition: msPartitions) {
      HdfsPartition hdfsPartition = createPartition(partition.getSd(), partition);
      Preconditions.checkNotNull(hdfsPartition);
      addedParts.add(hdfsPartition);
      Path partitionPath = hdfsPartition.getLocationPath();
      List<HdfsPartition> hdfsPartitions = partsByPath.get(partitionPath);
      if (hdfsPartitions == null) {
        partsByPath.put(partitionPath, Lists.newArrayList(hdfsPartition));
      } else {
        hdfsPartitions.add(hdfsPartition);
      }
    }
    loadMetadataAndDiskIds(partsByPath, false);
    return addedParts;
  }


  /**
   * Creates a new HdfsPartition from a specified StorageDescriptor and an HMS partition
   * object.
   */
  private HdfsPartition createPartition(StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition)
      throws CatalogException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name_, storageDescriptor);
    List<LiteralExpr> keyValues = Lists.newArrayList();
    if (msPartition != null) {
      // Load key values
      for (String partitionKey: msPartition.getValues()) {
        Type type = getColumns().get(keyValues.size()).getType();
        // Deal with Hive's special NULL partition key.
        if (partitionKey.equals(nullPartitionKeyValue_)) {
          keyValues.add(NullLiteral.create(type));
        } else {
          try {
            keyValues.add(LiteralExpr.create(partitionKey, type));
          } catch (Exception ex) {
            LOG.warn("Failed to create literal expression of type: " + type, ex);
            throw new CatalogException("Invalid partition key value of type: " + type,
                ex);
          }
        }
      }
      for (Expr v: keyValues) v.analyzeNoThrow(null);
    }

    Path partDirPath = new Path(storageDescriptor.getLocation());
    try {
      FileSystem fs = partDirPath.getFileSystem(CONF);
      multipleFileSystems_ = multipleFileSystems_ ||
          !FileSystemUtil.isPathOnFileSystem(new Path(getLocation()), fs);
      if (msPartition != null) {
        HdfsCachingUtil.validateCacheParams(msPartition.getParameters());
      }
      HdfsPartition partition =
          new HdfsPartition(this, msPartition, keyValues, fileFormatDescriptor,
          new ArrayList<FileDescriptor>(), getAvailableAccessLevel(fs, partDirPath));
      partition.checkWellFormed();
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
    partitionIds_.add(partition.getId());
    nameToPartitionMap_.put(partition.getPartitionName(), partition);
    if (!isStoredInImpaladCatalogCache()) return;
    for (int i = 0; i < partition.getPartitionValues().size(); ++i) {
      ColumnStats stats = getColumns().get(i).getStats();
      LiteralExpr literal = partition.getPartitionValues().get(i);
      // Store partitions with null partition values separately
      if (literal instanceof NullLiteral) {
        stats.setNumNulls(stats.getNumNulls() + 1);
        if (nullPartitionIds_.get(i).isEmpty()) {
          stats.setNumDistinctValues(stats.getNumDistinctValues() + 1);
        }
        nullPartitionIds_.get(i).add(Long.valueOf(partition.getId()));
        continue;
      }
      HashSet<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
      if (partitionIds == null) {
        partitionIds = Sets.newHashSet();
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
   */
  private HdfsPartition dropPartition(HdfsPartition partition) {
    if (partition == null) return null;
    fileMetadataStats_.totalFileBytes -= partition.getSize();
    fileMetadataStats_.numFiles -= partition.getNumFileDescriptors();
    Preconditions.checkArgument(partition.getPartitionValues().size() ==
        numClusteringCols_);
    Long partitionId = partition.getId();
    // Remove the partition id from the list of partition ids and other mappings.
    partitionIds_.remove(partitionId);
    partitionMap_.remove(partitionId);
    nameToPartitionMap_.remove(partition.getPartitionName());
    if (!isStoredInImpaladCatalogCache()) return partition;
    for (int i = 0; i < partition.getPartitionValues().size(); ++i) {
      ColumnStats stats = getColumns().get(i).getStats();
      LiteralExpr literal = partition.getPartitionValues().get(i);
      // Check if this is a null literal.
      if (literal instanceof NullLiteral) {
        nullPartitionIds_.get(i).remove(partitionId);
        stats.setNumNulls(stats.getNumNulls() - 1);
        if (nullPartitionIds_.get(i).isEmpty()) {
          stats.setNumDistinctValues(stats.getNumDistinctValues() - 1);
        }
        continue;
      }
      HashSet<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
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

  /**
   * Drops the given partitions from this table. Cleans up its metadata from all the
   * mappings used to speed up partition pruning/lookup. Also updates partitions column
   * statistics. Returns the list of partitions that were dropped.
   */
  public List<HdfsPartition> dropPartitions(List<HdfsPartition> partitions) {
    ArrayList<HdfsPartition> droppedPartitions = Lists.newArrayList();
    for (HdfsPartition partition: partitions) {
      HdfsPartition hdfsPartition = dropPartition(partition);
      if (hdfsPartition != null) droppedPartitions.add(hdfsPartition);
    }
    return droppedPartitions;
  }

  /**
   * Adds or replaces the default partition.
   */
  public void addDefaultPartition(StorageDescriptor storageDescriptor)
      throws CatalogException {
    // Default partition has no files and is not referred to by scan nodes. Data sinks
    // refer to this to understand how to create new partitions.
    HdfsStorageDescriptor hdfsStorageDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name_, storageDescriptor);
    HdfsPartition partition = HdfsPartition.defaultPartition(this,
        hdfsStorageDescriptor);
    partitionMap_.put(partition.getId(), partition);
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
   * There are several cases where existing file descriptors might be reused incorrectly:
   * 1. an ALTER TABLE ADD PARTITION or dynamic partition insert is executed through
   *    Hive. This does not update the lastDdlTime.
   * 2. Hdfs rebalancer is executed. This changes the block locations but doesn't update
   *    the mtime (file modification time).
   * If any of these occur, user has to execute "invalidate metadata" to invalidate the
   * metadata cache of the table and trigger a fresh load.
   */
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean loadParitionFileMetadata, boolean loadTableSchema,
      Set<String> partitionsToUpdate) throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    try {
      // turn all exceptions into TableLoadingException
      msTable_ = msTbl;
      try {
        if (loadTableSchema) {
            // set nullPartitionKeyValue from the hive conf.
            nullPartitionKeyValue_ = client.getConfigValue(
                "hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__");
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
    if (msTbl.getPartitionKeysSize() == 0) {
      Path location = new Path(hdfsBaseDir_);
      FileSystem fs = location.getFileSystem(CONF);
      accessLevel_ = getAvailableAccessLevel(fs, location);
    }
    setMetaStoreTable(msTbl);
  }

  /**
   * Updates the file metadata of an unpartitioned HdfsTable.
   */
  private void updateUnpartitionedTableFileMd() throws Exception {
    if (LOG.isTraceEnabled()) {
      LOG.trace("update unpartitioned table: " + getFullName());
    }
    resetPartitions();
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    Preconditions.checkNotNull(msTbl);
    addDefaultPartition(msTbl.getSd());
    HdfsPartition part = createPartition(msTbl.getSd(), null);
    addPartition(part);
    if (isMarkedCached_) part.markCached();
    refreshPartitionFileMetadata(part);
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
      Set<String> partitionsToUpdate, boolean loadParitionFileMetadata)
      throws Exception {
    if (LOG.isTraceEnabled()) LOG.trace("Sync table partitions: " + getFullName());
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    Preconditions.checkNotNull(msTbl);
    Preconditions.checkState(msTbl.getPartitionKeysSize() != 0);
    Preconditions.checkState(loadParitionFileMetadata || partitionsToUpdate == null);

    // Retrieve all the partition names from the Hive Metastore. We need this to
    // identify the delta between partitions of the local HdfsTable and the table entry
    // in the Hive Metastore. Note: This is a relatively "cheap" operation
    // (~.3 secs for 30K partitions).
    Set<String> msPartitionNames = Sets.newHashSet();
    msPartitionNames.addAll(
        client.listPartitionNames(db_.getName(), name_, (short) -1));
    // Names of loaded partitions in this table
    Set<String> partitionNames = Sets.newHashSet();
    // Partitions for which file metadata must be loaded, grouped by partition paths.
    Map<Path, List<HdfsPartition>> partitionsToUpdateFileMdByPath = Maps.newHashMap();
    // Partitions that need to be dropped and recreated from scratch
    List<HdfsPartition> dirtyPartitions = Lists.newArrayList();
    // Partitions that need to be removed from this table. That includes dirty
    // partitions as well as partitions that were removed from the Hive Metastore.
    List<HdfsPartition> partitionsToRemove = Lists.newArrayList();
    // Identify dirty partitions that need to be loaded from the Hive Metastore and
    // partitions that no longer exist in the Hive Metastore.
    for (HdfsPartition partition: partitionMap_.values()) {
      // Ignore the default partition
      if (partition.isDefaultPartition()) continue;
      // Remove partitions that don't exist in the Hive Metastore. These are partitions
      // that were removed from HMS using some external process, e.g. Hive.
      if (!msPartitionNames.contains(partition.getPartitionName())) {
        partitionsToRemove.add(partition);
      }
      if (partition.isDirty()) {
        // Dirty partitions are updated by removing them from table's partition
        // list and loading them from the Hive Metastore.
        dirtyPartitions.add(partition);
      } else {
        if (partitionsToUpdate == null && loadParitionFileMetadata) {
          Path partitionPath = partition.getLocationPath();
          List<HdfsPartition> partitions =
            partitionsToUpdateFileMdByPath.get(partitionPath);
          if (partitions == null) {
            partitionsToUpdateFileMdByPath.put(
                partitionPath, Lists.newArrayList(partition));
          } else {
            partitions.add(partition);
          }
        }
      }
      Preconditions.checkNotNull(partition.getCachedMsPartitionDescriptor());
      partitionNames.add(partition.getPartitionName());
    }
    partitionsToRemove.addAll(dirtyPartitions);
    dropPartitions(partitionsToRemove);
    // Load dirty partitions from Hive Metastore
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
    if (loadParitionFileMetadata) {
      if (partitionsToUpdate != null) {
        // Only reload file metadata of partitions specified in 'partitionsToUpdate'
        Preconditions.checkState(partitionsToUpdateFileMdByPath.isEmpty());
        partitionsToUpdateFileMdByPath = getPartitionsByPath(partitionsToUpdate);
      }
      loadMetadataAndDiskIds(partitionsToUpdateFileMdByPath, true);
    }
  }

  /**
   * Given a set of partition names, returns the corresponding HdfsPartition
   * objects grouped by their base directory path.
   */
  private HashMap<Path, List<HdfsPartition>> getPartitionsByPath(
      Collection<String> partitionNames) {
    HashMap<Path, List<HdfsPartition>> partsByPath = Maps.newHashMap();
    for (String partitionName: partitionNames) {
      String partName = DEFAULT_PARTITION_NAME;
      if (partitionName.length() > 0) {
        // Trim the last trailing char '/' from each partition name
        partName = partitionName.substring(0, partitionName.length()-1);
      }
      HdfsPartition partition = nameToPartitionMap_.get(partName);
      Preconditions.checkNotNull(partition, "Invalid partition name: " + partName);
      Path partitionPath = partition.getLocationPath();
      List<HdfsPartition> partitions = partsByPath.get(partitionPath);
      if (partitions == null) {
        partsByPath.put(partitionPath, Lists.newArrayList(partition));
      } else {
        partitions.add(partition);
      }
    }
    return partsByPath;
  }

  @Override
  public void setTableStats(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    super.setTableStats(msTbl);
    // For unpartitioned tables set the numRows in its partitions
    // to the table's numRows.
    if (numClusteringCols_ == 0 && !partitionMap_.isEmpty()) {
      // Unpartitioned tables have a 'dummy' partition and a default partition.
      // Temp tables used in CTAS statements have one partition.
      Preconditions.checkState(partitionMap_.size() == 2 || partitionMap_.size() == 1);
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

  /**
   * Parses and returns the value of the 'skip.header.line.count' table property. If the
   * value is not set for the table, returns 0. If parsing fails or a value < 0 is found,
   * the error parameter is updated to contain an error message.
   */
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
      List<Map<String, String>> schemaSearchLocations = Lists.newArrayList();
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
        // Generate new FieldSchemas from the Avro schema. This step reconciles
        // differences in the column definitions and the Avro schema. For
        // Impala-created tables this step is not necessary because the same
        // resolution is done during table creation. But Hive-created tables
        // store the original column definitions, and not the reconciled ones.
        List<ColumnDef> colDefs =
            ColumnDef.createFromFieldSchemas(msTbl.getSd().getCols());
        List<ColumnDef> avroCols = AvroSchemaParser.parse(avroSchema_);
        StringBuilder warning = new StringBuilder();
        List<ColumnDef> reconciledColDefs =
            AvroSchemaUtils.reconcileSchemas(colDefs, avroCols, warning);
        if (warning.length() != 0) {
          LOG.warn(String.format("Warning while loading table %s:\n%s",
              getFullName(), warning.toString()));
        }
        AvroSchemaUtils.setFromSerdeComment(reconciledColDefs);
        // Reset and update nonPartFieldSchemas_ to the reconcicled colDefs.
        nonPartFieldSchemas_.clear();
        nonPartFieldSchemas_.addAll(ColumnDef.toFieldSchemas(reconciledColDefs));
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
    if (nullColumnValue_ == null) nullColumnValue_ = DEFAULT_NULL_COLUMN_VALUE;

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
    Set<String> partitionNames = Sets.newHashSet();
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
        Lists.newArrayList();
    msPartitions.addAll(MetaStoreUtil.fetchPartitionsByName(client,
        Lists.newArrayList(partitionNames), db_.getName(), name_));

    for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
      HdfsPartition partition = createPartition(msPartition.getSd(), msPartition);
      addPartition(partition);
      // If the partition is null, its HDFS path does not exist, and it was not added to
      // this table's partition list. Skip the partition.
      if (partition == null) continue;
      if (msPartition.getParameters() != null) {
        partition.setNumRows(getRowCount(msPartition.getParameters()));
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
      refreshPartitionFileMetadata(partition);
    }
  }

  @Override
  protected List<String> getColumnNamesWithHmsStats() {
    List<String> ret = Lists.newArrayList();
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
    multipleFileSystems_ = hdfsTable.multiple_filesystems;
    hostIndex_.populate(hdfsTable.getNetwork_addresses());
    resetPartitions();
    try {
      for (Map.Entry<Long, THdfsPartition> part: hdfsTable.getPartitions().entrySet()) {
        HdfsPartition hdfsPart =
            HdfsPartition.fromThrift(this, part.getKey(), part.getValue());
        addPartition(hdfsPart);
      }
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
    // Create thrift descriptors to send to the BE.  The BE does not
    // need any information below the THdfsPartition level.
    TTableDescriptor tableDesc = new TTableDescriptor(tableId, TTableType.HDFS_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    tableDesc.setHdfsTable(getTHdfsTable(false, referencedPartitions));
    return tableDesc;
  }

  @Override
  public TTable toThrift() {
    // Send all metadata between the catalog service and the FE.
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HDFS_TABLE);
    table.setHdfs_table(getTHdfsTable(true, null));
    return table;
  }

  /**
   * Create a THdfsTable corresponding to this HdfsTable. If includeFileDesc is true,
   * then then all partitions and THdfsFileDescs of each partition should be included.
   * Otherwise, don't include any THdfsFileDescs, and include only those partitions in
   * the refPartitions set (the backend doesn't need metadata for unreferenced
   * partitions). To prevent the catalog from hitting an OOM error while trying to
   * serialize large partition incremental stats, we estimate the stats size and filter
   * the incremental stats data from partition objects if the estimate exceeds
   * --inc_stats_size_limit_bytes. This function also collects storage related statistics
   *  (e.g. number of blocks, files, etc) in order to compute an estimate of the metadata
   *  size of this table.
   */
  private THdfsTable getTHdfsTable(boolean includeFileDesc, Set<Long> refPartitions) {
    // includeFileDesc implies all partitions should be included (refPartitions == null).
    Preconditions.checkState(!includeFileDesc || refPartitions == null);
    long memUsageEstimate = 0;
    int numPartitions =
        (refPartitions == null) ? partitionMap_.values().size() : refPartitions.size();
    memUsageEstimate += numPartitions * PER_PARTITION_MEM_USAGE_BYTES;
    long statsSizeEstimate =
        numPartitions * getColumns().size() * STATS_SIZE_PER_COLUMN_BYTES;
    boolean includeIncrementalStats =
        (statsSizeEstimate < BackendConfig.INSTANCE.getIncStatsMaxSize());
    FileMetadataStats stats = new FileMetadataStats();
    Map<Long, THdfsPartition> idToPartition = Maps.newHashMap();
    for (HdfsPartition partition: partitionMap_.values()) {
      long id = partition.getId();
      if (refPartitions == null || refPartitions.contains(id)) {
        THdfsPartition tHdfsPartition =
            partition.toThrift(includeFileDesc, includeIncrementalStats);
        if (tHdfsPartition.isSetHas_incremental_stats() &&
            tHdfsPartition.isHas_incremental_stats()) {
          memUsageEstimate += getColumns().size() * STATS_SIZE_PER_COLUMN_BYTES;
          hasIncrementalStats_ = true;
        }
        if (includeFileDesc) {
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
    if (includeFileDesc) fileMetadataStats_.set(stats);

    memUsageEstimate += fileMetadataStats_.numFiles * PER_FD_MEM_USAGE_BYTES +
        fileMetadataStats_.numBlocks * PER_BLOCK_MEM_USAGE_BYTES;
    setEstimatedMetadataSize(memUsageEstimate);
    THdfsTable hdfsTable = new THdfsTable(hdfsBaseDir_, getColumnNames(),
        nullPartitionKeyValue_, nullColumnValue_, idToPartition);
    hdfsTable.setAvroSchema(avroSchema_);
    hdfsTable.setMultiple_filesystems(multipleFileSystems_);
    if (includeFileDesc) {
      // Network addresses are used only by THdfsFileBlocks which are inside
      // THdfsFileDesc, so include network addreses only when including THdfsFileDesc.
      hdfsTable.setNetwork_addresses(hostIndex_.getList());
    }
    hdfsTable.setPartition_prefixes(partitionLocationCompressor_.getPrefixes());
    return hdfsTable;
  }

  public long getTotalHdfsBytes() { return fileMetadataStats_.totalFileBytes; }
  public String getHdfsBaseDir() { return hdfsBaseDir_; }
  public Path getHdfsBaseDirPath() { return new Path(hdfsBaseDir_); }
  public boolean isAvroTable() { return avroSchema_ != null; }

  /**
   * Get the index of hosts that store replicas of blocks of this table.
   */
  public ListMap<TNetworkAddress> getHostIndex() { return hostIndex_; }

  /**
   * Returns the file format that the majority of partitions are stored in.
   */
  public HdfsFileFormat getMajorityFormat() {
    Map<HdfsFileFormat, Integer> numPartitionsByFormat = Maps.newHashMap();
    for (HdfsPartition partition: partitionMap_.values()) {
      HdfsFileFormat format = partition.getInputFormatDescriptor().getFileFormat();
      Integer numPartitions = numPartitionsByFormat.get(format);
      if (numPartitions == null) {
        numPartitions = Integer.valueOf(1);
      } else {
        numPartitions = Integer.valueOf(numPartitions.intValue() + 1);
      }
      numPartitionsByFormat.put(format, numPartitions);
    }

    int maxNumPartitions = Integer.MIN_VALUE;
    HdfsFileFormat majorityFormat = null;
    for (Map.Entry<HdfsFileFormat, Integer> entry: numPartitionsByFormat.entrySet()) {
      if (entry.getValue().intValue() > maxNumPartitions) {
        majorityFormat = entry.getKey();
        maxNumPartitions = entry.getValue().intValue();
      }
    }
    Preconditions.checkNotNull(majorityFormat);
    return majorityFormat;
  }

  /**
   * Returns the HDFS paths corresponding to HdfsTable partitions that don't exist in
   * the Hive Metastore. An HDFS path is represented as a list of strings values, one per
   * partition key column.
   */
  public List<List<String>> getPathsWithoutPartitions() throws CatalogException {
    HashSet<List<LiteralExpr>> existingPartitions = new HashSet<List<LiteralExpr>>();
    // Get the list of partition values of existing partitions in Hive Metastore.
    for (HdfsPartition partition: partitionMap_.values()) {
      if (partition.isDefaultPartition()) continue;
      existingPartitions.add(partition.getPartitionValues());
    }

    List<String> partitionKeys = Lists.newArrayList();
    for (int i = 0; i < numClusteringCols_; ++i) {
      partitionKeys.add(getColumns().get(i).getName());
    }
    Path basePath = new Path(hdfsBaseDir_);
    List<List<String>> partitionsNotInHms = new ArrayList<List<String>>();
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
      HashSet<List<LiteralExpr>> existingPartitions,
      List<List<String>> partitionsNotInHms) throws IOException {
    FileSystem fs = path.getFileSystem(CONF);
    List<String> partitionValues = Lists.newArrayList();
    List<LiteralExpr> partitionExprs = Lists.newArrayList();
    getAllPartitionsNotInHms(path, partitionKeys, 0, fs, partitionValues,
        partitionExprs, existingPartitions, partitionsNotInHms);
  }

  /**
   * Returns all partitions which match the partition keys directory structure and pass
   * the type compatibility check.
   *
   * path e.g. c1=1/c2=2/c3=3
   * partitionKeys The ordered partition keys. e.g.("c1", "c2", "c3")
   * depth The start position in partitionKeys to match the path name.
   * partitionValues The partition values used to create a partition.
   * partitionExprs The list of LiteralExprs which is used to avoid duplicate partitions.
   * E.g. Having /c1=0001 and /c1=01, we should make sure only one partition
   * will be added.
   * existingPartitions All partitions which exist in Hive Metastore or newly added.
   * partitionsNotInHms Contains all the recovered partitions.
   */
  private void getAllPartitionsNotInHms(Path path, List<String> partitionKeys,
      int depth, FileSystem fs, List<String> partitionValues,
      List<LiteralExpr> partitionExprs, HashSet<List<LiteralExpr>> existingPartitions,
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

    FileStatus[] statuses = FileSystemUtil.listStatus(fs, path);
    if (statuses == null) return;
    for (FileStatus status: statuses) {
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
      String partitionKey) {
    String partName[] = path.getName().split("=");
    if (partName.length != 2 || !partName[0].equals(partitionKey)) return null;

    // Check Type compatibility for Partition value.
    Column column = getColumn(partName[0]);
    Preconditions.checkNotNull(column);
    Type type = column.getType();
    LiteralExpr expr = null;
    if (!partName[1].equals(getNullPartitionKeyValue())) {
      try {
        expr = LiteralExpr.create(partName[1], type);
        // Skip large value which exceeds the MAX VALUE of specified Type.
        if (expr instanceof NumericLiteral) {
          if (NumericLiteral.isOverflow(((NumericLiteral)expr).getValue(), type)) {
            LOG.warn(String.format("Skip the overflow value (%s) for Type (%s).",
                partName[1], type.toSql()));
            return null;
          }
        }
      } catch (Exception ex) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Invalid partition value (%s) for Type (%s).",
              partName[1], type.toSql()));
        }
        return null;
      }
    } else {
      expr = new NullLiteral();
    }
    return new Pair<String, LiteralExpr>(partName[1], expr);
  }

  /**
   * Returns an estimated row count for the given number of file bytes. The row count is
   * extrapolated using the table-level row count and file bytes statistics.
   * Returns zero only if the given file bytes is zero.
   * Returns -1 if:
   * - stats extrapolation has been disabled
   * - the given file bytes statistic is negative
   * - the row count or the file byte statistic is missing
   * - the file bytes statistic is zero or negative
   * - the row count statistic is zero and the file bytes is non-zero
   * Otherwise, returns a value >= 1.
   */
  public long getExtrapolatedNumRows(long fileBytes) {
    if (!isStatsExtrapolationEnabled()) return -1;
    if (fileBytes == 0) return 0;
    if (fileBytes < 0) return -1;
    if (tableStats_.num_rows < 0 || tableStats_.total_file_bytes <= 0) return -1;
    if (tableStats_.num_rows == 0 && tableStats_.total_file_bytes != 0) return -1;
    double rowsPerByte = tableStats_.num_rows / (double) tableStats_.total_file_bytes;
    double extrapolatedNumRows = fileBytes * rowsPerByte;
    return (long) Math.max(1, Math.round(extrapolatedNumRows));
  }

  /**
   * Returns true if stats extrapolation is enabled for this table, false otherwise.
   * Reconciles the Impalad-wide --enable_stats_extrapolation flag and the
   * TBL_PROP_ENABLE_STATS_EXTRAPOLATION table property
   */
  public boolean isStatsExtrapolationEnabled() {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    String propVal = msTbl.getParameters().get(TBL_PROP_ENABLE_STATS_EXTRAPOLATION);
    if (propVal == null) return BackendConfig.INSTANCE.isStatsExtrapolationEnabled();
    return Boolean.parseBoolean(propVal);
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);

    for (int i = 0; i < numClusteringCols_; ++i) {
      // Add the partition-key values as strings for simplicity.
      Column partCol = getColumns().get(i);
      TColumn colDesc = new TColumn(partCol.getName(), Type.STRING.toThrift());
      resultSchema.addToColumns(colDesc);
    }

    boolean statsExtrap = isStatsExtrapolationEnabled();

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
    ArrayList<HdfsPartition> orderedPartitions =
        Lists.newArrayList(partitionMap_.values());
    Collections.sort(orderedPartitions);

    long totalCachedBytes = 0L;
    for (HdfsPartition p: orderedPartitions) {
      // Ignore dummy default partition.
      if (p.isDefaultPartition()) continue;
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
      if (statsExtrap) rowBuilder.add(getExtrapolatedNumRows(p.getSize()));
      rowBuilder.add(p.getFileDescriptors().size()).addBytes(p.getSize());
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
            numClusteringCols_ == 0 ?
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
    if (numClusteringCols_ > 0) {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      int numEmptyCells = numClusteringCols_ - 1;
      rowBuilder.add("Total");
      for (int i = 0; i < numEmptyCells; ++i) {
        rowBuilder.add("");
      }

      // Total rows, extrapolated rows, files, bytes, cache stats.
      // Leave format empty.
      rowBuilder.add(getNumRows());
      // Compute and report the extrapolated row count because the set of files could
      // have changed since we last computed stats for this partition. We also follow
      // this policy during scan-cardinality estimation.
      if (statsExtrap) {
        rowBuilder.add(getExtrapolatedNumRows(fileMetadataStats_.totalFileBytes));
      }
      rowBuilder.add(fileMetadataStats_.numFiles)
          .addBytes(fileMetadataStats_.totalFileBytes)
          .addBytes(totalCachedBytes).add("").add("").add("").add("");
      result.addToRows(rowBuilder.get());
    }
    return result;
  }

  /**
   * Returns files info for the given dbname/tableName and partition spec.
   * Returns files info for all partitions, if partition spec is null, ordered
   * by partition.
   */
  public TResultSet getFiles(List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(new TColumn("Path", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Partition", Type.STRING.toThrift()));
    result.setRows(Lists.<TResultRow>newArrayList());

    List<HdfsPartition> orderedPartitions;
    if (partitionSet == null) {
      orderedPartitions = Lists.newArrayList(partitionMap_.values());
    } else {
      // Get a list of HdfsPartition objects for the given partition set.
      orderedPartitions = getPartitionsFromPartitionSet(partitionSet);
    }
    Collections.sort(orderedPartitions);

    for (HdfsPartition p: orderedPartitions) {
      List<FileDescriptor> orderedFds = Lists.newArrayList(p.getFileDescriptors());
      Collections.sort(orderedFds);
      for (FileDescriptor fd: orderedFds) {
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        rowBuilder.add(p.getLocation() + "/" + fd.getFileName());
        rowBuilder.add(PrintUtils.printBytes(fd.getFileLength()));
        rowBuilder.add(p.getPartitionName());
        result.addToRows(rowBuilder.get());
      }
    }
    return result;
  }

  /**
   * Constructs a partition name from a list of TPartitionKeyValue objects.
   */
  public static String constructPartitionName(List<TPartitionKeyValue> partitionSpec) {
    List<String> partitionCols = Lists.newArrayList();
    List<String> partitionVals = Lists.newArrayList();
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
    HdfsPartition refreshedPartition = createPartition(
        hmsPartition.getSd(), hmsPartition);
    refreshPartitionFileMetadata(refreshedPartition);
    Preconditions.checkArgument(oldPartition == null
        || oldPartition.compareTo(refreshedPartition) == 0);
    dropPartition(oldPartition);
    addPartition(refreshedPartition);
  }

  /**
   * Selects a random sample of files from the given list of partitions such that the sum
   * of file sizes is at least 'percentBytes' percent of the total number of bytes in
   * those partitions and at least 'minSampleBytes'. The sample is returned as a map from
   * partition id to a list of file descriptors selected from that partition.
   * This function allocates memory proportional to the number of files in 'inputParts'.
   * Its implementation tries to minimize the constant factor and object generation.
   * The given 'randomSeed' is used for random number generation.
   * The 'percentBytes' parameter must be between 0 and 100.
   */
  public Map<Long, List<FileDescriptor>> getFilesSample(
      Collection<HdfsPartition> inputParts, long percentBytes, long minSampleBytes,
      long randomSeed) {
    Preconditions.checkState(percentBytes >= 0 && percentBytes <= 100);
    Preconditions.checkState(minSampleBytes >= 0);

    // Conservative max size for Java arrays. The actual maximum varies
    // from JVM version and sometimes between configurations.
    final long JVM_MAX_ARRAY_SIZE = Integer.MAX_VALUE - 10;
    if (fileMetadataStats_.numFiles > JVM_MAX_ARRAY_SIZE) {
      throw new IllegalStateException(String.format(
          "Too many files to generate a table sample. " +
          "Table '%s' has %s files, but a maximum of %s files are supported.",
          getTableName().toString(), fileMetadataStats_.numFiles, JVM_MAX_ARRAY_SIZE));
    }
    int totalNumFiles = (int) fileMetadataStats_.numFiles;

    // Ensure a consistent ordering of files for repeatable runs. The files within a
    // partition are already ordered based on how they are loaded in the catalog.
    List<HdfsPartition> orderedParts = Lists.newArrayList(inputParts);
    Collections.sort(orderedParts);

    // fileIdxs contains indexes into the file descriptor lists of all inputParts
    // parts[i] contains the partition corresponding to fileIdxs[i]
    // fileIdxs[i] is an index into the file descriptor list of the partition parts[i]
    // Use max size to avoid looping over inputParts for the exact size.
    // The purpose of these arrays is to efficiently avoid selecting the same file
    // multiple times during the sampling, regardless of the sample percent. We purposely
    // avoid generating objects proportional to the number of files.
    int[] fileIdxs = new int[totalNumFiles];
    HdfsPartition[] parts = new HdfsPartition[totalNumFiles];
    int idx = 0;
    long totalBytes = 0;
    for (HdfsPartition part: orderedParts) {
      totalBytes += part.getSize();
      int numFds = part.getNumFileDescriptors();
      for (int fileIdx = 0; fileIdx < numFds; ++fileIdx) {
        fileIdxs[idx] = fileIdx;
        parts[idx] = part;
        ++idx;
      }
    }

    int numFilesRemaining = idx;
    double fracPercentBytes = (double) percentBytes / 100;
    long targetBytes = (long) Math.round(totalBytes * fracPercentBytes);
    targetBytes = Math.max(targetBytes, minSampleBytes);

    // Randomly select files until targetBytes has been reached or all files have been
    // selected.
    Random rnd = new Random(randomSeed);
    long selectedBytes = 0;
    Map<Long, List<FileDescriptor>> result = Maps.newHashMap();
    while (selectedBytes < targetBytes && numFilesRemaining > 0) {
      int selectedIdx = Math.abs(rnd.nextInt()) % numFilesRemaining;
      HdfsPartition part = parts[selectedIdx];
      Long partId = Long.valueOf(part.getId());
      List<FileDescriptor> sampleFileIdxs = result.get(partId);
      if (sampleFileIdxs == null) {
        sampleFileIdxs = Lists.newArrayList();
        result.put(partId, sampleFileIdxs);
      }
      FileDescriptor fd = part.getFileDescriptors().get(fileIdxs[selectedIdx]);
      sampleFileIdxs.add(fd);
      selectedBytes += fd.getFileLength();
      // Avoid selecting the same file multiple times.
      fileIdxs[selectedIdx] = fileIdxs[numFilesRemaining - 1];
      parts[selectedIdx] = parts[numFilesRemaining - 1];
      --numFilesRemaining;
    }
    return result;
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
        "hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__");
    tmpTable.loadSchema(msTbl);
    tmpTable.initializePartitionMetadata(msTbl);
    tmpTable.setTableStats(msTbl);
    return tmpTable;
  }
}
