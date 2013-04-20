// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.analysis.PartitionKeyValue;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.HdfsTableSink;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Internal representation of table-related metadata of an hdfs-resident table.
 * Owned by Catalog instance.
 * The partition keys constitute the clustering columns.
 *
 * This class is not thread-safe due to the static counter variable inside HdfsPartition.
 */
public class HdfsTable extends Table {
  // Hive uses this string for NULL partition keys. Set in load().
  private String nullPartitionKeyValue;

  private static boolean hasLoggedDiskIdFormatWarning = false;

  /**
   * Block metadata used for scheduling.
   */
  public static class BlockMetadata {
    private final String fileName;
    private final long fileSize; // total size of the file holding the block, in bytes
    private final long offset;
    private final long length;

    // result of BlockLocation.getNames(): list of (IP:port) hosting this block;
    // the strings recorded here point to what's in HdfsTable.uniqueHostPorts,
    // rather than allocating separate strings for each block, in order to save space
    private final String[] hostPorts;

    // hostPorts[i] stores this block on diskId[i]; the BE uses this information to
    // schedule scan ranges
    private int[] diskIds;

    public BlockMetadata(String fileName, long fileSize, BlockLocation blockLocation,
                         String[] hostPorts) {
      Preconditions.checkNotNull(blockLocation);
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.offset = blockLocation.getOffset();
      this.length = blockLocation.getLength();
      this.hostPorts = hostPorts;
    }

    public String getFileName() { return fileName; }
    public long getFileSize() { return fileSize; }
    public long getOffset() { return offset; }
    public long getLength() { return length; }
    public String[] getHostPorts() { return hostPorts; }

    public void setDiskIds(int[] diskIds) {
      Preconditions.checkArgument(diskIds.length == hostPorts.length);
      this.diskIds = diskIds;
    }

    /**
     * Return the disk id of the block in BlockLocation.getNames()[hostIndex]; -1 if
     * disk id is not supported.
     */
    public int getDiskId(int hostIndex) {
      if (diskIds == null) {
        return -1;
      }
      Preconditions.checkArgument(hostIndex >= 0);
      Preconditions.checkArgument(hostIndex < diskIds.length);
      return diskIds[hostIndex];
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("offset", offset)
          .add("length", length)
          .add("#disks", diskIds.length)
          .toString();
    }
  }

  /**
   * All of the block metadata for a single partition.
   *
   * Tries to be as compact as possible by maintaining central pools of unique String
   * objects (file names, hostports) that are referenced in BlockMetadata.
   */
  public static class PartitionBlockMetadata {
    private final HdfsPartition partition;

    private final ArrayList<BlockMetadata> blockMetadata = Lists.newArrayList();

    // unique file names across all blockMetadata
    // (this is really a set, but HashSet does not have a get() function; this maps
    // each entry to itself)
    private final HashMap<String, String> uniqueFileNames = Maps.newHashMap();

    private long totalStringLengths = 0;

    public PartitionBlockMetadata(HdfsPartition partition) {
      this.partition = partition;
    }

    /**
     * Add metadata for a single block and update uniqueHostPorts/-FileNames.
     */
    public void addBlock(String fileName, long fileSize, BlockLocation location,
        HashMap<String, String> uniqueHostPorts) {
      // update uniqueFileNames
      String recordedFileName = uniqueFileNames.get(fileName);
      if (recordedFileName == null) {
        uniqueFileNames.put(fileName, fileName);
        totalStringLengths += fileName.length();
        recordedFileName = fileName;
      }

      // update uniqueHostPorts
      String[] hostPorts;
      try {
        hostPorts = location.getNames();
      } catch (IOException e) {
        // this shouldn't happen, getNames() doesn't throw anything
        String errorMsg = "BlockLocation.getNames() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }
      String[] recordedHostPorts = new String[hostPorts.length];
      for (int i = 0; i < hostPorts.length; ++i) {
        recordedHostPorts[i] = uniqueHostPorts.get(hostPorts[i]);
        if (recordedHostPorts[i] == null) {
          uniqueHostPorts.put(hostPorts[i], hostPorts[i]);
          totalStringLengths += hostPorts[i].length();
          recordedHostPorts[i] = hostPorts[i];
        }
      }

      blockMetadata.add(new BlockMetadata(recordedFileName, fileSize, location,
          recordedHostPorts));
    }

    /**
     * Set diskIds for blockMetadata[blockIdx].
     */
    protected void setBlockDiskIds(int blockIdx, int[] diskIds) {
      Preconditions.checkArgument(blockIdx >= 0);
      Preconditions.checkArgument(blockIdx < blockMetadata.size());
      blockMetadata.get(blockIdx).setDiskIds(diskIds);
    }

    public ArrayList<BlockMetadata> getBlockMetadata() { return blockMetadata; }

    public HdfsPartition getPartition() { return partition; }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("#blocks", blockMetadata.size())
          .add("#filenames", uniqueFileNames.size())
          .add("totalStringLen", totalStringLengths)
          .toString();
    }
  }

  private final List<HdfsPartition> partitions; // these are only non-empty partitions

  // A set of the unique host/ports of datanodes that hold data for this table
  // (HashSet does not have a get() function, hence the map).
  // TODO: keep a per-database pool of unique hosts/ports?
  private final HashMap<String, String> uniqueHostPorts = Maps.newHashMap();

  private final HashMap<HdfsPartition, PartitionBlockMetadata> partitionBlockMdMap =
      Maps.newHashMap();

  // Base Hdfs directory where files of this table are stored.
  // For unpartitioned tables it is simply the path where all files live.
  // For partitioned tables it is the root directory
  // under which partition dirs are placed.
  protected String hdfsBaseDir;

  private final static Logger LOG = LoggerFactory.getLogger(HdfsTable.class);

  // Caching this configuration object makes calls to getFileSystem much quicker
  // (saves ~50ms on a standard plan)
  // TODO(henry): confirm that this is thread safe - cursory inspection of the class
  // and its usage in getFileSystem suggests it should be.
  private static final Configuration CONF = new Configuration();

  private static final DistributedFileSystem DFS;

  private static final boolean SUPPORTS_VOLUME_ID;

  static {
    try {
      // call newInstance() instead of using a shared instance from a cache
      // to avoid accidentally having it closed by someone else
      FileSystem fs = FileSystem.newInstance(FileSystem.getDefaultUri(CONF), CONF);
      if (!(fs instanceof DistributedFileSystem)) {
        String error = "Cannot connect to HDFS. " +
            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY +
            "(" + CONF.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + ")" +
            " might be set incorrectly";
        throw new RuntimeException(error);
      }
      DFS = (DistributedFileSystem)fs;
    } catch (IOException e) {
      throw new RuntimeException("couldn't retrieve FileSystem:\n" + e.getMessage(), e);
    }

    SUPPORTS_VOLUME_ID =
        CONF.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
                        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
  }

  /**
   *  Returns a disk id (0-based) index from the Hdfs VolumeId object.
   *  There is currently no public API to get at the volume id.  We'll have to get it
   *  by accessing the internals.
   */
  private static int getDiskId(VolumeId hdfsVolumeId) {
    // Initialize the diskId as -1 to indicate it is unknown
    int diskId = -1;

    if (hdfsVolumeId != null && hdfsVolumeId.isValid()) {
      // TODO: this is a hack and we'll have to address this by getting the
      // public API.  Also, we need to be very mindful of this when we change
      // the version of HDFS.
      String volumeIdString = hdfsVolumeId.toString();
      // This is the hacky part.  The toString is currently the underlying id
      // encoded in base64.
      byte[] volumeIdBytes = Base64.decodeBase64(volumeIdString);
      if (volumeIdBytes.length == 4) {
        diskId = Bytes.toInt(volumeIdBytes);
      } else if (!hasLoggedDiskIdFormatWarning) {
        LOG.warn("wrong disk id format: " + volumeIdString);
        hasLoggedDiskIdFormatWarning = true;
      }
    }
    return diskId;
  }

  /**
   * Populate partitionBlockMd based on 'partitions'.
   */
  private void loadPartitionBlockMd() throws RuntimeException {
    LOG.info("load partition block md for " + name);
    // Block locations for all the files in all the partitions.
    List<BlockLocation> blockLocations = Lists.newArrayList();

    // Partition block metadata for all the partitions
    List<PartitionBlockMetadata> partitionBlockMdList = Lists.newArrayList();

    for (HdfsPartition partition: partitions) {
      PartitionBlockMetadata partitionBlockMd = new PartitionBlockMetadata(partition);
      partitionBlockMdMap.put(partition, partitionBlockMd);

      // loop over all files and record their block metadata, minus volume ids
      for (FileDescriptor fileDescriptor: partition.getFileDescriptors()) {
        Path p = new Path(fileDescriptor.getFilePath());
        // Check to see if the file has a compression suffix.
        // We only support .lzo on text files that have been declared in
        // the metastore as TEXT_LZO.  For now, raise an error on any
        // other type.
        HdfsCompression compressionType =
            HdfsCompression.fromFileName(fileDescriptor.getFilePath());
        fileDescriptor.setCompression(compressionType);
        if (compressionType == HdfsCompression.LZO_INDEX) {
          // Skip index files, these are read by the LZO scanner directly.
          continue;
        }

        HdfsStorageDescriptor sd = partition.getInputFormatDescriptor();
        if (compressionType == HdfsCompression.LZO) {
          if (sd.getFileFormat() != HdfsFileFormat.LZO_TEXT) {
            throw new RuntimeException(
                "Compressed file not supported without compression input format: " + p);
          }
        } else if (sd.getFileFormat() == HdfsFileFormat.LZO_TEXT) {
          throw new RuntimeException("Expected file with .lzo suffix: " + p);
        } else if (sd.getFileFormat() == HdfsFileFormat.TEXT
                   && compressionType != HdfsCompression.NONE) {
          throw new RuntimeException("Compressed text files are not supported: " + p);
        }

        BlockLocation[] locations = null;
        try {
          FileStatus fileStatus = DFS.getFileStatus(p);
          // Ignore directories (and files in them) - if a directory is erroneously
          // created as a subdirectory of a partition dir we should ignore it and move on
          // (getFileBlockLocations will throw when
          // called on a directory). Hive will not recurse into directories.
          if (!fileStatus.isDirectory()) {
            locations = DFS.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
          }
          if (locations != null) {
            blockLocations.addAll(Arrays.asList(locations));
            for (int i = 0; i < locations.length; ++i) {
              partitionBlockMd.addBlock(fileDescriptor.getFilePath(),
                  fileDescriptor.getFileLength(), locations[i],
                  partition.getTable().uniqueHostPorts);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("couldn't determine block locations for path '"
              + p + "':\n" + e.getMessage(), e);
        }
      }
      partitionBlockMdList.add(partitionBlockMd);
      LOG.info("loaded partition " + partitionBlockMd.toString());
    }

    if (!SUPPORTS_VOLUME_ID) {
      return;
    }

    // BlockStorageLocations for all the blocks
    // block described by blockMetadataList[i] is located at locations[i]
    BlockStorageLocation[] locations = null;
    try {
      // Get the BlockStorageLocations for all the blocks
      locations = DFS.getFileBlockStorageLocations(blockLocations);
    } catch (IOException e) {
      LOG.error("Couldn't determine block storage locations:\n" + e.getMessage());
      return;
    }

    if (locations == null || locations.length == 0) {
      LOG.warn("Attempted to get block locations but the call returned nulls");
      return;
    }

    if (locations.length != blockLocations.size()) {
      // blocks and locations don't match up
      LOG.error("Number of block locations not equal to number of blocks: "
          + "#locations=" + Long.toString(locations.length)
          + " #blocks=" + Long.toString(blockLocations.size()));
      return;
    }

    int locationsIdx = 0;
    int unknownDiskIdCount = 0;
    for (PartitionBlockMetadata partitionBlockMd: partitionBlockMdList) {
      for (BlockMetadata blockMd: partitionBlockMd.getBlockMetadata()) {
        VolumeId[] volumeIds = locations[locationsIdx++].getVolumeIds();
        // Convert opaque VolumeId to 0 based ids.
        // TODO: the diskId should be eventually retrievable from Hdfs when
        // the community agrees this API is useful.
        int[] diskIds = new int[volumeIds.length];
        for (int i = 0; i < volumeIds.length; ++i) {
          diskIds[i] = getDiskId(volumeIds[i]);
          if (diskIds[i] < 0) {
            ++unknownDiskIdCount;
          }
        }
        blockMd.setDiskIds(diskIds);
      }
    }
    LOG.info("loaded disk ids for table " + getFullName());
    LOG.info(Integer.toString(getNumNodes()));
    if (unknownDiskIdCount > 0) {
      LOG.warn("unknown disk id count " + unknownDiskIdCount);
    }
  }

  /**
   * Returns true if the Hive table represents an Hdfs table that Impala understands,
   * by checking the input format for a known data format.
   */
  public static boolean isHdfsTable(org.apache.hadoop.hive.metastore.api.Table table) {
    StorageDescriptor sd = table.getSd();
    return HdfsFileFormat.isHdfsFormatClass(sd.getInputFormat());
  }

  protected HdfsTable(TableId id, org.apache.hadoop.hive.metastore.api.Table msTbl, Db db,
      String name, String owner) {
    super(id, msTbl, db, name, owner);
    this.partitions = Lists.newArrayList();
  }

  public List<HdfsPartition> getPartitions() {
    return partitions;
  }

  /**
   * Returns the value Hive is configured to use for NULL partition key values.
   * Set during load.
   */
  public String getNullPartitionKeyValue() {
    return nullPartitionKeyValue;
  }

  /**
   * Gets the HdfsPartition matching the given partition spec. Returns null if no match
   * was found.
   */
  public HdfsPartition getPartition(List<PartitionKeyValue> partitionSpec) {
    List<TPartitionKeyValue> partitionKeyValues = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec) {
      String value = kv.getPartitionKeyValueString(getNullPartitionKeyValue());
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
          targetValues.add(kv.getValue().toLowerCase());
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

    // Now search through all the partitions and check if their partition key values match
    // the values being searched for.
    for (HdfsPartition partition: getPartitions()) {
      if (partition.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
        continue;
      }
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
        if (!targetValues.get(i).equals(value.toLowerCase())) {
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

  public boolean isClusteringColumn(Column col) {
    return col.getPosition() < getNumClusteringCols();
  }

  /**
   * Create columns corresponding to fieldSchemas, including column statistics.
   * Throws a TableLoadingException if the metadata is incompatible with what we
   * support.
   */
  private void loadColumns(List<FieldSchema> fieldSchemas, HiveMetaStoreClient client)
      throws TableLoadingException {
    int pos = 0;
    for (FieldSchema s : fieldSchemas) {
      // catch currently unsupported hive schema elements
      if (!serdeConstants.PrimitiveTypes.contains(s.getType())) {
        throw new TableLoadingException(
            String.format("Failed to load metadata for table '%s' due to unsupported " +
                "column type '%s' in column '%s'", getName(), s.getType(), s.getName()));
      }
      PrimitiveType type = getPrimitiveType(s.getType());
      // Check if we support partitioning on columns of such a type.
      if (pos < numClusteringCols && !type.supportsTablePartitioning()) {
        throw new TableLoadingException(
            String.format("Failed to load metadata for table '%s' because of " +
                "unsupported partition-column type '%s' in partition column '%s'",
                getName(), type.toString(), s.getName()));
      }

      Column col = new Column(s.getName(), type, s.getComment(), pos);
      colsByPos.add(col);
      colsByName.put(s.getName(), col);
      ++pos;

      ColumnStatistics colStats = null;
      try {
        colStats = client.getTableColumnStatistics(db.getName(), name, s.getName());
      } catch (Exception e) {
        // don't try to load stats for this column
        continue;
      }

      // we should never see more than one ColumnStatisticsObj here
      if (colStats.getStatsObj().size() > 1) {
        continue;
      }
      col.updateStats(colStats.getStatsObj().get(0).getStatsData());
    }
  }

  /**
   * Create HdfsPartition objects corresponding to 'partitions'.
   *
   * If there are no partitions in the Hive metadata, a single partition is added with no
   * partition keys.
   */
  public void loadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws IOException, InvalidStorageDescriptorException {
    partitions.clear();
    hdfsBaseDir = msTbl.getSd().getLocation();

    // INSERT statements need to refer to this if they try to write to new partitions.
    // Scans don't refer to this because by definition all partitions they refer to
    // exist.
    addDefaultPartition(msTbl.getSd());

    if (msTbl.getPartitionKeysSize() == 0) {
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      addPartition(msTbl.getSd(), null, new ArrayList<LiteralExpr>());
    } else {
      // keep track of distinct partition key values and how many nulls there are
      Set<String>[] uniquePartitionKeys = new HashSet[numClusteringCols];
      long[] numNullKeys = new long[numClusteringCols];
      for (int i = 0; i < numClusteringCols; ++i) {
        uniquePartitionKeys[i] = new HashSet<String>();
        numNullKeys[i] = 0;
      }

      for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
        // load key values
        List<LiteralExpr> keyValues = Lists.newArrayList();
        int i = 0;
        for (String partitionKey: msPartition.getValues()) {
          uniquePartitionKeys[i].add(partitionKey);
          // Deal with Hive's special NULL partition key.
          if (partitionKey.equals(nullPartitionKeyValue)) {
            keyValues.add(new NullLiteral());
            ++numNullKeys[i];
          } else {
            PrimitiveType type = colsByPos.get(keyValues.size()).getType();
            try {
              Expr expr = LiteralExpr.create(partitionKey, type);
              // Force the literal to be of type declared in the metadata.
              expr = expr.castTo(type);
              keyValues.add((LiteralExpr)expr);
            } catch (AnalysisException ex) {
              LOG.warn("Failed to create literal expression of type: " + type, ex);
              throw new InvalidStorageDescriptorException(ex);
            }
          }
          ++i;
        }
        HdfsPartition partition =
            addPartition(msPartition.getSd(), msPartition, keyValues);

        if (partition != null && msPartition.getParameters() != null) {
          partition.setNumRows(getRowCount(msPartition.getParameters()));
          LOG.info("partition #rows=" + Long.toString(partition.getNumRows()));
        }
      }

      // update col stats for partition key cols
      for (int i = 0; i < numClusteringCols; ++i) {
        ColumnStats stats = colsByPos.get(i).getStats();
        stats.setNumNulls(numNullKeys[i]);
        stats.setNumDistinctValues(uniquePartitionKeys[i].size());
        // remove
        LOG.info("#col=" + Integer.toString(i) + " stats=" + stats.toString());
      }
    }

    loadPartitionBlockMd();
  }

  /**
   * Returns the value of the ROW_COUNT constant, or -1 if not found.
   */
  private long getRowCount(Map<String, String> parameters) {
    if (parameters == null) {
      return -1;
    }
    for (Map.Entry<String, String> e: parameters.entrySet()) {
      if (e.getKey().equals(StatsSetupConst.ROW_COUNT)) {
        try {
          long numRows = Long.valueOf(e.getValue());
          return numRows;
        } catch (NumberFormatException exc) {
          // ignore
        }
      }
    }
    return -1;
  }

  /**
   * Adds a new HdfsPartition to internal partition list, populating with file format
   * information and file locations. If a partition contains no files, it's not added.
   * Returns new partition or null, if none was added.
   *
   * @throws InvalidStorageDescriptorException if the supplied storage descriptor contains
   *         metadata that Impala can't understand.
   */
  private HdfsPartition addPartition(StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyExprs)
      throws IOException, InvalidStorageDescriptorException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name, storageDescriptor);
    Path path = new Path(storageDescriptor.getLocation());
    List<FileDescriptor> fileDescriptors = Lists.newArrayList();
    FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) {
      for (FileStatus fileStatus: fs.listStatus(path)) {
        String fileName = fileStatus.getPath().getName().toString();
        if (fileName.startsWith(".") || fileName.startsWith("_")) {
          // Ignore hidden file starting with . or _
          continue;
        }
        FileDescriptor fd = new FileDescriptor(fileStatus.getPath().toString(),
            fileStatus.getLen());
        fileDescriptors.add(fd);
      }

      HdfsPartition partition =
          new HdfsPartition(this, msPartition, partitionKeyExprs, fileFormatDescriptor,
                            fileDescriptors);
      partitions.add(partition);
      return partition;
    } else {
      LOG.warn("Path " + path + " does not exist for partition. Ignoring.");
      return null;
    }
  }

  private void addDefaultPartition(StorageDescriptor storageDescriptor)
      throws InvalidStorageDescriptorException {
    // Default partition has no files and is not referred to by scan nodes. Data sinks
    // refer to this to understand how to create new partitions
    HdfsStorageDescriptor hdfsStorageDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name, storageDescriptor);
    HdfsPartition partition = HdfsPartition.defaultPartition(this, hdfsStorageDescriptor);
    partitions.add(partition);
  }

  @Override
  public Table load(HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    LOG.info("load table " + name);
    // turn all exceptions into TableLoadingException
    try {
      // set nullPartitionKeyValue from the hive conf.
      nullPartitionKeyValue =
          client.getConfigValue("hive.exec.default.partition.name",
          "__HIVE_DEFAULT_PARTITION__");

      // populate with both partition keys and regular columns
      List<FieldSchema> partKeys = msTbl.getPartitionKeys();
      List<FieldSchema> tblFields = client.getFields(db.getName(), name);
      List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>(
          partKeys.size() + tblFields.size());
      fieldSchemas.addAll(partKeys);
      fieldSchemas.addAll(tblFields);
      // The number of clustering columns is the number of partition keys.
      numClusteringCols = partKeys.size();
      loadColumns(fieldSchemas, client);

      loadPartitions(client.listPartitions(db.getName(), name, Short.MAX_VALUE), msTbl);

      // load table stats
      numRows = getRowCount(msTbl.getParameters());
      LOG.info("table #rows=" + Long.toString(numRows));
    } catch (TableLoadingException e) {
      throw e;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for table: " + name, e);
    }
    return this;
  }

  @Override
  public TTableDescriptor toThrift() {
    TTableDescriptor TTableDescriptor =
        new TTableDescriptor(
            id.asInt(), TTableType.HDFS_TABLE, colsByPos.size(), numClusteringCols, name,
            db.getName());
    List<String> colNames = new ArrayList<String>();
    for (int i = 0; i < colsByPos.size(); ++i) {
      colNames.add(colsByPos.get(i).getName());
    }

    // TODO: Remove unused partitions (according to scan node / data sink usage) from
    // Thrift representation
    Map<Long, THdfsPartition> idToValue = Maps.newHashMap();
    for (HdfsPartition partition: partitions) {
      idToValue.put(partition.getId(), partition.toThrift());
    }
    THdfsTable tHdfsTable = new THdfsTable(hdfsBaseDir,
        colNames, nullPartitionKeyValue, idToValue);

    TTableDescriptor.setHdfsTable(tHdfsTable);
    return TTableDescriptor;
  }

  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    return new HdfsTableSink(this, partitionKeyExprs, overwrite);
  }

  /**
   * Return locations for all blocks in all files in the given partitions.
   */
  public static List<PartitionBlockMetadata> getBlockMetadata(
      List<HdfsPartition> partitions) {
    List<PartitionBlockMetadata> result = Lists.newArrayList();
    for (HdfsPartition partition: partitions) {
      result.add(partition.getTable().partitionBlockMdMap.get(partition));
    }
    return result;
  }

  public String getHdfsBaseDir() {
    return hdfsBaseDir;
  }

  @Override
  public int getNumNodes() {
    return uniqueHostPorts.size();
  }

  /**
   * Return a partition name formed from concatenating partition keys and their values,
   * compatible with the way Hive names partitions.
   */
  static public String getPartitionName(Table table, String hdfsPath) {
    Preconditions.checkState(table.getNumClusteringCols() > 0);
    List<Column> cols = table.getColumns();
    int firstPartColPos = hdfsPath.indexOf(cols.get(0).getName() + "=");
    int lastPartColPos =
      hdfsPath.indexOf(cols.get(table.getNumClusteringCols() - 1).getName() + "=");
    // Find the first '/' after the last partitioning-column folder.
    lastPartColPos = hdfsPath.indexOf('/', lastPartColPos);
    String partitionName = hdfsPath.substring(firstPartColPos, lastPartColPos);
    return partitionName;
  }

}
