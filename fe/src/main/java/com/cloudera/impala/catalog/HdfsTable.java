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
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
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
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.analysis.PartitionKeyValue;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.HdfsTableSink;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
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
  // hive's default value for table property 'serialization.null.format'
  private static final String DEFAULT_NULL_COLUMN_VALUE = "\\N";

  // string to indicate NULL. set in load() from table properties
  private String nullColumnValue;

  // hive uses this string for NULL partition keys. Set in load().
  private String nullPartitionKeyValue;

  // Avro schema of this table if this is an Avro table, otherwise null. Set in load().
  private String avroSchema = null;

  private static boolean hasLoggedDiskIdFormatWarning = false;

  private final List<HdfsPartition> partitions; // these are only non-empty partitions

  // Map from filename to FileDescriptor
  private final Map<String, FileDescriptor> fileDescMap = Maps.newHashMap();

  // Number of the unique host/ports of datanodes that hold data for this table
  private int uniqueHostPortsCount = 0;

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
   * Returns a disk id (0-based) index from the Hdfs VolumeId object.
   * There is currently no public API to get at the volume id. We'll have to get it
   * by accessing the internals.
   */
  private static int getDiskId(VolumeId hdfsVolumeId) {
    // Initialize the diskId as -1 to indicate it is unknown
    int diskId = -1;

    if (hdfsVolumeId != null && hdfsVolumeId.isValid()) {
      // TODO: this is a hack and we'll have to address this by getting the
      // public API. Also, we need to be very mindful of this when we change
      // the version of HDFS.
      String volumeIdString = hdfsVolumeId.toString();
      // This is the hacky part. The toString is currently the underlying id
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
   * Populate file block metadata inside each file descriptors.
   */
  private void loadBlockMd(List<FileDescriptor> fileDescriptors)
      throws RuntimeException {
    LOG.info("load block md for " + name);
    // Block locations for all the files
    List<BlockLocation> blockLocations = Lists.newArrayList();

    // loop over all files and record their block metadata, minus volume ids
    for (FileDescriptor fileDescriptor: fileDescriptors) {
      Path p = new Path(fileDescriptor.getFilePath());
      BlockLocation[] locations = null;
      try {
        FileStatus fileStatus = DFS.getFileStatus(p);
        // fileDescriptors should not contain directories.
        Preconditions.checkArgument(!fileStatus.isDirectory());
        locations = DFS.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        if (locations != null) {
          blockLocations.addAll(Arrays.asList(locations));
          for (int i = 0; i < locations.length; ++i) {
            FileBlock blockMd = new FileBlock(fileDescriptor.getFilePath(),
                fileDescriptor.getFileLength(), locations[i]);
            fileDescriptor.addFileBlock(blockMd);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("couldn't determine block locations for path '"
            + p + "':\n" + e.getMessage(), e);
      }
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
    for (FileDescriptor fileDescriptor: fileDescriptors) {
      for (FileBlock blockMd: fileDescriptor.getFileBlocks()) {
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

  protected HdfsTable(TableId id, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(id, msTbl, db, name, owner);
    this.partitions = Lists.newArrayList();
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }
  public List<HdfsPartition> getPartitions() { return partitions; }

  /**
   * Returns the value Hive is configured to use for NULL partition key values.
   * Set during load.
   */
  public String getNullPartitionKeyValue() { return nullPartitionKeyValue; }
  public String getNullColumnValue() { return nullColumnValue; }

  /*
   * Returns the storage location (HDFS path) of this table.
   */
  public String getLocation() { return super.getMetaStoreTable().getSd().getLocation(); }

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
    for (FieldSchema s: fieldSchemas) {
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
   *
   * For files that have not been changed, reuses file descriptors from oldFileDescMap.
   */
  private void loadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      Map<String, FileDescriptor> oldFileDescMap)
      throws IOException, InvalidStorageDescriptorException {
    partitions.clear();
    hdfsBaseDir = msTbl.getSd().getLocation();

    List<FileDescriptor> newFileDescs = Lists.newArrayList();

    // INSERT statements need to refer to this if they try to write to new partitions.
    // Scans don't refer to this because by definition all partitions they refer to
    // exist.
    addDefaultPartition(msTbl.getSd());

    if (msTbl.getPartitionKeysSize() == 0) {
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      Preconditions.checkArgument(msPartitions == null || msPartitions.isEmpty());
      HdfsPartition part = addPartition(msTbl.getSd(), null,
          new ArrayList<LiteralExpr>(), oldFileDescMap, newFileDescs);
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
              keyValues.add((LiteralExpr) expr);
            } catch (AnalysisException ex) {
              LOG.warn("Failed to create literal expression of type: " + type, ex);
              throw new InvalidStorageDescriptorException(ex);
            }
          }
          ++i;
        }
        HdfsPartition partition = addPartition(msPartition.getSd(), msPartition,
            keyValues, oldFileDescMap, newFileDescs);

        if (partition != null && msPartition.getParameters() != null) {
          partition.setNumRows(getRowCount(msPartition.getParameters()));
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

    if (newFileDescs.size() > 0) {
      loadBlockMd(newFileDescs);
    }

    uniqueHostPortsCount = countUniqueHostPorts(partitions);
  }

  /**
   * Count the number of unique data node for the given partitions
   */
  private static int countUniqueHostPorts(List<HdfsPartition> partitions) {
    Set<String> uniqueHostPorts = Sets.newHashSet();
    for (HdfsPartition partition: partitions) {
      for (FileDescriptor fileDesc: partition.getFileDescriptors()) {
        for (FileBlock blockMd: fileDesc.getFileBlocks()) {
          String[] hostports = blockMd.getHostPorts();
          for (int i = 0; i < hostports.length; ++i) {
            uniqueHostPorts.add(hostports[i]);
          }
        }
      }
    }
    return uniqueHostPorts.size();
  }

  /**
   * Returns the value of the ROW_COUNT constant, or -1 if not found.
   */
  private static long getRowCount(Map<String, String> parameters) {
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
   * For unchanged files (indicated by unchanged mtime), reuses the FileDescriptor from
   * the oldFileDescMap. Otherwise, creates a new FileDescriptor for each modified or
   * new file and adds it to newFileDescs.
   * Returns new partition or null, if none was added.
   *
   * @throws InvalidStorageDescriptorException
   *           if the supplied storage descriptor contains
   *           metadata that Impala can't understand.
   */
  private HdfsPartition addPartition(StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyExprs, Map<String, FileDescriptor> oldFileDescMap,
      List<FileDescriptor> newFileDescs)
      throws IOException, InvalidStorageDescriptorException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name, storageDescriptor);
    Path partDirPath = new Path(storageDescriptor.getLocation());
    List<FileDescriptor> fileDescriptors = Lists.newArrayList();
    if (DFS.exists(partDirPath)) {
      // DistributedFilesystem does not have an API that takes in a timestamp and return
      // a list of files that has been added/changed since. Therefore, we are calling
      // DFS.listStatus() to list all the files.
      for (FileStatus fileStatus: DFS.listStatus(partDirPath)) {
        String fileName = fileStatus.getPath().getName().toString();
        if (fileStatus.isDirectory() || FileSystemUtil.isHiddenFile(fileName) ||
            HdfsCompression.fromFileName(fileName) == HdfsCompression.LZO_INDEX) {
          // Ignore directory, hidden file starting with . or _, and LZO index files
          // If a directory is erroneously created as a subdirectory of a partition dir
          // we should ignore it and move on. Hive will not recurse into directories.
          // Skip index files, these are read by the LZO scanner directly.
          continue;
        }

        String fullPath = fileStatus.getPath().toString();
        FileDescriptor fd = (oldFileDescMap != null) ?
            oldFileDescMap.get(fullPath): null;
        if (fd != null && fd.getFileLength() == fileStatus.getLen() &&
            fd.getModificationTime() == fileStatus.getModificationTime()) {
          // Reuse the old file descriptor along with its block metadata if the file
          // length and mtime has not been changed.
        } else {
          // Create a new file descriptor. The block metadata will be populated by
          // loadFileDescriptorsBlockMd.
          fd = new FileDescriptor(
              fullPath, fileStatus.getLen(), fileStatus.getModificationTime());
          newFileDescs.add(fd);
        }
        fileDescriptors.add(fd);
        fileDescMap.put(fullPath, fd);
      }

      HdfsPartition partition = new HdfsPartition(this, msPartition, partitionKeyExprs,
          fileFormatDescriptor, fileDescriptors);
      partitions.add(partition);
      return partition;
    } else {
      LOG.warn("Path " + partDirPath + " does not exist for partition. Ignoring.");
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
  /**
   * Load the table metadata and reuse metadata to speed up metadata loading.
   * If the lastDdlTime has not been changed, that means the Hive metastore metadata has
   * not been changed. Reuses the old Hive partition metadata from oldValue.
   * To speed up Hdfs metadata loading, if a file's mtime has not been changed, reuses
   * the old file block metadata from old value.
   *
   * There are several cases where the oldValue might be reused incorrectly:
   * 1. an ALTER TABLE ADD PARTITION or dynamic partition insert is executed through
   *    Hive. This does not update the lastDdlTime.
   * 2. Hdfs rebalancer is executed. This changes the block locations but won't update
   *    the mtime.
   * If any of these occurs, user has to execute "invalidate metadata" to invalidate the
   * metadata cache of the table to trigger a fresh load.
   */
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    LOG.info("load table " + name);
    // turn all exceptions into TableLoadingException
    try {
      // set nullPartitionKeyValue from the hive conf.
      nullPartitionKeyValue =
          client.getConfigValue("hive.exec.default.partition.name",
          "__HIVE_DEFAULT_PARTITION__");

      // set NULL indicator string from table properties
      nullColumnValue =
          msTbl.getParameters().get(serdeConstants.SERIALIZATION_NULL_FORMAT);
      if (nullColumnValue == null) nullColumnValue = DEFAULT_NULL_COLUMN_VALUE;

      // populate with both partition keys and regular columns
      List<FieldSchema> partKeys = msTbl.getPartitionKeys();
      List<FieldSchema> tblFields = Lists.newArrayList();
      String inputFormat = msTbl.getSd().getInputFormat();
      if (HdfsFileFormat.fromJavaClassName(inputFormat) == HdfsFileFormat.AVRO) {
        tblFields.addAll(client.getFields(db.getName(), name));
      } else {
        tblFields.addAll(msTbl.getSd().getCols());
      }
      List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>(
          partKeys.size() + tblFields.size());
      fieldSchemas.addAll(partKeys);
      fieldSchemas.addAll(tblFields);
      // The number of clustering columns is the number of partition keys.
      numClusteringCols = partKeys.size();
      loadColumns(fieldSchemas, client);

      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions;
      if ((oldValue == this) || (oldValue != null && oldValue.lastDdlTime > -1 &&
          oldValue.lastDdlTime == this.lastDdlTime)) {
        Preconditions.checkArgument(oldValue instanceof HdfsTable);
        // Reuse the old metastore partitions if the table has not been altered.
        msPartitions = ((HdfsTable)oldValue).getMetaStorePartitions();
      } else {
        msPartitions = client.listPartitions(db.getName(), name, Short.MAX_VALUE);
      }
      Map<String, FileDescriptor> oldFileDescMap = null;
      if (oldValue != null) {
        Preconditions.checkArgument(oldValue instanceof HdfsTable);
        oldFileDescMap = ((HdfsTable)oldValue).fileDescMap;
      }
      loadPartitions(msPartitions, msTbl, oldFileDescMap);

      // load table stats
      numRows = getRowCount(msTbl.getParameters());
      LOG.info("table #rows=" + Long.toString(numRows));

      // populate Avro schema if necessary
      if (HdfsFileFormat.fromJavaClassName(inputFormat) == HdfsFileFormat.AVRO) {
        avroSchema = getAvroSchema();
      }
    } catch (TableLoadingException e) {
      throw e;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for table: " + name, e);
    }
  }

  /**
   * Returns the list of Metastore partitions
   */
  private List<org.apache.hadoop.hive.metastore.api.Partition> getMetaStorePartitions() {
    List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions =
        Lists.newArrayList();
    // Return an empty partition list for an unpartitioned table.
    if (numClusteringCols == 0) return msPartitions;

    for (HdfsPartition part: partitions) {
      // Skip the default partition.
      if (part.getId() != ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
        org.apache.hadoop.hive.metastore.api.Partition msPart =
            part.getMetaStorePartition();
        Preconditions.checkArgument(msPart != null);
        msPartitions.add(msPart);
      }
    }
    return msPartitions;
  }

  /**
   * Gets an Avro table's JSON schema. The schema may be specified as a string
   * literal or provided as an HDFS/http URL that points to the schema. The schema
   * can be specified in the TBLPROPERTIES or in the SERDEPROPERTIES, with the
   * latter taking precedence. This function does not perform any validation on
   * the returned string (e.g., it may not be a valid schema).
   */
  private String getAvroSchema() throws TableLoadingException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = this.getMetaStoreTable();

    // Look for the schema in TBLPROPERTIES and in SERDEPROPERTIES, with the latter
    // taking precedence.
    List<Map<String, String>> schemaSearchLocations = Lists.newArrayList();
    schemaSearchLocations.add(msTbl.getSd().getSerdeInfo().getParameters());
    schemaSearchLocations.add(msTbl.getParameters());

    String url = null;
    // Search all locations and break out on the first valid schema found.
    for (Map<String, String> schemaLocation: schemaSearchLocations) {
      if (schemaLocation == null) continue;

      String literal = schemaLocation.get(AvroSerdeUtils.SCHEMA_LITERAL);
      if (literal != null && !literal.equals(AvroSerdeUtils.SCHEMA_NONE)) return literal;

      url = schemaLocation.get(AvroSerdeUtils.SCHEMA_URL);
      if (url != null) {
        url = url.trim();
        break;
      }
    }

    if (url == null || url.equals(AvroSerdeUtils.SCHEMA_NONE)) {
      throw new TableLoadingException("No Avro schema provided for table " + name);
    }
    return loadAvroSchemaFromUrl(url);
  }

  private static String loadAvroSchemaFromUrl(String url)
      throws TableLoadingException {
    if (!url.toLowerCase().startsWith("hdfs://") &&
        !url.toLowerCase().startsWith("http://")) {
      throw new TableLoadingException("avro.schema.url must be of form " +
          "\"http://path/to/schema/file\" or " +
          "\"hdfs://namenode:port/path/to/schema/file\", got " + url);
    }

    if (url.toLowerCase().startsWith("hdfs://")) {
      try {
        return FileSystemUtil.readFile(new Path(url));
      } catch (IOException e) {
        throw new TableLoadingException(
            "Problem reading Avro schema at: " + url, e);
      }
    } else {
      Preconditions.checkState(url.toLowerCase().startsWith("http://"));
      InputStream urlStream = null;
      try {
        urlStream = new URL(url).openStream();
        return IOUtils.toString(urlStream);
      } catch (IOException e) {
        throw new TableLoadingException("Problem reading Avro schema from: " + url, e);
      } finally {
        IOUtils.closeQuietly(urlStream);
      }
    }
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
        colNames, nullPartitionKeyValue, nullColumnValue, idToValue);
    if (avroSchema != null) {
      tHdfsTable.setAvroSchema(avroSchema);
    }

    TTableDescriptor.setHdfsTable(tHdfsTable);
    return TTableDescriptor;
  }

  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    return new HdfsTableSink(this, partitionKeyExprs, overwrite);
  }

  public String getHdfsBaseDir() { return hdfsBaseDir; }
  @Override
  public int getNumNodes() { return uniqueHostPortsCount; }

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
