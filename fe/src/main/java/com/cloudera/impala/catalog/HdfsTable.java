// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.HdfsTableSink;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

  /**
   * Captures three important pieces of information for a block: its location, the path
   * of the file to which it belongs, and the partition to which that file belongs.
   */
  public static class BlockMetadata {
    private String fileName;
    private HdfsPartition parentPartition;
    private final BlockLocation blockLocation;
    // For each replica, this is the 0-based disk index for this block.  The BE uses
    // this information to schedule the order scan ranges are read.
    private final int[] diskIds;

    public BlockMetadata(BlockLocation blockLocation, int[] diskIds) {
      Preconditions.checkNotNull(blockLocation);
      this.blockLocation = blockLocation;
      this.diskIds = diskIds;
    }

    public String getFileName() { return fileName; }
    public BlockLocation getLocation() { return blockLocation; }
    public HdfsPartition getPartition() { return parentPartition; }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public void setPartition(HdfsPartition partition) {
      this.parentPartition = partition;
    }

    /**
     * Return the volume id of the block in BlockLocation.getName()[hostIndex]; -1 if
     * volumn id is not supported.
     */
    public int getVolumeId(int hostIndex) {
      if (diskIds == null) return -1;
      Preconditions.checkArgument(hostIndex >= 0);
      Preconditions.checkArgument(hostIndex < diskIds.length);
      return diskIds[hostIndex];
    }
  }

  private final List<HdfsPartition> partitions; // these are only non-empty partitions

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

  /**
   * Returns true if the Hive table represents an Hdfs table that Impala understands,
   * by checking the input format for a known data format.
   */
  public static boolean isHdfsTable(org.apache.hadoop.hive.metastore.api.Table table) {
    StorageDescriptor sd = table.getSd();
    return HdfsFileFormat.isHdfsFormatClass(sd.getInputFormat());
  }

  protected HdfsTable(TableId id, Db db, String name, String owner) {
    super(id, db, name, owner);
    this.partitions = Lists.newArrayList();
  }

  public List<HdfsPartition> getPartitions() {
    return partitions;
  }

  public boolean isClusteringColumn(Column col) {
    return col.getPosition() < getNumClusteringCols();
  }

  /**
   * Create columns corresponding to fieldSchemas.
   * @param fieldSchemas
   * @return true if success, false otherwise
   */
  private boolean loadColumns(List<FieldSchema> fieldSchemas) {
    int pos = 0;
    for (FieldSchema s : fieldSchemas) {
      // catch currently unsupported hive schema elements
      if (!Constants.PrimitiveTypes.contains(s.getType())) {
        LOG.warn("Ignoring table {} because column {} " +
            "contains a field of unsupported type {}. " +
            "Only primitive types are currently supported.",
            new Object[] {getName(), s.getName(), s.getType()});
        return false;
      }
      Column col = new Column(s.getName(), getPrimitiveType(s.getType()), pos);
      colsByPos.add(col);
      colsByName.put(s.getName(), col);
      ++pos;
    }
    return true;
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
      addPartition(msTbl.getSd(), new ArrayList<LiteralExpr>());
      return;
    }
    for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
      // load key values
      List<LiteralExpr> keyValues = Lists.newArrayList();
      for (String partitionKey: msPartition.getValues()) {
        // Deal with Hive's special NULL partition key.
        if (partitionKey.equals(nullPartitionKeyValue)) {
          keyValues.add(new NullLiteral());
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
      }
      addPartition(msPartition.getSd(), keyValues);
    }
  }

  /**
   * Adds a new HdfsPartition to internal partition list, populating with file format
   * information and file locations. If a partition contains no files, it's not added.
   *
   * @throws InvalidStorageDescriptorException if the supplied storage descriptor contains
   *         metadata that Impala can't understand.
   */
  private void addPartition(StorageDescriptor storageDescriptor,
      List<LiteralExpr> partitionKeyExprs)
      throws IOException, InvalidStorageDescriptorException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(storageDescriptor);
    Path path = new Path(storageDescriptor.getLocation());
    List<FileDescriptor> fileDescriptors = Lists.newArrayList();
    FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) {
      for (FileStatus fileStatus: fs.listStatus(path)) {
        FileDescriptor fd = new FileDescriptor(fileStatus.getPath().toString(),
            fileStatus.getLen());
        fileDescriptors.add(fd);
      }

      HdfsPartition partition =
          new HdfsPartition(partitionKeyExprs, fileFormatDescriptor, fileDescriptors);
      partitions.add(partition);
    } else {
      LOG.warn("Path " + path + " does not exist for partition. Ignoring.");
    }
  }

  private void addDefaultPartition(StorageDescriptor storageDescriptor)
      throws InvalidStorageDescriptorException {
    // Default partition has no files and is not referred to by scan nodes. Data sinks
    // refer to this to understand how to create new partitions
    HdfsStorageDescriptor hdfsStorageDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(storageDescriptor);
    HdfsPartition partition = HdfsPartition.defaultPartition(hdfsStorageDescriptor);
    partitions.add(partition);
  }

  @Override
  public Table load(HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // turn all exceptions into unchecked exception
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
      if (!loadColumns(fieldSchemas)) {
        return null;
      }

      // The number of clustering columns is the number of partition keys.
      numClusteringCols = partKeys.size();
      try {
        loadPartitions(client.listPartitions(db.getName(), name, Short.MAX_VALUE), msTbl);
      } catch (Exception ex) {
        // TODO: Do we want this behaviour for all possible exceptions?
        LOG.warn("Ignoring HDFS table '" + msTbl.getTableName() +
            "' due to errors loading metadata", ex);
        return null;
      }
    } catch (TException e) {
      throw new UnsupportedOperationException(e);
    } catch (UnknownDBException e) {
      throw new UnsupportedOperationException(e);
    } catch (MetaException e) {
      throw new UnsupportedOperationException(e);
    } catch (UnknownTableException e) {
      throw new UnsupportedOperationException(e);
    } catch (ConfigValSecurityException e) {
      throw new UnsupportedOperationException(e);
    }
    return this;
  }

  @Override
  public TTableDescriptor toThrift() {
    TTableDescriptor TTableDescriptor =
        new TTableDescriptor(
            id.asInt(), TTableType.HDFS_TABLE, colsByPos.size(), numClusteringCols, name,
            db.getName());
    List<String> partitionKeyNames = new ArrayList<String>();
    for (int i = 0; i < numClusteringCols; ++i) {
      partitionKeyNames.add(colsByPos.get(i).getName());
    }

    // TODO: Remove unused partitions (according to scan node / data sink usage) from
    // Thrift representation
    Map<Long, THdfsPartition> idToValue = Maps.newHashMap();
    for (HdfsPartition partition: partitions) {
      idToValue.put(partition.getId(), partition.toThrift());
    }
    THdfsTable tHdfsTable = new THdfsTable(hdfsBaseDir,
        partitionKeyNames, nullPartitionKeyValue, idToValue);

    TTableDescriptor.setHdfsTable(tHdfsTable);
    return TTableDescriptor;
  }

  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    return new HdfsTableSink(this, partitionKeyExprs, overwrite);
  }

  /**
   * Return locations for all blocks in all files in the given partitions.
   * @return list of HdfsTable.BlockMetadata objects.
   */
  public static List<BlockMetadata> getBlockMetadata(List<HdfsPartition> partitions) {
    List<BlockMetadata> result = Lists.newArrayList();

    // Block locations for all the files in all the partitions.
    List<BlockLocation> blocks = Lists.newArrayList();

    // List of ending index in blocks per file. The file index to this list is obtained
    // traversing the file list in each partitions sequentially. For file i, its block
    // locations are from blocks[endingBlockIndexes[i-1]] to blocks[endingBlockIndexes[i]]
    List<Integer> endingBlockIndexes = Lists.newArrayList();

    boolean supportsVolumeId =
        CONF.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, false);

    // TODO: Is DistributedFileSystem thread safe? If so, make it a static final object.
    DistributedFileSystem dfs;
    try {
      FileSystem fs;
      fs = FileSystem.get(CONF);
      if (!(fs instanceof DistributedFileSystem)) {
        throw new RuntimeException("HDFS FileSystem should be DistributedFileSystem but "
            + "got " + fs.getClass().getName());
      }
      dfs = (DistributedFileSystem)fs;
    } catch (IOException e) {
      throw new RuntimeException("couldn't retrieve FileSystem:\n" + e.getMessage(), e);
    }

    for (HdfsPartition partition: partitions) {
      for (FileDescriptor fileDescriptor: partition.getFileDescriptors()) {
        Path p = new Path(fileDescriptor.getFilePath());
        BlockLocation[] locations = null;
        try {
          FileStatus fileStatus = dfs.getFileStatus(p);
          // Ignore directories (and files in them) - if a directory is erroneously
          // created as a subdirectory of a partition dir we should ignore it and move on
          // (getFileBlockLocations will throw when
          // called on a directory). Hive will not recurse into directories.
          if (!fileStatus.isDirectory()) {
            locations = dfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            blocks.addAll(Arrays.asList(locations));
          }
          endingBlockIndexes.add(blocks.size());
        } catch (IOException e) {
          throw new RuntimeException("couldn't determine block locations for path '"
              + fileDescriptor.getFilePath() + "':\n" + e.getMessage(), e);
        }
      }
    }

    if (supportsVolumeId) {
      try {
        // Get the BlockStorageLocations for all the blocks
        BlockStorageLocation[] locations = dfs.getFileBlockStorageLocations(blocks);

        // Convert block locations to 0 based ids.  The block location ids returned
        // from HDFS are unique but opaque (only defining comparison operators).  We need
        // to turn them indices.  
        // TODO: the diskId should be eventually retrievable from Hdfs when 
        // the community agrees this API is useful.
        
        // For each host, this is a mapping of the VolumeId object to a 0 based index.
        Map<String, Map<VolumeId, Integer>> hostDiskIds = Maps.newHashMap();

        for (int i = 0; i < locations.length; ++i) {
          String[] hosts = locations[i].getHosts();
          VolumeId[] volumeIds = locations[i].getVolumeIds();
          Preconditions.checkState(hosts.length == volumeIds.length);

          // For each block replica, the disk id for the block on that host
          int[] diskIds = new int[volumeIds.length];

          boolean found_null = false;
          for (int j = 0; j < volumeIds.length; ++j) {
            if (volumeIds[j] == null) {
              found_null = true;
              break;
            }

            Map<VolumeId, Integer> hostDisks;
            if (!hostDiskIds.containsKey(hosts[j])) {
              hostDisks = Maps.newHashMap();
              hostDiskIds.put(hosts[j], hostDisks);
            } else {
              hostDisks = hostDiskIds.get(hosts[j]);
            }

            if (!volumeIds[j].isValid()) {
              // The data node with this block did not respond to the block location 
              // rpc.  Mark it as -1 for the BE which will assign it a random disk.
              diskIds[j] = -1;
            } else if (hostDisks.containsKey(volumeIds[j])) {
              // This is a VolumeId we've seen on this host, assign it the id we already
              // assigned to this VolumeId
              diskIds[j] = hostDisks.get(volumeIds[j]);
            } else {
              // This is a VolumeId we haven't seen.  Give it the next index.
              int index = hostDisks.size();
              hostDisks.put(volumeIds[j], index);
              diskIds[j] = index;
            }
          }
          if (found_null) break;
          result.add(new BlockMetadata(locations[i], diskIds));
        }
      } catch (IOException e) {
        throw new RuntimeException("couldn't determine block storage locations:\n"
            + e.getMessage(), e);
      }
    } 

    if (result.size() == 0) {
      if (supportsVolumeId) {
        LOG.warn("Attempted to get block locations but the call returned nulls");
      }
      // No disk locations.
      for (int i = 0; i < blocks.size(); ++i) {
        result.add(new BlockMetadata(blocks.get(i), null));
      }
    }

    // Construct block metadata to also include file names and partition information
    int firstBlockIndex = 0;
    int fileIndex = 0;
    for (HdfsPartition partition: partitions) {
      for (FileDescriptor fileDescriptor: partition.getFileDescriptors()) {
        int lastBlockIndex = endingBlockIndexes.get(fileIndex);
        for (int i = firstBlockIndex; i < lastBlockIndex; ++i) {
          result.get(i).setFileName(fileDescriptor.getFilePath());
          result.get(i).setPartition(partition);
        }
        ++fileIndex;
        firstBlockIndex = lastBlockIndex;
      }
    }
    return result;
  }

  public String getHdfsBaseDir() {
    return hdfsBaseDir;
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
