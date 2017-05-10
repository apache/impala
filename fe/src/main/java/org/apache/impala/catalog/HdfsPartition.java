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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.fb.FbCompression;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.thrift.ImpalaInternalServiceConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.ListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Query-relevant information for one table partition. Partitions are comparable
 * based on their partition-key values. The comparison orders partitions in ascending
 * order with NULLs sorting last. The ordering is useful for displaying partitions
 * in SHOW statements.
 */
public class HdfsPartition implements Comparable<HdfsPartition> {
  /**
   * Metadata for a single file in this partition.
   */
  static public class FileDescriptor implements Comparable<FileDescriptor> {
    // An invalid network address, which will always be treated as remote.
    private final static TNetworkAddress REMOTE_NETWORK_ADDRESS =
        new TNetworkAddress("remote*addr", 0);

    // Minimum block size in bytes allowed for synthetic file blocks (other than the last
    // block, which may be shorter).
    private final static long MIN_SYNTHETIC_BLOCK_SIZE = 1024 * 1024;

    // Internal representation of a file descriptor using a FlatBuffer.
    private final FbFileDesc fbFileDescriptor_;

    private FileDescriptor(FbFileDesc fileDescData) { fbFileDescriptor_ = fileDescData; }

    public static FileDescriptor fromThrift(THdfsFileDesc desc) {
      ByteBuffer bb = ByteBuffer.wrap(desc.getFile_desc_data());
      return new FileDescriptor(FbFileDesc.getRootAsFbFileDesc(bb));
    }

    /**
     * Creates the file descriptor of a file represented by 'fileStatus' with blocks
     * stored in 'blockLocations'. 'fileSystem' is the filesystem where the
     * file resides and 'hostIndex' stores the network addresses of the hosts that store
     * blocks of the parent HdfsTable. Populates 'numUnknownDiskIds' with the number of
     * unknown disk ids.
     */
    public static FileDescriptor create(FileStatus fileStatus,
        BlockLocation[] blockLocations, FileSystem fileSystem,
        ListMap<TNetworkAddress> hostIndex, Reference<Long> numUnknownDiskIds)
        throws IOException {
      Preconditions.checkState(FileSystemUtil.supportsStorageIds(fileSystem));
      FlatBufferBuilder fbb = new FlatBufferBuilder(1);
      int[] fbFileBlockOffsets = new int[blockLocations.length];
      int blockIdx = 0;
      for (BlockLocation loc: blockLocations) {
        fbFileBlockOffsets[blockIdx++] = FileBlock.createFbFileBlock(fbb, loc, hostIndex,
            numUnknownDiskIds);
      }
      return new FileDescriptor(createFbFileDesc(fbb, fileStatus, fbFileBlockOffsets));
    }

    /**
     * Creates the file descriptor of a file represented by 'fileStatus' that
     * resides in a filesystem that doesn't support the BlockLocation API (e.g. S3).
     * fileFormat' is the file format of the partition where this file resides and
     * 'hostIndex' stores the network addresses of the hosts that store blocks of
     * the parent HdfsTable.
     */
    public static FileDescriptor createWithSynthesizedBlockMd(FileStatus fileStatus,
        HdfsFileFormat fileFormat, ListMap<TNetworkAddress> hostIndex) {
      FlatBufferBuilder fbb = new FlatBufferBuilder(1);
      int[] fbFileBlockOffets =
          synthesizeFbBlockMd(fbb, fileStatus, fileFormat, hostIndex);
      return new FileDescriptor(createFbFileDesc(fbb, fileStatus, fbFileBlockOffets));
    }

    /**
     * Serializes the metadata of a file descriptor represented by 'fileStatus' into a
     * FlatBuffer using 'fbb' and returns the associated FbFileDesc object. 'blockOffsets'
     * are the offsets of the serialized block metadata of this file in the underlying
     * buffer.
     */
    private static FbFileDesc createFbFileDesc(FlatBufferBuilder fbb,
        FileStatus fileStatus, int[] fbFileBlockOffets) {
      int fileNameOffset = fbb.createString(fileStatus.getPath().getName());
      int blockVectorOffset = FbFileDesc.createFileBlocksVector(fbb, fbFileBlockOffets);
      FbFileDesc.startFbFileDesc(fbb);
      FbFileDesc.addFileName(fbb, fileNameOffset);
      FbFileDesc.addLength(fbb, fileStatus.getLen());
      FbFileDesc.addLastModificationTime(fbb, fileStatus.getModificationTime());
      HdfsCompression comp = HdfsCompression.fromFileName(fileStatus.getPath().getName());
      FbFileDesc.addCompression(fbb, comp.toFb());
      FbFileDesc.addFileBlocks(fbb, blockVectorOffset);
      fbb.finish(FbFileDesc.endFbFileDesc(fbb));
      // To eliminate memory fragmentation, copy the contents of the FlatBuffer to the
      // smallest possible ByteBuffer.
      ByteBuffer bb = fbb.dataBuffer().slice();
      ByteBuffer compressedBb = ByteBuffer.allocate(bb.capacity());
      compressedBb.put(bb);
      return FbFileDesc.getRootAsFbFileDesc((ByteBuffer)compressedBb.flip());
    }

    /**
     * Synthesizes the block metadata of a file represented by 'fileStatus' that resides
     * in a filesystem that doesn't support the BlockLocation API. The block metadata
     * consist of the length and offset of each file block. It serializes the
     * block metadata into a FlatBuffer using 'fbb' and returns their offsets in the
     * underlying buffer. 'fileFormat' is the file format of the underlying partition and
     * 'hostIndex' stores the network addresses of the hosts that store the blocks of the
     * parent HdfsTable.
     */
    private static int[] synthesizeFbBlockMd(FlatBufferBuilder fbb, FileStatus fileStatus,
        HdfsFileFormat fileFormat, ListMap<TNetworkAddress> hostIndex) {
      long start = 0;
      long remaining = fileStatus.getLen();
      long blockSize = fileStatus.getBlockSize();
      if (blockSize < MIN_SYNTHETIC_BLOCK_SIZE) blockSize = MIN_SYNTHETIC_BLOCK_SIZE;
      if (!fileFormat.isSplittable(HdfsCompression.fromFileName(
          fileStatus.getPath().getName()))) {
        blockSize = remaining;
      }
      List<Integer> fbFileBlockOffets = Lists.newArrayList();
      while (remaining > 0) {
        long len = Math.min(remaining, blockSize);
        fbFileBlockOffets.add(FileBlock.createFbFileBlock(fbb, start, len,
            (short) hostIndex.getIndex(REMOTE_NETWORK_ADDRESS)));
        remaining -= len;
        start += len;
      }
      return Ints.toArray(fbFileBlockOffets);
    }

    public String getFileName() { return fbFileDescriptor_.fileName(); }
    public long getFileLength() { return fbFileDescriptor_.length(); }

    public HdfsCompression getFileCompression() {
      return HdfsCompression.valueOf(FbCompression.name(fbFileDescriptor_.compression()));
    }

    public long getModificationTime() { return fbFileDescriptor_.lastModificationTime(); }
    public int getNumFileBlocks() { return fbFileDescriptor_.fileBlocksLength(); }

    public FbFileBlock getFbFileBlock(int idx) {
      return fbFileDescriptor_.fileBlocks(idx);
    }

    public THdfsFileDesc toThrift() {
      THdfsFileDesc fd = new THdfsFileDesc();
      ByteBuffer bb = fbFileDescriptor_.getByteBuffer();
      fd.setFile_desc_data(bb);
      return fd;
    }

    @Override
    public String toString() {
      int numFileBlocks = getNumFileBlocks();
      List<String> blocks = Lists.newArrayListWithCapacity(numFileBlocks);
      for (int i = 0; i < numFileBlocks; ++i) {
        blocks.add(FileBlock.debugString(getFbFileBlock(i)));
      }
      return Objects.toStringHelper(this)
          .add("FileName", getFileName())
          .add("Length", getFileLength())
          .add("Compression", getFileCompression())
          .add("ModificationTime", getModificationTime())
          .add("Blocks", Joiner.on(", ").join(blocks)).toString();
    }

    @Override
    public int compareTo(FileDescriptor otherFd) {
      return getFileName().compareTo(otherFd.getFileName());
    }
  }

  /**
   * Represents metadata of a single block replica.
   */
  public static class BlockReplica {
    private final boolean isCached_;
    private final short hostIdx_;

    /**
     * Creates a BlockReplica given a host ID/index and a flag specifying whether this
     * replica is cahced. Host IDs are assigned when loading the block metadata in
     * HdfsTable.
     */
    public BlockReplica(short hostIdx, boolean isCached) {
      hostIdx_ = hostIdx;
      isCached_ = isCached;
    }

    /**
     * Parses the location (an ip address:port string) of the replica and returns a
     * TNetworkAddress with this information, or null if parsing fails.
     */
    public static TNetworkAddress parseLocation(String location) {
      Preconditions.checkNotNull(location);
      String[] ip_port = location.split(":");
      if (ip_port.length != 2) return null;
      try {
        return new TNetworkAddress(ip_port[0], Integer.parseInt(ip_port[1]));
      } catch (NumberFormatException e) {
        return null;
      }
    }

    public boolean isCached() { return isCached_; }
    public short getHostIdx() { return hostIdx_; }
  }

  /**
   * Static utility methods to serialize and access file block metadata from FlatBuffers.
   */
  public static class FileBlock {
    // Bit mask used to extract the replica host id and cache info of a file block.
    // Use ~REPLICA_HOST_IDX_MASK to extract the cache info (stored in MSB).
    private static short REPLICA_HOST_IDX_MASK = (1 << 15) - 1;

    /**
     * Constructs an FbFileBlock object from the block location metadata
     * 'loc'. Serializes the file block metadata into a FlatBuffer using 'fbb' and
     * returns the offset in the underlying buffer where the encoded file block starts.
     * 'hostIndex' stores the network addresses of the datanodes that store the files of
     * the parent HdfsTable. Populates 'numUnknownDiskIds' with the number of unknown disk
     * ids.
     */
    public static int createFbFileBlock(FlatBufferBuilder fbb, BlockLocation loc,
        ListMap<TNetworkAddress> hostIndex, Reference<Long> numUnknownDiskIds)
        throws IOException {
      Preconditions.checkNotNull(fbb);
      Preconditions.checkNotNull(loc);
      Preconditions.checkNotNull(hostIndex);
      // replica host ids
      FbFileBlock.startReplicaHostIdxsVector(fbb, loc.getNames().length);
      Set<String> cachedHosts = Sets.newHashSet(loc.getCachedHosts());
      // Enumerate all replicas of the block, adding any unknown hosts
      // to hostIndex. We pick the network address from getNames() and
      // map it to the corresponding hostname from getHosts().
      for (int i = 0; i < loc.getNames().length; ++i) {
        TNetworkAddress networkAddress = BlockReplica.parseLocation(loc.getNames()[i]);
        short replicaIdx = (short) hostIndex.getIndex(networkAddress);
        boolean isReplicaCached = cachedHosts.contains(loc.getHosts()[i]);
        replicaIdx = isReplicaCached ?
            (short) (replicaIdx | ~REPLICA_HOST_IDX_MASK) : replicaIdx;
        fbb.addShort(replicaIdx);
      }
      int fbReplicaHostIdxOffset = fbb.endVector();

      // disk ids
      short[] diskIds = createDiskIds(loc, numUnknownDiskIds);
      Preconditions.checkState(diskIds.length != 0);
      int fbDiskIdsOffset = FbFileBlock.createDiskIdsVector(fbb, diskIds);
      FbFileBlock.startFbFileBlock(fbb);
      FbFileBlock.addOffset(fbb, loc.getOffset());
      FbFileBlock.addLength(fbb, loc.getLength());
      FbFileBlock.addReplicaHostIdxs(fbb, fbReplicaHostIdxOffset);
      FbFileBlock.addDiskIds(fbb, fbDiskIdsOffset);
      return FbFileBlock.endFbFileBlock(fbb);
    }

    /**
     * Constructs an FbFileBlock object from the file block metadata that comprise block's
     * 'offset', 'length' and replica index 'replicaIdx'. Serializes the file block
     * metadata into a FlatBuffer using 'fbb' and returns the offset in the underlying
     * buffer where the encoded file block starts.
     */
    public static int createFbFileBlock(FlatBufferBuilder fbb, long offset, long length,
        short replicaIdx) {
      Preconditions.checkNotNull(fbb);
      FbFileBlock.startReplicaHostIdxsVector(fbb, 1);
      fbb.addShort(replicaIdx);
      int fbReplicaHostIdxOffset = fbb.endVector();
      FbFileBlock.startFbFileBlock(fbb);
      FbFileBlock.addOffset(fbb, offset);
      FbFileBlock.addLength(fbb, length);
      FbFileBlock.addReplicaHostIdxs(fbb, fbReplicaHostIdxOffset);
      return FbFileBlock.endFbFileBlock(fbb);
    }

    /**
     * Creates the disk ids of a block from its BlockLocation 'location'. Returns the
     * disk ids and populates 'numUnknownDiskIds' with the number of unknown disk ids.
     */
    private static short[] createDiskIds(BlockLocation location,
        Reference<Long> numUnknownDiskIds) {
      long unknownDiskIdCount = 0;
      String[] storageIds = location.getStorageIds();
      String[] hosts;
      try {
        hosts = location.getHosts();
      } catch (IOException e) {
        LOG.error("Couldn't get hosts for block: " + location.toString(), e);
        return new short[0];
      }
      if (storageIds.length != hosts.length) {
        LOG.error("Number of storage IDs and number of hosts for block: " + location
            .toString() + " mismatch. Skipping disk ID loading for this block.");
        return Shorts.toArray(Collections.<Short>emptyList());
      }
      short[] diskIDs = new short[storageIds.length];
      for (int i = 0; i < storageIds.length; ++i) {
        if (Strings.isNullOrEmpty(storageIds[i])) {
          diskIDs[i] = (short) -1;
          ++unknownDiskIdCount;
        } else {
          diskIDs[i] = DiskIdMapper.INSTANCE.getDiskId(hosts[i], storageIds[i]);
        }
      }
      long count = numUnknownDiskIds.getRef() + unknownDiskIdCount;
      numUnknownDiskIds.setRef(Long.valueOf(count));
      return diskIDs;
    }

    public static long getOffset(FbFileBlock fbFileBlock) { return fbFileBlock.offset(); }
    public static long getLength(FbFileBlock fbFileBlock) { return fbFileBlock.length(); }
    // Returns true if there is at least one cached replica.
    public static boolean hasCachedReplica(FbFileBlock fbFileBlock) {
      boolean hasCachedReplica = false;
      for (int i = 0; i < fbFileBlock.replicaHostIdxsLength(); ++i) {
        hasCachedReplica |= isReplicaCached(fbFileBlock, i);
      }
      return hasCachedReplica;
    }

    public static int getNumReplicaHosts(FbFileBlock fbFileBlock) {
      return fbFileBlock.replicaHostIdxsLength();
    }

    public static int getReplicaHostIdx(FbFileBlock fbFileBlock, int pos) {
      int idx = fbFileBlock.replicaHostIdxs(pos);
      return idx & REPLICA_HOST_IDX_MASK;
    }

    // Returns true if the block replica 'replicaIdx' is cached.
    public static boolean isReplicaCached(FbFileBlock fbFileBlock, int replicaIdx) {
      int idx = fbFileBlock.replicaHostIdxs(replicaIdx);
      return (idx & ~REPLICA_HOST_IDX_MASK) != 0;
    }

    /**
     * Return the disk id of the block in BlockLocation.getNames()[hostIndex]; -1 if
     * disk id is not supported.
     */
    public static int getDiskId(FbFileBlock fbFileBlock, int hostIndex) {
      if (fbFileBlock.diskIdsLength() == 0) return -1;
      return fbFileBlock.diskIds(hostIndex);
    }

    /**
     * Returns a string representation of a FbFileBlock.
     */
    public static String debugString(FbFileBlock fbFileBlock) {
      int numReplicaHosts = getNumReplicaHosts(fbFileBlock);
      List<Integer> diskIds = Lists.newArrayListWithCapacity(numReplicaHosts);
      List<Integer> replicaHosts = Lists.newArrayListWithCapacity(numReplicaHosts);
      List<Boolean> isBlockCached = Lists.newArrayListWithCapacity(numReplicaHosts);
      for (int i = 0; i < numReplicaHosts; ++i) {
        diskIds.add(getDiskId(fbFileBlock, i));
        replicaHosts.add(getReplicaHostIdx(fbFileBlock, i));
        isBlockCached.add(isReplicaCached(fbFileBlock, i));
      }
      StringBuilder builder = new StringBuilder();
      return builder.append("Offset: " + getOffset(fbFileBlock))
          .append("Length: " + getLength(fbFileBlock))
          .append("IsCached: " + hasCachedReplica(fbFileBlock))
          .append("ReplicaHosts: " + Joiner.on(", ").join(replicaHosts))
          .append("DiskIds: " + Joiner.on(", ").join(diskIds))
          .append("Caching: " + Joiner.on(", ").join(isBlockCached))
          .toString();
    }
  }

  private final HdfsTable table_;
  private final List<LiteralExpr> partitionKeyValues_;
  // estimated number of rows in partition; -1: unknown
  private long numRows_ = -1;
  private static AtomicLong partitionIdCounter_ = new AtomicLong();

  // A unique ID for each partition, used to identify a partition in the thrift
  // representation of a table.
  private final long id_;

  /*
   * Note: Although you can write multiple formats to a single partition (by changing
   * the format before each write), Hive won't let you read that data and neither should
   * we. We should therefore treat mixing formats inside one partition as user error.
   * It's easy to add per-file metadata to FileDescriptor if this changes.
   */
  private final HdfsStorageDescriptor fileFormatDescriptor_;
  private List<FileDescriptor> fileDescriptors_;
  private HdfsPartitionLocationCompressor.Location location_;
  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartition.class);
  private boolean isDirty_ = false;
  // True if this partition is marked as cached. Does not necessarily mean the data is
  // cached.
  private boolean isMarkedCached_ = false;
  private final TAccessLevel accessLevel_;

  // (k,v) pairs of parameters for this partition, stored in the HMS. Used by Impala to
  // store intermediate state for statistics computations.
  private Map<String, String> hmsParameters_;

  public HdfsStorageDescriptor getInputFormatDescriptor() {
    return fileFormatDescriptor_;
  }

  public boolean isDefaultPartition() {
    return id_ == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID;
  }

  /**
   * Returns true if the partition resides at a location which can be cached (e.g. HDFS).
   */
  public boolean isCacheable() {
    return FileSystemUtil.isPathCacheable(new Path(getLocation()));
  }

  /**
   * Return a partition name formed by concatenating partition keys and their values,
   * compatible with the way Hive names partitions. Reuses Hive's
   * org.apache.hadoop.hive.common.FileUtils.makePartName() function to build the name
   * string because there are a number of special cases for how partition names are URL
   * escaped.
   * TODO: Consider storing the PartitionKeyValue in HdfsPartition. It would simplify
   * this code would be useful in other places, such as fromThrift().
   */
  public String getPartitionName() {
    List<String> partitionCols = Lists.newArrayList();
    for (int i = 0; i < getTable().getNumClusteringCols(); ++i) {
      partitionCols.add(getTable().getColumns().get(i).getName());
    }

    return org.apache.hadoop.hive.common.FileUtils.makePartName(
        partitionCols, getPartitionValuesAsStrings(true));
  }

  /**
   * Returns a list of partition values as strings. If mapNullsToHiveKey is true, any NULL
   * value is returned as the table's default null partition key string value, otherwise
   * they are returned as 'NULL'.
   */
  public List<String> getPartitionValuesAsStrings(boolean mapNullsToHiveKey) {
    List<String> ret = Lists.newArrayList();
    for (LiteralExpr partValue: getPartitionValues()) {
      if (mapNullsToHiveKey) {
        ret.add(PartitionKeyValue.getPartitionKeyValueString(
                partValue, getTable().getNullPartitionKeyValue()));
      } else {
        ret.add(partValue.getStringValue());
      }
    }
    return ret;
  }

  /**
   * Utility method which returns a string of conjuncts of equality exprs to exactly
   * select this partition (e.g. ((month=2009) AND (year=2012)).
   * TODO: Remove this when the TODO elsewhere in this file to save and expose the
   * list of TPartitionKeyValues has been resolved.
   */
  public String getConjunctSql() {
    List<String> partColSql = Lists.newArrayList();
    for (Column partCol: getTable().getClusteringColumns()) {
      partColSql.add(ToSqlUtils.getIdentSql(partCol.getName()));
    }

    List<String> conjuncts = Lists.newArrayList();
    for (int i = 0; i < partColSql.size(); ++i) {
      LiteralExpr partVal = getPartitionValues().get(i);
      String partValSql = partVal.toSql();
      if (partVal instanceof NullLiteral || partValSql.isEmpty()) {
        conjuncts.add(partColSql.get(i) + " IS NULL");
      } else {
        conjuncts.add(partColSql.get(i) + "=" + partValSql);
      }
    }
    return "(" + Joiner.on(" AND " ).join(conjuncts) + ")";
  }

  /**
   * Returns a string of the form part_key1=value1/part_key2=value2...
   */
  public String getValuesAsString() {
    StringBuilder partDescription = new StringBuilder();
    for (int i = 0; i < getTable().getNumClusteringCols(); ++i) {
      String columnName = getTable().getColumns().get(i).getName();
      String value = PartitionKeyValue.getPartitionKeyValueString(
          getPartitionValues().get(i),
          getTable().getNullPartitionKeyValue());
      partDescription.append(columnName + "=" + value);
      if (i != getTable().getNumClusteringCols() - 1) partDescription.append("/");
    }
    return partDescription.toString();
  }

  /**
   * Returns the storage location (HDFS path) of this partition. Should only be called
   * for partitioned tables.
   */
  public String getLocation() {
    return (location_ != null) ? location_.toString() : null;
  }
  public Path getLocationPath() { return new Path(getLocation()); }
  public long getId() { return id_; }
  public HdfsTable getTable() { return table_; }
  public void setNumRows(long numRows) { numRows_ = numRows; }
  public long getNumRows() { return numRows_; }
  public boolean isMarkedCached() { return isMarkedCached_; }
  void markCached() { isMarkedCached_ = true; }

  /**
   * Updates the file format of this partition and sets the corresponding input/output
   * format classes.
   */
  public void setFileFormat(HdfsFileFormat fileFormat) {
    fileFormatDescriptor_.setFileFormat(fileFormat);
    cachedMsPartitionDescriptor_.sdInputFormat = fileFormat.inputFormat();
    cachedMsPartitionDescriptor_.sdOutputFormat = fileFormat.outputFormat();
    cachedMsPartitionDescriptor_.sdSerdeInfo.setSerializationLib(
        fileFormatDescriptor_.getFileFormat().serializationLib());
  }

  public HdfsFileFormat getFileFormat() {
    return fileFormatDescriptor_.getFileFormat();
  }

  public void setLocation(String place) {
    location_ = table_.getPartitionLocationCompressor().new Location(place);
  }

  public org.apache.hadoop.hive.metastore.api.SerDeInfo getSerdeInfo() {
    return cachedMsPartitionDescriptor_.sdSerdeInfo;
  }

  // May return null if no per-partition stats were recorded, or if the per-partition
  // stats could not be deserialised from the parameter map.
  public TPartitionStats getPartitionStats() {
    try {
      return PartitionStatsUtil.partStatsFromParameters(hmsParameters_);
    } catch (ImpalaException e) {
      LOG.warn("Could not deserialise incremental stats state for " + getPartitionName() +
          ", consider DROP INCREMENTAL STATS ... PARTITION ... and recomputing " +
          "incremental stats for this table.");
      return null;
    }
  }

  public boolean hasIncrementalStats() {
    TPartitionStats partStats = getPartitionStats();
    return partStats != null && partStats.intermediate_col_stats != null;
  }

  /**
   * Returns the HDFS permissions Impala has to this partition's directory - READ_ONLY,
   * READ_WRITE, etc.
   */
  public TAccessLevel getAccessLevel() { return accessLevel_; }

  public Map<String, String> getParameters() { return hmsParameters_; }

  public void putToParameters(String k, String v) { hmsParameters_.put(k, v); }
  public void putToParameters(Pair<String, String> kv) {
    putToParameters(kv.first, kv.second);
  }

  /**
   * Marks this partition's metadata as "dirty" indicating that changes have been
   * made and this partition's metadata should not be reused during the next
   * incremental metadata refresh.
   */
  public void markDirty() { isDirty_ = true; }
  public boolean isDirty() { return isDirty_; }

  /**
   * Returns an immutable list of partition key expressions
   */
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues_; }
  public LiteralExpr getPartitionValue(int i) { return partitionKeyValues_.get(i); }
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors_;
  }
  public void setFileDescriptors(List<FileDescriptor> descriptors) {
    fileDescriptors_ = descriptors;
  }
  public int getNumFileDescriptors() {
    return fileDescriptors_ == null ? 0 : fileDescriptors_.size();
  }

  public boolean hasFileDescriptors() { return !fileDescriptors_.isEmpty(); }

  // Struct-style class for caching all the information we need to reconstruct an
  // HMS-compatible Partition object, for use in RPCs to the metastore. We do this rather
  // than cache the Thrift partition object itself as the latter can be large - thanks
  // mostly to the inclusion of the full FieldSchema list. This class is read-only - if
  // any field can be mutated by Impala it should belong to HdfsPartition itself (see
  // HdfsPartition.location_ for an example).
  //
  // TODO: Cache this descriptor in HdfsTable so that identical descriptors are shared
  // between HdfsPartition instances.
  // TODO: sdInputFormat and sdOutputFormat can be mutated by Impala when the file format
  // of a partition changes; move these fields to HdfsPartition.
  private static class CachedHmsPartitionDescriptor {
    public String sdInputFormat;
    public String sdOutputFormat;
    public final boolean sdCompressed;
    public final int sdNumBuckets;
    public final org.apache.hadoop.hive.metastore.api.SerDeInfo sdSerdeInfo;
    public final List<String> sdBucketCols;
    public final List<org.apache.hadoop.hive.metastore.api.Order> sdSortCols;
    public final Map<String, String> sdParameters;
    public final int msCreateTime;
    public final int msLastAccessTime;

    public CachedHmsPartitionDescriptor(
        org.apache.hadoop.hive.metastore.api.Partition msPartition) {
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = null;
      if (msPartition != null) {
        sd = msPartition.getSd();
        msCreateTime = msPartition.getCreateTime();
        msLastAccessTime = msPartition.getLastAccessTime();
      } else {
        msCreateTime = msLastAccessTime = 0;
      }
      if (sd != null) {
        sdInputFormat = sd.getInputFormat();
        sdOutputFormat = sd.getOutputFormat();
        sdCompressed = sd.isCompressed();
        sdNumBuckets = sd.getNumBuckets();
        sdSerdeInfo = sd.getSerdeInfo();
        sdBucketCols = ImmutableList.copyOf(sd.getBucketCols());
        sdSortCols = ImmutableList.copyOf(sd.getSortCols());
        sdParameters = ImmutableMap.copyOf(sd.getParameters());
      } else {
        sdInputFormat = "";
        sdOutputFormat = "";
        sdCompressed = false;
        sdNumBuckets = 0;
        sdSerdeInfo = null;
        sdBucketCols = ImmutableList.of();
        sdSortCols = ImmutableList.of();
        sdParameters = ImmutableMap.of();
      }
    }
  }

  private CachedHmsPartitionDescriptor cachedMsPartitionDescriptor_;

  public CachedHmsPartitionDescriptor getCachedMsPartitionDescriptor() {
    return cachedMsPartitionDescriptor_;
  }

  /**
   * Returns a Hive-compatible partition object that may be used in calls to the
   * metastore.
   */
  public org.apache.hadoop.hive.metastore.api.Partition toHmsPartition() {
    if (cachedMsPartitionDescriptor_ == null) return null;
    Preconditions.checkNotNull(table_.getNonPartitionFieldSchemas());
    // Update the serde library class based on the currently used file format.
    org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor =
        new org.apache.hadoop.hive.metastore.api.StorageDescriptor(
            table_.getNonPartitionFieldSchemas(),
            getLocation(),
            cachedMsPartitionDescriptor_.sdInputFormat,
            cachedMsPartitionDescriptor_.sdOutputFormat,
            cachedMsPartitionDescriptor_.sdCompressed,
            cachedMsPartitionDescriptor_.sdNumBuckets,
            cachedMsPartitionDescriptor_.sdSerdeInfo,
            cachedMsPartitionDescriptor_.sdBucketCols,
            cachedMsPartitionDescriptor_.sdSortCols,
            cachedMsPartitionDescriptor_.sdParameters);
    org.apache.hadoop.hive.metastore.api.Partition partition =
        new org.apache.hadoop.hive.metastore.api.Partition(
            getPartitionValuesAsStrings(true), getTable().getDb().getName(),
            getTable().getName(), cachedMsPartitionDescriptor_.msCreateTime,
            cachedMsPartitionDescriptor_.msLastAccessTime, storageDescriptor,
            getParameters());
    return partition;
  }

  private HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      Collection<HdfsPartition.FileDescriptor> fileDescriptors, long id,
      HdfsPartitionLocationCompressor.Location location, TAccessLevel accessLevel) {
    table_ = table;
    if (msPartition == null) {
      cachedMsPartitionDescriptor_ = null;
    } else {
      cachedMsPartitionDescriptor_ = new CachedHmsPartitionDescriptor(msPartition);
    }
    location_ = location;
    partitionKeyValues_ = ImmutableList.copyOf(partitionKeyValues);
    fileDescriptors_ = ImmutableList.copyOf(fileDescriptors);
    fileFormatDescriptor_ = fileFormatDescriptor;
    id_ = id;
    accessLevel_ = accessLevel;
    if (msPartition != null && msPartition.getParameters() != null) {
      isMarkedCached_ = HdfsCachingUtil.getCacheDirectiveId(
          msPartition.getParameters()) != null;
      hmsParameters_ = msPartition.getParameters();
    } else {
      hmsParameters_ = Maps.newHashMap();
    }

    // TODO: instead of raising an exception, we should consider marking this partition
    // invalid and moving on, so that table loading won't fail and user can query other
    // partitions.
    for (FileDescriptor fileDescriptor: fileDescriptors_) {
      StringBuilder errorMsg = new StringBuilder();
      if (!getInputFormatDescriptor().getFileFormat().isFileCompressionTypeSupported(
          fileDescriptor.getFileName(), errorMsg)) {
        throw new RuntimeException(errorMsg.toString());
      }
    }
  }

  public HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      Collection<HdfsPartition.FileDescriptor> fileDescriptors,
      TAccessLevel accessLevel) {
    this(table, msPartition, partitionKeyValues, fileFormatDescriptor, fileDescriptors,
        partitionIdCounter_.getAndIncrement(),
        table.getPartitionLocationCompressor().new Location(msPartition != null
                ? msPartition.getSd().getLocation()
                : table.getLocation()),
        accessLevel);
  }

  public static HdfsPartition defaultPartition(
      HdfsTable table, HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = Lists.newArrayList();
    List<FileDescriptor> emptyFileDescriptorList = Lists.newArrayList();
    return new HdfsPartition(table, null, emptyExprList,
        storageDescriptor, emptyFileDescriptorList,
        ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID, null,
        TAccessLevel.READ_WRITE);
  }

  /**
   * Return the size (in bytes) of all the files inside this partition
   */
  public long getSize() {
    long result = 0;
    for (HdfsPartition.FileDescriptor fileDescriptor: fileDescriptors_) {
      result += fileDescriptor.getFileLength();
    }
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fileDescriptors", fileDescriptors_)
      .toString();
  }

  private static Predicate<String> isIncrementalStatsKey = new Predicate<String>() {
    @Override
    public boolean apply(String key) {
      return !(key.startsWith(PartitionStatsUtil.INCREMENTAL_STATS_NUM_CHUNKS)
          || key.startsWith(PartitionStatsUtil.INCREMENTAL_STATS_CHUNK_PREFIX));
    }
  };

  /**
   * Returns hmsParameters_ after filtering out all the partition
   * incremental stats information.
   */
  private Map<String, String> getFilteredHmsParameters() {
    return Maps.filterKeys(hmsParameters_, isIncrementalStatsKey);
  }

  public static HdfsPartition fromThrift(HdfsTable table,
      long id, THdfsPartition thriftPartition) {
    HdfsStorageDescriptor storageDesc = new HdfsStorageDescriptor(table.getName(),
        HdfsFileFormat.fromThrift(thriftPartition.getFileFormat()),
        thriftPartition.lineDelim,
        thriftPartition.fieldDelim,
        thriftPartition.collectionDelim,
        thriftPartition.mapKeyDelim,
        thriftPartition.escapeChar,
        (byte) '"', // TODO: We should probably add quoteChar to THdfsPartition.
        thriftPartition.blockSize);

    List<LiteralExpr> literalExpr = Lists.newArrayList();
    if (id != ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
      List<Column> clusterCols = Lists.newArrayList();
      for (int i = 0; i < table.getNumClusteringCols(); ++i) {
        clusterCols.add(table.getColumns().get(i));
      }

      List<TExprNode> exprNodes = Lists.newArrayList();
      for (TExpr expr: thriftPartition.getPartitionKeyExprs()) {
        for (TExprNode node: expr.getNodes()) {
          exprNodes.add(node);
        }
      }
      Preconditions.checkState(clusterCols.size() == exprNodes.size(),
          String.format("Number of partition columns (%d) does not match number " +
              "of partition key expressions (%d)",
              clusterCols.size(), exprNodes.size()));

      for (int i = 0; i < exprNodes.size(); ++i) {
        literalExpr.add(LiteralExpr.fromThrift(
            exprNodes.get(i), clusterCols.get(i).getType()));
      }
    }

    List<HdfsPartition.FileDescriptor> fileDescriptors = Lists.newArrayList();
    if (thriftPartition.isSetFile_desc()) {
      for (THdfsFileDesc desc: thriftPartition.getFile_desc()) {
        fileDescriptors.add(HdfsPartition.FileDescriptor.fromThrift(desc));
      }
    }

    TAccessLevel accessLevel = thriftPartition.isSetAccess_level() ?
        thriftPartition.getAccess_level() : TAccessLevel.READ_WRITE;
    HdfsPartitionLocationCompressor.Location location = thriftPartition.isSetLocation()
        ? table.getPartitionLocationCompressor().new Location(
              thriftPartition.getLocation())
        : null;
    HdfsPartition partition = new HdfsPartition(table, null, literalExpr, storageDesc,
        fileDescriptors, id, location, accessLevel);
    if (thriftPartition.isSetStats()) {
      partition.setNumRows(thriftPartition.getStats().getNum_rows());
    }
    if (thriftPartition.isSetIs_marked_cached()) {
      partition.isMarkedCached_ = thriftPartition.isIs_marked_cached();
    }

    if (thriftPartition.isSetHms_parameters()) {
      partition.hmsParameters_ = thriftPartition.getHms_parameters();
    } else {
      partition.hmsParameters_ = Maps.newHashMap();
    }

    return partition;
  }

  /**
   * Checks that this partition's metadata is well formed. This does not necessarily
   * mean the partition is supported by Impala.
   * Throws a CatalogException if there are any errors in the partition metadata.
   */
  public void checkWellFormed() throws CatalogException {
    try {
      // Validate all the partition key/values to ensure you can convert them toThrift()
      Expr.treesToThrift(getPartitionValues());
    } catch (Exception e) {
      throw new CatalogException("Partition (" + getPartitionName() +
          ") has invalid partition column values: ", e);
    }
  }

  public THdfsPartition toThrift(boolean includeFileDesc,
      boolean includeIncrementalStats) {
    List<TExpr> thriftExprs = Expr.treesToThrift(getPartitionValues());

    THdfsPartition thriftHdfsPart = new THdfsPartition(
        fileFormatDescriptor_.getLineDelim(),
        fileFormatDescriptor_.getFieldDelim(),
        fileFormatDescriptor_.getCollectionDelim(),
        fileFormatDescriptor_.getMapKeyDelim(),
        fileFormatDescriptor_.getEscapeChar(),
        fileFormatDescriptor_.getFileFormat().toThrift(), thriftExprs,
        fileFormatDescriptor_.getBlockSize());
    if (location_ != null) thriftHdfsPart.setLocation(location_.toThrift());
    thriftHdfsPart.setStats(new TTableStats(numRows_));
    thriftHdfsPart.setAccess_level(accessLevel_);
    thriftHdfsPart.setIs_marked_cached(isMarkedCached_);
    thriftHdfsPart.setId(getId());
    // IMPALA-4902: Shallow-clone the map to avoid concurrent modifications. One thread
    // may try to serialize the returned THdfsPartition after releasing the table's lock,
    // and another thread doing DDL may modify the map.
    thriftHdfsPart.setHms_parameters(Maps.newHashMap(
        includeIncrementalStats ? hmsParameters_ : getFilteredHmsParameters()));
    if (includeFileDesc) {
      // Add block location information
      for (FileDescriptor fd: fileDescriptors_) {
        thriftHdfsPart.addToFile_desc(fd.toThrift());
      }
    }

    return thriftHdfsPart;
  }

  /**
   * Comparison method to allow ordering of HdfsPartitions by their partition-key values.
   */
  @Override
  public int compareTo(HdfsPartition o) {
    return comparePartitionKeyValues(partitionKeyValues_, o.getPartitionValues());
  }

  @VisibleForTesting
  public static int comparePartitionKeyValues(List<LiteralExpr> lhs,
      List<LiteralExpr> rhs) {
    int sizeDiff = lhs.size() - rhs.size();
    if (sizeDiff != 0) return sizeDiff;
    for(int i = 0; i < lhs.size(); ++i) {
      int cmp = lhs.get(i).compareTo(rhs.get(i));
      if (cmp != 0) return cmp;
    }
    return 0;
  }
}
