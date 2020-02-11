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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.catalog.events.InFlightEvents;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.fb.FbCompression;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.ListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Query-relevant information for one table partition. Partitions are comparable
 * based on their partition-key values. The comparison orders partitions in ascending
 * order with NULLs sorting last. The ordering is useful for displaying partitions
 * in SHOW statements.
 */
public class HdfsPartition implements FeFsPartition, PrunablePartition {
  /**
   * Metadata for a single file in this partition.
   */
  static public class FileDescriptor implements Comparable<FileDescriptor> {
    // An invalid network address, which will always be treated as remote.
    private final static TNetworkAddress REMOTE_NETWORK_ADDRESS =
        new TNetworkAddress("remote*addr", 0);

    // Minimum block size in bytes allowed for synthetic file blocks (other than the last
    // block, which may be shorter).
    public final static long MIN_SYNTHETIC_BLOCK_SIZE = 1024 * 1024;

    // Internal representation of a file descriptor using a FlatBuffer.
    private final FbFileDesc fbFileDescriptor_;

    private FileDescriptor(FbFileDesc fileDescData) { fbFileDescriptor_ = fileDescData; }

    public static FileDescriptor fromThrift(THdfsFileDesc desc) {
      ByteBuffer bb = ByteBuffer.wrap(desc.getFile_desc_data());
      return new FileDescriptor(FbFileDesc.getRootAsFbFileDesc(bb));
    }

    /**
     * Clone the descriptor, but change the replica indexes to reference the new host
     * index 'dstIndex' instead of the original index 'origIndex'.
     */
    public FileDescriptor cloneWithNewHostIndex(List<TNetworkAddress> origIndex,
        ListMap<TNetworkAddress> dstIndex) {
      // First clone the flatbuffer with no changes.
      ByteBuffer oldBuf = fbFileDescriptor_.getByteBuffer();
      ByteBuffer newBuf = ByteBuffer.allocate(oldBuf.remaining());
      newBuf.put(oldBuf.array(), oldBuf.position(), oldBuf.remaining());
      newBuf.rewind();
      FbFileDesc cloned = FbFileDesc.getRootAsFbFileDesc(newBuf);

      // Now iterate over the blocks in the new flatbuffer and mutate the indexes.
      FbFileBlock it = new FbFileBlock();
      for (int i = 0; i < cloned.fileBlocksLength(); i++) {
        it = cloned.fileBlocks(it, i);
        for (int j = 0; j < it.replicaHostIdxsLength(); j++) {
          int origHostIdx = FileBlock.getReplicaHostIdx(it, j);
          boolean isCached = FileBlock.isReplicaCached(it, j);
          TNetworkAddress origHost = origIndex.get(origHostIdx);
          int newHostIdx = dstIndex.getIndex(origHost);
          it.mutateReplicaHostIdxs(j, FileBlock.makeReplicaIdx(isCached, newHostIdx));
        }
      }
      return new FileDescriptor(cloned);
    }

    /**
     * Creates the file descriptor of a file represented by 'fileStatus' with blocks
     * stored in 'blockLocations'. 'fileSystem' is the filesystem where the
     * file resides and 'hostIndex' stores the network addresses of the hosts that store
     * blocks of the parent HdfsTable. 'isEc' indicates whether the file is erasure-coded.
     * Populates 'numUnknownDiskIds' with the number of unknown disk ids.
     *
     *
     */
    /**
     * Creates a FileDescriptor with block locations.
     *
     * @param fileStatus the status returned from file listing
     * @param relPath the path of the file relative to the partition directory
     * @param blockLocations the block locations for the file
     * @param hostIndex the host index to use for encoding the hosts
     * @param isEc true if the file is known to be erasure-coded
     * @param numUnknownDiskIds reference which will be set to the number of blocks
     *                          for which no disk ID could be determined
     */
    public static FileDescriptor create(FileStatus fileStatus, String relPath,
        BlockLocation[] blockLocations, ListMap<TNetworkAddress> hostIndex, boolean isEc,
        Reference<Long> numUnknownDiskIds) throws IOException {
      FlatBufferBuilder fbb = new FlatBufferBuilder(1);
      int[] fbFileBlockOffsets = new int[blockLocations.length];
      int blockIdx = 0;
      for (BlockLocation loc: blockLocations) {
        if (isEc) {
          fbFileBlockOffsets[blockIdx++] = FileBlock.createFbFileBlock(fbb,
              loc.getOffset(), loc.getLength(),
              (short) hostIndex.getIndex(REMOTE_NETWORK_ADDRESS));
        } else {
          fbFileBlockOffsets[blockIdx++] =
              FileBlock.createFbFileBlock(fbb, loc, hostIndex, numUnknownDiskIds);
        }
      }
      return new FileDescriptor(createFbFileDesc(fbb, fileStatus, relPath,
          fbFileBlockOffsets, isEc));
    }

    /**
     * Creates the file descriptor of a file represented by 'fileStatus' that
     * resides in a filesystem that doesn't support the BlockLocation API (e.g. S3).
     */
    public static FileDescriptor createWithNoBlocks(FileStatus fileStatus,
        String relPath) {
      FlatBufferBuilder fbb = new FlatBufferBuilder(1);
      return new FileDescriptor(createFbFileDesc(fbb, fileStatus, relPath, null, false));
    }

    /**
     * Serializes the metadata of a file descriptor represented by 'fileStatus' into a
     * FlatBuffer using 'fbb' and returns the associated FbFileDesc object.
     * 'fbFileBlockOffsets' are the offsets of the serialized block metadata of this file
     * in the underlying buffer. Can be null if there are no blocks.
     */
    private static FbFileDesc createFbFileDesc(FlatBufferBuilder fbb,
        FileStatus fileStatus, String relPath, int[] fbFileBlockOffets, boolean isEc) {
      int relPathOffset = fbb.createString(relPath);
      // A negative block vector offset is used when no block offsets are specified.
      int blockVectorOffset = -1;
      if (fbFileBlockOffets != null) {
        blockVectorOffset = FbFileDesc.createFileBlocksVector(fbb, fbFileBlockOffets);
      }
      FbFileDesc.startFbFileDesc(fbb);
      // TODO(todd) rename to RelativePathin the FBS
      FbFileDesc.addRelativePath(fbb, relPathOffset);
      FbFileDesc.addLength(fbb, fileStatus.getLen());
      FbFileDesc.addLastModificationTime(fbb, fileStatus.getModificationTime());
      FbFileDesc.addIsEc(fbb, isEc);
      HdfsCompression comp = HdfsCompression.fromFileName(fileStatus.getPath().getName());
      FbFileDesc.addCompression(fbb, comp.toFb());
      if (blockVectorOffset >= 0) FbFileDesc.addFileBlocks(fbb, blockVectorOffset);
      fbb.finish(FbFileDesc.endFbFileDesc(fbb));
      // To eliminate memory fragmentation, copy the contents of the FlatBuffer to the
      // smallest possible ByteBuffer.
      ByteBuffer bb = fbb.dataBuffer().slice();
      ByteBuffer compressedBb = ByteBuffer.allocate(bb.capacity());
      compressedBb.put(bb);
      return FbFileDesc.getRootAsFbFileDesc((ByteBuffer)compressedBb.flip());
    }

    public String getRelativePath() { return fbFileDescriptor_.relativePath(); }
    public long getFileLength() { return fbFileDescriptor_.length(); }

    /** Compute the total length of files in fileDescs */
    public static long computeTotalFileLength(Collection<FileDescriptor> fileDescs) {
      long totalLength = 0;
      for (FileDescriptor fileDesc: fileDescs) {
        totalLength += fileDesc.getFileLength();
      }
      return totalLength;
    }

    public HdfsCompression getFileCompression() {
      return HdfsCompression.valueOf(FbCompression.name(fbFileDescriptor_.compression()));
    }

    public long getModificationTime() { return fbFileDescriptor_.lastModificationTime(); }
    public int getNumFileBlocks() { return fbFileDescriptor_.fileBlocksLength(); }
    public boolean getIsEc() {return fbFileDescriptor_.isEc(); }

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
      return MoreObjects.toStringHelper(this)
          .add("RelativePath", getRelativePath())
          .add("Length", getFileLength())
          .add("Compression", getFileCompression())
          .add("ModificationTime", getModificationTime())
          .add("Blocks", Joiner.on(", ").join(blocks)).toString();
    }

    @Override
    public int compareTo(FileDescriptor otherFd) {
      return getRelativePath().compareTo(otherFd.getRelativePath());
    }

    /**
     * Function to convert from a byte[] flatbuffer to the wrapper class. Note that
     * this returns a shallow copy which continues to reflect any changes to the
     * passed byte[].
     */
    public static final Function<byte[], FileDescriptor> FROM_BYTES =
        new Function<byte[], FileDescriptor>() {
          @Override
          public FileDescriptor apply(byte[] input) {
            ByteBuffer bb = ByteBuffer.wrap(input);
            return new FileDescriptor(FbFileDesc.getRootAsFbFileDesc(bb));
          }
        };

    /**
     * Function to convert from the wrapper class to a raw byte[]. Note that
     * this returns a shallow copy and callers should not modify the returned array.
     */
    public static final Function<FileDescriptor, byte[]> TO_BYTES =
        new Function<FileDescriptor, byte[]>() {
          @Override
          public byte[] apply(FileDescriptor fd) {
            ByteBuffer bb = fd.fbFileDescriptor_.getByteBuffer();
            byte[] arr = bb.array();
            assert bb.arrayOffset() == 0 && bb.remaining() == arr.length;
            return arr;
          }
        };
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
        return CatalogInterners.internNetworkAddress(
            new TNetworkAddress(ip_port[0], Integer.parseInt(ip_port[1])));
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
        replicaIdx = makeReplicaIdx(isReplicaCached, replicaIdx);
        fbb.addShort(replicaIdx);
      }
      int fbReplicaHostIdxOffset = fbb.endVector();
      short[] diskIds = createDiskIds(loc, numUnknownDiskIds);
      Preconditions.checkState(diskIds.length == loc.getNames().length,
          "Mismatch detected between number of diskIDs and block locations for block: " +
          loc.toString());
      int fbDiskIdsOffset = FbFileBlock.createDiskIdsVector(fbb, diskIds);
      FbFileBlock.startFbFileBlock(fbb);
      FbFileBlock.addOffset(fbb, loc.getOffset());
      FbFileBlock.addLength(fbb, loc.getLength());
      FbFileBlock.addReplicaHostIdxs(fbb, fbReplicaHostIdxOffset);
      FbFileBlock.addDiskIds(fbb, fbDiskIdsOffset);
      return FbFileBlock.endFbFileBlock(fbb);
    }

    private static short makeReplicaIdx(boolean isReplicaCached, int hostIdx) {
      Preconditions.checkArgument((hostIdx & REPLICA_HOST_IDX_MASK) == hostIdx,
          "invalid hostIdx: %s", hostIdx);
      return isReplicaCached ? (short) (hostIdx | ~REPLICA_HOST_IDX_MASK)
          : (short)hostIdx;
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
        Reference<Long> numUnknownDiskIds) throws IOException {
      long unknownDiskIdCount = 0;
      String[] storageIds = location.getStorageIds();
      String[] hosts = location.getHosts();
      if (storageIds.length != hosts.length) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Number of storage IDs and number of hosts for block " +
              "%s mismatch (storageIDs:hosts) %d:%d. Skipping disk ID loading for this " +
              "block.", location.toString(), storageIds.length, hosts.length));
        }
        storageIds = new String[hosts.length];
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
        CatalogInterners.internFieldsInPlace(sd);
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
        sdParameters = ImmutableMap.copyOf(CatalogInterners.internParameters(
            sd.getParameters()));
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

  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartition.class);

  // A predicate for checking if a given string is a key used for serializing
  // TPartitionStats to HMS parameters.
  private static Predicate<String> IS_INCREMENTAL_STATS_KEY =
      new Predicate<String>() {
        @Override
        public boolean apply(String key) {
          return key.startsWith(PartitionStatsUtil.INCREMENTAL_STATS_NUM_CHUNKS)
              || key.startsWith(PartitionStatsUtil.INCREMENTAL_STATS_CHUNK_PREFIX);
        }
      };

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
  private HdfsStorageDescriptor fileFormatDescriptor_;

  /**
   * The file descriptors of this partition, encoded as flatbuffers. Storing the raw
   * byte arrays here instead of the FileDescriptor object saves 100 bytes per file
   * given the following overhead:
   * - FileDescriptor object:
   *    - 16 byte object header
   *    - 8 byte reference to FbFileDesc
   * - FbFileDesc superclass:
   *    - 16 byte object header
   *    - 56-byte ByteBuffer
   *    - 4-byte padding (objects are word-aligned)
   */
  @Nonnull
  private ImmutableList<byte[]> encodedFileDescriptors_;
  private HdfsPartitionLocationCompressor.Location location_;
  private boolean isDirty_;
  // True if this partition is marked as cached. Does not necessarily mean the data is
  // cached.
  private boolean isMarkedCached_;
  private final TAccessLevel accessLevel_;

  // (k,v) pairs of parameters for this partition, stored in the HMS.
  private Map<String, String> hmsParameters_;

  // Binary representation of the TPartitionStats for this partition. Populated
  // when the partition is loaded and updated using setPartitionStatsBytes().
  private byte[] partitionStats_;

  // True if partitionStats_ has intermediate_col_stats populated.
  private boolean hasIncrementalStats_ ;

  // The last committed write ID which modified this partition.
  // -1 means writeId_ is irrelevant(not supported).
  private long writeId_ = -1L;

  private final InFlightEvents inFlightEvents_ = new InFlightEvents(20);

  private HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, long id,
      HdfsPartitionLocationCompressor.Location location, TAccessLevel accessLevel) {
    table_ = table;
    if (msPartition == null) {
      cachedMsPartitionDescriptor_ = null;
    } else {
      cachedMsPartitionDescriptor_ = new CachedHmsPartitionDescriptor(msPartition);
    }
    location_ = location;
    partitionKeyValues_ = ImmutableList.copyOf(partitionKeyValues);
    setFileDescriptors(fileDescriptors);
    fileFormatDescriptor_ = fileFormatDescriptor;
    id_ = id;
    accessLevel_ = accessLevel;
    if (msPartition != null && msPartition.getParameters() != null) {
      isMarkedCached_ = HdfsCachingUtil.getCacheDirectiveId(
          msPartition.getParameters()) != null;
      hmsParameters_ = msPartition.getParameters();
    } else {
      hmsParameters_ = new HashMap<>();
    }
    addInflightVersionsFromParameters();
    extractAndCompressPartStats();
    // Intern parameters after removing the incremental stats
    hmsParameters_ = CatalogInterners.internParameters(hmsParameters_);
    if (MetastoreShim.getMajorVersion() > 2 && msPartition != null) {
      writeId_ = MetastoreShim.getWriteIdFromMSPartition(msPartition);
    }
  }

  public HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors,
      TAccessLevel accessLevel) {
    this(table, msPartition, partitionKeyValues, fileFormatDescriptor, fileDescriptors,
        partitionIdCounter_.getAndIncrement(),
        table.getPartitionLocationCompressor().new Location(msPartition != null
                ? msPartition.getSd().getLocation()
                : table.getLocation()),
        accessLevel);
  }

  @Override // FeFsPartition
  public HdfsStorageDescriptor getInputFormatDescriptor() {
    return fileFormatDescriptor_;
  }

  @Override // FeFsPartition
  public boolean isCacheable() {
    return FileSystemUtil.isPathCacheable(new Path(getLocation()));
  }

  @Override // FeFsPartition
  public String getPartitionName() {
    // TODO: Consider storing the PartitionKeyValue in HdfsPartition. It would simplify
    // this code would be useful in other places, such as fromThrift().
    return FeCatalogUtils.getPartitionName(this);
  }

  @Override
  public List<String> getPartitionValuesAsStrings(boolean mapNullsToHiveKey) {
    return FeCatalogUtils.getPartitionValuesAsStrings(this, mapNullsToHiveKey);
  }

  @Override // FeFsPartition
  public String getConjunctSql() {
    // TODO: Remove this when the TODO elsewhere in this file to save and expose the
    // list of TPartitionKeyValues has been resolved.
    return FeCatalogUtils.getConjunctSqlForPartition(this);
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
  @Override
  public String getLocation() {
    return (location_ != null) ? location_.toString() : null;
  }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    return location_ != null ? location_.toThrift() : null;
  }

  @Override // FeFsPartition
  public Path getLocationPath() {
    Preconditions.checkNotNull(getLocation(), "HdfsPartition location is null");
    return new Path(getLocation());
  }

  @Override // FeFsPartition
  public long getId() { return id_; }

  @Override // FeFsPartition
  public HdfsTable getTable() { return table_; }

  @Override
  public FileSystemUtil.FsType getFsType() {
    Preconditions.checkNotNull(getLocationPath().toUri().getScheme(),
        "Cannot get scheme from path " + getLocationPath());
    return FileSystemUtil.FsType.getFsType(getLocationPath().toUri().getScheme());
  }

  public void setNumRows(long numRows) { numRows_ = numRows; }
  @Override // FeFsPartition
  public long getNumRows() { return numRows_; }
  @Override
  public boolean isMarkedCached() { return isMarkedCached_; }
  void markCached() { isMarkedCached_ = true; }
  private CachedHmsPartitionDescriptor cachedMsPartitionDescriptor_;

  /**
   * Updates the file format of this partition and sets the corresponding input/output
   * format classes.
   */
  public void setFileFormat(HdfsFileFormat fileFormat) {
    fileFormatDescriptor_ = fileFormatDescriptor_.cloneWithChangedFileFormat(
        fileFormat);
    cachedMsPartitionDescriptor_.sdInputFormat = fileFormat.inputFormat();
    cachedMsPartitionDescriptor_.sdOutputFormat = fileFormat.outputFormat();
    cachedMsPartitionDescriptor_.sdSerdeInfo.setSerializationLib(
        fileFormatDescriptor_.getFileFormat().serializationLib());
  }

  @Override // FeFsPartition
  public HdfsFileFormat getFileFormat() {
    return fileFormatDescriptor_.getFileFormat();
  }

  public void setLocation(String place) {
    location_ = table_.getPartitionLocationCompressor().new Location(place);
  }

  public org.apache.hadoop.hive.metastore.api.SerDeInfo getSerdeInfo() {
    return cachedMsPartitionDescriptor_.sdSerdeInfo;
  }

  @Override // FeFsPartition
  public TPartitionStats getPartitionStats() {
    return PartitionStatsUtil.getPartStatsOrWarn(this);
  }

  @Override
  public byte[] getPartitionStatsCompressed() {
    return partitionStats_;
  }

  public void setPartitionStatsBytes(byte[] partitionStats, boolean hasIncrStats) {
    if (hasIncrStats) Preconditions.checkNotNull(partitionStats);
    partitionStats_ = partitionStats;
    hasIncrementalStats_ = hasIncrStats;
  }

  /**
   * Helper method that removes the partition stats from hmsParameters_, compresses them
   * and updates partitionsStats_.
   */
  private void extractAndCompressPartStats() {
    try {
      // Convert the stats stored in the hmsParams map to a deflate-compressed in-memory
      // byte array format. After conversion, delete the entries in the hmsParams map
      // as they are not needed anymore.
      Reference<Boolean> hasIncrStats = new Reference<Boolean>(false);
      byte[] partitionStats =
          PartitionStatsUtil.partStatsBytesFromParameters(hmsParameters_, hasIncrStats);
      setPartitionStatsBytes(partitionStats, hasIncrStats.getRef());
    } catch (ImpalaException e) {
      LOG.warn(String.format("Failed to set partition stats for table %s partition %s",
          getTable().getFullName(), getPartitionName()), e);
    } finally {
      // Delete the incremental stats entries. Cleared even on error conditions so that
      // we do not persist the corrupt entries in the hmsParameters_ map when it is
      // flushed to the HMS.
      Maps.filterKeys(hmsParameters_, IS_INCREMENTAL_STATS_KEY).clear();
    }
  }

  public void dropPartitionStats() { setPartitionStatsBytes(null, false); }

  @Override // FeFsPartition
  public boolean hasIncrementalStats() { return hasIncrementalStats_; }

  @Override // FeFsPartition
  public TAccessLevel getAccessLevel() { return accessLevel_; }

  @Override // FeFsPartition
  public Map<String, String> getParameters() {
    // Once the TPartitionStats in the parameters map are converted to a compressed
    // format, the hmsParameters_ map should not contain any partition stats keys.
    // Even though filterKeys() is O(n), we are not worried about the performance here
    // since hmsParameters_ should be pretty small once the partition stats are removed.
    Preconditions.checkState(
        Maps.filterKeys(hmsParameters_, IS_INCREMENTAL_STATS_KEY).isEmpty());
    return hmsParameters_;
  }

  public void putToParameters(String k, String v) {
    Preconditions.checkArgument(!IS_INCREMENTAL_STATS_KEY.apply(k));
    hmsParameters_.put(k, v);
  }

  public void putToParameters(Pair<String, String> kv) {
    putToParameters(kv.first, kv.second);
  }

  /**
   * Removes a given version from the in-flight events
   * @param versionNumber version number to remove
   * @return true if the versionNumber was removed, false if it didn't exist
   */
  public boolean removeFromVersionsForInflightEvents(long versionNumber) {
    Preconditions.checkState(table_.getLock().isHeldByCurrentThread(),
        "removeFromVersionsForInflightEvents called without holding the table lock on "
            + "partition " + getPartitionName() + " of table " + table_.getFullName());
    return inFlightEvents_.remove(versionNumber);
  }

  /**
   * Adds a version number to the in-flight events of this partition
   * @param versionNumber version number to add
   */
  public void addToVersionsForInflightEvents(long versionNumber) {
    Preconditions.checkState(table_.getLock().isHeldByCurrentThread(),
        "addToVersionsForInflightEvents called without holding the table lock on "
            + "partition " + getPartitionName() + " of table " + table_.getFullName());
    if (!inFlightEvents_.add(versionNumber)) {
      LOG.warn(String.format("Could not add %s version to the partition %s of table %s. "
          + "This could cause unnecessary refresh of the partition when the event is"
          + "received by the Events processor.", versionNumber, getPartitionName(),
          getTable().getFullName()));
    }
  }

  /**
   * Adds the version from the given Partition parameters. No-op if the parameters does
   * not contain the <code>MetastoreEventPropertyKey.CATALOG_VERSION</code>. This is
   * done to detect add partition events from this catalog which are generated when
   * partitions are added or recovered.
   */
  private void addInflightVersionsFromParameters() {
    Preconditions.checkNotNull(hmsParameters_);
    Preconditions.checkState(inFlightEvents_.size() == 0);
    // we should not check for table lock being held here since there are certain code
    // paths which call this method without holding the table lock (eg. getOrLoadTable())
    if (!hmsParameters_.containsKey(MetastoreEventPropertyKey.CATALOG_VERSION.getKey())) {
      return;
    }
    inFlightEvents_.add(Long.parseLong(
            hmsParameters_.get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey())));
  }

  /**
   * Marks this partition's metadata as "dirty" indicating that changes have been
   * made and this partition's metadata should not be reused during the next
   * incremental metadata refresh.
   */
  public void markDirty() { isDirty_ = true; }
  public boolean isDirty() { return isDirty_; }

  @Override // FeFsPartition
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues_; }
  @Override // FeFsPartition
  public LiteralExpr getPartitionValue(int i) { return partitionKeyValues_.get(i); }

  @Override // FeFsPartition
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    // Return a lazily transformed list from our internal bytes storage.
    return Lists.transform(encodedFileDescriptors_, FileDescriptor.FROM_BYTES);
  }

  /**
   * Returns a set of fully qualified file names in the partition.
   */
  public Set<String> getFileNames() {
    List<FileDescriptor> fdList = getFileDescriptors();
    Set<String> fileNames = new HashSet<>(fdList.size());
    // Fully qualified file names.
    String location = getLocation();
    for (FileDescriptor fd : fdList) {
      fileNames.add(location + Path.SEPARATOR + fd.getRelativePath());
    }
    return fileNames;
  }

  public void setFileDescriptors(List<FileDescriptor> descriptors) {
    // Store an eagerly transformed-and-copied list so that we drop the memory usage
    // of the flatbuffer wrapper.
    encodedFileDescriptors_ = ImmutableList.copyOf(Lists.transform(
        descriptors, FileDescriptor.TO_BYTES));
  }

  @Override // FeFsPartition
  public int getNumFileDescriptors() {
    return encodedFileDescriptors_.size();
  }

  @Override
  public boolean hasFileDescriptors() { return !encodedFileDescriptors_.isEmpty(); }

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
          // Make a shallow copy of the field schemas instead of passing a reference to
          // the source list since it could potentially be modified once the current
          // thread is out of table lock scope.
            new ArrayList<>(table_.getNonPartitionFieldSchemas()),
            getLocation(),
            cachedMsPartitionDescriptor_.sdInputFormat,
            cachedMsPartitionDescriptor_.sdOutputFormat,
            cachedMsPartitionDescriptor_.sdCompressed,
            cachedMsPartitionDescriptor_.sdNumBuckets,
            cachedMsPartitionDescriptor_.sdSerdeInfo,
            cachedMsPartitionDescriptor_.sdBucketCols,
            cachedMsPartitionDescriptor_.sdSortCols,
            cachedMsPartitionDescriptor_.sdParameters);
    // Make a copy so that the callers do not need to delete the incremental stats
    // strings from the hmsParams_ map later.
    Map<String, String> hmsParams = Maps.newHashMap(getParameters());
    PartitionStatsUtil.partStatsToParams(this, hmsParams);
    org.apache.hadoop.hive.metastore.api.Partition partition =
        new org.apache.hadoop.hive.metastore.api.Partition(
            getPartitionValuesAsStrings(true), getTable().getDb().getName(),
            getTable().getName(), cachedMsPartitionDescriptor_.msCreateTime,
            cachedMsPartitionDescriptor_.msLastAccessTime, storageDescriptor,
            hmsParams);
    return partition;
  }

  public static HdfsPartition prototypePartition(
      HdfsTable table, HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = new ArrayList<>();
    List<FileDescriptor> emptyFileDescriptorList = new ArrayList<>();
    return new HdfsPartition(table, null, emptyExprList,
        storageDescriptor, emptyFileDescriptorList,
        CatalogObjectsConstants.PROTOTYPE_PARTITION_ID, null,
        TAccessLevel.READ_WRITE);
  }

  @Override
  public long getSize() {
    long result = 0;
    for (HdfsPartition.FileDescriptor fileDescriptor: getFileDescriptors()) {
      result += fileDescriptor.getFileLength();
    }
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("fileDescriptors", getFileDescriptors())
      .toString();
  }

  public static HdfsPartition fromThrift(HdfsTable table,
      long id, THdfsPartition thriftPartition) {
    HdfsStorageDescriptor storageDesc = HdfsStorageDescriptor.fromThriftPartition(
        thriftPartition, table.getName());

    List<LiteralExpr> literalExpr = new ArrayList<>();
    if (id != CatalogObjectsConstants.PROTOTYPE_PARTITION_ID) {
      List<Column> clusterCols = new ArrayList<>();
      for (int i = 0; i < table.getNumClusteringCols(); ++i) {
        clusterCols.add(table.getColumns().get(i));
      }

      List<TExprNode> exprNodes = new ArrayList<>();
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

    List<HdfsPartition.FileDescriptor> fileDescriptors = new ArrayList<>();
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
      partition.hmsParameters_ = CatalogInterners.internParameters(
          thriftPartition.getHms_parameters());
    } else {
      partition.hmsParameters_ = new HashMap<>();
    }

    partition.hasIncrementalStats_ = thriftPartition.has_incremental_stats;
    if (thriftPartition.isSetPartition_stats()) {
      partition.partitionStats_ = thriftPartition.getPartition_stats();
    }

    partition.writeId_ = thriftPartition.isSetWrite_id() ?
        thriftPartition.getWrite_id() : -1L;

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

  /**
   * Comparator to allow ordering of partitions by their partition-key values.
   */
  public static final KeyValueComparator KV_COMPARATOR = new KeyValueComparator();
  public static class KeyValueComparator implements Comparator<FeFsPartition> {
    @Override
    public int compare(FeFsPartition o1, FeFsPartition o2) {
      return comparePartitionKeyValues(o1.getPartitionValues(), o2.getPartitionValues());
    }
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

  @Override
  public long getWriteId() {
    return writeId_;
  }
}
