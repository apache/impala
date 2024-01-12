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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.flatbuffers.FlatBufferBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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
import org.apache.impala.fb.FbFileMetadata;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.ListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query-relevant information for one table partition. Partitions are comparable
 * based on their partition-key values. The comparison orders partitions in ascending
 * order with NULLs sorting last. The ordering is useful for displaying partitions
 * in SHOW statements.
 * This class is supposed to be immutable. We should use HdfsPartition.Builder to create
 * new instances instead of updating the fields in-place.
 * This class extends CatalogObjectImpl so catalogd can send partition metadata
 * individually instead of carrying them inside the HdfsTable thrift objects. However, we
 * don't explicitly have a different catalog version for Partitions - all the partitions
 * of a HdfsTable will have the same catalogVersion as its parent table, because we use
 * the partition id to identify a partition instance (snapshot). The catalog versions are
 * not used actually.
 */
public class HdfsPartition extends CatalogObjectImpl
    implements FeFsPartition, PrunablePartition {
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

    // Internal representation of additional file metadata, e.g. Iceberg metadata.
    private final FbFileMetadata fbFileMetadata_;

    private FileDescriptor(FbFileDesc fileDescData) {
      fbFileDescriptor_ = fileDescData;
      fbFileMetadata_ = null;
    }

    private FileDescriptor(FbFileDesc fileDescData, FbFileMetadata fileMetadata) {
      fbFileDescriptor_ = fileDescData;
      fbFileMetadata_ = fileMetadata;
    }

    public static FileDescriptor fromThrift(THdfsFileDesc desc) {
      ByteBuffer bb = ByteBuffer.wrap(desc.getFile_desc_data());
      if (desc.isSetFile_metadata()) {
        ByteBuffer bbMd = ByteBuffer.wrap(desc.getFile_metadata());
        return new FileDescriptor(FbFileDesc.getRootAsFbFileDesc(bb),
                                  FbFileMetadata.getRootAsFbFileMetadata(bbMd));
      }
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
          int newHostIdx = dstIndex.getOrAddIndex(origHost);
          it.mutateReplicaHostIdxs(j, FileBlock.makeReplicaIdx(isCached, newHostIdx));
        }
      }
      return new FileDescriptor(cloned, fbFileMetadata_);
    }

    public FileDescriptor cloneWithFileMetadata(FbFileMetadata fileMetadata) {
      return new FileDescriptor(fbFileDescriptor_, fileMetadata);
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
        BlockLocation[] blockLocations, ListMap<TNetworkAddress> hostIndex,
        boolean isEncrypted, boolean isEc, Reference<Long> numUnknownDiskIds,
        String absPath) throws IOException {
      FlatBufferBuilder fbb = new FlatBufferBuilder(1);
      int[] fbFileBlockOffsets = new int[blockLocations.length];
      int blockIdx = 0;
      for (BlockLocation loc: blockLocations) {
        if (isEc) {
          fbFileBlockOffsets[blockIdx++] = FileBlock.createFbFileBlock(fbb,
              loc.getOffset(), loc.getLength(),
              (short) hostIndex.getOrAddIndex(REMOTE_NETWORK_ADDRESS));
        } else {
          fbFileBlockOffsets[blockIdx++] =
              FileBlock.createFbFileBlock(fbb, loc, hostIndex, numUnknownDiskIds);
        }
      }
      return new FileDescriptor(createFbFileDesc(fbb, fileStatus, relPath,
          fbFileBlockOffsets, isEncrypted, isEc, absPath));
    }

    /**
     * Creates the file descriptor of a file represented by 'fileStatus' that
     * resides in a filesystem that doesn't support the BlockLocation API (e.g. S3).
     */
    public static FileDescriptor createWithNoBlocks(
        FileStatus fileStatus, String relPath, String absPath) {
      FlatBufferBuilder fbb = new FlatBufferBuilder(1);
      return new FileDescriptor(
          createFbFileDesc(fbb, fileStatus, relPath, null, false, false, absPath));
    }
    /**
     * Serializes the metadata of a file descriptor represented by 'fileStatus' into a
     * FlatBuffer using 'fbb' and returns the associated FbFileDesc object.
     * 'fbFileBlockOffsets' are the offsets of the serialized block metadata of this file
     * in the underlying buffer. Can be null if there are no blocks.
     */
    private static FbFileDesc createFbFileDesc(FlatBufferBuilder fbb,
        FileStatus fileStatus, String relPath, int[] fbFileBlockOffsets,
        boolean isEncrypted, boolean isEc, String absPath) {
      int relPathOffset = fbb.createString(relPath == null ? StringUtils.EMPTY : relPath);
      // A negative block vector offset is used when no block offsets are specified.
      int blockVectorOffset = -1;
      if (fbFileBlockOffsets != null) {
        blockVectorOffset = FbFileDesc.createFileBlocksVector(fbb, fbFileBlockOffsets);
      }
      int absPathOffset = -1;
      if (StringUtils.isNotEmpty(absPath)) absPathOffset = fbb.createString(absPath);
      FbFileDesc.startFbFileDesc(fbb);
      // TODO(todd) rename to RelativePath in the FBS
      FbFileDesc.addRelativePath(fbb, relPathOffset);
      FbFileDesc.addLength(fbb, fileStatus.getLen());
      FbFileDesc.addLastModificationTime(fbb, fileStatus.getModificationTime());
      FbFileDesc.addIsEncrypted(fbb, isEncrypted);
      FbFileDesc.addIsEc(fbb, isEc);
      HdfsCompression comp = HdfsCompression.fromFileName(fileStatus.getPath().getName());
      FbFileDesc.addCompression(fbb, comp.toFb());
      if (blockVectorOffset >= 0) FbFileDesc.addFileBlocks(fbb, blockVectorOffset);
      if (absPathOffset >= 0) FbFileDesc.addAbsolutePath(fbb, absPathOffset);
      fbb.finish(FbFileDesc.endFbFileDesc(fbb));
      // To eliminate memory fragmentation, copy the contents of the FlatBuffer to the
      // smallest possible ByteBuffer.
      ByteBuffer bb = fbb.dataBuffer().slice();
      ByteBuffer compressedBb = ByteBuffer.allocate(bb.capacity());
      compressedBb.put(bb);
      return FbFileDesc.getRootAsFbFileDesc((ByteBuffer) compressedBb.flip());
    }

    public String getRelativePath() { return fbFileDescriptor_.relativePath(); }

    public String getAbsolutePath() {
      return StringUtils.isEmpty(fbFileDescriptor_.absolutePath()) ?
          StringUtils.EMPTY :
          fbFileDescriptor_.absolutePath();
    }

    public String getAbsolutePath(String rootPath) {
      if (StringUtils.isEmpty(fbFileDescriptor_.relativePath())
          && StringUtils.isNotEmpty(fbFileDescriptor_.absolutePath())) {
        return fbFileDescriptor_.absolutePath();
      } else {
        return rootPath + Path.SEPARATOR + fbFileDescriptor_.relativePath();
      }
    }

    public String getPath() {
      if (StringUtils.isEmpty(fbFileDescriptor_.relativePath())
          && StringUtils.isNotEmpty(fbFileDescriptor_.absolutePath())) {
        return fbFileDescriptor_.absolutePath();
      } else {
        return fbFileDescriptor_.relativePath();
      }
    }

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
    public boolean getIsEncrypted() {return fbFileDescriptor_.isEncrypted(); }
    public boolean getIsEc() {return fbFileDescriptor_.isEc(); }

    public FbFileBlock getFbFileBlock(int idx) {
      return fbFileDescriptor_.fileBlocks(idx);
    }

    public FbFileMetadata getFbFileMetadata() {
      return fbFileMetadata_;
    }

    public THdfsFileDesc toThrift() {
      THdfsFileDesc fd = new THdfsFileDesc();
      ByteBuffer bb = fbFileDescriptor_.getByteBuffer();
      fd.setFile_desc_data(bb);
      if (fbFileMetadata_ != null) {
        fd.setFile_metadata(fbFileMetadata_.getByteBuffer());
      }
      return fd;
    }

    @Override
    public String toString() {
      int numFileBlocks = getNumFileBlocks();
      List<String> blocks = Lists.newArrayListWithCapacity(numFileBlocks);
      for (int i = 0; i < numFileBlocks; ++i) {
        blocks.add(FileBlock.debugString(getFbFileBlock(i)));
      }
      ToStringHelper stringHelper = MoreObjects.toStringHelper(this)
          .add("RelativePath", getRelativePath())
          .add("Length", getFileLength())
          .add("Compression", getFileCompression())
          .add("ModificationTime", getModificationTime())
          .add("Blocks", Joiner.on(", ").join(blocks));
      if (StringUtils.isNotEmpty(getAbsolutePath())) {
        stringHelper.add("AbsolutePath", getAbsolutePath());
      }
      return stringHelper.toString();
    }

    @Override
    public int compareTo(FileDescriptor otherFd) {
      return getPath().compareTo(otherFd.getPath());
    }

    /**
     * Compares the modification time and file size between current FileDescriptor and the
     * latest FileStatus to determine if the file has changed. Returns true if the file
     * has changed and false otherwise. Note that block location changes are not
     * considered as file changes. Table reloading won't recognize block location changes
     * which require an INVALIDATE METADATA command on the table to clear the stale
     * locations.
     */
    public boolean isChanged(FileStatus latestStatus) {
      return latestStatus == null || getFileLength() != latestStatus.getLen()
          || getModificationTime() != latestStatus.getModificationTime();
    }

    /**
     * Same as above but compares to a FileDescriptor instance.
     */
    public boolean isChanged(FileDescriptor latestFd) {
      return latestFd == null || getFileLength() != latestFd.getFileLength()
          || getModificationTime() != latestFd.getModificationTime();
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
        short replicaIdx = (short) hostIndex.getOrAddIndex(networkAddress);
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
    public final ImmutableList<String> sdBucketCols;
    public final ImmutableList<org.apache.hadoop.hive.metastore.api.Order> sdSortCols;
    public final ImmutableMap<String, String> sdParameters;
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

    private CachedHmsPartitionDescriptor(CachedHmsPartitionDescriptor other) {
      sdInputFormat = other.sdInputFormat;
      sdOutputFormat = other.sdOutputFormat;
      sdCompressed = other.sdCompressed;
      sdNumBuckets = other.sdNumBuckets;
      sdSerdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo(other.sdSerdeInfo);
      sdBucketCols = other.sdBucketCols;
      sdSortCols = other.sdSortCols;
      sdParameters = ImmutableMap.copyOf(other.sdParameters);
      msCreateTime = other.msCreateTime;
      msLastAccessTime = other.msLastAccessTime;
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

  public final static long INITIAL_PARTITION_ID = 0;
  private final static AtomicLong partitionIdCounter_ = new AtomicLong();
  private final HdfsTable table_;
  private final ImmutableList<LiteralExpr> partitionKeyValues_;
  // Partition name generated from the partition keys and 'partitionKeyValues_'.
  // An example is 'p1=v1/p2=v2/p3=v3'. Use this to avoid generating the name repeatedly.
  private final String partName_;
  // estimated number of rows in partition; -1: unknown
  private final long numRows_;

  // A unique ID across the whole catalog for each partition, used to identify a partition
  // in the thrift representation of a table.
  private final long id_;
  // The partition id of the previous instance that is replaced by this. Used to send
  // partition level invalidation for catalog-v2 coordinators.
  private final long prevId_;

  /*
   * Note: Although you can write multiple formats to a single partition (by changing
   * the format before each write), Hive won't let you read that data and neither should
   * we. We should therefore treat mixing formats inside one partition as user error.
   * It's easy to add per-file metadata to FileDescriptor if this changes.
   */
  private final HdfsStorageDescriptor fileFormatDescriptor_;

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
  private final ImmutableList<byte[]> encodedFileDescriptors_;
  private final ImmutableList<byte[]> encodedInsertFileDescriptors_;
  private final ImmutableList<byte[]> encodedDeleteFileDescriptors_;
  private final HdfsPartitionLocationCompressor.Location location_;
  // True if this partition is marked as cached. Does not necessarily mean the data is
  // cached.
  private final boolean isMarkedCached_;
  private final TAccessLevel accessLevel_;

  // (k,v) pairs of parameters for this partition, stored in the HMS.
  private final ImmutableMap<String, String> hmsParameters_;
  private final CachedHmsPartitionDescriptor cachedMsPartitionDescriptor_;

  // Binary representation of the TPartitionStats for this partition. Populated
  // when the partition is being built in Builder#setPartitionStatsBytes().
  private final byte[] partitionStats_;

  // True if partitionStats_ has intermediate_col_stats populated.
  private final boolean hasIncrementalStats_ ;

  // The last committed write ID which modified this partition.
  // -1 means writeId_ is irrelevant(not supported).
  private final long writeId_;

  // The in-flight events of this partition tracked in catalogd. It's still mutable since
  // it's not used in coordinators.
  private final InFlightEvents inFlightEvents_;

  // event id in hive metastore which pertains to the creation of this partition object.
  // if this partition is not created by this catalogd or if the events processing is
  // not active this is set to -1.
  private final long createEventId_;

  // The last committed compaction ID
  // -1 means there is no previous compaction event or compaction is not supported
  private final long lastCompactionId_;

  // The last refresh event id of the partition
  // -1 means there is no previous refresh event happened
  private final long lastRefreshEventId_;

  /**
   * Constructor.  Needed for third party extensions that want to use their own builder
   * to construct the object.
   */
  protected HdfsPartition(HdfsTable table, long prevId, String partName,
      List<LiteralExpr> partitionKeyValues, HdfsStorageDescriptor fileFormatDescriptor,
      @Nonnull ImmutableList<byte[]> encodedFileDescriptors,
      ImmutableList<byte[]> encodedInsertFileDescriptors,
      ImmutableList<byte[]> encodedDeleteFileDescriptors,
      HdfsPartitionLocationCompressor.Location location,
      boolean isMarkedCached, TAccessLevel accessLevel, Map<String, String> hmsParameters,
      CachedHmsPartitionDescriptor cachedMsPartitionDescriptor,
      byte[] partitionStats, boolean hasIncrementalStats, long numRows, long writeId,
      InFlightEvents inFlightEvents) {
    this(table, partitionIdCounter_.getAndIncrement(), prevId, partName,
        partitionKeyValues, fileFormatDescriptor, encodedFileDescriptors,
        encodedInsertFileDescriptors, encodedDeleteFileDescriptors, location,
        isMarkedCached, accessLevel, hmsParameters, cachedMsPartitionDescriptor,
        partitionStats, hasIncrementalStats, numRows, writeId,
        inFlightEvents, /*createEventId=*/-1L, /*lastCompactionId*/-1L,
        /*lastRefreshEventId*/-1L);
  }

  protected HdfsPartition(HdfsTable table, long id, long prevId, String partName,
      List<LiteralExpr> partitionKeyValues, HdfsStorageDescriptor fileFormatDescriptor,
      @Nonnull ImmutableList<byte[]> encodedFileDescriptors,
      ImmutableList<byte[]> encodedInsertFileDescriptors,
      ImmutableList<byte[]> encodedDeleteFileDescriptors,
      HdfsPartitionLocationCompressor.Location location,
      boolean isMarkedCached, TAccessLevel accessLevel, Map<String, String> hmsParameters,
      CachedHmsPartitionDescriptor cachedMsPartitionDescriptor,
      byte[] partitionStats, boolean hasIncrementalStats, long numRows, long writeId,
      InFlightEvents inFlightEvents, long createEventId, long lastCompactionId) {
    this(table, partitionIdCounter_.getAndIncrement(), prevId, partName,
        partitionKeyValues, fileFormatDescriptor, encodedFileDescriptors,
        encodedInsertFileDescriptors, encodedDeleteFileDescriptors, location,
        isMarkedCached, accessLevel, hmsParameters, cachedMsPartitionDescriptor,
        partitionStats, hasIncrementalStats, numRows, writeId,
        inFlightEvents, /*createEventId=*/-1L, /*lastCompactionId*/-1L,
        /*lastRefreshEventId*/-1L);
  }

  protected HdfsPartition(HdfsTable table, long id, long prevId, String partName,
      List<LiteralExpr> partitionKeyValues, HdfsStorageDescriptor fileFormatDescriptor,
      @Nonnull ImmutableList<byte[]> encodedFileDescriptors,
      ImmutableList<byte[]> encodedInsertFileDescriptors,
      ImmutableList<byte[]> encodedDeleteFileDescriptors,
      HdfsPartitionLocationCompressor.Location location,
      boolean isMarkedCached, TAccessLevel accessLevel, Map<String, String> hmsParameters,
      CachedHmsPartitionDescriptor cachedMsPartitionDescriptor,
      byte[] partitionStats, boolean hasIncrementalStats, long numRows, long writeId,
      InFlightEvents inFlightEvents, long createEventId, long lastCompactionId,
      long lastRefreshEventId) {
    table_ = table;
    id_ = id;
    prevId_ = prevId;
    partitionKeyValues_ = ImmutableList.copyOf(partitionKeyValues);
    fileFormatDescriptor_ = fileFormatDescriptor;
    encodedFileDescriptors_ = encodedFileDescriptors;
    encodedInsertFileDescriptors_ = encodedInsertFileDescriptors;
    encodedDeleteFileDescriptors_ = encodedDeleteFileDescriptors;
    location_ = location;
    isMarkedCached_ = isMarkedCached;
    accessLevel_ = accessLevel;
    hmsParameters_ = ImmutableMap.copyOf(hmsParameters);
    cachedMsPartitionDescriptor_ = cachedMsPartitionDescriptor;
    partitionStats_ = partitionStats;
    hasIncrementalStats_ = hasIncrementalStats;
    numRows_ = numRows;
    writeId_ = writeId;
    inFlightEvents_ = inFlightEvents;
    createEventId_ = createEventId;
    lastCompactionId_ = lastCompactionId;
    lastRefreshEventId_ = lastRefreshEventId;
    if (partName == null && id_ != CatalogObjectsConstants.PROTOTYPE_PARTITION_ID) {
      partName_ = FeCatalogUtils.getPartitionName(this);
    } else {
      partName_ = partName;
    }
  }

  public long getCreateEventId() { return createEventId_; }

  public long getLastRefreshEventId() { return lastRefreshEventId_; }

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
    return partName_;
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

  @Override // FeFsPartition
  public ListMap<TNetworkAddress> getHostIndex() { return table_.getHostIndex(); }

  @Override
  public FileSystemUtil.FsType getFsType() {
    Path location = getLocationPath();
    Preconditions.checkNotNull(location.toUri().getScheme(),
        "Cannot get scheme from path " + location);
    return FileSystemUtil.FsType.getFsType(location.toUri().getScheme());
  }

  @Override // FeFsPartition
  public long getNumRows() { return numRows_; }
  @Override
  public boolean isMarkedCached() { return isMarkedCached_; }

  @Override // FeFsPartition
  public HdfsFileFormat getFileFormat() {
    return fileFormatDescriptor_.getFileFormat();
  }

  @Override // FeFsPartition
  public TPartitionStats getPartitionStats() {
    return PartitionStatsUtil.getPartStatsOrWarn(this);
  }

  @Override
  public byte[] getPartitionStatsCompressed() {
    return partitionStats_;
  }

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

  /**
   * Removes a given version from the in-flight events
   * @param isInsertEvent If true, remove eventId from list of eventIds for in-flight
   * Insert events. If false, remove version number from list of versions for in-flight
   * DDL events.
   * @param versionNumber when isInsertEvent is true, it's eventId to remove
   *                      when isInsertEvent is false, it's version number to
   *                      remove.
   * @return true if the versionNumber was removed, false if it didn't exist
   */
  public boolean removeFromVersionsForInflightEvents(
      boolean isInsertEvent, long versionNumber) {
    Preconditions.checkState(table_.isWriteLockedByCurrentThread(),
        "removeFromVersionsForInflightEvents called without holding the table lock on "
            + "partition " + getPartitionName() + " of table " + table_.getFullName());
    boolean ret = inFlightEvents_.remove(isInsertEvent, versionNumber);
    if (!ret) {
      LOG.trace("Failed to remove in-flight version number {}: in-flight events: {}",
          versionNumber, inFlightEvents_.print());
    }
    return ret;
  }

  /**
   * Adds a version number to the in-flight events of this partition
   * @param isInsertEvent if true, add eventId to list of eventIds for in-flight Insert
   * events if false, add version number to list of versions for in-flight DDL events
   * @param versionNumber when isInsertEvent is true, it's eventId to add
   *                      when isInsertEvent is false, it's version number to add
   */
  public void addToVersionsForInflightEvents(boolean isInsertEvent, long versionNumber) {
    Preconditions.checkState(table_.isWriteLockedByCurrentThread(),
        "addToVersionsForInflightEvents called without holding the table lock on "
            + "partition " + getPartitionName() + " of table " + table_.getFullName());
    boolean added = inFlightEvents_.add(isInsertEvent, versionNumber);
    if (!added) {
      LOG.warn(String.format("Could not add %s version to the partition %s of table %s. "
          + "This could cause unnecessary refresh of the partition when the event is"
          + "received by the Events processor.", versionNumber, getPartitionName(),
          getTable().getFullName()));
    }
    LOG.trace("{} {} to in-flight list {}",
        (added ? "Added" : "Could not add"), versionNumber, inFlightEvents_.print());
  }

  @Override // FeFsPartition
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues_; }
  @Override // FeFsPartition
  public LiteralExpr getPartitionValue(int i) { return partitionKeyValues_.get(i); }

  @Override // FeFsPartition
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    // Return a lazily transformed list from our internal bytes storage.
    List<HdfsPartition.FileDescriptor> ret = new ArrayList<>();
    ret.addAll(Lists.transform(encodedFileDescriptors_, FileDescriptor.FROM_BYTES));
    ret.addAll(Lists.transform(encodedInsertFileDescriptors_, FileDescriptor.FROM_BYTES));
    ret.addAll(Lists.transform(encodedDeleteFileDescriptors_, FileDescriptor.FROM_BYTES));
    return ret;
  }

  @Override // FeFsPartition
  public List<HdfsPartition.FileDescriptor> getInsertFileDescriptors() {
    // Return a lazily transformed list from our internal bytes storage.
    return Lists.transform(encodedInsertFileDescriptors_, FileDescriptor.FROM_BYTES);
  }

  @Override // FeFsPartition
  public List<HdfsPartition.FileDescriptor> getDeleteFileDescriptors() {
    // Return a lazily transformed list from our internal bytes storage.
    return Lists.transform(encodedDeleteFileDescriptors_, FileDescriptor.FROM_BYTES);
  }

  public long getLastCompactionId() {
    return lastCompactionId_;
  }

  /**
   * Returns a set of fully qualified file names in the partition.
   */
  public Set<String> getFileNames() {
    List<FileDescriptor> fdList = getFileDescriptors();
    Set<String> fileNames = new HashSet<>(fdList.size());
    // Fully qualified file names.
    for (FileDescriptor fd : fdList) {
      fileNames.add(fd.getAbsolutePath(getLocation()));
    }
    return fileNames;
  }

  @Override // FeFsPartition
  public int getNumFileDescriptors() {
    return encodedFileDescriptors_.size() +
           encodedInsertFileDescriptors_.size() +
           encodedDeleteFileDescriptors_.size();
  }

  @Override
  public boolean hasFileDescriptors() {
    return !encodedFileDescriptors_.isEmpty() ||
           !encodedInsertFileDescriptors_.isEmpty();
  }

  public CachedHmsPartitionDescriptor getCachedMsPartitionDescriptor() {
    return cachedMsPartitionDescriptor_;
  }

  public void setPartitionMetadata(TPartialPartitionInfo tPart) {
    // The special "prototype partition" or the only partition of an unpartitioned table
    // have a null cachedMsPartitionDescriptor.
    if (cachedMsPartitionDescriptor_ == null) return;
    // Don't need to make a copy here since the caller should not modify the parameters.
    tPart.hms_parameters = getParameters();
    tPart.write_id = writeId_;
    tPart.hdfs_storage_descriptor = fileFormatDescriptor_.toThrift();
    tPart.location = getLocationAsThrift();
  }

  /**
   * Returns a Hive-compatible partition object that may be used in calls to the
   * metastore.
   */
  public org.apache.hadoop.hive.metastore.api.Partition toHmsPartition() {
    StorageDescriptor storageDescriptor = getStorageDescriptor();
    if (storageDescriptor == null) return null;
    // Make a copy so that the callers can modify the parameters as needed.
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

  /**
   * Gets the StorageDescriptor for this partition. Useful for sending RPCs to metastore
   * and comparing against the partitions from metastore.
   */
  public StorageDescriptor getStorageDescriptor() {
    if (cachedMsPartitionDescriptor_ == null) return null;
    Preconditions.checkNotNull(table_.getNonPartitionFieldSchemas());
    // Update the serde library class based on the currently used file format.
    StorageDescriptor storageDescriptor =
        new StorageDescriptor(
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
    return storageDescriptor;
  }

  /**
   * Compares the {@link StorageDescriptor} of this partition with the one provided.
   * We only care of some fields of the StorageDescriptor (eg.
   * {@link org.apache.hadoop.hive.metastore.api.SkewedInfo} is not used) which are
   * relevant to determine of this HdfsPartition's storage descriptor
   * is same as the one provided.
   * @param hmsSd The StorageDescriptor object to compare against. Typically, this is
   *              fetched directly from HMS.
   * @return true if the HdfsPartition's StorageDescriptor is identical to the given
   * StorageDescriptor, false otherwise.
   */
  public boolean compareSd(StorageDescriptor hmsSd) {
    Preconditions.checkNotNull(hmsSd);
    StorageDescriptor sd = getStorageDescriptor();
    if (sd == null) return false;
    if (!sd.getCols().equals(hmsSd.getCols())) return false;
    if (!sd.getLocation().equals(hmsSd.getLocation())) return false;
    if (!sd.getInputFormat().equals(hmsSd.getInputFormat())) return false;
    if (!sd.getOutputFormat().equals(hmsSd.getOutputFormat())) return false;
    if (sd.isCompressed() != hmsSd.isCompressed()) return false;
    if (sd.getNumBuckets() != hmsSd.getNumBuckets()) return false;
    if (!sd.getSerdeInfo().equals(hmsSd.getSerdeInfo())) return false;
    if (!sd.getBucketCols().equals(hmsSd.getBucketCols())) return false;
    if (!sd.getSortCols().equals(hmsSd.getSortCols())) return false;
    return sd.getParameters().equals(hmsSd.getParameters());
  }

  public static HdfsPartition prototypePartition(
      HdfsTable table, HdfsStorageDescriptor storageDescriptor) {
    return new Builder(table, CatalogObjectsConstants.PROTOTYPE_PARTITION_ID)
        .setFileFormatDescriptor(storageDescriptor)
        .build();
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
  public long getWriteId() {
    return writeId_;
  }

  @Override
  public HdfsPartition genInsertDeltaPartition() {
    ImmutableList<byte[]> fileDescriptors = !encodedInsertFileDescriptors_.isEmpty() ?
        encodedInsertFileDescriptors_ : encodedFileDescriptors_;
    return new HdfsPartition.Builder(this)
        .setId(id_)
        .clearFileDescriptors()
        .setFileDescriptors(fileDescriptors)
        .build();
  }

  @Override
  public HdfsPartition genDeleteDeltaPartition() {
    if (encodedDeleteFileDescriptors_.isEmpty()) return null;
    return new HdfsPartition.Builder(this)
        .setId(id_)
        .clearFileDescriptors()
        .setFileDescriptors(encodedDeleteFileDescriptors_)
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("fileDescriptors", getFileDescriptors())
      .toString();
  }

  @Override // CatalogObjectImpl
  final public long getCatalogVersion() {
    throw new UnsupportedOperationException(
        "Catalog version of the partition should not be used");
  }

  @Override // CatalogObjectImpl
  final public void setCatalogVersion(long version) {
    throw new UnsupportedOperationException(
        "Catalog version of the partition should not be set");
  }

  @Override // CatalogObjectImpl
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setHdfs_partition(
        FeCatalogUtils.fsPartitionToThrift(this, ThriftObjectType.FULL));
    // Set the db and table names here instead of in FeCatalogUtils.fsPartitionToThrift(),
    // because FeCatalogUtils.fsPartitionToThrift() is also used in generating DDL
    // responses which don't require storing the names in each partition update.
    // Note that DDL responses have full (not incremental) table metadata.
    catalogObject.getHdfs_partition().setDb_name(table_.getDb().getName());
    catalogObject.getHdfs_partition().setTbl_name(table_.getName());
    catalogObject.getHdfs_partition().setPartition_name(partName_);
    catalogObject.getHdfs_partition().setId(id_);
    if (prevId_ != INITIAL_PARTITION_ID - 1) {
      catalogObject.getHdfs_partition().setPrev_id(prevId_);
    }
  }

  @Override // CatalogObject
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.HDFS_PARTITION;
  }

  public TCatalogObject toMinimalTCatalogObject() {
    // The catalog version is not used.
    TCatalogObject catalogPart = new TCatalogObject(TCatalogObjectType.HDFS_PARTITION,
        table_.getCatalogVersion());
    catalogPart.setHdfs_partition(toMinimalTHdfsPartition());
    return catalogPart;
  }

  public THdfsPartition toMinimalTHdfsPartition() {
    THdfsPartition part = new THdfsPartition();
    part.setDb_name(table_.getDb().getName());
    part.setTbl_name(table_.getName());
    part.setPartition_name(partName_);
    part.setId(id_);
    return part;
  }

  /**
   * Generate a new instance with the minimal metadata for toMinimalTHdfsPartition().
   */
  public HdfsPartition genMinimalPartition() {
    return new HdfsPartition.Builder(table_, id_)
        .setPartitionName(partName_)
        .setIsMinimalMode(true)
        .build();
  }

  protected HdfsPartitionLocationCompressor.Location getLocationStruct() {
    return location_;
  }

  public static class Builder {
    // For the meaning of these fields, see field comments of HdfsPartition.
    private HdfsTable table_;
    private long id_;
    private long prevId_ = INITIAL_PARTITION_ID - 1;
    private String partName_ = null;
    private List<LiteralExpr> partitionKeyValues_;
    private HdfsStorageDescriptor fileFormatDescriptor_ = null;
    private ImmutableList<byte[]> encodedFileDescriptors_;
    private ImmutableList<byte[]> encodedInsertFileDescriptors_;
    private ImmutableList<byte[]> encodedDeleteFileDescriptors_;
    private HdfsPartitionLocationCompressor.Location location_ = null;
    private boolean isMarkedCached_ = false;
    private TAccessLevel accessLevel_ = TAccessLevel.READ_WRITE;
    private Map<String, String> hmsParameters_;
    private CachedHmsPartitionDescriptor cachedMsPartitionDescriptor_;
    private byte[] partitionStats_ = null;
    private boolean hasIncrementalStats_ = false;
    private long numRows_ = -1;
    private long writeId_ = -1L;
    // event id in metastore which pertains to the creation of this partition. Defaults
    // to -1 if the partition was not created by this catalogd or if events processing
    // is not active.
    private long createEventId_ = -1L;
    private long lastCompactionId_ = -1L;
    private long lastRefreshEventId_ = -1L;
    private InFlightEvents inFlightEvents_ = new InFlightEvents();

    @Nullable
    private HdfsPartition oldInstance_ = null;
    // True if we are generating a minimal partition instance for
    // HdfsPartition.toMinimalTHdfsPartition()
    private boolean isMinimalMode_ = false;

    public Builder(HdfsTable table, long id) {
      Preconditions.checkNotNull(table);
      table_ = table;
      id_ = id;
    }

    public Builder(HdfsTable table) {
      this(table, partitionIdCounter_.getAndIncrement());
    }

    public Builder(HdfsPartition partition) {
      this(partition.table_);
      oldInstance_ = partition;
      prevId_ = oldInstance_.id_;
      copyFromPartition(partition);
    }

    public Builder copyFromPartition(HdfsPartition partition) {
      partitionKeyValues_ = partition.partitionKeyValues_;
      fileFormatDescriptor_ = partition.fileFormatDescriptor_;
      setFileDescriptors(partition);
      location_ = partition.location_;
      isMarkedCached_ = partition.isMarkedCached_;
      accessLevel_ = partition.accessLevel_;
      hmsParameters_ = Maps.newHashMap(partition.hmsParameters_);
      partitionStats_ = partition.partitionStats_;
      hasIncrementalStats_ = partition.hasIncrementalStats_;
      numRows_ = partition.numRows_;
      writeId_ = partition.writeId_;
      if (partition.cachedMsPartitionDescriptor_ != null) {
        cachedMsPartitionDescriptor_ = new CachedHmsPartitionDescriptor(
            partition.cachedMsPartitionDescriptor_);
      }
      // Take over the in-flight events
      inFlightEvents_ = partition.inFlightEvents_;
      // Don't lose the event ids
      createEventId_ = partition.createEventId_;
      lastCompactionId_ = partition.lastCompactionId_;
      lastRefreshEventId_ = partition.lastRefreshEventId_;
      return this;
    }

    public HdfsPartition build() {
      if (partitionKeyValues_ == null) partitionKeyValues_ = Collections.emptyList();
      if (encodedFileDescriptors_ == null) setFileDescriptors(Collections.emptyList());
      if (encodedInsertFileDescriptors_ == null) {
        setInsertFileDescriptors(Collections.emptyList());
      }
      if (encodedDeleteFileDescriptors_ == null) {
        setDeleteFileDescriptors(Collections.emptyList());
      }
      if (hmsParameters_ == null) hmsParameters_ = Collections.emptyMap();
      if (location_ == null) {
        // Only prototype or minimal mode partitions can have null locations.
        Preconditions.checkState(id_ == CatalogObjectsConstants.PROTOTYPE_PARTITION_ID
            || isMinimalMode_);
      }
      return new HdfsPartition(table_, id_, prevId_, partName_, partitionKeyValues_,
          fileFormatDescriptor_, encodedFileDescriptors_, encodedInsertFileDescriptors_,
          encodedDeleteFileDescriptors_, location_, isMarkedCached_, accessLevel_,
          hmsParameters_, cachedMsPartitionDescriptor_, partitionStats_,
          hasIncrementalStats_, numRows_, writeId_, inFlightEvents_, createEventId_,
          lastCompactionId_, lastRefreshEventId_);
    }

    public Builder setId(long id) {
      id_ = id;
      return this;
    }

    public Builder setCreateEventId(long eventId) {
      createEventId_ = eventId;
      return this;
    }

    public Builder setLastRefreshEventId(long eventId) {
      lastRefreshEventId_ = eventId;
      return this;
    }

    public Builder setPrevId(long prevId) {
      prevId_ = prevId;
      return this;
    }

    public Builder setPartitionName(String partName) {
      partName_ = partName;
      return this;
    }

    public Builder setIsMinimalMode(boolean isMinimalMode) {
      isMinimalMode_ = isMinimalMode;
      return this;
    }

    public Builder setMsPartition(
        org.apache.hadoop.hive.metastore.api.Partition msPartition)
        throws CatalogException {
      if (msPartition == null) {
        setLocation(table_.getLocation());
        cachedMsPartitionDescriptor_ = null;
        hmsParameters_ = Collections.emptyMap();
        partitionKeyValues_ = Collections.emptyList();
        return this;
      }
      setPartitionKeyValues(
          FeCatalogUtils.parsePartitionKeyValues(table_, msPartition.getValues()));
      setLocation(msPartition.getSd().getLocation());
      cachedMsPartitionDescriptor_ = new CachedHmsPartitionDescriptor(msPartition);
      if (msPartition.getParameters() != null) {
        isMarkedCached_ = HdfsCachingUtil.getCacheDirectiveId(
            msPartition.getParameters()) != null;
        numRows_ = FeCatalogUtils.getRowCount(msPartition.getParameters());
        if (table_.getDebugMetadataScale() > 1.0 && numRows_ > 0) {
          numRows_ *= table_.getDebugMetadataScale();
        }
        hmsParameters_ = msPartition.getParameters();
        extractAndCompressPartStats();
        // Intern parameters after removing the incremental stats
        hmsParameters_ = CatalogInterners.internParameters(hmsParameters_);
      }
      if (MetastoreShim.getMajorVersion() > 2) {
        writeId_ = MetastoreShim.getWriteIdFromMSPartition(msPartition);
      }
      // If we have taken over the in-flight events from an old partition instance, don't
      // overwrite the in-flight event list.
      if (oldInstance_ == null) addInflightVersionsFromParameters();
      return this;
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

    /**
     * Updates the file format of this partition and sets the corresponding input/output
     * format classes.
     */
    public Builder setFileFormat(HdfsFileFormat fileFormat) {
      Preconditions.checkNotNull(fileFormatDescriptor_);
      Preconditions.checkNotNull(cachedMsPartitionDescriptor_);
      fileFormatDescriptor_ = fileFormatDescriptor_.cloneWithChangedFileFormat(
          fileFormat);
      cachedMsPartitionDescriptor_.sdInputFormat = fileFormat.inputFormat();
      cachedMsPartitionDescriptor_.sdOutputFormat = fileFormat.outputFormat();
      cachedMsPartitionDescriptor_.sdSerdeInfo.setSerializationLib(
          fileFormatDescriptor_.getFileFormat().serializationLib());
      return this;
    }

    public void putToParameters(Pair<String, String> kv) {
      putToParameters(kv.first, kv.second);
    }
    public void putToParameters(String k, String v) {
      Preconditions.checkArgument(!IS_INCREMENTAL_STATS_KEY.apply(k));
      Preconditions.checkNotNull(hmsParameters_);
      hmsParameters_.put(k, v);
    }
    public Map<String, String> getParameters() { return hmsParameters_; }

    public org.apache.hadoop.hive.metastore.api.SerDeInfo getSerdeInfo() {
      Preconditions.checkNotNull(cachedMsPartitionDescriptor_);
      return cachedMsPartitionDescriptor_.sdSerdeInfo;
    }

    public Builder setFileFormatDescriptor(HdfsStorageDescriptor fileFormatDescriptor) {
      fileFormatDescriptor_ = fileFormatDescriptor;
      return this;
    }

    public Builder setPartitionKeyValues(List<LiteralExpr> partitionKeyValues) {
      partitionKeyValues_ = partitionKeyValues;
      return this;
    }

    public Builder setAccessLevel(TAccessLevel accessLevel) {
      accessLevel_ = accessLevel;
      return this;
    }

    /**
     * Set the number of rows for this partition. setRowCountParam() and
     * removeRowCountParam() should generally be used instead of this, except
     * for the single "partition" in an unpartitioned table.
     */
    public Builder setNumRows(long numRows) {
      numRows_ = numRows;
      return this;
    }

    /**
     * Update the row count in the partitions parameters and update the numRows stat
     * to match.
     */
    public Builder setRowCountParam(long numRows) {
      numRows_ = numRows;
      putToParameters(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
      return this;
    }

    /**
     * Remove the row count in the partitions parameters and update the numRows stat
     * to match.
     */
    public Builder removeRowCountParam() {
      numRows_ = -1;
      getParameters().remove(StatsSetupConst.ROW_COUNT);
      return this;
    }

    public Builder dropPartitionStats() { return setPartitionStatsBytes(null, false); }
    public Builder setPartitionStatsBytes(byte[] partitionStats, boolean hasIncrStats) {
      if (hasIncrStats) Preconditions.checkNotNull(partitionStats);
      partitionStats_ = partitionStats;
      hasIncrementalStats_ = hasIncrStats;
      return this;
    }

    public Builder setLocation(HdfsPartitionLocationCompressor.Location location) {
      location_ = location;
      return this;
    }
    public Builder setLocation(String place) {
      location_ = table_.getPartitionLocationCompressor().new Location(place);
      return this;
    }

    public String getLocation() {
      return (location_ != null) ? location_.toString() : null;
    }

    public List<FileDescriptor> getFileDescriptors() {
      // Set an empty descriptors in case that setFileDescriptors hasn't been called.
      if (encodedFileDescriptors_ == null) setFileDescriptors(new ArrayList<>());
      // Return a lazily transformed list from our internal bytes storage.
      return Lists.transform(encodedFileDescriptors_, FileDescriptor.FROM_BYTES);
    }

    public List<FileDescriptor> getInsertFileDescriptors() {
      // Set an empty descriptors in case that setInsertFileDescriptors hasn't been called
      if (encodedInsertFileDescriptors_ == null) setFileDescriptors(new ArrayList<>());
      // Return a lazily transformed list from our internal bytes storage.
      return Lists.transform(encodedInsertFileDescriptors_, FileDescriptor.FROM_BYTES);
    }

    public List<FileDescriptor> getDeleteFileDescriptors() {
      // Set an empty descriptors in case that setDeleteFileDescriptors hasn't been called
      if (encodedDeleteFileDescriptors_ == null) setFileDescriptors(new ArrayList<>());
      // Return a lazily transformed list from our internal bytes storage.
      return Lists.transform(encodedDeleteFileDescriptors_, FileDescriptor.FROM_BYTES);
    }

    public Builder clearFileDescriptors() {
      encodedFileDescriptors_ = ImmutableList.of();
      encodedInsertFileDescriptors_ = ImmutableList.of();
      encodedDeleteFileDescriptors_ = ImmutableList.of();
      return this;
    }

    public Builder setFileDescriptors(List<FileDescriptor> descriptors) {
      // Store an eagerly transformed-and-copied list so that we drop the memory usage
      // of the flatbuffer wrapper.
      encodedFileDescriptors_ = ImmutableList.copyOf(Lists.transform(
          descriptors, FileDescriptor.TO_BYTES));
      return this;
    }

    public Builder setFileDescriptors(HdfsPartition partition) {
      encodedFileDescriptors_ = partition.encodedFileDescriptors_;
      encodedInsertFileDescriptors_ = partition.encodedInsertFileDescriptors_;
      encodedDeleteFileDescriptors_ = partition.encodedDeleteFileDescriptors_;
      return this;
    }

    public Builder setInsertFileDescriptors(List<FileDescriptor> descriptors) {
      // Store an eagerly transformed-and-copied list so that we drop the memory usage
      // of the flatbuffer wrapper.
      encodedInsertFileDescriptors_ = ImmutableList.copyOf(Lists.transform(
          descriptors, FileDescriptor.TO_BYTES));
      return this;
    }

    public Builder setDeleteFileDescriptors(List<FileDescriptor> descriptors) {
      // Store an eagerly transformed-and-copied list so that we drop the memory usage
      // of the flatbuffer wrapper.
      encodedDeleteFileDescriptors_ = ImmutableList.copyOf(Lists.transform(
          descriptors, FileDescriptor.TO_BYTES));
      return this;
    }

    public Builder setFileDescriptors(ImmutableList<byte[]> encodedDescriptors) {
      encodedFileDescriptors_ = encodedDescriptors;
      return this;
    }

    public HdfsFileFormat getFileFormat() {
      return fileFormatDescriptor_.getFileFormat();
    }

    public boolean isMarkedCached() { return isMarkedCached_; }
    public Builder setIsMarkedCached(boolean isCached) {
      isMarkedCached_ = isCached;
      return this;
    }

    public HdfsTable getTable() { return table_; }

    public String getPartitionName() {
      return FeCatalogUtils.getPartitionName(this);
    }

    public Builder setLastCompactionId(long compactionId) {
      lastCompactionId_ = compactionId;
      return this;
    }

    public long getLastCompactionId() { return lastCompactionId_; }

    /**
     * Adds a version number to the in-flight events of this partition
     * @param isInsertEvent if true, add eventId to list of eventIds for in-flight Insert
     * events if false, add version number to list of versions for in-flight DDL events
     * @param versionNumber when isInsertEvent is true, it's eventId to add
     *                      when isInsertEvent is false, it's version number to add
     * TODO: merge this with HdfsPartition.addToVersionsForInflightEvents()
     */
    public void addToVersionsForInflightEvents(boolean isInsertEvent,
        long versionNumber) {
      Preconditions.checkState(table_.isWriteLockedByCurrentThread(),
          "addToVersionsForInflightEvents called without holding the table lock on "
              + "partition " + getPartitionName() + " of table " + table_.getFullName());
      if (!inFlightEvents_.add(isInsertEvent, versionNumber)) {
        LOG.warn("Could not add {} version to the partition {} of table {}. This could " +
                "cause unnecessary refresh of the partition when the event is received " +
                "by the Events processor.",
            versionNumber, getPartitionName(), getTable().getFullName());
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
      Preconditions.checkState(inFlightEvents_.size(false) == 0);
      // We should not check for table lock being held here since there are certain
      // code paths which call this method without holding the table lock
      // (e.g. getOrLoadTable())
      if (!hmsParameters_.containsKey(
          MetastoreEventPropertyKey.CATALOG_VERSION.getKey())) {
        return;
      }
      boolean added = inFlightEvents_.add(false, Long.parseLong(
          hmsParameters_.get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey())));
      LOG.trace("{} {} to inflight events {}",
          (added ? "Added" : "Could not add"), Long.parseLong(
              hmsParameters_.get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey())),
          inFlightEvents_.print());
    }

    private List<FileDescriptor> fdsFromThrift(List<THdfsFileDesc> tFileDescs) {
      List<FileDescriptor> ret = new ArrayList<>();
      for (THdfsFileDesc desc : tFileDescs) {
        ret.add(HdfsPartition.FileDescriptor.fromThrift(desc));
      }
      return ret;
    }

    public List<LiteralExpr> getPartitionValues() {
      return partitionKeyValues_;
    }

    public HdfsPartition getOldInstance() { return oldInstance_; }
    public long getOldId() { return Preconditions.checkNotNull(oldInstance_).id_; }

    public org.apache.hadoop.hive.metastore.api.Partition toHmsPartition() {
      // Build a temp HdfsPartition to create the HmsPartition.
      return build().toHmsPartition();
    }

    public Builder fromThrift(THdfsPartition thriftPartition) {
      fileFormatDescriptor_ = HdfsStorageDescriptor.fromThrift(
          thriftPartition.hdfs_storage_descriptor, table_.getName());

      partitionKeyValues_ = new ArrayList<>();
      if (id_ != CatalogObjectsConstants.PROTOTYPE_PARTITION_ID) {
        List<Column> clusterCols = table_.getClusteringColumns();
        List<TExprNode> exprNodes = new ArrayList<>();
        for (TExpr expr : thriftPartition.getPartitionKeyExprs()) {
          exprNodes.addAll(expr.getNodes());
        }
        Preconditions.checkState(clusterCols.size() == exprNodes.size(),
            String.format("Number of partition columns (%d) does not match number " +
                    "of partition key expressions (%d)",
                clusterCols.size(), exprNodes.size()));

        for (int i = 0; i < exprNodes.size(); ++i) {
          partitionKeyValues_.add(LiteralExpr.fromThrift(
              exprNodes.get(i), clusterCols.get(i).getType()));
        }
      }

      if (thriftPartition.isSetFile_desc()) {
        setFileDescriptors(fdsFromThrift(thriftPartition.getFile_desc()));
      }
      if (thriftPartition.isSetInsert_file_desc()) {
        setInsertFileDescriptors(fdsFromThrift(thriftPartition.getInsert_file_desc()));
      }
      if (thriftPartition.isSetDelete_file_desc()) {
        setDeleteFileDescriptors(fdsFromThrift(thriftPartition.getDelete_file_desc()));
      }

      accessLevel_ = thriftPartition.isSetAccess_level() ?
          thriftPartition.getAccess_level() : TAccessLevel.READ_WRITE;
      location_ = thriftPartition.isSetLocation()
          ? table_.getPartitionLocationCompressor().new Location(
          thriftPartition.getLocation())
          : null;
      if (thriftPartition.isSetStats()) {
        numRows_ = thriftPartition.getStats().getNum_rows();
      }
      hasIncrementalStats_ = thriftPartition.has_incremental_stats;
      if (thriftPartition.isSetPartition_stats()) {
        partitionStats_ = thriftPartition.getPartition_stats();
      }
      if (thriftPartition.isSetIs_marked_cached()) {
        isMarkedCached_ = thriftPartition.isIs_marked_cached();
      }
      if (thriftPartition.isSetHms_parameters()) {
        hmsParameters_ = CatalogInterners.internParameters(
            thriftPartition.getHms_parameters());
      } else {
        hmsParameters_ = new HashMap<>();
      }
      writeId_ = thriftPartition.isSetWrite_id() ?
          thriftPartition.getWrite_id() : -1L;
      return this;
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

    public boolean equalsToOriginal(HdfsPartition oldInstance) {
      return (oldInstance == oldInstance_
          && encodedFileDescriptors_ == oldInstance.encodedFileDescriptors_
          && encodedInsertFileDescriptors_ == oldInstance.encodedInsertFileDescriptors_
          && encodedDeleteFileDescriptors_ == oldInstance.encodedDeleteFileDescriptors_
          && fileFormatDescriptor_ == oldInstance.fileFormatDescriptor_
          && location_ == oldInstance.location_
          && isMarkedCached_ == oldInstance.isMarkedCached_
          && accessLevel_ == oldInstance.accessLevel_
          && hmsParameters_.equals(oldInstance.hmsParameters_)
          && partitionStats_ == oldInstance.partitionStats_
          && hasIncrementalStats_ == oldInstance.hasIncrementalStats_
          && numRows_ == oldInstance.numRows_ && writeId_ == oldInstance.writeId_
          && lastCompactionId_ == oldInstance.lastCompactionId_
          && lastRefreshEventId_ == oldInstance_.lastRefreshEventId_);
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
}
