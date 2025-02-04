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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.flatbuffers.FlatBufferBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.impala.common.Reference;
import org.apache.impala.fb.FbCompression;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;

// Metadata for a single file.
public class FileDescriptor implements Comparable<FileDescriptor> {
  // An invalid network address, which will always be treated as remote.
  private final static TNetworkAddress REMOTE_NETWORK_ADDRESS =
      new TNetworkAddress("remote*addr", 0);

  // Minimum block size in bytes allowed for synthetic file blocks (other than the last
  // block, which may be shorter).
  public final static long MIN_SYNTHETIC_BLOCK_SIZE = 1024 * 1024;

  // Internal representation of a file descriptor using a FlatBuffer.
  private final FbFileDesc fbFileDescriptor_;

  protected FileDescriptor(FbFileDesc fileDescData) {
    fbFileDescriptor_ = fileDescData;
  }

  public static FileDescriptor fromThrift(THdfsFileDesc desc) {
    ByteBuffer bb = ByteBuffer.wrap(desc.getFile_desc_data());
    Preconditions.checkState(!desc.isSetFile_metadata());
    return new FileDescriptor(FbFileDesc.getRootAsFbFileDesc(bb));
  }

  public THdfsFileDesc toThrift() {
    THdfsFileDesc fd = new THdfsFileDesc();
    ByteBuffer bb = fbFileDescriptor_.getByteBuffer();
    fd.setFile_desc_data(bb);
    return fd;
  }

  /**
   * Clone the descriptor, but change the replica indexes to reference the new host
   * index 'dstIndex' instead of the original index 'origIndex'.
   */
  public FileDescriptor cloneWithNewHostIndex(
      List<TNetworkAddress> origIndex, ListMap<TNetworkAddress> dstIndex) {
    return new FileDescriptor(fbFileDescWithNewHostIndex(origIndex, dstIndex));
  }

  protected FbFileDesc fbFileDescWithNewHostIndex(
      List<TNetworkAddress> origIndex, ListMap<TNetworkAddress> dstIndex) {
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

    return cloned;
  }

  /**
   * Creates the file descriptor of a file represented by 'fileStatus' with blocks
   * stored in 'blockLocations'. 'fileSystem' is the filesystem where the
   * file resides and 'hostIndex' stores the network addresses of the hosts that store
   * blocks of the parent HdfsTable. 'isEc' indicates whether the file is erasure-coded.
   * Populates 'numUnknownDiskIds' with the number of unknown disk ids.
   *
   * @param fileStatus        the status returned from file listing
   * @param relPath           the path of the file relative to the partition directory
   * @param blockLocations    the block locations for the file
   * @param hostIndex         the host index to use for encoding the hosts
   * @param isEc              true if the file is known to be erasure-coded
   * @param numUnknownDiskIds reference which will be set to the number of blocks
   *                          for which no disk ID could be determined
   */
  public static FileDescriptor create(
      FileStatus fileStatus,
      String relPath,
      BlockLocation[] blockLocations,
      ListMap<TNetworkAddress> hostIndex,
      boolean isEncrypted,
      boolean isEc,
      Reference<Long> numUnknownDiskIds,
      String absPath) throws IOException {
    FlatBufferBuilder fbb = new FlatBufferBuilder(1);
    int[] fbFileBlockOffsets = new int[blockLocations.length];
    int blockIdx = 0;
    for (BlockLocation loc : blockLocations) {
      if (isEc) {
        fbFileBlockOffsets[blockIdx++] = FileBlock.createFbFileBlock(
            fbb,
            loc.getOffset(),
            loc.getLength(),
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
  private static FbFileDesc createFbFileDesc(
      FlatBufferBuilder fbb,
      FileStatus fileStatus,
      String relPath,
      int[] fbFileBlockOffsets,
      boolean isEncrypted,
      boolean isEc,
      String absPath) {
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

  public String getRelativePath() {
    return fbFileDescriptor_.relativePath();
  }

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

  public long getFileLength() {
    return fbFileDescriptor_.length();
  }

  /**
   * Compute the total length of files in fileDescs
   */
  public static long computeTotalFileLength(Collection<FileDescriptor> fileDescs) {
    long totalLength = 0;
    for (FileDescriptor fileDesc : fileDescs) {
      totalLength += fileDesc.getFileLength();
    }
    return totalLength;
  }

  public HdfsCompression getFileCompression() {
    return HdfsCompression.valueOf(FbCompression.name(fbFileDescriptor_.compression()));
  }

  public long getModificationTime() {
    return fbFileDescriptor_.lastModificationTime();
  }

  public int getNumFileBlocks() {
    return fbFileDescriptor_.fileBlocksLength();
  }

  public boolean getIsEncrypted() {
    return fbFileDescriptor_.isEncrypted();
  }

  public boolean getIsEc() {
    return fbFileDescriptor_.isEc();
  }

  public FbFileBlock getFbFileBlock(int idx) {
    return fbFileDescriptor_.fileBlocks(idx);
  }

  public FbFileDesc getFbFileDescriptor() {
    return fbFileDescriptor_;
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