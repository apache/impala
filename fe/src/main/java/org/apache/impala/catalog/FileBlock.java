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
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.impala.common.Reference;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Static utility methods to serialize and access file block metadata from FlatBuffers.
public class FileBlock {
  private final static Logger LOG = LoggerFactory.getLogger(FileBlock.class);

  // Bit mask used to extract the replica host id and cache info of a file block.
  // Use ~REPLICA_HOST_IDX_MASK to extract the cache info (stored in MSB).
  private final static short REPLICA_HOST_IDX_MASK = (1 << 15) - 1;

  /**
   * Constructs an FbFileBlock object from the block location metadata
   * 'loc'. Serializes the file block metadata into a FlatBuffer using 'fbb' and
   * returns the offset in the underlying buffer where the encoded file block starts.
   * 'hostIndex' stores the network addresses of the datanodes that store the files of
   * the parent HdfsTable. Populates 'numUnknownDiskIds' with the number of unknown disk
   * ids.
   */
  public static int createFbFileBlock(
      FlatBufferBuilder fbb,
      BlockLocation loc,
      ListMap<TNetworkAddress> hostIndex,
      Reference<Long> numUnknownDiskIds) throws IOException {
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

  public static short makeReplicaIdx(boolean isReplicaCached, int hostIdx) {
    Preconditions.checkArgument((hostIdx & REPLICA_HOST_IDX_MASK) == hostIdx,
        "invalid hostIdx: %s", hostIdx);
    return isReplicaCached ? (short) (hostIdx | ~REPLICA_HOST_IDX_MASK)
        : (short) hostIdx;
  }

  /**
   * Constructs an FbFileBlock object from the file block metadata that comprise block's
   * 'offset', 'length' and replica index 'replicaIdx'. Serializes the file block
   * metadata into a FlatBuffer using 'fbb' and returns the offset in the underlying
   * buffer where the encoded file block starts.
   */
  public static int createFbFileBlock(
      FlatBufferBuilder fbb,
      long offset,
      long length,
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
  private static short[] createDiskIds(
      BlockLocation location,
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

  public static long getOffset(FbFileBlock fbFileBlock) {
    return fbFileBlock.offset();
  }

  public static long getLength(FbFileBlock fbFileBlock) {
    return fbFileBlock.length();
  }

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