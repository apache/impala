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

import java.util.HashSet;
import java.util.Set;

import org.apache.impala.common.Pair;
import org.apache.impala.fb.FbFileBlock;

/**
 * Transient statistics about unique hosts and host:disk pairs seen while loading file
 * metadata. These are used only for table-loading log lines and are not stored in the
 * catalog.
 */
public class FileMetadataHostStats {
  private final Set<Integer> uniqueHostIndices_ = new HashSet<>();
  // Disk IDs are 0-based per host, so pairs must be tracked together.
  // With erasure coding, disk IDs may not be available, so this may be empty even when
  // uniqueHostIndices_ is populated.
  private final Set<Pair<Integer, Short>> uniqueHostDiskPairs_ = new HashSet<>();

  public void merge(FileMetadataHostStats other) {
    uniqueHostIndices_.addAll(other.uniqueHostIndices_);
    uniqueHostDiskPairs_.addAll(other.uniqueHostDiskPairs_);
  }

  // Track unique hosts and host:disk pairs from file blocks
  public void accumulate(FileDescriptor fd) {
    for (int i = 0; i < fd.getNumFileBlocks(); ++i) {
      FbFileBlock block = fd.getFbFileBlock(i);
      int numReplicas = block.replicaHostIdxsLength();
      int numDiskIds = block.diskIdsLength();
      // Track host indices and pair them with disk IDs when available
      for (int j = 0; j < numReplicas; ++j) {
        int hostIdx = FileBlock.getReplicaHostIdx(block, j);
        uniqueHostIndices_.add(hostIdx);
        short diskId = (j < numDiskIds) ? block.diskIds(j) : -1;
        if (diskId >= 0) {  // Only track valid disk IDs
          uniqueHostDiskPairs_.add(Pair.create(hostIdx, diskId));
        }
      }
    }
  }

  public int getNumUniqueHosts() {
    return uniqueHostIndices_.size();
  }

  public int getNumUniqueHostDiskPairs() {
    return uniqueHostDiskPairs_.size();
  }
}
