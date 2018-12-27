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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Shorts;

/**
 * A singleton class that maps HDFS storage-UUIDs to per-host 0-based, sequential disk
 * ids. This mapping is internally implemented as a global static object shared
 * across all the table instances. The rationale behind this implementation is
 * - To maintain a consistent mapping across all the table instances so that the
 *   assignment of scan ranges to I/O threads is balanced and consistent for all scans
 *   on the same host.
 * - To Reduce memory usage in the Catalog since UUIDs can potentially consume a lot of
 *   memory when maintained per table instance.
 */
public class DiskIdMapper {

    public static DiskIdMapper INSTANCE = new DiskIdMapper();

    private DiskIdMapper() {}

    // Maps each storage ID UUID string returned by the BlockLocation API, to a per-node
    // sequential 0-based disk id used by the BE scanners. This assumes that
    // the storage ID of a particular disk is unique across all the nodes in the cluster.
    private Map<String, Short> storageUuidToDiskId_ =
        new ConcurrentHashMap<String, Short>();

    // Per-host ID generator for storage UUID to Short ID mapping. This maps each host
    // to the corresponding latest 0-based ID stored in a short.
    private final Map<String, Short> storageIdGenerator_ = new HashMap<>();

    /**
     * Returns a disk id (0-based) index for storageUuid on host 'host'. Generates a
     * new disk ID for storageUuid if one doesn't already exist. We cache the mappings
     * already generated for faster lookups.
     *
     * TODO: It is quite possible that there will be lock contention in this method during
     * the initial metadata load. Figure out ways to fix it using finer locking scheme.
     */
    public short getDiskId(String host, String storageUuid) {
      Preconditions.checkState(!Strings.isNullOrEmpty(host));
      // Initialize the diskId as -1 to indicate it is unknown
      short diskId = -1;
      // Check if an existing mapping is already present. This is intentionally kept
      // out of the synchronized block to avoid contention for lookups. Once a reasonable
      // amount of data loading is done and storageUuidToDiskId_ is populated with storage
      // IDs across the cluster, we expect to have a good hit rate.
      Short shortId = storageUuidToDiskId_.get(storageUuid);
      if (shortId != null) return shortId;
      synchronized (storageIdGenerator_) {
        // Mapping might have been added by another thread that entered the synchronized
        // block first.
        shortId = storageUuidToDiskId_.get(storageUuid);
        if (shortId != null) return shortId;
        // No mapping exists, create a new disk ID for 'storageUuid'
        if (storageIdGenerator_.containsKey(host)) {
          try {
            diskId = Shorts.checkedCast(storageIdGenerator_.get(host) + 1);
          } catch (IllegalStateException e) {
            Preconditions.checkState(false,
                "Number of hosts exceeded " + Short.MAX_VALUE);
          }
        } else {
          // First diskId of this host.
          diskId = 0;
        }
        storageIdGenerator_.put(host, Short.valueOf(diskId));
        storageUuidToDiskId_.put(storageUuid, Short.valueOf(diskId));
      }
      return diskId;
    }
}
