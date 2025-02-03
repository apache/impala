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

import com.google.common.base.Preconditions;

import org.apache.impala.thrift.TNetworkAddress;

/**
 * Represents the metadata of a single block replica.
 */
public class BlockReplica {
  private final boolean isCached_;
  private final short hostIdx_;

  /**
   * Creates a BlockReplica given a host ID/index and a flag specifying whether this
   * replica is cached. Host IDs are assigned when loading the block metadata in
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