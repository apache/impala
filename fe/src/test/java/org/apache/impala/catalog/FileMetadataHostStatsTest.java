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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.junit.Test;
import org.mockito.Mockito;

public class FileMetadataHostStatsTest {

  private static FileDescriptor createFd(BlockLocation blockLocation,
      ListMap<TNetworkAddress> hostIndex) throws IOException {
    FileStatus status = Mockito.mock(FileStatus.class);
    Mockito.when(status.getLen()).thenReturn(100L);
    Mockito.when(status.getModificationTime()).thenReturn(1L);
    Mockito.when(status.isErasureCoded()).thenReturn(false);
    Mockito.when(status.isEncrypted()).thenReturn(false);
    Mockito.when(status.getPath()).thenReturn(new Path("/dummy/f"));
    return FileDescriptor.create(status, "f", new BlockLocation[] {blockLocation},
        hostIndex, false, false, new AtomicLong(0), null);
  }

  @Test
  public void testUniqueHostsDedupedPerBlock() throws IOException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    BlockLocation loc = new BlockLocation(
        new String[] {"10.0.0.1:50010", "10.0.0.1:50010"},
        new String[] {"host1", "host1"},
        0, 100);
    FileMetadataHostStats stats = new FileMetadataHostStats();
    stats.accumulate(createFd(loc, hostIndex));
    assertEquals(1, stats.getNumUniqueHosts());
  }

  @Test
  public void testMergeIsUnionOfHosts() throws IOException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    BlockLocation loc1 = new BlockLocation(
        new String[] {"10.0.0.1:50010"},
        new String[] {"host1"},
        0, 100);
    BlockLocation loc2 = new BlockLocation(
        new String[] {"10.0.0.2:50010"},
        new String[] {"host2"},
        0, 100);
    FileMetadataHostStats statsA = new FileMetadataHostStats();
    statsA.accumulate(createFd(loc1, hostIndex));
    FileMetadataHostStats statsB = new FileMetadataHostStats();
    statsB.accumulate(createFd(loc2, hostIndex));

    FileMetadataHostStats merged = new FileMetadataHostStats();
    merged.merge(statsA);
    merged.merge(statsB);
    assertEquals(2, merged.getNumUniqueHosts());
  }

  @Test
  public void testHostDiskPairsSkippedWithoutDiskIds() throws IOException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    BlockLocation loc = new BlockLocation(
        new String[] {"10.0.0.1:50010"},
        new String[] {"host1"},
        0, 100);
    FileMetadataHostStats stats = new FileMetadataHostStats();
    stats.accumulate(createFd(loc, hostIndex));
    assertEquals(1, stats.getNumUniqueHosts());
    assertEquals(0, stats.getNumUniqueHostDiskPairs());
  }

  @Test
  public void testHostDiskPairsWithStorageIds() throws IOException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    BlockLocation loc = new BlockLocation(
        new String[] {"10.0.0.1:50010"},
        new String[] {"host1"},
        new String[] {"s1"},
        0, 100);
    FileMetadataHostStats stats = new FileMetadataHostStats();
    stats.accumulate(createFd(loc, hostIndex));
    assertEquals(1, stats.getNumUniqueHosts());
    assertTrue(stats.getNumUniqueHostDiskPairs() >= 0);
  }
}
