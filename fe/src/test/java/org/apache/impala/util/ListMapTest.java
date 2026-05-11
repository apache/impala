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

package org.apache.impala.util;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ListMapTest {

  @Test
  public void testGetOrAddIndex() {
    ListMap<String> map = new ListMap<>();
    assertEquals(0, map.getOrAddIndex("a"));
    assertEquals(1, map.getOrAddIndex("b"));
    assertEquals(0, map.getOrAddIndex("a"));
    assertEquals(2, map.getOrAddIndex("c"));
    assertEquals(3, map.size());
  }

  @Test
  public void testPopulateWithMutableList() {
    ListMap<String> map = new ListMap<>();
    List<String> input = new ArrayList<>();
    input.add("host1");
    input.add("host2");
    map.populate(input);

    assertEquals(2, map.size());
    assertEquals(0, map.getOrAddIndex("host1"));
    assertEquals(1, map.getOrAddIndex("host2"));
    assertEquals(2, map.getOrAddIndex("host3"));
    assertEquals(3, map.size());
  }

  @Test
  public void testPopulateWithImmutableList() {
    ListMap<String> map = new ListMap<>();
    List<String> input = ImmutableList.of("host1", "host2");
    map.populate(input);

    assertEquals(2, map.size());
    assertEquals(0, map.getOrAddIndex("host1"));
    assertEquals(1, map.getOrAddIndex("host2"));
    // Our ListMap should have a modifiable copy of the initial list, so the
    // following should succeed.
    assertEquals(2, map.getOrAddIndex("host3"));
    assertEquals(3, map.size());
    assertEquals("host3", map.getEntry(2));
  }

  /**
   * Simulates the time-travel scenario: a ListMap is populated via getList() from
   * another ListMap (which returns an ImmutableList), then getOrAddIndex() is called
   * for a host not already in the map (as happens when a file block is on a datanode
   * not present in the cached host index).
   */
  @Test
  public void testPopulateFromGetListThenAdd() {
    ListMap<String> source = new ListMap<>();
    source.getOrAddIndex("dn1.example.com:50010");
    source.getOrAddIndex("dn2.example.com:50010");

    // Simulate what LocalIcebergTable does:
    // getHostIndex().populate(cachedFiles.hostIndex.getList())
    ListMap<String> target = new ListMap<>();
    target.populate(source.getList());

    assertEquals(2, target.size());
    assertEquals(0, target.getOrAddIndex("dn1.example.com:50010"));
    assertEquals(1, target.getOrAddIndex("dn2.example.com:50010"));

    // Simulate time-travel finding a file on a new datanode not in the original index.
    // With replication factor 1, data files after OPTIMIZE may reside on a different
    // datanode than the original files. Time-traveling to a pre-optimize snapshot
    // encounters these old files whose blocks are on a host not yet in the index.
    assertEquals(2, target.getOrAddIndex("dn3.example.com:50010"));
    assertEquals(3, target.size());
    assertEquals("dn3.example.com:50010", target.getEntry(2));
  }
}
