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

package org.apache.impala.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.impala.thrift.TUniqueId;

import org.junit.Test;

/**
 * Basic unit tests for TupleCacheInfo
 */
public class TupleCacheInfoTest {

  @Test
  public void testHashThrift() {
    TupleCacheInfo info1 = new TupleCacheInfo();
    info1.hashThrift(new TUniqueId(1L, 2L));
    info1.finalize();

    TupleCacheInfo info2 = new TupleCacheInfo();
    info2.hashThrift(new TUniqueId(1L, 2L));
    info2.finalize();

    assertEquals(info1.getHashTrace(), "TUniqueId(hi:1, lo:2)");
    assertEquals(info1.getHashTrace(), info2.getHashTrace());
    // Hashes are stable over time, so check the actual hash value
    assertEquals(info1.getHashString(), "b3f5384f81770c6adb83209b2a171dfa");
    assertEquals(info1.getHashString(), info2.getHashString());
  }

  @Test
  public void testMergeHash() {
    TupleCacheInfo child1 = new TupleCacheInfo();
    child1.hashThrift(new TUniqueId(1L, 2L));
    child1.finalize();

    TupleCacheInfo child2 = new TupleCacheInfo();
    child2.hashThrift(new TUniqueId(3L, 4L));
    child2.finalize();

    TupleCacheInfo parent = new TupleCacheInfo();
    parent.mergeChild(child1);
    parent.mergeChild(child2);
    parent.hashThrift(new TUniqueId(5L, 6L));
    parent.finalize();

    assertEquals(parent.getHashTrace(),
        "TUniqueId(hi:1, lo:2)TUniqueId(hi:3, lo:4)TUniqueId(hi:5, lo:6)");
    // Hashes are stable over time, so check the actual hash value
    assertEquals(parent.getHashString(), "edf5633bed2280c3c3edb703182f3122");
  }

  @Test
  public void testMergeEligibility() {
    // Child 1 is eligible
    TupleCacheInfo child1 = new TupleCacheInfo();
    child1.hashThrift(new TUniqueId(1L, 2L));
    child1.finalize();
    assertTrue(child1.isEligible());

    // Child 2 is ineligible
    TupleCacheInfo child2 = new TupleCacheInfo();
    child2.setIneligible(TupleCacheInfo.IneligibilityReason.NOT_IMPLEMENTED);
    child2.finalize();
    assertTrue(!child2.isEligible());

    TupleCacheInfo parent = new TupleCacheInfo();
    parent.mergeChild(child1);
    // Still eligible after adding child1 without child2
    assertTrue(parent.isEligible());
    parent.mergeChild(child2);
    // It is allowed to check eligibility before finalize()
    assertTrue(!parent.isEligible());
    parent.finalize();

    assertTrue(!parent.isEligible());
  }

}
