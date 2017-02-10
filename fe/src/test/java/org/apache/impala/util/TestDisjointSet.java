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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for the DisjointSet data structure.
 */
public class TestDisjointSet {

  @Test
  public void testMakeSet() throws Exception {
    DisjointSet<Integer> ds = new DisjointSet<Integer>();

    // Expect success.
    ds.makeSet(1);
    assertTrue(ds.get(1).contains(1));
    ds.makeSet(2);
    assertTrue(ds.get(2).contains(2));
    ds.checkConsistency();

    Set<Integer> existingSet = ds.get(1);
    // Manually tamper with the item set for testing purposes.
    existingSet.add(6);
    existingSet.add(7);
    // The item set already exists.
    try {
      ds.makeSet(1);
      fail("makeSet() on an item with an existing item set did not fail");
    } catch (Exception e) {
      // Check that the failed makeSet didn't change the existing set.
      assertTrue(existingSet == ds.get(1));
      assertTrue(existingSet.contains(1));
      assertTrue(existingSet.contains(6));
      assertTrue(existingSet.contains(7));
    }

    // Tests detecting the inconsistency due to the manual modifications.
    try {
      ds.checkConsistency();
      fail("Failed to detect an inconsistency in the DisjointSet data structure.");
    } catch (Exception e) {
    }
  }

  @Test
  public void testUnion() throws Exception {
    DisjointSet<Integer> ds = new DisjointSet<Integer>();
    ds.makeSet(1);
    // test idempotence
    assertFalse(ds.union(1, 1));
    assertTrue(ds.get(1).contains(1) && ds.get(1).size() == 1);
    ds.checkConsistency();

    // test creating a new single-item set with union()
    assertTrue(ds.union(2, 2));
    assertTrue(ds.get(2).contains(2) && ds.get(2).size() == 1);
    ds.checkConsistency();

    // test creating a multi-item set with union()
    assertTrue(ds.union(3, 4));
    assertTrue(ds.get(3) == ds.get(4) && ds.get(3).contains(4) && ds.get(4).contains(3));
    ds.checkConsistency();
    // test idempotence
    assertFalse(ds.union(3, 4));
    assertTrue(ds.get(3) == ds.get(4) && ds.get(3).contains(4) && ds.get(4).contains(3));
    ds.checkConsistency();

    // test merging an existing item set with a non-existent item
    assertTrue(ds.union(4, 5));
    assertTrue(ds.get(4) == ds.get(5) && ds.get(4).contains(5)
        && ds.get(4).containsAll(Lists.newArrayList(3, 4, 5)));
    assertTrue(ds.union(6, 4));
    assertTrue(ds.get(4) == ds.get(6) && ds.get(6).contains(4)
        && ds.get(4).containsAll(Lists.newArrayList(3, 4, 5, 6)));
    // already in the same set
    assertFalse(ds.union(4, 6));
    ds.checkConsistency();

    // test merging two existing single-item item sets
    assertTrue(ds.union(1, 2));
    assertTrue(ds.get(1) == ds.get(2) &&
        ds.get(1).containsAll(Lists.newArrayList(1, 2)));
    ds.checkConsistency();

    // test merging two multi-item item sets
    assertTrue(ds.union(1, 3));
    assertTrue(ds.get(1) == ds.get(3) &&
        ds.get(1).containsAll(Lists.newArrayList(1, 2, 3, 4, 5, 6)));
    // already in the same set
    for (int i = 1; i <= 6; ++i) {
      for (int j = 1; j <= 6; ++j) {
        assertFalse(ds.union(i, j));
      }
    }
    ds.checkConsistency();
  }

  @Test
  public void testBulkUnion() throws Exception {
    DisjointSet<Integer> ds = new DisjointSet<Integer>();

    // test creating a new single-item set
    assertTrue(ds.bulkUnion(Sets.newHashSet(1)));
    assertTrue(ds.get(1).contains(1) && ds.get(1).size() == 1);
    ds.checkConsistency();

    // test creating a new multi-item item set
    assertTrue(ds.bulkUnion(Sets.newHashSet(2, 3, 4)));
    assertTrue(ds.get(2) == ds.get(3) && ds.get(2) == ds.get(4)
        && ds.get(2).containsAll(Lists.newArrayList(2, 3, 4)));
    // already in the same set
    for (int i = 2; i <= 4; ++i) {
      for (int j = 2; j <= 4; ++j) {
        assertFalse(ds.union(i, j));
        assertFalse(ds.bulkUnion(Sets.newHashSet(i, j)));
      }
    }
    ds.checkConsistency();

    // create another new multi-item item set
    assertTrue(ds.bulkUnion(Sets.newHashSet(5, 6, 7, 8)));
    assertTrue(ds.get(5) == ds.get(6) && ds.get(6) == ds.get(7) && ds.get(7) == ds.get(8)
        && ds.get(5).containsAll(Lists.newArrayList(5, 6, 7, 8)));
    ds.checkConsistency();

    // merge all existing item sets
    assertTrue(ds.bulkUnion(Sets.newHashSet(1, 3, 8)));
    assertTrue(ds.get(1) == ds.get(2) && ds.get(2) == ds.get(3) && ds.get(3) == ds.get(4)
        && ds.get(4) == ds.get(5) && ds.get(5) == ds.get(6) && ds.get(6) == ds.get(7)
        && ds.get(7) == ds.get(8)
        && ds.get(1).containsAll(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8)));
    ds.checkConsistency();
  }

  /**
   * IMPALA-4916: Tests that the set of item sets is maintained correctly.
   */
  @Test
  public void testUniqueSets() throws Exception {
    DisjointSet<Integer> ds = new DisjointSet<Integer>();

    int uniqueElements = 100;
    // Generate several 2-element item sets.
    for (int i = 0; i < uniqueElements; i += 2) ds.union(i, i + 1);
    // Connect several item sets into one big item set. This stresses the logic
    // of maintaining the set of item sets.
    for (int i = uniqueElements/2; i < uniqueElements; ++i) ds.union(i, i + 1);

    ds.checkConsistency();
  }
}
