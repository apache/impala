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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.google.common.base.Function;

import java.util.List;

/**
 * Unit tests for the TopNCache class.
 */
public class TestTopNCache {

  /**
   * Create a TopNCache with 'capacity' max capacity and populate it with 'numEntries'
   * entries where each entry is a number from 0 to 'numEntries'.
   */
  private static TopNCache<Long, Long> createAndPopulate(int capacity,
      long numEntries, boolean policy) {
    TopNCache<Long, Long> cache =
        new TopNCache<Long, Long>(new Function<Long, Long>() {
            @Override
            public Long apply(Long element) { return element; }
        }, capacity, policy);
    for (long i = 0; i < numEntries; ++i) cache.putOrUpdate(i);
    return cache;
  }

  @Test
  public void testCreateAndPopulateCache() throws Exception {
    int[] capacities = {1, 10, 1000};
    boolean[] evictionPolicies = {true, false};
    for (int capacity: capacities) {
      for (boolean policy: evictionPolicies) {
        TopNCache<Long, Long> cache =
            createAndPopulate(capacity, 2 * capacity, policy);
        assertEquals(cache.listEntries().size(), capacity);
        for (long i = 0; i < capacity * 2; i++) cache.remove(i);
        assertEquals(cache.listEntries().size(), 0);
      }
    }
  }

  @Test
  public void testUpdateExistingElements() throws Exception {
    final int capacity = 10;
    TopNCache<Long, Long> cache = createAndPopulate(capacity, capacity / 2, true);
    assertEquals(cache.listEntries().size(), capacity / 2);
    // Adding the same elements should not alter the number of elements stored in the
    // cache.
    for (long i = 0; i < capacity / 2; i++) cache.putOrUpdate(i);
    assertEquals(cache.listEntries().size(), capacity / 2);
  }

  @Test
  public void testAlwaysEvictPolicy() throws Exception {
    final int capacity = 10;
    TopNCache<Long, Long> cache = createAndPopulate(capacity, capacity, true);
    assertEquals(cache.listEntries().size(), capacity);
    cache.putOrUpdate((long) capacity);
    assertEquals(cache.listEntries().size(), capacity);
    // Assert that the new element replaced the smallest element in the cache
    assertTrue(!cache.listEntries().contains(Long.valueOf(0)));
    cache.putOrUpdate((long) capacity + 1);
    assertTrue(!cache.listEntries().contains(Long.valueOf(1)));
    List<Long> cacheElements = cache.listEntries();
    for (long i = 2; i < capacity + 2; i++) {
      assertTrue(cacheElements.contains(Long.valueOf(i)));
    }
  }

  @Test
  public void testRankBasedEvictionPolicy() throws Exception {
    final int capacity = 10;
    TopNCache<Long, Long> cache = new TopNCache<Long, Long>(
        new Function<Long, Long>() {
            @Override
            public Long apply(Long element) { return element; }
        }, capacity, false);
    for (long i = 1; i < capacity + 1; i++) cache.putOrUpdate(i);
    assertEquals(cache.listEntries().size(), capacity);
    cache.putOrUpdate((long) 0);
    // 0 shouldn't be added to the cache because it's rank is smaller than the lowest rank
    // in the cache.
    assertTrue(!cache.listEntries().contains(Long.valueOf(0)));
    cache.putOrUpdate((long) capacity + 1);
    assertEquals(cache.listEntries().size(), capacity);
    assertTrue(cache.listEntries().contains(Long.valueOf(capacity + 1)));
  }

  @Test
  public void testRankBasedEvictionPolicyWithRandomInput() throws Exception {
    final int capacity = 5;
    long[] values = {10, 8, 1, 2, 5, 4, 3, 6, 9, 7};
    TopNCache<Long, Long> cache = new TopNCache<Long, Long>(
        new Function<Long, Long>() {
            @Override
            public Long apply(Long element) { return element; }
        }, capacity, false);
    for (Long entry: values) cache.putOrUpdate(entry);
    List<Long> entries = cache.listEntries();
    assertEquals(entries.size(), capacity);
    // Make sure only the top-5 elements are in the cache
    for (long i = 1; i <= capacity; ++i) {
      assertTrue(!entries.contains(i));
      assertTrue(entries.contains(i + capacity));
    }
  }
}

