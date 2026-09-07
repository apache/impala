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

package org.apache.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.impala.thrift.THboStatsType;
import org.junit.Test;

public class InMemoryCacheBackendTest {
  private static final long CACHE_SIZE_BYTES = 1024 * 1024;

  @Test
  public void testClearDropsEveryEntry() {
    InMemoryCacheBackend cache = new InMemoryCacheBackend(1, CACHE_SIZE_BYTES);
    cache.put(THboStatsType.CARDINALITY, "key1", "value1");
    cache.put(THboStatsType.CARDINALITY, "key2", "value2");
    assertEquals("value1", cache.getIfPresent(THboStatsType.CARDINALITY, "key1"));
    assertEquals("value2", cache.getIfPresent(THboStatsType.CARDINALITY, "key2"));

    cache.clear();

    assertNull(cache.getIfPresent(THboStatsType.CARDINALITY, "key1"));
    assertNull(cache.getIfPresent(THboStatsType.CARDINALITY, "key2"));
  }

  @Test
  public void testClearOnEmptyCache() {
    InMemoryCacheBackend cache = new InMemoryCacheBackend(1, CACHE_SIZE_BYTES);
    cache.clear();
    assertNull(cache.getIfPresent(THboStatsType.CARDINALITY, "key1"));

    // The cache stays usable afterwards.
    cache.put(THboStatsType.CARDINALITY, "key1", "value1");
    assertEquals("value1", cache.getIfPresent(THboStatsType.CARDINALITY, "key1"));
  }
}
