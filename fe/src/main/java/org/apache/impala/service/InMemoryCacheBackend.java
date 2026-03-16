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

import java.util.List;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TScanInputStats;

/**
 * In-memory cache backend implementation using Guava Cache.
 * This is the default cache backend that stores data in local memory.
 */
public class InMemoryCacheBackend implements CacheBackend {
  private final Cache<String, Object> cache_;

  /**
   * Weigher implementation for calculating the memory footprint of cache entries.
   * Uses approximate Java object layout (64-bit JVM, compressed OOPs).
   */
  static class HBOWeigher implements Weigher<String, Object> {
    // String: object header (16) + hash fields (8) + char[] ref (8) + char[] header (16)
    //     + padding.
    private static final int STRING_BASE_BYTES = 56;
    private static final int BYTES_PER_CHAR = 2;

    // HistoricalStatsValue: object header (16) + runs ref (8) = 24
    private static final int HISTORICAL_STATS_VALUE_BYTES = 24;

    // ArrayList: object header (16) + size (4) + modCount (4) + elementData ref (8)
    //     + padding = 40
    private static final int ARRAYLIST_BASE_BYTES = 40;

    // Reference size (compressed OOPs)
    private static final int REFERENCE_BYTES = 8;

    // TPlanNodeRun: object header (16) + isset byte (1) + padding (7) + 2 longs (16)
    //     + list ref (8) = 48
    private static final int T_PLAN_NODE_RUN_BASE_BYTES = 48;

    // ArrayList<TScanInputStats>: base (40) + refs (8*N) + TScanInputStats objects
    private static final int SCAN_INPUT_STATS_LIST_BASE_BYTES = 40;
    // TScanInputStats: ~16 header + 1 isset + 7 pad + 4×8 primitive fields = 56
    private static final int T_SCAN_INPUT_STATS_BYTES = 56;

    @Override
    public int weigh(String key, Object value) {
      if (value instanceof HistoricalStatsValue) {
        return weighHistoricalStatsValue(key, (HistoricalStatsValue<?>) value);
      }
      return key.length() + 100;
    }

    private static int weighHistoricalStatsValue(String key,
        HistoricalStatsValue<?> value) {
      long size = STRING_BASE_BYTES + key.length() * BYTES_PER_CHAR;
      size += HISTORICAL_STATS_VALUE_BYTES + ARRAYLIST_BASE_BYTES;
      List<?> runs = value.getRuns();
      size += (long) REFERENCE_BYTES * runs.size();
      for (Object run : runs) {
        size += T_PLAN_NODE_RUN_BASE_BYTES;
        if (run instanceof TPlanNodeRun) {
          List<TScanInputStats> scanStats =
              ((TPlanNodeRun) run).getScan_input_stats();
          if (scanStats != null) {
            size += SCAN_INPUT_STATS_LIST_BASE_BYTES
                + (long) REFERENCE_BYTES * scanStats.size()
                + (long) T_SCAN_INPUT_STATS_BYTES * scanStats.size();
          }
        }
      }
      return (int) Math.min(size, Integer.MAX_VALUE);
    }
  }

  /**
   * Create an in-memory cache backend with custom configuration.
   * @param concurrencyLevel The estimated number of concurrent threads
   * @param cacheSizeBytes The maximum weight (approximate memory) of the cache
   */
  public InMemoryCacheBackend(int concurrencyLevel, long cacheSizeBytes) {
    cache_ = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .maximumWeight(cacheSizeBytes)
        .weigher(new HBOWeigher())
        .recordStats()
        .build();
  }

  @Override
  public void put(THboStatsType statsType, String key, Object value) {
    cache_.put(key, value);
  }

  @Override
  public Object getIfPresent(THboStatsType statsType, String key) {
    return cache_.getIfPresent(key);
  }

  @Override
  public String getStats() {
    return cache_.stats().toString();
  }
}

