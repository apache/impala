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

import org.apache.impala.thrift.THboStatsType;

/**
 * Interface for cache storage backends used by HistoryStats.
 * Implementations can provide different storage mechanisms such as
 * in-memory caching or distributed caching via Redis or HBase.
 */
public interface CacheBackend {
  /**
   * Store a key-value pair in the cache.
   * @param statsType The type of HBO statistics (CARDINALITY or PEAK_MEMORY)
   * @param key The cache key
   * @param value The value to store
   */
  void put(THboStatsType statsType, String key, Object value);

  /**
   * Retrieve a value from the cache if present.
   * @param statsType The type of HBO statistics (CARDINALITY or PEAK_MEMORY)
   * @param key The cache key
   * @return The cached value, or null if not present
   */
  Object getIfPresent(THboStatsType statsType, String key);

  /**
   * Get statistics about the cache (for monitoring/debugging).
   * @return A human-readable string with cache statistics
   */
  String getStats();
}

