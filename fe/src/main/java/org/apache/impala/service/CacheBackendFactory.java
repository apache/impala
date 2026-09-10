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

import org.apache.impala.thrift.THboBackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects and constructs the {@link CacheBackend} used by {@link HistoricalStats}, based
 * on the {@code --hbo_cache_backend} startup flag.
 *
 * <p>The {@code --hbo_cache_backend} flag is validated in the backend and reaches the
 * frontend as a {@link THboBackendType}. An invalid flag value fails startup in the
 * backend, so only recognized values ever arrive here.
 */
public class CacheBackendFactory {
  private final static Logger LOG = LoggerFactory.getLogger(CacheBackendFactory.class);

  private CacheBackendFactory() {}

  /**
   * Create the configured cache backend. Never null.
   */
  public static CacheBackend create() {
    BackendConfig config = BackendConfig.INSTANCE;
    THboBackendType backend = THboBackendType.IN_MEMORY;
    int concurrencyLevel = 4;
    long cacheSizeBytes = 1024L * 1024 * 1024;
    // BackendConfig.INSTANCE could be null in tests.
    if (config != null) {
      if (config.getHboCacheBackend() != null) backend = config.getHboCacheBackend();
      concurrencyLevel = config.getUnregistrationThreadPoolSize();
      cacheSizeBytes = config.getHboInMemoryBackendCacheSizeBytes();
    }
    switch (backend) {
      case REDIS:
        LOG.info("Initializing HBO cache with the Redis/Valkey backend at {}:{} db={}",
            config.getHboCacheRedisHost(), config.getHboCacheRedisPort(),
            config.getHboCacheRedisDb());
        return new RedisCacheBackend(config.getHboCacheRedisHost(),
            config.getHboCacheRedisPort(), config.getHboCacheRedisPassword(),
            config.getHboCacheRedisDb(), config.getHboCacheRedisTimeoutMs(),
            config.getHboCacheRedisMaxConnections(),
            config.getHboCacheRedisTtlSeconds());
      case IN_MEMORY:
      default:
        LOG.info("Initializing HBO cache with the in-memory backend");
        return new InMemoryCacheBackend(concurrencyLevel, cacheSizeBytes);
    }
  }
}
