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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.THistoricalStatsValue;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.Nonnull;

/**
 * Distributed HBO cache backend backed by a Redis/Valkey server.
 *
 * <p>Values are the same {@link HistoricalStatsValue}&lt;{@link TPlanNodeRun}&gt; objects
 * the in-memory backend stores, serialized to Thrift binary via the
 * {@link THistoricalStatsValue} wrapper struct. Thrift's field-id-based schema evolution
 * keeps entries compatible as TPlanNodeRun / TScanInputStats gain fields; an entry that
 * cannot be decoded is treated as a cache miss rather than an error.
 *
 * <p>All Redis interactions are fail-soft: any client or serialization error is counted,
 * logged at WARN, and swallowed so that HBO degrades to the planner's default estimates
 * instead of failing the query.
 */
public class RedisCacheBackend implements CacheBackend {
  private final static Logger LOG = LoggerFactory.getLogger(RedisCacheBackend.class);

  // Prefix for every HBO key written by this backend. Namespacing by stats type keeps
  // future stats types (e.g. PEAK_MEMORY) from colliding in a shared Redis keyspace and
  // makes the HBO entries easy to spot/scan/flush independently of other Redis users.
  private static final String KEY_PREFIX = "hbo";

  private final JedisPool jedisPool_;
  // Sliding TTL in seconds applied on every write. 0 means entries never expire.
  private final int ttlSeconds_;
  private final AtomicLong hits_ = new AtomicLong(0);
  private final AtomicLong misses_ = new AtomicLong(0);
  private final AtomicLong errors_ = new AtomicLong(0);

  /**
   * Create a Redis/Valkey cache backend.
   * @param host Redis server hostname
   * @param port Redis server port
   * @param password Redis server password (null or empty if no auth required)
   * @param database Redis logical database index
   * @param timeoutMs Connection and socket timeout in milliseconds
   * @param maxConnections Maximum number of connections in the pool (JedisPool maxTotal)
   * @param ttlSeconds Sliding TTL in seconds applied on every write
   */
  public RedisCacheBackend(String host, int port, String password, int database,
      int timeoutMs, int maxConnections, int ttlSeconds) {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(maxConnections);
    poolConfig.setMaxWait(Duration.ofMillis(timeoutMs));
    poolConfig.setMaxIdle(8);
    poolConfig.setMinIdle(2);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(false);
    poolConfig.setTestWhileIdle(true);

    String auth = Strings.isNullOrEmpty(password) ? null : password;
    jedisPool_ = new JedisPool(poolConfig, host, port, timeoutMs, auth, database);
    ttlSeconds_ = ttlSeconds;
    LOG.info("Initialized Redis/Valkey HBO cache backend: {}:{} db={} ttlSeconds={}",
        host, port, database, ttlSeconds);
  }

  @Override
  public void put(THboStatsType statsType, String key, Object value) {
    if (!(value instanceof HistoricalStatsValue)) {
      // Should not happen: HistoricalStats only stores HistoricalStatsValue. Guard
      // defensively so an unexpected value type can never crash a write.
      errors_.incrementAndGet();
      LOG.warn("Unexpected HBO cache value type {} for key {}; skipping Redis put",
          value == null ? "null" : value.getClass().getName(), key);
      return;
    }
    @SuppressWarnings("unchecked")
    HistoricalStatsValue<TPlanNodeRun> statsValue =
        (HistoricalStatsValue<TPlanNodeRun>) value;
    try (Jedis jedis = jedisPool_.getResource()) {
      byte[] redisKey = buildKey(statsType, key);
      byte[] payload = serialize(statsValue);
      if (ttlSeconds_ > 0) {
        jedis.setex(redisKey, ttlSeconds_, payload);
      } else {
        jedis.set(redisKey, payload);
      }
    } catch (JedisException | TException e) {
      errors_.incrementAndGet();
      LOG.warn("Failed to put HBO key {} (stats type {}) to Redis: {}", key, statsType,
          e.getMessage());
    }
  }

  @Override
  public Object getIfPresent(THboStatsType statsType, String key) {
    try (Jedis jedis = jedisPool_.getResource()) {
      byte[] serialized = jedis.get(buildKey(statsType, key));
      if (serialized == null) {
        misses_.incrementAndGet();
        return null;
      }
      hits_.incrementAndGet();
      return deserialize(serialized);
    } catch (JedisException | TException e) {
      errors_.incrementAndGet();
      LOG.warn("Failed to get HBO key {} (stats type {}) from Redis: {}", key, statsType,
          e.getMessage());
      return null;
    }
  }

  @Override
  public @Nonnull String clear() {
    // Flush the entire configured Redis/Valkey logical database.
    try (Jedis jedis = jedisPool_.getResource()) {
      jedis.flushDB();
      return "";
    } catch (JedisException e) {
      errors_.incrementAndGet();
      LOG.warn("Failed to clear HBO Redis database: {}", e.getMessage());
      return "Failed to clear HBO Redis database: " + e.getMessage();
    }
  }

  @Override
  public String getStats() {
    return String.format("Redis cache stats - hits: %d, misses: %d, errors: %d",
        hits_.get(), misses_.get(), errors_.get());
  }

  /**
   * Build the Redis key for an HBO entry, namespaced by stats type. The format is
   * {@code hbo:<STATS_TYPE>:<hashKey>} encoded as UTF-8 bytes.
   */
  @VisibleForTesting
  static byte[] buildKey(THboStatsType statsType, String key) {
    return (KEY_PREFIX + ":" + statsType.name() + ":" + key)
        .getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Serialize a cached HBO value to Thrift binary via the THistoricalStatsValue wrapper.
   */
  @VisibleForTesting
  static byte[] serialize(HistoricalStatsValue<TPlanNodeRun> value) throws TException {
    THistoricalStatsValue wrapper = new THistoricalStatsValue();
    // Copy into a fresh list so the wrapper never aliases the live cached list.
    wrapper.setRuns(new ArrayList<>(value.getRuns()));
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    return serializer.serialize(wrapper);
  }

  /**
   * Deserialize a Thrift-binary HBO value produced by {@link #serialize}. Returns a
   * HistoricalStatsValue holding the decoded runs (possibly empty).
   */
  @VisibleForTesting
  static HistoricalStatsValue<TPlanNodeRun> deserialize(byte[] bytes) throws TException {
    THistoricalStatsValue wrapper = new THistoricalStatsValue();
    TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    deserializer.deserialize(wrapper, bytes);
    List<TPlanNodeRun> runs =
        wrapper.isSetRuns() ? wrapper.getRuns() : new ArrayList<>();
    return new HistoricalStatsValue<>(runs);
  }

  /**
   * Close the Redis connection pool.
   */
  public void close() {
    if (jedisPool_ != null && !jedisPool_.isClosed()) {
      jedisPool_.close();
      LOG.info("Redis/Valkey HBO cache connection pool closed");
    }
  }
}
