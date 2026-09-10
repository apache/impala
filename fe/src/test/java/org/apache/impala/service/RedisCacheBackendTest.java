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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TScanInputStats;
import org.apache.thrift.TException;
import org.junit.Test;

/**
 * Unit tests for {@link RedisCacheBackend} that do not require a running Redis/Valkey
 * server.
 */
public class RedisCacheBackendTest {

  private static TScanInputStats scan(long inputRows, long catalogVersion,
      long numInputFiles, long inputFileSize) {
    TScanInputStats s = new TScanInputStats();
    s.setInput_rows(inputRows);
    s.setCatalog_version(catalogVersion);
    s.setNum_input_files(numInputFiles);
    s.setInput_file_size(inputFileSize);
    return s;
  }

  private static TPlanNodeRun run(long numRows, long memUsage,
      TScanInputStats... scans) {
    TPlanNodeRun r = new TPlanNodeRun();
    r.setScan_input_stats(new ArrayList<>(Arrays.asList(scans)));
    r.setNum_rows(numRows);
    r.setMem_usage(memUsage);
    return r;
  }

  /** Bind and immediately release a port to get one that is (very likely) not listening,
   * so a connection attempt gets "connection refused" fast. */
  private static int findClosedPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * A fake TCP server that accepts connections but never answers them. Unlike a closed
   * port (which fails fast with "connection refused"), this holds each accepted socket
   * open without writing a reply, so a Jedis read blocks until the socket timeout fires.
   * This is what lets a test exercise the operation timeout against a slow/unresponsive
   * Redis/Valkey server rather than an unreachable one.
   */
  private static final class SlowServer implements Closeable {
    private final ServerSocket serverSocket_ = new ServerSocket(0);
    // Accepted connections are held open (never read to completion, never replied to) so
    // the client stalls. Keep references so they can be closed during teardown.
    private final Collection<Socket> accepted_ = new ConcurrentLinkedQueue<>();

    SlowServer() throws IOException {
      Thread acceptor = new Thread(() -> {
        while (!serverSocket_.isClosed()) {
          try {
            // Accept and hold: do not read the request or write a response. The client's
            // read (e.g. the testOnBorrow PING) blocks until its socket timeout.
            accepted_.add(serverSocket_.accept());
          } catch (IOException e) {
            return; // Socket closed during teardown; exit the accept loop.
          }
        }
      });
      acceptor.setDaemon(true);
      acceptor.start();
    }

    int getLocalPort() {
      return serverSocket_.getLocalPort();
    }

    @Override
    public void close() {
      closeQuietly(serverSocket_);
      accepted_.forEach(SlowServer::closeQuietly);
    }

    private static void closeQuietly(Closeable closeable) {
      try {
        closeable.close();
      } catch (IOException ignored) {
        // Best effort during teardown
      }
    }
  }

  @Test
  public void testSerializeDeserializeRoundTrip() throws Exception {
    TPlanNodeRun run1 = run(1000L, 4096L,
        scan(500L, 7L, 3L, 123456L), scan(250L, 7L, 1L, 65536L));
    TPlanNodeRun run2 = run(2000L, 8192L, scan(1500L, 8L, 5L, 987654L));
    HistoricalStatsValue<TPlanNodeRun> value =
        new HistoricalStatsValue<>(Arrays.asList(run1, run2));

    byte[] bytes = RedisCacheBackend.serialize(value);
    HistoricalStatsValue<TPlanNodeRun> decoded = RedisCacheBackend.deserialize(bytes);

    // TPlanNodeRun / TScanInputStats are Thrift structs with value-based equals(), so the
    // decoded runs must equal the originals field-for-field and preserve their order.
    assertEquals(Arrays.asList(run1, run2), decoded.getRuns());
  }

  @Test
  public void testRoundTripSingleRunConstructor() throws Exception {
    // The single-run constructor is what HistoricalStats uses for a brand-new key.
    TPlanNodeRun run = run(42L, 2048L, scan(10L, 1L, 1L, 1024L));
    HistoricalStatsValue<TPlanNodeRun> value = new HistoricalStatsValue<>(run);

    byte[] bytes = RedisCacheBackend.serialize(value);
    HistoricalStatsValue<TPlanNodeRun> decoded = RedisCacheBackend.deserialize(bytes);

    assertEquals(1, decoded.size());
    assertEquals(run, decoded.getRuns().get(0));
  }

  @Test
  public void testRoundTripEmptyRuns() throws Exception {
    // A value with no runs must round-trip to an empty (non-null) run list.
    HistoricalStatsValue<TPlanNodeRun> value =
        new HistoricalStatsValue<>(Collections.<TPlanNodeRun>emptyList());

    byte[] bytes = RedisCacheBackend.serialize(value);
    HistoricalStatsValue<TPlanNodeRun> decoded = RedisCacheBackend.deserialize(bytes);

    assertTrue(decoded.getRuns().isEmpty());
  }

  @Test(expected = TException.class)
  public void testDeserializeGarbageThrows() throws TException {
    // Undecodable bytes must surface as a TException so the caller counts an error and
    // treats the entry as a cache miss, rather than returning a bogus value. The bytes
    // below are not a valid TBinaryProtocol encoding of THistoricalStatsValue.
    byte[] garbage = new byte[] {0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f};
    RedisCacheBackend.deserialize(garbage);
  }

  @Test
  public void testBuildKeyFormatAndNamespacing() {
    byte[] key = RedisCacheBackend.buildKey(THboStatsType.CARDINALITY, "abc123");
    String keyStr = new String(key, StandardCharsets.UTF_8);
    // Format is hbo:<STATS_TYPE>:<hashKey>.
    assertEquals("hbo:CARDINALITY:abc123", keyStr);
  }

  @Test
  public void testFailSoftWhenServerUnreachable() throws Exception {
    int closedPort = findClosedPort();
    // Short timeout so the (refused) connection attempts do not slow the test.
    RedisCacheBackend backend =
        new RedisCacheBackend("localhost", closedPort, "", 0, 500, 16, 3600);
    try {
      TPlanNodeRun run = run(1L, 1L, scan(1L, 1L, 1L, 1L));
      HistoricalStatsValue<TPlanNodeRun> value = new HistoricalStatsValue<>(run);

      // Neither of put() nor get() may throw even though the server is unreachable.
      backend.put(THboStatsType.CARDINALITY, "somekey", value);
      Object got = backend.getIfPresent(THboStatsType.CARDINALITY, "somekey");
      assertNull("A failed get must be a miss (null), not an exception", got);

      // Each failed operation increments the error counter. Neither counts as a hit nor
      // a real (key-absent) miss.
      assertEquals("Redis cache stats - hits: 0, misses: 0, errors: 2",
          backend.getStats());
    } finally {
      backend.close();
    }
  }

  @Test
  public void testClearFailSoftWhenServerUnreachable() throws Exception {
    int closedPort = findClosedPort();
    RedisCacheBackend backend =
        new RedisCacheBackend("localhost", closedPort, "", 0, 500, 16, 3600);
    try {
      // clear() against an unreachable server must not throw. It's counted as a single
      // error, and the error message is returned.
      String error = backend.clear();
      assertNotNull("A failed clear must return an error message, not null", error);
      assertTrue("Unexpected clear error message: " + error,
          error.contains("Failed to clear HBO Redis database"));
      assertEquals("Redis cache stats - hits: 0, misses: 0, errors: 1",
          backend.getStats());
    } finally {
      backend.close();
    }
  }

  /**
   * A server that accepts the connection but never responds must not hang a cache
   * operation: the configured timeout has to fire so put()/getIfPresent() fail soft
   * within a bounded time. This complements testFailSoftWhenServerUnreachable, which only
   * covers a closed port (a fast "connection refused" that never exercises a timeout).
   */
  @Test(timeout = 60000)
  public void testFailSoftWhenServerSlow() throws Exception {
    try (SlowServer slowServer = new SlowServer()) {
      // Short 500ms timeout so the stalled operations return quickly.
      RedisCacheBackend backend = new RedisCacheBackend(
          "localhost", slowServer.getLocalPort(), "", 0, 500, 16, 3600);
      try {
        TPlanNodeRun run = run(1L, 1L, scan(1L, 1L, 1L, 1L));
        HistoricalStatsValue<TPlanNodeRun> value = new HistoricalStatsValue<>(run);

        long startNanos = System.nanoTime();
        // Neither of put() nor get() may throw even though the server never answers. The
        // timeout turns the stalled read into a swallowed error.
        backend.put(THboStatsType.CARDINALITY, "somekey", value);
        Object got = backend.getIfPresent(THboStatsType.CARDINALITY, "somekey");
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertNull("A timed-out get must be a miss (null), not an exception", got);
        // put and get are counted as errors.
        assertEquals("Redis cache stats - hits: 0, misses: 0, errors: 2",
            backend.getStats());
        // Both operations together are bounded to a small multiple of the 500ms timeout
        // (each is roughly one timeout, so ~1s total). The generous 10s bound proves the
        // timeout fired without being flaky on a loaded machine.
        assertTrue("Operations must time out, not hang; elapsedMs=" + elapsedMs,
            elapsedMs < 10_000);
      } finally {
        backend.close();
      }
    }
  }

  @Test
  public void testPutIgnoresUnexpectedValueType() throws Exception {
    int closedPort = findClosedPort();
    RedisCacheBackend backend =
        new RedisCacheBackend("localhost", closedPort, "", 0, 500, 16, 0);
    try {
      // A non-HistoricalStatsValue must be rejected before any Redis call. It is counted
      // as an error and swallowed, and must not reach the server (so the connection is
      // never even attempted).
      backend.put(THboStatsType.CARDINALITY, "somekey", "not a stats value");
      assertEquals("Redis cache stats - hits: 0, misses: 0, errors: 1",
          backend.getStats());
    } finally {
      backend.close();
    }
  }
}
