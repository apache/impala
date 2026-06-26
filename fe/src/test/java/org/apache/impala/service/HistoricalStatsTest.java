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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.impala.planner.CanonicalizationStrategy;
import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TPlanNodeRunWithKeys;
import org.apache.impala.thrift.TScanInputStats;
import org.junit.Test;

/**
 * Unit tests for the per-scan-node-pair similarity matching in {@link HistoricalStats}.
 * Exercises the public read/write API directly. The default similarity threshold in tests
 * (BackendConfig.INSTANCE == null) is 0.1 (10%). Each test uses unique hash keys to avoid
 * cross-test pollution of the shared singleton cache.
 */
public class HistoricalStatsTest {

  private static TScanInputStats scan(long inputRows) {
    return scan(inputRows, -1, -1);
  }

  private static TScanInputStats scan(long inputRows, long catalogVersion,
      long fileSize) {
    TScanInputStats s = new TScanInputStats();
    s.setInput_rows(inputRows);
    s.setCatalog_version(catalogVersion);
    s.setInput_file_size(fileSize);
    return s;
  }

  private static TPlanNodeRun run(long numRows, TScanInputStats... scans) {
    TPlanNodeRun r = new TPlanNodeRun();
    List<TScanInputStats> list = new ArrayList<>(Arrays.asList(scans));
    r.setScan_input_stats(list);
    r.setNum_rows(numRows);
    return r;
  }

  private void write(CanonicalizationStrategy strategy, String key, TPlanNodeRun run) {
    HistoricalStats.INSTANCE.writePlanNodeStats(
        new TPlanNodeRunWithKeys(run, Collections.singletonMap(strategy.toThrift(), key),
            THboStatsType.CARDINALITY));
  }

  private Long read(CanonicalizationStrategy strategy, String key, TPlanNodeRun run) {
    return HistoricalStats.INSTANCE.getPlanNodeOutputRows(
        Collections.singletonMap(strategy, key), "test.tbl", run);
  }

  /** Both scans have valid input_rows: row-count similarity is used. */
  @Test
  public void testRowCountMatchWhenBothValid() {
    String key = "rows-both-valid";
    write(CanonicalizationStrategy.EXPR_REWRITE, key, run(500, scan(1000)));
    // Within 10% -> match.
    assertEquals(Long.valueOf(500),
        read(CanonicalizationStrategy.EXPR_REWRITE, key, run(0, scan(1050))));
    // Beyond 10% -> no match.
    assertNull(
        read(CanonicalizationStrategy.EXPR_REWRITE, key, run(0, scan(1200))));
  }

  /**
   * Mixed run: one scan has valid rows, the other is missing. The valid pair uses row
   * counts; the missing pair uses catalog version then file size (EXPR_REWRITE).
   */
  @Test
  public void testMixedValidAndMissingRows() {
    String key = "rows-mixed";
    // scan0: rows=1000 (valid); scan1: rows=-1, catalog=5, size=2000.
    write(CanonicalizationStrategy.EXPR_REWRITE, key,
        run(777, scan(1000), scan(-1, 5, 2000)));

    // scan0 rows within 10%, scan1 catalog matches -> match.
    assertEquals(Long.valueOf(777), read(CanonicalizationStrategy.EXPR_REWRITE, key,
        run(0, scan(1050), scan(-1, 5, 2100))));

    // scan0 rows off -> no match even though scan1 catalog version matches.
    assertNull(read(CanonicalizationStrategy.EXPR_REWRITE, key,
        run(0, scan(1300), scan(-1, 5, 2000))));

    // scan1 catalog version differs, file size within 10% -> match.
    assertEquals(Long.valueOf(777), read(CanonicalizationStrategy.EXPR_REWRITE, key,
        run(0, scan(1000), scan(-1, 6, 2100))));

    // scan1 catalog version differs and file size off -> no match.
    assertNull(read(CanonicalizationStrategy.EXPR_REWRITE, key,
        run(0, scan(1000), scan(-1, 6, 3000))));
  }

  /**
   * All rows missing, EXPR_REWRITE: exact catalog version match wins; otherwise fall back
   * to file size.
   */
  @Test
  public void testAllMissingExprRewrite() {
    String key = "missing-expr";
    write(CanonicalizationStrategy.EXPR_REWRITE, key, run(42, scan(-1, 10, 1000)));

    // Catalog version matches -> match (file size irrelevant).
    assertEquals(Long.valueOf(42),
        read(CanonicalizationStrategy.EXPR_REWRITE, key, run(0, scan(-1, 10, 9999))));
    // Catalog differs, file size within 10% -> match.
    assertEquals(Long.valueOf(42),
        read(CanonicalizationStrategy.EXPR_REWRITE, key, run(0, scan(-1, 11, 1050))));
    // Catalog differs, file size off -> no match.
    assertNull(
        read(CanonicalizationStrategy.EXPR_REWRITE, key, run(0, scan(-1, 11, 2000))));
  }

  /**
   * All rows missing, non-EXPR_REWRITE strategy: catalog version is ignored; only file
   * size is used.
   */
  @Test
  public void testAllMissingNonExprRewriteUsesFileSizeOnly() {
    String key = "missing-ignore-part";
    CanonicalizationStrategy strategy =
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS;
    write(strategy, key, run(99, scan(-1, 10, 1000)));

    // Catalog version matches (ignored) but file size off -> no match.
    assertNull(read(strategy, key, run(0, scan(-1, 10, 2000))));
    // Catalog version differs but file size within 10% -> match.
    assertEquals(Long.valueOf(99), read(strategy, key, run(0, scan(-1, 999, 1050))));
  }

  /** Different number of scans never matches. */
  @Test
  public void testSizeMismatch() {
    String key = "size-mismatch";
    write(CanonicalizationStrategy.EXPR_REWRITE, key, run(7, scan(1000)));
    assertNull(read(CanonicalizationStrategy.EXPR_REWRITE, key,
        run(0, scan(1000), scan(1000, -1, -1))));
  }

  /**
   * Write/read consistency: two runs that are "similar" only by file size must be deduped
   * on write, so the read path returns the newest and never an accumulated stale run.
   * getSimilarRunIndex returns the first matching index; writes append to the tail. If
   * dedup failed, the read would return the older run's num_rows (100) instead of the
   * newer (200).
   */
  @Test
  public void testWriteReadDedupBySize() {
    String key = "dedup-by-size";
    // Both rows=-1, different catalog versions, similar file sizes -> similar runs.
    write(CanonicalizationStrategy.EXPR_REWRITE, key, run(100, scan(-1, 1, 1000)));
    write(CanonicalizationStrategy.EXPR_REWRITE, key, run(200, scan(-1, 2, 1050)));

    // Newest run returned -> the older one was deduped away on the second write.
    assertEquals(Long.valueOf(200),
        read(CanonicalizationStrategy.EXPR_REWRITE, key, run(0, scan(-1, 3, 1020))));
  }
}
