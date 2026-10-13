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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.service.HistoricalStats;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TCanonicalizationStrategy;
import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TPlanNodeRunWithKeys;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TUnit;
import org.junit.Test;

/**
 * Tests the HBO_MAX_READ_TIME_MS query option, which caps the planner's accumulated
 * HBOStats.TotalTime: once the limit is reached, tryUpdateCardinalityFromHbo() stops
 * reading from the HBO cache for the rest of the query.
 *
 * The cap is enforced against the HBOStats.TotalTime FrontendProfile counter. The counter
 * is seeded directly here so the cutoff is deterministic and does not depend on
 * wall-clock timing. The HBO history is also seeded directly, so the tests do not depend
 * on a previous run of the same query. Each test uses a unique predicate so its keys stay
 * away from the other tests sharing the process-wide history.
 */
public class HboReadTimeLimitTest extends FrontendTestBase {

  private List<PlanNode> planNodes(String query) throws ImpalaException {
    TQueryOptions options = new TQueryOptions();
    options.setUse_hbo_stats(true);
    options.setStore_hbo_stats(true);
    TQueryCtx queryCtx = TestUtils.createQueryContext(options);
    queryCtx.client_request.setStmt(query);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.requestPlanCapture();
    frontend_.createExecRequest(planCtx);
    List<PlanNode> out = new ArrayList<>();
    for (PlanFragment fragment : planCtx.getPlan()) {
      collect(fragment.getPlanRoot(), out);
    }
    return out;
  }

  private static void collect(PlanNode node, List<PlanNode> out) {
    if (node == null) return;
    out.add(node);
    for (PlanNode child : node.getChildren()) collect(child, out);
  }

  private static HdfsScanNode scanOf(List<PlanNode> nodes, String table) {
    for (PlanNode n : nodes) {
      if (n instanceof HdfsScanNode scan
          && scan.getTupleDesc().getTable().getFullName().equals(table)) {
        return scan;
      }
    }
    return null;
  }

  /**
   * Records 'numRows' in the HBO history under 'node's own keys, so that reading HBO
   * stats for that node finds a match.
   */
  private void seedHistory(PlanNode node, long numRows) {
    TPlanNodeRun run = new TPlanNodeRun();
    node.appendScanInputStats(run);
    run.setNum_rows(numRows);
    Map<TCanonicalizationStrategy, String> keys = new HashMap<>();
    for (Map.Entry<CanonicalizationStrategy, String> e :
        node.generateHboHashStrings(THboStatsType.CARDINALITY).entrySet()) {
      keys.put(e.getKey().toThrift(), e.getValue());
    }
    HistoricalStats.INSTANCE.writePlanNodeStats(
        new TPlanNodeRunWithKeys(run, keys, THboStatsType.CARDINALITY));
  }

  /**
   * Builds an analyzer whose query options enable HBO reads with the given time limit.
   */
  private Analyzer analyzerWithLimit(int limitMs) throws ImpalaException {
    TQueryOptions options = new TQueryOptions();
    options.setUse_hbo_stats(true);
    options.setHbo_max_read_time_ms(limitMs);
    AnalysisContext ctx = createAnalysisCtx(options);
    // Parse a trivial query to create the analyzer
    parseAndAnalyze("select 1", ctx);
    return ctx.getAnalyzer();
  }

  private static long skippedCount() {
    return FrontendProfile.getCurrent().getCounterValue(
        HistoricalStats.HBO_STATS_SKIPPED_COUNT);
  }

  private static String query(int salt) {
    // The history is process-wide, so each test uses a predicate of its own.
    return "select id from functional.alltypes where int_col != " + salt;
  }

  @Test
  public void testReadsWhenUnderBudget() throws ImpalaException {
    HdfsScanNode scan = scanOf(planNodes(query(-15037)), "functional.alltypes");
    assertNotNull(scan);
    assertFalse(scan.hasHboMatch());
    seedHistory(scan, 1);

    // Build the analyzer outside the scope: parseAndAnalyze opens its own
    // FrontendProfile scope, which must not nest inside ours.
    Analyzer analyzer = analyzerWithLimit(10);
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      // The counter starts at 0, below the 10ms limit, so the read proceeds.
      scan.tryUpdateCardinalityFromHbo(analyzer);
      assertEquals(0, skippedCount());
    }
    assertTrue(scan.hasHboMatch());
    assertEquals(1L, scan.getCardinality());
  }

  @Test
  public void testStopsWhenBudgetExhausted() throws ImpalaException {
    HdfsScanNode scan = scanOf(planNodes(query(-15038)), "functional.alltypes");
    assertNotNull(scan);
    assertFalse(scan.hasHboMatch());
    long cardinalityBefore = scan.getCardinality();
    seedHistory(scan, 1);

    Analyzer analyzer = analyzerWithLimit(10);
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      // Push accumulated HBOStats.TotalTime above the 10ms limit before the lookup.
      FrontendProfile.getCurrent().setToCounter(
          HistoricalStats.HBO_STATS_TOTAL_TIME, TUnit.TIME_NS, 20L * 1_000_000L);
      scan.tryUpdateCardinalityFromHbo(analyzer);
      assertEquals(1, skippedCount());
      // Every later lookup is skipped and counted. Only the first one logs a warning.
      scan.tryUpdateCardinalityFromHbo(analyzer);
      assertEquals(2, skippedCount());
    }
    assertFalse(scan.hasHboMatch());
    assertEquals(cardinalityBefore, scan.getCardinality());
  }

  @Test
  public void testBudgetBoundary() throws ImpalaException {
    HdfsScanNode first = scanOf(planNodes(query(-15040)), "functional.alltypes");
    HdfsScanNode second = scanOf(planNodes(query(-15041)), "functional.alltypes");
    assertNotNull(first);
    assertNotNull(second);
    assertFalse(first.hasHboMatch());
    assertFalse(second.hasHboMatch());
    long secondCardinalityBefore = second.getCardinality();
    seedHistory(first, 1);
    seedHistory(second, 1);

    long limitNs = 10L * 1_000_000L;
    Analyzer analyzer = analyzerWithLimit(10);
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      FrontendProfile profile = FrontendProfile.getCurrent();
      // 1ns below the limit: the lookup still runs. Its own time pushes the counter to
      // or past the limit, i.e. the node that crosses the threshold still finishes.
      profile.setToCounter(
          HistoricalStats.HBO_STATS_TOTAL_TIME, TUnit.TIME_NS, limitNs - 1);
      first.tryUpdateCardinalityFromHbo(analyzer);
      assertTrue(first.hasHboMatch());
      assertEquals(1, first.getCardinality());
      assertTrue(
          profile.getCounterValue(HistoricalStats.HBO_STATS_TOTAL_TIME) >= limitNs);
      assertEquals(0, skippedCount());

      // Exactly at the limit counts as exhausted (the check is >=), so later nodes
      // skip. Resetting to the exact value keeps this independent of the first
      // lookup's duration.
      profile.setToCounter(HistoricalStats.HBO_STATS_TOTAL_TIME, TUnit.TIME_NS, limitNs);
      second.tryUpdateCardinalityFromHbo(analyzer);
      assertEquals(1, skippedCount());
    }
    assertFalse(second.hasHboMatch());
    assertEquals(secondCardinalityBefore, second.getCardinality());
  }

  @Test
  public void testZeroMeansUnlimited() throws ImpalaException {
    HdfsScanNode scan = scanOf(planNodes(query(-15039)), "functional.alltypes");
    assertNotNull(scan);
    assertFalse(scan.hasHboMatch());
    seedHistory(scan, 1);

    // A limit of 0 disables the cap, so reads happen regardless of accumulated time.
    Analyzer analyzer = analyzerWithLimit(0);
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      FrontendProfile.getCurrent().setToCounter(
          HistoricalStats.HBO_STATS_TOTAL_TIME, TUnit.TIME_NS, 10_000L * 1_000_000L);
      scan.tryUpdateCardinalityFromHbo(analyzer);
      assertEquals(0, skippedCount());
    }
    assertTrue(scan.hasHboMatch());
    assertEquals(1, scan.getCardinality());
  }
}
