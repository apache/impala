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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.JoinNode.HboBuildSideEffect;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.service.HistoricalStats;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TCanonicalizationStrategy;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TPlanNodeRunWithKeys;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the note a join carries when an HBO cardinality on one of its inputs is what
 * decided which side it builds on. The history is seeded directly, so the tests do not
 * depend on a previous run of the same query.
 */
public class HboJoinInversionTest extends FrontendTestBase {
  private static final String EXPLAIN_LINE = "HBO chose the build side";

  private List<PlanFragment> planFragments(String query) throws ImpalaException {
    TQueryOptions options = new TQueryOptions();
    options.setUse_hbo_stats(true);
    options.setStore_hbo_stats(true);
    TQueryCtx queryCtx = TestUtils.createQueryContext(options);
    queryCtx.client_request.setStmt(query);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.requestPlanCapture();
    frontend_.createExecRequest(planCtx);
    return planCtx.getPlan();
  }

  private static void collect(PlanNode node, List<PlanNode> out) {
    if (node == null) return;
    out.add(node);
    for (PlanNode child : node.getChildren()) collect(child, out);
  }

  private List<PlanNode> planNodes(String query) throws ImpalaException {
    List<PlanNode> out = new ArrayList<>();
    collect(planFragments(query).get(0).getPlanRoot(), out);
    return out;
  }

  private static JoinNode firstJoin(List<PlanNode> nodes) {
    for (PlanNode n : nodes) {
      if (n instanceof JoinNode) return (JoinNode) n;
    }
    return null;
  }

  private static HdfsScanNode scanOf(List<PlanNode> nodes, String table) {
    for (PlanNode n : nodes) {
      if (n instanceof HdfsScanNode
          && ((HdfsScanNode) n).getTupleDesc().getTable().getFullName().equals(table)) {
        return (HdfsScanNode) n;
      }
    }
    return null;
  }

  /**
   * Records 'numRows' in the HBO history under 'node's own keys, so that planning the
   * same query again finds a match on that node.
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
   * Explain at EXTENDED prints a fragment header, and that path asserts on MT_DOP being
   * set, so an empty TQueryOptions is not enough to render a plan node.
   */
  private static TQueryOptions explainOptions() {
    TQueryOptions options = new TQueryOptions();
    options.setMt_dop(0);
    return options;
  }

  /**
   * The history is process-wide, so each test uses a predicate of its own to keep its
   * keys away from the other tests here.
   */
  private static String query(int salt) {
    return "select a.id from functional.alltypes a "
        + "join functional.alltypestiny b on a.id = b.id "
        + "where a.int_col != " + salt;
  }

  @Test
  public void testNoMatchLeavesNoNote() throws ImpalaException {
    JoinNode join = firstJoin(planNodes(query(-15312)));
    assertNotNull(join);
    assertEquals(HboBuildSideEffect.NONE, join.getHboBuildSideEffect());
    assertFalse(join.getExplainString(explainOptions()).contains(EXPLAIN_LINE));
  }

  /**
   * alltypes (6.57K rows under the predicate) against alltypestiny (8 rows): the cost
   * model inverts the join to build on the small side. An HBO match that cuts alltypes
   * to a single row takes that inversion away, and the join records it.
   */
  @Test
  public void testMatchKeepsJoinUninverted() throws ImpalaException {
    String q = query(-15313);
    List<PlanNode> before = planNodes(q);
    assertEquals(HboBuildSideEffect.NONE, firstJoin(before).getHboBuildSideEffect());
    HdfsScanNode alltypes = scanOf(before, "functional.alltypes");
    assertNotNull(alltypes);
    seedHistory(alltypes, 1);

    JoinNode join = firstJoin(planNodes(q));
    assertNotNull(join);
    assertEquals(HboBuildSideEffect.KEPT, join.getHboBuildSideEffect());
    String explain = join.getExplainString(explainOptions());
    assertTrue(explain, explain.contains(
        "HBO chose the build side: without it this join would have been inverted"));
  }

  /** The note is a detail line, so it stays out of the default explain level. */
  @Test
  public void testNoteIsExtendedOnly() throws ImpalaException {
    String q = query(-15314);
    HdfsScanNode alltypes = scanOf(planNodes(q), "functional.alltypes");
    assertNotNull(alltypes);
    seedHistory(alltypes, 1);

    JoinNode join = firstJoin(planNodes(q));
    assertNotNull(join);
    assertEquals(HboBuildSideEffect.KEPT, join.getHboBuildSideEffect());
    TQueryOptions options = explainOptions();
    assertFalse(join.getExplainString("", "", options, TExplainLevel.STANDARD)
        .contains(EXPLAIN_LINE));
    assertTrue(join.getExplainString("", "", options, TExplainLevel.EXTENDED)
        .contains(EXPLAIN_LINE));
  }
}
