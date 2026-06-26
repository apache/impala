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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.ToSqlOptions;
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for HBO key string generation. Verifies the raw key strings (before hashing)
 * for specific plan nodes.
 */
public class HboKeyStringTest extends FrontendTestBase {
  private final static Logger LOG = LoggerFactory.getLogger(HboKeyStringTest.class);

  // A shared map of scan child keys for aggregation tests that use the same WHERE clause.
  private static final Map<CanonicalizationStrategy, String> ALLTYPES_SCAN_CHILD_KEYS =
      new HashMap<>();

  static {
    ALLTYPES_SCAN_CHILD_KEYS.put(CanonicalizationStrategy.EXPR_REWRITE,
        "CARDINALITY:ScanNode:functional.alltypes|" +
        "`month` > 1|`year` = 2009|bigint_col IN (0, 1)|int_col = 0|");
    ALLTYPES_SCAN_CHILD_KEYS.put(CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS,
        "CARDINALITY:ScanNode:functional.alltypes|" +
        "`month` > 1|`year`=<CONST>|bigint_col IN (0, 1)|int_col = 0|");
  }

  private List<PlanFragment> planFragments(String query) throws ImpalaException {
    return planFragments(query, new TQueryOptions());
  }

  private List<PlanFragment> planFragments(String query, TQueryOptions options)
      throws ImpalaException {
    options.setUse_hbo_stats(true);
    options.setStore_hbo_stats(true);
    TQueryCtx queryCtx = TestUtils.createQueryContext(options);
    queryCtx.client_request.setStmt(query);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.requestPlanCapture();
    frontend_.createExecRequest(planCtx);
    LOG.info("Query plan:\n{}", planCtx.getExplainString());
    return planCtx.getPlan();
  }

  private void collectAllNodes(PlanNode node, Map<Integer, PlanNode> result) {
    if (node == null) return;
    result.put(node.getId().asInt(), node);
    for (PlanNode child : node.getChildren()) {
      collectAllNodes(child, result);
    }
  }

  private Map<Integer, PlanNode> collectPlanNodesInDistributedPlan(String query)
      throws ImpalaException {
    List<PlanFragment> frags = planFragments(query);
    Map<Integer, PlanNode> result = new HashMap<>();
    collectAllNodes(frags.get(0).getPlanRoot(), result);
    return result;
  }

  /** Returns the topmost (final) AggregationNode in the distributed plan for 'query'. */
  private AggregationNode finalAggNode(String query) throws ImpalaException {
    return findFirstAggNode(planFragments(query).get(0).getPlanRoot());
  }

  private AggregationNode findFirstAggNode(PlanNode node) {
    if (node == null) return null;
    if (node instanceof AggregationNode) return (AggregationNode) node;
    for (PlanNode child : node.getChildren()) {
      AggregationNode agg = findFirstAggNode(child);
      if (agg != null) return agg;
    }
    return null;
  }

  @Test
  public void testScanNodeKeys() throws ImpalaException {
    String query = "SELECT count(*) FROM functional.alltypes " +
        "WHERE year = 2009 AND month > 1 AND int_col = 0 AND bigint_col in (1, 0)";
    Map<Integer, PlanNode> planNodes = collectPlanNodesInDistributedPlan(query);
    HdfsScanNode scanNode = (HdfsScanNode) planNodes.get(0);
    String exprRewriteKey = scanNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE);
    String expectedExprRewriteKey = "CARDINALITY:ScanNode:functional.alltypes|" +
        "`month` > 1|`year` = 2009|bigint_col IN (0, 1)|int_col = 0|";
    assertEquals(expectedExprRewriteKey, exprRewriteKey);

    String ignorePartConstkey = scanNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS);
    String expectedIgnorePartConstKey = "CARDINALITY:ScanNode:functional.alltypes|" +
        "`month` > 1|`year`=<CONST>|bigint_col IN (0, 1)|int_col = 0|";
    assertEquals(expectedIgnorePartConstKey, ignorePartConstkey);
  }

  @Test
  public void testAggregationNodeKeys() throws ImpalaException {
    String query = "SELECT month, count(id) FROM functional.alltypes " +
        "WHERE year = 2009 AND month > 1 AND int_col = 0 AND bigint_col in (1, 0) " +
        "GROUP BY month HAVING count(id) > 10";
    Map<Integer, PlanNode> planNodes = collectPlanNodesInDistributedPlan(query);
    // PreAgg
    AggregationNode aggregationNode = (AggregationNode) planNodes.get(1);
    String exprRewriteKey = aggregationNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE);
    String childKey = ALLTYPES_SCAN_CHILD_KEYS.get(
        CanonicalizationStrategy.EXPR_REWRITE);
    String expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
        "preagg:true|groupingSet:false|AggClasses:[0:GROUP:`month`]|CHILD:[%s]";
    assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
    String ignorePartConstKey = aggregationNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS);
    String ignorePartConstChildKey = ALLTYPES_SCAN_CHILD_KEYS.get(
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS);
    assertEquals(String.format(expectedKeyFmt, ignorePartConstChildKey),
        ignorePartConstKey);
    // FinalAgg
    aggregationNode = (AggregationNode) planNodes.get(3);
    exprRewriteKey = aggregationNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE);
    expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
        "preagg:false|groupingSet:false|AggClasses:[0:GROUP:`month`]|" +
        "HAVING:count(id) > 10|CHILD:[%s]";
    assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
    ignorePartConstKey = aggregationNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS);
    assertEquals(String.format(expectedKeyFmt, ignorePartConstChildKey),
        ignorePartConstKey);
  }

  private static final String ALIAS_TEST_AGG_KEY_FMT =
      "CARDINALITY:AggregationNode:FIRST|preagg:false|groupingSet:false|" +
      "AggClasses:[0:GROUP:%s]|CHILD:[%s]";
  private static final String ALIAS_TEST_SCAN_CHILD_KEY =
      "CARDINALITY:ScanNode:functional.alltypes|";

  @Test
  public void testAggKeyResolvesColumnAlias() throws ImpalaException {
    // The same alias "ai" is used on different columns in the inline view.
    String q1 = "select ai, count(*) from " +
        "(select id ai, int_col from functional.alltypes) t group by ai";
    String q2 = "select ai, count(*) from " +
        "(select id, int_col ai from functional.alltypes) t group by ai";
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      String k1 = finalAggNode(q1).generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      String k2 = finalAggNode(q2).generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      assertEquals(String.format(ALIAS_TEST_AGG_KEY_FMT, "id",
          ALIAS_TEST_SCAN_CHILD_KEY), k1);
      assertEquals(String.format(ALIAS_TEST_AGG_KEY_FMT, "int_col",
          ALIAS_TEST_SCAN_CHILD_KEY), k2);
      assertNotEquals(k1, k2);
    }
  }

  @Test
  public void testAggKeyResolvesExprAlias() throws ImpalaException {
    // Same alias on different expressions.
    String q1 = "select ai, count(*) from " +
        "(select id + int_col ai from functional.alltypes) t group by ai";
    String q2 = "select ai, count(*) from " +
        "(select id + bigint_col ai from functional.alltypes) t group by ai";
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      String k1 = finalAggNode(q1).generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      String k2 = finalAggNode(q2).generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      assertEquals(String.format(ALIAS_TEST_AGG_KEY_FMT, "id + int_col",
          ALIAS_TEST_SCAN_CHILD_KEY), k1);
      assertEquals(String.format(ALIAS_TEST_AGG_KEY_FMT, "id + bigint_col",
          ALIAS_TEST_SCAN_CHILD_KEY), k2);
      assertNotEquals(k1, k2);
    }
  }

  @Test
  public void testAggKeyAliasIndependent() throws ImpalaException {
    // Different aliases "ai" vs. "bi" won't impact identical queries.
    String q1 = "select ai, count(*) from " +
        "(select id ai, int_col from functional.alltypes) t group by ai";
    String q2 = "select bi, count(*) from " +
        "(select id bi, int_col from functional.alltypes) t group by bi";
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      String expected = String.format(ALIAS_TEST_AGG_KEY_FMT, "id",
          ALIAS_TEST_SCAN_CHILD_KEY);
      assertEquals(expected, finalAggNode(q1).generateHboKeyString(
          THboStatsType.CARDINALITY, strategy));
      assertEquals(expected, finalAggNode(q2).generateHboKeyString(
          THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testDistinctAggKeys() throws ImpalaException {
    String query = "SELECT month, count(distinct int_col), count(distinct bigint_col) " +
        "FROM functional.alltypes " +
        "WHERE year = 2009 AND month > 1 AND int_col = 0 AND bigint_col in (1, 0) " +
        "GROUP BY month HAVING count(distinct int_col) > 10";
    Map<Integer, PlanNode> planNodes = collectPlanNodesInDistributedPlan(query);
    for (Map.Entry<CanonicalizationStrategy, String> entry :
        ALLTYPES_SCAN_CHILD_KEYS.entrySet()) {
      CanonicalizationStrategy strategy = entry.getKey();
      String childKey = entry.getValue();
      LOG.info("Testing strategy: {}", strategy);
      // PreAgg for FIRST phase
      AggregationNode aggregationNode = (AggregationNode) planNodes.get(1);
      String exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      String expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
          "preagg:true|groupingSet:false|" +
          "AggClasses:[0:GROUP:`month`,bigint_col,1:GROUP:`month`,int_col]|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // FinalAgg for FIRST phase
      aggregationNode = (AggregationNode) planNodes.get(5);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
          "preagg:false|groupingSet:false|" +
          "AggClasses:[0:GROUP:`month`,bigint_col,1:GROUP:`month`,int_col]|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // PreAgg for MERGE phase
      aggregationNode = (AggregationNode) planNodes.get(2);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:MERGE|" +
          "preagg:true|groupingSet:false|" +
          "AggClasses:[0:GROUP:`month`,1:GROUP:`month`]|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // FinalAgg for MERGE phase
      aggregationNode = (AggregationNode) planNodes.get(7);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:MERGE|" +
          "preagg:false|groupingSet:false|" +
          "AggClasses:[0:GROUP:`month`,1:GROUP:`month`]|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // Agg for TRANSPOSE phase
      aggregationNode = (AggregationNode) planNodes.get(3);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:TRANSPOSE|" +
          "preagg:false|groupingSet:false|AggClasses:[" +
          "0:GROUP:CASE valid_tid(2,4) WHEN 2 THEN `month` WHEN 4 THEN `month` END]|" +
          "HAVING:aggif(valid_tid(2,4) = 2, count(int_col)) > 10|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
    }
  }

  @Test
  public void testGroupingSetsAggKeys() throws ImpalaException {
    String query = "SELECT year, month, count(id) FROM functional.alltypes " +
        "WHERE year = 2009 AND month > 1 AND int_col = 0 AND bigint_col in (1, 0) " +
        "GROUP BY GROUPING SETS((month, year), ())" +
        "HAVING count(id) > 10";
    Map<Integer, PlanNode> planNodes = collectPlanNodesInDistributedPlan(query);
    for (Map.Entry<CanonicalizationStrategy, String> entry :
        ALLTYPES_SCAN_CHILD_KEYS.entrySet()) {
      CanonicalizationStrategy strategy = entry.getKey();
      String childKey = entry.getValue();
      LOG.info("Testing strategy: {}", strategy);
      // PreAgg
      AggregationNode aggregationNode = (AggregationNode) planNodes.get(1);
      String exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      String expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
          "preagg:true|groupingSet:true|" +
          "AggClasses:[0:GROUP:NULL,NULL,1:GROUP:`month`,`year`]|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // FinalAgg
      aggregationNode = (AggregationNode) planNodes.get(4);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
          "preagg:false|groupingSet:true|" +
          "AggClasses:[0:GROUP:NULL,NULL,1:GROUP:`month`,`year`]|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // TRANSPOSE Agg
      aggregationNode = (AggregationNode) planNodes.get(2);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:TRANSPOSE|" +
          "preagg:false|groupingSet:true|AggClasses:[" +
          "0:GROUP:CASE valid_tid(1,2) WHEN 1 THEN 1 WHEN 2 THEN 2 END," +
          "CASE valid_tid(1,2) WHEN 1 THEN `month` WHEN 2 THEN NULL END," +
          "CASE valid_tid(1,2) WHEN 1 THEN `year` WHEN 2 THEN NULL END]|" +
          "HAVING:aggif(valid_tid(1,2) IN (1, 2), " +
          "CASE valid_tid(1,2) WHEN 1 THEN count(id) WHEN 2 THEN count(id) END) > 10|" +
          "CHILD:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
    }
  }

  /**
   * Test for the FOR_HBO rendering of a grouping SlotRef on a column whose name contains
   * a backtick.
   */
  @Test
  public void testAggKeyWithSpecialColumnName() {
    DescriptorTable descTbl = new DescriptorTable();
    TupleDescriptor aggOutTuple = descTbl.createTupleDescriptor("agg-out");
    SlotDescriptor slot = descTbl.addSlotDescriptor(aggOutTuple);
    slot.setType(Type.INT);
    // The grouping expr's source column is literally named `name`one`.
    SlotRef sourceCol = new SlotRef(Lists.newArrayList("name`one"));
    slot.setSourceExpr(sourceCol);
    // Mimic the (fully-qualified) label set by SlotDescriptor.initFromExpr.
    slot.setLabel(ToSqlUtils.getPathSql(Lists.newArrayList("db", "tbl", "name`one")));

    // Building the ref from the (non-scan) descriptor drops rawPath_, matching
    // substitution in AggregationNode.init().
    SlotRef substituted = new SlotRef(slot);
    assertEquals("`name`one`", substituted.toSql(ToSqlOptions.FOR_HBO));
    // The unsubstituted source column (rawPath_ set) renders identically.
    assertEquals("`name`one`", sourceCol.toSql(ToSqlOptions.FOR_HBO));
  }

  @Test
  public void testAggregationNodeLimit() throws ImpalaException {
    String query = "select distinct id from functional.alltypestiny limit 2";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    PlanNode aggNode = nodes.get(1);
    assertEquals("CARDINALITY:AggregationNode:FIRST|preagg:false|groupingSet:false|"
        + "limit:2|AggClasses:[0:GROUP:id]|CHILD:["
        + "CARDINALITY:ScanNode:functional.alltypestiny|]",
        aggNode.generateHboKeyString(
            THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
  }

  private UnionNode singleUnion(String query) throws ImpalaException {
    return singleUnion(query, new TQueryOptions());
  }

  private UnionNode singleUnion(String query, TQueryOptions options)
      throws ImpalaException {
    List<PlanFragment> frags = planFragments(query, options);
    Map<Integer, PlanNode> nodes = new HashMap<>();
    collectAllNodes(frags.get(0).getPlanRoot(), nodes);
    int count = 0;
    UnionNode union = null;
    for (PlanNode n : nodes.values()) {
      if (n instanceof UnionNode) {
        union = (UnionNode) n;
        count++;
      }
    }
    assertEquals("Expected exactly one UnionNode for query: " + query, 1, count);
    return union;
  }

  @Test
  public void testUnionNodeKeys() throws ImpalaException {
    // UNION ALL output cardinality is the sum of the branch cardinalities, which is
    // independent of the branch order. The HBO key sorts the operands so two unions
    // whose branches are written in swapped order produce the same key.
    String q1 = "select id from functional.alltypes where year = 2009 and int_col = 0 "
        + "union all "
        + "select id from functional.alltypestiny where int_col = 0";
    String q2 = "select id from functional.alltypestiny where int_col = 0 "
        + "union all "
        + "select id from functional.alltypes where year = 2009 and int_col = 0";

    UnionNode union1 = singleUnion(q1);
    UnionNode union2 = singleUnion(q2);

    // Operands are sorted by base scan table name, so alltypes comes first.
    String scanAllER =
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009|int_col = 0|";
    String scanTinyER = "CARDINALITY:ScanNode:functional.alltypestiny|int_col = 0|";
    String expectedER = "CARDINALITY:UnionNode:operands:["
        + scanAllER + "," + scanTinyER + "]";
    assertEquals(expectedER, union1.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedER, union2.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));

    String scanAllIPC =
        "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>|int_col = 0|";
    String expectedIPC = "CARDINALITY:UnionNode:operands:["
        + scanAllIPC + "," + scanTinyER + "]";
    assertEquals(expectedIPC, union1.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));

    // Both branch orders hash identically for every strategy.
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals("Union branch order must not affect the key for " + strategy,
          union1.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          union2.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testUnionWithConstOperands() throws ImpalaException {
    // Constant select branches are tracked as constOps in the key and do not add
    // operands.
    String query = "select id, string_col from functional.alltypes "
        + "where year = 2009 and int_col = 0 "
        + "union all select 1, '1' "
        + "union all select 2, '2'";
    PlanNode union = singleUnion(query);
    String scanAllER =
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009|int_col = 0|";
    String expectedER = "CARDINALITY:UnionNode:"
        + "constRows:[(INT:1,STRING:'1'),(INT:2,STRING:'2')]|operands:["
        + scanAllER + "]";
    assertEquals(expectedER, union.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));

    // Test const-only union
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(
        "select 1 union select 2");
    union = nodes.get(0);
    PlanNode agg = nodes.get(1);
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setStore_hbo_stats(true);
    ThriftSerializationCtx serialCtx = new ThriftSerializationCtx(queryOptions);
    // HBO fields of the PlanNode shouldn't be populated for const-only UnionNode.
    TPlanNode msg = new TPlanNode();
    union.toThrift(msg, serialCtx);
    assertFalse(msg.isSetHbo_hash_keys());
    assertFalse(msg.isSetExec_stats());
    // HBO still tracks AggregationNode on const-only UnionNode.
    msg = new TPlanNode();
    agg.toThrift(msg, serialCtx);
    assertTrue(msg.isSetHbo_hash_keys());
    assertTrue(msg.isSetExec_stats());
    assertEquals(0, msg.getExec_stats().getScan_input_statsSize());
    assertEquals("CARDINALITY:AggregationNode:FIRST|preagg:false|groupingSet:false|"
        + "AggClasses:[0:GROUP:1]|CHILD:[CARDINALITY:UnionNode:constRows:"
        + "[(INT:1),(INT:2)]|operands:[]]",
        agg.generateHboKeyString(
            THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
  }

  @Test
  public void testUnionWithNondeterministicConst() throws ImpalaException {
    // A non-deterministic const operand (rand()) cannot be matched against historical
    // runs, so the whole UnionNode is skipped for HBO.
    String query = "select rand() union all select id from functional.alltypes";
    UnionNode union = singleUnion(query);
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertNull(union.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
    // toThrift must not populate HBO fields for a skipped UnionNode.
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setStore_hbo_stats(true);
    ThriftSerializationCtx serialCtx = new ThriftSerializationCtx(queryOptions);
    TPlanNode msg = new TPlanNode();
    union.toThrift(msg, serialCtx);
    assertFalse(msg.isSetHbo_hash_keys());
    assertFalse(msg.isSetExec_stats());
  }

  @Test
  public void testUnionWithNonLiteralConst() throws ImpalaException {
    // With expr rewrites disabled, "1 + 1" is not folded to a literal, but it is still
    // a deterministic constant, so it is tracked in the key via its SQL form.
    TQueryOptions options = new TQueryOptions();
    options.setEnable_expr_rewrites(false);
    String query = "select 1 + 1 union all select id from functional.alltypes";
    UnionNode union = singleUnion(query, options);
    String expectedER = "CARDINALITY:UnionNode:constRows:[(1 + 1)]|operands:["
        + "CARDINALITY:ScanNode:functional.alltypes|]";
    assertEquals(expectedER, union.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
  }

  @Test
  public void testUnionNodeLimit() throws ImpalaException {
    String q = "select id from functional.alltypestiny union all values(9),(10) limit 1";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(q);
    String scanKey = "CARDINALITY:ScanNode:functional.alltypestiny|";
    String innerUnionKey = "CARDINALITY:UnionNode:limit:1|"
        + "constRows:[(INT:9),(INT:10)]|operands:[]";
    String outerUnionKey = "CARDINALITY:UnionNode:operands:["
        + innerUnionKey + "," + scanKey + "]";
    UnionNode innerUnion = (UnionNode) nodes.get(2);
    UnionNode outerUnion = (UnionNode) nodes.get(0);
    assertEquals(innerUnionKey, innerUnion.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(outerUnionKey, outerUnion.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));

    q = "select * from ("
        + "select id from functional.alltypestiny union all values(9),(10)"
        + ") t limit 1";
    UnionNode union = singleUnion(q);
    String unionKey = "CARDINALITY:UnionNode:limit:1|"
        + "constRows:[(INT:9),(INT:10)]|operands:[" + scanKey + "]";
    assertEquals(unionKey, union.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
  }
}
