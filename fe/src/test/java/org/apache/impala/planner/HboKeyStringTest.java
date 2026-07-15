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
import static org.junit.Assert.assertNotNull;
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
        "`month` > 1|`year` = 2009|bigint_col IN (0, 1)|int_col = 0");
    ALLTYPES_SCAN_CHILD_KEYS.put(CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS,
        "CARDINALITY:ScanNode:functional.alltypes|" +
        "`month` > 1|`year`=<CONST>|bigint_col IN (0, 1)|int_col = 0");
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
        "`month` > 1|`year` = 2009|bigint_col IN (0, 1)|int_col = 0";
    assertEquals(expectedExprRewriteKey, exprRewriteKey);

    String ignorePartConstkey = scanNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS);
    String expectedIgnorePartConstKey = "CARDINALITY:ScanNode:functional.alltypes|" +
        "`month` > 1|`year`=<CONST>|bigint_col IN (0, 1)|int_col = 0";
    assertEquals(expectedIgnorePartConstKey, ignorePartConstkey);
  }

  /**
   * Returns the key of the scan in a query with a single scan, which is plan node 0.
   */
  private String singleScanKey(String query) throws ImpalaException {
    return collectPlanNodesInDistributedPlan(query).get(0).generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE);
  }

  /**
   * An optimized count(*) scan outputs one row per file or row group, so it must not
   * share a key with a regular scan of the same table and conjuncts.
   */
  @Test
  public void testCountStarScanNodeKey() throws ImpalaException {
    assertEquals("CARDINALITY:ScanNode:functional_parquet.alltypes|<COUNT_STAR>|" +
        "`year` = 2009", singleScanKey(
            "SELECT count(*) FROM functional_parquet.alltypes WHERE year = 2009"));
    assertEquals("CARDINALITY:ScanNode:functional_parquet.alltypes|`year` = 2009",
        singleScanKey(
            "SELECT max(id) FROM functional_parquet.alltypes WHERE year = 2009"));
  }

  /**
   * A partition key scan outputs one row per scan range and a sampled scan reads only
   * part of the files, so neither may share a key with a regular scan of the same table
   * and conjuncts.
   */
  @Test
  public void testPartitionKeyAndSampledScanNodeKeys() throws ImpalaException {
    String alltypesKey = "CARDINALITY:ScanNode:functional_parquet.alltypes";
    assertEquals(alltypesKey,
        singleScanKey("SELECT year FROM functional_parquet.alltypes"));
    assertEquals(alltypesKey + "|<PARTITION_KEY_SCAN>",
        singleScanKey("SELECT DISTINCT year FROM functional_parquet.alltypes"));
    assertEquals(alltypesKey + "|<SAMPLE:10>", singleScanKey("SELECT year FROM " +
        "functional_parquet.alltypes TABLESAMPLE SYSTEM(10) REPEATABLE(1)"));

    String icebergKey = "CARDINALITY:ScanNode:functional_parquet.iceberg_partitioned";
    assertEquals(icebergKey,
        singleScanKey("SELECT id FROM functional_parquet.iceberg_partitioned"));
    assertEquals(icebergKey + "|<SAMPLE:10>", singleScanKey("SELECT id FROM " +
        "functional_parquet.iceberg_partitioned TABLESAMPLE SYSTEM(10) REPEATABLE(1)"));
  }

  /**
   * With an IsNotEmptyPredicate, conjuncts on the items of a collection also filter the
   * rows of the scan, so they are part of its key. The collection is named by its path,
   * not by its alias.
   */
  @Test
  public void testCollectionConjunctsInScanNodeKey() throws ImpalaException {
    String tblKey =
        "CARDINALITY:ScanNode:functional_parquet.complextypestbl|!empty(int_array)";
    assertEquals(tblKey, singleScanKey("SELECT c.id FROM " +
        "functional_parquet.complextypestbl c, c.int_array a"));
    assertEquals(tblKey + "|functional_parquet.complextypestbl.int_array:[item > 1]",
        singleScanKey("SELECT c.id FROM functional_parquet.complextypestbl c, " +
            "c.int_array a WHERE a.item > 1"));
    assertEquals(tblKey + "|functional_parquet.complextypestbl.int_array:[item > 100]",
        singleScanKey("SELECT t.id FROM functional_parquet.complextypestbl t, " +
            "t.int_array x WHERE x.item > 100"));
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
        "Preagg:true|GroupingSet:false|AggClasses:[0:Group:`month`]|Child:[%s]";
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
        "Preagg:false|GroupingSet:false|AggClasses:[0:Group:`month`]|" +
        "Having:count(id) > 10|Child:[%s]";
    assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
    ignorePartConstKey = aggregationNode.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS);
    assertEquals(String.format(expectedKeyFmt, ignorePartConstChildKey),
        ignorePartConstKey);
  }

  private static final String ALIAS_TEST_AGG_KEY_FMT =
      "CARDINALITY:AggregationNode:FIRST|Preagg:false|GroupingSet:false|" +
      "AggClasses:[0:Group:%s]|Child:[%s]";
  private static final String ALLTYPES_SCAN_KEY =
      "CARDINALITY:ScanNode:functional.alltypes";
  private static final String ALLTYPESTINY_SCAN_KEY =
      "CARDINALITY:ScanNode:functional.alltypestiny";
  private static final String ALLTYPESSMALL_SCAN_KEY =
      "CARDINALITY:ScanNode:functional.alltypessmall";

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
          ALLTYPES_SCAN_KEY), k1);
      assertEquals(String.format(ALIAS_TEST_AGG_KEY_FMT, "int_col",
          ALLTYPES_SCAN_KEY), k2);
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
          ALLTYPES_SCAN_KEY), k1);
      assertEquals(String.format(ALIAS_TEST_AGG_KEY_FMT, "id + bigint_col",
          ALLTYPES_SCAN_KEY), k2);
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
          ALLTYPES_SCAN_KEY);
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
          "Preagg:true|GroupingSet:false|" +
          "AggClasses:[0:Group:`month`,bigint_col,1:Group:`month`,int_col]|" +
          "Child:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // FinalAgg for FIRST phase
      aggregationNode = (AggregationNode) planNodes.get(5);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
          "Preagg:false|GroupingSet:false|" +
          "AggClasses:[0:Group:`month`,bigint_col,1:Group:`month`,int_col]|" +
          "Child:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // PreAgg for MERGE phase
      aggregationNode = (AggregationNode) planNodes.get(2);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:MERGE|" +
          "Preagg:true|GroupingSet:false|" +
          "AggClasses:[0:Group:`month`,1:Group:`month`]|" +
          "Child:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // FinalAgg for MERGE phase
      aggregationNode = (AggregationNode) planNodes.get(7);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:MERGE|" +
          "Preagg:false|GroupingSet:false|" +
          "AggClasses:[0:Group:`month`,1:Group:`month`]|" +
          "Child:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // Agg for TRANSPOSE phase
      aggregationNode = (AggregationNode) planNodes.get(3);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:TRANSPOSE|" +
          "Preagg:false|GroupingSet:false|AggClasses:[" +
          "0:Group:CASE valid_tid(2,4) WHEN 2 THEN `month` WHEN 4 THEN `month` END]|" +
          "Having:aggif(valid_tid(2,4) = 2, count(int_col)) > 10|" +
          "Child:[%s]";
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
          "Preagg:true|GroupingSet:true|" +
          "AggClasses:[0:Group:NULL,NULL,1:Group:`month`,`year`]|" +
          "Child:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // FinalAgg
      aggregationNode = (AggregationNode) planNodes.get(4);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:FIRST|" +
          "Preagg:false|GroupingSet:true|" +
          "AggClasses:[0:Group:NULL,NULL,1:Group:`month`,`year`]|" +
          "Child:[%s]";
      assertEquals(String.format(expectedKeyFmt, childKey), exprRewriteKey);
      // TRANSPOSE Agg
      aggregationNode = (AggregationNode) planNodes.get(2);
      exprRewriteKey = aggregationNode.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy);
      expectedKeyFmt = "CARDINALITY:AggregationNode:TRANSPOSE|" +
          "Preagg:false|GroupingSet:true|AggClasses:[" +
          "0:Group:CASE valid_tid(1,2) WHEN 1 THEN 1 WHEN 2 THEN 2 END," +
          "CASE valid_tid(1,2) WHEN 1 THEN `month` WHEN 2 THEN NULL END," +
          "CASE valid_tid(1,2) WHEN 1 THEN `year` WHEN 2 THEN NULL END]|" +
          "Having:aggif(valid_tid(1,2) IN (1, 2), " +
          "CASE valid_tid(1,2) WHEN 1 THEN count(id) WHEN 2 THEN count(id) END) > 10|" +
          "Child:[%s]";
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
    assertEquals("CARDINALITY:AggregationNode:FIRST|limit:2|Preagg:false|"
        + "GroupingSet:false|AggClasses:[0:Group:id]|Child:["
        + "CARDINALITY:ScanNode:functional.alltypestiny]",
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
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009|int_col = 0";
    String scanTinyER = "CARDINALITY:ScanNode:functional.alltypestiny|int_col = 0";
    String expectedER = "CARDINALITY:UnionNode:|Operands:["
        + scanAllER + "," + scanTinyER + "]";
    assertEquals(expectedER, union1.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedER, union2.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));

    String scanAllIPC =
        "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>|int_col = 0";
    String expectedIPC = "CARDINALITY:UnionNode:|Operands:["
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
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009|int_col = 0";
    String expectedER = "CARDINALITY:UnionNode:"
        + "|ConstRows:[(INT:1,STRING:'1'),(INT:2,STRING:'2')]|Operands:["
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
    assertEquals("CARDINALITY:AggregationNode:FIRST|Preagg:false|GroupingSet:false|"
        + "AggClasses:[0:Group:1]|Child:[CARDINALITY:UnionNode:|ConstRows:"
        + "[(INT:1),(INT:2)]|Operands:[]]",
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
    String expectedER = "CARDINALITY:UnionNode:|ConstRows:[(1 + 1)]|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes]";
    assertEquals(expectedER, union.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
  }

  @Test
  public void testUnionNodeLimit() throws ImpalaException {
    String q = "select id from functional.alltypestiny union all values(9),(10) limit 1";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(q);
    String scanKey = "CARDINALITY:ScanNode:functional.alltypestiny";
    String innerUnionKey = "CARDINALITY:UnionNode:|limit:1|"
        + "ConstRows:[(INT:9),(INT:10)]|Operands:[]";
    String outerUnionKey = "CARDINALITY:UnionNode:|Operands:["
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
    String unionKey = "CARDINALITY:UnionNode:|limit:1|"
        + "ConstRows:[(INT:9),(INT:10)]|Operands:[" + scanKey + "]";
    assertEquals(unionKey, union.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
  }

  /** Counts the number of JoinNodes in the plan node map. */
  private int countJoinNodes(Map<Integer, PlanNode> planNodes) {
    int count = 0;
    for (PlanNode n : planNodes.values()) {
      if (n instanceof JoinNode) count++;
    }
    return count;
  }

  private JoinNode singleJoin(String query) throws ImpalaException {
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    assertEquals("Expected exactly one JoinNode for query: " + query, 1,
        countJoinNodes(nodes));
    for (PlanNode n : nodes.values()) {
      if (n instanceof JoinNode) return (JoinNode) n;
    }
    return null;
  }

  /**
   * Plans {@code query}, asserts it has exactly one JoinNode, and checks the
   * JoinNode's HBO key string against the expected values for both
   * canonicalization strategies.
   */
  private void verifySingleJoinKey(String query, String expectedExprRewrite,
      String expectedIgnorePartConsts) throws ImpalaException {
    JoinNode join = singleJoin(query);
    assertEquals("Wrong key for query: " + query, expectedExprRewrite,
        join.generateHboKeyString(THboStatsType.CARDINALITY,
            CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals("Wrong key for query: " + query, expectedIgnorePartConsts,
        join.generateHboKeyString(THboStatsType.CARDINALITY,
            CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
  }

  @Test
  public void testInnerJoinGroupKeyIsOrderIndependent() throws ImpalaException {
    // STRAIGHT_JOIN preserves FROM-clause order so the planner builds two different join
    // trees. The HBO key for the top join must be identical between the two queries
    // because the inner-join group flattens both trees into the same set of operands and
    // predicates.
    String filters = "where a.year=2009 and b.year=2009 and c.year=2009 "
        + "and a.int_col=0 and b.int_col=0 and c.int_col=0";
    String q1 = "select STRAIGHT_JOIN count(*) from functional.alltypes c "
        + "join functional.alltypessmall b on c.id = b.id "
        + "join functional.alltypestiny a on b.id = a.id " + filters;
    String q2 = "select STRAIGHT_JOIN count(*) from functional.alltypestiny a "
        + "join functional.alltypessmall b on a.id = b.id "
        + "join functional.alltypes c on b.id = c.id " + filters;

    // Plan IDs: scans 0/1/2, bottom join 3, top join 4.
    final int BOTTOM_JOIN_ID = 3;
    final int TOP_JOIN_ID = 4;

    Map<Integer, PlanNode> q1Nodes = collectPlanNodesInDistributedPlan(q1);
    Map<Integer, PlanNode> q2Nodes = collectPlanNodesInDistributedPlan(q2);

    // Scan keys in ExprRewrite (ER) and IgnorePartitionConstants (IPC) strategies.
    String scanAllER =
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009|int_col = 0";
    String scanSmallER =
        "CARDINALITY:ScanNode:functional.alltypessmall|`year` = 2009|int_col = 0";
    String scanTinyER =
        "CARDINALITY:ScanNode:functional.alltypestiny|`year` = 2009|int_col = 0";
    String scanAllIPC =
        "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>|int_col = 0";
    String scanSmallIPC =
        "CARDINALITY:ScanNode:functional.alltypessmall|`year`=<CONST>|int_col = 0";
    String scanTinyIPC =
        "CARDINALITY:ScanNode:functional.alltypestiny|`year`=<CONST>|int_col = 0";

    // OPERANDS are sorted by scan table name (functional.alltypes <
    // functional.alltypessmall < functional.alltypestiny), and join-predicate columns are
    // qualified with their operand's index in that sorted list ("op<idx>").
    // q1 bottom join (op0:alltypes, op1:alltypessmall)
    // q2 bottom join (op0:alltypestiny, op1:alltypessmall)
    String expectedQ1BottomER = "CARDINALITY:JoinNode:INNER|Operands:["
        + scanAllER + "," + scanSmallER + "]|Predicates:[op0.id=op1.id]";
    String expectedQ2BottomER = "CARDINALITY:JoinNode:INNER|Operands:["
        + scanSmallER + "," + scanTinyER + "]|Predicates:[op0.id=op1.id]";
    // Top join operands sorted: 0 = all, 1 = small, 2 = tiny. Edges all-small and
    // small-tiny render as op0.id=op1.id and op1.id=op2.id (then sorted).
    String expectedTopER = "CARDINALITY:JoinNode:INNER|Operands:["
        + scanAllER + "," + scanSmallER + "," + scanTinyER
        + "]|Predicates:[op0.id=op1.id,op1.id=op2.id]";

    String expectedQ1BottomIPC = "CARDINALITY:JoinNode:INNER|Operands:["
        + scanAllIPC + "," + scanSmallIPC + "]|Predicates:[op0.id=op1.id]";
    String expectedQ2BottomIPC = "CARDINALITY:JoinNode:INNER|Operands:["
        + scanSmallIPC + "," + scanTinyIPC + "]|Predicates:[op0.id=op1.id]";
    String expectedTopIPC = "CARDINALITY:JoinNode:INNER|Operands:["
        + scanAllIPC + "," + scanSmallIPC + "," + scanTinyIPC
        + "]|Predicates:[op0.id=op1.id,op1.id=op2.id]";

    // q1 bottom join is (alltypes, alltypessmall).
    JoinNode q1Bottom = (JoinNode) q1Nodes.get(BOTTOM_JOIN_ID);
    JoinNode q1Top = (JoinNode) q1Nodes.get(TOP_JOIN_ID);
    assertEquals(expectedQ1BottomER, q1Bottom.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedQ1BottomIPC, q1Bottom.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
    assertEquals(expectedTopER, q1Top.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedTopIPC, q1Top.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));

    // q2 bottom join is (alltypestiny, alltypessmall).
    JoinNode q2Bottom = (JoinNode) q2Nodes.get(BOTTOM_JOIN_ID);
    JoinNode q2Top = (JoinNode) q2Nodes.get(TOP_JOIN_ID);
    assertEquals(expectedQ2BottomER, q2Bottom.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedQ2BottomIPC, q2Bottom.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
    assertEquals(expectedTopER, q2Top.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedTopIPC, q2Top.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));

    // Compare keys of two queries.
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertNotEquals(
          "Bottom join keys must differ between FROM-clause orderings for "
              + strategy,
          q1Bottom.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          q2Bottom.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(
          "Top join keys must match between FROM-clause orderings for " + strategy,
          q1Top.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          q2Top.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testJoinOperandAlias() throws ImpalaException {
    // Two joins of the same two tables on the same columns but with the column-to-table
    // assignment (alias) swapped must NOT produce the same key.
    String q1 = "select count(*) from functional.alltypes a "
        + "join functional.alltypestiny b on a.id = b.int_col";
    String q2 = "select count(*) from functional.alltypestiny a "
        + "join functional.alltypes b on a.id = b.int_col";

    JoinNode join1 = singleJoin(q1);
    JoinNode join2 = singleJoin(q2);

    // OPERANDS sorted by table name: alltypes is operand 0 and alltypestiny is operand 1.
    String operands = "CARDINALITY:JoinNode:INNER|Operands:["
        + ALLTYPES_SCAN_KEY + "," + ALLTYPESTINY_SCAN_KEY + "]|";
    // q1: a.id (alltypes.id) = b.int_col (alltypestiny.int_col) -> op0.id=op1.int_col
    String expectedQ1 = operands + "Predicates:[op0.id=op1.int_col]";
    // q2: a.id (alltypestiny.id) = b.int_col (alltypes.int_col) -> op0.int_col=op1.id
    String expectedQ2 = operands + "Predicates:[op0.int_col=op1.id]";

    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(expectedQ1,
          join1.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(expectedQ2,
          join2.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testJoinClauses() throws ImpalaException {
    String[] fromClauses = {
        "functional.alltypes join functional.alltypestiny using (id)",
        "functional.alltypes a join functional.alltypestiny b on a.id = b.id",
        "functional.alltypes a, functional.alltypestiny b where a.id = b.id"
    };
    String expectedKey = "CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|"
        + "Predicates:[op0.id=op1.id]";
    for (String fromClause : fromClauses) {
      verifySingleJoinKey("select count(*) from " + fromClause, expectedKey, expectedKey);
    }
  }

  @Test
  public void testRightHandedJoins() throws ImpalaException {
    // Right-handed joins (RIGHT_ANTI / RIGHT_SEMI / RIGHT_OUTER) must produce
    // the same HBO key as their LEFT-handed counterparts.
    String semiAntiFilters = "where a.year=2009 and a.int_col=0";
    String outerFilters =
        "where a.year=2009 and b.year=2009 and a.int_col=0 and b.int_col=0";
    String leftAntiQuery = "select count(*) from functional_parquet.alltypes a "
        + "left anti join functional.alltypes b on a.id = b.id " + semiAntiFilters;
    String rightAntiQuery = "select count(*) from functional.alltypes b "
        + "right anti join functional_parquet.alltypes a on b.id = a.id "
        + semiAntiFilters;
    String leftSemiQuery = "select count(*) from functional_parquet.alltypes a "
        + "left semi join functional.alltypes b on a.id = b.id " + semiAntiFilters;
    String rightSemiQuery = "select count(*) from functional.alltypes b "
        + "right semi join functional_parquet.alltypes a on b.id = a.id "
        + semiAntiFilters;
    String leftOuterQuery = "select count(*) from functional_parquet.alltypes a "
        + "left outer join functional.alltypes b on a.id = b.id " + outerFilters;
    String rightOuterQuery = "select count(*) from functional.alltypes b "
        + "right outer join functional_parquet.alltypes a on b.id = a.id "
        + outerFilters;

    String parquetScanER = "CARDINALITY:ScanNode:functional_parquet.alltypes|"
        + "`year` = 2009|int_col = 0";
    String parquetScanIPC = "CARDINALITY:ScanNode:functional_parquet.alltypes|"
        + "`year`=<CONST>|int_col = 0";
    String plainScanWithPredsER =
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009|int_col = 0";
    String plainScanWithPredsIPC =
        "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>|int_col = 0";

    // Anti
    String exprRewriteKey = "CARDINALITY:JoinNode:LEFT_ANTI_JOIN|Eq:op0.id=op1.id|"
        + "Left:" + parquetScanER + "|Right:" + ALLTYPES_SCAN_KEY;
    String ignorePartConstsKey = "CARDINALITY:JoinNode:LEFT_ANTI_JOIN|Eq:op0.id=op1.id|"
        + "Left:" + parquetScanIPC + "|Right:" + ALLTYPES_SCAN_KEY;
    verifySingleJoinKey(leftAntiQuery, exprRewriteKey, ignorePartConstsKey);
    verifySingleJoinKey(rightAntiQuery, exprRewriteKey, ignorePartConstsKey);

    // Semi
    exprRewriteKey = "CARDINALITY:JoinNode:LEFT_SEMI_JOIN|Eq:op0.id=op1.id|"
        + "Left:" + parquetScanER + "|Right:" + ALLTYPES_SCAN_KEY;
    ignorePartConstsKey = "CARDINALITY:JoinNode:LEFT_SEMI_JOIN|Eq:op0.id=op1.id|"
        + "Left:" + parquetScanIPC + "|Right:" + ALLTYPES_SCAN_KEY;
    verifySingleJoinKey(leftSemiQuery, exprRewriteKey, ignorePartConstsKey);
    verifySingleJoinKey(rightSemiQuery, exprRewriteKey, ignorePartConstsKey);

    // Outer: outer joins keep their preserved side's WHERE predicates as conjuncts_ on
    // the join. The two scans here both have their predicates pushed down, but b's
    // predicates stay on the join in leftOuter (and equivalently on rightOuter after
    // inversion) and are canonicalized with op1.
    String outerWhereER = "Where:op1.`year` = 2009,op1.int_col = 0|";
    String outerWhereIPC = "Where:op1.`year`=<CONST>,op1.int_col = 0|";
    exprRewriteKey = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|Eq:op0.id=op1.id|"
        + outerWhereER + "Left:" + parquetScanER + "|Right:" + plainScanWithPredsER;
    ignorePartConstsKey = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|Eq:op0.id=op1.id|"
        + outerWhereIPC + "Left:" + parquetScanIPC + "|Right:" + plainScanWithPredsIPC;
    verifySingleJoinKey(leftOuterQuery, exprRewriteKey, ignorePartConstsKey);
    verifySingleJoinKey(rightOuterQuery, exprRewriteKey, ignorePartConstsKey);
  }

  @Test
  public void testCrossJoin() throws ImpalaException {
    // CROSS JOIN is a special case of INNER JOIN that the join condition is always true.
    // So the key prefix is "JoinNode:INNER" and the Predicates list is empty.
    String query = "select count(*) from functional.alltypes a "
        + "cross join functional.alltypestiny b where a.year=2009";
    JoinNode join = singleJoin(query);
    // Keys for ExprRewrite strategy.
    String alltypesScanER =
        "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009";
    String alltypesTinyScanER = "CARDINALITY:ScanNode:functional.alltypestiny";
    // OPERANDS sorted by scan table name: alltypes < alltypestiny.
    String expectedER = "CARDINALITY:JoinNode:INNER|Operands:["
        + alltypesScanER + "," + alltypesTinyScanER + "]|Predicates:[]";
    assertEquals(expectedER, join.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    // Keys for IgnorePartitionConstants strategy.
    String alltypesScanIPC =
        "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>";
    String expectedIPC = "CARDINALITY:JoinNode:INNER|Operands:["
        + alltypesScanIPC + "," + alltypesTinyScanER + "]|Predicates:[]";
    assertEquals(expectedIPC, join.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
  }

  @Test
  public void testFullOuterJoin() throws ImpalaException {
    // Full outer joins sort child keys so that "FROM a FULL OUTER JOIN b" and
    // "FROM b FULL OUTER JOIN a" produce the same HBO key.
    String q1 = "select count(*) from functional.alltypestiny a "
        + "full outer join functional.alltypes b on a.id = b.id";
    String q2 = "select count(*) from functional.alltypes b "
        + "full outer join functional.alltypestiny a on a.id = b.id";
    JoinNode join1 = singleJoin(q1);
    JoinNode join2 = singleJoin(q2);

    String expectedER = "CARDINALITY:JoinNode:FULL_OUTER_JOIN|Eq:op0.id=op1.id|"
        + "Left:" + ALLTYPES_SCAN_KEY + "|Right:" + ALLTYPESTINY_SCAN_KEY;
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(expectedER, join1.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy));
      assertEquals(expectedER, join2.generateHboKeyString(
          THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testOtherJoinConjuncts() throws ImpalaException {
    // Non-equi ON-clause predicates become otherJoinConjuncts_ and appear in
    // the key as a separate Other: section between Eq: and Where:.
    String query = "select count(*) from functional.alltypes a "
        + "left outer join functional.alltypes b "
        + "on a.id = b.id and a.int_col != b.int_col "
        + "where a.year=2009 and b.year=2010";
    JoinNode join = singleJoin(query);

    String leftScanER = "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009";
    String leftScanIPC = "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>";
    String rightScanER = "CARDINALITY:ScanNode:functional.alltypes|`year` = 2010";
    String rightScanIPC = "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>";
    String keyFmt = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|Eq:op0.id=op1.id|"
        + "Other:op0.int_col != op1.int_col|Where:%s|"
        + "Left:%s|Right:%s";

    String expectedER =
        String.format(keyFmt, "op1.`year` = 2010", leftScanER, rightScanER);
    String expectedIPC =
        String.format(keyFmt, "op1.`year`=<CONST>", leftScanIPC, rightScanIPC);
    assertEquals(expectedER, join.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(expectedIPC, join.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
  }

  @Test
  public void testAggOverInnerJoin() throws ImpalaException {
    // An aggregation on top of a join whose grouping columns come from different join
    // operands must qualify each column with its canonical operand index.
    String q1 = "select a.int_col, b.bigint_col, count(*) "
        + "from functional.alltypes a join functional.alltypestiny b on a.id = b.id "
        + "group by a.int_col, b.bigint_col";
    String q2 = "select b.int_col, a.bigint_col, count(*) "
        + "from functional.alltypes a join functional.alltypestiny b on a.id = b.id "
        + "group by b.int_col, a.bigint_col";

    // Plan IDs
    final int PREAGG_ID = 3;
    final int FINAL_AGG_ID = 6;

    // Operands sort by table name: alltypes is operand 0, alltypestiny operand 1.
    // Neither table has partition predicates so both strategies match.
    String childKey = "CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|Predicates:[op0.id=op1.id]";
    String q1Group = "0:Group:op0.int_col,op1.bigint_col";
    String q2Group = "0:Group:op0.bigint_col,op1.int_col";
    String preaggFmt = "CARDINALITY:AggregationNode:FIRST|Preagg:true|GroupingSet:false|"
        + "AggClasses:[%s]|Child:[" + childKey + "]";
    String finalFmt = "CARDINALITY:AggregationNode:FIRST|Preagg:false|GroupingSet:false|"
        + "AggClasses:[%s]|Child:[" + childKey + "]";

    Map<Integer, PlanNode> q1Nodes = collectPlanNodesInDistributedPlan(q1);
    Map<Integer, PlanNode> q2Nodes = collectPlanNodesInDistributedPlan(q2);
    AggregationNode q1Preagg = (AggregationNode) q1Nodes.get(PREAGG_ID);
    AggregationNode q1Final = (AggregationNode) q1Nodes.get(FINAL_AGG_ID);
    AggregationNode q2Preagg = (AggregationNode) q2Nodes.get(PREAGG_ID);
    AggregationNode q2Final = (AggregationNode) q2Nodes.get(FINAL_AGG_ID);

    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(String.format(preaggFmt, q1Group),
          q1Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(String.format(finalFmt, q1Group),
          q1Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(String.format(preaggFmt, q2Group),
          q2Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(String.format(finalFmt, q2Group),
          q2Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy));

      // Swapped column-to-table assignment must hash differently, at every agg phase.
      assertNotEquals("Pre-agg keys must differ for " + strategy,
          q1Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          q2Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertNotEquals("Final agg keys must differ for " + strategy,
          q1Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          q2Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testAggOnMultiLevelJoinOperands() throws ImpalaException {
    // An aggregation above an OUTER join whose preserved side is itself a multi-table
    // join must distinguish columns from the different tables on that side. Both t1
    // (alltypes) and t2 (alltypestiny) sit on the LEFT (preserved) side of the
    // LEFT OUTER JOIN.
    String q1 = "select t1.int_col, t2.bigint_col, count(*) "
        + "from functional.alltypes t1 "
        + "join functional.alltypestiny t2 on t1.id = t2.id "
        + "left join functional.alltypessmall t3 on t1.id = t3.id "
        + "group by t1.int_col, t2.bigint_col";
    String q2 = "select t1.bigint_col, t2.int_col, count(*) "
        + "from functional.alltypes t1 "
        + "join functional.alltypestiny t2 on t1.id = t2.id "
        + "left join functional.alltypessmall t3 on t1.id = t3.id "
        + "group by t1.bigint_col, t2.int_col";

    // Plan IDs
    final int PREAGG_ID = 5;
    final int FINAL_AGG_ID = 10;

    // The LEFT side (op0) is an inner group with operands sorted, so within it
    // alltypes=op0.0 and alltypestiny=op0.1. alltypessmall is the outer join's right
    // operand (op1).
    String innerGroup = "CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|Predicates:[op0.id=op1.id]";
    String childKey = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|Eq:op0.0.id=op1.id|"
        + "Left:" + innerGroup + "|Right:CARDINALITY:ScanNode:functional.alltypessmall";
    String q1Group = "0:Group:op0.0.int_col,op0.1.bigint_col";
    String q2Group = "0:Group:op0.0.bigint_col,op0.1.int_col";
    String preaggFmt = "CARDINALITY:AggregationNode:FIRST|Preagg:true|GroupingSet:false|"
        + "AggClasses:[%s]|Child:[" + childKey + "]";
    String finalFmt = "CARDINALITY:AggregationNode:FIRST|Preagg:false|GroupingSet:false|"
        + "AggClasses:[%s]|Child:[" + childKey + "]";

    Map<Integer, PlanNode> q1Nodes = collectPlanNodesInDistributedPlan(q1);
    Map<Integer, PlanNode> q2Nodes = collectPlanNodesInDistributedPlan(q2);
    AggregationNode q1Preagg = (AggregationNode) q1Nodes.get(PREAGG_ID);
    AggregationNode q1Final = (AggregationNode) q1Nodes.get(FINAL_AGG_ID);
    AggregationNode q2Preagg = (AggregationNode) q2Nodes.get(PREAGG_ID);
    AggregationNode q2Final = (AggregationNode) q2Nodes.get(FINAL_AGG_ID);

    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(String.format(preaggFmt, q1Group),
          q1Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(String.format(finalFmt, q1Group),
          q1Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(String.format(preaggFmt, q2Group),
          q2Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertEquals(String.format(finalFmt, q2Group),
          q2Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy));

      assertNotEquals("Nested-operand column swap must hash differently for " + strategy,
          q1Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          q2Preagg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      assertNotEquals("Nested-operand column swap must hash differently for " + strategy,
          q1Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          q2Final.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testMultiLevelOpIndexUnderLeftJoin() throws ImpalaException {
    // The conjunct "t1.id + t2.id = t3.id" mixes columns from different operands.
    // Each column must be qualified with a unique prefix.
    String query = "select t1.int_col, t2.int_col, t3.int_col "
        + "from functional.alltypes t1 "
        + "join functional.alltypestiny t2 on t1.id = t2.id "
        + "left join functional.alltypessmall t3 on t1.id + t2.id = t3.id";

    // The LEFT side (op0) is an inner group with operands sorted, so within it
    // alltypes=op0.0 and alltypestiny=op0.1. alltypessmall is the outer join's right
    // operand (op1).
    String innerJoinKey = "CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|Predicates:[op0.id=op1.id]";
    String leftJoinKey = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|"
        + "Eq:op0.0.id + op0.1.id=op1.id|"
        + "Left:" + innerJoinKey + "|"
        + "Right:CARDINALITY:ScanNode:functional.alltypessmall";

    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    JoinNode leftJoin = (JoinNode) nodes.get(4);
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(leftJoinKey,
          leftJoin.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testMultiLevelOpIndexUnderInnerJoin() throws ImpalaException {
    // The conjunct "t1.id + t2.id = t3.id" mixes columns from different operands.
    // Each column must be qualified with a unique prefix.
    String query = "select t1.int_col, t2.int_col, t3.int_col "
        + "from functional.alltypestiny t1 "
        + "left join functional.alltypes t2 on t1.id = t2.id "
        + "join functional.alltypessmall t3 on t1.id + t2.id = t3.id";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    String leftJoinKey = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|Eq:op0.id=op1.id|"
        + "Left:CARDINALITY:ScanNode:functional.alltypestiny|"
        + "Right:CARDINALITY:ScanNode:functional.alltypes";
    String innerJoinKey = "CARDINALITY:JoinNode:INNER|Operands:[" + leftJoinKey
        + ",CARDINALITY:ScanNode:functional.alltypessmall]|"
        + "Predicates:[op0.0.id + op0.1.id=op1.id]";
    JoinNode innerJoin = (JoinNode) nodes.get(5);
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(innerJoinKey,
          innerJoin.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testMultiTableInPredicate() throws ImpalaException {
    // Test IN predicate referencing a partitioned table and a non-partitioned table
    // is not canonicalized as a partition equality predicate.
    String query = "select t1.id "
        + "from functional.alltypestiny t1, functional.alltypesnopart t2 "
        + "where t1.id IN (1, 2, t2.id)";
    JoinNode join = singleJoin(query);
    String expectedKey = "CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypesnopart,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|"
        + "Predicates:[op1.id IN (1, 2, op0.id)]";
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(expectedKey,
          join.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testGroupByMultiTableFuncExpr() throws ImpalaException {
    // Test agg that has a materialized slot whose single source is a FunctionCallExpr
    // over two join operands. The columns should be qualified individually, i.e.
    // coalesce(a.id, b.int_col) -> coalesce(op0.id, op1.int_col).
    String[] queries = {
        "select coalesce(a.id, b.int_col), count(*) "
            + "from functional.alltypes a join functional.alltypestiny b on a.id = b.id "
            + "group by coalesce(a.id, b.int_col)",
        // Test an equivalent query using inline view
        "select col, count(*) from ( "
            + "select coalesce(a.id, b.int_col) as col "
            + "from functional.alltypes a join functional.alltypestiny b on a.id = b.id "
            + ") t group by col"
    };
    String expectedFinalAggKey = "CARDINALITY:AggregationNode:FIRST|Preagg:false|"
        + "GroupingSet:false|AggClasses:[0:Group:coalesce(op0.id, op1.int_col)]|"
        + "Child:[CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|"
        + "Predicates:[op0.id=op1.id]]";
    for (String query : queries) {
      Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
      PlanNode finalAgg = nodes.get(6);
      for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
        assertEquals(expectedFinalAggKey,
            finalAgg.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      }
    }
  }

  @Test
  public void testInnerJoinWithLimit() throws ImpalaException {
    String query = "select a.id from functional.alltypes a "
        + "join functional.alltypestiny b on a.id = b.id limit 5";
    String expectedKey = "CARDINALITY:JoinNode:INNER|limit:5|Operands:["
        + ALLTYPES_SCAN_KEY + "," + ALLTYPESTINY_SCAN_KEY
        + "]|Predicates:[op0.id=op1.id]";
    verifySingleJoinKey(query, expectedKey, expectedKey);
  }

  @Test
  public void testLeftJoinWithLimit() throws ImpalaException {
    String query = "select a.id from functional.alltypes a "
        + "left outer join functional.alltypestiny b on a.id = b.id limit 5";
    String expectedKey = "CARDINALITY:JoinNode:LEFT_OUTER_JOIN|limit:5|Eq:op0.id=op1.id|"
        + "Left:" + ALLTYPES_SCAN_KEY + "|Right:" + ALLTYPESTINY_SCAN_KEY;
    verifySingleJoinKey(query, expectedKey, expectedKey);
  }

  @Test
  public void testInnerJoinLimitBreaksGroup() throws ImpalaException {
    String query = "select y.id from functional.alltypessmall x join ("
        + "select a.id from functional.alltypes a "
        + "join functional.alltypestiny b on a.id = b.id limit 5) y on x.id = y.id";

    // Operands are sorted by scan table name. The sub-join's operands (alltypes,
    // alltypestiny) sort before the alltypessmall scan, so the sub-join is op0 and
    // alltypessmall is op1.
    String subJoinKey = "CARDINALITY:JoinNode:INNER|limit:5|Operands:["
        + ALLTYPES_SCAN_KEY + "," + ALLTYPESTINY_SCAN_KEY
        + "]|Predicates:[op0.id=op1.id]";
    String expectedWithLimit = "CARDINALITY:JoinNode:INNER|Operands:["
        + subJoinKey + "," + ALLTYPESSMALL_SCAN_KEY + "]|Predicates:[op0.0.id=op1.id]";

    PlanNode topWithLimit = collectPlanNodesInDistributedPlan(query).get(4);
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals("LIMIT sub-join must stay an opaque operand for " + strategy,
          expectedWithLimit,
          topWithLimit.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  private SortNode singleSort(String query) throws ImpalaException {
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    SortNode sort = null;
    int count = 0;
    for (PlanNode n : nodes.values()) {
      if (n instanceof SortNode) {
        sort = (SortNode) n;
        count++;
      }
    }
    assertNotNull("Expected a sort for query: " + query, sort);
    assertEquals("Expected exactly one sort for query: " + query, 1, count);
    return sort;
  }

  @Test
  public void testSortNodeCardinalityPreservation() throws ImpalaException {
    // A plain ORDER BY (no limit) preserves cardinality and is ignored in the HBO key:
    // its key is exactly its child's key.
    SortNode plainSort = singleSort(
        "select * from functional.alltypes where year = 2009 order by id");
    assertTrue(plainSort.ignoredInHboCardKey());
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(
          plainSort.getChild(0).generateHboKeyString(
              THboStatsType.CARDINALITY, strategy),
          plainSort.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }

    // A TopN (ORDER BY ... LIMIT) caps its child's row count, so it is not ignored in the
    // HBO key and carries its own HBO key that wraps the child.
    SortNode topN = singleSort(
        "select * from functional.alltypes where year = 2009 order by id limit 100");
    assertFalse(topN.ignoredInHboCardKey());
    String topNKeyFmt = "CARDINALITY:SortNode:Type:TOPN|IncludeTies:false|OrderKind:"
        + "LEXICAL|Limit:100|Offset:0|OrderExprs:[id ASC NULLS LAST]|Child:[%s]";
    String scanER = "CARDINALITY:ScanNode:functional.alltypes|`year` = 2009";
    String scanIPC = "CARDINALITY:ScanNode:functional.alltypes|`year`=<CONST>";
    assertEquals(String.format(topNKeyFmt, scanER), topN.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(String.format(topNKeyFmt, scanIPC), topN.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
    // The TopN key is distinct from its child key for every strategy.
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertNotEquals("TopN sort must not be transparent",
          topN.getChild(0).generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          topN.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testTopNOrderingAffectsKey() throws ImpalaException {
    // Two queries differing only in the TopN ordering select different top-100 row sets,
    // so the number of distinct int_col groups - the aggregation's cardinality - differs.
    // The ordering is part of the SortNode key, so the two aggregation keys must differ.
    String base = "select int_col, count(*) from ("
        + "select id, int_col from functional.alltypes where year = 2009 "
        + "order by %s limit 100) t group by int_col";
    // The aggregation node is node 2 in the single-fragment plan for these queries.
    PlanNode aggById = collectPlanNodesInDistributedPlan(
        String.format(base, "id")).get(2);
    PlanNode aggByIntCol = collectPlanNodesInDistributedPlan(
        String.format(base, "int_col")).get(2);

    String aggOverTopNKeyFmt =
        "CARDINALITY:AggregationNode:FIRST|Preagg:false|GroupingSet:false|"
        + "AggClasses:[0:Group:int_col]|Child:[CARDINALITY:SortNode:Type:TOPN|"
        + "IncludeTies:false|OrderKind:LEXICAL|Limit:100|Offset:0|OrderExprs:"
        + "[%s ASC NULLS LAST]|Child:[CARDINALITY:ScanNode:functional.alltypes|%s]]";
    assertEquals(String.format(aggOverTopNKeyFmt, "id", "`year` = 2009"),
        aggById.generateHboKeyString(THboStatsType.CARDINALITY,
            CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(String.format(aggOverTopNKeyFmt, "int_col", "`year` = 2009"),
        aggByIntCol.generateHboKeyString(THboStatsType.CARDINALITY,
            CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(String.format(aggOverTopNKeyFmt, "id", "`year`=<CONST>"),
        aggById.generateHboKeyString(THboStatsType.CARDINALITY,
            CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
    assertEquals(String.format(aggOverTopNKeyFmt, "int_col", "`year`=<CONST>"),
        aggByIntCol.generateHboKeyString(THboStatsType.CARDINALITY,
            CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
  }

  @Test
  public void testPartitionedTopN() throws ImpalaException {
    // A rank()<=K analytic produces a Partitioned TopN. In a distributed plan it is split
    // into a local TopN and a merge TopN. The merge TopN SKIPS the local TopN in its HBO
    // key so its key matches the single-node plan (CHILD is the scan). The local TopN
    // reads the same stat under the same key (its cardinality is a whole-query summed
    // total, on the same footing as HBO), so its key equals the merge TopN's key; only
    // the merge TopN stores.
    String query ="select * from ("
        + "select int_col, id, rank() over ("
        + "  partition by string_col, int_col"
        + "  order by id desc, bigint_col) rnk "
        + "from functional.alltypes WHERE month = 11) t where rnk <= 5";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    // Node ids: scan(0), topN(1,5), analytic(2), select(3), exchange(4)
    SortNode local = (SortNode) nodes.get(1);
    SortNode merge = (SortNode) nodes.get(5);
    assertTrue(merge.isIncludeTies() && local.isIncludeTies());
    assertTrue(local.isSortMergeInput());

    // The merge TopN skips the local TopN: CHILD is the scan directly, so its key is
    // identical to a single-node plan's key.
    String mergeKeyFmt = "CARDINALITY:SortNode:Type:PARTITIONED_TOPN|IncludeTies:true|"
        + "OrderKind:LEXICAL|PerPartLimit:5|PartExprs:[int_col,string_col]|"
        + "OrderExprs:[id DESC NULLS FIRST,bigint_col ASC NULLS LAST]|"
        + "Child:[CARDINALITY:ScanNode:functional.alltypes|%s]";
    String scanER = "`month` = 11";
    String scanIPC = "`month`=<CONST>";

    assertEquals(String.format(mergeKeyFmt, scanER), merge.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    assertEquals(String.format(mergeKeyFmt, scanIPC), merge.generateHboKeyString(
        THboStatsType.CARDINALITY,
        CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS));
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals("Local Partitioned TopN must share the merge TopN's HBO key",
          merge.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          local.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
    assertTopNStorePath(local, merge);
  }

  private void assertTopNStorePath(SortNode local, PlanNode merge) {
    // Only the merge parent stores. The local reads but never stores.
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setStore_hbo_stats(true);
    ThriftSerializationCtx serialCtx = new ThriftSerializationCtx(queryOptions);
    TPlanNode localMsg = new TPlanNode();
    local.toThrift(localMsg, serialCtx);
    assertFalse("Local Top-N must not store HBO stats", localMsg.isSetHbo_hash_keys());
    TPlanNode mergeMsg = new TPlanNode();
    merge.toThrift(mergeMsg, serialCtx);
    assertTrue("Merge of Top-N must store HBO stats", mergeMsg.isSetHbo_hash_keys());
  }

  @Test
  public void testTopNExprOperands() throws ImpalaException {
    // A rank()<K analytic over a join produces a Partitioned TopN whose PARTITION BY and
    // ORDER BY exprs reference columns from two different join operands. Each such column
    // must be qualified with its canonical operand index.
    String query = "select * from ( "
        + "select a.int_col, rank() over("
        + "  partition by a.int_col, b.int_col order by a.id, b.id desc) rnk "
        + "from functional.alltypes a join functional.alltypestiny b on a.id = b.id"
        + ") t where rnk <= 5";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    // In the distributed plan the partitioned TopN is split into a local TopN (node 3)
    // and a merge TopN (node 8).
    SortNode local = (SortNode) nodes.get(3);
    SortNode merge = (SortNode) nodes.get(8);

    // Operands sort by scan table name: alltypes is operand 0, alltypestiny operand 1.
    String childKey = "CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|Predicates:[op0.id=op1.id]";
    // PartExprs is sorted (partition keys are groups): [op0.int_col,op1.int_col].
    // OrderExprs keeps the original sequence: a.id ASC (op0), b.id DESC (op1).
    String expected = "CARDINALITY:SortNode:Type:PARTITIONED_TOPN|IncludeTies:true|"
        + "OrderKind:LEXICAL|PerPartLimit:5|PartExprs:[op0.int_col,op1.int_col]|"
        + "OrderExprs:[op0.id ASC NULLS LAST,op1.id DESC NULLS FIRST]|"
        + "Child:[" + childKey + "]";

    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(expected,
          merge.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      // The local TopN and final TopN shares the same key.
      assertEquals(expected,
          local.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
    assertTopNStorePath(local, merge);
  }

  @Test
  public void testSortByMultiTableFuncExpr() throws ImpalaException {
    // Test a sort expr that is a function call over two join operands. The columns should
    // be qualified individually, i.e.
    // coalesce(a.id, b.int_col) -> coalesce(op0.id, op1.int_col).
    String[] queries = {
        "select * from functional.alltypes a "
            + "join functional.alltypestiny b on a.id = b.id "
            + "order by coalesce(a.id, b.int_col) limit 10",
        // Test an equivalent query using inline view
        "select * from ("
            + "select coalesce(a.id, b.int_col) as col "
            + "from functional.alltypes a "
            + "join functional.alltypestiny b on a.id = b.id) v "
            + "order by col limit 10"
    };
    String expectedTopNKey = "CARDINALITY:SortNode:Type:TOPN|IncludeTies:false|"
        + "OrderKind:LEXICAL|Limit:10|Offset:0|"
        + "OrderExprs:[coalesce(op0.id, op1.int_col) ASC NULLS LAST]|"
        + "Child:[CARDINALITY:JoinNode:INNER|Operands:["
        + "CARDINALITY:ScanNode:functional.alltypes,"
        + "CARDINALITY:ScanNode:functional.alltypestiny]|Predicates:[op0.id=op1.id]]";
    for (String query : queries) {
      Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
      PlanNode topN = nodes.get(3);
      for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
        assertEquals(expectedTopNKey,
            topN.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
      }
    }
  }

  @Test
  public void testDistributedSortStorePath() throws ImpalaException {
    // A plain distributed Top-N splits into a local Top-N and a MERGING-EXCHANGE. Only
    // the merging-exchange stores the HBO stat. The local Top-N reads under the same key
    // but must not store since its cardinality in profile is the summed value across all
    // instances.
    String query = "select id from functional.alltypes where year = 2009 "
        + "order by id limit 100";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    SortNode local = (SortNode) nodes.get(1);
    ExchangeNode mergeExch = (ExchangeNode) nodes.get(2);

    // The local Top-N and the merging-exchange produce the same HBO key.
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals("Local Top-N and merging-exchange must share the HBO key",
          mergeExch.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          local.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
    assertTopNStorePath(local, mergeExch);
  }

  @Test
  public void testDistributedSortOffsetKey() throws ImpalaException {
    String query = "select id from functional.alltypes where year = 2009 "
        + "order by id limit 10 offset 5";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);
    SortNode local = (SortNode) nodes.get(1);
    ExchangeNode mergeExch = (ExchangeNode) nodes.get(2);

    String keyFmt = "CARDINALITY:SortNode:Type:TOPN|IncludeTies:false|OrderKind:LEXICAL|"
        + "Limit:10|Offset:5|OrderExprs:[id ASC NULLS LAST]|"
        + "Child:[CARDINALITY:ScanNode:functional.alltypes|`year` = 2009]";
    // The local TopN's key uses the exchange's original limit(10)/offset(5), not its own
    // mutated limit(15)/offset(0).
    assertEquals(keyFmt, local.generateHboKeyString(
        THboStatsType.CARDINALITY, CanonicalizationStrategy.EXPR_REWRITE));
    // The merging-exchange delegates its key to the local TopN, so the keys are equal.
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals(
          local.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          mergeExch.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
  }

  @Test
  public void testDistributedTopNWithTiesStorePath() throws ImpalaException {
    String query = "select * from ("
        + "select id, rank() over (order by id) rnk "
        + "from functional.alltypes where year = 2009) t where rnk < 100";
    Map<Integer, PlanNode> nodes = collectPlanNodesInDistributedPlan(query);

    // Node ids: scan(0), topN(1,5), analytic(2), select(3), exchange(4)
    SortNode merge = (SortNode) nodes.get(5);
    SortNode local = (SortNode) nodes.get(1);
    assertTrue(merge.isIncludeTies() && local.isIncludeTies());
    assertTrue(local.isSortMergeInput());

    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      assertEquals("Local and merge Top-N must share the HBO key",
          merge.generateHboKeyString(THboStatsType.CARDINALITY, strategy),
          local.generateHboKeyString(THboStatsType.CARDINALITY, strategy));
    }
    assertTopNStorePath(local, merge);
  }
}
