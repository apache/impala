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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test the planner's tuple cache node keys and eligibility
 *
 * This uses test infrastructure adapted from CardinalityTest to run a query and produce
 * the query plan. It then checks for TupleCacheNodes and examines their keys. This
 * relies on TupleCachePlanner placing TupleCacheNodes at all eligible locations.
 */
public class TupleCacheTest extends PlannerTestBase {

  /**
   * Test some basic cases for tuple cache keys
   */
  @Test
  public void testBasicCacheKeys() {
    verifyIdenticalCacheKeys("select id from functional.alltypes",
        "select id from functional.alltypes");
    // Different table
    verifyDifferentCacheKeys("select id from functional.alltypes",
        "select id from functional.alltypestiny");
    verifyDifferentCacheKeys("select id from functional.alltypes",
        "select id from functional_parquet.alltypes");
    // Different column
    verifyDifferentCacheKeys("select id from functional.alltypes",
        "select int_col from functional.alltypes");
    // Extra filter
    verifyDifferentCacheKeys("select id from functional.alltypes",
        "select id from functional.alltypes where id = 1");
    // Different filter
    verifyDifferentCacheKeys("select id from functional.alltypes where id = 1",
        "select id from functional.alltypes where id = 2");
  }

  @Test
  public void testRuntimeFilterCacheKeys() {
    String basicJoinTmpl = "select straight_join probe.id from functional.alltypes " +
        "probe, functional.alltypestiny build where %s";
    verifyIdenticalCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        String.format(basicJoinTmpl, "probe.id = build.id"));
    // Trivial rewrite produces same plan
    verifyIdenticalCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        String.format(basicJoinTmpl, "build.id = probe.id"));
    // Larger join with same subquery. Cache keys match because cache is disabled when
    // the build side is too complex.
    verifyIdenticalCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        "select straight_join p.id from functional.alltypes p, (" +
        String.format(basicJoinTmpl, "probe.id = build.id") + ") b where p.id = b.id");
    // Different filter slot
    verifyOverlappingCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        String.format(basicJoinTmpl, "probe.id + 1 = build.id"));
    // Different target expression
    verifyOverlappingCacheKeys(
        String.format(basicJoinTmpl, "probe.id + 1 = build.id"),
        String.format(basicJoinTmpl, "probe.id + 2 = build.id"));
    // Larger join with similar subquery and simpler plan tree.
    verifyOverlappingCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        "select straight_join a.id from functional.alltypes a, functional.alltypes b, " +
        "functional.alltypestiny c where a.id = b.id and b.id = c.id");
    // Different build-side source table
    verifyDifferentCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        "select straight_join a.id from functional.alltypes a, functional.alltypes b " +
        "where a.id = b.id");
    // Different build-side predicates
    verifyDifferentCacheKeys(
        String.format(basicJoinTmpl, "probe.id = build.id"),
        String.format(basicJoinTmpl, "probe.id = build.id and build.id < 100"));
  }

  @Test
  public void testAggregateCacheKeys() {
    // Scan and aggregate above scan are cached, aggregate above exchange is not.
    String basicAgg = "select count(*), count(tinyint_col), min(tinyint_col), " +
        "max(tinyint_col), sum(tinyint_col), avg(tinyint_col) " +
        "from functional.alltypesagg";
    verifyNIdenticalCacheKeys(basicAgg, basicAgg, 2);
    // Scan and aggregate above scan are cached, aggregate above exchange is not.
    String groupingAgg = "select tinyint_col, bigint_col, count(*), min(tinyint_col), " +
        "max(tinyint_col), sum(tinyint_col), avg(tinyint_col) " +
        "from functional.alltypesagg group by 2, 1";
    verifyNIdenticalCacheKeys(groupingAgg, groupingAgg, 2);
    // Scan and aggregate above scan are cached, later aggregates are not.
    String distinctAgg = "select avg(l_quantity), ndv(l_discount), " +
        "count(distinct l_partkey) from tpch_parquet.lineitem";
    verifyNIdenticalCacheKeys(distinctAgg, distinctAgg, 2);
    // Scan and aggregate above scan are cached, later aggregates are not.
    String groupDistinctAgg = "select group_concat(distinct string_col) " +
        "from functional.alltypesagg";
    verifyNIdenticalCacheKeys(groupDistinctAgg, groupDistinctAgg, 2);
    // Scan and the only aggregate are cached.
    String havingAgg = "select 1 from functional.alltypestiny having count(*) > 0";
    verifyNIdenticalCacheKeys(havingAgg, havingAgg, 2);
    // Scan and aggregate above scan are cached. All later aggregates are above the
    // exchange and thus not cached.
    String twoPhaseAgg = "select bigint_col bc, count(smallint_col) c1, " +
        "count(distinct int_col) c2 from functional.alltypessmall " +
        "group by bigint_col order by bc";
    verifyNIdenticalCacheKeys(twoPhaseAgg, twoPhaseAgg, 2);
    // Build scan and aggregate above scan are cached. Probe scan caching is invalid
    // because runtime filter contains agg with exchange. Other aggregates are above
    // an exchange or hash join and thus not cached.
    String rightJoinAgg = "with v1 as (select c_nationkey, c_custkey, count(*) " +
        "from tpch.customer group by c_nationkey, c_custkey), " +
        "v2 as (select c_nationkey, c_custkey, count(*) from v1, tpch.orders " +
        "where c_custkey = o_custkey group by c_nationkey, c_custkey) " +
        "select c_nationkey, count(*) from v2 group by c_nationkey";
    verifyNIdenticalCacheKeys(rightJoinAgg, rightJoinAgg, 2);
    // Both scans are cached, but aggregates above hash join and exchange are not.
    String innerJoinAgg = "select count(*) from functional.alltypes t1 inner join " +
        "functional.alltypestiny t2 on t1.smallint_col = t2.smallint_col group by " +
        "t1.tinyint_col, t2.smallint_col having count(t2.int_col) = count(t1.bigint_col)";
    verifyNIdenticalCacheKeys(innerJoinAgg, innerJoinAgg, 2);
    // Both scans are cached, but aggregate is not because it's above a union. Limit only
    // applies to aggregate above exchange, which is obviously not cached.
    String unionAgg = "select count(*) from (select * from functional.alltypes " +
        "union all select * from functional.alltypessmall) t limit 10";
    verifyNIdenticalCacheKeys(unionAgg, unionAgg, 2);
    // Only scan is cached, as aggregates are above an exchange and TOP-N.
    String groupConcatGroupAgg = "select day, group_concat(distinct string_col) " +
        "from (select * from functional.alltypesagg where id % 100 = day order by id " +
        "limit 99999) a group by day";
    verifyNIdenticalCacheKeys(groupConcatGroupAgg, groupConcatGroupAgg, 1);
    // Only scan is cached, appx_median disables caching on aggregate.
    String appxMedianAgg = "select appx_median(tinyint_col) from functional.alltypesagg";
    verifyNIdenticalCacheKeys(appxMedianAgg, appxMedianAgg, 1);
  }

  /**
   * Test cases that rely on masking out unnecessary data to have cache hits.
   */
  @Test
  public void testCacheKeyMasking() {
    // The table alias is masked out and doesn't result in a different cache key
    verifyIdenticalCacheKeys("select id from functional.alltypes",
        "select id from functional.alltypes a");
    // The column alias doesn't impact the cache key
    verifyIdenticalCacheKeys("select id from functional.alltypes",
        "select id as a from functional.alltypes");
  }

  /**
   * Test cases where a statement should be cache ineligible
   */
  @Test
  public void testIneligibility() {
    // Tuple caching not implemented for Kudu or HBase
    verifyCacheIneligible("select id from functional_kudu.alltypes");
    verifyCacheIneligible("select id from functional_hbase.alltypes");

    // Runtime filter produced by Kudu table is not implemented
    verifyCacheIneligible("select a.id from functional.alltypes a, " +
        "functional_kudu.alltypes b where a.id = b.id");

    // Verify that a limit makes a statement ineligible
    verifyCacheIneligible("select id from functional.alltypes limit 10");
    // A limit higher in the plan doesn't impact cache eligibility for the scan nodes.
    verifyIdenticalCacheKeys(
        "select a.id from functional.alltypes a, functional.alltypes b",
        "select a.id from functional.alltypes a, functional.alltypes b limit 10");

    // Random functions should make a location ineligible
    // rand()/random()/uuid()
    verifyCacheIneligible(
        "select id from functional.alltypes where id < 7300 * rand()");
    verifyCacheIneligible(
        "select id from functional.alltypes where id < 7300 * random()");
    verifyCacheIneligible(
        "select id from functional.alltypes where string_col != uuid()");

    // NOTE: Expression rewrites can replace the constant functions with constants,
    // so the following tests only work for enable_expr_rewrites=false (which is the
    // default for getPlan()). If they are replaced with a constant, then the constant
    // is hashed into the cache key. This can lead to cache misses, but it does not
    // lead to incorrect results.

    // now()/current_date()/current_timestamp()/unix_timestamp()/utc_timestamp()/etc.
    verifyCacheIneligible(
        "select timestamp_col from functional.alltypes where timestamp_col < now()");
    verifyCacheIneligible(
        "select date_col from functional.date_tbl where date_col < current_date()");

    // Mixed-case equivalent to check that it is not case sensitive
    verifyCacheIneligible(
        "select date_col from functional.date_tbl where date_col < CURRENT_date()");

    // System / session information can change over time (e.g. different sessions/users
    // can be connected to different coordinators, etc).
    verifyCacheIneligible(
        "select string_col from functional.alltypes where string_col = current_user()");
    verifyCacheIneligible(
        "select string_col from functional.alltypes where string_col = coordinator()");

    // AI functions are nondeterministic
    verifyCacheIneligible(
       "select string_col from functional.alltypes " +
       "where string_col = ai_generate_text_default(string_col)");

    // UDFs are considered nondeterministic
    addTestFunction("TestUdf", Lists.newArrayList(Type.INT), false);
    verifyCacheIneligible(
       "select string_col from functional.alltypes where TestUdf(int_col) = int_col");

    // Mixing eligible conjuncts with ineligible conjuncts doesn't change the eligibility
    verifyCacheIneligible(
       "select * from functional.alltypes where id < 5 and string_col = current_user()");
  }

  /**
   * Test cases to verify that pieces of different queries can match
   * An important piece of this is translation of tuple/slot IDs so that other parts of
   * the plan tree don't influence the cache key.
   */
  @Test
  public void testCacheKeyGenerality() {
    // Build side is the same. Probe side has a different table
    // (alltypes vs alltypestiny).
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id",
        "select straight_join probe.id from functional.alltypestiny probe, " +
        "functional.alltypes build where probe.id = build.id");

    // Build side is the same. Probe side has an extra int_col = 100 filter.
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id",
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id and probe.int_col=100");

    // A limit on the probe side doesn't impact eligibility on the build side.
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from " +
        "(select id from functional.alltypes limit 10) probe, " +
        "functional.alltypes build where probe.id = build.id",
        "select straight_join probe.id from " +
        "(select id from functional.alltypes) probe, " +
        "functional.alltypes build where probe.id = build.id");

    // Build side is the same. Probe side has an extra int_col = 100 filter.
    // Note that this test requires id translation, because the probe side's reference
    // to int_col takes up a slot id and shifts the slot ids of the build side.
    // Id translation fixes the build side's int_col slot id so that the probe side
    // doesn't influence it.
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id and build.int_col = 100",
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id and probe.int_col = 100 " +
        "and build.int_col = 100");

    // This has one build side that is the same, but one query has an extra join feeding
    // the probe side. The extra join takes up an extra tuple id, shifting the tuple id
    // for the overlapping build. This test requires translation of the tuple ids to
    // work. It also relies on masking out the node ids, because the node id will also be
    // different.
    verifyOverlappingCacheKeys(
        "select straight_join probe1.id from functional.alltypes probe1, " +
        "functional.alltypes probe2, functional.alltypes build " +
        "where probe1.id = probe2.id and probe2.id = build.id",
        "select straight_join probe1.id from functional.alltypes probe1, " +
        "functional.alltypes build where probe1.id = build.id");

    // This query has a tuple cache location for the build-side scan that feeds into the
    // join on the build side. If we add a bunch of additional filters on the probe side
    // table, that changes the slot ids, but we should still get identical keys for the
    // build-side caching location.
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from functional.alltypes probe, (select " +
        "build1.id from functional.alltypes build1) build where probe.id = build.id",
        "select straight_join probe.id from functional.alltypes probe, (select " +
        "build1.id from functional.alltypes build1) build where probe.id = build.id " +
        "and probe.bool_col = true and probe.int_col = 100");
  }

  @Test
  public void testDeterministicScheduling() {
    // Verify that the HdfsScanNode that feeds into a TupleCacheNode uses deterministic
    // scan range scheduling. When there are more ways for locations to be cache
    // ineligible, this test will be expanded to cover the case where scan nodes don't
    // use deterministic scheduling.
    List<PlanNode> cacheEligibleNodes =
        getCacheEligibleNodes("select id from functional.alltypes where int_col = 500");
    for (PlanNode node : cacheEligibleNodes) {
      // The HdfsScanNode for this query will have determinstic scan range assignment set
      // This test uses mt_dop=0, so the value wouldn't matter for execution, but it
      // still verifies that it is set properly.
      if (node instanceof HdfsScanNode) {
        HdfsScanNode hdfsScanNode = (HdfsScanNode) node;
        assertTrue(hdfsScanNode.usesDeterministicScanRangeAssignment());
      }
    }
  }

  protected List<PlanNode> getCacheEligibleNodes(String query) {
    List<PlanFragment> plan = getPlan(query);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    // Walk over the plan and produce a list of cache keys.
    List<PlanNode> preOrderPlanNodes = planRoot.getNodesPreOrder();
    List<PlanNode> cacheEligibleNodes = new ArrayList<PlanNode>();
    for (PlanNode node : preOrderPlanNodes) {
      if (node instanceof TupleCacheNode) continue;
      TupleCacheInfo info = node.getTupleCacheInfo();
      if (info.isEligible()) {
        cacheEligibleNodes.add(node);
      }
    }

    return cacheEligibleNodes;
  }

  private List<String> getCacheKeys(List<PlanNode> cacheEligibleNodes) {
    List<String> cacheKeys = new ArrayList<String>();
    for (PlanNode node : cacheEligibleNodes) {
      cacheKeys.add(node.getTupleCacheInfo().getHashString());
    }
    return cacheKeys;
  }

  private List<String> getCacheHashTraces(List<PlanNode> cacheEligibleNodes) {
    List<String> cacheHashTraces = new ArrayList<String>();
    for (PlanNode node : cacheEligibleNodes) {
      cacheHashTraces.add(node.getTupleCacheInfo().getHashTrace());
    }
    return cacheHashTraces;
  }

  private void printCacheEligibleNode(PlanNode node, StringBuilder log) {
    log.append(node.getDisplayLabel());
    log.append("\n");
    log.append("cache key: ");
    log.append(node.getTupleCacheInfo().getHashString());
    log.append("\n");
    log.append("cache key hash trace: ");
    log.append(node.getTupleCacheInfo().getHashTrace());
    log.append("\n");
  }

  private void printQueryCacheEligibleNodes(String query,
      List<PlanNode> cacheEligibleNodes, StringBuilder log) {
    log.append("Query: ");
    log.append(query);
    log.append("\n");
    for (PlanNode node : cacheEligibleNodes) {
      printCacheEligibleNode(node, log);
    }
  }

  protected void verifyCacheIneligible(String query) {
    List<PlanNode> cacheEligibleNodes = getCacheEligibleNodes(query);

    // No eligible locations
    if (cacheEligibleNodes.size() != 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected no cache eligible nodes. Instead found:\n");
      printQueryCacheEligibleNodes(query, cacheEligibleNodes, errorLog);
      fail(errorLog.toString());
    }
  }

  protected void verifyIdenticalCacheKeys(String query1, String query2) {
    verifyNIdenticalCacheKeys(query1, query2, 1);
  }

  protected void verifyNIdenticalCacheKeys(String query1, String query2, int n) {
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2);
    assertTrue(cacheEligibleNodes1.size() >= n);
    List<String> cacheKeys1 = getCacheKeys(cacheEligibleNodes1);
    List<String> cacheKeys2 = getCacheKeys(cacheEligibleNodes2);
    List<String> cacheHashTraces1 = getCacheHashTraces(cacheEligibleNodes1);
    List<String> cacheHashTraces2 = getCacheHashTraces(cacheEligibleNodes2);
    if (!cacheKeys1.equals(cacheKeys2) || !cacheHashTraces1.equals(cacheHashTraces2)) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected identical cache keys. Instead found:\n");
      printQueryCacheEligibleNodes(query1, cacheEligibleNodes1, errorLog);
      printQueryCacheEligibleNodes(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }
  }

  protected void verifyOverlappingCacheKeys(String query1, String query2) {
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2);

    // None of this makes any sense unless they both have eligible nodes
    assertTrue(cacheEligibleNodes1.size() > 0);
    assertTrue(cacheEligibleNodes2.size() > 0);

    Set<String> cacheKeys1 = new HashSet(getCacheKeys(cacheEligibleNodes1));
    Set<String> cacheKeys2 = new HashSet(getCacheKeys(cacheEligibleNodes2));
    Set<String> keyIntersection = new HashSet(cacheKeys1);
    keyIntersection.retainAll(cacheKeys2);

    Set<String> cacheHashTraces1 = new HashSet(getCacheHashTraces(cacheEligibleNodes1));
    Set<String> cacheHashTraces2 = new HashSet(getCacheHashTraces(cacheEligibleNodes2));
    Set<String> hashTraceIntersection = new HashSet(cacheHashTraces1);
    hashTraceIntersection.retainAll(cacheHashTraces2);

    // The hash trace for a cache key should be a one-to-one thing, so
    // any difference in keys should be a difference in hash traces.
    assertEquals(keyIntersection.size(), hashTraceIntersection.size());

    if (keyIntersection.size() == 0 || hashTraceIntersection.size() == 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected overlapping cache keys. Instead found:\n");
      printQueryCacheEligibleNodes(query1, cacheEligibleNodes1, errorLog);
      printQueryCacheEligibleNodes(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }

    if (cacheKeys1.equals(cacheKeys2) || cacheHashTraces1.equals(cacheHashTraces2)) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected some cache keys to differ. Instead found:\n");
      printQueryCacheEligibleNodes(query1, cacheEligibleNodes1, errorLog);
      printQueryCacheEligibleNodes(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }
  }

  protected void verifyDifferentCacheKeys(String query1, String query2) {
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2);

    // None of this makes any sense unless they both have eligible nodes
    assertTrue(cacheEligibleNodes1.size() > 0);
    assertTrue(cacheEligibleNodes2.size() > 0);
    Set<String> cacheKeys1 = new HashSet(getCacheKeys(cacheEligibleNodes1));
    Set<String> cacheKeys2 = new HashSet(getCacheKeys(cacheEligibleNodes2));
    Set<String> keyIntersection = new HashSet(cacheKeys1);
    keyIntersection.retainAll(cacheKeys2);

    Set<String> cacheHashTraces1 = new HashSet(getCacheHashTraces(cacheEligibleNodes1));
    Set<String> cacheHashTraces2 = new HashSet(getCacheHashTraces(cacheEligibleNodes2));
    Set<String> hashTraceIntersection = new HashSet(cacheHashTraces1);
    hashTraceIntersection.retainAll(cacheHashTraces2);

    // The hash trace for a cache key should be a one-to-one thing, so
    // any difference in keys should be a difference in hash traces.
    assertEquals(keyIntersection.size(), hashTraceIntersection.size());

    if (keyIntersection.size() != 0 || hashTraceIntersection.size() != 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected different cache keys. Instead found:\n");
      printQueryCacheEligibleNodes(query1, cacheEligibleNodes1, errorLog);
      printQueryCacheEligibleNodes(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }
  }

  /**
   * Given a query, run the planner and extract the physical plan prior
   * to conversion to Thrift. Extract the first (or, more typically, only
   * fragment) and return it for inspection. This currently runs with num_nodes=1
   * and tuple caching enabled. This was adapted from CardinalityTest.
   *
   * @param query the query to run
   * @return the first (or only) fragment plan node
   */
  private List<PlanFragment> getPlan(String query) {
    // Create a query context with rewrites disabled
    // TODO: Should probably turn them on, or run a test
    // both with and without rewrites.
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);

    // Force the plan to run on a single node so it
    // resides in a single fragment.
    TQueryOptions queryOptions = queryCtx.client_request.getQuery_options();
    queryOptions.setNum_nodes(1);
    // Turn on tuple caching
    queryOptions.setEnable_tuple_cache(true);

    // Plan the query, discard the actual execution plan, and
    // return the plan tree.
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.requestPlanCapture();
    try {
      frontend_.createExecRequest(planCtx);
    } catch (ImpalaException e) {
      fail(e.getMessage());
    }
    return planCtx.getPlan();
  }
}
