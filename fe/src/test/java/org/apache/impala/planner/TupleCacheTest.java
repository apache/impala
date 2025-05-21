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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.TupleCacheInfo.HashTraceElement;
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

    // Sanity check for complex types
    verifyIdenticalCacheKeys(
        "select nested_struct from functional_parquet.complextypestbl",
        "select nested_struct from functional_parquet.complextypestbl");
  }

  @Test
  public void testRuntimeFilterCacheKeys() {
    for (boolean isDistributedPlan : Arrays.asList(false, true)) {
      String basicJoinTmpl = "select straight_join probe.id from functional.alltypes " +
          "probe, functional.alltypestiny build where %s";
      verifyIdenticalCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          String.format(basicJoinTmpl, "probe.id = build.id"), isDistributedPlan);
      // Trivial rewrite produces same plan
      verifyIdenticalCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          String.format(basicJoinTmpl, "build.id = probe.id"), isDistributedPlan);
      // Larger join with same subquery. Cache keys match for the subquery, but don't
      // match for the outer query.
      verifyOverlappingCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          "select straight_join p.id from functional.alltypes p, (" +
          String.format(basicJoinTmpl, "probe.id = build.id") + ") b where p.id = b.id",
          isDistributedPlan);
      // Different filter slot
      verifyOverlappingCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          String.format(basicJoinTmpl, "probe.id + 1 = build.id"), isDistributedPlan);
      // Different target expression
      verifyOverlappingCacheKeys(
          String.format(basicJoinTmpl, "probe.id + 1 = build.id"),
          String.format(basicJoinTmpl, "probe.id + 2 = build.id"), isDistributedPlan);
      // Larger join with similar subquery and simpler plan tree.
      verifyOverlappingCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          "select straight_join a.id from functional.alltypes a, " +
          "functional.alltypes b, functional.alltypestiny c where a.id = b.id " +
          "and b.id = c.id", isDistributedPlan);
      // Different build-side source table
      verifyDifferentCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          "select straight_join a.id from functional.alltypes a, functional.alltypes b " +
          "where a.id = b.id", isDistributedPlan);
      // Different build-side predicates
      verifyDifferentCacheKeys(
          String.format(basicJoinTmpl, "probe.id = build.id"),
          String.format(basicJoinTmpl, "probe.id = build.id and build.id < 100"),
          isDistributedPlan);
    }
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
    verifyNIdenticalCacheKeys(rightJoinAgg, rightJoinAgg, 2,
        /* isDistributedPlan */ true);
    // For single node plan, there are no exchanges and everything is eligible.
    verifyAllEligible(rightJoinAgg, /* isDistributedPlan */ false);
    // Both scans are cached, but aggregates above hash join and exchange are not.
    String innerJoinAgg = "select count(*) from functional.alltypes t1 inner join " +
        "functional.alltypestiny t2 on t1.smallint_col = t2.smallint_col group by " +
        "t1.tinyint_col, t2.smallint_col having count(t2.int_col) = count(t1.bigint_col)";
    verifyNIdenticalCacheKeys(innerJoinAgg, innerJoinAgg, 2,
        /* isDistributedPlan */ true);
    // For single node plan, there are no exchanges and everything is eligible.
    verifyAllEligible(innerJoinAgg, /* isDistributedPlan */ false);
    // The scans and union are eligible. The aggregate is not because in a single
    // node plan, the limit attaches to the aggregate. More sophisticated logic could
    // detect that the limit is not binding as count(*) returns only a single row.
    String unionAgg = "select count(*) from (select * from functional.alltypes " +
        "union all select * from functional.alltypessmall) t limit 10";
    verifyNIdenticalCacheKeys(unionAgg, unionAgg, 3, /* isDistributedPlan */ false);
    // With a distributed plan, the limit attaches to the aggregate finalize, so the
    // initial aggregate is eligible in addition to the union and two scans.
    verifyNIdenticalCacheKeys(unionAgg, unionAgg, 4, /* isDistributedPlan */ true);
    // Only scan is cached, as aggregates are above an exchange and TOP-N.
    String groupConcatGroupAgg = "select day, group_concat(distinct string_col) " +
        "from (select * from functional.alltypesagg where id % 100 = day order by id " +
        "limit 99999) a group by day";
    verifyNIdenticalCacheKeys(groupConcatGroupAgg, groupConcatGroupAgg, 1);

    // Only scan is cached, variable aggregate disable caching above that.
    for (String aggFn : Arrays.asList("appx_median", "histogram", "sample")) {
      String variableAggQuery =
          String.format("select %s(tinyint_col) from functional.alltypesagg", aggFn);
      verifyNIdenticalCacheKeys(variableAggQuery, variableAggQuery, 1);
    }
    String groupConcatOnlyScan =
      "select group_concat(string_col) from functional.alltypes";
    verifyNIdenticalCacheKeys(groupConcatOnlyScan, groupConcatOnlyScan, 1);
  }

  @Test
  public void testUnions() {
    String select_alltypes_id = "select id from functional.alltypes";
    String select_alltypessmall_id = "select id from functional.alltypessmall";
    verifyAllEligible(select_alltypes_id + " union all " + select_alltypessmall_id,
        /*isDistributedPlan*/ false);
    verifyAllEligible("select count(*) from (select * from functional.alltypes " +
        "union all select * from functional.alltypessmall) t",
        /*isDistributedPlan*/ false);
    // A union distinct has an aggregate above the union to deduplicate the results.
    // The whole thing is supported now.
    verifyAllEligible(select_alltypes_id + " union distinct " + select_alltypessmall_id,
        /*isDistributedPlan*/ false);
    // The order of the union matters to the key above the union.
    verifyOverlappingCacheKeys(
        select_alltypes_id + " union all " + select_alltypessmall_id,
        select_alltypessmall_id + " union all " + select_alltypes_id);
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
    // A limit higher in the plan doesn't impact cache eligibility for the scan nodes,
    // but it does make the join node ineligible.
    verifyOverlappingCacheKeys(
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

    // This has a union on the build side. This adds an extra filter on the probe side and
    // changes the slot ids, but that does not change the cache keys for the build-side.
    String join_union_build = "select straight_join u.id from functional.alltypes t, " +
      "(select id from functional.alltypessmall union all " +
      "select id from functional.alltypestiny) u where t.id = u.id";
    verifyOverlappingCacheKeys(join_union_build, join_union_build + " and t.int_col = 1");
  }

  @Test
  public void testIceberg() {
    // Sanity test that we can plan a basic scenario querying from an Iceberg table
    // that has different types of deletes
    verifyIdenticalCacheKeys(
        "select * from functional_parquet.iceberg_v2_delete_both_eq_and_pos",
        "select * from functional_parquet.iceberg_v2_delete_both_eq_and_pos");
  }

  @Test
  public void testJoins() {
    // A plan with a hash join is eligible
    verifyAllEligible("select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id and " +
        "probe.int_col <= build.int_col", /* isDistributedPlan*/ false);

    // A plan with a nested loop join is eligible
    verifyAllEligible("select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id < build.id",
        /* isDistributedPlan*/ false);

    // An iceberg plan with a delete join
    verifyAllEligible(
        "select * from functional_parquet.iceberg_v2_positional_delete_all_rows",
        /* isDistributedPlan*/ false);

    // Different predicates on the join node makes the cache keys different, but it
    // doesn't impact the scan nodes.
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id and " +
        "probe.int_col <= build.int_col",
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id = build.id and " +
        "probe.int_col + 1 <= build.int_col");

    // Different predicates on the nested loop join node makes the cache keys different
    verifyOverlappingCacheKeys(
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id < build.id",
        "select straight_join probe.id from functional.alltypes probe, " +
        "functional.alltypes build where probe.id + 1 < build.id");

    // Using broadcast/shuffle hints to compare the same query with a broadcast
    // join vs a partitioned join. They share the build-side scan location.
    String simple_join_with_hint_template =
        "select straight_join probe.id from functional.alltypes probe join " +
        "%s functional.alltypes build on (probe.id = build.id)";
    verifyOverlappingCacheKeys(
        String.format(simple_join_with_hint_template, "/* +broadcast */"),
        String.format(simple_join_with_hint_template, "/* +shuffle */"),
        /* isDistributedPlan */ true);

    // With a broadcast join, we can cache past the join.
    verifyJoinNodesEligible(
        String.format(simple_join_with_hint_template, "/* +broadcast */"), 1,
        /* isDistributedPlan */ true);

    // With a partitioned join, verify that we don't cache past the join.
    verifyJoinNodesEligible(
        String.format(simple_join_with_hint_template, "/* +shuffle */"), 0,
        /* isDistributedPlan */ true);

    // Verify that we can cache past a directed exchange
    verifyJoinNodesEligible(
        "select * from functional_parquet.iceberg_v2_positional_delete_all_rows", 1,
        /* isDistributedPlan */ true);

    // When incorporating the scan range information from the build side of the
    // join, we need to also incorporate information about the partitions involved.
    // scale_db.num_partitions_1234_blocks_per_partition_1 is an exotic table where
    // all the partitions point to the same file. If we don't incorporate partition
    // information, then it can't tell apart queries against different partitions.
    String incorporatePartitionSqlTemplate =
        "select straight_join build.j, probe.id from functional.alltypes probe, " +
        "scale_db.num_partitions_1234_blocks_per_partition_1 build " +
        "where probe.id = build.i and build.j = %s";
    verifyOverlappingCacheKeys(String.format(incorporatePartitionSqlTemplate, 1),
        String.format(incorporatePartitionSqlTemplate, 2));
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

  @Test
  public void testSkipCorrectnessChecking() {
    // Locations after a streaming aggregation before the finalize can have variability
    // that correctness checking can't handle.
    List<PlanNode> cacheEligibleNodes =
      getCacheEligibleNodes(
          "select int_col, count(*) from functional.alltypes group by int_col",
          /* isDistributedPlan */ true);
    // In this plan, there is a streaming aggregation that feeds into a partitioned
    // exchange. The finalize phase is past that partitioned exchange. That means that
    // the only eligible node marked with streaming agg variability is the initial
    // streaming AggregationNode.
    for (PlanNode node : cacheEligibleNodes) {
      if (node instanceof AggregationNode) {
        assertTrue(node.getTupleCacheInfo().getStreamingAggVariability());
      }
    }
  }

  protected List<PlanNode> getCacheEligibleNodes(String query,
      boolean isDistributedPlan) {
    List<PlanFragment> plan = getPlan(query, isDistributedPlan);
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

  // Simplified signature for single node plan
  protected List<PlanNode> getCacheEligibleNodes(String query) {
    return getCacheEligibleNodes(query, /* isDistributedPlan */ false);
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
      StringBuilder builder = new StringBuilder();
      for (HashTraceElement elem : node.getTupleCacheInfo().getHashTraces()) {
        // Use only the hash trace pieces as the comments are intended for human
        // consumption and can be different
        builder.append(elem.getHashTrace());
      }
      cacheHashTraces.add(builder.toString());
    }
    return cacheHashTraces;
  }

  private void printQueryNodesCacheInfo(String query,
      List<PlanNode> nodes, StringBuilder log) {
    log.append("Query: ");
    log.append(query);
    log.append("\n");
    for (PlanNode node : nodes) {
      log.append(node.getDisplayLabel());
      log.append("\n");
      log.append(node.getTupleCacheInfo().toString());
    }
  }

  protected void verifyCacheIneligible(String query, boolean isDistributedPlan) {
    List<PlanNode> cacheEligibleNodes = getCacheEligibleNodes(query, isDistributedPlan);

    // No eligible locations
    if (cacheEligibleNodes.size() != 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected no cache eligible nodes. Instead found:\n");
      printQueryNodesCacheInfo(query, cacheEligibleNodes, errorLog);
      fail(errorLog.toString());
    }
  }

  // Simplified signature for single node plan
  protected void verifyCacheIneligible(String query) {
    verifyCacheIneligible(query, /* isDistributedPlan */ false);
  }

  protected void verifyIdenticalCacheKeys(String query1, String query2,
      boolean isDistributedPlan) {
    verifyNIdenticalCacheKeys(query1, query2, 1, isDistributedPlan);
  }

  // Simplified signature for single node plan
  protected void verifyIdenticalCacheKeys(String query1, String query2) {
    verifyIdenticalCacheKeys(query1, query2, /* isDistributedPlan */ false);
  }

  protected void verifyNIdenticalCacheKeys(String query1, String query2, int n,
     boolean isDistributedPlan) {
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1, isDistributedPlan);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2, isDistributedPlan);
    assertTrue(cacheEligibleNodes1.size() >= n);
    List<String> cacheKeys1 = getCacheKeys(cacheEligibleNodes1);
    List<String> cacheKeys2 = getCacheKeys(cacheEligibleNodes2);
    List<String> cacheHashTraces1 = getCacheHashTraces(cacheEligibleNodes1);
    List<String> cacheHashTraces2 = getCacheHashTraces(cacheEligibleNodes2);
    if (!cacheKeys1.equals(cacheKeys2) || !cacheHashTraces1.equals(cacheHashTraces2)) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected identical cache keys. Instead found:\n");
      printQueryNodesCacheInfo(query1, cacheEligibleNodes1, errorLog);
      printQueryNodesCacheInfo(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }
  }

  // Simplified signature for single node plan
  protected void verifyNIdenticalCacheKeys(String query1, String query2, int n) {
    verifyNIdenticalCacheKeys(query1, query2, n, /* isDistributedPlan */ false);
  }

  protected void verifyOverlappingCacheKeys(String query1, String query2,
      boolean isDistributedPlan) {
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1, isDistributedPlan);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2, isDistributedPlan);

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

    // Since the hash trace is defined as a single node's contribution,
    // it can have additional matches higher in the plan that don't correspond
    // to the same cache key. The number of single-node hash trace matches
    // should be equal or greater than the number of cache key matches.
    assertTrue(keyIntersection.size() <= hashTraceIntersection.size());

    if (keyIntersection.size() == 0 || hashTraceIntersection.size() == 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected overlapping cache keys. Instead found:\n");
      printQueryNodesCacheInfo(query1, cacheEligibleNodes1, errorLog);
      printQueryNodesCacheInfo(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }

    if (cacheKeys1.equals(cacheKeys2) || cacheHashTraces1.equals(cacheHashTraces2)) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected some cache keys to differ. Instead found:\n");
      printQueryNodesCacheInfo(query1, cacheEligibleNodes1, errorLog);
      printQueryNodesCacheInfo(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }
  }

  // Simplified signature for single node plan
  protected void verifyOverlappingCacheKeys(String query1, String query2) {
    verifyOverlappingCacheKeys(query1, query2, /* isDistributedPlan */ false);
  }

  protected void verifyDifferentCacheKeys(String query1, String query2,
      boolean isDistributedPlan) {
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1, isDistributedPlan);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2, isDistributedPlan);

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

    // Since the hash trace is defined as a single node's contribution,
    // it can have additional matches higher in the plan that don't correspond
    // to the same cache key. The number of single-node hash trace matches
    // should be equal or greater than the number of cache key matches.
    assertTrue(keyIntersection.size() <= hashTraceIntersection.size());

    if (keyIntersection.size() != 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected different cache keys. Instead found:\n");
      printQueryNodesCacheInfo(query1, cacheEligibleNodes1, errorLog);
      printQueryNodesCacheInfo(query2, cacheEligibleNodes2, errorLog);
      fail(errorLog.toString());
    }
  }

  // Simplified signature for single node plan
  protected void verifyDifferentCacheKeys(String query1, String query2) {
    verifyDifferentCacheKeys(query1, query2, /* isDistributedPlan */ false);
  }

  protected void verifyAllEligible(String query, boolean isDistributedPlan) {
    List<PlanFragment> plan = getPlan(query, isDistributedPlan);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    // Walk over the plan and produce a list of cache keys.
    List<PlanNode> preOrderPlanNodes = planRoot.getNodesPreOrder();
    List<PlanNode> ineligibleNodes = new ArrayList<PlanNode>();
    for (PlanNode node : preOrderPlanNodes) {
      if (node instanceof TupleCacheNode) continue;
      TupleCacheInfo info = node.getTupleCacheInfo();
      if (!info.isEligible()) {
        ineligibleNodes.add(node);
      }
    }
    if (ineligibleNodes.size() != 0) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append("Expected all nodes eligible, instead found ineligible nodes:\n");
      printQueryNodesCacheInfo(query, ineligibleNodes, errorLog);
      fail(errorLog.toString());
    }
  }

  protected void verifyJoinNodesEligible(String query, int expectedEligibleJoins,
      boolean isDistributedPlan) {
    List<PlanFragment> plan = getPlan(query, isDistributedPlan);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    // Walk over the plan and produce a list of cache keys.
    List<PlanNode> preOrderPlanNodes = planRoot.getNodesPreOrder();
    List<PlanNode> ineligibleJoinNodes = new ArrayList<PlanNode>();
    int eligibleJoins = 0;
    for (PlanNode node : preOrderPlanNodes) {
      if (node instanceof JoinNode) {
        TupleCacheInfo info = node.getTupleCacheInfo();
        if (!info.isEligible()) {
          ineligibleJoinNodes.add(node);
        } else {
          eligibleJoins++;
        }
      }
    }
    if (eligibleJoins != expectedEligibleJoins) {
      StringBuilder errorLog = new StringBuilder();
      errorLog.append(
          String.format("Expected %d join nodes eligible, instead found %d.\n",
              eligibleJoins, expectedEligibleJoins));
      errorLog.append("Ineligible join nodes:");
      printQueryNodesCacheInfo(query, ineligibleJoinNodes, errorLog);
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
  private List<PlanFragment> getPlan(String query, boolean isDistributedPlan) {
    // Create a query context with rewrites disabled
    // TODO: Should probably turn them on, or run a test
    // both with and without rewrites.
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);

    // Force the plan to run on a single node so it
    // resides in a single fragment.
    TQueryOptions queryOptions = queryCtx.client_request.getQuery_options();
    queryOptions.setNum_nodes(isDistributedPlan ? 0 : 1);
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

  /**
   * Simplified signature of getPlan() to get the single node plan
   */
  private List<PlanFragment> getPlan(String query) {
    return getPlan(query, false);
  }
}
