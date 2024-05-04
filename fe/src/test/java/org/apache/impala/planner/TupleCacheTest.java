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

import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

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

    // TODO: Random functions should make a location ineligible
    // rand()/random()/uuid()
    // verifyCacheIneligible(
    //     "select id from functional.alltypes where id < 7300 * rand()");
    // verifyCacheIneligible(
    //     "select id from functional.alltypes where id < 7300 * random()");
    // verifyCacheIneligible(
    //     "select id from functional.alltypes where string_col != uuid()");

    // TODO: Time functions are replaced by constant folding
    // now()/current_date()/current_timestamp()/unix_timestamp()/utc_timestamp()/etc.
    // verifyCacheIneligible(
    //     "select timestamp_col from functional.alltypes where timestamp_col < now()");
    // verifyCacheIneligible(
    //     "select date_col from functional.date_tbl where date_col < current_date()");
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
    List<PlanNode> cacheEligibleNodes1 = getCacheEligibleNodes(query1);
    List<PlanNode> cacheEligibleNodes2 = getCacheEligibleNodes(query2);
    assertTrue(cacheEligibleNodes1.size() > 0);
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
