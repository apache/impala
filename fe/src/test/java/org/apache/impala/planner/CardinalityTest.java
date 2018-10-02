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
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

/**
 * Test the planner's inference of tuple cardinality from metadata NDV and
 * resulting selectivity.
 */
public class CardinalityTest extends PlannerTestBase {

  /**
   * Test the happy path: table with stats, no all-null cols.
   */
  @Test
  public void testBasicsWithStats() {

    // Return all rows. Cardinality is row count;
    verifyCardinality("SELECT id FROM functional.alltypes", 7300);

    // Return all rows. Cardinality is row count,
    // should not be influenced by limited NDV of selected
    // column.
    verifyCardinality("SELECT bool_col FROM functional.alltypes", 7300);

    // Result cardinality reduced by limited NDV.
    // Boolean column has cardinality 3 (true, false, null).
    // Since we have metadata, and know the column is non-null,
    // NDV is 2. We select one of them.
    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE bool_col = TRUE", 7300/2);

    // Result cardinality reduced by NDV.
    // NDV should be 10 (from metadata).
    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE int_col = 1", 7300/10);

    // Assume classic 0.1 selectivity for other operators
    // IMPALA-7560 says this should be revised.
    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE int_col != 1", 730);

    // IMPALA-7601 says the following should be revised.
    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE int_col > 1", 730);
    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE int_col > 1", 730);

    // Not using NDV of int_col, instead uses default 0.1 value.
    // Since NULL is one of the NDV's, nullvalue() is true
    // 1/NDV of the time, on average.
    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE nullvalue(int_col)", 730);

    // IMPALA-7695: IS [NOT] NULL predicates should factor in null count.
    verifyCardinality(
        "select id from functional.alltypesagg where tinyint_col IS NULL", 2000);
    verifyCardinality(
        "select id from functional.alltypesagg where tinyint_col IS NOT NULL", 9000);

    // Grouping should reduce cardinality
    verifyCardinality(
        "SELECT COUNT(*) FROM functional.alltypes GROUP BY int_col", 10);
    verifyCardinality(
        "SELECT COUNT(*) FROM functional.alltypes GROUP BY id", 7300);
    verifyCardinality(
        "SELECT COUNT(*) FROM functional.alltypes GROUP BY bool_col", 2);
  }

  /**
   * Joins should multiply out cardinalities.
   */
  @Test
  public void testJoins() {
    // Cartesian product
    String joinClause = " FROM functional.alltypes t1, functional.alltypes t2 ";
    verifyCardinality("SELECT t1.id" + joinClause, 7300 * 7300);
    // Cartesian product, reduced by NDV of group key
    verifyCardinality(
        "SELECT COUNT(*)" + joinClause + "GROUP BY t1.id", 7300);
    verifyCardinality(
        "SELECT COUNT(*)" + joinClause + "GROUP BY t1.id, t1.int_col", 7300 * 10);
  }

  @Test
  public void testBasicsWithoutStats() {
    // IMPALA-7608: no cardinality is available (result is -1)
    verifyCardinality("SELECT a FROM functional.tinytable", -1);
  }

  /**
   * Given a query and an expected cardinality, checks that the root
   * node of the single-fragment plan has the expected cardinality.
   * Verifies that cardinality is propagated upward through the plan
   * as expected.
   *
   * @param query query to test
   * @param expected expected cardinality at the root node
   */
  protected void verifyCardinality(String query, long expected) {
    List<PlanFragment> plan = getPlan(query);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    assertEquals("Cardinality error for: " + query,
        expected, planRoot.getCardinality());
  }

  /**
   * Given a query, run the planner and extract the physical plan prior
   * to conversion to Thrift. Extract the first (or, more typically, only
   * fragment) and return it for inspection.
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
