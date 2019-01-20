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
   * Test tables with all-null columns. Test need for IMPALA-7310, NDV of an
   * all-null column should be 1.
   */
  @Test
  public void testNulls() {
    verifyCardinality("SELECT null_int FROM functional.nullrows", 26);
    // a has unique values, so NDV = 26, card = 26/26 = 1
    verifyCardinality("SELECT null_int FROM functional.nullrows WHERE id = 'x'", 1);
    // f repeats for 5 rows, so NDV=7, 26/7 =~ 4
    verifyCardinality("SELECT null_int FROM functional.nullrows WHERE group_str = 'x'",
        4);
    // Revised use of nulls per IMPALA-7310
    // null_str is all nulls, NDV = 1, selectivity = 1/1, cardinality = 26
    // BUG: At present selectivity is assumed to be 0.1
    //verifyCardinality(
    //      "SELECT null_int FROM functional.nullrows WHERE null_str = 'x'", 26);
    verifyCardinality("SELECT null_int FROM functional.nullrows WHERE null_str = 'x'",
        3);
  }

  @Test
  public void testGroupBy() {
    String baseStmt = "SELECT COUNT(*) " +
                      "FROM functional.nullrows " +
                      "GROUP BY ";
    // NDV(a) = 26
    verifyCardinality(baseStmt + "id", 26);
    // f has NDV=3
    verifyCardinality(baseStmt + "group_str", 6);
    // b has NDV=1 (plus 1 for nulls)
    // Bug: Nulls not counted in NDV
    //verifyCardinality(baseStmt + "blank", 2);
    verifyCardinality(baseStmt + "blank", 1);
    // c is all nulls
    // Bug: Nulls not counted in NDV
    //verifyCardinality(baseStmt + "null_str", 1);
    verifyCardinality(baseStmt + "null_str", 0);
    // NDV(a) * ndv(c) = 26 * 1 = 26
    // Bug: Nulls not counted in NDV
    //verifyCardinality(baseStmt + "id, null_str", 26);
    verifyCardinality(baseStmt + "id, null_str", 0);
    // NDV(a) * ndv(f) = 26 * 3 = 78, capped at row count = 26
    verifyCardinality(baseStmt + "id, group_str", 26);
  }

  /**
   * Compute join cardinality using a table without stats. We estimate row count.
   * Combine with an all-nulls column.
   */
  @Test
  public void testNullColumnJoinCardinality() throws ImpalaException {
    // IMPALA-7565: Make sure there is no division by zero during cardinality calculation
    // in a many to many join on null columns (ndv = 0).
    String query = "select * from functional.nulltable t1 "
        + "inner join [shuffle] functional.nulltable t2 on t1.d = t2.d";
    checkCardinality(query, 1, 1);
  }

  /**
   * Compute join cardinality using a table with stats.
   * Focus on an all-nulls column.
   */
  @Test
  public void testJoinWithStats() {
    // NDV multiplied out on group by
    verifyCardinality(
        "SELECT null_int FROM functional.alltypes, functional.nullrows", 7300 * 26);
    // With that as the basis, add a GROUP BY
    String baseStmt = "SELECT COUNT(*) " +
                      "FROM functional.alltypes, functional.nullrows " +
                      "GROUP BY ";
    // Unique values, one group per row
    verifyCardinality(baseStmt + "alltypes.id", 7300);
    // NDV(id) = 26
    verifyCardinality(baseStmt + "nullrows.id", 26);
    // blank has NDV=1, but adjust for nulls
    // Bug: Nulls not counted in NDV
    //verifyCardinality(baseStmt + "blank", 2);
    verifyCardinality(baseStmt + "blank", 1);
    // group_str has NDV=6
    verifyCardinality(baseStmt + "group_str", 6);
    // null_str is all nulls
    // Bug: Nulls not counted in NDV
    //verifyCardinality(baseStmt + "null_str", 1);
    verifyCardinality(baseStmt + "null_str", 0);
    // NDV(id) = 26 * ndv(null_str) = 1
    // Bug: Nulls not counted in NDV
    // Here and for similar bugs: see IMPALA-7310 and IMPALA-8094
    //verifyCardinality(baseStmt + "id, null_str", 26);
    verifyCardinality(baseStmt + "nullrows.id, null_str", 0);
    // NDV(id) = 26 * ndv(group_str) = 156
    // Planner does not know that id determines group_str
    verifyCardinality(baseStmt + "nullrows.id, group_str", 156);
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
