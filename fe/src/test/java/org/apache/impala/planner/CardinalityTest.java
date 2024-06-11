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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.QueryConstants;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Test the planner's inference of tuple cardinality from metadata NDV and
 * resulting selectivity.
 */
public class CardinalityTest extends PlannerTestBase {

  private static double CARDINALITY_TOLERANCE = 0.05;

  @BeforeClass
  public static void setUpClass() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUpClass() {
    RuntimeEnv.INSTANCE.reset();
  }

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

    verifyCardinality(
        "SELECT id FROM functional.alltypes WHERE int_col != 1", 6570);

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

    verifyCardinality(
        "select id from functional.alltypesaggnonulls where int_col IS NULL", 1);
    verifyCardinality(
        "select id from functional.alltypesaggnonulls where int_col IS NOT NULL",
        10000);

    // Grouping should reduce cardinality
    verifyCardinality(
        "SELECT COUNT(*) FROM functional.alltypes GROUP BY int_col", 10);
    verifyCardinality(
        "SELECT COUNT(*) FROM functional.alltypes GROUP BY id", 7300);
    verifyCardinality(
        "SELECT COUNT(*) FROM functional.alltypes GROUP BY bool_col", 2);

    // Regression test for IMPALA-11301.
    verifyCardinality(
        "SELECT * FROM tpcds_parquet.date_dim WHERE d_current_day != 'a'", 36525);
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
    // null_str is all nulls, NDV = 1, selectivity =0, cardinality = 0
    // PlanNode#applySelectivity, If cardinality is 0, set cardinality to 1
    // IMPALA-8647: don't round cardinality down to zero for safety.
    verifyCardinality(
          "SELECT null_int FROM functional.nullrows WHERE null_str = 'x'", 1);
  }

  @Test
  public void testGroupBy() {
    String baseStmt = "SELECT COUNT(*) " +
                      "FROM functional.nullrows " +
                      "GROUP BY ";
    // NDV(id) = 26
    verifyCardinality(baseStmt + "id", 26);
    // group_str has NDV=3
    verifyCardinality(baseStmt + "group_str", 6);
    // blank has NDV=1 (and no nulls)
    verifyCardinality(baseStmt + "blank", 1);
    // null_str is all nulls
    verifyCardinality(baseStmt + "null_str", 1);
    // NDV(id) * ndv(null_str) = 26 * 1 = 26
    verifyCardinality(baseStmt + "id, null_str", 26);
    // NDV(id) * ndv(group_str) = 26 * 3 = 78, capped at row count = 26
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
    verifyCardinality(baseStmt + "null_str", 1);
    // NDV(id) = 26 * ndv(null_str) = 1 ; 26 * 1 = 26
    verifyCardinality(baseStmt + "nullrows.id, null_str", 26);
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
    verifyApproxCardinality("SELECT a FROM functional.tinytable", 2);
  }

  @Test
  public void testBasicsWithoutStatsWithHDFSNumRowsEstDisabled() {
    verifyCardinality("SELECT a FROM functional.tinytable", -1, false,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE));
  }

  /**
   *  functional.alltypesmixedformat is a table of 4 partitions, each having a different
   *  format. These formats are text, sequence file, rc file, and parquet.
   *  True cardinality of functional.alltypesmixedformat is 1200.
   *  Estimated cardinality of functional.alltypesmixedformat is 2536.
   */
  @Test
  public void testTableOfMixedTypesWithoutStats() {
    verifyApproxCardinality("SELECT * FROM functional.alltypesmixedformat", 2536);
  }

  @Test
  public void testTableOfMixedTypesWithoutStatsWithHDFSNumRowsEstDisabled() {
    verifyCardinality("SELECT * FROM functional.alltypesmixedformat", -1, false,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE));
  }


  /**
   * tpch_text_gzip.lineitem is a table of 1 partition but having 6 files.
   * True cardinality of tpch_text_gzip.lineitem is 6,001,215.
   * Estimated cardinality of tpch_text_gzip.lineitem is 5,141,177.
   */
  @Test
  public void testTableOfMultipleFilesWithoutStats() {
    verifyApproxCardinality("SELECT * FROM tpch_text_gzip.lineitem", 5_141_177);
  }

  @Test
  public void testTableOfMultipleFilesWithoutStatsWithHDFSNumRowsEstDisabled() {
    verifyCardinality("SELECT * FROM tpch_text_gzip.lineitem", -1, false,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE));
  }

  @Test
  public void testAggregationNodeCount() {
    // Create the paths to the AggregationNode's of interest
    // in a distributed plan not involving GROUP BY.
    // Since there are two resulting AggregationNode's, we create two paths.
    List<Integer> pathToFirstAggregationNode = Arrays.asList();
    List<Integer> pathToSecondAggregationNode = Arrays.asList(0, 0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    // Estimated cardinality of the resulting AggregateNode's not involving
    // GROUP BY is 1 for both AggregationNodes's no matter whether or not
    // there is available statistics for the underlying hdfs table.
    verifyApproxCardinality("SELECT COUNT(a) FROM functional.tinytable", 1, true,
        ImmutableSet.of(), pathToFirstAggregationNode, AggregationNode.class);
    verifyApproxCardinality("SELECT COUNT(a) FROM functional.tinytable", 1, true,
        ImmutableSet.of(), pathToSecondAggregationNode, AggregationNode.class);

  }

  @Test
  public void testAggregationNodeCountWithHDFSNumRowsEstDisabled() {
    // Create the paths to the AggregationNode's of interest
    // in a distributed plan not involving GROUP BY.
    // Since there are two resulting AggregationNode's, we create two paths.
    List<Integer> pathToFirstAggregationNode = Arrays.asList();
    List<Integer> pathToSecondAggregationNode = Arrays.asList(0, 0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    // Estimated cardinality of the resulting AggregateNode's not involving
    // GROUP BY is 1 for both AggregationNodes's no matter whether or not
    // there is available statistics for the underlying hdfs table.
    verifyApproxCardinality("SELECT COUNT(a) FROM functional.tinytable", 1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyApproxCardinality("SELECT COUNT(a) FROM functional.tinytable", 1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        pathToSecondAggregationNode, AggregationNode.class);

  }

  @Test
  public void testAggregationNodeGroupBy() {
    // Create the paths to the AggregationNode's of interest
    // in a distributed plan involving GROUP BY.
    // Since there are two resulting AggregationNode's, we create two paths.
    List<Integer> pathToFirstAggregationNode = Arrays.asList(0);
    List<Integer> pathToSecondAggregationNode = Arrays.asList(0, 0, 0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    // Estimated cardinality of the resulting AggregationNode's involving
    // GROUP BY is 2 for both AggregationNode's.
    verifyCardinality("SELECT COUNT(a) FROM functional.tinytable "
        + "GROUP BY a", 2, true, ImmutableSet.of(),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyCardinality("SELECT COUNT(a) FROM functional.tinytable "
        + "GROUP BY a", 2, true, ImmutableSet.of(),
        pathToSecondAggregationNode, AggregationNode.class);

    // Test that having predicate reduces cardinality on merge aggregation
    // but not to zero. Preaggregation should not be affected.
    verifyCardinality("SELECT COUNT(a) FROM functional.tinytable "
        + "GROUP BY a HAVING COUNT(a) > 0", 1, true, ImmutableSet.of(),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyCardinality("SELECT COUNT(a) FROM functional.tinytable "
        + "GROUP BY a HAVING COUNT(a) > 0", 2, true, ImmutableSet.of(),
        pathToSecondAggregationNode, AggregationNode.class);
  }

  @Test
  public void testAggregationNodeGroupByWithHDFSNumRowsEstDisabled() {
    // Create the paths to the AggregationNode's of interest
    // in a distributed plan involving GROUP BY..
    // Since there are two resulting AggregationNode's, we create two paths.
    List<Integer> pathToFirstAggregationNode = Arrays.asList(0);
    List<Integer> pathToSecondAggregationNode = Arrays.asList(0, 0, 0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of the resulting AggregationNode's involving
    // GROUP BY is -1 for both AggregationNode's.
    verifyApproxCardinality("SELECT COUNT(a) FROM functional.tinytable "
        + "GROUP BY a", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyApproxCardinality("SELECT COUNT(a) FROM functional.tinytable "
        + "GROUP BY a", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        pathToSecondAggregationNode, AggregationNode.class);
  }

  /**
   * Test that agg nodes correctly gap output cardinality based on input
   * cardinality with and without stats.
   */
  @Test
  public void testAggregationNodeGroupByCardinalityCapping() {
    // Create the paths to the AggregationNode's of interest
    // in a distributed plan involving GROUP BY.
    // Since there are two resulting AggregationNode's, we create two paths.
    List<Integer> pathToFirstAggregationNode = Arrays.asList(0);
    List<Integer> pathToSecondAggregationNode = Arrays.asList(0, 0, 0);

    // With table and column stats available.
    verifyCardinality("select distinct id, int_col from functional.alltypes",
        7300, true, ImmutableSet.of(),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyCardinality("select distinct id, int_col from functional.alltypes",
        7300, true, ImmutableSet.of(),
        pathToSecondAggregationNode, AggregationNode.class);

    // No column stats available but number of rows inferred from file size.
    // The estimated number of rows caps the output cardinality.
    verifyApproxCardinality(
        "select distinct id, int_col from functional_parquet.alltypes",
        12400, true, ImmutableSet.of(),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyApproxCardinality(
        "select distinct id, int_col from functional_parquet.alltypes",
        12400, true, ImmutableSet.of(),
        pathToSecondAggregationNode, AggregationNode.class);
    // No column stats available and row estimation disabled - no estimate is possible.
    verifyCardinality(
        "select distinct id, int_col from functional_parquet.alltypes",
        -1, true, ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyCardinality(
        "select distinct id, int_col from functional_parquet.alltypes",
        -1, true, ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        pathToSecondAggregationNode, AggregationNode.class);
  }

  @Test
  public void testAnalyticEvalNode() {
    // Since the root node of the generated distributed plan is
    // the AnalyticEvalNode of interest, we do not have to create a
    // path to it explicitly.
    List<Integer> path = Arrays.asList();

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // Estimated cardinality of functional_parquet.alltypestiny is 742.
    verifyApproxCardinality("SELECT SUM(int_col) OVER() int_col "
        + "FROM functional_parquet.alltypestiny", 742, true,
        ImmutableSet.of(), path, AnalyticEvalNode.class);

    // Regression test for IMPALA-11301. row_number() is (incorrectly) assumed to have
    // NDV=1, which was leading to selectivity=0.0 in rn != 5. Will break if someone
    // implements correct ndv estimates for analytic functions.
    verifyCardinality("SELECT * FROM (SELECT *, row_number() OVER(order by id) "
        + "as rn FROM functional.alltypestiny) v where rn != 5", 4);
  }

  @Test
  public void testAnalyticEvalNodeWithHDFSNumRowsEstDisabled() {
    // Since the root node of the generated distributed plan is
    // the AnalyticEvalNode of interest, we do not have to create a
    // path to it explicitly.
    List<Integer> path = Arrays.asList();

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    verifyApproxCardinality("SELECT SUM(int_col) OVER() int_col "
        + "FROM functional_parquet.alltypestiny", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, AnalyticEvalNode.class);
  }

  @Test
  public void testCardinalityCheckNode() {
    // Create the path to the CardinalityCheckNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 0);

    String subQuery = "(SELECT id "
        + "FROM functional_parquet.alltypestiny b "
        + "WHERE id = 1)";

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // Estimated cardinality of functional_parquet.alltypestiny is 523.
    verifyApproxCardinality("SELECT bigint_col "
        + "FROM functional_parquet.alltypestiny a "
        + "WHERE id = " + subQuery, 1, true,
        ImmutableSet.of(), path, CardinalityCheckNode.class);
  }

  @Test
  public void testCardinalityCheckNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the CardinalityCheckNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 0);

    String subQuery = "(SELECT id "
        + "FROM functional_parquet.alltypestiny b "
        + "WHERE id = 1)";

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    verifyApproxCardinality("SELECT bigint_col "
        + "FROM functional_parquet.alltypestiny a "
        + "WHERE id = " + subQuery, 1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, CardinalityCheckNode.class);
  }

  @Test
  public void testEmptySetNode() {
    // Since the root node of the generated distributed plan is
    // the EmptySetNode of interest, we do not have to create a
    // path to it explicitly.
    List<Integer> path = Arrays.asList();

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // Estimated cardinality of functional_parquet.alltypestiny is 523.
    String subQuery = "(SELECT * "
        + "FROM functional_parquet.alltypestiny "
        + "LIMIT 0)";
    verifyApproxCardinality("SELECT 1 "
        + "FROM functional_parquet.alltypessmall "
        + "WHERE EXISTS " + subQuery, 0, true,
        ImmutableSet.of(), path, EmptySetNode.class);
  }

  @Test
  public void testEmptySetNodeWithHDFSNumRowsEstDisabled() {
    // Since the root node of the generated distributed plan is
    // the EmptySetNode of interest, we do not have to create a
    // path to it explicitly.
    List<Integer> path = Arrays.asList();

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    String subQuery = "(SELECT * "
        + "FROM functional_parquet.alltypestiny "
        + "LIMIT 0)";
    verifyApproxCardinality("SELECT 1 "
        + "FROM functional_parquet.alltypessmall "
        + "WHERE EXISTS " + subQuery, 0, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, EmptySetNode.class);
  }

  @Test
  public void testExchangeNode() {
    // Since the root node of the generated distributed plan is
    // the ExchangeNode of interest, we do not have to create a
    // path to it explicitly.
    List<Integer> path = Arrays.asList();

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    verifyApproxCardinality("SELECT a FROM functional.tinytable", 2, true,
        ImmutableSet.of(), path, ExchangeNode.class);
  }

  @Test
  public void testExchangeNodeWithHDFSNumRowsEstDisabled() {
    // Since the root node of the generated distributed plan is
    // the ExchangeNode of interest, we do not have to create a
    // path to it explicitly.
    List<Integer> path = Arrays.asList();

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    verifyApproxCardinality("SELECT a FROM functional.tinytable", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, ExchangeNode.class);
  }

  @Test
  public void testHashJoinNode() {
    // Create the path to the HashJoinNode of interest
    // in the distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    verifyApproxCardinality("SELECT * "
        + "FROM functional.tinytable x INNER JOIN "
        + "functional.tinytable y ON x.a = y.a", 2, true,
        ImmutableSet.of(), path, HashJoinNode.class);
  }

  @Test
  public void testHashJoinNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the HashJoinNode of interest
    // in the distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    verifyApproxCardinality("SELECT * "
        + "FROM functional.tinytable x INNER JOIN "
        + "functional.tinytable y ON x.a = y.a", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, HashJoinNode.class);
  }

  @Test
  public void testNestedLoopJoinNode() {
    // Create the path to the NestedLoopJoinNode of interest
    // in the distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // Estimated cardinality of functional_parquet.alltypestiny is 742.
    // Estimated cardinality of the NestedLoopJoinNode is 550,564 = 742 * 742.
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypestiny a, "
        + "functional_parquet.alltypestiny b", 550_564, true,
        ImmutableSet.of(), path, NestedLoopJoinNode.class);
  }

  @Test
  public void testNestedLoopJoinNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the NestedLoopJoinNode of interest
    // in the distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypestiny a, "
        + "functional_parquet.alltypestiny b", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, NestedLoopJoinNode.class);
  }

  @Test
  public void testHdfsScanNode() {
    // Create the path to the HdfsScanNode of interest
    // in the distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    verifyApproxCardinality("SELECT a FROM functional.tinytable", 2, true,
        ImmutableSet.of(), path, HdfsScanNode.class);
  }

  @Test
  public void testHdfsScanNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the HdfsScanNode of interest
    // in the distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    verifyApproxCardinality("SELECT a FROM functional.tinytable", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, HdfsScanNode.class);
  }

  /**
   * Test that cardinality estimates for partition key scans reflect the
   * number of scan ranges, not the number of rows in the files.
   */
  @Test
  public void testHdfsScanNodePartitionKeyScan() {
    // Non-distinct, partition key scan optimisation is disabled.
    verifyCardinality("SELECT year FROM functional.alltypes", 7300, true,
        ImmutableSet.of(), Arrays.asList(0), HdfsScanNode.class);

    // Distinct, partition key scan optimisation is enabled.
    verifyCardinality("SELECT distinct year FROM functional.alltypes", 24, true,
        ImmutableSet.of(), Arrays.asList(0, 0, 0, 0), HdfsScanNode.class);

    // Distinct, partition key scan optimisation is enabled but no stats. We should still
    // estimate based on the number of files.
    verifyCardinality("SELECT distinct year FROM functional_parquet.alltypes", 24,
        true, ImmutableSet.of(), Arrays.asList(0, 0, 0, 0), HdfsScanNode.class);
    verifyCardinality("SELECT distinct year FROM functional_parquet.alltypes", 24,
        true, ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        Arrays.asList(0, 0, 0, 0), HdfsScanNode.class);
  }

  @Test
  public void testSelectNode() {
    // Create the path to the SelectNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 0);

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // Estimated cardinality of functional_parquet.alltypestiny is 523.
    String singleRowSubQuery = "(SELECT int_col "
        + "FROM functional_parquet.alltypestiny "
        + "LIMIT 1)";
    // There are no available statistics in functional_parquet.alltypessmall.
    // True cardinality of functional_parquet.alltypessmall is 100.
    // Estimated cardinality of functional_parquet.alltypessmall is 649.
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypessmall "
        + "WHERE 1 IN " + singleRowSubQuery, 1, true,
        ImmutableSet.of(), path, SelectNode.class);


    String manyRowSubQuery = "(SELECT int_col "
        + "FROM functional_parquet.alltypes "
        + "LIMIT 1000)";
    // There are no available statistics in functional_parquet.alltypes.
    // True cardinality of functional_parquet.alltypes is 7300.
    // Estimated cardinality of functional_parquet.alltypes is 12740.
    // Estimated selectivity of predicate is 10%.
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypessmall "
        + "WHERE 1 IN " + manyRowSubQuery, 100, true,
        ImmutableSet.of(), path, SelectNode.class);
  }

  @Test
  public void testSelectNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the SelectNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 0);

    // There are no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // There are no available statistics in functional_parquet.alltypessmall.
    // True cardinality of functional_parquet.alltypessmall is 100.
    String subQuery = "(SELECT int_col "
        + "FROM functional_parquet.alltypestiny "
        + "LIMIT 1)";
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypes "
        + "WHERE 1 IN " + subQuery, 1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, SelectNode.class);
  }

  @Test
  public void testSingularRowSrcNode() {
    // Create the path to the SingularRowSrcNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 1);

    verifyApproxCardinality("SELECT c_custkey, pos "
        + "FROM tpch_nested_parquet.customer c, c.c_orders", 1, true,
        ImmutableSet.of(), path, SingularRowSrcNode.class);
  }

  @Test
  public void testSingularRowSrcNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the SingularRowSrcNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 1);

    verifyApproxCardinality("SELECT c_custkey, pos "
        + "FROM tpch_nested_parquet.customer c, c.c_orders", 1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, SingularRowSrcNode.class);
  }

  @Test
  public void testSortNode() {
    // Create the path to the SortNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    // Estimated cardinality of functional_parquet.alltypestiny is 742.
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypestiny "
        + "ORDER BY int_col", 742, true, ImmutableSet.of(), path,
        SortNode.class);
  }

  @Test
  public void testSortNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the SortNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional_parquet.alltypestiny.
    // True cardinality of functional_parquet.alltypestiny is 8.
    verifyApproxCardinality("SELECT * "
        + "FROM functional_parquet.alltypestiny "
        + "ORDER BY int_col", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, SortNode.class);
  }

  @Test
  public void testPartitionedTopNNode() {
    // Create the path to the SortNode and SelectNode of interest in a distributed plan.
    List<Integer> selectPath = Arrays.asList(0);
    List<Integer> sortPath = Arrays.asList(0, 0, 0);

    // NDV(smallint_col) = 97, NDV(bool_col) = 2
    // 5 rows per top-N partition
    // 97 * 2 * 5 = 970
    String lessThanQuery = "select * from (" +
            "  select *, row_number() over " +
            "  (partition by smallint_col, bool_col order by id) as rn" +
            "  from functional.alltypesagg where id % 777 = 0 or id % 10 = 7) v" +
            " where rn <= 5";
    final int EXPECTED_CARDINALITY = 970;
    // Both TOP-N node and the upper Select  node are expected to return the same number
    // of nodes - the predicate in the Select is effectively pushed to the top-n, so
    // its selectivity should be 1.
    verifyApproxCardinality(lessThanQuery, EXPECTED_CARDINALITY, true, ImmutableSet.of(),
            sortPath, SortNode.class);
    verifyApproxCardinality(lessThanQuery, EXPECTED_CARDINALITY, true, ImmutableSet.of(),
            selectPath, SelectNode.class);


    // Any equality predicate results in the same cardinality estimate for the top-n.
    // It also results in the same estimate for the Select node, but for a different
    // reason: the NDV of row_number() is estimated as 1, so the equality predicate
    // has a selectivity estimate of 1.0.
    String eqQuery = "select * from (" +
            "  select *, row_number() over " +
            "  (partition by smallint_col, bool_col order by id) as rn" +
            "  from functional.alltypesagg where id % 777 = 0 or id % 10 = 7) v" +
            " where rn = 5";
    verifyApproxCardinality(eqQuery, EXPECTED_CARDINALITY, true, ImmutableSet.of(),
            sortPath, SortNode.class);
    verifyApproxCardinality(eqQuery, EXPECTED_CARDINALITY, true, ImmutableSet.of(),
            selectPath, SelectNode.class);
  }


  @Test
  public void testSubPlanNode() {
    // Create the path to the SubplanNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0);

    verifyApproxCardinality("SELECT c_custkey, pos "
        + "FROM tpch_nested_parquet.customer c, c.c_orders", 1_500_000, true,
        ImmutableSet.of(), path, SubplanNode.class);
  }

  @Test
  public void testSubPlanNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the SubplanNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0);

    verifyApproxCardinality("SELECT c_custkey, pos "
        + "FROM tpch_nested_parquet.customer c, c.c_orders", 1_500_000, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, SubplanNode.class);
  }

  @Test
  public void testUnionNode() {
    // Create the path to the UnionNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    // Estimated cardinality of functional.tinytable is 2.
    String subQuery = "SELECT * FROM functional.tinytable";
    verifyApproxCardinality(subQuery + " UNION ALL " + subQuery, 4, true,
        ImmutableSet.of(), path, UnionNode.class);
  }

  @Test
  public void testUnionNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the UnionNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0);

    // There is no available statistics in functional.tinytable.
    // True cardinality of functional.tinytable is 3.
    String subQuery = "SELECT * FROM functional.tinytable";
    verifyApproxCardinality(subQuery + " UNION ALL " + subQuery, -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, UnionNode.class);

    // Same test, except with some constant expressions added to the union.
    // The output cardinality still cannot be estimated.
    verifyApproxCardinality(
        subQuery + " UNION ALL " + subQuery + " UNION ALL SELECT 'a', 'b'", -1, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, UnionNode.class);

    // If one branch of the union has known cardinality, that value is used.
    // alltypestiny has 8 rows and computed stats.
    verifyApproxCardinality(subQuery + " UNION ALL " +
        "SELECT string_col, date_string_col from functional.alltypestiny",
        8, true, ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, UnionNode.class);
  }

  @Test
  public void testUnnestNode() {
    // Create the path to the UnnestNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 0);

    verifyApproxCardinality("SELECT c_custkey, pos "
        + "FROM tpch_nested_parquet.customer c, c.c_orders", 10, true,
        ImmutableSet.of(), path, UnnestNode.class);
  }

  @Test
  public void testUnnestNodeWithHDFSNumRowsEstDisabled() {
    // Create the path to the UnnestNode of interest
    // in a distributed plan.
    List<Integer> path = Arrays.asList(0, 1, 0);

    verifyApproxCardinality("SELECT c_custkey, pos "
        + "FROM tpch_nested_parquet.customer c, c.c_orders", 10, true,
        ImmutableSet.of(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE),
        path, UnnestNode.class);
  }

  @Test
  public void testAggregationNodeMemoryEstimate() {
    // Create the paths to the AggregationNode's of interest
    // in a distributed plan involving GROUP BY.
    // Since there are two resulting AggregationNode's, we create two paths.
    List<Integer> pathToFirstAggregationNode = Arrays.asList(0);
    List<Integer> pathToSecondAggregationNode = Arrays.asList(0, 0, 0);
    // Single node execution plan, there is only one AggregationNode
    List<Integer> pathToAggregationNode = Arrays.asList();

    // There is available statistics in functional.alltypes.
    // True cardinality of functional.alltypes is 7300.
    // Ndv of int_col is 10;
    // MIN_HASH_TBL_MEM is 10M
    verifyApproxMemoryEstimate("SELECT COUNT(int_col) FROM functional.alltypes "
            + "GROUP BY int_col",
        QueryConstants.MIN_HASH_TBL_MEM, true, false, ImmutableSet.of(),
        pathToFirstAggregationNode, AggregationNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT COUNT(int_col) FROM functional.alltypes "
            + "GROUP BY int_col",
        QueryConstants.MIN_HASH_TBL_MEM, false, false, ImmutableSet.of(),
        pathToAggregationNode, AggregationNode.class);

    // FUNCTIONAL.ALLTYPES.ID's Ndv is 7300 and avgRowSize is 4
    // FUNCTIONAL.ALLTYPESSMALL.TIMESTAMP_COL's Ndv is 100 and avgRowSize is 16
    // FUNCTIONAL.NULLROWS.BOOL_NULLS's Ndv is 3 and avgRowSize is 1
    // COUNT(*)'s avgRowSize is 8 and MAX(B.BIGINT_COL) avgRowSize is 8
    // 2190000 = 7300*100*3
    verifyApproxMemoryEstimate("SELECT COUNT(*),MAX(B.BIGINT_COL) FROM " +
        "FUNCTIONAL.ALLTYPES A , FUNCTIONAL.ALLTYPESSMALL B, " +
        "FUNCTIONAL.NULLROWS C GROUP BY A.ID, B.TIMESTAMP_COL,C.BOOL_NULLS",
        2190000*(4+16+1+8+8+12), true, true, ImmutableSet.of(),
        pathToFirstAggregationNode, AggregationNode.class);
    verifyApproxMemoryEstimate("SELECT COUNT(*),MAX(B.BIGINT_COL) FROM " +
        "FUNCTIONAL.ALLTYPES A , FUNCTIONAL.ALLTYPESSMALL B, " +
        "FUNCTIONAL.NULLROWS C GROUP BY A.ID, B.TIMESTAMP_COL,C.BOOL_NULLS",
        2190000*(4+16+1+8+8+12), true, false, ImmutableSet.of(),
        pathToSecondAggregationNode, AggregationNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT COUNT(*),MAX(B.BIGINT_COL) FROM " +
        "FUNCTIONAL.ALLTYPES A , FUNCTIONAL.ALLTYPESSMALL B, " +
        "FUNCTIONAL.NULLROWS C GROUP BY A.ID, B.TIMESTAMP_COL,C.BOOL_NULLS",
        2190000*(4+16+1+8+8+12), false, false, ImmutableSet.of(),
        pathToAggregationNode, AggregationNode.class);

    List<Integer> countFirstAggregationNode = Arrays.asList();
    List<Integer> countSecondAggregationNode = Arrays.asList();
    // Query has no Group by Clause
    verifyApproxMemoryEstimate("SELECT COUNT(*) FROM FUNCTIONAL.ALLTYPES A ",
        AggregationNode.MIN_PLAIN_AGG_MEM, true, false, ImmutableSet.of(),
        countFirstAggregationNode, AggregationNode.class);
    verifyApproxMemoryEstimate("SELECT COUNT(*) FROM FUNCTIONAL.ALLTYPES A ",
        AggregationNode.MIN_PLAIN_AGG_MEM, true, false, ImmutableSet.of(),
        countSecondAggregationNode, AggregationNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT COUNT(*) FROM FUNCTIONAL.ALLTYPES A ",
        AggregationNode.MIN_PLAIN_AGG_MEM, false, false, ImmutableSet.of(),
        pathToAggregationNode, AggregationNode.class);
  }

  @Test
  public void testSortNodeMemoryEstimate() {
    List<Integer> pathToSortNode = Arrays.asList(0);

    List<Integer> pathToSortNodeSg = Arrays.asList();

    // FUNCTIONAL.ALLTYPES.ID's Ndv is 7300 and avgRowSize is 4
    // FUNCTIONAL.ALLTYPESSMALL.TIMESTAMP_COL's Ndv is 100 and avgRowSize is 16
    verifyApproxMemoryEstimate("SELECT A.TIMESTAMP_COL,B.TIMESTAMP_COL FROM " +
        "FUNCTIONAL.ALLTYPES A , FUNCTIONAL.ALLTYPESSMALL B " +
        "ORDER BY A.TIMESTAMP_COL,B.TIMESTAMP_COL",
        7300*100*(16+16), true, true, ImmutableSet.of(),
        pathToSortNode, SortNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT A.TIMESTAMP_COL,B.TIMESTAMP_COL FROM " +
        "FUNCTIONAL.ALLTYPES A , FUNCTIONAL.ALLTYPESSMALL B " +
        "ORDER BY A.TIMESTAMP_COL,B.TIMESTAMP_COL",
        7300*100*(16+16), false, false, ImmutableSet.of(),
        pathToSortNodeSg, SortNode.class);
  }

  @Test
  public void testHashJoinNodeMemoryEstimate() {
    List<Integer> pathToJoinNode = Arrays.asList(0);
    List<Integer> pathToJoinNodeSg = Arrays.asList();

    // FUNCTIONAL.ALLTYPES.ID's Ndv is 7300 and avgRowSize is 4
    // FUNCTIONAL.ALLTYPES.DATE_STRING_COL's Ndv is 736 and avgRowSize is 8
    // Cardinality estimate of subquery is 5372800
    // BitUtil.roundUpToPowerOf2((long) Math.ceil(3 * Cardinality / 2)) is 8388608
    // Size of Bucket is 12.
    verifyApproxMemoryEstimate("SELECT A.ID FROM (SELECT A1.ID,B1.DATE_STRING_COL " +
        "IDB FROM FUNCTIONAL.ALLTYPES A1 , FUNCTIONAL.ALLTYPES B1 GROUP BY A1.ID," +
        "B1.DATE_STRING_COL) A JOIN (SELECT A1.ID,B1.DATE_STRING_COL IDB FROM " +
        "FUNCTIONAL.ALLTYPES A1,FUNCTIONAL.ALLTYPES B1 GROUP BY " +
        "A1.ID,B1.DATE_STRING_COL) B ON A.ID = B.ID AND A.IDB=B.IDB",
        5372800*(4+8+4+8) + 8388608*12, true, true, ImmutableSet.of(),
        pathToJoinNode, HashJoinNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT A.ID FROM (SELECT A1.ID,B1.DATE_STRING_COL " +
        "IDB FROM FUNCTIONAL.ALLTYPES A1 , FUNCTIONAL.ALLTYPES B1 GROUP BY A1.ID," +
        "B1.DATE_STRING_COL) A JOIN (SELECT A1.ID,B1.DATE_STRING_COL IDB FROM " +
        "FUNCTIONAL.ALLTYPES A1,FUNCTIONAL.ALLTYPES B1 GROUP BY " +
        "A1.ID,B1.DATE_STRING_COL) B ON A.ID = B.ID AND A.IDB=B.IDB",
        5372800*(4+8+4+8) + 8388608*12, false, false, ImmutableSet.of(),
        pathToJoinNodeSg, HashJoinNode.class);

    // FUNCTIONAL.ALLTYPES.ID's Ndv is 7300 and avgRowSize is 4
    // FUNCTIONAL.ALLTYPES.DATE_STRING_COL's Ndv is 736 and avgRowSize is 8
    // Cardinality estimate of subquery is 53290000
    // BitUtil.roundUpToPowerOf2((long) Math.ceil(3 * Cardinality / 2)) is 134217728
    // Size of Bucket is 12. Size of Duplicatenode is 16
    // Ndv estimate of subquery is 5372800
    verifyApproxMemoryEstimate("SELECT A.ID FROM (SELECT A1.ID,B1.DATE_STRING_COL " +
        "IDB FROM FUNCTIONAL.ALLTYPES A1 , FUNCTIONAL.ALLTYPES B1) A JOIN " +
        "(SELECT A1.ID,B1.DATE_STRING_COL IDB FROM FUNCTIONAL.ALLTYPES A1," +
        "FUNCTIONAL.ALLTYPES B1) B ON A.ID = B.ID AND A.IDB=B.IDB",
        53290000L*(4L+8L+4L+8L)+134217728L*12L+(53290000L-5372800L)*16L, true, true,
        ImmutableSet.of(), pathToJoinNode, HashJoinNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT A.ID FROM (SELECT A1.ID,B1.DATE_STRING_COL " +
        "IDB FROM FUNCTIONAL.ALLTYPES A1 , FUNCTIONAL.ALLTYPES B1) A JOIN " +
        "(SELECT A1.ID,B1.DATE_STRING_COL IDB FROM FUNCTIONAL.ALLTYPES A1," +
        "FUNCTIONAL.ALLTYPES B1) B ON A.ID = B.ID AND A.IDB=B.IDB",
        53290000L*(4L+8L+4L+8L)+134217728L*12L+(53290000L-5372800L)*16L, false, false,
        ImmutableSet.of(), pathToJoinNodeSg, HashJoinNode.class);
  }

  @Test
  public void testKuduScanNodeMemoryEstimate() {
    List<Integer> pathToKuduScanNode = Arrays.asList(0);
    List<Integer> pathToKuduScanNodeSg = Arrays.asList();
    // Scan 1 column.
    verifyApproxMemoryEstimate("SELECT ID FROM FUNCTIONAL_KUDU.ALLTYPESSMALL",
        393216*1*2, true, false,
        ImmutableSet.of(), pathToKuduScanNode, KuduScanNode.class);
    // create a single node plan.
    verifyApproxMemoryEstimate("SELECT ID FROM FUNCTIONAL_KUDU.ALLTYPESSMALL",
        393216*1*2, false, false,
        ImmutableSet.of(), pathToKuduScanNodeSg, KuduScanNode.class);
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
    assertEquals("Cardinality error for: " + query, expected,
        planRoot.getCardinality());
  }

  protected void verifyCardinality(String query, long expected,
      boolean isDistributedPlan, Set<PlannerTestOption> testOptions) {
    List<PlanFragment> plan = getPlan(query, isDistributedPlan, testOptions);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    assertEquals("Cardinality error for: " + query, expected,
        planRoot.getCardinality());
  }


  /* This method allows us to inspect the cardinality of a PlanNode located by
  * path with respect to the root of the retrieved query plan. The class of
  * the located PlanNode by path will also be checked against cl, the class of
  * the PlanNode of interest.
  *
  * @param query query to test
  * @param expected expected cardinality at the PlanNode of interest
  * @param isDistributedPlan set to true if we would like to generate
  * a distributed plan
  * @param testOptions specified test options
  * @param path path to the PlanNode of interest
  * @param cl class of the PlanNode of interest
  */
  protected void verifyCardinality(String query, long expected,
      boolean isDistributedPlan, Set<PlannerTestOption> testOptions,
      List<Integer> path, Class<?> cl) {
    List<PlanFragment> plan = getPlan(query, isDistributedPlan, testOptions);
    // We use the last element on the List of PlanFragment
    // because this PlanFragment encloses all the PlanNode's
    // in the query plan (either the single node plan or
    // the distributed plan).
    PlanNode currentNode = plan.get(plan.size() - 1).getPlanRoot();
    for (Integer currentChildIndex: path) {
      currentNode = currentNode.getChild(currentChildIndex);
    }
    assertEquals("PlanNode class not matched: ", cl.getName(),
        currentNode.getClass().getName());
    assertEquals("Cardinality error for: " + query,
        expected, currentNode.getCardinality());
  }

  /**
   * The cardinality check performed by this method allows for a margin of
   * error. Specifically, two cardinalities are considered equal as long as
   * the difference is upper bounded by the expected number multiplied by
   * CARDINALITY_TOLERANCE.
   *
   * @param query query to test
   * @param expected expected cardinality at the root node
   */
  protected void verifyApproxCardinality(String query, long expected) {
    List<PlanFragment> plan = getPlan(query);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    assertEquals("Cardinality error for: " + query, expected,
        planRoot.getCardinality(), expected * CARDINALITY_TOLERANCE);
  }

  /**
   * This method allows us to inspect the cardinality of a PlanNode located by
   * path with respect to the root of the retrieved query plan. The class of
   * the located PlanNode by path will also be checked against cl, the class of
   * the PlanNode of interest.
   * In addition, the cardinality check performed by this method allows for a
   * margin of error. Specifically, two cardinalities are considered equal as
   * long as the difference is upper bounded by the expected number multiplied
   * by CARDINALITY_TOLERANCE.
   *
   * @param query query to test
   * @param expected expected cardinality at the PlanNode of interest
   * @param isDistributedPlan set to true if we would like to generate
   * a distributed plan
   * @param testOptions specified test options
   * @param path path to the PlanNode of interest
   * @param cl class of the PlanNode of interest
   */
  protected void verifyApproxCardinality(String query, long expected,
      boolean isDistributedPlan, Set<PlannerTestOption> testOptions,
      List<Integer> path, Class<?> cl) {

    List<PlanFragment> plan = getPlan(query, isDistributedPlan, testOptions);
    // We use the last element on the List of PlanFragment
    // because this PlanFragment encloses all the PlanNode's
    // in the query plan (either the single node plan or
    // the distributed plan).
    PlanNode currentNode = plan.get(plan.size() - 1).getPlanRoot();
    for (Integer currentChildIndex: path) {
      assertTrue(currentNode.getDisplayLabel() + " does not have child index "
              + currentChildIndex,
          currentNode.hasChild(currentChildIndex));
      currentNode = currentNode.getChild(currentChildIndex);
    }
    assertEquals("PlanNode class not matched: ", cl.getName(),
        currentNode.getClass().getName());
    assertEquals("Cardinality error for: " + query,
        expected, currentNode.getCardinality(), expected * CARDINALITY_TOLERANCE);
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

  private List<PlanFragment> getPlan(String query,
      boolean isDistributedPlan, Set<PlannerTestOption> testOptions) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);

    // Disable the attempt to compute an estimated number of rows in an
    // hdfs table.
    TQueryOptions queryOptions = queryCtx.client_request.getQuery_options();
    queryOptions.setDisable_hdfs_num_rows_estimate(
        testOptions.contains(PlannerTestOption.DISABLE_HDFS_NUM_ROWS_ESTIMATE));
    // Instruct the planner to generate a distributed plan
    // by setting the query option of NUM_NODES to 0 if
    // distributedPlan is set to true.
    // Otherwise, set NUM_NODES to 1 to instruct the planner to
    // create a single node plan.
    queryOptions.setNum_nodes(isDistributedPlan? 0: 1);

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
  * This method allows us to get a PlanNode located by
  * path with respect to the root of the retrieved query plan. The class of
  * the located PlanNode by path will also be checked against cl, the class of
  * the PlanNode of interest.
  *
  * @param query query to test
  * @param isDistributedPlan set to true if we would like to generate
  * a distributed plan
  * @param testOptions specified test options
  * @param path path to the PlanNode of interest
  * @param cl class of the PlanNode of interest
  */
  protected PlanNode getPlanNode(String query, boolean isDistributedPlan,
      Set<PlannerTestOption> testOptions, List<Integer> path, Class<?> cl) {
    List<PlanFragment> plan = getPlan(query, isDistributedPlan, testOptions);
    // We use the last element on the List of PlanFragment
    // because this PlanFragment encloses all the PlanNode's
    // in the query plan (either the single node plan or
    // the distributed plan).
    PlanNode currentNode = plan.get(plan.size() - 1).getPlanRoot();
    for (Integer currentChildIndex: path) {
      currentNode = currentNode.getChild(currentChildIndex);
    }
    assertEquals("PlanNode class not matched: ", cl.getName(),
        currentNode.getClass().getName());
    return currentNode;
  }

  /**
  * This method allows us to inspect the Memory Estimate of a PlanNode located by
  * path with respect to the root of the retrieved query plan. The class of
  * the located PlanNode by path will also be checked against cl, the class of
  * the PlanNode of interest.
  *
  * @param query query to test
  * @param expected expected Memory at the PlanNode of interest
  * @param isDistributedPlan set to true if we would like to generate
  * a distributed plan
  * @param testOptions specified test options
  * @param path path to the PlanNode of interest
  * @param cl class of the PlanNode of interest
  */
  protected void verifyApproxMemoryEstimate(String query, long expected,
      boolean isDistributedPlan, boolean isMultiNodes,
      Set<PlannerTestOption> testOptions, List<Integer> path, Class<?> cl) {
    PlanNode pNode = getPlanNode(query, isDistributedPlan, testOptions, path, cl);
    long result = pNode.getNodeResourceProfile().getMemEstimateBytes();
    if (isMultiNodes) {
      result = (long)Math.ceil(result * pNode.getFragment().getNumInstances());
    }
    assertEquals("Memory Estimate error for: " + query, expected, result,
        expected * CARDINALITY_TOLERANCE);
  }

}
