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

import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TJoinDistributionMode;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

// All planner tests, except for S3 specific tests should go here.
public class PlannerTest extends PlannerTestBase {

  @Test
  public void testPredicatePropagation() {
    runPlannerTestFile("predicate-propagation");
  }

  @Test
  public void testConstant() {
    runPlannerTestFile("constant");
  }

  @Test
  public void testConstantFolding() {
    // Tests that constant folding is applied to all relevant PlanNodes and DataSinks.
    // Note that not all Exprs are printed in the explain plan, so validating those
    // via this test is currently not possible.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("constant-folding", options);
  }

  @Test
  public void testConstantPropagataion() {
    runPlannerTestFile("constant-propagation");
  }

  @Test
  public void testEmpty() {
    runPlannerTestFile("empty");
  }

  @Test
  public void testDistinct() {
    runPlannerTestFile("distinct");
  }

  @Test
  public void testAggregation() {
    runPlannerTestFile("aggregation");
  }

  @Test
  public void testAnalyticFns() {
    runPlannerTestFile("analytic-fns");
  }

  @Test
  public void testHbase() {
    runPlannerTestFile("hbase");
  }

  @Test
  public void testInsert() {
    runPlannerTestFile("insert");
  }

  @Test
  public void testInsertSortBy() {
    // Add a test table with a SORT BY clause to test that the corresponding sort nodes
    // are added by the insert statements in insert-sort-by.test.
    addTestDb("test_sort_by", "Test DB for SORT BY clause.");
    addTestTable("create table test_sort_by.t (id int, int_col int, " +
        "bool_col boolean) partitioned by (year int, month int) " +
        "sort by (int_col, bool_col) location '/'");
    addTestTable("create table test_sort_by.t_nopart (id int, int_col int, " +
        "bool_col boolean) sort by (int_col, bool_col) location '/'");
    runPlannerTestFile("insert-sort-by", "test_sort_by");
  }

  @Test
  public void testHdfs() {
    runPlannerTestFile("hdfs");
  }

  @Test
  public void testNestedCollections() {
    runPlannerTestFile("nested-collections");
  }

  @Test
  public void testComplexTypesFileFormats() {
    runPlannerTestFile("complex-types-file-formats");
  }

  @Test
  public void testJoins() {
    runPlannerTestFile("joins");
  }

  @Test
  public void testJoinOrder() {
    runPlannerTestFile("join-order");
  }

  @Test
  public void testOuterJoins() {
    runPlannerTestFile("outer-joins");
  }

  @Test
  public void testImplicitJoins() {
    runPlannerTestFile("implicit-joins");
  }

  @Test
  public void testFkPkJoinDetection() {
    TQueryOptions options = defaultQueryOptions();
    // The FK/PK detection result is included in EXTENDED or higher.
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("fk-pk-join-detection", options);
  }

  @Test
  public void testOrder() {
    runPlannerTestFile("order");
  }

  @Test
  public void testTopN() {
    runPlannerTestFile("topn");
  }

  @Test
  public void testInlineView() {
    runPlannerTestFile("inline-view");
  }

  @Test
  public void testInlineViewLimit() {
    runPlannerTestFile("inline-view-limit");
  }

  @Test
  public void testSubqueryRewrite() {
    runPlannerTestFile("subquery-rewrite");
  }

  @Test
  public void testUnion() {
    runPlannerTestFile("union");
  }

  @Test
  public void testValues() {
    runPlannerTestFile("values");
  }

  @Test
  public void testViews() {
    runPlannerTestFile("views");
  }

  @Test
  public void testWithClause() {
    runPlannerTestFile("with-clause");
  }

  @Test
  public void testDistinctEstimate() {
    runPlannerTestFile("distinct-estimate");
  }

  @Test
  public void testDataSourceTables() {
    runPlannerTestFile("data-source-tables");
  }

  @Test
  public void testPartitionKeyScans() {
    TQueryOptions options = new TQueryOptions();
    options.setOptimize_partition_key_scans(true);
    runPlannerTestFile("partition-key-scans", options);
  }

  @Test
  public void testLineage() {
    runPlannerTestFile("lineage");
  }

  @Test
  public void testDdl() {
    runPlannerTestFile("ddl");
  }

  @Test
  public void testTpch() {
    runPlannerTestFile("tpch-all", "tpch");
  }

  @Test
  public void testTpchViews() {
    // Re-create TPCH with views on the base tables. Used for testing
    // that plan generation works as expected through views.
    addTestDb("tpch_views", "Test DB for TPCH with views.");
    Db tpchDb = catalog_.getDb("tpch");
    for (String tblName: tpchDb.getAllTableNames()) {
      addTestView(String.format(
          "create view tpch_views.%s as select * from tpch.%s", tblName, tblName));
    }
    runPlannerTestFile("tpch-views", "tpch_views");
  }

  @Test
  public void testTpchNested() {
    runPlannerTestFile("tpch-nested", "tpch_nested_parquet");
  }

  @Test
  public void testTpcds() {
    // Uses ss_sold_date_sk as the partition key of store_sales to allow static partition
    // pruning. The original predicates were rephrased in terms of the ss_sold_date_sk
    // partition key, with the query semantics identical to the original queries.
    runPlannerTestFile("tpcds-all", "tpcds");
  }

  @Test
  public void testSmallQueryOptimization() {
    TQueryOptions options = new TQueryOptions();
    options.setExec_single_node_rows_threshold(8);
    runPlannerTestFile("small-query-opt", options);
  }

  @Test
  public void testDisableCodegenOptimization() {
    TQueryOptions options = new TQueryOptions();
    options.setDisable_codegen_rows_threshold(3000);
    runPlannerTestFile("disable-codegen", options, false);
  }

  @Test
  public void testSingleNodeNlJoin() {
    TQueryOptions options = new TQueryOptions();
    options.setNum_nodes(1);
    runPlannerTestFile("nested-loop-join", options);
  }

  @Test
  public void testMemLimit() {
    // TODO: Create a new test case section for specifying options
    TQueryOptions options = new TQueryOptions();
    options.setMem_limit(500);
    runPlannerTestFile("mem-limit-broadcast-join", options);
  }

  @Test
  public void testDisablePreaggregations() {
    TQueryOptions options = new TQueryOptions();
    options.setDisable_streaming_preaggregations(true);
    runPlannerTestFile("disable-preaggregations", options);
  }

  @Test
  public void testRuntimeFilterPropagation() {
    TQueryOptions options = new TQueryOptions();
    options.setRuntime_filter_mode(TRuntimeFilterMode.GLOBAL);
    runPlannerTestFile("runtime-filter-propagation", options);
  }

  @Test
  public void testRuntimeFilterQueryOptions() {
    runPlannerTestFile("runtime-filter-query-options");
  }

  @Test
  public void testConjunctOrdering() {
    runPlannerTestFile("conjunct-ordering");
  }

  @Test
  public void testParquetStatsAgg() { runPlannerTestFile("parquet-stats-agg"); }

  @Test
  public void testParquetFiltering() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("parquet-filtering", options);
  }

  @Test
  public void testKudu() {
    Assume.assumeTrue(RuntimeEnv.INSTANCE.isKuduSupported());
    addTestDb("kudu_planner_test", "Test DB for Kudu Planner.");
    addTestTable("CREATE EXTERNAL TABLE kudu_planner_test.no_stats STORED AS KUDU " +
        "TBLPROPERTIES ('kudu.table_name' = 'impala::functional_kudu.alltypes');");
    runPlannerTestFile("kudu");
  }

  @Test
  public void testKuduUpsert() {
    Assume.assumeTrue(RuntimeEnv.INSTANCE.isKuduSupported());
    runPlannerTestFile("kudu-upsert");
  }

  @Test
  public void testKuduUpdate() {
    Assume.assumeTrue(RuntimeEnv.INSTANCE.isKuduSupported());
    runPlannerTestFile("kudu-update");
  }

  @Test
  public void testKuduDelete() {
    Assume.assumeTrue(RuntimeEnv.INSTANCE.isKuduSupported());
    runPlannerTestFile("kudu-delete");
  }

  @Test
  public void testKuduSelectivity() {
    Assume.assumeTrue(RuntimeEnv.INSTANCE.isKuduSupported());
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.VERBOSE);
    runPlannerTestFile("kudu-selectivity", options);
  }

  @Test
  public void testKuduTpch() {
    Assume.assumeTrue(RuntimeEnv.INSTANCE.isKuduSupported());
    runPlannerTestFile("tpch-kudu");
  }

  @Test
  public void testMtDopValidation() {
    // Tests that queries supported with mt_dop > 0 produce a parallel plan, or
    // throw a NotImplementedException otherwise (e.g. plan has a distributed join).
    TQueryOptions options = defaultQueryOptions();
    options.setMt_dop(3);
    try {
      // Temporarily unset the test env such that unsupported queries with mt_dop > 0
      // throw an exception. Those are otherwise allowed for testing parallel plans.
      RuntimeEnv.INSTANCE.setTestEnv(false);
      runPlannerTestFile("mt-dop-validation", options);
    } finally {
      RuntimeEnv.INSTANCE.setTestEnv(true);
    }
  }

  @Test
  public void testComputeStatsMtDop() {
    for (int mtDop: new int[] {-1, 0, 1, 16}) {
      int effectiveMtDop = (mtDop != -1) ? mtDop : 0;
      // MT_DOP is not set automatically for stmt other than COMPUTE STATS.
      testEffectiveMtDop(
          "select * from functional_parquet.alltypes", mtDop, effectiveMtDop);
      // MT_DOP is not set automatically for COMPUTE STATS on non-Parquet tables.
      testEffectiveMtDop(
          "compute stats functional.alltypes", mtDop, effectiveMtDop);
    }
    // MT_DOP is set automatically for COMPUTE STATS on Parquet tables,
    // but can be overridden by a user-provided MT_DOP.
    testEffectiveMtDop("compute stats functional_parquet.alltypes", -1, 4);
    testEffectiveMtDop("compute stats functional_parquet.alltypes", 0, 0);
    testEffectiveMtDop("compute stats functional_parquet.alltypes", 1, 1);
    testEffectiveMtDop("compute stats functional_parquet.alltypes", 16, 16);
  }

  /**
   * Creates an exec request for 'stmt' setting the MT_DOP query option to 'userMtDop',
   * or leaving it unset if 'userMtDop' is -1. Asserts that the MT_DOP of the generated
   * exec request is equal to 'expectedMtDop'.
   */
  private void testEffectiveMtDop(String stmt, int userMtDop, int expectedMtDop) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        Catalog.DEFAULT_DB, System.getProperty("user.name"));
    queryCtx.client_request.setStmt(stmt);
    queryCtx.client_request.query_options = defaultQueryOptions();
    if (userMtDop != -1) queryCtx.client_request.query_options.setMt_dop(userMtDop);
    StringBuilder explainBuilder = new StringBuilder();
    TExecRequest request = null;
    try {
      request = frontend_.createExecRequest(queryCtx, explainBuilder);
    } catch (ImpalaException e) {
      Assert.fail("Failed to create exec request for '" + stmt + "': " + e.getMessage());
    }
    Preconditions.checkNotNull(request);
    int actualMtDop = -1;
    if (request.query_options.isSetMt_dop()) actualMtDop = request.query_options.mt_dop;
    // Check that the effective MT_DOP is as expected.
    Assert.assertEquals(actualMtDop, expectedMtDop);
  }

  @Test
  public void testResourceRequirements() {
    // Tests the resource requirement computation from the planner.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    runPlannerTestFile("resource-requirements", options, false);
  }

  @Test
  public void testSpillableBufferSizing() {
    // Tests the resource requirement computation from the planner when it is allowed to
    // vary the spillable buffer size.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    runPlannerTestFile("spillable-buffer-sizing", options, false);
  }

  @Test
  public void testMaxRowSize() {
    // Tests that an increased value of 'max_row_size' is correctly factored into the
    // resource calculations by the planner.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    options.setMax_row_size(8L * 1024L * 1024L);
    runPlannerTestFile("max-row-size", options, false);
  }

  @Test
  public void testSortExprMaterialization() {
    addTestFunction("TestFn", Lists.newArrayList(Type.DOUBLE), false);
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("sort-expr-materialization", options);
  }

  @Test
  public void testTableSample() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("tablesample", options);
  }

  @Test
  public void testDefaultJoinDistributionMode() {
    TQueryOptions options = defaultQueryOptions();
    Preconditions.checkState(
        options.getDefault_join_distribution_mode() == TJoinDistributionMode.BROADCAST);
    runPlannerTestFile("default-join-distr-mode-broadcast", options);
    options.setDefault_join_distribution_mode(TJoinDistributionMode.SHUFFLE);
    runPlannerTestFile("default-join-distr-mode-shuffle", options);
  }

  @Test
  public void testPartitionPruning() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("partition-pruning", options);
  }

  @Test
  public void testComputeStatsDisableSpill() throws ImpalaException {
    TQueryCtx queryCtx = TestUtils.createQueryContext(Catalog.DEFAULT_DB,
        System.getProperty("user.name"));
    TExecRequest requestWithDisableSpillOn = null;
    // Setting up a table with computed stats
    queryCtx.client_request.setStmt("compute stats functional.alltypes");
    queryCtx.client_request.query_options = defaultQueryOptions();
    StringBuilder explainBuilder = new StringBuilder();
    frontend_.createExecRequest(queryCtx, explainBuilder);
    // Setting up an arbitrary query involving a table with stats.
    queryCtx.client_request.setStmt("select * from functional.alltypes");
    // Setting disable_unsafe_spills = true to verify that it no longer
    // throws a NPE with computed stats (IMPALA-5524)
    queryCtx.client_request.query_options.setDisable_unsafe_spills(true);
    requestWithDisableSpillOn = frontend_.createExecRequest(queryCtx, explainBuilder);
    Assert.assertNotNull(requestWithDisableSpillOn);
  }

  @Test
  public void testMinMaxRuntimeFilters() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("min-max-runtime-filters", options);
  }

  @Test
  public void testCardinalityOverflow() throws ImpalaException {
    String tblName = "tpch.cardinality_overflow";
    String colDefs = "("
        + "l_orderkey BIGINT, "
        + "l_partkey BIGINT, "
        + "l_suppkey BIGINT, "
        + "l_linenumber INT, "
        + "l_shipmode STRING, "
        + "l_comment STRING"
        + ")";
    String tblLocation = "LOCATION "
        + "'hdfs://localhost:20500/test-warehouse/tpch.lineitem'";
    String tblPropsTemplate = "TBLPROPERTIES('numRows'='%s')";
    String tblProps = String.format(tblPropsTemplate, Long.toString(Long.MAX_VALUE));

    addTestTable(String.format("CREATE EXTERNAL TABLE %s %s %s %s;",
        tblName, colDefs, tblLocation, tblProps));

    // CROSS JOIN query: tests that multiplying the input cardinalities does not overflow
    // the cross-join's estimated cardinality
    String query = "select * from tpch.cardinality_overflow a,"
        + "tpch.cardinality_overflow b, tpch.cardinality_overflow c";
    checkCardinality(query, 0, Long.MAX_VALUE);

    // FULL OUTER JOIN query: tests that adding the input cardinalities does not overflow
    // the full outer join's estimated cardinality
    query = "select a.l_comment from tpch.cardinality_overflow a full outer join "
        + "tpch.cardinality_overflow b on a.l_orderkey = b.l_partkey";
    checkCardinality(query, 0, Long.MAX_VALUE);

    // UNION query: tests that adding the input cardinalities does not overflow
    // the union's estimated cardinality
    query = "select l_shipmode from tpch.cardinality_overflow "
        + "union select l_comment from tpch.cardinality_overflow";
    checkCardinality(query, 0, Long.MAX_VALUE);

    // JOIN query: tests that multiplying the input cardinalities does not overflow
    // the join's estimated cardinality
    query = "select a.l_comment from tpch.cardinality_overflow a inner join "
        + "tpch.cardinality_overflow b on a.l_linenumber < b.l_orderkey";
    checkCardinality(query, 0, Long.MAX_VALUE);

    // creates an empty table and tests that the cardinality is 0
    tblName = "tpch.ex_customer_cardinality_zero";
    tblProps = String.format(tblPropsTemplate, 0);
    addTestTable(String.format("CREATE EXTERNAL TABLE  %s %s %s %s;",
        tblName, colDefs, tblLocation, tblProps));
    query = "select * from tpch.ex_customer_cardinality_zero";
    checkCardinality(query, 0, 0);

    // creates a table with negative row count and
    // tests that the cardinality is not negative
    tblName = "tpch.ex_customer_cardinality_neg";
    tblProps = String.format(tblPropsTemplate, -1);
    addTestTable(String.format("CREATE EXTERNAL TABLE  %s %s %s %s;",
        tblName, colDefs, tblLocation, tblProps));
    query = "select * from tpch.ex_customer_cardinality_neg";
    checkCardinality(query, -1, Long.MAX_VALUE);

    // SUBPLAN query: tests that adding the input cardinalities does not overflow
    // the SUBPLAN's estimated cardinality
    tblName = "functional_parquet.cardinality_overflow";
    colDefs = "("
        + "id BIGINT, "
        + "int_array ARRAY<INT>"
        + ")";
    String storedAs = "STORED AS PARQUET";
    tblLocation = "LOCATION "
        + "'hdfs://localhost:20500/test-warehouse/complextypestbl_parquet'";
    tblProps = String.format(tblPropsTemplate, Long.toString(Long.MAX_VALUE));
    addTestTable(String.format("CREATE EXTERNAL TABLE  %s %s %s %s %s;",
        tblName, colDefs, storedAs, tblLocation, tblProps));
    query = "select id from functional_parquet.cardinality_overflow t, t.int_array";
    checkCardinality(query, 0, Long.MAX_VALUE);
  }
}
