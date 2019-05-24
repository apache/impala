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

import java.util.ArrayList;

import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.HBaseColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.testutil.TestUtils.IgnoreValueFilter;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

// All planner tests, except for S3 specific tests should go here.
public class PlannerTest extends PlannerTestBase {

  /**
   * Scan node cardinality test
   */
  @Test
  public void testScanCardinality() {
    runPlannerTestFile("card-scan");
  }

  /**
   * Inner join cardinality test
   */
  @Test
  public void testInnerJoinCardinality() {
    runPlannerTestFile("card-inner-join");
  }

  /**
   * Outer join cardinality test
   */
  @Test
  public void testOuterJoinCardinality() {
    runPlannerTestFile("card-outer-join");
  }

  /**
   * 3+ table join cardinality test
   */
  @Test
  public void testMultiJoinCardinality() {
    runPlannerTestFile("card-multi-join");
  }

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
    runPlannerTestFile("constant-folding",
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.INCLUDE_QUERY_WITH_IMPLICIT_CASTS));
  }

  @Test
  public void testConstantPropagataion() {
    runPlannerTestFile("constant-propagation");
  }

  @Test
  public void testEmpty() {
    runPlannerTestFile("empty",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testDistinct() {
    runPlannerTestFile("distinct");
  }

  @Test
  public void testMultipleDistinct() {
    // TODO: Multiple distinct with count(distinct a,b,c) variants.
    // TODO: Multiple distinct in subplan.
    // TODO: Multiple distinct in subqueries.
    // TODO: Multiple distinct lineage tests.
    // TODO: Multiple distinct and SHUFFLE_DISTINCT_EXPRS tests
    runPlannerTestFile("multiple-distinct");
  }

  @Test
  public void testMultipleDistinctMaterialization() {
    runPlannerTestFile("multiple-distinct-materialization");
  }

  @Test
  public void testMultipleDistinctPredicates() {
    runPlannerTestFile("multiple-distinct-predicates");
  }

  @Test
  public void testMultipleDistinctLimit() {
    runPlannerTestFile("multiple-distinct-limit");
  }

  @Test
  public void testShuffleByDistinctExprs() {
    runPlannerTestFile("shuffle-by-distinct-exprs");
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

  /**
   * Test of HBase in the case of disabling the key scan.
   * Normally the HBase scan node goes out to HBase to query the
   * set of keys within the target key range. There are times when this
   * can fail. In these times we fall back to using HMS row count and
   * the estimated key predicate cardinality (which will use key column
   * NDV.) It is hard to test this case in "real life" with an actual
   * HBase cluster. Instead, we simply disable the key scan via an
   * option, then rerun all HBase tests with keys.
   *
   * TODO: Once node cardinality is available (IMPALA-8021), compare
   * estimated cardinality with both methods to ensure we get adequate
   * estimates.
   */
  @Test
  public void testHbaseNoKeyEstimate() {
    runPlannerTestFile("hbase-no-key-est",
        ImmutableSet.of(PlannerTestOption.DISABLE_HBASE_KEY_ESTIMATE));
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
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(false);
    runPlannerTestFile("joins-hdfs-num-rows-est-enabled", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testJoinsWithHDFSNumRowsEstDisabled() {
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("joins", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testJoinOrder() {
    runPlannerTestFile("join-order",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testOuterJoins() {
    runPlannerTestFile("outer-joins",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testImplicitJoins() {
    runPlannerTestFile("implicit-joins",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testFkPkJoinDetection() {
    // The FK/PK detection result is included in EXTENDED or higher.
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(false);
    runPlannerTestFile("fk-pk-join-detection-hdfs-num-rows-est-enabled",
        options, ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testFkPkJoinDetectionWithHDFSNumRowsEstDisabled() {
    // The FK/PK detection result is included in EXTENDED or higher.
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("fk-pk-join-detection",
        options, ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testOrder() {
    runPlannerTestFile("order");
  }

  @Test
  public void testTopN() {
    TQueryOptions options = new TQueryOptions();
    options.setTopn_bytes_limit(0);
    runPlannerTestFile("topn", options);
  }

  @Test
  public void testTopNBytesLimit() {
    runPlannerTestFile("topn-bytes-limit");
  }

  @Test
  public void testTopNBytesLimitSmall() {
    TQueryOptions options = new TQueryOptions();
    options.setTopn_bytes_limit(6);
    runPlannerTestFile("topn-bytes-limit-small", options);
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
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(false);
    runPlannerTestFile("subquery-rewrite-hdfs-num-rows-est-enabled", options);
  }

  @Test
  public void testSubqueryRewriteWithHDFSNumRowsEstDisabled() {
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("subquery-rewrite", options);
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
    runPlannerTestFile("tpch-all", "tpch",
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES,
            PlannerTestOption.VALIDATE_CARDINALITY));
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
    runPlannerTestFile("tpch-nested", "tpch_nested_parquet",
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES,
            PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testTpcds() {
    // Uses ss_sold_date_sk as the partition key of store_sales to allow static partition
    // pruning. The original predicates were rephrased in terms of the ss_sold_date_sk
    // partition key, with the query semantics identical to the original queries.
    runPlannerTestFile("tpcds-all", "tpcds",
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
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
    runPlannerTestFile("disable-codegen", options,
        ImmutableSet.of(PlannerTestOption.INCLUDE_EXPLAIN_HEADER));
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
  public void testParquetStatsAgg() {
    runPlannerTestFile("parquet-stats-agg");
  }

  @Test
  public void testParquetFiltering() {
    runPlannerTestFile("parquet-filtering",
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN));
  }

  @Test
  public void testParquetFilteringDisabled() {
    TQueryOptions options = new TQueryOptions();
    options.setParquet_dictionary_filtering(false);
    options.setParquet_read_statistics(false);
    runPlannerTestFile("parquet-filtering-disabled", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN));
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
    runPlannerTestFile("tpch-kudu", ImmutableSet.of(
        PlannerTestOption.INCLUDE_RESOURCE_HEADER,
        PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testMtDopValidation() {
    // Tests that queries supported with mt_dop > 0 produce a parallel plan, or
    // throw a NotImplementedException otherwise (e.g. plan has a distributed join).
    TQueryOptions options = defaultQueryOptions();
    options.setMt_dop(3);
    options.setDisable_hdfs_num_rows_estimate(false);
    try {
      // Temporarily unset the test env such that unsupported queries with mt_dop > 0
      // throw an exception. Those are otherwise allowed for testing parallel plans.
      RuntimeEnv.INSTANCE.setTestEnv(false);
      runPlannerTestFile("mt-dop-validation-hdfs-num-rows-est-enabled", options);
    } finally {
      RuntimeEnv.INSTANCE.setTestEnv(true);
    }
  }

  @Test
  public void testMtDopValidationWithHDFSNumRowsEstDisabled() {
    // Tests that queries supported with mt_dop > 0 produce a parallel plan, or
    // throw a NotImplementedException otherwise (e.g. plan has a distributed join).
    TQueryOptions options = defaultQueryOptions();
    options.setMt_dop(3);
    options.setDisable_hdfs_num_rows_estimate(true);
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
    TExecRequest request = null;
    try {
      PlanCtx planCtx = new PlanCtx(queryCtx);
      request = frontend_.createExecRequest(planCtx);
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
  public void testResourceRequirementsWithHDFSNumRowsEstDisabled() {
    // Tests the resource requirement computation from the planner.
    TQueryOptions options = defaultQueryOptions();
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("resource-requirements", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.INCLUDE_EXPLAIN_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testSpillableBufferSizingWithHDFSNumRowsEstDisabled() {
    // Tests the resource requirement computation from the planner when it is allowed to
    // vary the spillable buffer size.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("spillable-buffer-sizing", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.INCLUDE_EXPLAIN_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testMaxRowSize() {
    // Tests that an increased value of 'max_row_size' is correctly factored into the
    // resource calculations by the planner.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    options.setMax_row_size(8L * 1024L * 1024L);
    runPlannerTestFile("max-row-size", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
          PlannerTestOption.INCLUDE_EXPLAIN_HEADER,
          PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testSortExprMaterialization() {
    addTestFunction("TestFn", Lists.newArrayList(Type.DOUBLE), false);
    runPlannerTestFile("sort-expr-materialization",
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN));
  }

  @Test
  public void testTableSample() {
    TQueryOptions options = defaultQueryOptions();
    runPlannerTestFile("tablesample", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN));
  }

  @Test
  public void testDefaultJoinDistributionBroadcastMode() {
    TQueryOptions options = defaultQueryOptions();
    Preconditions.checkState(
        options.getDefault_join_distribution_mode() == TJoinDistributionMode.BROADCAST);
    runPlannerTestFile("default-join-distr-mode-broadcast", options);
  }

  @Test
  public void testDefaultJoinDistributionShuffleMode() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_join_distribution_mode(TJoinDistributionMode.SHUFFLE);
    options.setDisable_hdfs_num_rows_estimate(false);
    runPlannerTestFile("default-join-distr-mode-shuffle-hdfs-num-rows-est-enabled",
        options);
  }

  @Test
  public void testDefaultJoinDistributionShuffleModeWithHDFSNumRowsEstDisabled() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_join_distribution_mode(TJoinDistributionMode.SHUFFLE);
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("default-join-distr-mode-shuffle",
        options);
  }

  @Test
  public void testPartitionPruning() {
    runPlannerTestFile("partition-pruning",
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN));
  }

  @Test
  public void testComputeStatsDisableSpill() throws ImpalaException {
    TQueryCtx queryCtx = TestUtils.createQueryContext(Catalog.DEFAULT_DB,
        System.getProperty("user.name"));
    TExecRequest requestWithDisableSpillOn = null;
    // Setting up a table with computed stats
    queryCtx.client_request.setStmt("compute stats functional.alltypes");
    queryCtx.client_request.query_options = defaultQueryOptions();
    PlanCtx planCtx = new PlanCtx(queryCtx);
    frontend_.createExecRequest(planCtx);
    // Setting up an arbitrary query involving a table with stats.
    queryCtx.client_request.setStmt("select * from functional.alltypes");
    // Setting disable_unsafe_spills = true to verify that it no longer
    // throws a NPE with computed stats (IMPALA-5524)
    queryCtx.client_request.query_options.setDisable_unsafe_spills(true);
    planCtx = new PlanCtx(queryCtx);
    requestWithDisableSpillOn = frontend_.createExecRequest(planCtx);
    Assert.assertNotNull(requestWithDisableSpillOn);
  }

  @Test
  public void testMinMaxRuntimeFilters() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setDisable_hdfs_num_rows_estimate(false);
    runPlannerTestFile("min-max-runtime-filters-hdfs-num-rows-est-enabled", options);
  }

  @Test
  public void testMinMaxRuntimeFiltersWithHDFSNumRowsEstDisabled() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setDisable_hdfs_num_rows_estimate(true);
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

  @Test
  public void testHBaseScanNodeMemEstimates() {
    // Single key non-string column
    HBaseColumn intCol = new HBaseColumn("", FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY, "",
        false, Type.INT, "", 1);
    assertEquals(
        HBaseScanNode.memoryEstimateForFetchingColumns(Lists.newArrayList(intCol)), 8);

    // Single key string column without max length stat.
    HBaseColumn stringColWithoutStats = new HBaseColumn("",
        FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY, "", false, Type.STRING, "", 1);
    assertEquals(HBaseScanNode.memoryEstimateForFetchingColumns(Lists
        .newArrayList(stringColWithoutStats)), 64 * 1024);

    // Single key string column with max length stat.
    HBaseColumn stringColwithSmallMaxSize = new HBaseColumn("",
        FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY, "", false, Type.STRING, "", 1);
    stringColwithSmallMaxSize.getStats().update(ColumnStats.StatsKey.MAX_SIZE,
        Long.valueOf(50));
    assertEquals(HBaseScanNode.memoryEstimateForFetchingColumns(Lists
        .newArrayList(stringColwithSmallMaxSize)), 128);

    // Case that triggers the upper bound if some columns have stats are missing.
    HBaseColumn stringColwithLargeMaxSize = new HBaseColumn("",
        FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY, "", false, Type.STRING, "", 1);
    stringColwithLargeMaxSize.getStats().update(ColumnStats.StatsKey.MAX_SIZE,
        Long.valueOf(128 * 1024 * 1024));
    assertEquals(HBaseScanNode.memoryEstimateForFetchingColumns(Lists.newArrayList(
        stringColwithLargeMaxSize, stringColWithoutStats)), 128 * 1024 * 1024);

    // Single non-key non-string column.
    HBaseColumn intNonKeyCol = new HBaseColumn("", "columnFamily", "columnQualifier",
        false, Type.INT, "", 1);
    assertEquals(
        HBaseScanNode.memoryEstimateForFetchingColumns(Lists.newArrayList(intNonKeyCol)),
        64);

    // Case with a string key and non-key int column.
    assertEquals(HBaseScanNode.memoryEstimateForFetchingColumns(Lists.newArrayList(
        stringColwithLargeMaxSize, intNonKeyCol)), 512 * 1024 * 1024);

    // Case with a huge number of string columns.
    ArrayList<HBaseColumn> largeColumnList = new ArrayList<HBaseColumn>();
    largeColumnList.add(stringColwithSmallMaxSize);
    for (int i = 0; i < 100; i++) largeColumnList.add(stringColWithoutStats);
    assertEquals(HBaseScanNode.memoryEstimateForFetchingColumns(largeColumnList),
        8 * 1024 * 1024);
  }

  /**
   * Verify that various expected-result filters work on a
   * variety of sample input lines.
   */
  @Test
  public void testFilters() {
    IgnoreValueFilter filter = TestUtils.CARDINALITY_FILTER;
    assertEquals(" foo=bar cardinality=",
        filter.transform(" foo=bar cardinality=10"));
    assertEquals(" foo=bar cardinality=",
        filter.transform(" foo=bar cardinality=10.3K"));
    assertEquals(" foo=bar cardinality=",
        filter.transform(" foo=bar cardinality=unavailable"));
    filter = TestUtils.ROW_SIZE_FILTER;
    assertEquals(" row-size= cardinality=10.3K",
        filter.transform(" row-size=10B cardinality=10.3K"));
  }

  @Test
  public void testScanNodeFsScheme() {
    addTestTable("CREATE TABLE abfs_tbl (col int) LOCATION "
        + "'abfs://dummy-fs@dummy-account.dfs.core.windows.net/abfs_tbl'");
    addTestTable("CREATE TABLE abfss_tbl (col int) LOCATION "
        + "'abfss://dummy-fs@dummy-account.dfs.core.windows.net/abfs_tbl'");
    addTestTable("CREATE TABLE adl_tbl (col int) LOCATION "
        + "'adl://dummy-account.azuredatalakestore.net/adl_tbl'");
    addTestTable("CREATE TABLE s3a_tbl (col int) LOCATION "
        + "'s3a://dummy-bucket/s3_tbl'");
    runPlannerTestFile(
        "scan-node-fs-scheme", ImmutableSet.of(PlannerTestOption.VALIDATE_SCAN_FS));
  }
}
