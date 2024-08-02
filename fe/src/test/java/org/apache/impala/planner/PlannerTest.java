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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.HBaseColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.datagenerator.HBaseTestDataRegionAssignment;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.testutil.TestUtils.IgnoreValueFilter;
import org.apache.impala.thrift.TRuntimeFilterType;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TJoinDistributionMode;
import org.apache.impala.thrift.TKuduReplicaSelection;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// All planner tests, except for S3 specific tests should go here.
public class PlannerTest extends PlannerTestBase {

  @BeforeClass
  public static void setUp() throws Exception {
    PlannerTestBase.setUp();
    // Rebalance the HBase tables. This is necessary because some tests rely on HBase
    // tables being arranged in a deterministic way. See IMPALA-7061 for details.

    HBaseTestDataRegionAssignment assignment = new HBaseTestDataRegionAssignment();
    assignment.performAssignment("functional_hbase.alltypessmall");
    assignment.performAssignment("functional_hbase.alltypesagg");
    assignment.close();
  }

  /**
   * Scan node cardinality test
   */
  @Test
  public void testScanCardinality() {
    runPlannerTestFile("card-scan",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Inner join cardinality test
   */
  @Test
  public void testInnerJoinCardinality() {
    runPlannerTestFile("card-inner-join",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Outer join cardinality test
   */
  @Test
  public void testOuterJoinCardinality() {
    runPlannerTestFile("card-outer-join",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * 3+ table join cardinality test
   */
  @Test
  public void testMultiJoinCardinality() {
    runPlannerTestFile("card-multi-join",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Cardinality for aggregations.
   */
  @Test
  public void testAggCardinality() {
    runPlannerTestFile("card-agg",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
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
  public void testConstantPropagation() {
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
  public void testGroupingSets() {
    runPlannerTestFile("grouping-sets");
  }

  @Test
  public void testAnalyticFns() {
    runPlannerTestFile("analytic-fns",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Tests for analytic functions with higher mt_dop to exercise partitioning logic that
   * depends on estimates of # of instances.
   */
  @Test
  public void testAnalyticFnsMtDop() {
    TQueryOptions options = defaultQueryOptions();
    options.setMt_dop(4);
    runPlannerTestFile("analytic-fns-mt-dop", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testAnalyticRankPushdown() {
    runPlannerTestFile("analytic-rank-pushdown");
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
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hbase_num_rows_estimate(true);
    runPlannerTestFile("hbase-no-key-est", options);
  }

  @Test
  public void testInsert() {
    runPlannerTestFile("insert");
  }

  @Test
  public void testInsertDefaultClustered() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("clustered");
    runPlannerTestFile("insert-default-clustered", options);
  }

  @Test
  public void testInsertDefaultNoClustered() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("noclustered  ");
    runPlannerTestFile("insert-default-noclustered", options);
  }

  @Test
  public void testInsertDefaultShuffle() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("shuffle");
    runPlannerTestFile("insert-default-shuffle", options);
  }

  @Test
  public void testInsertDefaultNoShuffle() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("  noshuffle ");
    runPlannerTestFile("insert-default-noshuffle", options);
  }

  @Test
  public void testInsertDefaultClusteredShuffle() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("clustered:shuffle");
    runPlannerTestFile("insert-default-clustered-shuffle", options);
  }

  @Test
  public void testInsertDefaultClusteredNoShuffle() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("clustered : noshuffle");
    runPlannerTestFile("insert-default-clustered-noshuffle", options);
  }

  @Test
  public void testInsertDefaultNoClusteredShuffle() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("  noclustered:  shuffle");
    runPlannerTestFile("insert-default-noclustered-shuffle", options);
  }

  @Test
  public void testInsertDefaultNoClusteredNoShuffle() {
    TQueryOptions options = defaultQueryOptions();
    options.setDefault_hints_insert_statement("  noclustered  :  noshuffle  ");
    runPlannerTestFile("insert-default-noclustered-noshuffle", options);
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
  public void testInsertSortByZorder() {
    // Add a test table with a SORT BY ZORDER clause to test that the corresponding sort
    // nodes are added by the insert statements in insert-sort-by.test.
    addTestDb("test_sort_by_zorder", "Test DB for SORT BY ZORDER clause.");
    addTestTable("create table test_sort_by_zorder.t (id int, int_col int, " +
        "bool_col boolean) partitioned by (year int, month int) " +
        "sort by zorder (int_col, bool_col) location '/'");
    addTestTable("create table test_sort_by_zorder.t_nopart (id int, int_col int, " +
        "bool_col boolean) sort by zorder (int_col, bool_col) location '/'");
    runPlannerTestFile("insert-sort-by-zorder", "test_sort_by_zorder");
  }

  @Test
  public void testHdfsInsertWriterLimit() {
    addTestDb("test_hdfs_insert_writer_limit",
        "Test DB for MAX_FS_WRITERS query option.");
    addTestTable( "create table test_hdfs_insert_writer_limit.partitioned_table "
        + "(id int) partitioned by (year int, month int) location '/'");
    addTestTable("create table test_hdfs_insert_writer_limit.unpartitioned_table"
        + " (id int) location '/'");
    runPlannerTestFile("insert-hdfs-writer-limit", "test_hdfs_insert_writer_limit",
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  @Test
  public void testHdfs() {
    runPlannerTestFile("hdfs");
  }

  @Test
  public void testNestedCollections() {
    TQueryOptions options = new TQueryOptions();
    options.setMinmax_filter_sorted_columns(false);
    runPlannerTestFile("nested-collections", options);
  }

  @Test
  public void testComplexTypesFileFormats() {
    runPlannerTestFile("complex-types-file-formats");
  }

  @Test
  public void testZippingUnnest() {
    addTestDb("test_zipping_unnest_db", "For creating views for zipping unnest queries.");
    addTestView("create view test_zipping_unnest_db.view_arrays as " +
        "select id, arr1, arr2 from functional_parquet.complextypes_arrays");
    runPlannerTestFile("zipping-unnest");
  }

  @Test
  public void testJoins() {
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(false);
    // Skip cardinality validation because some tables do not have stats
    // and estimated file sizes are non-deterministic.
    runPlannerTestFile("joins-hdfs-num-rows-est-enabled", options);
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
    runPlannerTestFile("fk-pk-join-detection-hdfs-num-rows-est-enabled", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  @Test
  public void testFkPkJoinDetectionWithHDFSNumRowsEstDisabled() {
    // The FK/PK detection result is included in EXTENDED or higher.
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(true);
    options.setMinmax_filter_threshold(0.0);
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

  /**
   * Tests for the IMPALA-1270 optimization of automatically adding a distinct
   * agg to semi joins.
   */
  @Test
  public void testSemiJoinDistinct() {
    runPlannerTestFile("semi-join-distinct");
  }

  @Test
  public void testUnion() {
    runPlannerTestFile("union");
  }

  @Test
  public void testSetOperationRewrite() {
    TQueryOptions options = defaultQueryOptions();
    options.setMinmax_filter_threshold(0.0);
    runPlannerTestFile("setoperation-rewrite", options);
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
    runPlannerTestFile("partition-key-scans", options, ImmutableSet.of(
            PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test partition key scans with the option disabled - backend support is
   * used to only return one row per file.
   */
  @Test
  public void testPartitionKeyScansDefault() {
    TQueryOptions options = new TQueryOptions();
    runPlannerTestFile("partition-key-scans-default", options, ImmutableSet.of(
            PlannerTestOption.VALIDATE_CARDINALITY));
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
    TQueryOptions options = new TQueryOptions();
    options.setMinmax_filter_sorted_columns(false);
    runPlannerTestFile("tpch-nested", "tpch_nested_parquet", options,
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES,
            PlannerTestOption.VALIDATE_CARDINALITY));
  }

  @Test
  public void testSmallQueryOptimization() {
    TQueryOptions options = new TQueryOptions();
    options.setExec_single_node_rows_threshold(8);
    addTestDb("kudu_planner_test", "Test DB for Kudu Planner.");
    addTestTable("CREATE EXTERNAL TABLE kudu_planner_test.no_stats STORED AS KUDU "
        + "TBLPROPERTIES ('kudu.table_name' = 'impala::functional_kudu.alltypes');");
    runPlannerTestFile("small-query-opt", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
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
    options.setMem_limit(5000);
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
  public void testDisableRuntimeOverlapFilter() {
    TQueryOptions options = new TQueryOptions();
    options.setMinmax_filter_threshold(0.0);
    runPlannerTestFile("disable-runtime-overlap-filter", options);

    options.setMinmax_filter_threshold(1.0);
    options.unsetEnabled_runtime_filter_types();
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.BLOOM);
    runPlannerTestFile("disable-runtime-overlap-filter", options);
  }

  @Test
  public void testRuntimeFilterQueryOptions() {
    runPlannerTestFile("runtime-filter-query-options",
        ImmutableSet.of(
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  @Test
  public void testBloomFilterAssignment() {
    TQueryOptions options = defaultQueryOptions();
    options.setMinmax_filter_sorted_columns(false);
    options.setMinmax_filter_partition_columns(false);
    runPlannerTestFile("bloom-filter-assignment",
        ImmutableSet.of(
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
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
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  @Test
  public void testParquetFilteringDisabled() {
    TQueryOptions options = new TQueryOptions();
    options.setParquet_dictionary_filtering(false);
    options.setParquet_read_statistics(false);
    runPlannerTestFile("parquet-filtering-disabled", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  @Test
  public void testKudu() {
    TQueryOptions options = defaultQueryOptions();
    options.unsetEnabled_runtime_filter_types();
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.BLOOM);
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.MIN_MAX);
    runPlannerTestFile("kudu", options);
  }

  @Test
  public void testKuduUpsert() {
    runPlannerTestFile("kudu-upsert");
  }

  @Test
  public void testKuduUpdate() {
    TQueryOptions options = defaultQueryOptions();
    options.unsetEnabled_runtime_filter_types();
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.BLOOM);
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.MIN_MAX);
    runPlannerTestFile("kudu-update", options);
  }

  @Test
  public void testKuduDelete() {
    runPlannerTestFile("kudu-delete");
  }

  @Test
  public void testKuduSelectivity() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.VERBOSE);
    runPlannerTestFile("kudu-selectivity", options);
  }

  @Test
  public void testKuduReplicaSelection() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.VERBOSE);
    options.setKudu_replica_selection(TKuduReplicaSelection.LEADER_ONLY);
    runPlannerTestFile("kudu-replica-selection-leader-only", options);

    options.setKudu_replica_selection(TKuduReplicaSelection.CLOSEST_REPLICA);
    runPlannerTestFile("kudu-replica-selection-closest-replica", options);
  }

  @Test
  public void testKuduTpch() {
    TQueryOptions options = defaultQueryOptions();
    options.unsetEnabled_runtime_filter_types();
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.BLOOM);
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.MIN_MAX);
    runPlannerTestFile("tpch-kudu", options,
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testKuduStatsAgg() {
    runPlannerTestFile("kudu-stats-agg");
  }

  @Test
  public void testKuduDmlWithUtcConversion() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.VERBOSE);
    options.setWrite_kudu_utc_timestamps(true);
    // convert_kudu_utc_timestamps is not really needed for the planner test, but it would
    // be critical for update/delete if the queries were actually executed.
    options.setConvert_kudu_utc_timestamps(true);
    runPlannerTestFile("kudu-dml-with-utc-conversion", options);
  }

  @Test
  public void testMtDopValidation() {
    // Tests that queries planned with mt_dop > 0 produce a parallel plan.
    // Since IMPALA-9812 was fixed all plans are supported. Previously some plans
    // were rejected.
    TQueryOptions options = defaultQueryOptions();
    options.setMt_dop(3);
    options.setDisable_hdfs_num_rows_estimate(true);
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("mt-dop-validation", options);
  }

  @Test
  public void testComputeStatsMtDop() {
    for (int mtDop: new int[] {-1, 0, 1, 16}) {
      int effectiveMtDop = (mtDop != -1) ? mtDop : 0;
      // MT_DOP is not set automatically for stmt other than COMPUTE STATS.
      testEffectiveMtDop(
          "select * from functional_parquet.alltypes", mtDop, effectiveMtDop);

      // MT_DOP is set automatically for COMPUTE STATS, but can be overridden by a
      // user-provided MT_DOP.
      int computeStatsEffectiveMtDop = (mtDop != -1) ? mtDop : 4;
      testEffectiveMtDop(
          "compute stats functional_parquet.alltypes", mtDop, computeStatsEffectiveMtDop);
      testEffectiveMtDop(
          "compute stats functional.alltypes", mtDop, computeStatsEffectiveMtDop);
      testEffectiveMtDop(
          "compute stats functional_kudu.alltypes", mtDop, computeStatsEffectiveMtDop);
    }
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
  public void testOnlyNeededStructFieldsMaterialized() throws ImpalaException {
    // Tests that if a struct is selected in an inline view but only a subset of its
    // fields are selected in the top level query, only the selected fields are
    // materialised, not the whole struct.
    // Also tests that selecting the whole struct or fields from an inline view or
    // directly from the table give the same row size.

    TQueryOptions queryOpts = defaultQueryOptions();

    String queryWholeStruct =
        "select outer_struct from functional_orc_def.complextypes_nested_structs";
    int rowSizeWholeStruct = getRowSize(queryWholeStruct, queryOpts);

    String queryWholeStructFromInlineView =
        "with sub as (" +
          "select id, outer_struct from functional_orc_def.complextypes_nested_structs)" +
        "select sub.outer_struct from sub";
    int rowSizeWholeStructFromInlineView = getRowSize(
        queryWholeStructFromInlineView, queryOpts);

    String queryOneField =
        "select outer_struct.str from functional_orc_def.complextypes_nested_structs";
    int rowSizeOneField = getRowSize(queryOneField, queryOpts);

    String queryOneFieldFromInlineView =
        "with sub as (" +
          "select id, outer_struct from functional_orc_def.complextypes_nested_structs)" +
        "select sub.outer_struct.str from sub";
    int rowSizeOneFieldFromInlineView = getRowSize(
        queryOneFieldFromInlineView, queryOpts);

    String queryTwoFields =
        "select outer_struct.str, outer_struct.inner_struct1 " +
        "from functional_orc_def.complextypes_nested_structs";
    int rowSizeTwoFields = getRowSize(queryTwoFields, queryOpts);

    String queryTwoFieldsFromInlineView =
        "with sub as (" +
          "select id, outer_struct from functional_orc_def.complextypes_nested_structs)" +
        "select sub.outer_struct.str, sub.outer_struct.inner_struct1 from sub";
    int rowSizeTwoFieldsFromInlineView = getRowSize(
        queryTwoFieldsFromInlineView, queryOpts);

    Assert.assertEquals(rowSizeWholeStruct, rowSizeWholeStructFromInlineView);
    Assert.assertEquals(rowSizeOneField, rowSizeOneFieldFromInlineView);
    Assert.assertEquals(rowSizeTwoFields, rowSizeTwoFieldsFromInlineView);

    Assert.assertTrue(rowSizeOneField < rowSizeTwoFields);
    Assert.assertTrue(rowSizeTwoFields < rowSizeWholeStruct);
  }

  @Test
  public void testStructFieldSlotSharedWithStruct() throws ImpalaException {
    // Tests that in the case where both a struct and some of its fields are present in
    // the select list, no extra slots are generated in the row for the struct fields but
    // the memory of the struct is reused, i.e. the row size is the same as when only the
    // struct is queried.
    TQueryOptions queryOpts = defaultQueryOptions();
    String queryTemplate =
        "select %s from functional_orc_def.complextypes_nested_structs";

    // The base case is when the top-level struct is selected.
    String queryWithoutFields =
        String.format(queryTemplate, "outer_struct");
    int rowSizeWithoutFields = getRowSize(queryWithoutFields, queryOpts);

    // Try permutations of (nested) fields of the top-level struct.
    String[] fields = {"outer_struct", "outer_struct.str", "outer_struct.inner_struct3",
      "outer_struct.inner_struct3.s"};
    Collection<List<String>> permutations =
      Collections2.permutations(java.util.Arrays.asList(fields));
    for (List<String> permutation : permutations) {
      String query = String.format(queryTemplate, String.join(", ", permutation));
      int rowSize = getRowSize(query, queryOpts);
      Assert.assertEquals(rowSizeWithoutFields, rowSize);
    }
  }

  @Test
  public void testStructFieldSlotSharedWithStructFromStarExpansion()
      throws ImpalaException {
    // Like testStructFieldSlotSharedWithStruct(), but involving structs that come from a
    // star expansion.

    TQueryOptions queryOpts = defaultQueryOptions();
    // Enable star-expandion of complex types.
    queryOpts.setExpand_complex_types(true);

    String queryTemplate =
        "select %s from functional_orc_def.complextypes_nested_structs";

    // The base case is when only the star is given in the select list.
    String queryWithoutFields =
        String.format(queryTemplate, "*");
    int rowSizeWithoutFields = getRowSize(queryWithoutFields, queryOpts);

    // Try permutations of (nested) fields of the top-level struct.
    String[] fields = {"*", "outer_struct", "outer_struct.inner_struct1",
      "outer_struct.inner_struct1.str"};
    Collection<List<String>> permutations =
      Collections2.permutations(java.util.Arrays.asList(fields));
    for (List<String> permutation : permutations) {
      String query = String.format(queryTemplate, String.join(", ", permutation));
      int rowSize = getRowSize(query, queryOpts);
      Assert.assertEquals(rowSizeWithoutFields, rowSize);
    }
  }

  @Test
  public void testResourceRequirements() {
    // Tests the resource requirement computation from the planner.
    TQueryOptions options = defaultQueryOptions();
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    options.setMinmax_filter_threshold(0.0);
    // Required so that output doesn't vary by whether parquet tables are used or not.
    options.setDisable_hdfs_num_rows_estimate(true);
    runPlannerTestFile("resource-requirements", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.INCLUDE_EXPLAIN_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testSpillableBufferSizing() {
    // Tests the resource requirement computation from the planner when it is allowed to
    // vary the spillable buffer size.
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    options.setMinmax_filter_threshold(0.0);
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
    // Required so that output doesn't vary by the format of the table used.
    options.setMinmax_filter_threshold(0.0);
    options.setMax_row_size(8L * 1024L * 1024L);
    runPlannerTestFile("max-row-size", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
          PlannerTestOption.INCLUDE_EXPLAIN_HEADER,
          PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testSortExprMaterialization() {
    addTestFunction("TestFn", Lists.newArrayList(Type.DOUBLE), false);
    // Avoid conversion of RANK()/ROW_NUMBER() predicates to Top-N limits, which
    // would interfere with the purpose of this test.
    TQueryOptions options = defaultQueryOptions();
    options.setAnalytic_rank_pushdown_threshold(0);
    runPlannerTestFile("sort-expr-materialization", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN));
  }

  @Test
  public void testTableSample() {
    TQueryOptions options = defaultQueryOptions();
    runPlannerTestFile("tablesample", options,
        ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS,
            PlannerTestOption.VALIDATE_ICEBERG_SNAPSHOT_IDS));
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
    options.unsetEnabled_runtime_filter_types();
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.MIN_MAX);
    runPlannerTestFile("min-max-runtime-filters-hdfs-num-rows-est-enabled", options,
        ImmutableSet.of(
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  @Test
  public void testMinMaxRuntimeFiltersWithHDFSNumRowsEstDisabled() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setDisable_hdfs_num_rows_estimate(true);
    options.setMinmax_filter_partition_columns(false);
    options.unsetEnabled_runtime_filter_types();
    options.addToEnabled_runtime_filter_types(TRuntimeFilterType.MIN_MAX);
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
    stringColwithSmallMaxSize.getStats().update(
        Type.STRING, ColumnStats.StatsKey.MAX_SIZE, Long.valueOf(50));
    assertEquals(HBaseScanNode.memoryEstimateForFetchingColumns(Lists
        .newArrayList(stringColwithSmallMaxSize)), 128);

    // Case that triggers the upper bound if some columns have stats are missing.
    HBaseColumn stringColwithLargeMaxSize = new HBaseColumn("",
        FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY, "", false, Type.STRING, "", 1);
    stringColwithLargeMaxSize.getStats().update(
        Type.STRING, ColumnStats.StatsKey.MAX_SIZE, Long.valueOf(128 * 1024 * 1024));
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
    assertEquals(" foo=bar cardinality=",
        filter.transform(" foo=bar cardinality=1.58K(filtered from 2.88M)"));
    filter = TestUtils.ROW_SIZE_FILTER;
    assertEquals(" row-size= cardinality=10.3K",
        filter.transform(" row-size=10B cardinality=10.3K"));
    filter = TestUtils.PARTITIONS_FILTER;
    assertEquals(" partitions: 0/24 rows=",
        filter.transform(" partitions: 0/24 rows=10.3K"));
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

  /**
   * Validate the resource requirements of the PLAN-ROOT SINK when result spooling is
   * enabled.
   */
  @Test
  public void testResultSpooling() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setSpool_query_results(true);
    options.setNum_scanner_threads(1); // Required so that output doesn't vary by machine
    runPlannerTestFile(
        "result-spooling", options, ImmutableSet.of(PlannerTestOption.EXTENDED_EXPLAIN,
                                        PlannerTestOption.INCLUDE_RESOURCE_HEADER,
                                        PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Test
  public void testBroadcastBytesLimit() {
    TQueryOptions options = new TQueryOptions();
    // broadcast limit is smaller than the build side of hash join, so we should
    // NOT pick broadcast unless it is overridden through a join hint
    options.setBroadcast_bytes_limit(100);
    runPlannerTestFile("broadcast-bytes-limit", "tpch_parquet", options);

    // broadcast limit is larger than the build side of hash join, so we SHOULD
    // pick broadcast (i.e verify the standard case)
    options.setBroadcast_bytes_limit(1000000);
    runPlannerTestFile("broadcast-bytes-limit-large", "tpch_parquet", options);
  }

  /**
   * Check that planner estimates reflect the preagg bytes limit.
   */
  @Test
  public void testPreaggBytesLimit() {
    TQueryOptions options = defaultQueryOptions();
    options.setPreagg_bytes_limit(64 * 1024 * 1024);
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("preagg-bytes-limit", "tpch_parquet", options);
  }

  /**
   * Check conversion of predicates to conjunctive normal form.
   */
  @Test
  public void testConvertToCNF() {
    TQueryOptions options = defaultQueryOptions();
    options.setMinmax_filter_threshold(0.0);
    runPlannerTestFile("convert-to-cnf", "tpch_parquet", options);
  }

  /**
   * Check that ACID table scans work as expected.
   */
  @Test
  public void testAcidTableScans() {
    runPlannerTestFile("acid-scans", "functional_orc_def",
        ImmutableSet.of(
            PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  /**
   * Checks exercising predicate pushdown with Iceberg tables.
   */
  @Test
  public void testIcebergPredicates() {
    runPlannerTestFile("iceberg-predicates", "functional_parquet",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Checks exercising predicate pushdown with Iceberg tables, without predicate
   * subsetting.
   */
  @Test
  public void testDisabledIcebergPredicateSubsetting() {
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setIceberg_predicate_pushdown_subsetting(false);
    runPlannerTestFile("iceberg-predicates-disabled-subsetting", "functional_parquet",
        queryOptions, ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY,
            PlannerTestOption.VALIDATE_ICEBERG_SNAPSHOT_IDS));
  }

  /**
   * Check that Iceberg V2 table scans work as expected.
   */
  @Test
  public void testIcebergV2TableScans() {
    TQueryOptions options = defaultQueryOptions();
    options.setTimezone("UTC");
    runPlannerTestFile("iceberg-v2-tables", "functional_parquet", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY,
            PlannerTestOption.VALIDATE_ICEBERG_SNAPSHOT_IDS));
  }

  /**
   * Check that Iceberg V2 table scans work as expected with hash join
   */
  @Test
  public void testIcebergV2TableScansHashJoin() {
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_optimized_iceberg_v2_read(true);
    runPlannerTestFile("iceberg-v2-tables-hash-join", "functional_parquet", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY,
            PlannerTestOption.VALIDATE_ICEBERG_SNAPSHOT_IDS));
  }

  /**
   * Check that Iceberg V2 DELETE statements work as expected.
   */
  @Test
  public void testIcebergV2Delete() {
    runPlannerTestFile("iceberg-v2-delete", "functional_parquet",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY,
            PlannerTestOption.VALIDATE_ICEBERG_SNAPSHOT_IDS));
  }

  @Test
  public void testIcebergV2Update() {
    runPlannerTestFile("iceberg-v2-update", "functional_parquet",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test that OPTIMIZE TABLE statements on Iceberg tables work as expected.
   */
  @Test
  public void testIcebergOptimize() {
    TQueryOptions options = defaultQueryOptions();
    options.setMax_fs_writers(2);
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("iceberg-optimize", "functional_parquet", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Check that Iceberg metadata table scan plans are as expected.
   */
  @Test
  public void testIcebergMetadataTableScans() {
    runPlannerTestFile("iceberg-metadata-table-scan", "functional_parquet",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));

    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.EXTENDED);
    runPlannerTestFile("iceberg-metadata-table-joined-with-regular-table",
        "functional_parquet", options, ImmutableSet.of(
        PlannerTestOption.DO_NOT_VALIDATE_ROWCOUNT_ESTIMATION_FOR_PARTITIONS));
  }

  /**
   * Test limit pushdown into analytic sort in isolation.
   */
  @Test
  public void testLimitPushdownAnalytic() {
    // The partitioned top-n optimization interacts with limit pushdown. We run the
    // basic limit pushdown tests with it disabled.
    TQueryOptions options = defaultQueryOptions();
    options.setAnalytic_rank_pushdown_threshold(0);
    runPlannerTestFile("limit-pushdown-analytic", options);
  }

  /**
   * Test limit pushdown into analytic sort with the partitioned top-n transformation
   * also enabled.
   */
  @Test
  public void testLimitPushdownPartitionedTopN() {
    TQueryOptions options = defaultQueryOptions();
    options.setDisable_hdfs_num_rows_estimate(true); // Needed to test IMPALA-11443.
    runPlannerTestFile("limit-pushdown-partitioned-top-n", options);
  }

  /**
   * Test outer join simplification.
   */
  @Test
  public void testSimplifyOuterJoins() {
    TQueryOptions options = new TQueryOptions();
    options.setEnable_outer_join_to_inner_transformation(true);
    runPlannerTestFile("outer-to-inner-joins", options,
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test simple limit optimization
   */
  @Test
  public void testSimpleLimitOptimization() {
    TQueryOptions options = new TQueryOptions();
    options.setOptimize_simple_limit(true);
    runPlannerTestFile("optimize-simple-limit", options);
  }

  /**
   * Test the distribution method for a join
   */
  @Test
  public void testDistributionMethod() {
    runPlannerTestFile("tpcds-dist-method", "tpcds");
  }

  @Test
  public void testOrcStatsAgg() {
    runPlannerTestFile("orc-stats-agg");
  }

  /**
   * Test new hint of 'TABLE_NUM_ROWS'
   */
  @Test
  public void testTableCardinalityHint() {
    runPlannerTestFile("table-cardinality-hint",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test EXPLAIN_LEVEL=VERBOSE is displayed properly with MT_DOP>0
   */
  @Test
  public void testExplainVerboseMtDop() {
    runPlannerTestFile("explain-verbose-mt_dop", "tpcds_parquet",
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER));
  }

  /**
   * Test that processing cost can adjust effective instance count of fragment.
   */
  @Test
  public void testProcessingCost() {
    TQueryOptions options = tpcdsParquetQueryOptions();
    options.setCompute_processing_cost(true);
    options.setProcessing_cost_min_threads(2);
    options.setMax_fragment_instances_per_node(16);
    runPlannerTestFile("tpcds-processing-cost", "tpcds_partitioned_parquet_snap", options,
        tpcdsParquetTestOptions());
  }

  /**
   * Test that shows query plan for the same test cases at EE test
   * test_processing_cost.py::TestProcessingCost::test_admission_slots.
   */
  @Test
  public void testProcessingCostPlanAdmissionSlots() {
    TQueryOptions options = tpcdsParquetQueryOptions();
    runPlannerTestFile("processing-cost-plan-admission-slots",
        "tpcds_partitioned_parquet_snap", options, tpcdsParquetTestOptions());
  }

  /**
   * Test SELECTIVITY hints
   */
  @Test
  public void testPredicateSelectivityHints() {
    runPlannerTestFile("predicate-selectivity-hint",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test that memory estimate of aggregation node equals to the default estimation
   * (with NDV multiplication method) if AGG_MEM_CORRELATION_FACTOR=0.0 or
   * LARGE_AGG_MEM_THRESHOLD is less than or equal to zero.
   */
  @Test
  public void testAggNodeMaxMemEstimate() {
    Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();
    TQueryOptions options = tpcdsParquetQueryOptions();
    options.setAgg_mem_correlation_factor(0.0);
    options.setLarge_agg_mem_threshold(512 * 1024 * 1024);
    runPlannerTestFile(
        "agg-node-max-mem-estimate", "tpcds_parquet", options, testOptions);

    options.setAgg_mem_correlation_factor(0.5);
    options.setLarge_agg_mem_threshold(0);
    runPlannerTestFile(
        "agg-node-max-mem-estimate", "tpcds_parquet", options, testOptions);
  }

  /**
   * Test that high AGG_MEM_CORRELATION_FACTOR results in low memory estimate
   * for aggregation node.
   */
  @Test
  public void testAggNodeLowMemEstimate() {
    Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();
    TQueryOptions options = tpcdsParquetQueryOptions();
    options.setAgg_mem_correlation_factor(0.9);
    runPlannerTestFile(
        "agg-node-low-mem-estimate", "tpcds_parquet", options, testOptions);
  }

  /**
   * Test that low AGG_MEM_CORRELATION_FACTOR results in high memory estimate
   * for aggregation node.
   */
  @Test
  public void testAggNodeHighMemEstimate() {
    Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();
    TQueryOptions options = tpcdsParquetQueryOptions();
    options.setAgg_mem_correlation_factor(0.1);
    runPlannerTestFile(
        "agg-node-high-mem-estimate", "tpcds_parquet", options, testOptions);
  }

  /**
   * Test cardinality reduction by runtime filter.
   */
  @Test
  public void testRuntimeFilterCardinalityReduction() {
    runPlannerTestFile("runtime-filter-cardinality-reduction", "tpcds_parquet",
        ImmutableSet.of(
            PlannerTestOption.EXTENDED_EXPLAIN, PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test cardinality reduction by runtime filter against Kudu.
   */
  @Test
  public void testRuntimeFilterCardinalityReductionOnKudu() {
    runPlannerTestFile("runtime-filter-cardinality-reduction-on-kudu", "tpch_kudu",
        ImmutableSet.of(PlannerTestOption.VALIDATE_CARDINALITY));
  }

  /**
   * Test that how the different equality delete field ID lists are ordered for reading.
   */
  @Test
  public void testEqualityDeleteFieldIdOrdering() {
    Map<List<Integer>, Long> inp = Maps.newHashMap();
    inp.put(Lists.newArrayList(2, 3), 2L);
    inp.put(Lists.newArrayList(1, 2, 3), 2L);
    inp.put(Lists.newArrayList(1, 2), 2L);
    inp.put(Lists.newArrayList(4), 3L);
    inp.put(Lists.newArrayList(1), 3L);
    Assert.assertEquals(
        Lists.newArrayList(
            Lists.newArrayList(1),
            Lists.newArrayList(4),
            Lists.newArrayList(1, 2, 3),
            Lists.newArrayList(1, 2),
            Lists.newArrayList(2, 3)),
        IcebergScanPlanner.getOrderedEqualityFieldIds(inp));
  }

  /**
   * Performance test of planning for a query with a large number of expressions,
   * as commonly seen in auto-generated queries.
   */
  @Test
  public void testManyExpressionPerformance() {
    addTestDb("test_many_expressions", "Test DB for many-expression perf test.");
    addTestTable("create table test_many_expressions.i6bd46c0a (i20868d03 bigint, " +
        "i89d57f06 bigint, idc633f9e bigint, i9e32db9e bigint, ic02dc181 date, " +
        "ia74a4bc0 string, i3ee20502 bigint, ic05c995e bigint, i49dcf5c5 string, " +
        "i99bd997e string, i510ab56c string, i11be1ec1 bigint, i81012615 bigint) " +
        "location '/'");
    runPlannerTestFile("many-expression", "test_many_expressions");
  }

  /**
   * Test that runtime filters with the same expression repeated multiple times are
   * planned successfully (IMPALA-13270).
   */
  @Test
  public void testRuntimeFilterRepeatedExpr() {
    runPlannerTestFile("runtime-filter-repeated-expr", "functional");
  }
}
