// Copyright (c) 2015 Cloudera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import org.junit.Test;

import com.cloudera.impala.thrift.TQueryOptions;

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
    runPlannerTestFile("tpch-all");
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

}
