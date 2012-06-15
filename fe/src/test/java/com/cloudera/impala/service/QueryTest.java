// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import org.junit.Test;

public class QueryTest extends BaseQueryTest {
  @Test
  public void TestDistinct() {
    runTestInExecutionMode(EXECUTION_MODE, "distinct", false, 1000);
  }

  @Test
  public void TestAggregation() {
    runTestInExecutionMode(EXECUTION_MODE, "aggregation", false, 1000);
  }

  @Test
  public void TestExprs() {
    runTestInExecutionMode(EXECUTION_MODE, "exprs", false, 1000);
  }

  @Test
  public void TestHdfsScanNode() {
    // Run fully distributed (nodes = 0) with all batch sizes.
    runPairTestFile("hdfs-scan-node", false, 1000,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);
    // Run other node numbers with small batch sizes on text
    runQueryInAllBatchAndClusterPerms("hdfs-scan-node", false, 1000, TEXT_FORMAT_ONLY,
        SMALL_BATCH_SIZES, SMALL_CLUSTER_SIZES);
  }

  @Test
  public void TestFilePartitions() {
    // Run fully distributed with all batch sizes.
    runPairTestFile("hdfs-partitions", false, 1000,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);

    // Run other node numbers with small batch sizes on text
    runQueryInAllBatchAndClusterPerms("hdfs-partitions", false, 1000, TEXT_FORMAT_ONLY,
        SMALL_BATCH_SIZES, SMALL_CLUSTER_SIZES);
  }

  @Test
  public void TestLimit() {
    runTestInExecutionMode(EXECUTION_MODE, "limit", false, 1000);
  }

  @Test
  public void TestTopN() {
    runTestInExecutionMode(EXECUTION_MODE, "top-n", false, 1000);
  }

  @Test
  public void TestEmpty() {
    runTestInExecutionMode(EXECUTION_MODE, "empty", false, 1000);
  }

  @Test
  public void TestSubquery() {
    runTestInExecutionMode(EXECUTION_MODE, "subquery", false, 1000);
  }

  @Test
  public void TestMixedFormat() {
    runTestInExecutionMode(EXECUTION_MODE, "mixed-format", false, 1000);
  }
}
