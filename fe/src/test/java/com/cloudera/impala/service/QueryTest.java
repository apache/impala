// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.List;

import org.junit.Test;

import com.cloudera.impala.testutil.TestExecContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class QueryTest extends BaseQueryTest {

  @Test
  public void TestDistinct() {
    runTestInExecutionMode(EXECUTION_MODE, "distinct", true, 0);
  }

  @Test
  public void TestAggregation() {
    runTestInExecutionMode(EXECUTION_MODE, "aggregation", true, 0);
  }

  @Test
  public void TestExprs() {
    runTestInExecutionMode(EXECUTION_MODE, "exprs", true, 0);
  }

  @Test
  public void TestHdfsScanNode() {
    // Run fully distributed (nodes = 0) with all batch sizes.
    runPairTestFile("hdfs-scan-node", true, 0,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);
    // Run other node numbers with small batch sizes on text
    runQueryInAllBatchAndClusterPerms("hdfs-scan-node", true, 0, TEXT_FORMAT_ONLY,
        SMALL_BATCH_SIZES, SMALL_CLUSTER_SIZES);
  }

  @Test
  public void TestScanRange() {
    // Testing short scan ranges exercises reading to the next logical break
    // in the data.  For sequence this is to the next sync mark for text
    // it is the next end of record delimiter.
    TestExecContext execContext1 =
      new TestExecContext(1, 0, false, true, 0, 5000, false, true);
    List<TestConfiguration> testConfigs = Lists.newArrayList();
    testConfigs.add(
        new TestConfiguration(execContext1, CompressionFormat.NONE, TableFormat.TEXT));
    testConfigs.add(new TestConfiguration(execContext1,
          CompressionFormat.NONE, TableFormat.SEQUENCEFILE));
    testConfigs.add(new TestConfiguration(execContext1,
          CompressionFormat.SNAPPY, TableFormat.SEQUENCEFILE));
    runQueryWithTestConfigs(testConfigs, "hdfs-partitions", true, 0);
  }

  @Test
  public void TestFilePartitions() {
    // Run fully distributed with all batch sizes.
    runPairTestFile("hdfs-partitions", true, 0,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);
    // Run other node numbers with small batch sizes on text
    runQueryInAllBatchAndClusterPerms("hdfs-partitions", true, 0, TEXT_FORMAT_ONLY,
        SMALL_BATCH_SIZES, SMALL_CLUSTER_SIZES);
  }

  @Test
  public void TestLimit() {
    runTestInExecutionMode(EXECUTION_MODE, "limit", true, 0);
  }

  @Test
  public void TestTopN() {
    runTestInExecutionMode(EXECUTION_MODE, "top-n", true, 0);
  }

  @Test
  public void TestEmpty() {
    runTestInExecutionMode(EXECUTION_MODE, "empty", true, 0);
  }

  @Test
  public void TestSubquery() {
    runTestInExecutionMode(EXECUTION_MODE, "subquery", true, 0);
  }

  @Test
  public void TestUnion() {
    runQueryInAllBatchAndClusterPerms("union", true, 0, null,
        ImmutableList.of(0), ImmutableList.of(1));
  }

  @Test
  public void TestMixedFormat() {
    runTestInExecutionMode(EXECUTION_MODE, "mixed-format", true, 0);
  }

  @Test
  public void TestHdfsTinyScan() {
    // We use very small scan ranges to exercise corner cases in the HDFS scanner more
    // thoroughly. In particular, it will exercise:
    // 1. scan range with no tuple
    // 2. tuple that span across multiple scan ranges
    TestExecContext execContext1 =
        new TestExecContext(2, 1, true, true, 0, 1, false, false);

    // We use a very small file buffer to test the HDFS scanner init code that seeks the
    // first tuple delimiter.
    TestExecContext execContext2 =
        new TestExecContext(2, 1, true, true, 0, 5, false, false);

    List<TestConfiguration> testConfigs = Lists.newArrayList();
    testConfigs.add(
        new TestConfiguration(execContext1, CompressionFormat.NONE, TableFormat.TEXT));
    testConfigs.add(
        new TestConfiguration(execContext2, CompressionFormat.NONE, TableFormat.TEXT));

    runQueryWithTestConfigs(testConfigs, "hdfs-tiny-scan", true, 0);
  }

}
