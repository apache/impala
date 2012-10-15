// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.dataerror;

import java.util.List;

import org.junit.Test;

import com.cloudera.impala.service.BaseQueryTest;
import com.google.common.collect.Lists;

public class DataErrorsTest extends BaseQueryTest {
  public DataErrorsTest() {
    super("functional-query/queries/DataErrorsTest");
  }

  @Test
  public void TestHdfsScanNodeErrors() {
    // IMP-259: Trevni has no errors in the data
    List<TableFormat> all_format_except_trevni = Lists.newArrayList();
    all_format_except_trevni.addAll(ALL_TABLE_FORMATS);
    all_format_except_trevni.remove(TableFormat.TREVNI);
    runPairTestFile("hdfs-scan-node-errors", true, 10, all_format_except_trevni,
        ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);

    // IMP-250: can't use num_nodes=2
    // IMP-251: max_errors doesn't really have any effect because we cannot retrieve
    // any conversion error at this moment.
    runPairTestFile("hdfs-scan-node-errors", false, 100,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);
    runPairTestFile("hdfs-scan-node-errors", false, 5,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);
  }

  @Test
  public void TestHBaseScanNodeErrors() {
    runPairTestFile("hbase-scan-node-errors", false, 100,
        TEXT_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);
    runPairTestFile("hbase-scan-node-errors", false, 5,
        TEXT_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);
    runPairTestFile("hbase-scan-node-errors", true, 10,
        TEXT_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);
  }

  @Test
  public void TestSequnceNodeErrors() {
    List<TestConfiguration> testConfigs = generateAllConfigurationPermutations(
        SEQUENCE_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES,
        SMALL_CLUSTER_SIZES, ALL_LLVM_OPTIONS);

    // TODO: Need to set the read size to 10240 to hit a SYNC split across buffers.
    // This must be done when the impala server is started so either this needs
    // to run on its own server or all tests need to run with that size.
    runQueryWithTestConfigs(testConfigs, "hdfs-sequence-scan-errors", false, 20);
  }
}
