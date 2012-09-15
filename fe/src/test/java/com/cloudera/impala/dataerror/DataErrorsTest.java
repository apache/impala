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
    runPairTestFile("hdfs-scan-node-errors", true, 1, all_format_except_trevni,
        ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, SINGLE_NODE_ONLY);

    // IMP-250: can't use num_nodes=2
    // IMP-251: max_errors doesn't really have any effect because we cannot retrieve
    // any conversion error at this moment.
    runPairTestFile("hdfs-scan-node-errors", false, 100,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, SINGLE_NODE_ONLY);
    runPairTestFile("hdfs-scan-node-errors", false, 5,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, SINGLE_NODE_ONLY);
  }

  @Test
  public void TestHBaseScanNodeErrors() {
    runPairTestFile("hbase-scan-node-errors", false, 100,
        TEXT_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, SINGLE_NODE_ONLY);
    runPairTestFile("hbase-scan-node-errors", false, 5,
        TEXT_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, SINGLE_NODE_ONLY);
    runPairTestFile("hbase-scan-node-errors", true, 1,
        TEXT_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, SINGLE_NODE_ONLY);
  }
}
