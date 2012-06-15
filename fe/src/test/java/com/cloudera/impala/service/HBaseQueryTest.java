// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import org.junit.Test;


public class HBaseQueryTest extends BaseQueryTest {

  @Test
  public void TestHBaseScanNode() {
    runQueryUncompressedTextOnly("hbase-scan-node", false, 1000);
  }

  @Test
  public void TestHBaseRowKeys() {
    runQueryUncompressedTextOnly("hbase-rowkeys", false, 1000);
  }

  @Test
  public void TestHBaseFilters() {
    runQueryUncompressedTextOnly("hbase-filters", false, 1000);
  }
}