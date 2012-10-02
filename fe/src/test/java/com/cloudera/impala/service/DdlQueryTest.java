// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DdlQueryTest extends BaseQueryTest {
  @Test
  public void TestShowTables() {
    runQueryInAllBatchAndClusterPerms("show", false, 1000, TEXT_FORMAT_ONLY,
        ImmutableList.of(0), ImmutableList.of(1));    
  }

  @Test
  public void TestDescribeTable() {
    runQueryInAllBatchAndClusterPerms("describe", false, 1000, TEXT_FORMAT_ONLY,
        ImmutableList.of(0), ImmutableList.of(1));    
  }


  @Test
  public void TestUseTable() {
    runQueryInAllBatchAndClusterPerms("use", false, 1000, TEXT_FORMAT_ONLY,
        ImmutableList.of(0), ImmutableList.of(1));    
  }    
}
