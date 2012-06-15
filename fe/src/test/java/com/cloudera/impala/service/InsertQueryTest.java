// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class InsertQueryTest extends BaseQueryTest {
  @Test
  public void TestInsert() {
    // TODO: Currently the INSERT tests only support running against an in-process
    // query executor environment.
    Assume.assumeTrue(getTargetTestEnvironment() == TargetTestEnvironment.IN_PROCESS);

    runQueryInAllBatchAndClusterPerms("insert", false, 1000, null,
        ImmutableList.of(0), ImmutableList.of(1));
  }

  //TODO - see hdfs-text-scanner.cc for what needs to be done to support NULL partition
  //keys.
  //@Test
  //public void TestInsertNulls() {
  //  runQueryTestFile("insert-nulls", false, 1000);
  //}
}