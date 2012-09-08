// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;
import org.junit.Test;

public class MiscQueryTest extends BaseQueryTest {
  @Test
  public void TestMisc() {
    runTestInExecutionMode(EXECUTION_MODE, "misc", false, 1000);
  }
}
