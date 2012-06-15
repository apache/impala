// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;
import org.junit.Test;

public class JoinQueryTest extends BaseQueryTest {
  @Test
  public void TestJoins() {
    runTestInExecutionMode(EXECUTION_MODE, "joins", false, 1000);
  }

  @Test
  public void TestOuterJoins() {
    runQueryUncompressedTextOnly("outer-joins", false, 1000);
  }
}