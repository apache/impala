// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;
import org.junit.Assume;
import org.junit.Test;

public class TpchQueryTest extends BaseQueryTest {

  @Test
  public void TestTpchQ1() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q1", false, 1000);
  }

  @Test
  public void TestTpchQ2() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q2", false, 1000);
  }

  @Test
  public void TestTpchQ3() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q3", false, 1000);
  }

  @Test
  public void TestTpchQ4() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q4", false, 1000);
  }

  @Test
  public void TestTpchQ5() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q5", false, 1000);
  }

  @Test
  public void TestTpchQ6() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q6", false, 1000);
  }

  // TODO: We don't currently support some of the features in Query 7.
  // Please see IMP-128
  //@Test
  //public void TestTpchQ7() {
    //runTestInExecutionMode(EXECUTION_MODE, "tpch-q7", false, 1000);
  //}

  @Test
  public void TestTpchQ8() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q8", false, 1000);
  }

  @Test
  public void TestTpchQ9() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q9", false, 1000);
  }

  @Test
  public void TestTpchQ10() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q10", false, 1000);
  }

  @Test
  public void TestTpchQ11() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q11", false, 1000);
  }

  @Test
  public void TestTpchQ12() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q12", false, 1000);
  }

  @Test
  public void TestTpchQ13() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q13", false, 1000);
  }

  @Test
  public void TestTpchQ14() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q14", false, 1000);
  }

  @Test
  public void TestTpchQ15() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q15", false, 1000);
  }

  @Test
  public void TestTpchQ16() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q16", false, 1000);
  }

  @Test
  public void TestTpchQ17() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q17", false, 1000);
  }

  @Test
  public void TestTpchQ18() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q18", false, 1000);
  }

  @Test
  public void TestTpchQ19() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q19", false, 1000);
  }

  @Test
  public void TestTpchQ20() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q20", false, 1000);
  }

  @Test
  public void TestTpchQ21() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q21", false, 1000);
  }

  @Test
  public void TestTpchQ22() {
    Assume.assumeTrue(EXECUTION_MODE == TestExecMode.EXHAUSTIVE);
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q22", false, 1000);
  }
}
