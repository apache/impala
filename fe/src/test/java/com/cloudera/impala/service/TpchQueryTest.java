// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;
import org.junit.Test;

public class TpchQueryTest extends BaseQueryTest {

  @Test
  public void TestTpchQ1() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q1", false, 1000);
  }

  @Test
  public void TestTpchQ2() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q2", false, 1000);
  }

  @Test
  public void TestTpchQ3() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q3", false, 1000);
  }

  @Test
  public void TestTpchQ4() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q4", false, 1000);
  }

  @Test
  public void TestTpchQ5() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q5", false, 1000);
  }

  @Test
  public void TestTpchQ6() {
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
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q8", false, 1000);
  }

  @Test
  public void TestTpchQ9() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q9", false, 1000);
  }

  @Test
  public void TestTpchQ10() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q10", false, 1000);
  }

  @Test
  public void TestTpchQ11() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q11", false, 1000);
  }

  @Test
  public void TestTpchQ12() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q12", false, 1000);
  }

  @Test
  public void TestTpchQ13() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q13", false, 1000);
  }

  @Test
  public void TestTpchQ14() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q14", false, 1000);
  }

  @Test
  public void TestTpchQ15() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q15", false, 1000);
  }

  @Test
  public void TestTpchQ16() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q16", false, 1000);
  }

  @Test
  public void TestTpchQ17() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q17", false, 1000);
  }

  @Test
  public void TestTpchQ18() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q18", false, 1000);
  }

  @Test
  public void TestTpchQ19() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q19", false, 1000);
  }

  @Test
  public void TestTpchQ20() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q20", false, 1000);
  }

  @Test
  public void TestTpchQ21() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q21", false, 1000);
  }

  @Test
  public void TestTpchQ22() {
    runTestInExecutionMode(EXECUTION_MODE, "tpch-q22", false, 1000);
  }
}
