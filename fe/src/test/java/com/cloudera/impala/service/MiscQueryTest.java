// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;
import java.util.List;
import org.junit.Test;

public class MiscQueryTest extends BaseQueryTest {
  @Test
  public void TestMisc() {
    runTestInExecutionMode(EXECUTION_MODE, "misc", false, 1000);
    List<TestConfiguration> testConfigs = generateAllConfigurationPermutations(
        SEQUENCE_FORMAT_ONLY, ALL_COMPRESSION_FORMATS, SMALL_BATCH_SIZES,
        SMALL_CLUSTER_SIZES, ALL_LLVM_OPTIONS);
    runQueryWithTestConfigs(testConfigs, "misc", true, 0);

  }
}
