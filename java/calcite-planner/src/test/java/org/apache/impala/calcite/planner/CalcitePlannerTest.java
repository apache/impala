// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.calcite.planner;

import java.nio.file.Paths;

import org.apache.impala.planner.PlannerTestBase;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

public class CalcitePlannerTest extends PlannerTestBase {

  private final static java.nio.file.Path calciteTestDir_ =
      Paths.get("functional-planner", "queries", "PlannerTest", "calcite");

  /**
   * Test limit pushdown into analytic sort in isolation.
   */
  @Test
  public void testLimitPushdownAnalytic() {
    // The partitioned top-n optimization interacts with limit pushdown. We run the
    // basic limit pushdown tests with it disabled.
    TQueryOptions options = defaultQueryOptions();
    options.setAnalytic_rank_pushdown_threshold(0);
    options.setUse_calcite_planner(true);
    runPlannerTestFile("limit-pushdown-analytic-calcite", options);
  }

  @Test
  public void testAnalyticRankPushdown() {
    TQueryOptions options = defaultQueryOptions();
    options.setUse_calcite_planner(true);
    runPlannerTestFile("analytic-rank-pushdown-calcite", options);
  }

  @Override
  protected java.nio.file.Path getTestDir() {
    return calciteTestDir_;
  }
}
