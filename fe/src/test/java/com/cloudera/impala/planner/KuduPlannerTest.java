// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TQueryOptions;
import org.junit.Before;
import org.junit.Test;

/**
 * This class contains all plan generation related tests with Kudu as a storage backend.
 */
public class KuduPlannerTest extends PlannerTestBase {

  @Before
  public void checkKuduSupport() { TestUtils.assumeKuduIsSupported(); }

  @Test
  public void testKudu() { runPlannerTestFile("kudu"); }

  @Test
  public void testUpdate() { runPlannerTestFile("kudu-update"); }

  @Test
  public void testDelete() { runPlannerTestFile("kudu-delete"); }

  @Test
  public void testSelectivity() {
    TQueryOptions options = defaultQueryOptions();
    options.setExplain_level(TExplainLevel.VERBOSE);
    runPlannerTestFile("kudu-selectivity", options);
  }

  @Test
  public void testTpch() { runPlannerTestFile("tpch-kudu"); }
}
