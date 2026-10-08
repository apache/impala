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

package org.apache.impala.planner;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.impala.catalog.SideloadTableStats;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestFileParser;
import org.apache.impala.testutil.TestFileParser.TestCase;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExecutorGroupSet;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Plans the TPC-DS queries of tpcds_cpu_cost with the 3TB table stats side-loaded and
 * COMPUTE_PROCESSING_COST=true on a default executor group of 10 executors with
 * NUM_CORES cores each, as reported by the backend. No request pool caps the cores
 * per node, so the plans must not assume more cores than the executors have.
 */
public class ProcessingCostDefaultGroupCoresTest extends PlannerTestBase {
  private static final String TEST_DB = "tpcds_partitioned_parquet_snap";
  private static final int NUM_EXECUTORS = 10;
  private static final int NUM_CORES = 16;

  private static Map<String, Map<String, SideloadTableStats>> sideloadStats_;

  // Not named setUp() so that PlannerTestBase.setUp() still runs first.
  @BeforeClass
  public static void setUpClusterAndStats() {
    TExecutorGroupSet groupSet = new TExecutorGroupSet();
    groupSet.setCurr_num_executors(NUM_EXECUTORS);
    groupSet.setExpected_num_executors(NUM_EXECUTORS);
    groupSet.setExec_group_name_prefix("");
    groupSet.setNum_cores_per_executor(NUM_CORES);
    TUpdateExecutorMembershipRequest updateReq = new TUpdateExecutorMembershipRequest();
    updateReq.setIp_addresses(Sets.newHashSet("127.0.0.1"));
    updateReq.setHostnames(Sets.newHashSet("localhost"));
    updateReq.setExec_group_sets(new ArrayList<>());
    updateReq.getExec_group_sets().add(groupSet);
    ExecutorMembershipSnapshot.update(updateReq);

    RuntimeEnv.INSTANCE.setTestEnv(true);
    sideloadStats_ = loadStatsJson("tpcds_cpu_cost/stats-3TB.json");
    RuntimeEnv.INSTANCE.setSideloadStats(sideloadStats_);
    invalidateTables();
  }

  @AfterClass
  public static void dropStats() {
    RuntimeEnv.INSTANCE.dropSideloadStats();
    invalidateTables();
    // Restore the cluster of PlannerTestBase.setUp().
    setUpTestCluster(3, 20, "");
  }

  /** Invalidates the tables so that they are reloaded with the current stats. */
  private static void invalidateTables() {
    for (String db : sideloadStats_.keySet()) {
      for (String table : sideloadStats_.get(db).keySet()) {
        catalog_.getSrcCatalog().invalidateTableIfExists(TEST_DB, table);
      }
    }
  }

  private List<TestCase> testCases() throws IOException {
    // TestFileParser resolves file names against the workloads directory.
    Path workloadsDir = Paths.get(System.getenv("IMPALA_HOME"), "testdata/workloads");
    List<Path> files;
    try (Stream<Path> paths =
        Files.list(workloadsDir.resolve(getTestDir()).resolve("tpcds_cpu_cost"))) {
      files = paths.filter(p -> p.getFileName().toString().matches("tpcds-q.*\\.test"))
          .map(workloadsDir::relativize)
          .sorted()
          .collect(Collectors.toList());
    }
    List<TestCase> testCases = new ArrayList<>();
    for (Path file : files) {
      TestFileParser parser = new TestFileParser(file.toString(), defaultQueryOptions());
      parser.parseFile();
      testCases.addAll(parser.getTestCases());
    }
    return testCases;
  }

  @Test
  public void testFragmentInstancesPerHost() throws Exception {
    List<String> violations = new ArrayList<>();
    for (TestCase testCase : testCases()) {
      TQueryCtx queryCtx =
          TestUtils.createQueryContext(TEST_DB, System.getProperty("user.name"));
      TQueryOptions options = defaultQueryOptions();
      options.setCompute_processing_cost(true);
      // In the test environment, ENABLE_REPLAN replaces the default group with test
      // groups. Disable it to plan for the default group as on a real cluster.
      options.setEnable_replan(false);
      queryCtx.client_request.setQuery_options(options);
      queryCtx.client_request.setStmt(testCase.getQuery());
      PlanCtx planCtx = new PlanCtx(queryCtx);
      planCtx.disableDescTblSerialization();
      planCtx.requestPlanCapture();
      TExecRequest req = frontend_.createExecRequest(planCtx);
      String where = testCase.getFileNameAndLineNum();
      int maxParallelism = req.query_exec_request.getMax_parallelism_per_node();
      if (maxParallelism > NUM_CORES) {
        violations.add(where + ": max_parallelism_per_node=" + maxParallelism);
      }
      for (PlanFragment root : planCtx.getPlan()) {
        for (PlanFragment f : root.<PlanFragment>getNodesPostOrder()) {
          int perHost = f.getNumInstancesPerHost(options);
          if (perHost > NUM_CORES) {
            violations.add(where + ": " + f.getId() + " plans " + perHost
                + " instances per host");
          }
        }
      }
    }
    if (!violations.isEmpty()) {
      fail("Plans assume more than " + NUM_CORES + " cores per executor:\n"
          + String.join("\n", violations));
    }
  }
}
