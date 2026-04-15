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

import org.apache.impala.catalog.SideloadTableStats;
import org.apache.impala.common.ByteUnits;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.planner.PlannerTestBase;
import org.apache.impala.thrift.TPlannerType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TReplicaPreference;
import org.apache.impala.thrift.TSlotCountStrategy;
import org.apache.impala.util.RequestPoolService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for TPC-DS queries with CTE suggestions enabled. For the most part, this is a
 * copy of {@link org.apache.impala.calcite.planner.TpcdsCpuCostPlannerTest} with few
 * tweaks to trigger CTE recommendations. The test only verifies single node plans since
 * DISTRIBUTED and PARALLEL plans do not yet support CTEs.
 */
@RunWith(Parameterized.class)
public class TpcdsCtePlannerTest extends PlannerTestBase {
  // Pool definitions and includes memory resource limits, copied to a temporary file
  private static final String ALLOCATION_FILE = "fair-scheduler-3-groups.xml";

  // Contains per-pool configurations for maximum number of running queries and queued
  // requests.
  private static final String LLAMA_CONFIG_FILE = "llama-site-3-groups.xml";

  // Planner test option to run each planner test.
  private static Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();

  // Query option to run each planner test.
  private static TQueryOptions options =
      tpcdsParquetQueryOptions()
          .setCompute_processing_cost(true)
          .setMax_fragment_instances_per_node(12)
          .setReplica_preference(TReplicaPreference.REMOTE)
          .setSlot_count_strategy(TSlotCountStrategy.PLANNER_CPU_ASK)
          .setMem_estimate_scale_for_spilling_operator(1.0)
          .setPlanner_testcase_mode(true)
          .setCte_threshold(1)
          // Required so that output doesn't vary by whether scanned tables have stats &
          // numRows property or not.
          .setDisable_hdfs_num_rows_estimate(true)
          .setPlanner(TPlannerType.CALCITE)
          .setFallback_planner(TPlannerType.CALCITE)
          .setEnable_explain_calcite(true);

  // Database name to run this test.
  private static String testDb = "tpcds_partitioned_parquet_snap";

  // Map of table stats that is obtained through loadStatsJson().
  private static Map<String, Map<String, SideloadTableStats>> sideloadStats;

  // Granular scan limit that will injected into individual ScanNode of tables.
  private static Map<String, Long> scanRangeLimit = new HashMap<String, Long>() {
    {
      // split a 5752989 bytes file to 10 ranges.
      put("customer", 580 * ByteUnits.KILOBYTE);
      // split a 1218792 bytes file to 10 ranges.
      put("customer_address", 125 * ByteUnits.KILOBYTE);
      // split a 7848768 bytes file to 10 ranges.
      put("customer_demographics", 790 * ByteUnits.KILOBYTE);
      // split a 1815300 bytes file to 4 ranges.
      put("item", 500L * ByteUnits.KILOBYTE);
    }
  };

  // Temporary folder to copy admission control files into.
  // Do not annotate with JUnit @Rule because we want to keep the tempFolder the same
  // for entire lifetime of test class.
  private static TemporaryFolder tempFolder;

  /**
   * Copies a file from the classpath (e.g. from the impala-frontend tests jar) into the
   * temporary folder so it is available as a real file on disk.
   */
  private static File copyClasspathFileToTemp(String filename) throws IOException {
    File destFile = tempFolder.newFile(filename);
    final ClassLoader loader = TpcdsCpuCostPlannerTest.class.getClassLoader();
    try (InputStream in = loader.getResourceAsStream(filename)) {
      if (in == null) {
        throw new IOException("Resource not found on classpath: " + filename);
      }
      Files.copy(in, destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
    return destFile;
  }

  private static void setupAdmissionControl() throws IOException {
    // Start admission control with config file fair-scheduler-3-groups.xml
    // and llama-site-3-groups.xml
    tempFolder = new TemporaryFolder();
    tempFolder.create();
    File allocationConfFile = copyClasspathFileToTemp(ALLOCATION_FILE);
    File llamaConfFile = copyClasspathFileToTemp(LLAMA_CONFIG_FILE);
    // Intentionally mark isTest = false to cache poolService as a singleton.
    RequestPoolService poolService =
        RequestPoolService.getInstance(allocationConfFile.getAbsolutePath(),
            llamaConfFile.getAbsolutePath(), /* isTest */ false);
    poolService.start();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    // Mimic the 10 node test mini-cluster with admission control enabled.
    setupAdmissionControl();
    // Add 10 node executor group set root.large. This group set also set with
    // impala.admission-control.max-query-mem-limit.root.large = 50GB.
    setUpTestCluster(10, 10, "root.large");
    setUpKuduClientAndLogDir();
    Paths.get(outDir_.toString(), "tpcds_cte").toFile().mkdirs();

    // Sideload stats through RuntimeEnv.
    RuntimeEnv.INSTANCE.setTestEnv(true);
    sideloadStats = loadStatsJson("tpcds_cpu_cost/stats-3TB.json");
    RuntimeEnv.INSTANCE.setSideloadStats(sideloadStats);

    // Artificially split single file table into mutiple scan ranges so that the scan
    // looks like a multi-files table.
    for (Map.Entry<String, Long> entry : scanRangeLimit.entrySet()) {
      RuntimeEnv.INSTANCE.addTableScanRangeLimit(
          testDb, entry.getKey(), entry.getValue());
    }
    invalidateTables();
  }

  @AfterClass
  public static void unsetMetadataScaleAndStopPoolService() {
    RuntimeEnv.INSTANCE.dropSideloadStats();
    RuntimeEnv.INSTANCE.dropTableScanRangeLimit();
    invalidateTables();

    RequestPoolService.getInstance().stop();
    tempFolder.delete();
  }

  /**
   * Invalidate tables to reload them with new stats.
   */
  private static void invalidateTables() {
    for (String db : sideloadStats.keySet()) {
      for (String table : sideloadStats.get(db).keySet()) {
        catalog_.getSrcCatalog().invalidateTableIfExists(testDb, table);
      }
    }
  }

  private final String query;

  public TpcdsCtePlannerTest(String query) { this.query = query; }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> queries() {
    Map<Integer, List<String>> queryVariants = Map.of(
      14, List.of("q14a", "q14b"),
      23, List.of("q23a", "q23b"),
      24, List.of("q24a", "q24b"),
      // IMPALA-15243: Error while applying rule MaterializedViewFilterScanRule for Q39
      39, List.of()
    );
    List<Object[]> queries = new ArrayList<>(99);
    for (int i = 1; i <= 99; i++) {
      if (queryVariants.containsKey(i)) {
        for (String variant : queryVariants.get(i)) {
          queries.add(new Object[] {variant});
        }
      } else {
        queries.add(new Object[] {String.format("q%02d", i)});
      }
    }
    return queries;
  }

  @Test
  public void testQuery() {
    runPlannerTestFile("tpcds_cte/tpcds-" + query, testDb, options, testOptions);
  }
}
