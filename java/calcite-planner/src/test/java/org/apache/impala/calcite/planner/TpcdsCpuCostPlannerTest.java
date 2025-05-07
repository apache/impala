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

import com.google.common.collect.Sets;
import com.google.common.io.Files;

import org.apache.impala.catalog.SideloadTableStats;
import org.apache.impala.common.ByteUnits;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.planner.PlannerTestBase;
import org.apache.impala.thrift.TExecutorGroupSet;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TReplicaPreference;
import org.apache.impala.thrift.TSlotCountStrategy;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.impala.util.RequestPoolService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Plans from the TPC-DS qualification queries at simulated scale factor 1TB and
 * COMPUTE_PROCESSING_COST option enabled. Single node, Distributed and Parallel plans
 * are all included. Cardinality and resource checks are also performed.
 * The large scale simulation is obtained by scaling up table statistics from 1GB scale
 * TPC-DS database. The scaling is limited on partitions numRows, table numRows, numNulls,
 * numTrues, and numFalses. Number of partitions, files, total bytes, and column NDVs are
 * remain unchanged.
 */
public class TpcdsCpuCostPlannerTest extends PlannerTestBase {
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
          // Required so that output doesn't vary by whether scanned tables have stats &
          // numRows property or not.
          .setDisable_hdfs_num_rows_estimate(true)
          .setUse_calcite_planner(true);

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
   * Returns a {@link File} for the file on the classpath.
   */
  private static File getClasspathFile(String filename) throws URISyntaxException {
    return new File(
        TpcdsCpuCostPlannerTest.class.getClassLoader().getResource(filename).toURI());
  }

  private static void setupAdmissionControl() throws IOException, URISyntaxException {
    // Start admission control with config file fair-scheduler-3-groups.xml
    // and llama-site-3-groups.xml
    tempFolder = new TemporaryFolder();
    tempFolder.create();
    File allocationConfFile = tempFolder.newFile(ALLOCATION_FILE);
    Files.copy(getClasspathFile(ALLOCATION_FILE), allocationConfFile);

    File llamaConfFile = tempFolder.newFile(LLAMA_CONFIG_FILE);
    Files.copy(getClasspathFile(LLAMA_CONFIG_FILE), llamaConfFile);
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
    Paths.get(outDir_.toString(), "calcite_tpcds").toFile().mkdirs();

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

  @Test
  public void testQ1() {
    runPlannerTestFile("calcite_tpcds/tpcds-q01", testDb, options, testOptions);
  }

  @Test
  public void testQ2() {
    runPlannerTestFile("calcite_tpcds/tpcds-q02", testDb, options, testOptions);
  }

  @Test
  public void testQ3() {
    runPlannerTestFile("calcite_tpcds/tpcds-q03", testDb, options, testOptions);
  }

  @Test
  public void testQ4() {
    runPlannerTestFile("calcite_tpcds/tpcds-q04", testDb, options, testOptions);
  }

  @Test
  public void testQ5() {
    runPlannerTestFile("calcite_tpcds/tpcds-q05", testDb, options, testOptions);
  }

  @Test
  public void testQ6() {
    runPlannerTestFile("calcite_tpcds/tpcds-q06", testDb, options, testOptions);
  }

  @Test
  public void testQ7() {
    runPlannerTestFile("calcite_tpcds/tpcds-q07", testDb, options, testOptions);
  }

  @Test
  public void testQ8() {
    runPlannerTestFile("calcite_tpcds/tpcds-q08", testDb, options, testOptions);
  }

  @Test
  public void testQ9() {
    runPlannerTestFile("calcite_tpcds/tpcds-q09", testDb, options, testOptions);
  }

  @Test
  public void testQ10() {
    // This is an official variant of q10 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("calcite_tpcds/tpcds-q10a", testDb, options, testOptions);
  }

  @Test
  public void testQ11() {
    runPlannerTestFile("calcite_tpcds/tpcds-q11", testDb, options, testOptions);
  }

  @Test
  public void testQ12() {
    runPlannerTestFile("calcite_tpcds/tpcds-q12", testDb, options, testOptions);
  }

  @Test
  public void testQ13() {
    runPlannerTestFile("calcite_tpcds/tpcds-q13", testDb, options, testOptions);
  }

  @Test
  public void testQ14a() {
    // First of the two query statements from the official q14.
    runPlannerTestFile("calcite_tpcds/tpcds-q14a", testDb, options, testOptions);
  }

  @Test
  public void testQ14b() {
    // Second of the two query statements from the official q14.
    runPlannerTestFile("calcite_tpcds/tpcds-q14b", testDb, options, testOptions);
  }

  @Test
  public void testQ15() {
    runPlannerTestFile("calcite_tpcds/tpcds-q15", testDb, options, testOptions);
  }

  @Test
  public void testQ16() {
    runPlannerTestFile("calcite_tpcds/tpcds-q16", testDb, options, testOptions);
  }

  @Test
  public void testQ17() {
    runPlannerTestFile("calcite_tpcds/tpcds-q17", testDb, options, testOptions);
  }

  @Test
  public void testQ18() {
    runPlannerTestFile("calcite_tpcds/tpcds-q18", testDb, options, testOptions);
  }

  @Test
  public void testQ19() {
    runPlannerTestFile("calcite_tpcds/tpcds-q19", testDb, options, testOptions);
  }

  @Test
  public void testQ20() {
    runPlannerTestFile("calcite_tpcds/tpcds-q20", testDb, options, testOptions);
  }

  @Test
  public void testQ21() {
    runPlannerTestFile("calcite_tpcds/tpcds-q21", testDb, options, testOptions);
  }

  @Test
  public void testQ22() {
    runPlannerTestFile("calcite_tpcds/tpcds-q22", testDb, options, testOptions);
  }

  @Test
  public void testQ23a() {
    // First of the two query statements from the official q23.
    runPlannerTestFile("calcite_tpcds/tpcds-q23a", testDb, options, testOptions);
  }

  @Test
  public void testQ23b() {
    // Second of the two query statements from the official q23.
    runPlannerTestFile("calcite_tpcds/tpcds-q23b", testDb, options, testOptions);
  }

  @Test
  public void testQ24a() {
    // First of the two query statements from the official q24.
    runPlannerTestFile("calcite_tpcds/tpcds-q24a", testDb, options, testOptions);
  }

  @Test
  public void testQ24b() {
    // Second of the two query statements from the official q24.
    runPlannerTestFile("calcite_tpcds/tpcds-q24b", testDb, options, testOptions);
  }

  @Test
  public void testQ25() {
    runPlannerTestFile("calcite_tpcds/tpcds-q25", testDb, options, testOptions);
  }

  @Test
  public void testQ26() {
    runPlannerTestFile("calcite_tpcds/tpcds-q26", testDb, options, testOptions);
  }

  @Test
  public void testQ27() {
    runPlannerTestFile("calcite_tpcds/tpcds-q27", testDb, options, testOptions);
  }

  @Test
  public void testQ28() {
    runPlannerTestFile("calcite_tpcds/tpcds-q28", testDb, options, testOptions);
  }

  @Test
  public void testQ29() {
    runPlannerTestFile("calcite_tpcds/tpcds-q29", testDb, options, testOptions);
  }

  @Test
  public void testQ30() {
    runPlannerTestFile("calcite_tpcds/tpcds-q30", testDb, options, testOptions);
  }

  @Test
  public void testQ31() {
    runPlannerTestFile("calcite_tpcds/tpcds-q31", testDb, options, testOptions);
  }

  @Test
  public void testQ32() {
    runPlannerTestFile("calcite_tpcds/tpcds-q32", testDb, options, testOptions);
  }

  @Test
  public void testQ33() {
    runPlannerTestFile("calcite_tpcds/tpcds-q33", testDb, options, testOptions);
  }

  @Test
  public void testQ34() {
    runPlannerTestFile("calcite_tpcds/tpcds-q34", testDb, options, testOptions);
  }

  @Test
  public void testQ35() {
    // This is an official variant of q35 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("calcite_tpcds/tpcds-q35a", testDb, options, testOptions);
  }

  @Test
  public void testQ36() {
    runPlannerTestFile("calcite_tpcds/tpcds-q36", testDb, options, testOptions);
  }

  @Test
  public void testQ37() {
    runPlannerTestFile("calcite_tpcds/tpcds-q37", testDb, options, testOptions);
  }

  @Test
  public void testQ38() {
    runPlannerTestFile("calcite_tpcds/tpcds-q38", testDb, options, testOptions);
  }

  @Test
  public void testQ39a() {
    // First of the two query statements from the official q39.
    runPlannerTestFile("calcite_tpcds/tpcds-q39a", testDb, options, testOptions);
  }

  @Test
  public void testQ39b() {
    // Second of the two query statements from the official q39.
    runPlannerTestFile("calcite_tpcds/tpcds-q39b", testDb, options, testOptions);
  }

  @Test
  public void testQ40() {
    runPlannerTestFile("calcite_tpcds/tpcds-q40", testDb, options, testOptions);
  }

  @Test
  public void testQ41() {
    runPlannerTestFile("calcite_tpcds/tpcds-q41", testDb, options, testOptions);
  }

  @Test
  public void testQ42() {
    runPlannerTestFile("calcite_tpcds/tpcds-q42", testDb, options, testOptions);
  }

  @Test
  public void testQ43() {
    runPlannerTestFile("calcite_tpcds/tpcds-q43", testDb, options, testOptions);
  }

  @Test
  public void testQ44() {
    runPlannerTestFile("calcite_tpcds/tpcds-q44", testDb, options, testOptions);
  }

  @Test
  public void testQ45() {
    runPlannerTestFile("calcite_tpcds/tpcds-q45", testDb, options, testOptions);
  }

  @Test
  public void testQ46() {
    runPlannerTestFile("calcite_tpcds/tpcds-q46", testDb, options, testOptions);
  }

  @Test
  public void testQ47() {
    runPlannerTestFile("calcite_tpcds/tpcds-q47", testDb, options, testOptions);
  }

  @Test
  public void testQ48() {
    runPlannerTestFile("calcite_tpcds/tpcds-q48", testDb, options, testOptions);
  }

  @Test
  public void testQ49() {
    runPlannerTestFile("calcite_tpcds/tpcds-q49", testDb, options, testOptions);
  }

  @Test
  public void testQ50() {
    runPlannerTestFile("calcite_tpcds/tpcds-q50", testDb, options, testOptions);
  }

  @Test
  public void testQ51() {
    runPlannerTestFile("calcite_tpcds/tpcds-q51", testDb, options, testOptions);
  }

  @Test
  public void testQ52() {
    runPlannerTestFile("calcite_tpcds/tpcds-q52", testDb, options, testOptions);
  }

  @Test
  public void testQ53() {
    runPlannerTestFile("calcite_tpcds/tpcds-q53", testDb, options, testOptions);
  }

  @Test
  public void testQ54() {
    runPlannerTestFile("calcite_tpcds/tpcds-q54", testDb, options, testOptions);
  }

  @Test
  public void testQ55() {
    runPlannerTestFile("calcite_tpcds/tpcds-q55", testDb, options, testOptions);
  }

  @Test
  public void testQ56() {
    runPlannerTestFile("calcite_tpcds/tpcds-q56", testDb, options, testOptions);
  }

  @Test
  public void testQ57() {
    runPlannerTestFile("calcite_tpcds/tpcds-q57", testDb, options, testOptions);
  }

  @Test
  public void testQ58() {
    runPlannerTestFile("calcite_tpcds/tpcds-q58", testDb, options, testOptions);
  }

  @Test
  public void testQ59() {
    runPlannerTestFile("calcite_tpcds/tpcds-q59", testDb, options, testOptions);
  }

  @Test
  public void testQ60() {
    runPlannerTestFile("calcite_tpcds/tpcds-q60", testDb, options, testOptions);
  }

  @Test
  public void testQ61() {
    runPlannerTestFile("calcite_tpcds/tpcds-q61", testDb, options, testOptions);
  }

  @Test
  public void testQ62() {
    runPlannerTestFile("calcite_tpcds/tpcds-q62", testDb, options, testOptions);
  }

  @Test
  public void testQ63() {
    runPlannerTestFile("calcite_tpcds/tpcds-q63", testDb, options, testOptions);
  }

  @Test
  public void testQ64() {
    runPlannerTestFile("calcite_tpcds/tpcds-q64", testDb, options, testOptions);
  }

  @Test
  public void testQ65() {
    runPlannerTestFile("calcite_tpcds/tpcds-q65", testDb, options, testOptions);
  }

  @Test
  public void testQ66() {
    runPlannerTestFile("calcite_tpcds/tpcds-q66", testDb, options, testOptions);
  }

  @Test
  public void testQ67() {
    runPlannerTestFile("calcite_tpcds/tpcds-q67", testDb, options, testOptions);
  }

  @Test
  public void testQ68() {
    runPlannerTestFile("calcite_tpcds/tpcds-q68", testDb, options, testOptions);
  }

  @Test
  public void testQ69() {
    runPlannerTestFile("calcite_tpcds/tpcds-q69", testDb, options, testOptions);
  }

  @Test
  public void testQ70() {
    runPlannerTestFile("calcite_tpcds/tpcds-q70", testDb, options, testOptions);
  }

  @Test
  public void testQ71() {
    runPlannerTestFile("calcite_tpcds/tpcds-q71", testDb, options, testOptions);
  }

  @Test
  public void testQ72() {
    runPlannerTestFile("calcite_tpcds/tpcds-q72", testDb, options, testOptions);
  }

  @Test
  public void testQ73() {
    runPlannerTestFile("calcite_tpcds/tpcds-q73", testDb, options, testOptions);
  }

  @Test
  public void testQ74() {
    runPlannerTestFile("calcite_tpcds/tpcds-q74", testDb, options, testOptions);
  }

  @Test
  public void testQ75() {
    runPlannerTestFile("calcite_tpcds/tpcds-q75", testDb, options, testOptions);
  }

  @Test
  public void testQ76() {
    runPlannerTestFile("calcite_tpcds/tpcds-q76", testDb, options, testOptions);
  }

  @Test
  public void testQ77() {
    runPlannerTestFile("calcite_tpcds/tpcds-q77", testDb, options, testOptions);
  }

  @Test
  public void testQ78() {
    runPlannerTestFile("calcite_tpcds/tpcds-q78", testDb, options, testOptions);
  }

  @Test
  public void testQ79() {
    runPlannerTestFile("calcite_tpcds/tpcds-q79", testDb, options, testOptions);
  }

  @Test
  public void testQ80() {
    runPlannerTestFile("calcite_tpcds/tpcds-q80", testDb, options, testOptions);
  }

  @Test
  public void testQ81() {
    runPlannerTestFile("calcite_tpcds/tpcds-q81", testDb, options, testOptions);
  }

  @Test
  public void testQ82() {
    runPlannerTestFile("calcite_tpcds/tpcds-q82", testDb, options, testOptions);
  }

  @Test
  public void testQ83() {
    runPlannerTestFile("calcite_tpcds/tpcds-q83", testDb, options, testOptions);
  }

  @Test
  public void testQ84() {
    runPlannerTestFile("calcite_tpcds/tpcds-q84", testDb, options, testOptions);
  }

  @Test
  public void testQ85() {
    runPlannerTestFile("calcite_tpcds/tpcds-q85", testDb, options, testOptions);
  }

  @Test
  public void testQ86() {
    runPlannerTestFile("calcite_tpcds/tpcds-q86", testDb, options, testOptions);
  }

  @Test
  public void testQ87() {
    runPlannerTestFile("calcite_tpcds/tpcds-q87", testDb, options, testOptions);
  }

  @Test
  public void testQ88() {
    runPlannerTestFile("calcite_tpcds/tpcds-q88", testDb, options, testOptions);
  }

  @Test
  public void testQ89() {
    runPlannerTestFile("calcite_tpcds/tpcds-q89", testDb, options, testOptions);
  }

  @Test
  public void testQ90() {
    runPlannerTestFile("calcite_tpcds/tpcds-q90", testDb, options, testOptions);
  }

  @Test
  public void testQ91() {
    runPlannerTestFile("calcite_tpcds/tpcds-q91", testDb, options, testOptions);
  }

  @Test
  public void testQ92() {
    runPlannerTestFile("calcite_tpcds/tpcds-q92", testDb, options, testOptions);
  }

  @Test
  public void testQ93() {
    runPlannerTestFile("calcite_tpcds/tpcds-q93", testDb, options, testOptions);
  }

  @Test
  public void testQ94() {
    runPlannerTestFile("calcite_tpcds/tpcds-q94", testDb, options, testOptions);
  }

  @Test
  public void testQ95() {
    runPlannerTestFile("calcite_tpcds/tpcds-q95", testDb, options, testOptions);
  }

  @Test
  public void testQ96() {
    runPlannerTestFile("calcite_tpcds/tpcds-q96", testDb, options, testOptions);
  }

  @Test
  public void testQ97() {
    runPlannerTestFile("calcite_tpcds/tpcds-q97", testDb, options, testOptions);
  }

  @Test
  public void testQ98() {
    runPlannerTestFile("calcite_tpcds/tpcds-q98", testDb, options, testOptions);
  }

  @Test
  public void testQ99() {
    runPlannerTestFile("calcite_tpcds/tpcds-q99", testDb, options, testOptions);
  }

  @Test
  public void testQ43Verbose() {
    runPlannerTestFile("calcite_tpcds/tpcds-q43-verbose", testDb, options,
        testOptions);
  }

}
