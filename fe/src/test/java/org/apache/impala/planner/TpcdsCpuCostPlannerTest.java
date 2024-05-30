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

import org.apache.impala.catalog.SideloadTableStats;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
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
  // Planner test option to run each planner test.
  private static Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();

  // Query option to run each planner test.
  private static TQueryOptions options = tpcdsParquetCpuCostQueryOptions();

  // Database name to run this test.
  private static String testDb = "tpcds_partitioned_parquet_snap";

  // Map of table stats that is obtained through loadStatsJson().
  private static Map<String, Map<String, SideloadTableStats>> sideloadStats;

  // Granular scan limit that will injected into individual ScanNode of tables.
  private static Map<String, Long> scanRangeLimit = new HashMap<String, Long>() {
    {
      // split a 5752989 bytes file to 10 ranges.
      put("customer", 580 * 1024L);
      // split a 1218792 bytes file to 10 ranges.
      put("customer_address", 125 * 1024L);
      // split a 7848768 bytes file to 10 ranges.
      put("customer_demographics", 790 * 1024L);
      // split a 1815300 bytes file to 4 ranges.
      put("item", 500L * 1024L);
    }
  };

  @BeforeClass
  public static void setUp() throws Exception {
    // Mimic the 10 node test mini-cluster.
    // 20 is the default num_expected_executors startup flag.
    PlannerTestBase.setUpWithSize(10, 20);
    Paths.get(outDir_.toString(), "tpcds_cpu_cost").toFile().mkdirs();

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
  public static void unsetMetadataScale() {
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
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q01", testDb, options, testOptions);
  }

  @Test
  public void testQ2() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q02", testDb, options, testOptions);
  }

  @Test
  public void testQ3() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q03", testDb, options, testOptions);
  }

  @Test
  public void testQ4() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q04", testDb, options, testOptions);
  }

  @Test
  public void testQ5() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q05", testDb, options, testOptions);
  }

  @Test
  public void testQ6() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q06", testDb, options, testOptions);
  }

  @Test
  public void testQ7() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q07", testDb, options, testOptions);
  }

  @Test
  public void testQ8() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q08", testDb, options, testOptions);
  }

  @Test
  public void testQ9() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q09", testDb, options, testOptions);
  }

  @Test
  public void testQ10() {
    // This is an official variant of q10 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q10a", testDb, options, testOptions);
  }

  @Test
  public void testQ11() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q11", testDb, options, testOptions);
  }

  @Test
  public void testQ12() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q12", testDb, options, testOptions);
  }

  @Test
  public void testQ13() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q13", testDb, options, testOptions);
  }

  @Test
  public void testQ14a() {
    // First of the two query statements from the official q14.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q14a", testDb, options, testOptions);
  }

  @Test
  public void testQ14b() {
    // Second of the two query statements from the official q14.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q14b", testDb, options, testOptions);
  }

  @Test
  public void testQ15() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q15", testDb, options, testOptions);
  }

  @Test
  public void testQ16() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q16", testDb, options, testOptions);
  }

  @Test
  public void testQ17() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q17", testDb, options, testOptions);
  }

  @Test
  public void testQ18() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q18", testDb, options, testOptions);
  }

  @Test
  public void testQ19() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q19", testDb, options, testOptions);
  }

  @Test
  public void testQ20() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q20", testDb, options, testOptions);
  }

  @Test
  public void testQ21() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q21", testDb, options, testOptions);
  }

  @Test
  public void testQ22() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q22", testDb, options, testOptions);
  }

  @Test
  public void testQ23a() {
    // First of the two query statements from the official q23.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q23a", testDb, options, testOptions);
  }

  @Test
  public void testQ23b() {
    // Second of the two query statements from the official q23.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q23b", testDb, options, testOptions);
  }

  @Test
  public void testQ24a() {
    // First of the two query statements from the official q24.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q24a", testDb, options, testOptions);
  }

  @Test
  public void testQ24b() {
    // Second of the two query statements from the official q24.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q24b", testDb, options, testOptions);
  }

  @Test
  public void testQ25() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q25", testDb, options, testOptions);
  }

  @Test
  public void testQ26() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q26", testDb, options, testOptions);
  }

  @Test
  public void testQ27() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q27", testDb, options, testOptions);
  }

  @Test
  public void testQ28() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q28", testDb, options, testOptions);
  }

  @Test
  public void testQ29() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q29", testDb, options, testOptions);
  }

  @Test
  public void testQ30() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q30", testDb, options, testOptions);
  }

  @Test
  public void testQ31() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q31", testDb, options, testOptions);
  }

  @Test
  public void testQ32() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q32", testDb, options, testOptions);
  }

  @Test
  public void testQ33() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q33", testDb, options, testOptions);
  }

  @Test
  public void testQ34() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q34", testDb, options, testOptions);
  }

  @Test
  public void testQ35() {
    // This is an official variant of q35 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q35a", testDb, options, testOptions);
  }

  @Test
  public void testQ36() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q36", testDb, options, testOptions);
  }

  @Test
  public void testQ37() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q37", testDb, options, testOptions);
  }

  @Test
  public void testQ38() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q38", testDb, options, testOptions);
  }

  @Test
  public void testQ39a() {
    // First of the two query statements from the official q39.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q39a", testDb, options, testOptions);
  }

  @Test
  public void testQ39b() {
    // Second of the two query statements from the official q39.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q39b", testDb, options, testOptions);
  }

  @Test
  public void testQ40() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q40", testDb, options, testOptions);
  }

  @Test
  public void testQ41() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q41", testDb, options, testOptions);
  }

  @Test
  public void testQ42() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q42", testDb, options, testOptions);
  }

  @Test
  public void testQ43() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q43", testDb, options, testOptions);
  }

  @Test
  public void testQ44() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q44", testDb, options, testOptions);
  }

  @Test
  public void testQ45() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q45", testDb, options, testOptions);
  }

  @Test
  public void testQ46() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q46", testDb, options, testOptions);
  }

  @Test
  public void testQ47() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q47", testDb, options, testOptions);
  }

  @Test
  public void testQ48() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q48", testDb, options, testOptions);
  }

  @Test
  public void testQ49() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q49", testDb, options, testOptions);
  }

  @Test
  public void testQ50() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q50", testDb, options, testOptions);
  }

  @Test
  public void testQ51() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q51", testDb, options, testOptions);
  }

  @Test
  public void testQ52() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q52", testDb, options, testOptions);
  }

  @Test
  public void testQ53() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q53", testDb, options, testOptions);
  }

  @Test
  public void testQ54() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q54", testDb, options, testOptions);
  }

  @Test
  public void testQ55() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q55", testDb, options, testOptions);
  }

  @Test
  public void testQ56() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q56", testDb, options, testOptions);
  }

  @Test
  public void testQ57() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q57", testDb, options, testOptions);
  }

  @Test
  public void testQ58() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q58", testDb, options, testOptions);
  }

  @Test
  public void testQ59() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q59", testDb, options, testOptions);
  }

  @Test
  public void testQ60() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q60", testDb, options, testOptions);
  }

  @Test
  public void testQ61() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q61", testDb, options, testOptions);
  }

  @Test
  public void testQ62() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q62", testDb, options, testOptions);
  }

  @Test
  public void testQ63() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q63", testDb, options, testOptions);
  }

  @Test
  public void testQ64() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q64", testDb, options, testOptions);
  }

  @Test
  public void testQ65() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q65", testDb, options, testOptions);
  }

  @Test
  public void testQ66() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q66", testDb, options, testOptions);
  }

  @Test
  public void testQ67() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q67", testDb, options, testOptions);
  }

  @Test
  public void testQ68() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q68", testDb, options, testOptions);
  }

  @Test
  public void testQ69() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q69", testDb, options, testOptions);
  }

  @Test
  public void testQ70() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q70", testDb, options, testOptions);
  }

  @Test
  public void testQ71() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q71", testDb, options, testOptions);
  }

  @Test
  public void testQ72() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q72", testDb, options, testOptions);
  }

  @Test
  public void testQ73() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q73", testDb, options, testOptions);
  }

  @Test
  public void testQ74() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q74", testDb, options, testOptions);
  }

  @Test
  public void testQ75() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q75", testDb, options, testOptions);
  }

  @Test
  public void testQ76() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q76", testDb, options, testOptions);
  }

  @Test
  public void testQ77() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q77", testDb, options, testOptions);
  }

  @Test
  public void testQ78() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q78", testDb, options, testOptions);
  }

  @Test
  public void testQ79() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q79", testDb, options, testOptions);
  }

  @Test
  public void testQ80() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q80", testDb, options, testOptions);
  }

  @Test
  public void testQ81() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q81", testDb, options, testOptions);
  }

  @Test
  public void testQ82() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q82", testDb, options, testOptions);
  }

  @Test
  public void testQ83() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q83", testDb, options, testOptions);
  }

  @Test
  public void testQ84() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q84", testDb, options, testOptions);
  }

  @Test
  public void testQ85() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q85", testDb, options, testOptions);
  }

  @Test
  public void testQ86() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q86", testDb, options, testOptions);
  }

  @Test
  public void testQ87() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q87", testDb, options, testOptions);
  }

  @Test
  public void testQ88() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q88", testDb, options, testOptions);
  }

  @Test
  public void testQ89() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q89", testDb, options, testOptions);
  }

  @Test
  public void testQ90() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q90", testDb, options, testOptions);
  }

  @Test
  public void testQ91() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q91", testDb, options, testOptions);
  }

  @Test
  public void testQ92() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q92", testDb, options, testOptions);
  }

  @Test
  public void testQ93() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q93", testDb, options, testOptions);
  }

  @Test
  public void testQ94() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q94", testDb, options, testOptions);
  }

  @Test
  public void testQ95() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q95", testDb, options, testOptions);
  }

  @Test
  public void testQ96() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q96", testDb, options, testOptions);
  }

  @Test
  public void testQ97() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q97", testDb, options, testOptions);
  }

  @Test
  public void testQ98() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q98", testDb, options, testOptions);
  }

  @Test
  public void testQ99() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q99", testDb, options, testOptions);
  }

  @Test
  public void testQ43Verbose() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q43-verbose", testDb, options, testOptions);
  }
}
