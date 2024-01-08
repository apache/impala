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

import org.apache.impala.thrift.TQueryOptions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Set;

/**
 * Plans from the TPC-DS qualification queries at scale factor 1 and
 * COMPUTE_PROCESSING_COST option enabled. Single node, Distributed and Parallel plans
 * are all included. Cardinality and resource checks are also preformed.
 */
public class TpcdsCpuCostPlannerTest extends PlannerTestBase {
  private static Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();

  private static TQueryOptions options = tpcdsParquetCpuCostQueryOptions();

  @BeforeClass
  public static void setUp() throws Exception {
    PlannerTestBase.setUp();
    Paths.get(outDir_.toString(), "tpcds_cpu_cost").toFile().mkdirs();
  }

  @Test
  public void testQ1() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q01", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ2() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q02", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ3() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q03", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ4() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q04", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ5() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q05", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ6() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q06", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ7() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q07", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ8() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q08", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ9() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q09", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ10() {
    // This is an official variant of q10 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q10a", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ11() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q11", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ12() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q12", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ13() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q13", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ14a() {
    // First of the two query statements from the official q14.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q14a", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ14b() {
    // Second of the two query statements from the official q14.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q14b", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ15() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q15", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ16() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q16", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ17() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q17", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ18() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q18", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ19() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q19", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ20() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q20", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ21() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q21", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ22() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q22", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ23a() {
    // First of the two query statements from the official q23.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q23a", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ23b() {
    // Second of the two query statements from the official q23.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q23b", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ24a() {
    // First of the two query statements from the official q24.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q24a", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ24b() {
    // Second of the two query statements from the official q24.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q24b", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ25() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q25", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ26() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q26", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ27() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q27", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ28() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q28", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ29() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q29", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ30() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q30", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ31() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q31", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ32() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q32", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ33() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q33", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ34() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q34", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ35() {
    // This is an official variant of q35 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q35a", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ36() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q36", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ37() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q37", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ38() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q38", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ39a() {
    // First of the two query statements from the official q39.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q39a", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ39b() {
    // Second of the two query statements from the official q39.
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q39b", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ40() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q40", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ41() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q41", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ42() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q42", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ43() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q43", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ44() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q44", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ45() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q45", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ46() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q46", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ47() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q47", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ48() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q48", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ49() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q49", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ50() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q50", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ51() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q51", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ52() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q52", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ53() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q53", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ54() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q54", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ55() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q55", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ56() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q56", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ57() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q57", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ58() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q58", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ59() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q59", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ60() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q60", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ61() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q61", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ62() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q62", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ63() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q63", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ64() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q64", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ65() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q65", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ66() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q66", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ67() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q67", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ68() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q68", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ69() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q69", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ70() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q70", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ71() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q71", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ72() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q72", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ73() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q73", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ74() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q74", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ75() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q75", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ76() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q76", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ77() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q77", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ78() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q78", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ79() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q79", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ80() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q80", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ81() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q81", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ82() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q82", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ83() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q83", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ84() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q84", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ85() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q85", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ86() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q86", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ87() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q87", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ88() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q88", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ89() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q89", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ90() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q90", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ91() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q91", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ92() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q92", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ93() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q93", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ94() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q94", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ95() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q95", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ96() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q96", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ97() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q97", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ98() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q98", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }

  @Test
  public void testQ99() {
    runPlannerTestFile("tpcds_cpu_cost/tpcds-q99", "tpcds_partitioned_parquet_snap",
        options, testOptions);
  }
}
