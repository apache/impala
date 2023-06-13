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

import java.nio.file.Paths;
import java.util.Set;

import org.apache.impala.thrift.TQueryOptions;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Plans from the TPC-DS qualification queries at scale factor 1. Single node,
 * Distributed and Parallel plans are all included. Cardinality and resource checks are
 * also preformed.
 */
public class TpcdsPlannerTest extends PlannerTestBase {
  private static Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();

  private static TQueryOptions options = tpcdsParquetQueryOptions();

  @BeforeClass
  public static void setUp() throws Exception {
    PlannerTestBase.setUp();
    Paths.get(outDir_.toString(), "tpcds").toFile().mkdirs();
  }

  @Test
  public void testQ1() {
    runPlannerTestFile("tpcds/tpcds-q01", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ2() {
    runPlannerTestFile("tpcds/tpcds-q02", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ3() {
    runPlannerTestFile("tpcds/tpcds-q03", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ4() {
    runPlannerTestFile("tpcds/tpcds-q04", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ5() {
    runPlannerTestFile("tpcds/tpcds-q05", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ6() {
    runPlannerTestFile("tpcds/tpcds-q06", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ7() {
    runPlannerTestFile("tpcds/tpcds-q07", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ8() {
    runPlannerTestFile("tpcds/tpcds-q08", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ9() {
    runPlannerTestFile("tpcds/tpcds-q09", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ10() {
    // This is an official variant of q10 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("tpcds/tpcds-q10a", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ11() {
    runPlannerTestFile("tpcds/tpcds-q11", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ12() {
    runPlannerTestFile("tpcds/tpcds-q12", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ13() {
    runPlannerTestFile("tpcds/tpcds-q13", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ14a() {
    // First of the two query statements from the official q14.
    runPlannerTestFile("tpcds/tpcds-q14a", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ14b() {
    // Second of the two query statements from the official q14.
    runPlannerTestFile("tpcds/tpcds-q14b", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ15() {
    runPlannerTestFile("tpcds/tpcds-q15", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ16() {
    runPlannerTestFile("tpcds/tpcds-q16", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ17() {
    runPlannerTestFile("tpcds/tpcds-q17", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ18() {
    runPlannerTestFile("tpcds/tpcds-q18", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ19() {
    runPlannerTestFile("tpcds/tpcds-q19", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ20() {
    runPlannerTestFile("tpcds/tpcds-q20", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ21() {
    runPlannerTestFile("tpcds/tpcds-q21", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ22() {
    runPlannerTestFile("tpcds/tpcds-q22", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ23a() {
    // First of the two query statements from the official q23.
    runPlannerTestFile("tpcds/tpcds-q23a", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ23b() {
    // Second of the two query statements from the official q23.
    runPlannerTestFile("tpcds/tpcds-q23b", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ24a() {
    // First of the two query statements from the official q24.
    runPlannerTestFile("tpcds/tpcds-q24a", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ24b() {
    // Second of the two query statements from the official q24.
    runPlannerTestFile("tpcds/tpcds-q24b", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ25() {
    runPlannerTestFile("tpcds/tpcds-q25", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ26() {
    runPlannerTestFile("tpcds/tpcds-q26", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ27() {
    runPlannerTestFile("tpcds/tpcds-q27", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ28() {
    runPlannerTestFile("tpcds/tpcds-q28", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ29() {
    runPlannerTestFile("tpcds/tpcds-q29", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ30() {
    runPlannerTestFile("tpcds/tpcds-q30", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ31() {
    runPlannerTestFile("tpcds/tpcds-q31", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ32() {
    runPlannerTestFile("tpcds/tpcds-q32", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ33() {
    runPlannerTestFile("tpcds/tpcds-q33", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ34() {
    runPlannerTestFile("tpcds/tpcds-q34", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ35() {
    // This is an official variant of q35 that uses a rewrite for lack of support for
    // multiple subqueries in disjunctive predicates.
    runPlannerTestFile("tpcds/tpcds-q35a", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ36() {
    runPlannerTestFile("tpcds/tpcds-q36", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ37() {
    runPlannerTestFile("tpcds/tpcds-q37", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ38() {
    runPlannerTestFile("tpcds/tpcds-q38", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ39a() {
    // First of the two query statements from the official q39.
    runPlannerTestFile("tpcds/tpcds-q39a", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ39b() {
    // Second of the two query statements from the official q39.
    runPlannerTestFile("tpcds/tpcds-q39b", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ40() {
    runPlannerTestFile("tpcds/tpcds-q40", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ41() {
    runPlannerTestFile("tpcds/tpcds-q41", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ42() {
    runPlannerTestFile("tpcds/tpcds-q42", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ43() {
    runPlannerTestFile("tpcds/tpcds-q43", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ44() {
    runPlannerTestFile("tpcds/tpcds-q44", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ45() {
    runPlannerTestFile("tpcds/tpcds-q45", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ46() {
    runPlannerTestFile("tpcds/tpcds-q46", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ47() {
    runPlannerTestFile("tpcds/tpcds-q47", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ48() {
    runPlannerTestFile("tpcds/tpcds-q48", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ49() {
    runPlannerTestFile("tpcds/tpcds-q49", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ50() {
    runPlannerTestFile("tpcds/tpcds-q50", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ51() {
    runPlannerTestFile("tpcds/tpcds-q51", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ52() {
    runPlannerTestFile("tpcds/tpcds-q52", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ53() {
    runPlannerTestFile("tpcds/tpcds-q53", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ54() {
    runPlannerTestFile("tpcds/tpcds-q54", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ55() {
    runPlannerTestFile("tpcds/tpcds-q55", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ56() {
    runPlannerTestFile("tpcds/tpcds-q56", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ57() {
    runPlannerTestFile("tpcds/tpcds-q57", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ58() {
    runPlannerTestFile("tpcds/tpcds-q58", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ59() {
    runPlannerTestFile("tpcds/tpcds-q59", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ60() {
    runPlannerTestFile("tpcds/tpcds-q60", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ61() {
    runPlannerTestFile("tpcds/tpcds-q61", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ62() {
    runPlannerTestFile("tpcds/tpcds-q62", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ63() {
    runPlannerTestFile("tpcds/tpcds-q63", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ64() {
    runPlannerTestFile("tpcds/tpcds-q64", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ65() {
    runPlannerTestFile("tpcds/tpcds-q65", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ66() {
    runPlannerTestFile("tpcds/tpcds-q66", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ67() {
    runPlannerTestFile("tpcds/tpcds-q67", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ68() {
    runPlannerTestFile("tpcds/tpcds-q68", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ69() {
    runPlannerTestFile("tpcds/tpcds-q69", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ70() {
    runPlannerTestFile("tpcds/tpcds-q70", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ71() {
    runPlannerTestFile("tpcds/tpcds-q71", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ72() {
    runPlannerTestFile("tpcds/tpcds-q72", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ73() {
    runPlannerTestFile("tpcds/tpcds-q73", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ74() {
    runPlannerTestFile("tpcds/tpcds-q74", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ75() {
    runPlannerTestFile("tpcds/tpcds-q75", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ76() {
    runPlannerTestFile("tpcds/tpcds-q76", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ77() {
    runPlannerTestFile("tpcds/tpcds-q77", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ78() {
    runPlannerTestFile("tpcds/tpcds-q78", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ79() {
    runPlannerTestFile("tpcds/tpcds-q79", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ80() {
    runPlannerTestFile("tpcds/tpcds-q80", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ81() {
    runPlannerTestFile("tpcds/tpcds-q81", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ82() {
    runPlannerTestFile("tpcds/tpcds-q82", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ83() {
    runPlannerTestFile("tpcds/tpcds-q83", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ84() {
    runPlannerTestFile("tpcds/tpcds-q84", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ85() {
    runPlannerTestFile("tpcds/tpcds-q85", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ86() {
    runPlannerTestFile("tpcds/tpcds-q86", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ87() {
    runPlannerTestFile("tpcds/tpcds-q87", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ88() {
    runPlannerTestFile("tpcds/tpcds-q88", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ89() {
    runPlannerTestFile("tpcds/tpcds-q89", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ90() {
    runPlannerTestFile("tpcds/tpcds-q90", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ91() {
    runPlannerTestFile("tpcds/tpcds-q91", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ92() {
    runPlannerTestFile("tpcds/tpcds-q92", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ93() {
    runPlannerTestFile("tpcds/tpcds-q93", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ94() {
    runPlannerTestFile("tpcds/tpcds-q94", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ95() {
    runPlannerTestFile("tpcds/tpcds-q95", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ96() {
    runPlannerTestFile("tpcds/tpcds-q96", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ97() {
    runPlannerTestFile("tpcds/tpcds-q97", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ98() {
    runPlannerTestFile("tpcds/tpcds-q98", "tpcds_parquet", options, testOptions);
  }

  @Test
  public void testQ99() {
    runPlannerTestFile("tpcds/tpcds-q99", "tpcds_parquet", options, testOptions);
  }
}
