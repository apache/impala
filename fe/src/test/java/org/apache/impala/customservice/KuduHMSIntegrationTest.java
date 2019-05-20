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

package org.apache.impala.customservice;

import static org.junit.Assert.assertEquals;

import org.apache.impala.analysis.AnalyzeKuduDDLTest;
import org.apache.impala.analysis.AuditingKuduTest;
import org.apache.impala.analysis.ParserTest;
import org.apache.impala.analysis.ToSqlTest;
import org.apache.impala.customservice.CustomServiceRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(Suite.class)
@SuiteClasses({ AnalyzeKuduDDLTest.class, AuditingKuduTest.class,
                ParserTest.class, ToSqlTest.class })
/**
 * Test suite on Kudu tables when HMS integration is enabled.
 */
public class KuduHMSIntegrationTest {
  /**
   * Restarts Kudu cluster with or without HMS Integration.
   */
  private static void restartKudu(boolean enableHMSIntegration)
      throws Exception {
    List<String> envp = getSystemEnv(enableHMSIntegration);
    int exitVal = CustomServiceRunner.RestartMiniclusterComponent(
        "kudu", envp.toArray(new String[envp.size()]));
    assertEquals(0, exitVal);
  }

  /**
   * Parsing system environment variables and set IMPALA_KUDU_STARTUP_FLAGS
   * if HMS integration should be enabled.
   */
  private static List<String> getSystemEnv(boolean enableHMSIntegration) {
    List<String> envp = new ArrayList<>();
    for (Map.Entry<String,String> entry : System.getenv().entrySet()) {
      envp.add(entry.getKey() + "=" + entry.getValue());
    }
    if (enableHMSIntegration) {
      final String hmsIntegrationEnv = String.format("IMPALA_KUDU_STARTUP_FLAGS=" +
          "-hive_metastore_uris=thrift://%s:9083",
          System.getenv("INTERNAL_LISTEN_HOST"));
      envp.add(hmsIntegrationEnv);
    }
    return envp;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    restartKudu(true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    restartKudu(false);
  }
}