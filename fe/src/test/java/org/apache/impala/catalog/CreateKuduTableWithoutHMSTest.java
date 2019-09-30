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

package org.apache.impala.catalog;

import org.apache.impala.testutil.ImpalaJdbcClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test kudu managed table create/drop operation when connected
 * to mysql/postgreSQL directly instead of HMS
 */
public class CreateKuduTableWithoutHMSTest {
  private static String IMPALA_HOME = System.getenv().get("IMPALA_HOME");
  private static String HIVE_CONF_DIR = System.getenv().get("HIVE_CONF_DIR");
  private static String TMP_HIVE_SITE = HIVE_CONF_DIR + "/hive-site-ext";
  private static String URL =
      "jdbc:hive2://localhost:21050/default;auth=noSasl;user=impala";

  @Test
  public void TestKuduTable() throws ClassNotFoundException,SQLException {
    ImpalaJdbcClient client = ImpalaJdbcClient.createClientUsingHiveJdbcDriver(URL);
    try {
      client.connect();
      client.execQuery("CREATE TABLE IF NOT EXISTS default.test_kudu_without_hms (" +
          "id INT, " +
          "PRIMARY KEY (id)" +
          ") " +
          "STORED AS KUDU " +
          "TBLPROPERTIES ('kudu.master_addresses'='localhost');");
      ResultSet rs = client.getConnection().getMetaData().getTables("",
          "default", "test_kudu_without_hms", new String[]{"TABLE"});
      validateTable(rs);
      rs.close();
    } finally {
      client.execQuery("DROP TABLE IF EXISTS default.test_kudu_without_hms");
      client.close();
    }
  }

  /**
   * Validate new created kudu managed table for testing.
   */
  private void validateTable(ResultSet rs) throws SQLException {
    assertTrue(rs.next());
    String resultTableName = rs.getString("TABLE_NAME");
    assertEquals(rs.getString(3), resultTableName);
    String tableType = rs.getString("TABLE_TYPE");
    assertEquals("table", tableType.toLowerCase());
    assertFalse(rs.next());
  }

  /**
   * Restart impala cluster with or without HMS config
   */
  private static void restartImpalaCluster(boolean useHiveMetastore)
      throws IOException, InterruptedException {
    String args = "";
    String kuduVariant = "";
    if (!useHiveMetastore) {
      args = " --env_vars=CUSTOM_CLASSPATH=" + TMP_HIVE_SITE;
      kuduVariant = "KUDU_VARIANT=without_hms_config";
    }

    List<String> envpList = initEnv(kuduVariant);
    String[] envpArray = new String[envpList.size()];
    envpList.toArray(envpArray);

    String generateCmd = String.format("bash %s/bin/create-test-configuration.sh",
                                       IMPALA_HOME);
    String restartCmd = IMPALA_HOME + "/bin/start-impala-cluster.py" + args;
    String cmdStr = generateCmd + " && " + restartCmd;
    Process p = Runtime.getRuntime().exec(getCmdsArray(cmdStr), envpArray);
    p.waitFor();
    assertEquals(0, p.exitValue());
  }

  /**
   * Reserve all enviroment path from current process, including parameters
   */
  private static List<String> initEnv(String kuduEnvp) {
    List<String> envp = new ArrayList<>();
    for (Map.Entry<String,String> entry : System.getenv().entrySet()) {
      envp.add(entry.getKey() + "=" + entry.getValue());
    }
    envp.add(kuduEnvp);
    return envp;
  }

  /**
   * Joint several commands as one command, and pass to Runtime.exec
   */
  private static String[] getCmdsArray(String cmdStr) {
    String[] cmdsArray = new String[3];
    cmdsArray[0] = "/bin/sh";
    cmdsArray[1] = "-c";
    cmdsArray[2] = cmdStr;
    return cmdsArray;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    restartImpalaCluster(false);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    restartImpalaCluster(true);
  }
}