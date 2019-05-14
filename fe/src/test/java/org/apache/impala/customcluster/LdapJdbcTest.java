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

package org.apache.impala.customcluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.impala.testutil.ImpalaJdbcClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.impala.service.JdbcTestBase;

/**
 * Tests for connecting to Impala when LDAP authentication is in use.
 */
@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class LdapJdbcTest extends JdbcTestBase {
  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  // These correspond to the values in fe/src/test/resources/users.ldif
  private static final String testUser_ = "Test1Ldap";
  private static final String testPassword_ = "12345";

  @BeforeClass
  public static void setUp() throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String ldapArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
        + "--ldap_bind_pattern='%s' --ldap_passwords_in_clear_ok", uri, dn);
    int ret = CustomClusterRunner.StartImpalaCluster(ldapArgs);
    assertEquals(ret, 0);

    con_ =
        createConnection(ImpalaJdbcClient.getLdapConnectionStr(testUser_, testPassword_));
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    JdbcTestBase.cleanUp();
    CustomClusterRunner.StartImpalaCluster();
  }

  @Test
  public void testLoggedInUser() throws Exception {
    ResultSet rs = con_.createStatement().executeQuery("select logged_in_user() user");
    assertTrue(rs.next());
    assertEquals(rs.getString("user"), testUser_);
    assertFalse(rs.next());
  }

  @Test
  public void testFailedConnection() throws Exception {
    try {
      Connection con = createConnection(
          ImpalaJdbcClient.getLdapConnectionStr(testUser_, "invalid-password"));
      fail("Connecting with an invalid password should throw an error.");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not open client transport"));
    }

    try {
      Connection con = createConnection(
          ImpalaJdbcClient.getLdapConnectionStr("invalid-user", testPassword_));
      fail("Connecting with an invalid user name should throw an error.");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not open client transport"));
    }
  }
}
