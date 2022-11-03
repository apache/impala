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

import com.google.common.collect.Range;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.impala.testutil.ImpalaJdbcClient;
import org.apache.impala.testutil.WebClient;
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

  private static final Range<Long> zero = Range.closed(0L, 0L);
  private static final Range<Long> one = Range.closed(1L, 1L);

  WebClient client_ = new WebClient();

  public LdapJdbcTest(String connectionType) { super(connectionType); }

  public void setUp(String extraArgs) throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String impalaArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
            + "--ldap_bind_pattern='%s' --ldap_passwords_in_clear_ok "
            + "--cookie_require_secure=false %s",
        uri, dn, extraArgs);
    int ret = CustomClusterRunner.StartImpalaCluster(impalaArgs);
    assertEquals(ret, 0);

    con_ = createConnection(
        ImpalaJdbcClient.getLdapConnectionStr(connectionType_, testUser_, testPassword_));
    if (connectionType_.equals("http")) {
      // There should have been one successful connection auth to create the session.
      verifyMetrics(one, zero, zero, zero);
    }
  }

  private void verifyMetrics(Range<Long> expectedBasicSuccess,
      Range<Long> expectedBasicFailure, Range<Long> expectedCookieSuccess,
      Range<Long> expectedCookieFailure) throws Exception {
    long actualBasicSuccess = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-basic-auth-success");
    assertTrue("Expected: " + expectedBasicSuccess + ", Actual: " + actualBasicSuccess,
        expectedBasicSuccess.contains(actualBasicSuccess));
    long actualBasicFailure = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-basic-auth-failure");
    assertTrue("Expected: " + expectedBasicFailure + ", Actual: " + actualBasicFailure,
        expectedBasicFailure.contains(actualBasicFailure));

    long actualCookieSuccess = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-cookie-auth-success");
    assertTrue("Expected: " + expectedCookieSuccess + ", Actual: " + actualCookieSuccess,
        expectedCookieSuccess.contains(actualCookieSuccess));
    long actualCookieFailure = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-cookie-auth-failure");
    assertTrue("Expected: " + expectedCookieFailure + ", Actual: " + actualCookieFailure,
        expectedCookieFailure.contains(actualCookieFailure));
  }

  @Test
  public void testLoggedInUser() throws Exception {
    setUp("");
    ResultSet rs = con_.createStatement().executeQuery("select logged_in_user() user");
    if (connectionType_.equals("http")) {
      // After the initial auth, the driver should use cookies for all other requests.
      verifyMetrics(one, zero, Range.atLeast(1L), zero);
    }
    assertTrue(rs.next());
    assertEquals(rs.getString("user"), testUser_);
    assertFalse(rs.next());
  }

  @Test
  public void testFailedConnection() throws Exception {
    setUp("");
    try {
      Connection con = createConnection(ImpalaJdbcClient.getLdapConnectionStr(
          connectionType_, testUser_, "invalid-password"));
      fail("Connecting with an invalid password should throw an error.");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not open client transport"));
    }

    try {
      Connection con = createConnection(ImpalaJdbcClient.getLdapConnectionStr(
          connectionType_, "invalid-user", testPassword_));
      fail("Connecting with an invalid user name should throw an error.");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not open client transport"));
    }
  }

  @Test
  public void testExpireCookies() throws Exception {
    if (connectionType_.equals("http")) {
      setUp("--max_cookie_lifetime_s=1");
      // Sleep long enough for the cookie returned in the initial connection to expire.
      Thread.sleep(2000);
      ResultSet rs = con_.createStatement().executeQuery("select logged_in_user() user");
      // The driver should have supplied an incorrect cookie at least once, requiring at
      // least one more auth to LDAP. There may also have been some successful cookie
      // attempts, depending on timing.
      verifyMetrics(Range.atLeast(2L), zero, Range.atLeast(0L), Range.atLeast(1L));
      assertTrue(rs.next());
      assertEquals(rs.getString("user"), testUser_);
      assertFalse(rs.next());
    }
  }
}
