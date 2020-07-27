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

import static org.apache.impala.testutil.LdapUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Range;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.impala.util.Metrics;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class LdapWebserverTest {
  private static final Logger LOG = Logger.getLogger(LdapWebserverTest.class);
  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  private static final Range<Long> zero = Range.closed(0L, 0L);

  Metrics metrics_ = new Metrics(TEST_USER_1, TEST_PASSWORD_1);

  public void setUp(String extraArgs, String startArgs) throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String impalaArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
            + "--ldap_bind_pattern='%s' --ldap_passwords_in_clear_ok "
            + "--webserver_require_ldap=true %s",
        uri, dn, extraArgs);
    Map<String, String> env = new HashMap<>();
    env.put("IMPALA_WEBSERVER_USERNAME", TEST_USER_1);
    env.put("IMPALA_WEBSERVER_PASSWORD", TEST_PASSWORD_1);
    int ret = CustomClusterRunner.StartImpalaCluster(impalaArgs, env, startArgs);
    assertEquals(ret, 0);
  }

  @After
  public void cleanUp() throws IOException {
    metrics_.Close();
  }

  private void verifyMetrics(Range<Long> expectedBasicSuccess,
      Range<Long> expectedBasicFailure, Range<Long> expectedCookieSuccess,
      Range<Long> expectedCookieFailure) throws Exception {
    long actualBasicSuccess =
        (long) metrics_.getMetric("impala.webserver.total-basic-auth-success");
    assertTrue("Expected: " + expectedBasicSuccess + ", Actual: " + actualBasicSuccess,
        expectedBasicSuccess.contains(actualBasicSuccess));
    long actualBasicFailure =
        (long) metrics_.getMetric("impala.webserver.total-basic-auth-failure");
    assertTrue("Expected: " + expectedBasicFailure + ", Actual: " + actualBasicFailure,
        expectedBasicFailure.contains(actualBasicFailure));

    long actualCookieSuccess =
        (long) metrics_.getMetric("impala.webserver.total-cookie-auth-success");
    assertTrue("Expected: " + expectedCookieSuccess + ", Actual: " + actualCookieSuccess,
        expectedCookieSuccess.contains(actualCookieSuccess));
    long actualCookieFailure =
        (long) metrics_.getMetric("impala.webserver.total-cookie-auth-failure");
    assertTrue("Expected: " + expectedCookieFailure + ", Actual: " + actualCookieFailure,
        expectedCookieFailure.contains(actualCookieFailure));
  }

  @Test
  public void testWebserver() throws Exception {
    setUp("", "");
    // start-impala-cluster contacts the webui to confirm the impalads have started, so
    // there will already be some successful auth attempts.
    verifyMetrics(Range.atLeast(1L), zero, Range.atLeast(1L), zero);

    // Attempt to access the webserver without a username/password.
    Metrics noUsername = new Metrics();
    String result = noUsername.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is one unsuccessful auth attempt.
    verifyMetrics(Range.atLeast(1L), Range.closed(1L, 1L), Range.atLeast(1L), zero);

    // Attempt to access the webserver with invalid username/password.
    Metrics invalidUserPass = new Metrics("invalid", "invalid");
    result = invalidUserPass.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is now two unsuccessful auth attempts.
    verifyMetrics(Range.atLeast(1L), Range.closed(2L, 2L), Range.atLeast(1L), zero);
  }

  /**
   * Tests the webserver specific LDAP user and group filter configs.
   */
  @Test
  public void testWebserverFilters() throws Exception {
    // Set up the filters. Note that we don't use the optional '%s' in the group dn
    // pattern or list multiple groups, these features are covered by LdapImpalaShellTest.
    setUp(String.format("--webserver_ldap_group_filter=%s "
            + "--webserver_ldap_user_filter=%s,%s "
            + "--ldap_group_dn_pattern=ou=Groups,dc=myorg,dc=com "
            + "--ldap_group_membership_key=uniqueMember "
            + "--ldap_group_class_key=groupOfUniqueNames",
        TEST_USER_GROUP, TEST_USER_1, TEST_USER_3), "");
    // start-impala-cluster contacts the webui to confirm the impalads have started, so
    // there will already be some successful auth attempts.
    verifyMetrics(Range.atLeast(1L), zero, Range.atLeast(1L), zero);

    // Access the webserver with a user that passes the group filter but not the user
    // filter, should fail.
    Metrics metricsUser2 = new Metrics(TEST_USER_2, TEST_PASSWORD_2);
    String result = metricsUser2.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is one unsuccessful auth attempt.
    verifyMetrics(Range.atLeast(1L), Range.closed(1L, 1L), Range.atLeast(1L), zero);

    // Access the webserver with a user that passes the user filter but not the group
    // filter, should fail.
    Metrics metricsUser3 = new Metrics(TEST_USER_3, TEST_PASSWORD_3);
    result = metricsUser3.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is now two unsuccessful auth attempts.
    verifyMetrics(Range.atLeast(1L), Range.closed(2L, 2L), Range.atLeast(1L), zero);

    // Access the webserver with a user that doesn't pass either filter, should fail.
    Metrics metricsUser4 = new Metrics(TEST_USER_4, TEST_PASSWORD_4);
    result = metricsUser4.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is now three unsuccessful auth attempts.
    verifyMetrics(Range.atLeast(1L), Range.closed(3L, 3L), Range.atLeast(1L), zero);
  }

  /**
   * Tests that the metrics webserver servers the correct endpoints and without security
   * even if LDAP auth is turned on for the regular webserver.
   */
  @Test
  public void testMetricsWebserver() throws Exception {
    // Use 'per_impalad_args' to turn the metrics webserver on only for the first impalad.
    setUp("", "--per_impalad_args=--metrics_webserver_port=25030");
    // Attempt to access the regular webserver without a username/password, should fail.
    Metrics noUsername = new Metrics();
    String result = noUsername.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));

    // Attempt to access the regular webserver with invalid username/password.
    Metrics invalidUserPass = new Metrics("invalid", "invalid");
    result = invalidUserPass.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));

    // Attempt to access the metrics webserver without a username/password.
    Metrics noUsernameMetrics = new Metrics(25030);
    // Should succeed for the metrics endpoints.
    for (String endpoint :
        new String[] {"/metrics", "/jsonmetrics", "/metrics_prometheus", "/healthz"}) {
      result = noUsernameMetrics.readContent(endpoint);
      assertFalse(
          result, result.contains("Must authenticate with Basic authentication."));
    }

    for (String endpoint : new String[] {"/varz", "/backends"}) {
      result = noUsernameMetrics.readContent(endpoint);
      assertTrue(result, result.contains("No URI handler for"));
    }
  }
}
