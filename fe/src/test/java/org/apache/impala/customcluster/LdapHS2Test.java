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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.impala.testutil.WebClient;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
/**
 * Tests that hiveserver2 operations over the http interface work as expected when
 * ldap authentication is being used.
 */
public class LdapHS2Test {
  private static final Logger LOG = LoggerFactory.getLogger(LdapHS2Test.class);

  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  WebClient client_ = new WebClient();

  public void setUp(String extraArgs) throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String ldapArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
            + "--ldap_bind_pattern='%s' --ldap_passwords_in_clear_ok %s ",
        uri, dn, extraArgs);
    int ret = CustomClusterRunner.StartImpalaCluster(ldapArgs);
    assertEquals(ret, 0);
  }

  static void verifySuccess(TStatus status) throws Exception {
    if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS
        || status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
      return;
    }
    throw new Exception(status.toString());
  }

  /**
   * Executes 'query' and fetches the results. Expects there to be exactly one string
   * returned, which be be equal to 'expectedResult'.
   */
  static TOperationHandle execAndFetch(TCLIService.Iface client,
      TSessionHandle sessionHandle, String query, String expectedResult)
      throws Exception {
    TExecuteStatementReq execReq = new TExecuteStatementReq(sessionHandle, query);
    TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    verifySuccess(execResp.getStatus());

    TFetchResultsReq fetchReq = new TFetchResultsReq(
        execResp.getOperationHandle(), TFetchOrientation.FETCH_NEXT, 1000);
    TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
    verifySuccess(fetchResp.getStatus());
    List<TColumn> columns = fetchResp.getResults().getColumns();
    assertEquals(1, columns.size());
    if (expectedResult != null) {
      assertEquals(expectedResult, columns.get(0).getStringVal().getValues().get(0));
    }

    return execResp.getOperationHandle();
  }

  /**
   * Executes async 'query'.
   */
  static TOperationHandle execQueryAsync(TCLIService.Iface client,
      TSessionHandle sessionHandle, String query)
      throws Exception {
    TExecuteStatementReq execReq = new TExecuteStatementReq(sessionHandle, query);
    TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    verifySuccess(execResp.getStatus());

    return execResp.getOperationHandle();
  }

  private void verifyMetrics(long expectedBasicAuthSuccess, long expectedBasicAuthFailure)
      throws Exception {
    long actualBasicAuthSuccess = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-basic-auth-success");
    assertEquals(expectedBasicAuthSuccess, actualBasicAuthSuccess);
    long actualBasicAuthFailure = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-basic-auth-failure");
    assertEquals(expectedBasicAuthFailure, actualBasicAuthFailure);
  }

  private void verifyCookieMetrics(
      long expectedCookieAuthSuccess, long expectedCookieAuthFailure) throws Exception {
    long actualCookieAuthSuccess = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-cookie-auth-success");
    assertEquals(expectedCookieAuthSuccess, actualCookieAuthSuccess);
    long actualCookieAuthFailure = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-cookie-auth-failure");
    assertEquals(expectedCookieAuthFailure, actualCookieAuthFailure);
  }

  private void verifyTrustedDomainMetrics(long expectedAuthSuccess) throws Exception {
    long actualAuthSuccess = (long) client_
        .getMetric("impala.thrift-server.hiveserver2-http-frontend."
            + "total-trusted-domain-check-success");
    assertEquals(expectedAuthSuccess, actualAuthSuccess);
  }

  private void verifyTrustedAuthHeaderMetrics(long expectedAuthSuccess) throws Exception {
    long actualAuthSuccess =
        (long) client_.getMetric("impala.thrift-server.hiveserver2-http-frontend."
            + "total-trusted-auth-header-check-success");
    assertEquals(expectedAuthSuccess, actualAuthSuccess);
  }

  private void verifyJwtAuthMetrics(long expectedAuthSuccess, long expectedAuthFailure)
      throws Exception {
    long actualAuthSuccess =
        (long) client_.getMetric("impala.thrift-server.hiveserver2-http-frontend."
            + "total-jwt-token-auth-success");
    assertEquals(expectedAuthSuccess, actualAuthSuccess);
    long actualAuthFailure =
        (long) client_.getMetric("impala.thrift-server.hiveserver2-http-frontend."
            + "total-jwt-token-auth-failure");
    assertEquals(expectedAuthFailure, actualAuthFailure);
  }

  /**
   * Tests LDAP authentication to the HTTP hiveserver2 endpoint.
   */
  @Test
  public void testHiveserver2() throws Exception {
    setUp("");
    verifyMetrics(0, 0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();
    // Authenticate as 'Test1Ldap' with password '12345'
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(1, 0);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(3, 0);

    // Authenticate as 'Test2Ldap' with password 'abcde'
    headers.put("Authorization", "Basic VGVzdDJMZGFwOmFiY2Rl");
    transport.setCustomHeaders(headers);
    String expectedError = "The user authorized on the connection 'Test2Ldap' does not "
        + "match the session username 'Test1Ldap'\n";
    try {
      // Run a query which should fail.
      execAndFetch(client, openResp.getSessionHandle(), "select 1", "1");
      fail("Expected error: " + expectedError);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(expectedError));
      // The connection for the Exec() will be successful.
      verifyMetrics(4, 0);
    }

    // Try to cancel the first query, which should fail.
    TCancelOperationReq cancelReq = new TCancelOperationReq(operationHandle);
    TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
    verifyMetrics(5, 0);
    assertEquals(cancelResp.getStatus().getStatusCode(), TStatusCode.ERROR_STATUS);
    assertEquals(cancelResp.getStatus().getErrorMessage(), expectedError);

    // Open another session which will get username 'Test2Ldap'.
    TOpenSessionReq openReq2 = new TOpenSessionReq();
    TOpenSessionResp openResp2 = client.OpenSession(openReq);
    verifyMetrics(6, 0);
    // Running a query with the new session should succeed.
    execAndFetch(
        client, openResp2.getSessionHandle(), "select logged_in_user()", "Test2Ldap");
    verifyMetrics(8, 0);

    // Attempt to authenticate with some bad headers:
    // - invalid username/password combination
    // - invalid base64 encoded value
    // - Invalid mechanism
    int numFailures = 0;
    for (String authStr :
        new String[] {"Basic VGVzdDJMZGFwOjEyMzQ1", "Basic invalid-base64"}) {
      // Attempt to authenticate with an invalid password.
      headers.put("Authorization", authStr);
      transport.setCustomHeaders(headers);
      try {
        TOpenSessionReq openReq3 = new TOpenSessionReq();
        TOpenSessionResp openResp3 = client.OpenSession(openReq);
        fail("Exception exception.");
      } catch (Exception e) {
        ++numFailures;
        verifyMetrics(8, numFailures);
        assertEquals(e.getMessage(), "HTTP Response code: 401");
      }
    }

    // Attempt to authenticate with a different mechanism. Should fail, but won't
    // increment the total-basic-auth-failure metric because its not considered a 'Basic'
    // auth attempt.
    headers.put("Authorization", "Negotiate VGVzdDFMZGFwOjEyMzQ1");
    transport.setCustomHeaders(headers);
    try {
      TOpenSessionReq openReq3 = new TOpenSessionReq();
      TOpenSessionResp openResp3 = client.OpenSession(openReq);
      fail("Exception exception.");
    } catch (Exception e) {
      verifyMetrics(8, numFailures);
      assertEquals(e.getMessage(), "HTTP Response code: 401");
    }

    // Attempt to authenticate with a bad cookie and valid user/password, should succeed.
    headers.put("Authorization", "Basic VGVzdDJMZGFwOmFiY2Rl");
    headers.put("Cookie", "invalid-cookie");
    transport.setCustomHeaders(headers);
    TOpenSessionReq openReq4 = new TOpenSessionReq();
    TOpenSessionResp openResp4 = client.OpenSession(openReq);
    // We should see one more successful connection and one failed cookie attempt.
    verifyMetrics(9, numFailures);
    int numCookieFailures = 1;
    verifyCookieMetrics(0, numCookieFailures);

    // Attempt to authenticate with no username/password and some bad cookies.
    headers.remove("Authorization");
    String[] badCookies = new String[] {
        "invalid-format", // invalid cookie format
        "x&impala&0&0", // signature value that is invalid base64
        "eA==&impala&0&0", // signature decodes to an incorrect length
        "\"eA==&impala&0&0\"", // signature decodes to an incorrect length
        "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg=&impala&0&0" // incorrect signature
    };
    for (String cookieStr : badCookies) {
      headers.put("Cookie", "impala.auth=" + cookieStr);
      transport.setCustomHeaders(headers);
      try {
        TOpenSessionReq openReq5 = new TOpenSessionReq();
        TOpenSessionResp openResp5 = client.OpenSession(openReq);
        fail("Exception exception from cookie: " + cookieStr);
      } catch (Exception e) {
        // We should see both another failed cookie attempt.
        ++numCookieFailures;
        verifyMetrics(9, numFailures);
        verifyCookieMetrics(0, numCookieFailures);
        assertEquals(e.getMessage(), "HTTP Response code: 401");
      }
    }

    // Authenticate as 'Test1Ldap' with password '12345' - the request
    // contains an empty 'a' header to test the fix of IMPALA-12341
    // should succeed
    Map<String, String> orderedHeaders = new LinkedHashMap<>();
    orderedHeaders.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    // This empty "a" header breaks authentication in builds prior to IMPALA-12341
    orderedHeaders.put("a", "");
    transport.setCustomHeaders(orderedHeaders);
    transport.open();

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq6 = new TOpenSessionReq();
    TOpenSessionResp openResp6 = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(10, numFailures);
    // Running a query should succeed.
    TOperationHandle operationHandle6 = execAndFetch(
            client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(12, numFailures);
  }

  /**
   * Test for the interaction between the HS2 'impala.doas.user' property and LDAP user
   * and group filters.
   */
  @Test
  public void testHS2Impersonation() throws Exception {
    // These correspond to the values in fe/src/test/resources/users.ldif
    // Sets up a cluster where TEST_USER_4 can act as a proxy for any other user but
    // doesn't pass any filters themselves, TEST_USER_1 and TEST_USER_2 can pass the group
    // filter, and TEST_USER_1 and TEST_USER_3 pass the user filter.
    setUp(String.format("--ldap_group_filter=%s,another-group "
            + "--ldap_user_filter=%s,%s,another-user "
            + "--ldap_group_dn_pattern=%s "
            + "--ldap_group_membership_key=uniqueMember "
            + "--ldap_group_class_key=groupOfUniqueNames "
            + "--authorized_proxy_user_config=%s=* "
            + "--ldap_bind_dn=%s --ldap_bind_password_cmd='echo -n %s' ",
        TEST_USER_GROUP, TEST_USER_1, TEST_USER_3, GROUP_DN_PATTERN, TEST_USER_4,
        TEST_USER_DN_1, TEST_PASSWORD_1));

    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();
    // Authenticate as the proxy user 'Test4Ldap'
    headers.put("Authorization", "Basic VGVzdDRMZGFwOmZnaGlq");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session without specifying a 'doas', should fail as the proxy user won't
    // pass the filters.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    assertEquals(openResp.getStatus().getStatusCode(), TStatusCode.ERROR_STATUS);

    // Open a session with a 'doas' that will pass both filters, should succeed.
    Map<String, String> config = new HashMap<String, String>();
    config.put("impala.doas.user", TEST_USER_1);
    openReq.setConfiguration(config);
    openResp = client.OpenSession(openReq);
    assertEquals(openResp.getStatus().getStatusCode(), TStatusCode.SUCCESS_STATUS);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");

    // Open a session with a 'doas' that doesn't pass the user filter, should fail.
    config.put("impala.doas.user", TEST_USER_2);
    openResp = client.OpenSession(openReq);
    assertEquals(openResp.getStatus().getStatusCode(), TStatusCode.ERROR_STATUS);

    // Open a session with a 'doas' that doesn't pass the group filter, should fail.
    config.put("impala.doas.user", TEST_USER_3);
    openResp = client.OpenSession(openReq);
    assertEquals(openResp.getStatus().getStatusCode(), TStatusCode.ERROR_STATUS);

    // Open a session with a 'doas' that doesn't pass either filter, should fail.
    config.put("impala.doas.user", TEST_USER_4);
    openResp = client.OpenSession(openReq);
    assertEquals(openResp.getStatus().getStatusCode(), TStatusCode.ERROR_STATUS);
  }

  /**
   * Tests if authentication is skipped when connections to the HTTP hiveserver2
   * endpoint originate from a trusted domain. This is a shared test function
   * that is used for both the trusted_domain_strict_localhost=true and false
   * cases.
   */
  private void hiveserver2TrustedDomainAuthTestBody(boolean strictLocalhost)
      throws Exception {
    String strictLocalhostArgs = "--trusted_domain_strict_localhost=" +
        String.valueOf(strictLocalhost);
    setUp("--trusted_domain=localhost --trusted_domain_use_xff_header=true "
        + strictLocalhostArgs);
    verifyMetrics(0, 0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();

    // Case 1: Authenticate as 'Test1Ldap' with the right password '12345'
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    headers.put("X-Forwarded-For", "127.0.0.1");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(1);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(3);

    // Case 2a: Authenticate as 'Test1Ldap' without password
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.put("X-Forwarded-For", "127.0.0.1");
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(4);
    operationHandle = execAndFetch(client, openResp.getSessionHandle(),
        "select logged_in_user()", "Test1Ldap");
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(6);

    // Case 2b: Authenticate as 'Test1Ldap' without password. Tests that XFF header name
    // is case-insensitive.
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.remove("X-Forwarded-For");
    headers.put("x-Forwarded-for", "127.0.0.1");
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(7);
    operationHandle = execAndFetch(client, openResp.getSessionHandle(),
        "select logged_in_user()", "Test1Ldap");
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(9);
    headers.remove("x-Forwarded-for");

    // Case 3: Case 1: Authenticate as 'Test1Ldap' with the right password
    // '12345' but with a non trusted address in X-Forwarded-For header
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    // Sometimes RDNS resolves 127.* addresses as localhost, so this uses a 126.*
    // address for non-strict localhost to avoid RDNS issues.
    String nontrustedIp = strictLocalhost ? "127.0.23.1" : "126.0.23.1";
    headers.put("X-Forwarded-For", nontrustedIp);
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    verifyMetrics(1, 0);
    verifyTrustedDomainMetrics(9);
    operationHandle = execAndFetch(client, openResp.getSessionHandle(),
        "select logged_in_user()", "Test1Ldap");
    verifyMetrics(3, 0);
    verifyTrustedDomainMetrics(9);

    // Case 4: No auth header, does not work
    headers.remove("Authorization");
    headers.put("X-Forwarded-For", "127.0.0.1");
    transport.setCustomHeaders(headers);
    try {
      openResp = client.OpenSession(openReq);
      fail("Exception exception.");
    } catch (Exception e) {
      verifyTrustedDomainMetrics(9);
      assertEquals(e.getMessage(), "HTTP Response code: 401");
    }

    // Case 5: Case 1: Authenticate as 'Test1Ldap' with the no password
    // and a non trusted address in X-Forwarded-For header
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.put("X-Forwarded-For", nontrustedIp);
    transport.setCustomHeaders(headers);
    try {
      openResp = client.OpenSession(openReq);
      fail("Exception exception.");
    } catch (Exception e) {
      verifyMetrics(3, 1);
      verifyTrustedDomainMetrics(9);
      assertEquals(e.getMessage(), "HTTP Response code: 401");
    }

    // Case 6: Verify that there are no changes in metrics for trusted domain
    // check if the X-Forwarded-For header is not present
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    headers.remove("X-Forwarded-For");
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    // Account for 1 successful basic auth increment.
    verifyMetrics(4, 1);
    verifyTrustedDomainMetrics(9);
  }

  @Test
  public void testHiveserver2TrustedDomainAuthStrict() throws Exception {
    // Test variant with trusted_domain_strict_localhost=true
    hiveserver2TrustedDomainAuthTestBody(true);
  }

  @Test
  public void testHiveserver2TrustedDomainAuthNonstrict() throws Exception {
    // Test variant with trusted_domain_strict_localhost=false
    hiveserver2TrustedDomainAuthTestBody(false);
  }

  @Test
  public void testHiveserver2TrustedDomainEmptyXffHeaderUseOrigin() throws Exception {
    setUp("--trusted_domain=localhost --trusted_domain_use_xff_header=true " +
          "--trusted_domain_empty_xff_header_use_origin=true");
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();

    // Case 1: Authenticate as 'Test1Ldap' without password, send X-Forwarded-For header
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.put("X-Forwarded-For", "127.0.0.1");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(1);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
            client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(3);

    // Case 2: Authenticate as 'Test1Ldap' without password, do not send X-Forwarded-For
    // header
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.remove("X-Forwarded-For");
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(4);
    operationHandle = execAndFetch(client, openResp.getSessionHandle(),
            "select logged_in_user()", "Test1Ldap");
    verifyMetrics(0, 0);
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyTrustedDomainMetrics(6);

    // Case 3: Authenticate as 'Test1Ldap' without password, send X-Forwarded-For header
    // that does not match trusted_domain
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.put("X-Forwarded-For", "126.0.23.1");
    transport.setCustomHeaders(headers);
    try {
      openResp = client.OpenSession(openReq);
      fail("Authentication should fail without password.");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "HTTP Response code: 401");
      // One basic auth failure
      verifyMetrics(0, 1);
      // And no more successful authentication
      verifyTrustedDomainMetrics(6);
    }

    // Case 4: Authenticate as 'Test1Ldap' without password, do not send X-Forwarded-For
    // header and the origin does not match trusted_domain
    setUp("--trusted_domain=any.domain --trusted_domain_use_xff_header=true " +
            "--trusted_domain_empty_xff_header_use_origin=true");
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.remove("X-Forwarded-For");
    transport.setCustomHeaders(headers);
    verifyMetrics(0, 0);
    verifyTrustedDomainMetrics(0);
    try {
      openResp = client.OpenSession(openReq);
      fail("Authentication should fail without password.");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "HTTP Response code: 401");
      // One basic auth failure
      verifyMetrics(0, 1);
      // No successful trusted domain authentications
      verifyTrustedDomainMetrics(0);
    }
  }

  /**
   * Tests if authentication is skipped when connections to the HTTP hiveserver2
   * endpoint have trusted auth header.
   */
  @Test
  public void testHiveserver2TrustedAuthHeader() throws Exception {
    setUp("--trusted_auth_header=X-Trusted-Proxy-Auth-Header");
    verifyMetrics(0, 0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();

    // Case 1: Authenticate as 'Test1Ldap' with the right password '12345'
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    headers.put("X-Trusted-Proxy-Auth-Header", "on");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(0, 0);
    verifyTrustedAuthHeaderMetrics(1);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(0, 0);
    verifyTrustedAuthHeaderMetrics(3);

    // Case 2: Authentication as 'Test1Ldap' without password
    headers.put("Authorization", "Basic VGVzdDFMZGFwOg==");
    headers.put("X-Trusted-Proxy-Auth-Header", "");
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    verifyMetrics(0, 0);
    verifyTrustedAuthHeaderMetrics(4);
    operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    verifyMetrics(0, 0);
    verifyTrustedAuthHeaderMetrics(6);

    // Case 3: No Authorization header, does not work
    headers.remove("Authorization");
    headers.put("X-Trusted-Proxy-Auth-Header", "on");
    transport.setCustomHeaders(headers);
    try {
      openResp = client.OpenSession(openReq);
      fail("Exception exception.");
    } catch (Exception e) {
      verifyTrustedAuthHeaderMetrics(6);
      assertEquals(e.getMessage(), "HTTP Response code: 401");
    }

    // Case 4: Verify that there are no changes in metrics for trusted auth
    // header check if the X-Trusted-Proxy-Auth-Header header is not present
    long successMetricBefore =
        (long) client_.getMetric("impala.thrift-server.hiveserver2-http-frontend."
            + "total-trusted-auth-header-check-success");
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    headers.remove("X-Trusted-Proxy-Auth-Header");
    transport.setCustomHeaders(headers);
    openResp = client.OpenSession(openReq);
    // Account for 1 successful basic auth increment.
    verifyMetrics(1, 0);
    verifyTrustedAuthHeaderMetrics(successMetricBefore);
  }

  /**
   * Tests if sessions are authenticated by verifying the JWT token for connections
   * to the HTTP hiveserver2 endpoint.
   */
  @Test
  public void testHiveserver2JwtAuth() throws Exception {
    String jwksFilename =
        new File(System.getenv("IMPALA_HOME"), "testdata/jwt/jwks_rs256.json").getPath();
    setUp(String.format(
        "--jwt_token_auth=true --jwt_validate_signature=true --jwks_file_path=%s "
            + "--jwt_allow_without_tls=true",
        jwksFilename));
    verifyMetrics(0, 0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();

    // Case 1: Authenticate with valid JWT Token in HTTP header.
    String jwtToken =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1m"
        + "NzlkYTUwYjViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoia"
        + "W1wYWxhIn0.OW5H2SClLlsotsCarTHYEbqlbRh43LFwOyo9WubpNTwE7hTuJDsnFoVrvHiWI"
        + "02W69TZNat7DYcC86A_ogLMfNXagHjlMFJaRnvG5Ekag8NRuZNJmHVqfX-qr6x7_8mpOdU55"
        + "4kc200pqbpYLhhuK4Qf7oT7y9mOrtNrUKGDCZ0Q2y_mizlbY6SMg4RWqSz0RQwJbRgXIWSgc"
        + "bZd0GbD_MQQ8x7WRE4nluU-5Fl4N2Wo8T9fNTuxALPiuVeIczO25b5n4fryfKasSgaZfmk0C"
        + "oOJzqbtmQxqiK9QNSJAiH2kaqMwLNgAdgn8fbd-lB1RAEGeyPH8Px8ipqcKsPk0bg";
    headers.put("Authorization", "Bearer " + jwtToken);
    headers.put("X-Forwarded-For", "127.0.0.1");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'impala' from JWT token and use it as
    // login user.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(0, 0);
    verifyJwtAuthMetrics(1, 0);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "impala");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(0, 0);
    verifyJwtAuthMetrics(3, 0);

    // case 2: Authenticate fails with invalid JWT token which does not have signature.
    String invalidJwtToken =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1m"
        + "NzlkYTUwYjViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoia"
        + "W1wYWxhIn0.";
    headers.put("Authorization", "Bearer " + invalidJwtToken);
    headers.put("X-Forwarded-For", "127.0.0.1");
    transport.setCustomHeaders(headers);
    try {
      openResp = client.OpenSession(openReq);
      fail("Exception exception.");
    } catch (Exception e) {
      verifyJwtAuthMetrics(3, 1);
      assertEquals(e.getMessage(), "HTTP Response code: 401");
    }
  }

  /**
   * Tests LDAP for reading Impala table through JDBC external data source.
   * Assume that Impala JDBC driver has been downloaded and copied to
   * ${FILESYSTEM_PREFIX}/test-warehouse/data-sources/jdbc-drivers/.
   */
  @Test
  public void testImpalaExtJdbcTables() throws Exception {
    setUp("");
    verifyMetrics(0, 0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();
    // Authenticate as 'Test1Ldap' with password '12345'
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    TSessionHandle session = openResp.getSessionHandle();
    // One successful authentication.
    verifyMetrics(1, 0);

    // Define queries.
    String fileSystemPrefix = System.getenv("FILESYSTEM_PREFIX");
    String internalListenHost = System.getenv("INTERNAL_LISTEN_HOST");
    String dropTableQuery = "DROP TABLE IF EXISTS %s";
    // Set JDBC authentication mechanisms as LDAP (3) with username/password as
    // TEST_USER_1/TEST_PASSWORD_1.
    String createTableQuery =
        String.format("CREATE EXTERNAL TABLE impala_jdbc_ext_test_table (" +
        "id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT, " +
        "int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE, " +
        "date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP) " +
        "STORED BY JDBC TBLPROPERTIES (" +
        "\"database.type\"=\"IMPALA\", " +
        "\"jdbc.url\"=\"jdbc:impala://%s:21050/functional\", " +
        "\"jdbc.auth\"=\"AuthMech=3\", " +
        "\"jdbc.driver\"=\"com.cloudera.impala.jdbc.Driver\", " +
        "\"driver.url\"=\"%s/test-warehouse/data-sources/jdbc-drivers/" +
        "ImpalaJDBC42.jar\", " +
        "\"dbcp.username\"=\"%s\", " +
        "\"dbcp.password\"=\"%s\", " +
        "\"table\"=\"alltypes\")",
        internalListenHost, fileSystemPrefix, TEST_USER_1, TEST_PASSWORD_1);
    // Set JDBC authentication mechanisms as LDAP with wrong password.
    String createTableWithWrongPassword =
        String.format("CREATE EXTERNAL TABLE impala_jdbc_tbl_wrong_password (" +
        "id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT, " +
        "int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE, " +
        "date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP) " +
       "STORED BY JDBC TBLPROPERTIES (" +
        "\"database.type\"=\"IMPALA\", " +
        "\"jdbc.url\"=\"jdbc:impala://%s:21050/functional\", " +
        "\"jdbc.auth\"=\"AuthMech=3\", " +
        "\"jdbc.driver\"=\"com.cloudera.impala.jdbc.Driver\", " +
        "\"driver.url\"=\"%s/test-warehouse/data-sources/jdbc-drivers/" +
        "ImpalaJDBC42.jar\", " +
        "\"dbcp.username\"=\"%s\", " +
        "\"dbcp.password\"=\"wrong-password\", " +
        "\"table\"=\"alltypes\")",
        internalListenHost, fileSystemPrefix, TEST_USER_1);
    // Set JDBC authentication mechanisms as LDAP without AuthMech.
    String createTableWithoutAuthMech =
        String.format("CREATE EXTERNAL TABLE impala_jdbc_tbl_without_auth_mech (" +
        "id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT, " +
        "int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE, " +
        "date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP) " +
        "STORED BY JDBC TBLPROPERTIES (" +
        "\"database.type\"=\"IMPALA\", " +
        "\"jdbc.url\"=\"jdbc:impala://%s:21050/functional\", " +
        "\"jdbc.driver\"=\"com.cloudera.impala.jdbc.Driver\", " +
        "\"driver.url\"=\"%s/test-warehouse/data-sources/jdbc-drivers/" +
        "ImpalaJDBC42.jar\", " +
        "\"dbcp.username\"=\"%s\", " +
        "\"dbcp.password\"=\"%s\", " +
        "\"table\"=\"alltypes\")",
        internalListenHost, fileSystemPrefix, TEST_USER_1, TEST_PASSWORD_1);
    String selectQuery = "select string_col from %s where id=9";

    // Run queries.
    //
    // Create JDBC tables.
    execAndFetch(client, session,
        String.format(dropTableQuery, "impala_jdbc_ext_test_table"), null);
    execAndFetch(client, session, createTableQuery, "Table has been created.");
    execAndFetch(client, session,
        String.format(dropTableQuery, "impala_jdbc_tbl_wrong_password"), null);
    execAndFetch(client, session, createTableWithWrongPassword,
        "Table has been created.");
    execAndFetch(client, session,
        String.format(dropTableQuery, "impala_jdbc_tbl_without_auth_mech"), null);
    execAndFetch(client, session, createTableWithoutAuthMech, "Table has been created.");

    // Successfully access JDBC data source table with LDAP.
    execAndFetch(client, session,
        String.format(selectQuery, "impala_jdbc_ext_test_table"), "9");
    // Negative case for JDBC data source table with wrong password.
    String expectedError = "Error initialized or created transport for authentication";
    try {
      execAndFetch(client, session,
          String.format(selectQuery, "impala_jdbc_tbl_wrong_password"), "9");
      fail("Expected error: " + expectedError);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(expectedError));
    }
    // Negative case for JDBC data source table without AuthMech.
    expectedError = "Communication link failure. Failed to connect to server";
    try {
      execAndFetch(client, session,
          String.format(selectQuery, "impala_jdbc_tbl_without_auth_mech"), "9");
      fail("Expected error: " + expectedError);
    } catch (Exception e) {
      assertTrue(String.format("Authentication failed with error: %s", e.getMessage()),
          e.getMessage().contains(expectedError));
    }

    // Drop JDBC tables.
    execAndFetch(client, session,
        String.format(dropTableQuery, "impala_jdbc_ext_test_table"),
        "Table has been dropped.");
    execAndFetch(client, session,
        String.format(dropTableQuery, "impala_jdbc_tbl_wrong_password"),
        "Table has been dropped.");
    execAndFetch(client, session,
        String.format(dropTableQuery, "impala_jdbc_tbl_without_auth_mech"),
        "Table has been dropped.");

    // Two successful authentications for each ExecAndFetch().
    verifyMetrics(25, 0);
  }
}
