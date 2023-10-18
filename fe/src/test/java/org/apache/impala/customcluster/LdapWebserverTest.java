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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.http.cookie.Cookie;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.NameValuePair;
import org.apache.impala.testutil.WebClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class LdapWebserverTest {
  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  private static final Range<Long> zero = Range.closed(0L, 0L);

  WebClient client_ = new WebClient(TEST_USER_1, TEST_PASSWORD_1);

  public void setUp(String extraArgs, String startArgs, String catalogdArgs,
      String stateStoredArgs, String admissiondArgs) throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String impalaArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
            + "--ldap_bind_pattern='%s' --ldap_passwords_in_clear_ok "
            + "--webserver_require_ldap=true --webserver_ldap_passwords_in_clear_ok "
            + "--cookie_require_secure=false %s",
        uri, dn, extraArgs);
    Map<String, String> env = new HashMap<>();
    env.put("IMPALA_WEBSERVER_USERNAME", TEST_USER_1);
    env.put("IMPALA_WEBSERVER_PASSWORD", TEST_PASSWORD_1);
    catalogdArgs = catalogdArgs + " " + impalaArgs;
    stateStoredArgs = stateStoredArgs + " " + impalaArgs;
    int ret = CustomClusterRunner.StartImpalaCluster(impalaArgs, catalogdArgs,
        stateStoredArgs, admissiondArgs, env, startArgs);
    assertEquals(0, ret);
  }

  @After
  public void cleanUp() throws IOException {
    client_.Close();
  }

  private void verifyMetrics(Range<Long> expectedBasicSuccess,
      Range<Long> expectedBasicFailure, Range<Long> expectedCookieSuccess,
      Range<Long> expectedCookieFailure) throws Exception {
    long actualBasicSuccess =
        (long) client_.getMetric("impala.webserver.total-basic-auth-success");
    assertTrue("Expected: " + expectedBasicSuccess + ", Actual: " + actualBasicSuccess,
        expectedBasicSuccess.contains(actualBasicSuccess));
    long actualBasicFailure =
        (long) client_.getMetric("impala.webserver.total-basic-auth-failure");
    assertTrue("Expected: " + expectedBasicFailure + ", Actual: " + actualBasicFailure,
        expectedBasicFailure.contains(actualBasicFailure));

    long actualCookieSuccess =
        (long) client_.getMetric("impala.webserver.total-cookie-auth-success");
    assertTrue("Expected: " + expectedCookieSuccess + ", Actual: " + actualCookieSuccess,
        expectedCookieSuccess.contains(actualCookieSuccess));
    long actualCookieFailure =
        (long) client_.getMetric("impala.webserver.total-cookie-auth-failure");
    assertTrue("Expected: " + expectedCookieFailure + ", Actual: " + actualCookieFailure,
        expectedCookieFailure.contains(actualCookieFailure));
  }

  private void verifyTrustedDomainMetrics(Range<Long> expectedSuccess) throws Exception {
    long actualSuccess = (long) client_
        .getMetric("impala.webserver.total-trusted-domain-check-success");
    assertTrue("Expected: " + expectedSuccess + ", Actual: " + actualSuccess,
        expectedSuccess.contains(actualSuccess));
  }

  private void verifyTrustedAuthHeaderMetrics(Range<Long> expectedSuccess)
      throws Exception {
    long actualSuccess = (long) client_.getMetric(
        "impala.webserver.total-trusted-auth-header-check-success");
    assertTrue("Expected: " + expectedSuccess + ", Actual: " + actualSuccess,
        expectedSuccess.contains(actualSuccess));
  }

  private void verifyJwtAuthMetrics(
      Range<Long> expectedAuthSuccess, Range<Long> expectedAuthFailure) throws Exception {
    long actualAuthSuccess =
        (long) client_.getMetric("impala.webserver.total-jwt-token-auth-success");
    assertTrue("Expected: " + expectedAuthSuccess + ", Actual: " + actualAuthSuccess,
        expectedAuthSuccess.contains(actualAuthSuccess));
    long actualAuthFailure =
        (long) client_.getMetric("impala.webserver.total-jwt-token-auth-failure");
    assertTrue("Expected: " + expectedAuthFailure + ", Actual: " + actualAuthFailure,
        expectedAuthFailure.contains(actualAuthFailure));
  }

  @Test
  public void testWebserver() throws Exception {
    setUp("", "", "", "", "");
    // start-impala-cluster contacts the webui to confirm the impalads have started, so
    // there will already be some successful auth attempts.
    verifyMetrics(Range.atLeast(1L), zero, Range.atLeast(1L), zero);

    // Attempt to access the webserver without a username/password.
    WebClient noUsername = new WebClient();
    String result = noUsername.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is one unsuccessful auth attempt.
    verifyMetrics(Range.atLeast(1L), Range.closed(1L, 1L), Range.atLeast(1L), zero);

    // Attempt to access the webserver with invalid username/password.
    WebClient invalidUserPass = new WebClient("invalid", "invalid");
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
            + "--ldap_group_class_key=groupOfUniqueNames "
            + "--ldap_bind_dn=%s --ldap_bind_password_cmd='echo -n %s' ",
        TEST_USER_GROUP, TEST_USER_1, TEST_USER_3, TEST_USER_DN_1, TEST_PASSWORD_1),
        "", "", "", "");
    // start-impala-cluster contacts the webui to confirm the impalads have started, so
    // there will already be some successful auth attempts.
    verifyMetrics(Range.atLeast(1L), zero, Range.atLeast(1L), zero);

    // Access the webserver with a user that passes the group filter but not the user
    // filter, should fail.
    WebClient user2 = new WebClient(TEST_USER_2, TEST_PASSWORD_2);
    String result = user2.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is one unsuccessful auth attempt.
    verifyMetrics(Range.atLeast(1L), Range.closed(1L, 1L), Range.atLeast(1L), zero);

    // Access the webserver with a user that passes the user filter but not the group
    // filter, should fail.
    WebClient user3 = new WebClient(TEST_USER_3, TEST_PASSWORD_3);
    result = user3.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));
    // Check that there is now two unsuccessful auth attempts.
    verifyMetrics(Range.atLeast(1L), Range.closed(2L, 2L), Range.atLeast(1L), zero);

    // Access the webserver with a user that doesn't pass either filter, should fail.
    WebClient user4 = new WebClient(TEST_USER_4, TEST_PASSWORD_4);
    result = user4.readContent("/");
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
    setUp("", "--per_impalad_args=--metrics_webserver_port=25040 ",
           "--metrics_webserver_port=25021 ",
           "--metrics_webserver_port=25011 ",
           "--metrics_webserver_port=25031 ");
    // Attempt to access the regular webserver without a username/password, should fail.
    WebClient noUsername = new WebClient();
    String result = noUsername.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));

    // Attempt to access the regular webserver with invalid username/password.
    WebClient invalidUserPass = new WebClient("invalid", "invalid");
    result = invalidUserPass.readContent("/");
    assertTrue(result, result.contains("Must authenticate with Basic authentication."));

    // Attempt to access the metrics webserver without a username/password.
    WebClient noUsernameMetrics = new WebClient(25040);
    WebClient catalogdMetrics = new WebClient(25021);
    WebClient statestoredMetrics = new WebClient(25011);
    WebClient admissiondMetrics = new WebClient(25031);
    // Should succeed for the metrics endpoints.
    for (String endpoint :
        new String[] {"/metrics", "/jsonmetrics", "/metrics_prometheus", "/healthz"}) {
      result = noUsernameMetrics.readContent(endpoint);
      assertFalse(
          result, result.contains("Must authenticate with Basic authentication."));
      result = catalogdMetrics.readContent(endpoint);
      assertFalse(
          result, result.contains("Must authenticate with Basic authentication."));
      result = statestoredMetrics.readContent(endpoint);
      assertFalse(
          result, result.contains("Must authenticate with Basic authentication."));
      result = admissiondMetrics.readContent(endpoint);
      assertFalse(
          result, result.contains("Must authenticate with Basic authentication."));
    }

    for (String endpoint : new String[] {"/varz", "/backends"}) {
      result = noUsernameMetrics.readContent(endpoint);
      assertTrue(result, result.contains("No URI handler for"));
    }
  }

  /**
   * Tests if authentication is skipped when connections to the webserver originate
   * from a trusted domain. This is a shared test function that is used for both
   * trusted_domain_strict_localhost=true and false cases.
   */
  private void webserverTrustedDomainTestBody(boolean strictLocalhost) throws Exception {
    String strictLocalhostArgs = "--trusted_domain_strict_localhost=" +
      String.valueOf(strictLocalhost);
    setUp("--trusted_domain=localhost --trusted_domain_use_xff_header=true " +
        strictLocalhostArgs, "", "", "", "");

    // Case 1: Authenticate as 'Test1Ldap' with the right password '12345'
    attemptConnection("Basic VGVzdDFMZGFwOjEyMzQ1", "127.0.0.1", false);
    verifyTrustedDomainMetrics(Range.closed(1L, 1L));

    // Case 2: Authenticate as 'Test1Ldap' without password
    attemptConnection("Basic VGVzdDFMZGFwOg==", "127.0.0.1", false);
    verifyTrustedDomainMetrics(Range.closed(2L, 2L));

    // Case 3: Authenticate as 'Test1Ldap' with the right password
    // '12345' but with a non trusted address in X-Forwarded-For header
    // Sometimes RDNS resolves 127.* addresses as localhost, so this uses a 126.*
    // address for non-strict localhost to avoid RDNS issues.
    String nontrustedIp = strictLocalhost ? "127.0.23.1" : "126.0.23.1";
    attemptConnection("Basic VGVzdDFMZGFwOjEyMzQ1", nontrustedIp, false);
    verifyTrustedDomainMetrics(Range.closed(2L, 2L));

    // Case 4: No auth header, does not work
    try {
      attemptConnection(null, "127.0.0.1", false);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Server returned HTTP response code: 401"));
    }
    verifyTrustedDomainMetrics(Range.closed(2L, 2L));

    // Case 5: Authenticate as 'Test1Ldap' with the no password
    // and a non trusted address in X-Forwarded-For header
    try {
      attemptConnection("Basic VGVzdDFMZGFwOg==", nontrustedIp, false);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Server returned HTTP response code: 401"));
    }
    verifyTrustedDomainMetrics(Range.closed(2L, 2L));

    // Case 6: Verify that there are no changes in metrics for trusted domain
    // check if the X-Forwarded-For header is not present
    long successMetricBefore = (long) client_
        .getMetric("impala.webserver.total-trusted-domain-check-success");
    attemptConnection("Basic VGVzdDFMZGFwOjEyMzQ1", null, false);
    verifyTrustedDomainMetrics(Range.closed(successMetricBefore, successMetricBefore));
  }

  @Test
  public void testWebserverTrustedDomainStrict() throws Exception {
    // Test variant with trusted_domain_strict_localhost=true
    webserverTrustedDomainTestBody(true);
  }

  @Test
  public void testWebserverTrustedDomainNonstrict() throws Exception {
    // Test variant with trusted_domain_strict_localhost=false
    webserverTrustedDomainTestBody(false);
  }

  @Test
  public void testWebserverTrustedDomainEmptyXffHeaderUseOrigin() throws Exception {
    setUp("--trusted_domain=localhost --trusted_domain_use_xff_header=true " +
          "--trusted_domain_empty_xff_header_use_origin=true", "", "", "", "");

    // Case 1: Authenticate as 'Test1Ldap' without password, send X-Forwarded-For header
    attemptConnection("Basic VGVzdDFMZGFwOg==", "127.0.0.1", false);

    // Case 2: Authenticate as 'Test1Ldap' without password, do not send X-Forwarded-For
    // header
    attemptConnection("Basic VGVzdDFMZGFwOg==", null, false);

    // Case 3: Authenticate as 'Test1Ldap' without password, send X-Forwarded-For header
    // that does not match trusted_domain
    try {
      attemptConnection("Basic VGVzdDFMZGFwOg==", "126.0.23.1", false);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Server returned HTTP response code: 401"));
    }

    // Case 4: Authenticate as 'Test1Ldap' without password, do not send X-Forwarded-For
    // header and the origin does not match trusted_domain
    setUp("--trusted_domain=any.domain --trusted_domain_use_xff_header=true " +
            "--trusted_domain_empty_xff_header_use_origin=true", "", "", "", "");
    try {
      attemptConnection("Basic VGVzdDFMZGFwOg==", null, false);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Server returned HTTP response code: 401"));
    }
  }

  @Test
  public void testWebserverTrustedAuthHeader() throws Exception {
    setUp("--trusted_auth_header=X-Trusted-Proxy-Auth-Header", "", "", "", "");

    // Case 1: Authenticate as 'Test1Ldap' with the right password '12345'.
    attemptConnection("Basic VGVzdDFMZGFwOjEyMzQ1", null, true);
    verifyTrustedAuthHeaderMetrics(Range.closed(1L, 1L));

    // Case 2: Authenticate as 'Test1Ldap' without password.
    // The password is ignored.
    attemptConnection("Basic VGVzdDFMZGFwOg==", null, true);
    verifyTrustedAuthHeaderMetrics(Range.closed(2L, 2L));

    // Case 3: No Authentication header, does not work.
    try {
      attemptConnection(null, null, true);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Server returned HTTP response code: 401"));
    }
    verifyTrustedAuthHeaderMetrics(Range.closed(2L, 2L));

    // Case 4: Verify that there are no changes in metrics for trusted auth header
    // check if the trusted auth header is not present.
    long successMetricBefore = (long) client_.getMetric(
        "impala.webserver.total-trusted-auth-header-check-success");
    attemptConnection("Basic VGVzdDFMZGFwOjEyMzQ1", null, false);
    verifyTrustedAuthHeaderMetrics(
        Range.closed(successMetricBefore, successMetricBefore));
  }

  /**
   * Tests if sessions are authenticated by verifying the JWT token for connections
   * to the Web Server.
   */
  @Test
  public void testWebserverJwtAuth() throws Exception {
    String jwksFilename =
        new File(System.getenv("IMPALA_HOME"), "testdata/jwt/jwks_rs256.json").getPath();
    setUp(String.format(
              "--jwt_token_auth=true --jwt_validate_signature=true --jwks_file_path=%s "
                  + "--jwt_allow_without_tls=true",
              jwksFilename),
        "", "", "", "");

    // Case 1: Authenticate with valid JWT Token in HTTP header.
    String jwtToken =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1m"
        + "NzlkYTUwYjViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoia"
        + "W1wYWxhIn0.OW5H2SClLlsotsCarTHYEbqlbRh43LFwOyo9WubpNTwE7hTuJDsnFoVrvHiWI"
        + "02W69TZNat7DYcC86A_ogLMfNXagHjlMFJaRnvG5Ekag8NRuZNJmHVqfX-qr6x7_8mpOdU55"
        + "4kc200pqbpYLhhuK4Qf7oT7y9mOrtNrUKGDCZ0Q2y_mizlbY6SMg4RWqSz0RQwJbRgXIWSgc"
        + "bZd0GbD_MQQ8x7WRE4nluU-5Fl4N2Wo8T9fNTuxALPiuVeIczO25b5n4fryfKasSgaZfmk0C"
        + "oOJzqbtmQxqiK9QNSJAiH2kaqMwLNgAdgn8fbd-lB1RAEGeyPH8Px8ipqcKsPk0bg";
    attemptConnection("Bearer " + jwtToken, "127.0.0.1", false);
    verifyJwtAuthMetrics(Range.closed(1L, 1L), zero);

    // Case 2: Failed with invalid JWT Token.
    String invalidJwtToken =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1m"
        + "NzlkYTUwYjViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoia"
        + "W1wYWxhIn0.";
    try {
      attemptConnection("Bearer " + invalidJwtToken, "127.0.0.1", false);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Server returned HTTP response code: 401"));
    }
    verifyJwtAuthMetrics(Range.closed(1L, 1L), Range.closed(1L, 1L));
  }

  /**
   * Print the username closing a session or cancelling a query from the WebUI.
   */
  @Test
  public void testDisplaySrcUsernameInQueryCause() throws Exception {
    setUp("", "", "", "", "");
    // Create client
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

    // Execute a long running query then cancel it from the WebUI.
    // Check the runtime profile and the INFO logs for the cause message.
    TOperationHandle operationHandle = LdapHS2Test.execQueryAsync(
        client, openResp.getSessionHandle(), "select sleep(10000)");
    String queryId = PrintId(operationHandle.getOperationId());
    String cancelQueryUrl = String.format("/cancel_query?query_id=%s", queryId);
    String textProfileUrl = String.format("/query_profile_plain_text?query_id=%s",
            queryId);
    client_.readContent(cancelQueryUrl);
    String response =  client_.readContent(textProfileUrl);
    String cancelStatus = String.format("Cancelled from Impala&apos;s debug web interface"
        + " by user: &apos;%s&apos; at", TEST_USER_1);
    assertTrue(response.contains(cancelStatus));
    // Wait for logs to flush
    TimeUnit.SECONDS.sleep(6);
    response = client_.readContent("/logs");
    assertTrue(response.contains(cancelStatus));

    // Session closing from the WebUI does not produce the cause message in the profile,
    // so we will skip checking the runtime profile.
    String sessionId = PrintId(openResp.getSessionHandle().getSessionId());
    String closeSessionUrl =  String.format("/close_session?session_id=%s", sessionId);
    client_.readContent(closeSessionUrl);
    // Wait for logs to flush
    TimeUnit.SECONDS.sleep(6);
    String closeStatus = String.format("Session closed from Impala&apos;s debug web"
        + " interface by user: &apos;%s&apos; at", TEST_USER_1);
    response = client_.readContent("/logs");
    assertTrue(response.contains(closeStatus));
  }

  /*
   * Test that we can set glog level.
   */
  @Test
  public void testSetGLogLevel() throws Exception {
    setUp("", "", "", "", "");
    // Validate defaults
    JSONObject json = client_.jsonGet("/log_level?json");
    assertEquals("1", json.get("glog_level"));

    // Test GET set_glog_level returns an error
    json = client_.jsonGet("/set_glog_level?glog=0&json");
    assertEquals("1", json.get("glog_level"));
    assertEquals("Use form input to update glog level", json.get("error"));

    // Test GET reset_glog_level returns an error
    json = client_.jsonGet("/reset_glog_level?json");
    assertEquals("1", json.get("glog_level"));
    assertEquals("Use form input to reset glog level", json.get("error"));

    // Clients persist state like 400 errors and cookies. Use new client for each test.
    BasicHeader[] headers = { new BasicHeader("X-Requested-By", "anything") };
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("glog", "0"));

    // Test POST set_glog_level fails
    WebClient client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    String body = client.post("/set_glog_level?json", null, params, 403);
    assertEquals("rejected POST missing X-Requested-By header", body);

    // Test POST reset_glog_level fails
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    body = client.post("/reset_glog_level?json", null, null, 403);
    assertEquals("rejected POST missing X-Requested-By header", body);

    // Test POST set_glog_level with X-Requested-By succeeds
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonPost("/set_glog_level?json", headers, params);
    assertEquals("0", json.get("glog_level"));

    // Test POST reset_glog_level with X-Requested-By succeeds
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonPost("/reset_glog_level?json", headers, null);
    assertEquals("1", json.get("glog_level"));

    // Test POST set_glog_level with cookie gives 403
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonGet("/log_level?json");
    assertEquals("1", json.get("glog_level"));
    body = client.post("/set_glog_level?json", null, params, 403);
    assertEquals("", body);

    // Test POST reset_glog_level with cookie gives 403
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonGet("/log_level?json");
    assertEquals("1", json.get("glog_level"));
    body = client.post("/reset_glog_level?json", null, null, 403);
    assertEquals("", body);

    // Create a new client, get a cookie, and add csrf_token based on the cookie
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonGet("/log_level?json");
    assertEquals("1", json.get("glog_level"));
    String rand = getRandToken(client.getCookies());
    params.add(new BasicNameValuePair("csrf_token", rand));

    // Test POST set_glog_level with cookie and csrf_token succeeds
    json = client.jsonPost("/set_glog_level?json", null, params);
    assertEquals("0", json.get("glog_level"));

    // Test POST reset_glog_level with cookie and csrf_token succeeds
    json = client.jsonPost("/reset_glog_level?json", null, params);
    assertEquals("1", json.get("glog_level"));
  }

  /*
   * Test that we can set java log level.
   */
  @Test
  public void testSetJavaLogLevel() throws Exception {
    setUp("", "", "", "", "");
    // Validate defaults
    JSONObject json = client_.jsonGet("/log_level?json");
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));

    // Test GET set_java_loglevel does nothing
    json = client_.jsonGet("/set_java_loglevel?class=org.apache&level=WARN&json");
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));
    assertEquals("Use form input to update java log levels", json.get("error"));

    // Test GET reset_java_loglevel does nothing
    json = client_.jsonGet("/reset_java_loglevel?json");
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));
    assertEquals("Use form input to reset java log levels", json.get("error"));

    // Clients persist state like 400 errors and cookies. Use new client for each test.
    BasicHeader[] headers = { new BasicHeader("X-Requested-By", "anything") };
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("class", "org.apache"));
    params.add(new BasicNameValuePair("level", "WARN"));

    // Test POST set_java_loglevel fails
    WebClient client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    String body = client.post("/set_java_loglevel?json", null, params, 403);
    assertEquals("rejected POST missing X-Requested-By header", body);

    // Test POST reset_java_loglevel fails
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    body = client.post("/reset_java_loglevel?json", null, null, 403);
    assertEquals("rejected POST missing X-Requested-By header", body);

    // Test POST set_glog_level with X-Requested-By succeeds
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonPost("/set_java_loglevel?json", headers, params);
    assertEquals("org.apache : WARN\norg.apache.impala : DEBUG\n",
        json.get("get_java_loglevel_result"));

    // Test POST reset_glog_level with X-Requested-By succeeds
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonPost("/reset_java_loglevel?json", headers, null);
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));

    // Test POST set_java_loglevel with cookie gives 403
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonGet("/log_level?json");
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));
    body = client.post("/set_java_loglevel?json", null, params, 403);
    assertEquals("", body);

    // Test POST reset_java_loglevel with cookie gives 403
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonGet("/log_level?json");
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));
    body = client.post("/reset_java_loglevel?json", null, null, 403);
    assertEquals("", body);

    // Create a new client, get a cookie, and add csrf_token based on the cookie
    client = new WebClient(TEST_USER_1, TEST_PASSWORD_1);
    json = client.jsonGet("/log_level?json");
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));
    String rand = getRandToken(client.getCookies());
    params.add(new BasicNameValuePair("csrf_token", rand));

    // Test POST set_java_loglevel with cookie and csrf_token succeeds
    json = client.jsonPost("/set_java_loglevel?json", null, params);
    assertEquals("org.apache : WARN\norg.apache.impala : DEBUG\n",
        json.get("get_java_loglevel_result"));

    // Test POST reset_java_loglevel with cookie and csrf_token succeeds
    json = client.jsonPost("/reset_java_loglevel?json", null, params);
    assertEquals("org.apache.impala : DEBUG\n", json.get("get_java_loglevel_result"));
  }

  private String getRandToken(List<Cookie> cookies) {
    for (Cookie cookie : cookies) {
      String[] tokens = cookie.getValue().split("&");
      for (String token : tokens) {
        if (token.charAt(0) == 'r' && token.charAt(1) == '=') {
          String rand = token.substring(2);
          assertTrue("Expected number: " + rand, rand.matches("^[1-9][0-9]*$"));
          return rand;
        }
      }
    }
    fail("Expected cookie to contain random number");
    return "";
  }

  // Helper method to make a get call to the webserver using the input basic
  // auth token, x-forward-for and X-Trusted-Proxy-Auth-Header token.
  private void attemptConnection(String basic_auth_token, String xff_address,
      boolean add_trusted_auth_header) throws Exception {
    String url = "http://localhost:25000/?json";
    URLConnection connection = new URL(url).openConnection();
    if (basic_auth_token != null) {
      connection.setRequestProperty("Authorization", basic_auth_token);
    }
    if (xff_address != null) {
      connection.setRequestProperty("X-Forwarded-For", xff_address);
    }
    if (add_trusted_auth_header) {
      connection.setRequestProperty("X-Trusted-Proxy-Auth-Header", "");
    }
    connection.getInputStream();
  }

  // Helper method to get query id or session id
  private static String PrintId(THandleIdentifier handle) {
    // The binary representation is present in the query handle but we need to
    // massage it into the expected string representation.
    byte[] guid_bytes = handle.getGuid();
    assertEquals(guid_bytes.length,16);
    byte[] low_bytes = ArrayUtils.subarray(guid_bytes, 0, 8);
    byte[] high_bytes = ArrayUtils.subarray(guid_bytes, 8, 16);
    ArrayUtils.reverse(low_bytes);
    ArrayUtils.reverse(high_bytes);
    return Hex.encodeHexString(low_bytes) + ":" + Hex.encodeHexString(high_bytes);
  }
}
