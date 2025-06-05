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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
import org.ietf.jgss.*;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
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
 * SPNEGO authentication is being used.
 */
public class SpnegoAuthTest {
  private static final Logger LOG = LoggerFactory.getLogger(SpnegoAuthTest.class);

  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();
  @ClassRule
  public static KerberosKdcEnvironment kerberosKdcEnvironment =
          new KerberosKdcEnvironment(new TemporaryFolder());

  WebClient client_ = new WebClient();

  protected Map<String, String> getLdapFlags() {
    String ldapUri = String.format("ldap://localhost:%s",
            serverRule.getLdapServer().getPort());
    String passwordCommand = String.format("'echo -n %s'", TEST_PASSWORD_1);
    return ImmutableMap.<String, String>builder()
        .put("enable_ldap_auth", "true")
        .put("ldap_uri", ldapUri)
        .put("ldap_bind_pattern", "cn=#UID,ou=Users,dc=myorg,dc=com")
        .put("ldap_passwords_in_clear_ok", "true")
        .put("ldap_bind_dn", TEST_USER_DN_1)
        .put("ldap_bind_password_cmd", passwordCommand)
        .build();
  }

  protected int startImpalaCluster(String args) throws IOException, InterruptedException {
    return kerberosKdcEnvironment.startImpalaClusterWithArgs(args);
  }

  public static String flagsToArgs(Map<String, String> flags) {
    return flags.entrySet().stream()
            .map(entry -> "--" + entry.getKey() + "=" + entry.getValue() + " ")
            .collect(Collectors.joining());
  }

  @SafeVarargs
  public static Map<String, String> mergeFlags(Map<String, String>... flags) {
    return Arrays.stream(flags)
            .filter(Objects::nonNull)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static void verifySuccess(TStatus status) throws Exception {
    if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS
        || status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
      return;
    }
    throw new Exception(status.toString());
  }

  /**
   * Executes 'query', fetches the results and closes the 'query'. Expects there to be
   * exactly one string returned, which be be equal to 'expectedResult'.
   */
  static void execAndFetch(TCLIService.Iface client,
      TSessionHandle sessionHandle, String query, String expectedResult)
      throws Exception {
    TOperationHandle handle = null;
    try {
      TExecuteStatementReq execReq = new TExecuteStatementReq(sessionHandle, query);
      TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
      verifySuccess(execResp.getStatus());
      handle = execResp.getOperationHandle();

      TFetchResultsReq fetchReq = new TFetchResultsReq(
          handle, TFetchOrientation.FETCH_NEXT, 1000);
      TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
      verifySuccess(fetchResp.getStatus());
      List<TColumn> columns = fetchResp.getResults().getColumns();
      assertEquals(1, columns.size());
      if (expectedResult != null) {
        assertEquals(expectedResult, columns.get(0).getStringVal().getValues().get(0));
      }
    } finally {
      if (handle != null) {
        TCloseOperationReq closeReq = new TCloseOperationReq(handle);
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        verifySuccess(closeResp.getStatus());
      }
    }
  }

  private void verifyNegotiateAuthMetrics(
      long expectedBasicAuthSuccess, long expectedBasicAuthFailure) throws Exception {
    long actualBasicAuthSuccess = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-negotiate-auth-success");
    assertEquals(expectedBasicAuthSuccess, actualBasicAuthSuccess);
    long actualBasicAuthFailure = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-negotiate-auth-failure");
    assertEquals(expectedBasicAuthFailure, actualBasicAuthFailure);
  }

  private void verifyCookieAuthMetrics(
      long expectedCookieAuthSuccess, long expectedCookieAuthFailure) throws Exception {
    long actualCookieAuthSuccess = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-cookie-auth-success");
    assertEquals(expectedCookieAuthSuccess, actualCookieAuthSuccess);
    long actualCookieAuthFailure = (long) client_.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-cookie-auth-failure");
    assertEquals(expectedCookieAuthFailure, actualCookieAuthFailure);
  }

  @Test
  /**
   * Tests Authentication flow using a proxy client such as Knox, which uses SPNEGO Auth
   * to connect to Impala and impersonates other users. Initial Authentication is done
   * through SPNEGO and follow on requests are authenticated using Auth cookies. The test
   * uses multiple clients sharing the same Auth cookie similar to what a proxy client
   * would do and as a result adds coverage for interesting scenarios where OpenSession
   * RPC could also use Auth Cookies.
   */
  public void testImpersonation() throws Exception, Throwable {
    Map<String, String> flags = mergeFlags(
        // enable Kerberos authentication
        kerberosKdcEnvironment.getKerberosAuthFlags(),
        getLdapFlags(),
        // custom LDAP filters
        ImmutableMap.of(
            "ldap_group_filter", String.format("%s,another-group", TEST_USER_GROUP),
            "ldap_user_filter", String.format("%s,%s,another-user",
                TEST_USER_1, TEST_USER_3),
            "ldap_group_dn_pattern", GROUP_DN_PATTERN,
            "ldap_group_membership_key", "uniqueMember",
            "ldap_group_class_key", "groupOfUniqueNames",
            "allow_custom_ldap_filters_with_kerberos_auth", "true",
            // set proxy user: allow TEST_USER_4 to act as a proxy user for others
            "authorized_proxy_user_config", String.format("%s=*", TEST_USER_4)
        )
    );
    // Start Impala with configured flags.
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(0, ret); // cluster should start up

    // Open a session and authenticate using SPNEGO.
    THttpClientWithHeaders transport =
        new THttpClientWithHeaders("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();
    // Authenticate as the proxy user 'Test4Ldap'
    headers.put("Authorization", "Negotiate " + getSpnegoToken(TEST_USER_4));
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session without specifying a 'doas', should fail as the proxy user won't
    // pass the filters.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    assertEquals(TStatusCode.ERROR_STATUS, openResp.getStatus().getStatusCode());
    int negotiateAuthFailureCount = 0;
    int negotiateAuthSuccessCount = 1;
    verifyNegotiateAuthMetrics(negotiateAuthSuccessCount, negotiateAuthFailureCount);
    int cookieAuthFailureCount = 0;
    int cookieAuthSuccessCount = 0;
    verifyCookieAuthMetrics(cookieAuthSuccessCount, cookieAuthFailureCount);

    // SPNEGO doesn't like replay tokens, so use new tokens.
    headers.remove("Authorization");
    headers.put("Authorization", "Negotiate " + getSpnegoToken(TEST_USER_4));
    // Open a session with a 'doas' that will pass both filters, should succeed.
    Map<String, String> config = new HashMap<String, String>();
    config.put("impala.doas.user", TEST_USER_1);
    openReq.setConfiguration(config);
    openResp = client.OpenSession(openReq);
    assertEquals(TStatusCode.SUCCESS_STATUS, openResp.getStatus().getStatusCode());
    negotiateAuthSuccessCount++;
    verifyNegotiateAuthMetrics(negotiateAuthSuccessCount, negotiateAuthFailureCount);
    verifyCookieAuthMetrics(cookieAuthSuccessCount, cookieAuthFailureCount);

    // Use Auth Cookie for the remaining sessions and connections.
    Map<String, List<String>> responseHeaders = transport.getResponseHeaders();
    List<String> cookies = responseHeaders.get("Set-Cookie");
    if (cookies != null) {
      for (String cookie : cookies) {
        String authMech = extractCookieAuthMech(cookie);
        assertNotNull(authMech);
        assertEquals("SPNEGO", authMech);
        headers.put("Cookie", cookie);
      }
    } else {
      fail("'Set-Cookie' cookie not returned from Impala");
    }

    // Simulate 4 concurrent clients, with each running 100 exec and fetch RPCs.
    final int numClients = 4;
    final int numQueries = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numClients);
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numClients; i++) {
      final int clientId = i;
      Future<Void> future = executor.submit(() -> {
          simulateClient(headers, config, clientId, numQueries);
          return null;
      });
      futures.add(future);
    }

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.MINUTES);

    // Check for exceptions from client threads
    for (int i = 0; i < futures.size(); i++) {
      try {
        futures.get(i).get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        System.err.println("Client " + i + " failed: " + cause.getMessage());
        cause.printStackTrace();
        fail("Client " + i + " failed: " + cause.getMessage());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("Main thread interrupted.");
      }
    }
    // Each client uses one OpenSession RPC using cookie based authentication.
    // Each query runs one Exec, one Fetch and one Close RPC, using 3 cookie based
    // authentications per query.
    cookieAuthSuccessCount += numClients * (1 + numQueries * 3);
    verifyCookieAuthMetrics(cookieAuthSuccessCount, cookieAuthFailureCount);
    verifyNegotiateAuthMetrics(negotiateAuthSuccessCount, negotiateAuthFailureCount);
  }

  /**
   * Generates and returns Base64 encoded SPNEGO token for the input user.
   */
  private static String getSpnegoToken(String user) throws Exception {
    // Create a test user principal and generate Kerberos credentials cache (ccache)
    String ccacheFilePath =
        kerberosKdcEnvironment.createUserPrincipalAndCredentialsCache(user);
    File spngeoTokenFile =
        new File(kerberosKdcEnvironment.getTestFolderPath() + "/spngeoToken.bin");
    // Using ProcessBuilder to generate SPNEGO token becasue apparently some of the Java
    // security classes are initialized much earlier and cannot read required kerberos
    // config setup by the test.
    ProcessBuilder pb = new ProcessBuilder(
        "java", "-cp", System.getProperty("java.class.path"),
        "-Djava.security.krb5.conf=" + kerberosKdcEnvironment.getKrb5ConfigPath(),
        "-Dsun.security.krb5.debug=true",
        "-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext,JGSS",
        "-Djavax.security.auth.useSubjectCredsOnly=false",
        "org.apache.impala.customcluster.SpnegoTokenGenerator",
        spngeoTokenFile.getCanonicalPath());

    Map<String, String> env = pb.environment();
    env.put("KRB5CCNAME", "FILE:" + ccacheFilePath);

    pb.inheritIO();
    Process process = pb.start();
    int exitCode = process.waitFor();
    // Non zero exit code indicates token generation failed.
    assertEquals(0, exitCode);

    byte[] token = readTokenFromFile(spngeoTokenFile.getCanonicalPath());
    String base64Token = Base64.getEncoder().encodeToString(token);
    return base64Token;
  }

  /**
   * Helper function to read token from token file generated by SpnegoTokenGenerator.
   */
  private static byte[] readTokenFromFile(String path) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    InputStream is = new FileInputStream(path);
    byte[] temp = new byte[4096];
    int bytesRead;
    while ((bytesRead = is.read(temp)) != -1) {
      buffer.write(temp, 0, bytesRead);
    }
    is.close();
    return buffer.toByteArray();
  }

  /**
   * Simulates a client opening session and running a number of queries within a session.
   */
  private static void simulateClient(Map<String, String> headers,
      Map<String, String> config, int clientId, int numQueries) throws Exception {
    // Create and open the transport
    THttpClientWithHeaders transport =
        new THttpClientWithHeaders("http://localhost:28000");
    transport.setCustomHeaders(headers);
    transport.open();

    // Create client stub
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session
    TOpenSessionReq openReq = new TOpenSessionReq();
    openReq.setConfiguration(config);
    TOpenSessionResp openResp = client.OpenSession(openReq);

    if (openResp.getStatus().getStatusCode() != TStatusCode.SUCCESS_STATUS) {
      throw new RuntimeException("Failed to open session for client " + clientId);
    }

    System.out.println("Client " + clientId + " opened session successfully.");

    // Execute queries
    for (int i = 0; i < numQueries; i++) {
      execAndFetch(client, openResp.getSessionHandle(),
          "select logged_in_user()", "Test1Ldap");
      int sleepMillis = ThreadLocalRandom.current().nextInt(10, 100);
      Thread.sleep(sleepMillis);
    }

    // Close transport
    transport.close();
    System.out.println("Client " + clientId + " finished.");
  }

  /**
   * Extracts auth mechanism from cookie's value.
   */
  private static String extractCookieAuthMech(String cookie) throws Exception {
    if (cookie == null || cookie.isEmpty()) {
      return null;
    }
    // Expect cookie:
    // impala.auth=<base64signature>&<cookie_value>;HttpOnly;Max-Age=86400;Secure.
    String[] cookieFields = cookie.split(";");
    if (cookieFields.length == 0) {
      return null;
    }

    // We've impala.auth=<base64signature>&<cookie_value> as first token with
    // cookie_value like u=Test4Ldap@myorg.com&t=549158755&r=1800557187&a=SPNEGO.
    String[] cookieValueFields = cookieFields[0].trim().split("&");
    assertEquals(5, cookieValueFields.length);
    String[] authMech = cookieValueFields[4].trim().split("=");
    assertEquals(2, authMech.length);
    assertEquals("a", authMech[0]);
    return authMech[1];
  }
}
