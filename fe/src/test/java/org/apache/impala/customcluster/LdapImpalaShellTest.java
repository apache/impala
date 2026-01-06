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
import static org.apache.impala.testutil.TestUtils.retryUntilSuccess;
import static org.apache.impala.testutil.TestUtils.writeCookieSecret;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Range;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.impala.testutil.InterruptibleProxyServer;
import org.apache.impala.testutil.WebClient;
import org.junit.After;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala shell connectivity tests for LDAP authentication. This class contains the common
 * test and methods that are used to test Simple bind and Search bind authentication.
 */
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class LdapImpalaShellTest {
  private final static Logger LOG = LoggerFactory.getLogger(LdapImpalaShellTest.class);

  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  // The cluster will be set up to allow TEST_USER_1 to act as a proxy for delegateUser_.
  // Includes a special character to test HTTP path encoding.
  protected static final String delegateUser_ = "proxyUser$";

  private WebClient client_ = new WebClient();

  public void setUp(String extraArgs) throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String ldapArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
            + "--ldap_passwords_in_clear_ok %s",
        uri, extraArgs);
    int ret = startImpalaCluster(ldapArgs);
    assertEquals(ret, 0);
    verifyMetrics(zero, zero, zero, zero);
  }

  @After
  public void cleanUp() throws Exception {
    client_.close();
  }

  protected int startImpalaCluster(String args) throws IOException, InterruptedException {
    return CustomClusterRunner.StartImpalaCluster(args);
  }

  /**
   * Checks if the local python supports SSLContext needed by shell http
   * transport tests. Python version shipped with CentOS6 is known to
   * have an older version of python resulting in test failures.
   */
  protected boolean pythonSupportsSSLContext() throws Exception {
    // Runs the following command:
    // python -c "import ssl; print hasattr(ssl, 'create_default_context')"
    String[] cmd = {
        "python", "-c", "import ssl; print(hasattr(ssl, 'create_default_context'))"};
    return Boolean.parseBoolean(
        RunShellCommand.Run(cmd, true, "", "").stdout.replace("\n", ""));
  }

  /**
   * Returns list of transport protocols: "beeswax", "hs2" is always available,
   * "hs2-http" is not available on older version of python.
   */
  protected List<String> getProtocolsToTest() throws Exception {
    List<String> protocolsToTest = Arrays.asList("beeswax", "hs2");
    if (pythonSupportsSSLContext()) {
      // http transport tests will fail with older python versions (IMPALA-8873)
      protocolsToTest = Arrays.asList("beeswax", "hs2", "hs2-http");
    }
    return protocolsToTest;
  }

  protected String authMethod() {
    return "basic";
  }

  private void verifyMetrics(Range<Long> expectedBasicSuccess,
      Range<Long> expectedBasicFailure, Range<Long> expectedCookieSuccess,
      Range<Long> expectedCookieFailure) throws Exception {
    long actualBasicSuccess = (long) client_.getMetric("impala.thrift-server"
        + ".hiveserver2-http-frontend.total-" + authMethod() + "-auth-success");
    assertTrue("Expected: " + expectedBasicSuccess + ", Actual: " + actualBasicSuccess,
        expectedBasicSuccess.contains(actualBasicSuccess));
    long actualBasicFailure = (long) client_.getMetric("impala.thrift-server"
        + ".hiveserver2-http-frontend.total-" + authMethod() + "-auth-failure");
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

  private static final Range<Long> zero = Range.closed(0L, 0L);
  private static final Range<Long> one = Range.closed(1L, 1L);

  protected String[] buildCommand(
      String query, String protocol, String user, String password, String httpPath) {
    String[] command = {"impala-shell.sh", "--protocol=" + protocol, "--ldap",
        "--auth_creds_ok_in_clear", "--user=" + user,
        "--ldap_password_cmd=printf " + password, "--query=" + query,
        "--http_path=" + httpPath};
    return command;
  }

  /**
   * Tests ldap authentication using impala-shell.
   */
  protected void testShellLdapAuthImpl(String extra_cookie) throws Exception {
    String query = "select logged_in_user()";
    // Templated shell commands to test a simple 'show tables' command.
    // 1. Valid username, password and default http_cookie_names. Should succeed with
    // cookies are being used.
    String[] validCommand = {"impala-shell.sh", "", "--ldap", "--auth_creds_ok_in_clear",
        "--verbose",
        String.format("--user=%s", TEST_USER_1),
        String.format("--ldap_password_cmd=printf %s", TEST_PASSWORD_1),
        String.format("--query=%s", query)};
    // 2. Valid username, password and matching http_cookie_names. Should succeed with
    // cookies are being used.
    String[] validCommandMatchingCookieNames = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", "--verbose", "--http_cookie_names=impala.auth",
        String.format("--user=%s", TEST_USER_1),
        String.format("--ldap_password_cmd=printf %s", TEST_PASSWORD_1),
        String.format("--query=%s", query)};
    // 3. Valid username and password, but not matching http_cookie_names. Should succeed
    // with cookies are not being used.
    String[] validCommandMismatchingCookieNames = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", "--verbose", "--http_cookie_names=impala.conn",
        String.format("--user=%s", TEST_USER_1),
        String.format("--ldap_password_cmd=printf %s", TEST_PASSWORD_1),
        String.format("--query=%s", query)};
    // 4. Valid username and password, but empty http_cookie_names. Should succeed with
    // cookies are not being used.
    String[] validCommandEmptyCookieNames = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", "--verbose", "--http_cookie_names=",
        String.format("--user=%s", TEST_USER_1),
        String.format("--ldap_password_cmd=printf %s", TEST_PASSWORD_1),
        String.format("--query=%s", query)};
    // 5. Invalid username password combination. Should fail.
    String[] invalidCommand = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", "--user=foo", "--ldap_password_cmd=printf bar",
        String.format("--query=%s", query)};
    // 6. Without username and password. Should fail.
    String[] commandWithoutAuth = {
        "impala-shell.sh", "", String.format("--query=%s", query)};
    // 7. Valid username with long password to trigger IMPALA-13746. Should succeed.
    String[] validCommandLongPsw = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", "--verbose",
        String.format("--user=%s", TEST_USER_LONG_PSW),
        String.format("--ldap_password_cmd=printf %s", TEST_PASSWORD_LONG),
        String.format("--query=%s", query)};
    String protocolTemplate = "--protocol=%s";

    // Sorted list of cookies for validCommand, where all cookies are preserved.
    List<String> preservedCookiesList = new ArrayList<>();
    preservedCookiesList.add("impala.auth");
    if (extra_cookie != null) {
      preservedCookiesList.add(extra_cookie);
    }
    Collections.sort(preservedCookiesList);
    String preservedCookies = String.join(", ", preservedCookiesList);

    for (String p : getProtocolsToTest()) {
      String protocol = String.format(protocolTemplate, p);
      validCommand[1] = protocol;
      RunShellCommand.Output result = RunShellCommand.Run(validCommand,
          /*shouldSucceed*/ true, TEST_USER_1,
          "Starting Impala Shell with LDAP-based authentication");
      if (p.equals("hs2-http")) {
        // Check that cookies are being used.
        verifyMetrics(Range.atLeast(1L), zero, Range.atLeast(1L), zero);
        assertTrue(result.stderr,
            result.stderr.contains("Preserving cookies: " + preservedCookies));
      }
      validCommandMatchingCookieNames[1] = protocol;
      result = RunShellCommand.Run(validCommandMatchingCookieNames,
          /*shouldSucceed*/ true, TEST_USER_1,
          "Starting Impala Shell with LDAP-based authentication");
      if (p.equals("hs2-http")) {
        // Check that cookies are being used.
        verifyMetrics(Range.atLeast(2L), zero, Range.atLeast(2L), zero);
        assertTrue(result.stderr,
            result.stderr.contains("Preserving cookies: impala.auth"));
      }
      validCommandMismatchingCookieNames[1] = protocol;
      result = RunShellCommand.Run(validCommandMismatchingCookieNames,
          /*shouldSucceed*/ true, TEST_USER_1,
          "Starting Impala Shell with LDAP-based authentication");
      if (p.equals("hs2-http")) {
        // Check that cookies are NOT being used.
        verifyMetrics(Range.atLeast(2L), zero, Range.atLeast(2L), zero);
        assertFalse(result.stderr, result.stderr.contains("Preserving cookies:"));
      }
      validCommandEmptyCookieNames[1] = protocol;
      result = RunShellCommand.Run(validCommandEmptyCookieNames, /*shouldSucceed*/ true,
          TEST_USER_1, "Starting Impala Shell with LDAP-based authentication");
      if (p.equals("hs2-http")) {
        // Check that cookies are NOT being used.
        verifyMetrics(Range.atLeast(2L), zero, Range.atLeast(2L), zero);
        assertFalse(result.stderr, result.stderr.contains("Preserving cookies:"));
      }

      invalidCommand[1] = protocol;
      RunShellCommand.Run(
          invalidCommand, /*shouldSucceed*/ false, "", "Not connected to Impala");
      commandWithoutAuth[1] = protocol;
      RunShellCommand.Run(
          commandWithoutAuth, /*shouldSucceed*/ false, "", "Not connected to Impala");

      validCommandLongPsw[1] = protocol;
      RunShellCommand.Run(validCommandLongPsw,
          /*shouldSucceed*/ true, TEST_USER_LONG_PSW,
          "Starting Impala Shell with LDAP-based authentication");
    }
  }

  /**
   * Tests user impersonation over the HTTP protocol by using the HTTP path to specify the
   * 'doAs' parameter.
   */
  protected void testHttpImpersonationImpl() throws Exception {
    // Ignore the test if python SSLContext support is not available.
    Assume.assumeTrue(pythonSupportsSSLContext());
    String invalidDelegateUser = "invalid-delegate-user";
    String query = "select logged_in_user()";
    String errTemplate = "User '%s' is not authorized to delegate to '%s'";

    // Run with an invalid proxy user.
    String[] command = buildCommand(
        query, "hs2-http", TEST_USER_2, TEST_PASSWORD_2, "/?doAs=" + delegateUser_);
    RunShellCommand.Run(command, /* shouldSucceed */ false, "",
        String.format(errTemplate, TEST_USER_2, delegateUser_));

    // Run with a valid proxy user but invalid delegate user.
    command = buildCommand(
        query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/?doAs=" + invalidDelegateUser);
    RunShellCommand.Run(command, /* shouldSucceed */ false, "",
        String.format(errTemplate, TEST_USER_1, invalidDelegateUser));

    // 'doAs' parameter that cannot be decoded.
    command = buildCommand(query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/?doAs=%");
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Successfully delegate.
    command = buildCommand(
        query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/?doAs=" + delegateUser_);
    RunShellCommand.Run(command, /* shouldSucceed */ true, delegateUser_, "");
  }

  /**
   * Used to configure and return a temporary cookie secret file. This is needed to set
   * --cookie_secret_file=file.getCanonicalPath() and pass to testCookieRefreshImpl.
   */
  protected File getCookieSecretFile() throws Exception {
    // Write a temporary key file for the cookie secret.
    File keyFile = tempFolder.newFile();
    writeCookieSecret(keyFile);
    return keyFile;
  }

  /**
   * Tests cookie rotation during a query does not interrupt the session.
   */
  protected void testCookieRefreshImpl(File keyFile, String[] env) throws Exception {
    String query = "select sleep(3000)";
    String[] command = ArrayUtils.add(
        buildCommand(query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/cliservice"),
        "--use_new_http_connection");

    final RunShellCommand.Output[] resultHolder = new RunShellCommand.Output[1];
    Thread thread = new Thread(() -> {
      try {
        resultHolder[0] = RunShellCommand.Run(command, env,
            /* shouldSucceed */ true, /* sleep returns true */ "true",
            "Starting Impala Shell with LDAP-based authentication");
      } catch (Throwable e) {
        resultHolder[0] = new RunShellCommand.Output("", e.getMessage());
      }
    });
    thread.start();

    // Poll the metric until authentication succeeds, then update the cookie secret file.
    // Success can be seen from timing: client authenticates, key is updated, client
    // continues and finishes the query. The relevant log lines are:
    //   Loaded authentication key from ...
    //   Opened session
    //   Loaded authentication key from ...
    //   Invalid cookie provided
    //   Closed session
    try {
      retryUntilSuccess(() -> {
        long success = (long) client_.getMetric("impala.thrift-server."
            + "hiveserver2-http-frontend.total-" + authMethod() + "-auth-success");
        if (success < 1L) throw new Exception("Authentication not yet succeeded.");
        return null;
      }, 20, 100);
      writeCookieSecret(keyFile);
      thread.join(5000);
    } finally {
      if (resultHolder[0] != null) LOG.info(resultHolder[0].stderr);
    }

    RunShellCommand.Output result = resultHolder[0];
    assertTrue(result.stderr,
        result.stderr.contains("Preserving cookies: impala.auth"));
    assertTrue(result.stderr, result.stderr.contains("Fetched 1 row"));
    // Cookie auth should fail once due to key change, requiring two basic auths.
    // Cookie auth is expected to succeed at least once, possibly many times.
    verifyMetrics(Range.closed(2L, 2L), zero, Range.atLeast(1L), one);
  }

  /**
   * Tests that an interrupted connection reconnects.
   */
  protected void testReconnectImpl(String[] env) throws Exception {
    // impala-shell now re-uses connections. Add an interruptible proxy to force new
    // connections to be created.
    final RunShellCommand.Output[] resultHolder = new RunShellCommand.Output[1];
    try (InterruptibleProxyServer proxy =
        new InterruptibleProxyServer("localhost", 28000)) {
      proxy.start();

      String query = "select sleep(3000)";
      String[] command = ArrayUtils.add(
          buildCommand(query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/cliservice"),
          "--impalad=localhost:" + proxy.getLocalPort());

      Thread thread = new Thread(() -> {
        try {
          resultHolder[0] = RunShellCommand.Run(command, env,
              /* shouldSucceed */ true, /* sleep returns true */ "true",
              "Starting Impala Shell with LDAP-based authentication");
        } catch (Throwable e) {
          resultHolder[0] = new RunShellCommand.Output("", e.getMessage());
        }
      });
      thread.start();

      // Wait until the query is started before closing the connection.
      retryUntilSuccess(() -> {
        long success = (long) client_.getMetric("impala-server.num-queries-registered");
        if (success < 1L) throw new Exception("Query not yet registered.");
        return null;
      }, 20, 100);
      proxy.closeConnections();
      thread.join(5000);
    } finally {
      if (resultHolder[0] != null) LOG.info(resultHolder[0].stderr);
    }

    // Query should succeed.
    RunShellCommand.Output result = resultHolder[0];
    assertTrue(result.stderr, result.stderr.contains("Fetched 1 row"));
  }

  /**
   * Tests the LDAP user and group filter configs.
   */
  protected void testLdapFiltersImpl() throws Exception {
    String query = "select logged_in_user()";

    // Run with user that passes the group filter but not the user filter, should fail.
    String[] command =
        buildCommand(query, "hs2-http", TEST_USER_2, TEST_PASSWORD_2, "/cliservice");
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Run with user that passes the user filter but not the group filter, should fail.
    command =
        buildCommand(query, "hs2-http", TEST_USER_3, TEST_PASSWORD_3, "/cliservice");
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Run with user that doesn't pass either filter, should fail.
    command =
        buildCommand(query, "hs2-http", TEST_USER_4, TEST_PASSWORD_4, "/cliservice");
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Run with user that passes both filters, should succeed.
    command =
        buildCommand(query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/cliservice");
    RunShellCommand.Run(command, /* shouldSucceed */ true, TEST_USER_1, "");
  }

  /**
   * Tests the interaction between LDAP user and group filter configs and proxy user
   * configs.
   */
  protected void testLdapFiltersWithProxyImpl() throws Exception {
    String query = "select logged_in_user()";
    // Run as the proxy user with a delegate that passes both filters, should succeed
    // and return the delegate user's name.
    String[] command = buildCommand(
        query, "hs2-http", TEST_USER_4, TEST_PASSWORD_4, "/?doAs=" + TEST_USER_1);
    RunShellCommand.Run(command, /* shouldSucceed */ true, TEST_USER_1, "");

    // Run as the proxy user with a delegate that only passes the user filter, should
    // fail.
    command = buildCommand(
        query, "hs2-http", TEST_USER_4, TEST_PASSWORD_4, "/?doAs=" + TEST_USER_3);
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Run as the proxy user with a delegate that only passes the group filter, should
    // fail.
    command = buildCommand(
        query, "hs2-http", TEST_USER_4, TEST_PASSWORD_4, "/?doAs=" + TEST_USER_2);
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Run as the proxy with a delegate that doesn't pass either filter, should fail.
    command = buildCommand(
        query, "hs2-http", TEST_USER_4, TEST_PASSWORD_4, "/?doAs=" + TEST_USER_4);
    RunShellCommand.Run(
        command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Run as the proxy without a delegate user, should fail since the proxy user won't
    // pass the filters.
    command = buildCommand(query, "hs2-http", TEST_USER_4, TEST_PASSWORD_4, "/");
    RunShellCommand.Run(command, /* shouldSucceed */ false, "", "");
  }
}
