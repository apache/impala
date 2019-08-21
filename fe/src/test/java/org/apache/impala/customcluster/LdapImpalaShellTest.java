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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.impala.testutil.ImpalaJdbcClient;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.impala.service.JdbcTestBase;

/**
 * Impala shell connectivity tests with LDAP authentication.
 */
@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class LdapImpalaShellTest {

  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  // These correspond to the values in fe/src/test/resources/users.ldif
  private static final String testUser_ = "Test1Ldap";
  private static final String testPassword_ = "12345";
  private static final String testUser2_ = "Test2Ldap";
  private static final String testPassword2_ = "abcde";

  // The cluster will be set up to allow testUser_ to act as a proxy for delegateUser_.
  // Includes a special character to test HTTP path encoding.
  private static final String delegateUser_ = "proxyUser$";

  @Before
  public void setUp() throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String ldapArgs = String.format(
        "--enable_ldap_auth --ldap_uri='%s' --ldap_bind_pattern='%s' " +
        "--ldap_passwords_in_clear_ok --authorized_proxy_user_config=%s=%s",
        uri, dn, testUser_, delegateUser_);
    int ret = CustomClusterRunner.StartImpalaCluster(ldapArgs);
    assertEquals(ret, 0);
  }

  @After
  public void cleanUp() throws Exception {
    CustomClusterRunner.StartImpalaCluster();
  }

  /**
   * Helper to run a shell command 'cmd'. If 'shouldSucceed' is true, the command
   * is expected to succeed, failure otherwise. Returns the stdout from the command.
   */
  private String runShellCommand(String[] cmd, boolean shouldSucceed, String expectedOut,
      String expectedErr) throws Exception {
    Runtime rt = Runtime.getRuntime();
    Process process = rt.exec(cmd);
    // Collect the stderr.
    BufferedReader input = new BufferedReader(
        new InputStreamReader(process.getErrorStream()));
    StringBuffer stderrBuf = new StringBuffer();
    String line;
    while ((line = input.readLine()) != null) {
      stderrBuf.append(line);
      stderrBuf.append('\n');
    }
    String stderr = stderrBuf.toString();
    assertTrue(stderr, stderr.contains(expectedErr));
    // Collect the stdout (which has the resultsets).
    input = new BufferedReader(new InputStreamReader(process.getInputStream()));
    StringBuffer stdoutBuf = new StringBuffer();
    while ((line = input.readLine()) != null) {
      stdoutBuf.append(line);
      stdoutBuf.append('\n');
    }
    int expectedReturn = shouldSucceed ? 0 : 1;
    assertEquals(stderr.toString(), expectedReturn, process.waitFor());
    // If the query succeeds, assert that the output is correct.
    String stdout = stdoutBuf.toString();
    if (shouldSucceed) {
      assertTrue(stdout, stdout.contains(expectedOut));
    }
    return stdout;
  }

  /**
   * Checks if the local python supports SSLContext needed by shell http
   * transport tests. Python version shipped with CentOS6 is known to
   * have an older version of python resulting in test failures.
   */
  private boolean pythonSupportsSSLContext() throws Exception {
    // Runs the following command:
    // python -c "import ssl; print hasattr(ssl, 'create_default_context')"
    String[] cmd =
        {"python", "-c", "import ssl; print hasattr(ssl, 'create_default_context')"};
    return Boolean.parseBoolean(runShellCommand(cmd, true, "", "").replace("\n", ""));
  }

  /**
   * Tests ldap authentication using impala-shell.
   */
  @Test
  public void testShellLdapAuth() throws Exception {
    String query = "select logged_in_user()";
    // Templated shell commands to test a simple 'show tables' command.
    // 1. Valid username and password. Should succeed.
    String[] validCommand = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", String.format("--user=%s", testUser_),
        String.format("--ldap_password_cmd=printf %s", testPassword_),
        String.format("--query=%s", query)};
    // 2. Invalid username password combination. Should fail.
    String[] invalidCommand = {"impala-shell.sh", "", "--ldap",
        "--auth_creds_ok_in_clear", "--user=foo", "--ldap_password_cmd=printf bar",
        String.format("--query=%s", query)};
    // 3. Without username and password. Should fail.
    String[] commandWithoutAuth =
        {"impala-shell.sh", "", String.format("--query=%s", query)};
    String protocolTemplate = "--protocol=%s";
    List<String> protocolsToTest = Arrays.asList("beeswax", "hs2");
    if (pythonSupportsSSLContext()) {
      // http transport tests will fail with older python versions (IMPALA-8873)
      protocolsToTest = Arrays.asList("beeswax", "hs2", "hs2-http");
    }

    for (String protocol: protocolsToTest) {
      protocol = String.format(protocolTemplate, protocol);
      validCommand[1] = protocol;
      runShellCommand(validCommand, /*shouldSucceed*/ true, testUser_,
          "Starting Impala Shell using LDAP-based authentication");
      invalidCommand[1] = protocol;
      runShellCommand(
          invalidCommand, /*shouldSucceed*/ false, "", "Not connected to Impala");
      commandWithoutAuth[1] = protocol;
      runShellCommand(
          commandWithoutAuth, /*shouldSucceed*/ false, "", "Not connected to Impala");
    }
  }

  private String[] buildCommand(
      String query, String protocol, String user, String password, String httpPath) {
    String[] command = {"impala-shell.sh", "--protocol=" + protocol, "--ldap",
        "--auth_creds_ok_in_clear", "--user=" + user,
        "--ldap_password_cmd=printf " + password, "--query=" + query,
        "--http_path=" + httpPath};
    return command;
  }

  /**
   * Tests user impersonation over the HTTP protocol by using the HTTP path to specify the
   * 'doAs' parameter.
   */
  @Test
  public void testHttpImpersonation() throws Exception {
    // Ignore the test if python SSLContext support is not available.
    Assume.assumeTrue(pythonSupportsSSLContext());
    String invalidDelegateUser = "invalid-delegate-user";
    String query = "select logged_in_user()";
    String errTemplate = "User '%s' is not authorized to delegate to '%s'";

    // Run with an invalid proxy user.
    String[] command = buildCommand(
        query, "hs2-http", testUser2_, testPassword2_, "/?doAs=" + delegateUser_);
    runShellCommand(command, /* shouldSucceed */ false, "",
        String.format(errTemplate, testUser2_, delegateUser_));

    // Run with a valid proxy user but invalid delegate user.
    command = buildCommand(
        query, "hs2-http", testUser_, testPassword_, "/?doAs=" + invalidDelegateUser);
    runShellCommand(command, /* shouldSucceed */ false, "",
        String.format(errTemplate, testUser_, invalidDelegateUser));

    // 'doAs' parameter that cannot be decoded.
    command = buildCommand(
        query, "hs2-http", testUser_, testPassword_, "/?doAs=%");
    runShellCommand(command, /* shouldSucceed */ false, "", "Not connected to Impala");

    // Successfully delegate.
    command = buildCommand(
        query, "hs2-http", testUser_, testPassword_, "/?doAs=" + delegateUser_);
    runShellCommand(command, /* shouldSucceed */ true, delegateUser_, "");
  }
}
