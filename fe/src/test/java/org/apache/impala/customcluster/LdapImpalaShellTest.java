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
import org.junit.AfterClass;
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

  // The cluster will be set up to allow TEST_USER_1 to act as a proxy for delegateUser_.
  // Includes a special character to test HTTP path encoding.
  private static final String delegateUser_ = "proxyUser$";

  public void setUp(String extraArgs) throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String ldapArgs =
        String.format("--enable_ldap_auth --ldap_uri='%s' --ldap_bind_pattern='%s' "
                + "--ldap_passwords_in_clear_ok %s",
            uri, dn, extraArgs);
    int ret = CustomClusterRunner.StartImpalaCluster(ldapArgs);
    assertEquals(ret, 0);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    CustomClusterRunner.StartImpalaCluster();
  }

  /**

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
    return Boolean.parseBoolean(RunShellCommand.Run(cmd, true, "", "").replace("\n", ""));
  }

  /**
   * Tests ldap authentication using impala-shell.
   */
  @Test
  public void testShellLdapAuth() throws Exception {
    setUp("");
    String query = "select logged_in_user()";
    // Templated shell commands to test a simple 'show tables' command.
    // 1. Valid username and password. Should succeed.
    String[] validCommand = {"impala-shell.sh", "", "--ldap", "--auth_creds_ok_in_clear",
        String.format("--user=%s", TEST_USER_1),
        String.format("--ldap_password_cmd=printf %s", TEST_PASSWORD_1),
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
      RunShellCommand.Run(validCommand, /*shouldSucceed*/ true, TEST_USER_1,
          "Starting Impala Shell with LDAP-based authentication");
      invalidCommand[1] = protocol;
      RunShellCommand.Run(
          invalidCommand, /*shouldSucceed*/ false, "", "Not connected to Impala");
      commandWithoutAuth[1] = protocol;
      RunShellCommand.Run(
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
    setUp(String.format(
        "--authorized_proxy_user_config=%s=%s", TEST_USER_1, delegateUser_));
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
    RunShellCommand.Run(command, /* shouldSucceed */ false, "",
        "Not connected to Impala");

    // Successfully delegate.
    command = buildCommand(
        query, "hs2-http", TEST_USER_1, TEST_PASSWORD_1, "/?doAs=" + delegateUser_);
    RunShellCommand.Run(command, /* shouldSucceed */ true, delegateUser_, "");
  }

  /**
   * Tests the LDAP user and group filter configs.
   */
  @Test
  public void testLdapFilters() throws Exception {
    String groupDN = "cn=%s,ou=Groups,dc=myorg,dc=com";
    // These correspond to the values in fe/src/test/resources/users.ldif
    setUp(String.format("--ldap_group_filter=%s,another-group "
            + "--ldap_user_filter=%s,%s,another-user "
            + "--ldap_group_dn_pattern=%s "
            + "--ldap_group_membership_key=uniqueMember "
            + "--ldap_group_class_key=groupOfUniqueNames",
        TEST_USER_GROUP, TEST_USER_1, TEST_USER_3, groupDN));
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
}
