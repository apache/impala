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

import com.google.common.collect.Lists;

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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;


/**
 * Impyla HTTP connectivity tests with LDAP authentication.
 */
@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class LdapImpylaHttpTest {

  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  // Query used by all tests
  private static String query_ = "select logged_in_user()";

  // These correspond to the values in fe/src/test/resources/users.ldif
  private static final String testUser_ = "Test1Ldap";
  private static final String testPassword_ = "12345";
  private static final String testUser2_ = "Test2Ldap";
  private static final String testPassword2_ = "abcde";

  private static final String helper_ = System.getenv("IMPALA_HOME") +
      "/tests/util/run_impyla_http_query.py";

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
   * Tests ldap authentication using impala-shell.
   */
  @Test
  public void testImpylaHttpLdapAuth() throws Exception {
    // 1. Valid username and password. Should succeed.
    String[] validCmd = buildCommand(testUser_, testPassword_, null);
    RunShellCommand.Run(validCmd, /*shouldSucceed*/ true, testUser_, "");
    // 2. Invalid username password combination. Should fail.
    String[] invalidCmd = buildCommand("foo", "bar", null);
    RunShellCommand.Run(invalidCmd, /*shouldSucceed*/ false, "", "EOFError");
    // 3. Without username and password. Should fail.
    String[] noAuthCmd = {"impala-python", helper_, "--query", query_};
    RunShellCommand.Run(noAuthCmd, /*shouldSucceed*/ false, "", "EOFError");
  }

  private String[] buildCommand(String user, String password, String httpPath) {
    List<String> command = Lists.newArrayList(Arrays.asList("impala-python", helper_,
        "--user", user, "--password", password, "--query", query_));
    if (httpPath != null) command.addAll(Arrays.asList("--http_path", httpPath));
    return command.toArray(new String[0]);
  }

  /**
   * Tests user impersonation over the HTTP protocol by using the HTTP path to specify the
   * 'doAs' parameter.
   */
  @Test
  public void testImpylaHttpImpersonation() throws Exception {
    String invalidDelegateUser = "invalid-delegate-user";
    String query = "select logged_in_user()";
    String errTemplate = "User '%s' is not authorized to delegate to '%s'";

    // Run with an invalid proxy user.
    //String[] command = {"impala-python", helper_, "--user", testUser2_, "--password",
    //    testPassword2_, "--http_path=/?doAs=" + delegateUser_, "--query", query};
    String[] cmd = buildCommand(testUser2_, testPassword2_, "/?doAs=" + delegateUser_);
    RunShellCommand.Run(cmd, /*shouldSucceed*/ false, "",
        String.format(errTemplate, testUser2_, delegateUser_));

    // Run with a valid proxy user but invalid delegate user.
    cmd = buildCommand(testUser_, testPassword_, "/?doAs=" + invalidDelegateUser);
    RunShellCommand.Run(cmd, /*shouldSucceed*/ false, "",
        String.format(errTemplate, testUser_, invalidDelegateUser));

    // 'doAs' parameter that cannot be decoded.
    cmd = buildCommand(testUser_, testPassword_, "/?doAs=%");
    RunShellCommand.Run(cmd, /*shouldSucceed*/ false, "", "httplib.BadStatusLine");

    // Successfully delegate.
    cmd = buildCommand(testUser_, testPassword_, "/?doAs=" + delegateUser_);
    RunShellCommand.Run(cmd, /*shouldSucceed*/ true, delegateUser_, "");
  }
}
