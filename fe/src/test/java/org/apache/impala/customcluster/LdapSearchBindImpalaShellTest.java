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

import static org.apache.impala.customcluster.LdapKerberosImpalaShellTestBase.flagsToArgs;
import static org.apache.impala.customcluster.LdapKerberosImpalaShellTestBase.mergeFlags;
import static org.apache.impala.testutil.LdapUtil.*;

import com.google.common.collect.ImmutableMap;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

/**
 * Impala shell connectivity tests with Search Bind LDAP authentication.
 *
 * The test suite is parameterized, all tests are executed with both Kerberos
 * authentication disabled and with Kerberos authentication enabled to validate
 * that LDAP search bind authentication is not broken even if Kerberos authentication
 * is enabled.
 */
@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@RunWith(Parameterized.class)
public class LdapSearchBindImpalaShellTest extends LdapImpalaShellTest {

  @ClassRule
  public static KerberosKdcEnvironment kerberosKdcEnvironment =
          new KerberosKdcEnvironment(new TemporaryFolder());

  private final boolean kerberosAuthenticationEnabled;

  @Parameterized.Parameters(name = "kerberosAuthenticationEnabled={0}")
  public static Boolean[] kerberosAuthenticationEnabled() {
    return new Boolean[] {Boolean.FALSE, Boolean.TRUE};
  }

  public LdapSearchBindImpalaShellTest(boolean isKerberosAuthenticationEnabled) {
    this.kerberosAuthenticationEnabled = isKerberosAuthenticationEnabled;
  }


  @Override
  protected int startImpalaCluster(String args) throws IOException, InterruptedException {
    if (kerberosAuthenticationEnabled) {
      return kerberosKdcEnvironment.startImpalaClusterWithArgs(args);
    } else {
      return super.startImpalaCluster(args);
    }
  }

  @Override
  public void setUp(String extraArgs) throws Exception {
    String searchBindArgs = String.format("--ldap_search_bind_authentication=true "
                    + "--ldap_bind_dn=%s --ldap_bind_password_cmd='echo -n %s' %s %s",
            TEST_USER_DN_1, TEST_PASSWORD_1, getKerberosArgs(), extraArgs);
    super.setUp(searchBindArgs);
  }

  private String getKerberosArgs() throws IOException {
    return kerberosAuthenticationEnabled ?
            flagsToArgs(mergeFlags(
                    kerberosKdcEnvironment.getKerberosAuthFlags(),
                    ImmutableMap.of(
                            "allow_custom_ldap_filters_with_kerberos_auth", "true"
                    )
            ))
            :
            "";  // empty if Kerberos authentication is disabled
  }

  /**
   * Tests ldap authentication using impala-shell.
   */
  @Test
  public void testShellLdapAuth() throws Exception {
    setUp("--ldap_user_search_basedn=dc=myorg,dc=com "
        + "--ldap_user_filter=(&(objectClass=person)(cn={0}))");
    testShellLdapAuthImpl(null);
  }

  /**
   * Tests user impersonation over the HTTP protocol by using the HTTP path to specify the
   * 'doAs' parameter.
   */
  @Test
  public void testHttpImpersonation() throws Exception {
    setUp(String.format("--authorized_proxy_user_config=%s=%s "
            + "--ldap_user_search_basedn=dc=myorg,dc=com "
            + "--ldap_user_filter=(cn={0})",
        TEST_USER_1, delegateUser_));
    testHttpImpersonationImpl();
  }

  /**
   * Tests the LDAP user and group filter configs.
   */
  @Test
  public void testLdapFilters() throws Exception {
    // These correspond to the values in fe/src/test/resources/users.ldif
    // Sets up a cluster with user filter which forbids access only for TEST_USER_2 under
    // 'dc=myorg,dc=com' subtree.
    setUp(String.format("--ldap_user_search_basedn=dc=myorg,dc=com "
            + "--ldap_group_search_basedn=ou=Groups,dc=myorg,dc=com "
            + "--ldap_user_filter=(&(objectClass=person)(cn={0})(!(cn=%s))) "
            + "--ldap_group_filter=(uniqueMember={0})",
        TEST_USER_2));
    testLdapFiltersImpl();
  }

  /**
   * Tests the LDAP user and group filter configs, with narrow group search, only users in
   * a specific group are allowed.
   */
  @Test
  public void testLdapFiltersWithNarrowGroupSearch() throws Exception {
    // These correspond to the values in fe/src/test/resources/users.ldif
    // Sets up a cluster with user filter which forbids access only for TEST_USER_2 under
    // 'dc=myorg,dc=com' subtree and group filter that allows access only for users that
    // are in the TEST_USER_GROUP group.
    setUp(String.format("--ldap_user_search_basedn=dc=myorg,dc=com "
            + "--ldap_group_search_basedn=ou=Groups,dc=myorg,dc=com "
            + "--ldap_user_filter=(&(objectClass=person)(cn={0})(!(cn=%s))) "
            + "--ldap_group_filter=(&(cn=%s)(uniqueMember={0}))",
        TEST_USER_2, TEST_USER_GROUP));
    testLdapFiltersImpl();
  }

  /**
   * Tests the interaction between LDAP user and group filter configs and proxy user
   * configs.
   */
  @Test
  public void testLdapFiltersWithProxy() throws Exception {
    // These correspond to the values in fe/src/test/resources/users.ldif
    // Sets up a cluster where TEST_USER_4 can act as a proxy for any other user but
    // doesn't pass any filters themselves, TEST_USER_1 and TEST_USER_2 can pass the group
    // filter, and TEST_USER_1 and TEST_USER_3 pass the user filter.
    setUp(String.format("--ldap_user_search_basedn=dc=myorg,dc=com "
            + "--ldap_group_search_basedn=ou=Groups,dc=myorg,dc=com "
            + "--ldap_user_filter=(&(objectClass=person)(cn={0})(!(cn=Test2Ldap))) "
            + "--ldap_group_filter=(&(cn=group1)(uniqueMember={0})) "
            + "--authorized_proxy_user_config=%s=* ",
        TEST_USER_4));
    testLdapFiltersWithProxyImpl();
  }

  /**
   * Tests proxy-user authentication without impersonation over all available protocols.
   */
  @Test
  public void testLdapFiltersWithProxyWithoutDoAsUser() throws Exception {
    // These correspond to the values in fe/src/test/resources/users.ldif
    // Sets up a cluster where TEST_USER_1 can act as a proxy for any other user
    // and TEST_USER_1 passes the group filter, and user filter, too.
    setUp(String.format("--ldap_user_search_basedn=dc=myorg,dc=com "
            + "--ldap_group_search_basedn=ou=Groups,dc=myorg,dc=com "
            + "--ldap_user_filter=(&(objectClass=person)(cn={0})(!(cn=Test2Ldap))) "
            + "--ldap_group_filter=(&(cn=group1)(uniqueMember={0})) "
            + "--authorized_proxy_user_config=%s=* ", TEST_USER_1));

    String query = "select logged_in_user()";

    for (String protocol : getProtocolsToTest()) {
      // Run as the proxy without a delegate user,
      // testcase should pass since the proxy user passes the filters.
      String[] command =
          buildCommand(query, protocol, TEST_USER_1, TEST_PASSWORD_1, "/cliservice");
      RunShellCommand.Run(command, /* shouldSucceed */ true, TEST_USER_1, "");
    }
  }

  /**
   * Test LDAP Search on multiple OUs.
   */
  @Test
  public void testAuthenticationOverMultipleOUs() throws Exception {
    setUp("--ldap_user_search_basedn=dc=myorg,dc=com "
        + "--ldap_user_filter=(cn={0})");
    String query = "select logged_in_user()";

    // Authentications should succeed for TEST_USER_2 who is in "Users" org
    String[] command =
        buildCommand(query, "hs2-http", TEST_USER_2, TEST_PASSWORD_2, "/cliservice");
    RunShellCommand.Run(command, /* shouldSucceed */ true, "", "");

    // Authentications should succeed for TEST_USER_5 who is in "Users2" org
    command =
        buildCommand(query, "hs2-http", TEST_USER_5, TEST_PASSWORD_5, "/cliservice");
    RunShellCommand.Run(command, /* shouldSucceed */ true, "", "");

    // Authentications should fail for non-existing invalid user
    command = buildCommand(query, "hs2-http", "invalid", "123", "/cliservice");
    RunShellCommand.Run(command, /* shouldSucceed */ false, "", "");
  }

  /**
   * Test group search filter validity when there is an escaped character in the user DN.
   */
  @Test
  public void testEscapedCharactersInDN() throws Exception {
    setUp("--ldap_user_search_basedn=dc=myorg,dc=com "
        + "--ldap_group_search_basedn=ou=Groups,dc=myorg,dc=com "
        + "--ldap_user_filter=(cn={0}) "
        + "--ldap_group_filter=(uniqueMember={0}) ");
    String query = "select logged_in_user()";

    // Authentications should succeed with user who has escaped character in its DN
    String[] command =
        buildCommand(query, "hs2-http", TEST_USER_6, TEST_PASSWORD_6, "/cliservice");
    RunShellCommand.Run(command, /* shouldSucceed */ true, TEST_USER_6, "");
  }
}
