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

import com.google.common.collect.ImmutableMap;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.impala.testutil.LdapUtil.GROUP_DN_PATTERN;
import static org.apache.impala.testutil.LdapUtil.TEST_PASSWORD_1;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_1;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_2;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_3;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_4;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_7;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_DN_1;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_GROUP;
import static org.junit.Assert.assertEquals;

/**
 * Impala shell connectivity tests with Kerberos authentication.
 */
@CreateDS(name = "myDS",
        partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@ApplyLdifFiles({"users.ldif"})
public class LdapKerberosImpalaShellTest extends LdapKerberosImpalaShellTestBase {

  /**
   * Tests Kerberos authentication with custom LDAP user and group filter configs
   * with search bind enabled and group filter check disabled.
   */
  @Test
  public void testShellKerberosAuthWithCustomLdapFiltersAndSearchBindNoGroupFilterCheck()
          throws Exception {
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            getLdapSearchBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // define custom LDAP filters corresponding to the values
            // in fe/src/test/resources/users.ldif
            getCustomLdapFilterFlags()
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Cluster should pass impala-shell Kerberos auth tests executed with a user
    // that does not exist in LDAP.
    testShellKerberosAuthWithUser( kerberosKdcEnvironment, "user",
            /* shouldSucceed */ true);
  }

  /**
   * Tests Kerberos authentication with LDAP authentication and search bind enabled,
   * custom LDAP user and group filter configs provided,
   * and group filter check with Kerberos auth enabled.
   */
  @Test
  public void testShellKerberosAuthWithCustomLdapFiltersAndSearchBindGroupFilterCheck()
          throws Exception {
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            getLdapSearchBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // define custom LDAP filters corresponding to the values
            // in fe/src/test/resources/users.ldif
            getCustomLdapFilterFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "enable_group_filter_check_for_authenticated_kerberos_user", "true"
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Group filter check is enabled with Kerberos auth:
    // Cluster should pass impala-shell Kerberos auth tests executed
    // with a user that exists in LDAP and passes LDAP group filter check.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_1,
            /* shouldSucceed */ true);

    // Kerberos authentication should fail with impala-shell with a user
    // that exists in LDAP but it does not pass LDAP group filter check.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_2,
            /* shouldSucceed */ false);
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_3,
            /* shouldSucceed */ false);

    // Kerberos authentication should also fail with a user
    // that does not exist in LDAP.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment,"user",
            /* shouldSucceed */ false);
  }

  /**
   * Tests user impersonation with Kerberos authentication over the HTTP protocol
   * with LDAP authentication and LDAP search bind enabled, custom LDAP user filter
   * config provided.
   */
  @Test
  public void testHttpImpersonationWithKerberosAuthAndLdapSearchBind() throws Exception {
    // Ignore the test if python SSLContext support is not available.
    Assume.assumeTrue(pythonSupportsSSLContext());

    String ldapUri = String.format("ldap://localhost:%s",
            serverRule.getLdapServer().getPort());
    String passwordCommand = String.format("'echo -n %s'", TEST_PASSWORD_1);

    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind,
            // define custom LDAP user filter corresponding to the values
            // in fe/src/test/resources/users.ldif,
            // and allow using custom filters with Kerberos authentication
            ImmutableMap.<String, String>builder()
                .put("enable_ldap_auth", "true")
                .put("ldap_uri", ldapUri)
                .put("ldap_passwords_in_clear_ok", "true")
                .put("ldap_user_search_basedn", defaultUserSearchBaseDn)
                .put("ldap_user_filter", "(cn={0})")
                .put("ldap_search_bind_authentication", "true")
                .put("ldap_bind_dn", TEST_USER_DN_1)
                .put("ldap_bind_password_cmd", passwordCommand)
                .put("allow_custom_ldap_filters_with_kerberos_auth", "true")
                .build(),

            // set proxy user: allow TEST_USER_1 to act as a proxy for delegateUser_
            ImmutableMap.of(
                    "authorized_proxy_user_config",
                    String.format("%s=%s", TEST_USER_1, delegateUser_)
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    String errTemplate = "User '%s' is not authorized to delegate to '%s'";

    // Run with an invalid proxy user.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_2,
            "/?doAs=" + delegateUser_,
            /* shouldSucceed */ false, "",
            String.format(errTemplate,
                    kerberosKdcEnvironment.getUserPrincipal(TEST_USER_2),
                    delegateUser_));

    // Run with a valid proxy user but invalid delegate user.
    String invalidDelegateUser = "invalid-delegate-user";
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + invalidDelegateUser,
            /* shouldSucceed */ false, "",
            String.format(errTemplate,
                    kerberosKdcEnvironment.getUserPrincipal(TEST_USER_1),
                    invalidDelegateUser));

    // 'doAs' parameter that cannot be decoded.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=%",
            /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Successfully delegate.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + delegateUser_,
            /* shouldSucceed */ true,
            delegateUser_, "");

    // Proxy-user without delegation with different transport protocols.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_1,
            /* shouldSucceed */ true);
  }

  /**
   * Tests user impersonation with Kerberos authentication over the HTTP protocol
   * with LDAP authentication and LDAP search bind enabled, custom LDAP user and
   * group filter config provided.
   * This test validates that LDAP filters applied to delegate users regardless
   * of the value of enable_group_filter_check_for_authenticated_kerberos_user flag.
   */
  @Test
  public void testHttpImpersonationWithKerberosAuthAndLdapSearchBindWithGroupFilters()
          throws Exception {
    // Ignore the test if python SSLContext support is not available.
    Assume.assumeTrue(pythonSupportsSSLContext());

    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            getLdapSearchBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // define custom LDAP filters corresponding to the values
            // in fe/src/test/resources/users.ldif
            getCustomLdapFilterFlags(),

            // set proxy user: allow TEST_USER_1 to act as a proxy user for
            // any other user
            ImmutableMap.of(
                    "authorized_proxy_user_config", String.format("%s=*", TEST_USER_1)
            )

            // enable_group_filter_check_for_authenticated_kerberos_user flag
            // not defined
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Kerberos authentication should fail with a delegate user
    // that does not exist in LDAP.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + delegateUser_,
            /* shouldSucceed */ false,
            "", "User is not authorized.");

    // Kerberos authentication should fail with delegate users that exist in LDAP,
    // but do not pass LDAP filter checks.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + TEST_USER_2,
            /* shouldSucceed */ false,
            "", "User is not authorized.");
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + TEST_USER_3,
            /* shouldSucceed */ false,
            "", "User is not authorized.");

    // Kerberos authentication should succeed with a delegate user
    // that exists in LDAP and passes LDAP filter checks.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + TEST_USER_7,
            /* shouldSucceed */ true,
            TEST_USER_7, "");

    // Proxy-user without delegation with different transport protocols.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_1,
            /* shouldSucceed */ true);
  }

  /**
   * Tests the interaction between LDAP user and group filter configs and proxy user
   * configs when authenticating with Kerberos and LDAP authentication is also enabled,
   * LDAP search bind configured.
   */
  @Test
  public void testLdapFiltersWithProxyWithKerberosAuthAndLdapSearchBind()
          throws Exception {

    String customLdapUserFilter =
            String.format("(&(objectClass=person)(cn={0})(!(cn=%s)))", TEST_USER_2);
    String customLdapGroupFilter = "(&(cn=group1)(uniqueMember={0}))";

    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            getLdapSearchBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // define custom LDAP filters corresponding to the values
            // in fe/src/test/resources/users.ldif
            ImmutableMap.of(
                    "ldap_user_filter", customLdapUserFilter,
                    "ldap_group_filter", customLdapGroupFilter
            ),

            // set proxy user:
            // allow TEST_USER_4 to act as a proxy for any other user
            ImmutableMap.of(
                    "authorized_proxy_user_config",
                    String.format("%s=*", TEST_USER_4)
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);


    // Run as the proxy user with a delegate that passes both filters, should succeed
    // and return the delegate user's name.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_1,
            /* shouldSucceed */ true,
            TEST_USER_1, "");

    // Run as the proxy user with a delegate that only passes the user filter, should
    // fail.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_3,
            /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Run as the proxy user with a delegate that only passes the group filter, should
    // fail.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_2,
            /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Run as the proxy with a delegate that doesn't pass either filter, should fail.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_4,
            /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Run as the proxy without a delegate user, should fail since the proxy user won't
    // pass the filters.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/",
            /* shouldSucceed */ false,
            "", "");
  }

  /**
   * Tests user impersonation with Kerberos authentication over the HTTP protocol
   * with LDAP authentication and LDAP simple bind enabled.
   */
  @Test
  public void testHttpImpersonationWithKerberosAuthAndLdapSimpleBind() throws Exception {
    // Ignore the test if python SSLContext support is not available.
    Assume.assumeTrue(pythonSupportsSSLContext());

    String ldapUri = String.format("ldap://localhost:%s",
            serverRule.getLdapServer().getPort());

    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with simple bind
            ImmutableMap.of(
                    "enable_ldap_auth", "true",
                    "ldap_uri", ldapUri,
                    "ldap_passwords_in_clear_ok", "true",
                    "ldap_bind_pattern","'cn=#UID,ou=Users,dc=myorg,dc=com'"
            ),

            // set proxy user: allow TEST_USER_1 to act as a proxy for delegateUser_
            ImmutableMap.of(
                    "authorized_proxy_user_config",
                    String.format("%s=%s", TEST_USER_1, delegateUser_)
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    String errTemplate = "User '%s' is not authorized to delegate to '%s'";

    // Run with an invalid proxy user.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_2,
            "/?doAs=" + delegateUser_,
            /* shouldSucceed */ false,
            "",
            String.format(errTemplate,
                    kerberosKdcEnvironment.getUserPrincipal(TEST_USER_2),
                    delegateUser_));

    // Run with a valid proxy user but invalid delegate user.
    String invalidDelegateUser = "invalid-delegate-user";
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + invalidDelegateUser,
            /* shouldSucceed */ false,
            "",
            String.format(errTemplate,
                    kerberosKdcEnvironment.getUserPrincipal(TEST_USER_1),
                    invalidDelegateUser));

    // 'doAs' parameter that cannot be decoded.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=%",
            /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Successfully delegate.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_1,
            "/?doAs=" + delegateUser_,
            /* shouldSucceed */ true,
            delegateUser_, "");

    // Proxy-user without delegation with different transport protocols.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_1,
            /* shouldSucceed */ true);
  }

  /**
   * Tests Kerberos authentication with custom LDAP user and group filter configs
   * with simple bind enabled and group filter check disabled.
   */
  @Test
  public void testShellKerberosAuthWithCustomLdapFiltersAndSimpleBindNoGroupFilterCheck()
          throws Exception {
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with simple bind
            getLdapSimpleBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // set custom user and group filters
            getCustomLdapSimpleBindSearchFilterFlags()
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Cluster should pass impala-shell Kerberos auth tests executed with a user
    // that does not exist in LDAP.
    testShellKerberosAuthWithUser( kerberosKdcEnvironment, "user",
            /* shouldSucceed */ true);
  }

  /**
   * Tests Kerberos authentication with custom LDAP user and group filter configs
   * with simple bind and group filter check enabled.
   */
  @Test
  public void testShellKerberosAuthWithCustomLdapFiltersAndSimpleBindGroupFilterCheck()
          throws Exception {
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with simple bind
            getLdapSimpleBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // set custom user and group filters
            getCustomLdapSimpleBindSearchFilterFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "enable_group_filter_check_for_authenticated_kerberos_user", "true"
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Group filter check is enabled with Kerberos auth:
    // Cluster should pass impala-shell Kerberos auth tests executed
    // with a user that exists in LDAP and passes LDAP group filter check.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_1,
            /* shouldSucceed */ true);

    // Kerberos authentication should fail with impala-shell with a user
    // that exists in LDAP but it does not pass LDAP group filter check.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_2,
            /* shouldSucceed */ false);
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, TEST_USER_3,
            /* shouldSucceed */ false);

    // Kerberos authentication should also fail with a user
    // that does not exist in LDAP.
    testShellKerberosAuthWithUser(kerberosKdcEnvironment,"user",
            /* shouldSucceed */ false);
  }

  /**
   * Tests the interaction between LDAP user and group filter configs and proxy user
   * configs when authenticating with Kerberos and LDAP authentication is also enabled,
   * LDAP simple bind configured.
   */
  @Test
  public void testLdapFiltersWithProxyWithKerberosAuthAndLdapSimpleBind()
          throws Exception {
    // Sets up a cluster where TEST_USER_4 can act as a proxy for any other user but
    // doesn't pass any filters themselves, TEST_USER_1 and TEST_USER_2 can pass the group
    // filter, and TEST_USER_1 and TEST_USER_3 pass the user filter.
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with simple bind
            getLdapSimpleBindFlags(),

            // allow using custom filters with Kerberos authentication
            ImmutableMap.of(
                    "allow_custom_ldap_filters_with_kerberos_auth", "true"
            ),

            // set custom user and group filters
            getCustomLdapSimpleBindSearchFilterFlags(),

            // set proxy user:
            // allow TEST_USER_4 to act as a proxy for any other user
            ImmutableMap.of(
                    "authorized_proxy_user_config",
                    String.format("%s=*", TEST_USER_4)
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Run as the proxy user with a delegate that passes both filters, should succeed
    // and return the delegate user's name.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_1,
            /* shouldSucceed */ true,
            TEST_USER_1, "");

    // Run as the proxy user with a delegate that only passes the user filter, should
    // fail.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_3,
            /* shouldSucceed */false,
            "","Not connected to Impala");

    // Run as the proxy user with a delegate that only passes the group filter, should
    // fail.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_2, /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Run as the proxy with a delegate that doesn't pass either filter, should fail.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/?doAs=" + TEST_USER_4, /* shouldSucceed */ false,
            "", "Not connected to Impala");

    // Run as the proxy without a delegate user, should fail since the proxy user won't
    // pass the filters.
    testShellKerberosAuthWithUserWithHttpPath(kerberosKdcEnvironment, TEST_USER_4,
            "/", /* shouldSucceed */ false,
            "", "");
  }

  /**
   * Tests Kerberos authentication with the Kerberos hostname override option.
   */
  @Test
  public void testShellKerberosAuthWithKerberosHostnameOverride()
          throws Exception {
    String kerberosHostFqdn = "any.host";

    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlagsWithCustomServicePrincipal(
                    "impala", kerberosHostFqdn)

    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0);

    // Cluster should pass impala-shell Kerberos auth tests:
    // In this test, the impala daemon is running on localhost, and we connect to it
    // with impala-shell, but the service principal used in the impala daemon does
    // not have a "localhost" hostname, so we need to use the kerberos_host_fqdn option.
    testShellKerberosAuthWithUser( kerberosKdcEnvironment, "user",
            /* shouldSucceed */ true, kerberosHostFqdn);
  }

  /**
   * Tests Kerberos authentication using impala-shell.
   */
  protected void testShellKerberosAuthWithUser(
          KerberosKdcEnvironment kerberosKdcEnvironment, String username,
          boolean shouldSucceed) throws Exception {
    testShellKerberosAuthWithUser(kerberosKdcEnvironment, username, shouldSucceed, null);
  }

  /**
   * Tests Kerberos authentication using impala-shell.
   */
  protected void testShellKerberosAuthWithUser(
          KerberosKdcEnvironment kerberosKdcEnvironment, String username,
          boolean shouldSucceed, String kerberosHostFqdn) throws Exception {

    List<String> protocolsToTest = Arrays.asList("beeswax", "hs2");
    if (pythonSupportsSSLContext()) {
      // http transport tests will fail with older python versions (IMPALA-8873)
      protocolsToTest = Arrays.asList("beeswax", "hs2", "hs2-http");
    }

    // create user principal in KDC
    // and create a credentials cache file with a valid TGT ticket
    String credentialsCacheFilePath =
            kerberosKdcEnvironment.createUserPrincipalAndCredentialsCache(username);

    for (String protocol : protocolsToTest) {
      String[] command =
              kerberosHostFqdn == null ?
                      createCommandForProtocol(protocol) :
                      createCommandForProtocolAndKerberosHostFqdn(protocol,
                              kerberosHostFqdn);
      String expectedOut =
              shouldSucceed ? kerberosKdcEnvironment.getUserPrincipal(username) : "";
      String expectedErr =
              shouldSucceed ? "Starting Impala Shell with Kerberos authentication" :
                      "Not connected to Impala";
      RunShellCommand.Run(command,
              kerberosKdcEnvironment.getImpalaShellEnv(credentialsCacheFilePath),
              shouldSucceed, expectedOut, expectedErr);
    }
  }

  private String[] createCommandForProtocol(String protocol) {
    String[] command = {
            "impala-shell.sh",
            String.format("--protocol=%s", protocol),
            "--kerberos",
            "--query=select logged_in_user()"
    };
    return command;
  }

  private String[] createCommandForProtocolAndKerberosHostFqdn(String protocol,
                                                               String kerberosHostFqdn) {
    String[] command = {
            "impala-shell.sh",
            String.format("--protocol=%s", protocol),
            "--kerberos",
            String.format("--kerberos_host_fqdn=%s", kerberosHostFqdn),
            "--query=select logged_in_user()"
    };
    return command;
  }

  private Map<String, String> getLdapSimpleBindFlags() {
    String ldapUri = String.format("ldap://localhost:%s",
            serverRule.getLdapServer().getPort());
    String passwordCommand = String.format("'echo -n %s'", TEST_PASSWORD_1);
    return ImmutableMap.<String, String>builder()
        .put("enable_ldap_auth", "true")
        .put("ldap_uri", ldapUri)
        .put("ldap_passwords_in_clear_ok", "true")
        .put("ldap_bind_pattern","'cn=#UID,ou=Users,dc=myorg,dc=com'")
        .put("ldap_group_dn_pattern", GROUP_DN_PATTERN)
        .put("ldap_group_membership_key", "uniqueMember")
        .put("ldap_group_class_key", "groupOfUniqueNames")
        .put("ldap_bind_dn", TEST_USER_DN_1)
        .put("ldap_bind_password_cmd", passwordCommand)
        .build();
  }

  private Map<String, String> getCustomLdapSimpleBindSearchFilterFlags() {
    String customGroupFilter = String.format("%s,another-group", TEST_USER_GROUP);
    String customUserFilter =  String.format("%s,%s,another-user", TEST_USER_1,
            TEST_USER_3);
    return ImmutableMap.of(
            "ldap_group_filter", customGroupFilter,
            "ldap_user_filter", customUserFilter
    );
  }

}
