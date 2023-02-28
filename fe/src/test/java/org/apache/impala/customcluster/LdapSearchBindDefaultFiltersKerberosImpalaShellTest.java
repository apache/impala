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
import org.junit.Test;

import java.util.Map;

import static org.apache.impala.testutil.LdapUtil.TEST_USER_1;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_2;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_3;
import static org.junit.Assert.assertEquals;

/**
 * Impala shell connectivity tests with LDAP Search Bind and Kerberos authentication.
 *
 * These test are required to validate backwards compatibility with the changes
 * introduced in IMPALA-11726. Neither the allow_custom_ldap_filters_with_kerberos_auth,
 * nor the enable_group_filter_check_for_authenticated_kerberos_user flag is set in
 * these tests.
 */
@CreateDS(name = "myAD",
        partitions = {@CreatePartition(name = "test", suffix = "dc=myorg,dc=com")})
@ApplyLdifFiles({"adschema.ldif", "adusers.ldif"})
public class LdapSearchBindDefaultFiltersKerberosImpalaShellTest
        extends LdapKerberosImpalaShellTestBase {

  /**
   * Tests custom LDAP user and group filter configs with Search Bind and
   * Kerberos authentication enabled.
   *
   * With custom LDAP filters without Kerberos authentication the cluster
   * should start successfully, but in this test the Kerberos authentication
   * is enabled with the principal flag, so that the cluster should not start up.
   */
  @Test
  public void testCustomLdapFiltersNotAllowedWithKerberos() throws Exception {
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            getLdapSearchBindFlags(),

            // define custom LDAP filters
            getCustomLdapFilterFlags(),

            // to prevent the test from being considered unstable, we should
            // disable minidump creation
            ImmutableMap.of(
                    "enable_minidumps", "false"
            )
    );

    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 1); // cluster should not start up
  }

  /**
   * Tests default LDAP user and group filters with Search Bind and
   * Kerberos authentication enabled.
   *
   * Without custom LDAP filters the LDAP search bind and the Kerberos authentication
   * should work together.
   * This test uses an AD-like LDAP scheme that matches the default LDAP filters.
   */
  @Test
  public void testDefaultLdapFiltersAreAllowedWithSearchBindAndKerberos()
          throws Exception {
    String userSearchBaseDn = "ou=Users,dc=myorg,dc=com";
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            // this test setup requires different user search base dn
            getLdapSearchBindFlags(userSearchBaseDn, defaultGroupSearchBaseDn)

            // custom LDAP filters not defined
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0); // cluster should start up

    testLdapFiltersImpl(); // cluster should pass LDAP filter tests with default filters
  }

  /**
   * Tests default LDAP user and group filters are applied to delegate users when
   * Search Bind and Kerberos authentication enabled and the proxy user authenticates
   * with Kerberos.
   *
   * This test uses an AD-like LDAP scheme that matches the default LDAP filters.
   */
  @Test
  public void testDefaultLdapFiltersAreAppliedToDelegateUserWithKerberosAuth()
          throws Exception {
    String userSearchBaseDn = "ou=Users,dc=myorg,dc=com";
    Map<String, String> flags = mergeFlags(

            // enable Kerberos authentication
            kerberosKdcEnvironment.getKerberosAuthFlags(),

            // enable LDAP authentication with search bind
            // this test setup requires different user search base dn
            getLdapSearchBindFlags(userSearchBaseDn, defaultGroupSearchBaseDn),

            // custom LDAP filters not defined

            // set proxy user: allow TEST_USER_1 to act as a proxy user for
            // any other user
            ImmutableMap.of(
                    "authorized_proxy_user_config", String.format("%s=*", TEST_USER_1)
            )
    );
    int ret = startImpalaCluster(flagsToArgs(flags));
    assertEquals(ret, 0); // cluster should start up

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
            "/?doAs=" + TEST_USER_1,
            /* shouldSucceed */ true,
            TEST_USER_1, "");
  }

}
