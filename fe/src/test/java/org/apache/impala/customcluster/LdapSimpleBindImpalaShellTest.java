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

import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.junit.Test;

/**
 * Impala shell connectivity tests with Simple Bind LDAP authentication.
 */
@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
public class LdapSimpleBindImpalaShellTest extends LdapImpalaShellTest {
  @Override
  public void setUp(String extraArgs) throws Exception {
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String simpleBindArgs = String.format("--ldap_bind_pattern='%s' %s", dn, extraArgs);
    super.setUp(simpleBindArgs);
  }

  /**
   * Tests ldap authentication using impala-shell.
   */
  @Test
  public void testShellLdapAuth() throws Exception {
    setUp("");
    testShellLdapAuthImpl();
  }

  /**
   * Tests user impersonation over the HTTP protocol by using the HTTP path to specify the
   * 'doAs' parameter.
   */
  @Test
  public void testHttpImpersonation() throws Exception {
    setUp(String.format(
        "--authorized_proxy_user_config=%s=%s", TEST_USER_1, delegateUser_));
    testHttpImpersonationImpl();
  }

  /**
   * Tests the LDAP user and group filter configs.
   */
  @Test
  public void testLdapFilters() throws Exception {
    // These correspond to the values in fe/src/test/resources/users.ldif
    setUp(String.format("--ldap_group_filter=%s,another-group "
            + "--ldap_user_filter=%s,%s,another-user "
            + "--ldap_group_dn_pattern=%s "
            + "--ldap_group_membership_key=uniqueMember "
            + "--ldap_group_class_key=groupOfUniqueNames "
            + "--ldap_bind_dn=%s --ldap_bind_password_cmd='echo -n %s' ",
        TEST_USER_GROUP, TEST_USER_1, TEST_USER_3, GROUP_DN_PATTERN, TEST_USER_DN_1,
        TEST_PASSWORD_1));
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
    setUp(String.format("--ldap_group_filter=%s,another-group "
            + "--ldap_user_filter=%s,%s,another-user "
            + "--ldap_group_dn_pattern=%s "
            + "--ldap_group_membership_key=uniqueMember "
            + "--ldap_group_class_key=groupOfUniqueNames "
            + "--authorized_proxy_user_config=%s=* "
            + "--ldap_bind_dn=%s --ldap_bind_password_cmd='echo -n %s' ",
        TEST_USER_GROUP, TEST_USER_1, TEST_USER_3, GROUP_DN_PATTERN, TEST_USER_4,
        TEST_USER_DN_1, TEST_PASSWORD_1));
    testLdapFiltersWithProxyImpl();
  }
}
