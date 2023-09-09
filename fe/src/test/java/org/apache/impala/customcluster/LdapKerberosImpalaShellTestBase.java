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
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.impala.testutil.LdapUtil.TEST_PASSWORD_1;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_2;
import static org.apache.impala.testutil.LdapUtil.TEST_USER_DN_1;

/**
 * Base class for Impala shell connectivity tests with LDAP and Kerberos authentication.
 */
public class LdapKerberosImpalaShellTestBase extends LdapImpalaShellTest {

  protected final static String defaultUserSearchBaseDn = "dc=myorg,dc=com";
  protected final static String defaultGroupSearchBaseDn = "ou=Groups,dc=myorg,dc=com";

  @ClassRule
  public static KerberosKdcEnvironment kerberosKdcEnvironment =
          new KerberosKdcEnvironment(new TemporaryFolder());

  protected Map<String, String> getLdapSearchBindFlags() {
    return getLdapSearchBindFlags(defaultUserSearchBaseDn, defaultGroupSearchBaseDn);
  }

  protected Map<String, String> getLdapSearchBindFlags(
          String userSearchBaseDn, String groupSearchBaseDn) {
    String ldapUri = String.format("ldap://localhost:%s",
            serverRule.getLdapServer().getPort());
    String passwordCommand = String.format("'echo -n %s'", TEST_PASSWORD_1);
    return ImmutableMap.<String, String>builder()
        .put("enable_ldap_auth", "true")
        .put("ldap_uri", ldapUri)
        .put("ldap_passwords_in_clear_ok", "true")
        .put("ldap_user_search_basedn", userSearchBaseDn)
        .put("ldap_group_search_basedn", groupSearchBaseDn)
        .put("ldap_search_bind_authentication", "true")
        .put("ldap_bind_dn", TEST_USER_DN_1)
        .put("ldap_bind_password_cmd", passwordCommand)
        .build();
  }

  protected Map<String, String> getCustomLdapFilterFlags() {
    String customLdapUserFilter =
            String.format("(&(objectClass=person)(cn={0})(!(cn=%s)))", TEST_USER_2);
    String customLdapGroupFilter = "(uniqueMember={0})";
    return ImmutableMap.of(
            "ldap_user_filter", customLdapUserFilter,
            "ldap_group_filter", customLdapGroupFilter
    );
  }

  @Override
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

  /**
   * Tests Kerberos authentication using impala-shell over HTTP protocol,
   * using the HTTP path to specify the 'doAs' parameter.
   */
  protected void testShellKerberosAuthWithUserWithHttpPath(
          KerberosKdcEnvironment kerberosKdcEnvironment, String username,
          String httpPath, boolean shouldSucceed, String expectedOut,
          String expectedErr) throws Exception {

    // Run with an invalid proxy user.
    String[] command = {
            "impala-shell.sh",
            "--protocol=hs2-http",
            "--kerberos",
            "--query=select logged_in_user()",
            "--http_path=" + httpPath};

    // create user principal in KDC
    // and create a credentials cache file with a valid TGT ticket
    String credentialsCacheFilePath =
            kerberosKdcEnvironment.createUserPrincipalAndCredentialsCache(username);

    RunShellCommand.Run(command,
            kerberosKdcEnvironment.getImpalaShellEnv(credentialsCacheFilePath),
            shouldSucceed, expectedOut, expectedErr);
  }

}
