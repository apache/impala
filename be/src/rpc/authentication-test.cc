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

#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "common/init.h"
#include "common/logging.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/init.h"
#include "kudu/security/test/mini_kdc.h"
#include "rpc/authentication.h"
#include "rpc/authentication-util.h"
#include "rpc/thrift-server.h"
#include "util/auth-util.h"
#include "util/kudu-status-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"

#include <ldap.h>

DECLARE_bool(enable_ldap_auth);
DECLARE_string(ldap_uri);
DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(internal_principals_whitelist);
DECLARE_bool(allow_custom_ldap_filters_with_kerberos_auth);
DECLARE_bool(ldap_search_bind_authentication);
DECLARE_string(ldap_user_search_basedn);
DECLARE_string(ldap_user_filter);
DECLARE_string(ldap_group_filter);

// These are here so that we can grab them early in main() - the kerberos
// init can clobber KRB5_KTNAME in PrincipalSubstitution.
static const char *env_keytab = NULL;
static const char *env_princ = NULL;

#include "common/names.h"

namespace impala {

int SaslAuthorizeInternal(sasl_conn_t* conn, void* context,
    const char* requested_user, unsigned rlen,
    const char* auth_identity, unsigned alen,
    const char* def_realm, unsigned urlen,
    struct propctx* propctx);

TEST(Auth, PrincipalSubstitution) {
  string hostname;
  ASSERT_OK(GetHostname(&hostname));
  SecureAuthProvider sa(false); // false means it's external

  FLAGS_principal = "service_name/_HOST@some.realm";
  string principal;
  ASSERT_OK(GetExternalKerberosPrincipal(&principal));

  ASSERT_OK(sa.InitKerberos(principal));
  ASSERT_OK(sa.Start());
  ASSERT_EQ(string::npos, sa.principal().find("_HOST"));
  ASSERT_NE(string::npos, sa.principal().find(hostname));
  ASSERT_EQ("service_name", sa.service_name());
  ASSERT_EQ(hostname, sa.hostname());
  ASSERT_EQ("some.realm", sa.realm());
  FLAGS_principal.clear();
}

void AuthOk(const string& name, SecureAuthProvider* sa) {
  EXPECT_EQ(SASL_OK,
      SaslAuthorizeInternal(NULL, (void*)sa, name.c_str(), name.size(), NULL, 0, NULL, 0,
          NULL));
}

void AuthFails(const string& name, SecureAuthProvider* sa) {
  EXPECT_EQ(SASL_BADAUTH,
      SaslAuthorizeInternal(NULL, (void*)sa, name.c_str(), name.size(), NULL, 0, NULL, 0,
          NULL));
}

TEST(Auth, AuthorizeInternalPrincipals) {
  SecureAuthProvider sa(true); // false means it's external
  ASSERT_OK(sa.InitKerberos("service_name/localhost@some.realm"));

  AuthOk("service_name/localhost@some.realm", &sa);
  AuthFails("unknown/localhost@some.realm", &sa);

  FLAGS_internal_principals_whitelist = "hdfs1,hdfs2";
  AuthOk("hdfs1/localhost@some.realm", &sa);
  AuthOk("hdfs2/localhost@some.realm", &sa);
  AuthFails("hdfs/localhost@some.realm", &sa);

  AuthFails("hdfs1@some.realm", &sa);
  AuthFails("/localhost@some.realm", &sa);

  FLAGS_internal_principals_whitelist = "";
  AuthFails("", &sa);

  FLAGS_internal_principals_whitelist = ",";
  AuthFails("", &sa);

  FLAGS_internal_principals_whitelist = " ,";
  AuthFails("", &sa);
}

TEST(Auth, ValidAuthProviders) {
  ASSERT_OK(AuthManager::GetInstance()->Init());
  ASSERT_TRUE(AuthManager::GetInstance()->GetExternalAuthProvider() != NULL);
  ASSERT_TRUE(AuthManager::GetInstance()->GetExternalHttpAuthProvider() != NULL);
  ASSERT_TRUE(AuthManager::GetInstance()->GetInternalAuthProvider() != NULL);
}

// Set up ldap flags and ensure we make the appropriate auth providers
TEST(Auth, LdapAuth) {
  AuthProvider* ap = NULL;
  SecureAuthProvider* sa = NULL;

  FLAGS_enable_ldap_auth = true;
  FLAGS_ldap_uri = "ldaps://bogus.com";

  // Initialization based on above "command line" args
  ASSERT_OK(AuthManager::GetInstance()->Init());

  // External auth provider is sasl, ldap, but not kerberos
  ap = AuthManager::GetInstance()->GetExternalAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_TRUE(sa->has_ldap());
  ASSERT_EQ("", sa->principal());

  ap = AuthManager::GetInstance()->GetExternalHttpAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_TRUE(sa->has_ldap());
  ASSERT_EQ("", sa->principal());

  // Internal auth provider isn't sasl.
  ap = AuthManager::GetInstance()->GetInternalAuthProvider();
  ASSERT_FALSE(ap->is_secure());
}

// Set up ldap and kerberos flags and ensure we make the appropriate auth providers
TEST(Auth, LdapKerbAuth) {
  AuthProvider* ap = NULL;
  SecureAuthProvider* sa = NULL;

  if ((env_keytab == NULL) || (env_princ == NULL)) {
    return;     // In a non-kerberized environment
  }
  FLAGS_keytab_file = env_keytab;
  FLAGS_principal = env_princ;
  FLAGS_enable_ldap_auth = true;
  FLAGS_ldap_uri = "ldaps://bogus.com";

  // Initialization based on above "command line" args
  ASSERT_OK(AuthManager::GetInstance()->Init());

  // External auth provider is sasl, ldap, and kerberos
  ap = AuthManager::GetInstance()->GetExternalAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_TRUE(sa->has_ldap());
  ASSERT_EQ(FLAGS_principal, sa->principal());

  ap = AuthManager::GetInstance()->GetExternalHttpAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_TRUE(sa->has_ldap());
  ASSERT_EQ(FLAGS_principal, sa->principal());

  // Internal auth provider is sasl and kerberos
  ap = AuthManager::GetInstance()->GetInternalAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_FALSE(sa->has_ldap());
  ASSERT_EQ(FLAGS_principal, sa->principal());
}

// Test for IMPALA-2598: SSL and Kerberos do not mix on server<->server connections.
// Tests that Impala will successfully start if so configured.
TEST(Auth, KerbAndSslEnabled) {
  string hostname;
  ASSERT_OK(GetHostname(&hostname));
  FLAGS_ssl_client_ca_certificate = "some_path";
  FLAGS_ssl_server_certificate = "some_path";
  FLAGS_ssl_private_key = "some_path";
  ASSERT_TRUE(IsInternalTlsConfigured());
  SecureAuthProvider sa_internal(true);
  ASSERT_OK(sa_internal.InitKerberos("service_name/_HOST@some.realm"));
  SecureAuthProvider sa_external(false);
  ASSERT_OK(sa_external.InitKerberos("service_name/_HOST@some.realm"));
}

// Test principal with slash in hostname
TEST(Auth, InternalPrincipalWithSlash) {
  SecureAuthProvider sa(false); // false means it's external
  ASSERT_OK(sa.InitKerberos("service_name/local\\/host@some.realm"));
  ASSERT_OK(sa.Start());
  ASSERT_EQ("service_name", sa.service_name());
  ASSERT_EQ("local/host", sa.hostname());
  ASSERT_EQ("some.realm", sa.realm());
}

// Test bad principal format exception
TEST(Auth, BadPrincipalFormat) {
  SecureAuthProvider sa(false); // false means it's external
  EXPECT_ERROR(sa.InitKerberos(""), 2);
  EXPECT_ERROR(sa.InitKerberos("service_name@some.realm"), 2);
  EXPECT_ERROR(sa.InitKerberos("service_name/localhost"), 2);
}

// Set up ldap and kerberos flags and
// 1. check if there is an error when specifying custom search filters,
// 2. and the error does not occur when the
//    allow_custom_ldap_filters_with_kerberos_auth flag is turned on.
TEST(Auth, LdapKerbAuthCustomFiltersNotAllowed) {
  AuthProvider* ap = NULL;
  SecureAuthProvider* sa = NULL;

  // Initialize the mini kdc.
  kudu::MiniKdc kdc(kudu::MiniKdcOptions{});
  KUDU_ASSERT_OK(kdc.Start());
  kdc.SetKrb5Environment();
  string kt_path;
  KUDU_ASSERT_OK(kdc.CreateServiceKeytab("HTTP/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1));
  KUDU_ASSERT_OK(kdc.CreateUserPrincipal("alice"));
  KUDU_ASSERT_OK(kdc.Kinit("alice"));

  // Set up a fake impala server with Kerberos enabled.
  gflags::FlagSaver saver;

  FLAGS_principal = "HTTP/127.0.0.1@KRBTEST.COM";
  FLAGS_keytab_file = kt_path;
  FLAGS_enable_ldap_auth = true;
  FLAGS_ldap_uri = "ldaps://bogus.com";
  FLAGS_skip_external_kerberos_auth = false; // external Kerberos auth enabled
  FLAGS_skip_internal_kerberos_auth = true; // internal Kerberos auth disabled (no auth)
  FLAGS_ldap_user_search_basedn = "dc=kbrtest,dc=com";
  FLAGS_ldap_search_bind_authentication = true;
  FLAGS_ldap_user_filter = "(&(objectClass=user)(sAMAccountName={0}))";

  // Initialization based on above "command line" args should fail
  // due to the custom ldap search filters specified.
  ASSERT_ERROR_MSG(AuthManager::GetInstance()->Init(),
      "LDAP user and group filters may not be used "
      "if Kerberos auth is turned on for external connections.");

  // Initialization based on above "command line" args should not fail
  // if the use of custom ldap filters is enabled with Kerberos auth.
  FLAGS_allow_custom_ldap_filters_with_kerberos_auth = true;
  ASSERT_OK(AuthManager::GetInstance()->Init());

  // External auth provider is sasl, ldap, and kerberos
  ap = AuthManager::GetInstance()->GetExternalAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_TRUE(sa->has_ldap());
  ASSERT_EQ(FLAGS_principal, sa->principal());

  ap = AuthManager::GetInstance()->GetExternalHttpAuthProvider();
  ASSERT_TRUE(ap->is_secure());
  sa = dynamic_cast<SecureAuthProvider*>(ap);
  ASSERT_TRUE(sa->has_ldap());
  ASSERT_EQ(FLAGS_principal, sa->principal());

  // Internal auth provider is not secure (NoAuthProvider)
  ap = AuthManager::GetInstance()->GetInternalAuthProvider();
  ASSERT_FALSE(ap->is_secure());
}

TEST(Auth, GetXFFOriginClientAddress) {
  // Empty XFF address
  std::string xff_addresses;
  std::string origin;
  Status status = GetXFFOriginClientAddress(xff_addresses, origin);
  ASSERT_EQ(status.msg().msg(), "Empty XFF header");

  // Empty ',' separated XFF address
  xff_addresses = " ,";
  status = GetXFFOriginClientAddress(xff_addresses, origin);
  ASSERT_EQ(status.msg().msg(), "Empty XFF header");

  // Multiple XFF addresses
  xff_addresses = "  10.96.11.12, 172.45.88.78, 176.45.88.65";
  status = GetXFFOriginClientAddress(xff_addresses, origin);
  ASSERT_EQ(origin, "10.96.11.12");
  ASSERT_TRUE(status.ok());

  // Single XFF address
  xff_addresses = "10.95.66.23";
  status = GetXFFOriginClientAddress(xff_addresses, origin);
  ASSERT_EQ(origin, "10.95.66.23");
  ASSERT_TRUE(status.ok());

  xff_addresses = "      10.90.11.23, ";
  status = GetXFFOriginClientAddress(xff_addresses, origin);
  ASSERT_EQ(origin, "10.90.11.23");
  ASSERT_TRUE(status.ok());

  xff_addresses = " 10.90.11.23 , ";
  status = GetXFFOriginClientAddress(xff_addresses, origin);
  ASSERT_EQ(origin, "10.90.11.23");
  ASSERT_TRUE(status.ok());
}

// Checks that GetEffectiveUser() returns the expected values.
void assertEffectiveUser(
    const string& connected_user, const string& delegated_user, const string& expected) {
  TSessionState session;
  if (!connected_user.empty()) session.__set_connected_user(connected_user);
  if (!delegated_user.empty()) session.__set_delegated_user(delegated_user);
  ASSERT_EQ(GetEffectiveUser(session), expected);
}

// Checks that GetEffectiveShortUser() returns the expected values.
void assertEffectiveShortUser(
    const string& connected_user, const string& delegated_user, const string& expected) {
  TSessionState session;
  if (!connected_user.empty()) session.__set_connected_user(connected_user);
  if (!delegated_user.empty()) session.__set_delegated_user(delegated_user);
  string returned_user;
  ASSERT_OK(GetEffectiveShortUser(session, &returned_user));
  ASSERT_EQ(returned_user, expected)
      << "connected=" << connected_user << " delegated=" << delegated_user;
}

// Unit test for GetEffectiveShortUser().
TEST(Auth, UserUtilities) {
  // Enable Kerberos so we can test kerberos names in GetEffectiveShortUser().
  auto enable_kerberos =
      ScopedFlagSetter<string>::Make(&FLAGS_principal, "service_name/_HOST@some.realm");

  // Usernames that are not mutated by GetEffectiveShortUser().
  const char* unchanged_usernames[] = {
      "andrew", "andrew_sherman", "andrew-sherman", "Andrew"};
  for (auto& name : unchanged_usernames) {
    assertEffectiveUser(name, "", name);
    assertEffectiveShortUser(name, "", name);
  }

  // Test GetEffectiveShortUser() maps Kerberos usernames to the expected short name.
  std::pair<const char*, const char*> kerberos_name_mappings[] = {
      {"impala@ROOT.COMOPS.SITE", "impala"},
      {"changepw/kdc1.example.com@example.com", "changepw"},
      {"krbtgt/EAST.EXAMPLE.COM@WEST.EXAMPLE.COM", "krbtgt"},
      {"User1/admin/STAFF/employees@WEST.EXAMPLE.COM", "User1"},
      {"employees@", "employees"}
  };
  for (const auto& pair : kerberos_name_mappings) {
    assertEffectiveShortUser(pair.first, "", pair.second);
  }

  // Test GetEffectiveUser() logic.
  assertEffectiveUser("connected1", "delegated1", "delegated1");
  assertEffectiveUser("connected1", "", "connected1");
  assertEffectiveUser("impala@ROOT.COMOPS.SITE", "", "impala@ROOT.COMOPS.SITE");

  // Illegal kerberos user names.
  // These all fail kudu::security::MapPrincipalToLocalName().
  const char* illegal_kerberos_usernames[] = {
      "buggy@EAST.EXAMPLE.COM/WEST.EXAMPLE.COM",
      "two_ats@kdc1.example.com@EXAMPLE.COM",
      "two_slash/kdc1.example.com@/EXAMPLE.COM",
      "/User1/admin/STAFF/employees@WEST.EXAMPLE.COM",
  };
  for (auto& bad_name : illegal_kerberos_usernames) {
    TSessionState session;
    session.__set_connected_user(bad_name);
    string unused;
    stringstream ss;
    ss << "Could not parse Kerberos name " << bad_name;
    ASSERT_ERROR_MSG(GetEffectiveShortUser(session, &unused), ss.str());
  }
}

}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);

  env_keytab = getenv("KRB5_KTNAME");
  env_princ = getenv("MINIKDC_PRINC_IMPALA");

  return RUN_ALL_TESTS();
}
