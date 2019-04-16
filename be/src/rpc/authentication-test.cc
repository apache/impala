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
#include "common/init.h"
#include "common/logging.h"
#include "rpc/authentication.h"
#include "rpc/thrift-server.h"
#include "util/auth-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/thread.h"

#include <ldap.h>

DECLARE_bool(enable_ldap_auth);
DECLARE_string(ldap_uri);
DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(internal_principals_whitelist);

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

  ASSERT_OK(sa.InitKerberos(principal, "/etc/hosts"));
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
  ASSERT_OK(sa.InitKerberos("service_name/localhost@some.realm", "/etc/hosts"));

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
  ASSERT_OK(
      sa_internal.InitKerberos("service_name/_HOST@some.realm", "/etc/hosts"));
  SecureAuthProvider sa_external(false);
  ASSERT_OK(
      sa_external.InitKerberos("service_name/_HOST@some.realm", "/etc/hosts"));
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);

  env_keytab = getenv("KRB5_KTNAME");
  env_princ = getenv("MINIKDC_PRINC_IMPALA");

  return RUN_ALL_TESTS();
}
