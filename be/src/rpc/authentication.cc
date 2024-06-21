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

#include "rpc/authentication.h"

#include <stdio.h>
#include <signal.h>
#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/filesystem.hpp>
#include <gutil/casts.h>
#include <gutil/strings/escaping.h>
#include <gutil/strings/split.h>
#include <gutil/strings/strip.h>
#include <gutil/strings/substitute.h>
#include <algorithm>
#include <map>
#include <vector>
#include <string>
#include <vector>
#include <thrift/Thrift.h>
#include <transport/TSasl.h>
#include <transport/TSaslServerTransport.h>
#include <gflags/gflags.h>

#include <ldap.h>

#include "exec/kudu/kudu-util.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/gssapi.h"
#include "kudu/security/init.h"
#include "kudu/util/openssl_util.h"
#include "rpc/auth-provider.h"
#include "rpc/authentication-util.h"
#include "rpc/thrift-server.h"
#include "runtime/exec-env.h"
#include "service/frontend.h"
#include "transport/THttpServer.h"
#include "transport/TSaslClientTransport.h"
#include "util/auth-util.h"
#include "util/coding-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/jwt-util.h"
#include "util/ldap-util.h"
#include "util/network-util.h"
#include "util/os-util.h"
#include "util/promise.h"
#include "util/time.h"

#include <sys/types.h>    // for stat system call
#include <sys/stat.h>     // for stat system call
#include <unistd.h>       // for stat system call

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::trim;
using boost::mt19937;
using boost::uniform_int;
using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace boost::filesystem;   // for is_regular(), is_absolute()
using namespace strings;

DECLARE_bool(skip_external_kerberos_auth);
DECLARE_bool(skip_internal_kerberos_auth);
DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(be_principal);
DECLARE_string(krb5_ccname);
DECLARE_string(krb5_conf);
DECLARE_string(krb5_debug_file);
DECLARE_int32(external_fe_port);

// Defined in kudu/security/init.cc
DECLARE_bool(use_system_auth_to_local);

DECLARE_int64(max_cookie_lifetime_s);

DECLARE_int32(hs2_http_port);

DEFINE_string(sasl_path, "", "Colon separated list of paths to look for SASL "
    "security library plugins.");
DEFINE_bool(enable_ldap_auth, false,
    "If true, use LDAP authentication for client connections");
DEFINE_string(ldap_ca_certificate, "", "The full path to the certificate file used to"
    " authenticate the LDAP server's certificate for SSL / TLS connections.");
DEFINE_string(ldap_user_filter, "", "Used as filter for both simple and search "
    "bind mechanisms. For simple bind it is a comma separated list of user names. If "
    "specified, users must be on this list for authentication to succeed. For search "
    "bind it is an LDAP filter that will be used during LDAP search, it can contain "
    "'{0}' pattern which will be replaced with the user name.");
DEFINE_string(ldap_group_filter, "", "Used as filter for both simple and search "
    "bind mechanisms. For simple bind it is a comma separated list of groups. If "
    "specified, users must belong to one of these groups for authentication to succeed. "
    "For search bind it is an LDAP filter that will be used during LDAP group search, it "
    "can contain '{0}' pattern which will be replaced with the user name and/or '{1}' "
    "which will be replace with the user dn.");

DEFINE_string(internal_principals_whitelist, "hdfs", "(Advanced) Comma-separated list of "
    " additional usernames authorized to access Impala's internal APIs. Defaults to "
    "'hdfs' which is the system user that in certain deployments must access "
    "catalog server APIs.");

DEFINE_string(trusted_domain, "",
    "If set, Impala will skip authentication for connections originating from this "
    "domain. Currently, only connections over HTTP support this. Note: It still requires "
    "the client to specify a username via the Basic Authorization header in the format "
    "<username>:<password> where the password is not used and can be left blank.");

DEFINE_bool(trusted_domain_use_xff_header, false,
    "If set to true, this uses the 'X-Forwarded-For' HTML header to check for origin "
    "while attempting to verify if the connection request originated from a trusted "
    "domain. Only used if '--trusted_domain' is specified. Warning: Only use this if you "
    "trust the incoming connection to have this set correctly.");

DEFINE_bool(trusted_domain_empty_xff_header_use_origin, false,
    "If set to true and the 'X-Forwarded-For' HTML header value is empty in the request, "
    "then the origin of the the underlying transport is used while attempting to "
    "verify if the connection request originated from a trusted domain. Only used "
    "if '--trusted_domain' and '--trusted_domain_use_xff_header' flags are specified. "
    "Warning: In case the 'X-Forwarded-For' HTML header is empty or not in the request, "
    "this flag allows a fallback to the default behavior in trusted domain check "
    "(where '--trusted_domain' flag is specified, but '--trusted_domain_use_xff_header' "
    "flag is not set).");

DEFINE_bool(trusted_domain_strict_localhost, true,
    "If set to true and trusted_domain='localhost', this will not use reverse DNS to "
    "determine if something is from localhost. It will only match 127.0.0.1. This is "
    "important for security, because reverse DNS can resolve other non-local addresses "
    "to localhost.");

// This flag must be used with caution to avoid security risks.
DEFINE_string(trusted_auth_header, "",
    "If set as non empty string, Impala will look for this header in the HTTP headers. "
    "If the header is present, Impala will skip authentication and extract the username "
    "directly from the authorization header. Currently, only connections over HTTP "
    "support this. Note: It still requires the client to specify a username via the "
    "Basic Authorization header in the format <username>:<password> where the password "
    "is not used and can be left blank. Warning: Only use this flag if the connections "
    "are authenticated before the requests land on Impala so that Impala can avoid the "
    "authentication again. The system must remove the trusted HTTP headers from any "
    "requests that come from outside the system.");

DEFINE_bool_hidden(saml2_allow_without_tls_debug_only, false,
    "When this configuration is set to true, Impala allows SAML2 authentication "
    "on unsecure channel. This should be only enabled for development / testing.");

DECLARE_string(saml2_sp_callback_url);

// If set, Impala support for trusting an authentication based on JWT token in the HTTP
// header.
DEFINE_bool(jwt_token_auth, false,
    "When true, read the JWT token out of the HTTP Header and extract user name from "
    "the token payload.");
// The last segment of a JWT is the signature, which is used to verify that the token was
// signed by the sender and not altered in any way. By default, it's required to validate
// the signature of the JWT tokens. Otherwise it may expose security issue.
DEFINE_bool(jwt_validate_signature, true,
    "When true, validate the signature of JWT token with pre-installed JWKS.");
// JWKS consists the public keys used by the signing party to the clients that need to
// validate signatures. It represents cryptographic keys in JSON data structure.
DEFINE_string(jwks_file_path, "",
    "File path of the pre-installed JSON Web Key Set (JWKS) for JWT verification");
// This specifies the URL for JWKS to be downloaded.
DEFINE_string(jwks_url, "", "URL of the JSON Web Key Set (JWKS) for JWT verification");
// Enables retrieving the JWKS URL without verifying the presented TLS certificate
// from the server.
DEFINE_bool(jwks_verify_server_certificate, true,
    "Specifies if the TLS certificate of the JWKS server is verified when retrieving "
    "the JWKS from the specified JWKS URL.  A certificate is considered valid if a "
    "trust chain can be established for it, and if the certificate has a common name or "
    "SAN that matches the server's hostname. This should only be set to false for "
    "development / testing.");
// Enables defining a custom pem bundle file containing root certificates to trust.
DEFINE_string(jwks_ca_certificate, "", "File path of a pem bundle of root ca "
    "certificates that will be trusted when retrieving the JWKS from the "
    "specified JWKS URL.");
DEFINE_int32(jwks_update_frequency_s, 60,
    "(Advanced) The time in seconds to wait between downloading JWKS from the specified "
    "URL.");
DEFINE_int32(jwks_pulling_timeout_s, 10,
    "(Advanced) The time in seconds for connection timed out when pulling JWKS from the "
    "specified URL.");
// This specifies the custom claim in the JWT that contains the "username" for the
// session.
DEFINE_string(jwt_custom_claim_username, "username", "Custom claim 'username'");
// If set, Impala allows JWT authentication on unsecure channel.
// JWT is only secure when used with TLS. But in some deployment scenarios, TLS is handled
// by proxy so that it does not show up as TLS to Impala.
DEFINE_bool_hidden(jwt_allow_without_tls, false,
    "When this configuration is set to true, Impala allows JWT authentication on "
    "unsecure channel. This should be only enabled for testing, or development for which "
    "TLS is handled by proxy.");
DEFINE_bool(enable_group_filter_check_for_authenticated_kerberos_user, false,
    "If this configuration is set to true, Impala checks the provided "
    "LDAP group filter, if any, with the authenticated Kerberos user. "
    "This should be only enabled if both Kerberos and LDAP authentication are enabled "
    "and the users in KDC and LDAP are synchronized (e.g. when the KDC and the LDAP "
    "is the same Active Directory server). "
    "The default value is false, which provides backwards-compatible behavior.");

DEFINE_string_hidden(test_cookie, "",
    "Adds Set-Cookie: <test_cookie> to BasicAuth for testing.");

namespace impala {

// Sasl callbacks.  Why are these here?  Well, Sasl isn't that bright, and
// instead of copying the callbacks, it just saves a pointer to them.  If
// they're on the stack, this means that they *go away* when the function
// exits... so make these global and static here, and fill them in below.
// Vectors are used for the three latter items because that's what the thrift
// interface expects.
//
// The way the callbacks work is that the Sasl code will look for a registered
// callback in the connection-specific callback list (one of the latter three),
// and if not found, then look for a registered callback in the
// "GENERAL_CALLBACKS" list.
static sasl_callback_t GENERAL_CALLBACKS[5];        // Applies to all connections
static vector<sasl_callback_t> KERB_INT_CALLBACKS;  // Internal kerberos connections
static vector<sasl_callback_t> KERB_EXT_CALLBACKS;  // External kerberos connections
static vector<sasl_callback_t> LDAP_EXT_CALLBACKS;  // External LDAP connections

// The name of the application passed to Sasl which will retain reference to it.
// This is initialized the first time InitAuth() is called. Future call must pass
// the same 'appname' or InitAuth() will fail.
static string APP_NAME;

// Constants for the two Sasl mechanisms we support
static const string KERBEROS_MECHANISM = "GSSAPI";
static const string PLAIN_MECHANISM = "PLAIN";

// We implement an "auxprop" plugin for the Sasl layer in order to have a hook in which
// to log messages about the start of authentication. This is that plugin's name.
static const string IMPALA_AUXPROP_PLUGIN = "impala-auxprop";

AuthManager* AuthManager::auth_manager_ = new AuthManager();

// This Sasl callback is called when the underlying cyrus-sasl layer has
// something that it would like to say.  We catch it and turn it into the
// appropriate LOG() call.
//
// context: Passed a (char *) that comes from the initialization, used
//          to describe the kerb|ldap internal|external context
// level: The SASL_LOG_ level
// message: The message to log
// Return: Always SASL_OK, unless message is NULL, then it's SASL_BADPARAM.
static int SaslLogCallback(void* context, int level, const char* message) {
  if (message == NULL) return SASL_BADPARAM;
  const char* authctx = (context == NULL) ? "Unknown" :
      reinterpret_cast<const char*>(context);

  switch (level) {
  case SASL_LOG_NONE:  // "Don't log anything"
  case SASL_LOG_PASS:  // "Traces... including passwords" - don't log!
    break;
  case SASL_LOG_ERR:   // "Unusual errors"
  case SASL_LOG_FAIL:  // "Authentication failures"
    LOG(ERROR) << "SASL message (" << authctx << "): " << message;
    break;
  case SASL_LOG_WARN:  // "Non-fatal warnings"
    LOG(WARNING) << "SASL message (" << authctx << "): " << message;
    break;
  case SASL_LOG_NOTE:  // "More verbose than WARN"
    LOG(INFO) << "SASL message (" << authctx << "): " << message;
    break;
  case SASL_LOG_DEBUG: // "More verbose than NOTE"
    VLOG(1) << "SASL message (" << authctx << "): " << message;
    break;
  case SASL_LOG_TRACE: // "Traces of internal protocols"
  default:
    VLOG(3) << "SASL message (" << authctx << "): " << message;
    break;
  }

  return SASL_OK;
}

// Calls into the LDAP utils to check the provided group filters
bool DoLdapCheckFilters(const char* user) {
  ImpalaLdap* ldap = AuthManager::GetInstance()->GetLdap();
  ImpalaServer* server = ExecEnv::GetInstance()->impala_server();
  if (server == nullptr) {
    LOG(FATAL) << "Invalid config: SASL LDAP is only supported for client connections "
               << "to an impalad.";
  }
  // If the user is an authorized proxy user, we do not yet know the effective user as
  // it may be set by 'impala.doas.user', in which case we defer checking LDAP filters
  // until OpenSession(). Otherwise, we prefer to check the filters as easly as
  // possible, so check them here.
  if (server->IsAuthorizedProxyUser(user)) {
    return true;
  }
  return ldap->LdapCheckFilters(user);
}

// Calls into the LDAP utils to check the provided user/pass,
// and the provided group filters.
bool DoLdapCheck(const char* user, const char* pass, unsigned passlen) {
  ImpalaLdap* ldap = AuthManager::GetInstance()->GetLdap();
  bool success = ldap->LdapCheckPass(user, pass, passlen);
  if (!success) {
    return false;
  }

  return DoLdapCheckFilters(user);
}

// Wrapper around the function we use to check passwords with LDAP which has the function
// signature required to work with SASL.
//
// conn: The Sasl connection struct, which we ignore
// context: Ignored; always NULL
// user: The username to authenticate
// pass: The password to use
// passlen: The length of pass
// propctx: Ignored - properties requested
// Return: SASL_OK on success, SASL_FAIL otherwise
int SaslLdapCheckPass(sasl_conn_t* conn, void* context, const char* user,
    const char* pass, unsigned passlen, struct propctx* propctx) {
  return DoLdapCheck(user, pass, passlen) ? SASL_OK : SASL_FAIL;
}

// Sasl wants a way to ask us about some options, this function provides
// answers.  Currently we only support telling Sasl about the log_level; other
// items are printed out for curiosity's sake, but otherwise ignored.
//
// context: Ignored, always NULL
// plugin_name: If applicable, the name of the plugin making the
//              request.  NULL if it's a general option.
// option: The name of the configurable parameter
// result: A char * array to hold our answer
// len: Bytes at result
// Return: SASL_OK for things we deal with; SASL_FAIL otherwise.  The
//         cyrus-sasl code rarely checks the return value; it's more
//         interested in whether we fill in *result or not.
static int SaslGetOption(void* context, const char* plugin_name, const char* option,
    const char** result, unsigned* len) {

  if (plugin_name == NULL) {
    if (strcmp("log_level", option) == 0) {
      int level = SASL_LOG_WARN;
      if (VLOG_CONNECTION_IS_ON) {
        level = SASL_LOG_DEBUG;
      } else if (VLOG_ROW_IS_ON) {
        level = SASL_LOG_TRACE;
      }
      static char buf[4];
      snprintf(buf, 4, "%d", level);
      *result = buf;
      if (len != NULL) *len = strlen(buf);
      return SASL_OK;
    } else if (strcmp("auxprop_plugin", option) == 0) {
      *result = IMPALA_AUXPROP_PLUGIN.c_str();
      if (len != NULL) *len = strlen(*result);
      return SASL_OK;
    }

    VLOG(3) << "Sasl general option " << option << " requested";
    return SASL_FAIL;
  }

  VLOG(3) << "Sasl option " << plugin_name << " : "
          << option << " requested";
  return SASL_FAIL;
}

// The "auxprop" plugin interface was intended to be a database service for the "glue"
// layer between the mechanisms and applications.  We, however, hijack this interface
// simply in order to provide an audit message prior to that start of authentication.
#if SASL_VERSION_FULL >= ((2 << 16) | (1 << 8) | 25)
static int ImpalaAuxpropLookup(void* glob_context, sasl_server_params_t* sparams,
#else
static void ImpalaAuxpropLookup(void* glob_context, sasl_server_params_t* sparams,
#endif
    unsigned int flags, const char* user, unsigned ulen) {
  // This callback is called twice, once with this flag clear, and once with
  // this flag set.  We only want to log this message once, so only log it when
  // the flag is clear.
  if ((flags & SASL_AUXPROP_AUTHZID) == 0) {
    string ustr(user, ulen);
    VLOG(2) << "Attempting to authenticate user \"" << ustr << "\"";
  }
#if SASL_VERSION_FULL >= ((2 << 16) | (1 << 8) | 25)
  return SASL_OK;
#endif
}

// Singleton structure used to register our auxprop plugin with Sasl
static sasl_auxprop_plug_t impala_auxprop_plugin = {
  0, // feature flag
  0, // 'spare'
  NULL, // global plugin state
  NULL, // Free global state callback
  &ImpalaAuxpropLookup, // Auxprop lookup method
  const_cast<char*>(IMPALA_AUXPROP_PLUGIN.c_str()), // Name of plugin
  NULL // Store property callback
};

// This is a Sasl callback that's called in order to register our "auxprop"
// plugin.  We give it the structure above, which installs ImpalaAuxpropLookup
// as the lookup method.
int ImpalaAuxpropInit(const sasl_utils_t* utils, int max_version, int* out_version,
    sasl_auxprop_plug_t** plug, const char* plugname) {
  VLOG(2) << "Initializing Impala SASL plugin: " << plugname;
  *plug = &impala_auxprop_plugin;
  *out_version = max_version;
  return SASL_OK;
}

// This Sasl callback will tell us what files Sasl is trying to access.  It's
// here just for curiousity's sake at the moment. It might be useful for
// telling us precisely which plugins have been found.
//
// context: Ignored, always NULL
// file: The file being accessed
// type: What type of thing is it: plugin, config file, etc
// Return: SASL_OK
static int SaslVerifyFile(void* context, const char* file,
    sasl_verify_type_t type ) {
  switch(type) {
    case SASL_VRFY_PLUGIN:
      VLOG(2) << "Sasl found plugin " << file;
      break;
    case SASL_VRFY_CONF:
      VLOG(2) << "Sasl trying to access config file " << file;
      break;
    case SASL_VRFY_PASSWD:
      VLOG(2) << "Sasl accessing password file " << file;
      break;
    default:
      VLOG(2) << "Sasl found other file " << file;
      break;
  }
  return SASL_OK;
}

// Authorizes authenticated users on an internal connection after validating that the
// first components of the 'requested_user' and our principal are the same.
//
// conn: Sasl connection - Ignored
// context: Always NULL except for testing.
// requested_user: The identity/username to authorize
// rlen: Length of above
// auth_identity: "The identity associated with the secret"
// alen: Length of above
// def_realm: Default user realm
// urlen: Length of above
// propctx: Auxiliary properties - Ignored
// Return: SASL_OK
int SaslAuthorizeInternal(sasl_conn_t* conn, void* context,
    const char* requested_user, unsigned rlen,
    const char* auth_identity, unsigned alen,
    const char* def_realm, unsigned urlen,
    struct propctx* propctx) {
  string requested_principal(requested_user, rlen);
  vector<string> names;
  split(names, requested_principal, is_any_of("/@"));

  if (names.size() != 3) {
    LOG(INFO) << "Kerberos principal should be of the form: "
              << "<service>/<hostname>@<realm> - got: " << requested_user;
    return SASL_BADAUTH;
  }
  SecureAuthProvider* internal_auth_provider;
  if (context == NULL) {
    internal_auth_provider = static_cast<SecureAuthProvider*>(
        AuthManager::GetInstance()->GetInternalAuthProvider());
  } else {
    // Branch should only be taken for testing, where context is used to inject an auth
    // provider.
    internal_auth_provider = static_cast<SecureAuthProvider*>(context);
  }

  vector<string> whitelist;
  split(whitelist, FLAGS_internal_principals_whitelist, is_any_of(","));
  whitelist.push_back(internal_auth_provider->service_name());
  for (string& s: whitelist) {
    trim(s);
    if (s.empty()) continue;
    if (names[0] == s) {
      // We say "principal" here becase this is for internal communication, and hence
      // ought always be --principal or --be_principal
      VLOG(1) << "Successfully authenticated principal \"" << requested_principal
              << "\" on an internal connection";
      return SASL_OK;
    }
  }
  string expected_names = FLAGS_internal_principals_whitelist.empty() ? "" :
      Substitute(" or one of $0", FLAGS_internal_principals_whitelist);
  LOG(INFO) << "Principal \"" << requested_principal << "\" not authenticated. "
            << "Reason: 'service' does not match from <service>/<hostname>@<realm>.\n"
            << "Got: " << names[0]
            << ". Expected: " << internal_auth_provider->service_name() << expected_names;
  return SASL_BADAUTH;
}

// If Kerberos and LDAP authentications are enabled and
// enable_group_filter_check_for_authenticated_kerberos_user flag is set,
// then this callback checks if the authenticated user passes LDAP group
// filters.
//
// conn: Sasl connection - Ignored
// context: Ignored, always NULL
// requested_user: The identity/username to authorize
// rlen: Length of above
// auth_identity: "The identity associated with the secret"
// alen: Length of above
// def_realm: Default user realm
// urlen: Length of above
// propctx: Auxiliary properties - Ignored
// Return: SASL_OK
static int SaslKerberosAuthorizeExternal(sasl_conn_t* conn, void* context,
    const char* requested_user, unsigned rlen,
    const char* auth_identity, unsigned alen,
    const char* def_realm, unsigned urlen,
    struct propctx* propctx) {
  if (FLAGS_enable_ldap_auth &&
      FLAGS_enable_group_filter_check_for_authenticated_kerberos_user) {
    DCHECK(IsKerberosEnabled());

    string username = string(requested_user, rlen);
    string short_user =
        GetShortUsernameFromKerberosPrincipal(username);

    LOG(INFO) << "Checking LDAP group filters for "
              << "username \"" << short_user << "\" "
              << "parsed from user principal \""
              << username << "\".";

    bool success = DoLdapCheckFilters(short_user.c_str());
    if (!success) {
      LOG(WARNING) << "Got authenticated principal but the "
                   << "user \"" << short_user << "\" "
                   << "didn't pass the group filters.";
      return SASL_BADAUTH;
    }
  }

  LOG(INFO) << "Successfully authenticated client user \""
            << string(requested_user, rlen) << "\"";
  return SASL_OK;
}

// This callback could be used to authorize or restrict access to certain
// users when authenticating with LDAP.
// Currently it is used to log a message that we successfully
// authenticated with a user on an external connection.
//
// conn: Sasl connection - Ignored
// context: Ignored, always NULL
// requested_user: The identity/username to authorize
// rlen: Length of above
// auth_identity: "The identity associated with the secret"
// alen: Length of above
// def_realm: Default user realm
// urlen: Length of above
// propctx: Auxiliary properties - Ignored
// Return: SASL_OK
static int SaslLdapAuthorizeExternal(sasl_conn_t* conn, void* context,
    const char* requested_user, unsigned rlen,
    const char* auth_identity, unsigned alen,
    const char* def_realm, unsigned urlen,
    struct propctx* propctx) {
  LOG(INFO) << "Successfully authenticated client user \""
            << string(requested_user, rlen) << "\"";
  return SASL_OK;
}

// Sasl callback - where to look for plugins.  When SASL is dynamically linked, the plugin
// path is embedded in the library. However, for backwards compatibility in Impala, we
// provided the possibility to define a custom SASL plugin path that may override the
// system default. This function is only used, when the user actively chooses to manually
// define a custom SASL path, otherwise the automatic path resolution from the SASL
// library is used.
//
// context: Ignored, always NULL
// path: We return the plugin paths here.
// Return: SASL_OK
static int SaslGetPath(void* context, const char** path) {
  *path = FLAGS_sasl_path.c_str();
  return SASL_OK;
}

bool CookieAuth(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const std::string& cookie_header) {
  string username;
  Status cookie_status = AuthenticateCookie(hash, cookie_header, &username);
  if (cookie_status.ok()) {
    connection_context->username = username;
    return true;
  }

  LOG(INFO) << "Invalid cookie provided: " << cookie_header
            << " from: " << TNetworkAddressToString(connection_context->network_address)
            << ": " << cookie_status.GetDetail();
  connection_context->return_headers.push_back(
      Substitute("Set-Cookie: $0", GetDeleteCookie()));
  return false;
}

bool GetUsernameFromBasicAuthHeader(
    ThriftServer::ConnectionContext* connection_context, string& auth_header) {
  string stripped_basic_auth_token;
  StripWhiteSpace(&auth_header);
  bool got_basic_auth =
      TryStripPrefixString(auth_header, "Basic ", &stripped_basic_auth_token);
  string basic_auth_token = got_basic_auth ? move(stripped_basic_auth_token) : "";
  string username, password;
  Status status = BasicAuthExtractCredentials(basic_auth_token, username, password);
  if (!status.ok()) {
    LOG(ERROR) << "Error parsing basic authentication token from: "
               << TNetworkAddressToString(connection_context->network_address)
               << " Error: " << status;
    return false;
  }
  connection_context->username = username;
  return true;
}

bool TrustedDomainCheck(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const std::string& origin, string auth_header) {
  if (!IsTrustedDomain(origin, FLAGS_trusted_domain,
          FLAGS_trusted_domain_strict_localhost)) {
    return false;
  }

  if (!GetUsernameFromBasicAuthHeader(connection_context, auth_header)) return false;
  // Create a cookie to return.
  connection_context->return_headers.push_back(
      Substitute("Set-Cookie: $0", GenerateCookie(connection_context->username, hash)));
  return true;
}

bool HandleTrustedAuthHeader(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, string auth_header) {
  if (!GetUsernameFromBasicAuthHeader(connection_context, auth_header)) return false;
  // Create a cookie to return.
  connection_context->return_headers.push_back(
      Substitute("Set-Cookie: $0", GenerateCookie(connection_context->username, hash)));
  return true;
}

bool BasicAuth(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const string& base64) {
  string username, password;
  Status status = BasicAuthExtractCredentials(base64, username, password);
  if (!status.ok()) {
    LOG(ERROR) << "Error parsing basic authentication token from: "
               << TNetworkAddressToString(connection_context->network_address)
               << " Error: " << status;
    connection_context->return_headers.push_back("WWW-Authenticate: Basic");
    return false;
  }
  bool ret = DoLdapCheck(username.c_str(), password.c_str(), password.length());
  if (ret) {
    // Authenication was successful, so set the username on the connection.
    connection_context->username = username;
    // Create a cookie to return.
    connection_context->return_headers.push_back(
        Substitute("Set-Cookie: $0", GenerateCookie(username, hash)));
    if (!FLAGS_test_cookie.empty()) {
      connection_context->return_headers.push_back(
          Substitute("Set-Cookie: $0", FLAGS_test_cookie));
    }
    return true;
  }
  connection_context->return_headers.push_back("WWW-Authenticate: Basic");
  return false;
}

bool JWTTokenAuth(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const string& token) {
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  Status status = JWTHelper::Decode(token, decoded_token);
  if (!status.ok()) {
    LOG(ERROR) << "Error decoding JWT token received from: "
               << TNetworkAddressToString(connection_context->network_address)
               << " Error: " << status;
    return false;
  }
  if (FLAGS_jwt_validate_signature) {
    status = JWTHelper::GetInstance()->Verify(decoded_token.get());
    if (!status.ok()) {
      LOG(ERROR) << "Error verifying JWT token received from: "
                 << TNetworkAddressToString(connection_context->network_address)
                 << " Error: " << status;
      return false;
    }
  }

  DCHECK(!FLAGS_jwt_custom_claim_username.empty());
  string username;
  status = JWTHelper::GetCustomClaimUsername(
      decoded_token.get(), FLAGS_jwt_custom_claim_username, username);
  if (!status.ok()) {
    LOG(ERROR) << "Error extracting username from JWT token received from: "
               << TNetworkAddressToString(connection_context->network_address)
               << " Error: " << status;
    return false;
  }
  connection_context->username = username;
  // TODO: cookies are not added, but are not needed right now
  return true;
}

// Performs a step of SPNEGO auth for the HTTP transport and sets the username and
// kerberos_user_principal on 'connection_context' if auth is successful.
// 'header_token' is the value from an 'Authorization: Negotiate" header.
// Returns true if the step was successful and sets 'is_complete' to indicate
// if more steps are needed. Returns false if an error was encountered and the
// connection should be closed.
bool NegotiateAuth(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const std::string& header_token, bool* is_complete) {
  if (header_token.empty()) {
    connection_context->return_headers.push_back("WWW-Authenticate: Negotiate");
    *is_complete = false;
    return false;
  }
  std::string token;
  // Note: according to RFC 2616, the correct format for the header is:
  // 'Authorization: Negotiate <token>'. However, beeline incorrectly adds an additional
  // ':', i.e. 'Authorization: Negotiate: <token>'. We handle that here.
  TryStripPrefixString(header_token, ": ", &token);
  string resp_token;
  string username;
  kudu::Status spnego_status =
      kudu::gssapi::SpnegoStep(token, &resp_token, is_complete, &username);
  if (spnego_status.ok()) {
    if (!resp_token.empty()) {
      string resp_header = Substitute("WWW-Authenticate: Negotiate $0", resp_token);
      connection_context->return_headers.push_back(resp_header);
    }
    if (*is_complete) {
      if (username.empty()) {
        spnego_status = kudu::Status::RuntimeError(
            "SPNEGO indicated complete, but got empty principal");
        // Crash in debug builds, but fall through to treating as an error in release.
        LOG(DFATAL) << "Got no authenticated principal for SPNEGO-authenticated "
                    << " connection from "
                    << TNetworkAddressToString(connection_context->network_address)
                    << ": " << spnego_status.ToString();
      } else {
        if (FLAGS_enable_ldap_auth &&
            FLAGS_enable_group_filter_check_for_authenticated_kerberos_user) {

          string short_user = GetShortUsernameFromKerberosPrincipal(username);

          LOG(INFO) << "Checking LDAP group filters for "
                    << "username \"" << short_user << "\" "
                    << "parsed from user principal \""
                    << username << "\".";

          bool success = DoLdapCheckFilters(short_user.c_str());
          if (!success) {
            LOG(WARNING) << "Got authenticated principal for SPNEGO-authenticated "
                        << "connection from "
                        << TNetworkAddressToString(connection_context->network_address)
                        << " but the authenticated user \"" << short_user << "\" "
                        << "didn't pass the group filters.";
            return false;
          }
        }

        // Authentication was successful, so set the username on the connection.
        connection_context->username = username;
        // Save the username as Kerberos user principal in the connection context.
        connection_context->kerberos_user_principal = username;
        // Create a cookie to return.
        connection_context->return_headers.push_back(
            Substitute("Set-Cookie: $0", GenerateCookie(username, hash)));
      }
    }
  } else {
    LOG(WARNING) << "Failed to authenticate request from "
                 << TNetworkAddressToString(connection_context->network_address)
                 << " via SPNEGO: " << spnego_status.ToString();
  }
  return spnego_status.ok();
}

vector<string> ReturnHeaders(ThriftServer::ConnectionContext* connection_context) {
  return std::move(connection_context->return_headers);
}

// Parses a param string.
//  params_to_check: map from the name of params that should be parsed to pointers where
//                   the value should be written
//  original: the full http path (used only in error message)
//  params_string: & delimited param string to parse
//  err_msg: write detailed message here in case of error
bool ParseParams(std::map<string, string*>& params_to_check,
   const string& original, const string& params_string, string* err_msg) {
  for (auto pair : Split(params_string, "&")) {
    vector<string> key_value = Split(pair, delimiter::Limit("=", 1));
    if (key_value.size() == 2) {
      auto it = params_to_check.find(key_value[0]);
      if (it == params_to_check.end()) continue;
      string decoded;
      if (!UrlDecode(key_value[1], &decoded)) {
        *err_msg = Substitute(
            "Could not decode '$0' parameter from HTTP request with path: $1",
                key_value[0], original);
        return false;
      } else {
        *it->second = decoded;
      }
    }
  }
  return true;
}

// Takes the path component of an HTTP request and parses it.
// The followings are inspected in the path:
// - the 'doAs' parameter if it exists
// - if SAML auth is enabled then the path is checked whether it matches
//   the SP callback URL, if yes, then the whole body has to be read for,
//   authentication purposes, not just the headers
bool HttpPathFn(ThriftServer::ConnectionContext* connection_context,
     const string& saml_path, const string& path, string* err_msg,
     bool* read_whole_body) {
  TWrappedHttpRequest* request = connection_context->request.get();
  // 'path' should be of the form '/.*[?<key=value>[&<key=value>...]]'
  vector<string> split = Split(path, delimiter::Limit("?", 1));
  bool is_saml_response = !saml_path.empty() && split[0] == saml_path;
  *read_whole_body = is_saml_response;

  if (request) request->path = split[0];

  if (split.size() != 2) return true;

  std::map<string, string*> params_to_check;
  params_to_check["doAs"] = &connection_context->do_as_user;

  return ParseParams(params_to_check, path, split[1], err_msg);
}

TWrappedHttpResponse* GetSaml2Redirect(
    ThriftServer::ConnectionContext* connection_context) {
  TWrappedHttpRequest* request = connection_context->request.get();
  TWrappedHttpResponse* response = connection_context->response.get();
  DCHECK(request != nullptr);
  DCHECK(response == nullptr);
  response = new TWrappedHttpResponse();
  connection_context->response.reset(response);
  Status status =
      ExecEnv::GetInstance()->frontend()->GetSaml2Redirect(*request, response);
  if (!status.ok()) return nullptr;

  return response;
}

TWrappedHttpResponse* ValidateSaml2AuthnResponse(
    ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash) {
  TWrappedHttpRequest* request = connection_context->request.get();
  TWrappedHttpResponse* response = connection_context->response.get();
  DCHECK(request != nullptr);
  DCHECK(response == nullptr);
  response = new TWrappedHttpResponse();
  connection_context->response.reset(response);
  // Parse the body.
  std::map<string, string*> params_to_check;
  string saml_relay_state;
  params_to_check["RelayState"] =  &saml_relay_state;
  string error_msg;
  const string& content = request->content;
  if(!ParseParams(params_to_check, content, content, &error_msg)) {
    LOG(ERROR) << "failed to parse SAML response params: " << error_msg;
    return nullptr;
  }
  StripWhiteSpace(&saml_relay_state);
  request->params["RelayState"] = saml_relay_state;
  // We return some html in case of auth error. TODO: Should handle other
  // errors where no response is generated.
  Status status =
      ExecEnv::GetInstance()->frontend()->ValidateSaml2Response(*request, response);
  return response;
}

bool ValidateSaml2Bearer(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash) {
  TWrappedHttpRequest* request = connection_context->request.get();
  string username;
  Status status =
      ExecEnv::GetInstance()->frontend()->ValidateSaml2Bearer(*request, &username);

  if (!status.ok()) return false;

  connection_context->username = username;
  // Create a cookie to return.
  connection_context->return_headers.push_back(
      Substitute("Set-Cookie: $0", GenerateCookie(username, hash)));
  return true;
}

TWrappedHttpRequest* InitWrappedHttpRequest(
      ThriftServer::ConnectionContext* connection_context) {
  TWrappedHttpRequest* request = new TWrappedHttpRequest();
  request->remote_ip = connection_context->network_address.hostname;
  request->server_name = connection_context->server_name;
  // Intentionally lying to the pac4j lib  in the frontend to ensure that non-secure
  // test setups work similarly to secure production systems. SAML must use TLS if
  // FLAGS_saml2_allow_without_tls_debug_only is not true.
  request->secure = true;
  connection_context->request.reset(request);
  return request;
}

// Parses and validates FLAGS_saml2_sp_callback_url.
// Sets saml_sp_path to the path part if successful.
Status ParseSamlSpUrl(string* saml_sp_path) {
  vector<string> split =
      Split(FLAGS_saml2_sp_callback_url, delimiter::Limit("/", 3));
  if (split.size() != 4 || (split[0] != "http:" && split[0] != "https:")) {
    return Status(
        Substitute("Bad saml2_sp_callback_url: $0", FLAGS_saml2_sp_callback_url));
  }
  // The port in the url should be the same as FLAGS_hs2_http_port in general,
  // but this is not enforced to allow the use case when the port is mapped,
  // e.g. external_port->http_port. FLAGS_saml2_sp_callback_url has to contain the
  // external_port in this case.
  if (saml_sp_path != nullptr) *saml_sp_path = "/" + split[3];
  return Status::OK();
}

namespace {

// SASL requires mutexes for thread safety, but doesn't implement
// them itself. So, we have to hook them up to our mutex implementation.
static void* SaslMutexAlloc() {
  return static_cast<void*>(new mutex());
}
static void SaslMutexFree(void* m) {
  delete static_cast<mutex*>(m);
}
static int SaslMutexLock(void* m) {
  static_cast<mutex*>(m)->lock();
  return 0; // indicates success.
}
static int SaslMutexUnlock(void* m) {
  static_cast<mutex*>(m)->unlock();
  return 0; // indicates success.
}

void SaslSetMutex() {
  sasl_set_mutex(&SaslMutexAlloc, &SaslMutexLock, &SaslMutexUnlock, &SaslMutexFree);
}

}


Status InitAuth(const string& appname) {
  if (APP_NAME.empty()) {
    APP_NAME = appname;
  } else if (APP_NAME != appname) {
    return Status(TErrorCode::SASL_APP_NAME_MISMATCH, APP_NAME, appname);
  }

  // Setup basic callbacks for Sasl. We initialize SASL always since KRPC expects it to
  // be initialized.
  // Good idea to have logging everywhere
  GENERAL_CALLBACKS[0].id = SASL_CB_LOG;
  GENERAL_CALLBACKS[0].proc = (int (*)())&SaslLogCallback;
  GENERAL_CALLBACKS[0].context = ((void *)"General");

  int arr_offset = 0;
  if (!FLAGS_sasl_path.empty()) {
    // Need this here so we can find available mechanisms
    GENERAL_CALLBACKS[1].id = SASL_CB_GETPATH;
    GENERAL_CALLBACKS[1].proc = (int (*)())&SaslGetPath;
    GENERAL_CALLBACKS[1].context = NULL;
    arr_offset = 1;
  }

  // Allows us to view and set some options
  GENERAL_CALLBACKS[1 + arr_offset].id = SASL_CB_GETOPT;
  GENERAL_CALLBACKS[1 + arr_offset].proc = (int (*)())&SaslGetOption;
  GENERAL_CALLBACKS[1 + arr_offset].context = NULL;

  // For curiosity, let's see what files are being touched.
  GENERAL_CALLBACKS[2 + arr_offset].id = SASL_CB_VERIFYFILE;
  GENERAL_CALLBACKS[2 + arr_offset].proc = (int (*)())&SaslVerifyFile;
  GENERAL_CALLBACKS[2 + arr_offset].context = NULL;

  GENERAL_CALLBACKS[3 + arr_offset].id = SASL_CB_LIST_END;

  // Other than the general callbacks, we only setup other SASL things as required.
  if (FLAGS_enable_ldap_auth || IsKerberosEnabled()) {
    if (IsKerberosEnabled()) {
      // Callbacks for when we're a Kerberos Sasl internal connection.  Just do logging.
      KERB_INT_CALLBACKS.resize(3);

      KERB_INT_CALLBACKS[0].id = SASL_CB_LOG;
      KERB_INT_CALLBACKS[0].proc = (int (*)())&SaslLogCallback;
      KERB_INT_CALLBACKS[0].context = ((void *)"Kerberos (internal)");

      KERB_INT_CALLBACKS[1].id = SASL_CB_PROXY_POLICY;
      KERB_INT_CALLBACKS[1].proc = (int (*)())&SaslAuthorizeInternal;
      KERB_INT_CALLBACKS[1].context = NULL;

      KERB_INT_CALLBACKS[2].id = SASL_CB_LIST_END;

      // Our externally facing Sasl callbacks for Kerberos communication
      KERB_EXT_CALLBACKS.resize(3);

      KERB_EXT_CALLBACKS[0].id = SASL_CB_LOG;
      KERB_EXT_CALLBACKS[0].proc = (int (*)())&SaslLogCallback;
      KERB_EXT_CALLBACKS[0].context = ((void *)"Kerberos (external)");

      KERB_EXT_CALLBACKS[1].id = SASL_CB_PROXY_POLICY;
      KERB_EXT_CALLBACKS[1].proc = (int (*)())&SaslKerberosAuthorizeExternal;
      KERB_EXT_CALLBACKS[1].context = NULL;

      KERB_EXT_CALLBACKS[2].id = SASL_CB_LIST_END;
    }

    if (FLAGS_enable_ldap_auth) {
      // Our external server-side SASL callbacks for LDAP communication
      LDAP_EXT_CALLBACKS.resize(4);

      LDAP_EXT_CALLBACKS[0].id = SASL_CB_LOG;
      LDAP_EXT_CALLBACKS[0].proc = (int (*)())&SaslLogCallback;
      LDAP_EXT_CALLBACKS[0].context = ((void *)"LDAP");

      LDAP_EXT_CALLBACKS[1].id = SASL_CB_PROXY_POLICY;
      LDAP_EXT_CALLBACKS[1].proc = (int (*)())&SaslLdapAuthorizeExternal;
      LDAP_EXT_CALLBACKS[1].context = NULL;

      // This last callback is where we take the password and turn around and
      // call into openldap.
      LDAP_EXT_CALLBACKS[2].id = SASL_CB_SERVER_USERDB_CHECKPASS;
      LDAP_EXT_CALLBACKS[2].proc = (int (*)())&SaslLdapCheckPass;
      LDAP_EXT_CALLBACKS[2].context = NULL;

      LDAP_EXT_CALLBACKS[3].id = SASL_CB_LIST_END;
    }

    // Kudu Client and Kudu RPC shouldn't attempt to initialize SASL which would conflict
    // with Impala's SASL initialization. This must be called before any Kudu RPC objects
    // and KuduClients are created to ensure that Kudu doesn't init SASL first, and this
    // returns an error if Kudu has already initialized SASL.
    KUDU_RETURN_IF_ERROR(kudu::rpc::DisableSaslInitialization(),
        "Unable to disable Kudu RPC SASL initialization.");
    if (KuduIsAvailable()) {
      KUDU_RETURN_IF_ERROR(kudu::client::DisableSaslInitialization(),
          "Unable to disable Kudu SASL initialization.");
    }
  }

  SaslSetMutex();
  try {
    // We assume all impala processes are both server and client.
    sasl::TSaslServer::SaslInit(GENERAL_CALLBACKS, APP_NAME.c_str());
    sasl::TSaslClient::SaslInit(GENERAL_CALLBACKS);
  } catch (sasl::SaslServerImplException& e) {
    stringstream err_msg;
    err_msg << "Could not initialize Sasl library: " << e.what();
    return Status(err_msg.str());
  }

  // Add our auxprop plugin, which gives us a hook before authentication
  int rc = sasl_auxprop_add_plugin(IMPALA_AUXPROP_PLUGIN.c_str(), &ImpalaAuxpropInit);
  if (rc != SASL_OK) {
    return Status(Substitute("Error adding Sasl auxprop plugin: $0",
            sasl_errstring(rc, NULL, NULL)));
  }

  // Initializes OpenSSL.
  RETURN_IF_ERROR(AuthManager::GetInstance()->Init());

  // Prevent Kudu from re-initializing OpenSSL.
  if (KuduIsAvailable()) {
    KUDU_RETURN_IF_ERROR(kudu::client::DisableOpenSSLInitialization(),
        "Unable to disable Kudu SSL initialization.");
  }
  return Status::OK();
}

// Ensure that /var/tmp (the location of the Kerberos replay cache) has drwxrwxrwt
// permissions.  If it doesn't, Kerberos will be unhappy in a way that's very difficult
// to debug.  We do this using direct stat() calls because boost doesn't support the
// detail we need.
Status CheckReplayCacheDirPermissions() {
  DCHECK(IsKerberosEnabled());
  struct stat st;

  if (stat("/var/tmp", &st) < 0) {
    return Status(Substitute("Problem accessing /var/tmp: $0", GetStrErrMsg()));
  }

  if (!(st.st_mode & S_IFDIR)) {
    return Status("Error: /var/tmp is not a directory");
  }

  if ((st.st_mode & 01777) != 01777) {
    return Status("Error: The permissions on /var/tmp must precisely match "
        "\"drwxrwxrwt\". This directory is used by the Kerberos replay cache. To "
        "rectify this issue, run \"chmod 01777 /var/tmp\" as root.");
  }

  return Status::OK();
}

Status SecureAuthProvider::InitKerberos(const string& principal) {
  principal_ = principal;

  RETURN_IF_ERROR(ParseKerberosPrincipal(
      principal_, &service_name_, &hostname_, &realm_));

  LOG(INFO) << "Using " << (is_internal_ ? "internal" : "external")
            << " kerberos principal \"" << service_name_ << "/"
            << hostname_ << "@" << realm_ << "\"";

  return Status::OK();
}

// For the environment variable attr, append "-Dthing=thingval" if "thing" is not already
// in the current attr's value.
static Status EnvAppend(const string& attr, const string& thing, const string& thingval) {
  // Carefully append to attr. There are three distinct cases:
  // 1. Attr doesn't exist: set it
  // 2. Attr exists, and doesn't contain thing: append to it
  // 3. Attr exists, and already contains thing: do nothing
  string current_val;
  char* current_val_c = getenv(attr.c_str());
  if (current_val_c != NULL) {
    current_val = current_val_c;
  }

  if (!current_val.empty() && (current_val.find(thing) != string::npos)) {
    // Case 3 above
    return Status::OK();
  }

  stringstream val_out;
  if (!current_val.empty()) {
    // Case 2 above
    val_out << current_val << " ";
  }
  val_out << "-D" << thing << "=" << thingval;

  if (setenv(attr.c_str(), val_out.str().c_str(), 1) < 0) {
    return Status(Substitute("Bad $0=$1 value: Could not set environment variable $2: $3",
        thing, thingval, attr, GetStrErrMsg()));
  }

  return Status::OK();
}

Status AuthManager::InitKerberosEnv() {
  if (IsKerberosEnabled()) {
    RETURN_IF_ERROR(CheckReplayCacheDirPermissions());
    if (FLAGS_keytab_file.empty()) {
      return Status("--keytab_file must be configured if kerberos is enabled");
    }
    if (FLAGS_krb5_ccname.empty()) {
      return Status("--krb5_ccname must be configured if kerberos is enabled");
    }
  }

  if (!FLAGS_keytab_file.empty()) {
    if (!is_regular(FLAGS_keytab_file)) {
      return Status(Substitute("Bad --keytab_file value: The file $0 is not a "
          "regular file", FLAGS_keytab_file));
    }

    // Set the keytab name in the environment so that Sasl Kerberos and kinit can
    // find and use it.
    if (setenv("KRB5_KTNAME", FLAGS_keytab_file.c_str(), 1)) {
      return Status(Substitute("Kerberos could not set KRB5_KTNAME: $0",
          GetStrErrMsg()));
    }
  }

  if (!FLAGS_krb5_ccname.empty()) {
    // We want to set a custom location for the impala credential cache.
    // Usually, it's /tmp/krb5cc_xxx where xxx is the UID of the process.  This
    // is normally fine, but if you're not running impala daemons as user
    // 'impala', the kinit we perform is going to blow away credentials for the
    // current user.  Not setting this isn't technically fatal, so ignore errors.
    const path krb5_ccname_path(FLAGS_krb5_ccname);
    if (!krb5_ccname_path.is_absolute()) {
      return Status(Substitute("Bad --krb5_ccname value: $0 is not an absolute file path",
          FLAGS_krb5_ccname));
    }
    discard_result(setenv("KRB5CCNAME", FLAGS_krb5_ccname.c_str(), 1));
  }

  // If an alternate krb5_conf location is supplied, set both KRB5_CONFIG and
  // JAVA_TOOL_OPTIONS in the environment.
  if (!FLAGS_krb5_conf.empty()) {
    // Ensure it points to a regular file
    if (!is_regular(FLAGS_krb5_conf)) {
      return Status(Substitute("Bad --krb5_conf value: The file $0 is not a "
          "regular file", FLAGS_krb5_conf));
    }

    // Overwrite KRB5_CONFIG
    if (setenv("KRB5_CONFIG", FLAGS_krb5_conf.c_str(), 1) < 0) {
      return Status(Substitute("Bad --krb5_conf value: Could not set "
          "KRB5_CONFIG: $0", GetStrErrMsg()));
    }

    RETURN_IF_ERROR(EnvAppend("JAVA_TOOL_OPTIONS", "java.security.krb5.conf",
        FLAGS_krb5_conf));

    LOG(INFO) << "Using custom Kerberos configuration file at "
              << FLAGS_krb5_conf;
  }

  // Set kerberos debugging, if applicable.  Errors are non-fatal.
  if (!FLAGS_krb5_debug_file.empty()) {
    bool krb5_debug_fail = false;
    if (setenv("KRB5_TRACE", FLAGS_krb5_debug_file.c_str(), 1) < 0) {
      LOG(WARNING) << "Failed to set KRB5_TRACE; --krb5_debug_file not enabled for "
                      "back-end code";
      krb5_debug_fail = true;
    }
    if (!EnvAppend("JAVA_TOOL_OPTIONS", "sun.security.krb5.debug", "true").ok()) {
      LOG(WARNING) << "Failed to set JAVA_TOOL_OPTIONS; --krb5_debug_file not enabled "
                      "for front-end code";
      krb5_debug_fail = true;
    }
    if (!krb5_debug_fail) {
      LOG(INFO) << "Kerberos debugging is enabled; kerberos messages written to "
                << FLAGS_krb5_debug_file;
    }
  }
  return Status::OK();
}

Status SecureAuthProvider::Start() {
  if (has_ldap_) {
    DCHECK(!is_internal_);
    if (!FLAGS_ldap_ca_certificate.empty()) {
      int set_rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_CACERTFILE,
          FLAGS_ldap_ca_certificate.c_str());
      if (set_rc != LDAP_SUCCESS) {
        return Status(Substitute("Could not set location of LDAP server cert: $0",
            ldap_err2string(set_rc)));
      }
    } else {
      // A warning was already logged...
      int val = LDAP_OPT_X_TLS_ALLOW;
      int set_rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_REQUIRE_CERT,
          reinterpret_cast<void*>(&val));
      if (set_rc != LDAP_SUCCESS) {
        return Status(Substitute(
            "Could not disable certificate requirement for LDAP server: $0",
            ldap_err2string(set_rc)));
      }
    }

    if (hostname_.empty()) {
      RETURN_IF_ERROR(GetHostname(&hostname_));
    }
  }

  return Status::OK();
}

Status SecureAuthProvider::GetServerTransportFactory(
    ThriftServer::TransportType underlying_transport_type, const std::string& server_name,
    MetricGroup* metrics, std::shared_ptr<TTransportFactory>* factory) {
  DCHECK(!principal_.empty() || has_ldap_ || has_saml_ || has_jwt_);

  if (underlying_transport_type == ThriftServer::HTTP) {
    bool has_kerberos = !principal_.empty();
    bool use_cookies = FLAGS_max_cookie_lifetime_s > 0;
    bool check_trusted_domain = !FLAGS_trusted_domain.empty();
    bool check_trusted_auth_header = !FLAGS_trusted_auth_header.empty();
    factory->reset(new THttpServerTransportFactory(server_name, metrics, has_ldap_,
        has_kerberos, use_cookies, check_trusted_domain, check_trusted_auth_header,
        has_saml_, has_jwt_));
    return Status::OK();
  }

  DCHECK(underlying_transport_type == ThriftServer::BINARY);

  DCHECK(!principal_.empty() || has_ldap_);

  // This is the heart of the link between this file and thrift.  Here we
  // associate a Sasl mechanism with our callbacks.
  try {
    map<string, string> sasl_props; // Empty; unused by Thrift
    TSaslServerTransport::Factory* sst_factory = NULL;
    factory->reset(sst_factory = new TSaslServerTransport::Factory());

    if(!principal_.empty()) {
      // Tell it about Kerberos:
      sst_factory->addServerDefinition(KERBEROS_MECHANISM, service_name_,
          hostname_, realm_, 0, sasl_props,
          is_internal_ ? KERB_INT_CALLBACKS : KERB_EXT_CALLBACKS);
    }

    if (has_ldap_) {
      // Tell it about LDAP:
      sst_factory->addServerDefinition(PLAIN_MECHANISM, "LDAP", hostname_,
          "", 0, sasl_props, LDAP_EXT_CALLBACKS);
    }

  } catch (const TException& e) {
    LOG(ERROR) << "Failed to create Sasl Server transport factory: "
               << e.what();
    return Status(e.what());
  }

  VLOG_RPC << "Made " << (is_internal_ ? "internal" : "external")
           << " server transport factory with "
           << (!principal_.empty() ? "Kerberos " : " ")
           << (has_ldap_ ? "LDAP " : " ") << "authentication";

  return Status::OK();
}

Status SecureAuthProvider::WrapClientTransport(const string& hostname,
    std::shared_ptr<TTransport> raw_transport, const string& service_name,
    std::shared_ptr<TTransport>* wrapped_transport) {
  std::shared_ptr<sasl::TSasl> sasl_client;
  const map<string, string> props; // Empty; unused by thrift
  const string auth_id; // Empty; unused by thrift

  DCHECK(!has_ldap_);
  DCHECK(is_internal_);

  // Since the daemons are never LDAP clients, we go straight to Kerberos
  try {
    const string& service = service_name.empty() ? service_name_ : service_name;
    sasl_client.reset(new sasl::TSaslClient(KERBEROS_MECHANISM, auth_id,
        service, hostname, props, KERB_INT_CALLBACKS.data()));
  } catch (sasl::SaslClientImplException& e) {
    LOG(ERROR) << "Failed to create a GSSAPI/SASL client: " << e.what();
    return Status(e.what());
  }
  wrapped_transport->reset(new TSaslClientTransport(sasl_client, raw_transport));
  // Verify that the wrapped transport inherits the max message size properly.
  VerifyMaxMessageSizeInheritance(raw_transport.get(), wrapped_transport->get());

  // This function is called immediately prior to sasl_client_start(), and so
  // can be used to log an "I'm beginning authentication for this principal"
  // message.  Unfortunately, there are no hooks for us at this level to say
  // that we successfully authenticated as a client.
  VLOG_RPC << "Initiating client connection using principal " << principal_;

  return Status::OK();
}

void SecureAuthProvider::SetupConnectionContext(
    const shared_ptr<ThriftServer::ConnectionContext>& connection_ptr,
    ThriftServer::TransportType underlying_transport_type,
    TTransport* input_transport, TTransport* output_transport) {
  TSocket* socket = nullptr;
  switch (underlying_transport_type) {
    case ThriftServer::BINARY: {
      TBufferedTransport* buffered_transport =
          down_cast<TBufferedTransport*>(input_transport);
      TSaslServerTransport* sasl_transport = down_cast<TSaslServerTransport*>(
          buffered_transport->getUnderlyingTransport().get());
      socket = down_cast<TSocket*>(sasl_transport->getUnderlyingTransport().get());
      // Get the username from the transport.
      connection_ptr->username = sasl_transport->getUsername();
      if (sasl_transport->getMechanismName() == KERBEROS_MECHANISM) {
        // Save the username as Kerberos user principal in the connection context
        // if the actual auth mechanism is Kerberos.
        connection_ptr->kerberos_user_principal = connection_ptr->username;
      }
      break;
    }
    case ThriftServer::HTTP: {
      THttpServer* http_input_transport = down_cast<THttpServer*>(input_transport);
      THttpServer* http_output_transport = down_cast<THttpServer*>(output_transport);
      THttpServer::HttpCallbacks callbacks;

      string saml_path;
      if (has_saml_) {
        Status parse_status = ParseSamlSpUrl(&saml_path);
        DCHECK(parse_status.ok());
      }
      callbacks.path_fn = std::bind(
          HttpPathFn, connection_ptr.get(), saml_path, std::placeholders::_1,
          std::placeholders::_2, std::placeholders::_3);
      callbacks.return_headers_fn = std::bind(ReturnHeaders, connection_ptr.get());
      callbacks.cookie_auth_fn =
          std::bind(CookieAuth, connection_ptr.get(), hash_, std::placeholders::_1);
      callbacks.trusted_domain_check_fn = std::bind(TrustedDomainCheck,
          connection_ptr.get(), hash_, std::placeholders::_1, std::placeholders::_2);
      if (has_ldap_) {
        callbacks.basic_auth_fn =
            std::bind(BasicAuth, connection_ptr.get(), hash_, std::placeholders::_1);
      }
      if (!principal_.empty()) {
        callbacks.negotiate_auth_fn = std::bind(NegotiateAuth, connection_ptr.get(),
            hash_, std::placeholders::_1, std::placeholders::_2);
      }
      if (has_saml_) {
        callbacks.init_wrapped_http_request_fn = std::bind(
            InitWrappedHttpRequest, connection_ptr.get());
        callbacks.get_saml_redirect_fn =
            std::bind(GetSaml2Redirect, connection_ptr.get());
        callbacks.validate_saml2_authn_response_fn =
            std::bind(ValidateSaml2AuthnResponse, connection_ptr.get(), hash_);
        callbacks.validate_saml2_bearer_fn =
            std::bind(ValidateSaml2Bearer, connection_ptr.get(), hash_);
      }
      if (has_jwt_) {
        callbacks.jwt_token_auth_fn =
            std::bind(JWTTokenAuth, connection_ptr.get(), hash_, std::placeholders::_1);
      }
      if (!FLAGS_trusted_auth_header.empty()) {
        callbacks.trusted_auth_header_handle_fn = std::bind(
            HandleTrustedAuthHeader, connection_ptr.get(), hash_, std::placeholders::_1);
      }
      http_input_transport->setCallbacks(callbacks);
      http_output_transport->setCallbacks(callbacks);
      socket = down_cast<TSocket*>(http_input_transport->getUnderlyingTransport().get());
      break;
    }
    default:
      LOG(FATAL) << Substitute("Bad transport type: $0", underlying_transport_type);
  }
  connection_ptr->network_address =
      MakeNetworkAddress(socket->getPeerAddress(), socket->getPeerPort());
}

Status NoAuthProvider::GetServerTransportFactory(
    ThriftServer::TransportType underlying_transport_type, const std::string& server_name,
    MetricGroup* metrics, std::shared_ptr<TTransportFactory>* factory) {
  // No Sasl - yawn.  Here, have a regular old buffered transport.
  switch (underlying_transport_type) {
    case ThriftServer::BINARY:
      factory->reset(new ThriftServer::BufferedTransportFactory());
      break;
    case ThriftServer::HTTP:
      factory->reset(new THttpServerTransportFactory());
      break;
    default:
      LOG(FATAL) << Substitute("Bad transport type: $0", underlying_transport_type);
  }
  return Status::OK();
}

Status NoAuthProvider::WrapClientTransport(const string& hostname,
    std::shared_ptr<TTransport> raw_transport, const string& dummy_service,
    std::shared_ptr<TTransport>* wrapped_transport) {
  // No Sasl - yawn.  Don't do any transport wrapping for clients.
  *wrapped_transport = raw_transport;
  return Status::OK();
}

void NoAuthProvider::SetupConnectionContext(
    const std::shared_ptr<ThriftServer::ConnectionContext>& connection_ptr,
    ThriftServer::TransportType underlying_transport_type,
    TTransport* input_transport, TTransport* output_transport) {
  connection_ptr->username = "";
  connection_ptr->do_as_user = "";
  TSocket* socket = nullptr;
  switch (underlying_transport_type) {
    case ThriftServer::BINARY: {
      TBufferedTransport* buffered_transport =
          down_cast<TBufferedTransport*>(input_transport);
      socket = down_cast<TSocket*>(buffered_transport->getUnderlyingTransport().get());
      break;
    }
    case ThriftServer::HTTP: {
      THttpServer* http_input_transport = down_cast<THttpServer*>(input_transport);
      THttpServer* http_output_transport = down_cast<THttpServer*>(input_transport);
      THttpServer::HttpCallbacks callbacks;
      // Even though there's no security, we set up some callbacks, eg. to allow
      // impersonation over unsecured connections for testing purposes.
      callbacks.path_fn = std::bind(
          HttpPathFn, connection_ptr.get(), "", std::placeholders::_1,
          std::placeholders::_2, std::placeholders::_3);
      callbacks.return_headers_fn = std::bind(ReturnHeaders, connection_ptr.get());
      http_input_transport->setCallbacks(callbacks);
      http_output_transport->setCallbacks(callbacks);
      socket = down_cast<TSocket*>(http_input_transport->getUnderlyingTransport().get());
      break;
    }
    default:
      LOG(FATAL) << Substitute("Bad transport type: $0", underlying_transport_type);
  }
  connection_ptr->network_address =
      MakeNetworkAddress(socket->getPeerAddress(), socket->getPeerPort());
}

AuthManager::AuthManager() {}

AuthManager::~AuthManager() {}

Status AuthManager::Init() {
  // Tell Thrift not to initialize SSL for us, as we use Kudu's SSL initializtion.
  TSSLSocketFactory::setManualOpenSSLInitialization(true);
  kudu::security::InitializeOpenSSL();
  LOG(INFO) << "Initialized " << OPENSSL_VERSION_TEXT;
  LOG(INFO) << "Runtime OpenSSL version " << SSLeay_version(SSLEAY_VERSION);
  unsigned long openssl_version = SSLeay();
  // Check if we are running against an OpenSSL version that is vulnerable to
  // CVE-2009-3555
  if (openssl_version >= 0x10100000L && openssl_version <= 0x1010007fL)  {
    LOG(WARNING) <<
        "OpenSSL runtime version detected that is vulnerable to CVE-2009-3555";
  }

  // Could use any other requiered flag for SAML
  bool use_saml = !FLAGS_saml2_sp_callback_url.empty();
  if (use_saml) {
    RETURN_IF_ERROR(ParseSamlSpUrl(nullptr));
    if (!IsExternalTlsConfigured()) {
      if (!FLAGS_saml2_allow_without_tls_debug_only) {
        return Status("SAML SSO authentication should be only used with TLS enabled.");
      }
      LOG(WARNING) << "SAML SSO authentication is used without TLS.";
    }
  }

  bool use_jwt = FLAGS_jwt_token_auth;
  if (use_jwt) {
    if (!IsExternalTlsConfigured()) {
      if (!FLAGS_jwt_allow_without_tls) {
        return Status("JWT authentication should be only used with TLS enabled.");
      }
      LOG(WARNING) << "JWT authentication is used without TLS.";
    }
    if (FLAGS_jwt_custom_claim_username.empty()) {
      return Status(
          "JWT authentication requires jwt_custom_claim_username to be specified.");
    }
  }

  // Get all of the flag validation out of the way
  if (FLAGS_enable_ldap_auth) {
    RETURN_IF_ERROR(
        ImpalaLdap::CreateLdap(&ldap_, FLAGS_ldap_user_filter, FLAGS_ldap_group_filter));
  }

  if (FLAGS_principal.empty() && !FLAGS_be_principal.empty()) {
    return Status("A back end principal (--be_principal) was supplied without "
        "also supplying a regular principal (--principal). Either --principal "
        "must be supplied alone, in which case it applies to all communication, "
        "or --principal and --be_principal must be supplied together, in which "
        "case --principal is used in external communication and --be_principal "
        "is used in internal (back-end) communication.");
  }

  RETURN_IF_ERROR(InitKerberosEnv());

  // This is written from the perspective of the daemons - thus "internal"
  // means "I am used for communication with other daemons, both as a client
  // and as a server".  "External" means that "I am used when being a server
  // for clients that are external - that is, they aren't daemons - like the
  // impala shell, odbc, jdbc, etc.
  //
  // Note that Kerberos and LDAP are enabled when --principal and --enable_ldap_auth are
  // set, respectively.
  //
  // Flags     | Internal | External
  // --------- | -------- | --------
  // None      | NoAuth   | NoAuth
  // LDAP only | NoAuth   | Sasl(ldap)
  // Kerb only | Sasl(be) | Sasl(fe)
  // Both      | Sasl(be) | Sasl(fe+ldap)
  //
  // --skip_internal_kerberos_auth and --skip_external_kerberos_auth disable Kerberos
  // auth for the Internal and External columns respectively.
  //
  // Set up the internal auth provider as per above.  Since there's no LDAP on
  // the client side, this is just a check for the "back end" kerberos
  // principal.
  // When acting as a client, or as a server on internal connections:
  string kerberos_internal_principal;
  // When acting as a server on external connections:
  string kerberos_external_principal;
  if (IsKerberosEnabled()) {
    RETURN_IF_ERROR(GetInternalKerberosPrincipal(&kerberos_internal_principal));
    RETURN_IF_ERROR(GetExternalKerberosPrincipal(&kerberos_external_principal));
    DCHECK(!kerberos_internal_principal.empty());
    DCHECK(!kerberos_external_principal.empty());
  }

  if (IsInternalKerberosEnabled()) {
    // Initialize the auth provider first, in case validation of the principal fails.
    SecureAuthProvider* sap = nullptr;
    internal_auth_provider_.reset(sap = new SecureAuthProvider(true));
    RETURN_IF_ERROR(sap->InitKerberos(kerberos_internal_principal));
    LOG(INFO) << "Internal communication is authenticated with Kerberos";
  } else {
    internal_auth_provider_.reset(new NoAuthProvider());
    LOG(INFO) << "Internal communication is not authenticated";
  }

  bool external_kerberos_enabled = IsExternalKerberosEnabled();
  // Set up the external auth provider as per above.  Either a "front end"
  // principal or ldap tells us to use a SecureAuthProvider, and we fill in
  // details from there.
  if (FLAGS_enable_ldap_auth || external_kerberos_enabled) {
    SecureAuthProvider* sap = nullptr;
    external_auth_provider_.reset(sap = new SecureAuthProvider(false));
    if (external_kerberos_enabled) {
      RETURN_IF_ERROR(sap->InitKerberos(kerberos_external_principal));
      LOG(INFO) << "External communication is authenticated with Kerberos";
    }
    if (FLAGS_enable_ldap_auth) {
      sap->InitLdap();
      LOG(INFO) << "External communication is authenticated with LDAP";
    }
    if (use_saml) {
      LOG(INFO) << "External communication can be also authenticated with SAML2 SSO";
      sap->InitSaml();
    }
    if (use_jwt) {
      LOG(INFO) << "External communication can be also authenticated with JWT";
      sap->InitJwt();
    }
  } else {
    external_auth_provider_.reset(new NoAuthProvider());
    LOG(INFO) << "External communication is not authenticated for binary protocols";
    if (use_saml) {
      SecureAuthProvider* sap = nullptr;
      external_http_auth_provider_.reset(sap = new SecureAuthProvider(false));
      sap->InitSaml();
      LOG(INFO) << "External communication is authenticated for hs2-http protocol with "
                   "SAML2 SSO";
    } else if (use_jwt) {
      SecureAuthProvider* sap = nullptr;
      external_http_auth_provider_.reset(sap = new SecureAuthProvider(false));
      sap->InitJwt();
      LOG(INFO)
          << "External communication is authenticated for hs2-http protocol with JWT";
    } else {
      LOG(INFO) << "External communication is not authenticated for hs2-http protocol";
    }
  }

  // Acquire a kerberos ticket and start the background renewal thread before starting
  // the auth providers. Do this after the InitKerberos() calls above which validate the
  // principal format so that we don't try to do anything before the flags have been
  // validated.
  if (IsKerberosEnabled()) {
    // IMPALA-8154: Disable any Kerberos auth_to_local mappings.
    FLAGS_use_system_auth_to_local = false;
    // Starts a thread that periodically does a 'kinit'. The thread lives as long as the
    // process does. We only need to kinit as the internal principal, because that is the
    // identity we will use for authentication with both other Impala services (impalad,
    // statestore, catalogd) and other services lower in the stack (HMS, HDFS, Kudu, etc).
    // We do not need a TGT for the external principal because we do not create any
    // connections using that principal.
    KUDU_RETURN_IF_ERROR(kudu::security::InitKerberosForServer(
        kerberos_internal_principal, FLAGS_keytab_file,
        FLAGS_krb5_ccname, false), "Could not init kerberos");
    LOG(INFO) << "Kerberos ticket granted to " << kerberos_internal_principal;
  }
  RETURN_IF_ERROR(internal_auth_provider_->Start());
  RETURN_IF_ERROR(external_auth_provider_->Start());
  if (FLAGS_external_fe_port > 0 ||
      (TestInfo::is_test() && FLAGS_external_fe_port == 0)) {
    external_fe_auth_provider_.reset(new NoAuthProvider());
    LOG(INFO) << "External Frontend communication is not authenticated";
    RETURN_IF_ERROR(external_fe_auth_provider_->Start());
  }
  return Status::OK();
}

AuthProvider* AuthManager::GetExternalAuthProvider() {
  DCHECK(external_auth_provider_.get() != NULL);
  return external_auth_provider_.get();
}

AuthProvider* AuthManager::GetExternalFrontendAuthProvider() {
  DCHECK(external_fe_auth_provider_.get() != NULL);
  return external_fe_auth_provider_.get();
}

AuthProvider* AuthManager::GetInternalAuthProvider() {
  DCHECK(internal_auth_provider_.get() != NULL);
  return internal_auth_provider_.get();
}

AuthProvider* AuthManager::GetExternalHttpAuthProvider() {
  return external_http_auth_provider_.get() != nullptr ?
      external_http_auth_provider_.get() : GetExternalAuthProvider();
}

}
