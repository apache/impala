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
#include <boost/thread/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>
#include <random>
#include <string>
#include <vector>
#include <thrift/Thrift.h>
#include <transport/TSasl.h>
#include <transport/TSaslServerTransport.h>
#include <gflags/gflags.h>

#include <ldap.h>

#include "exec/kudu-util.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/init.h"
#include "rpc/auth-provider.h"
#include "rpc/thrift-server.h"
#include "transport/TSaslClientTransport.h"
#include "util/auth-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/network-util.h"
#include "util/os-util.h"
#include "util/promise.h"
#include "util/thread.h"
#include "util/time.h"

#include <sys/types.h>    // for stat system call
#include <sys/stat.h>     // for stat system call
#include <unistd.h>       // for stat system call

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::replace_all;
using boost::algorithm::split;
using boost::algorithm::trim;
using boost::mt19937;
using boost::uniform_int;
using namespace apache::thrift;
using namespace boost::filesystem;   // for is_regular()
using namespace strings;

DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(be_principal);
DECLARE_string(krb5_conf);
DECLARE_string(krb5_debug_file);

// TODO: Remove this flag in a compatibility-breaking release. (IMPALA-5893)
DEFINE_int32(kerberos_reinit_interval, 60,
    "Interval, in minutes, between kerberos ticket renewals. "
    "Only used when FLAGS_use_krpc is false");

DEFINE_string(sasl_path, "", "Colon separated list of paths to look for SASL "
    "security library plugins.");
DEFINE_bool(enable_ldap_auth, false,
    "If true, use LDAP authentication for client connections");

DEFINE_string(ldap_uri, "", "The URI of the LDAP server to authenticate users against");
DEFINE_bool(ldap_tls, false, "If true, use the secure TLS protocol to connect to the LDAP"
    " server");
DEFINE_string(ldap_ca_certificate, "", "The full path to the certificate file used to"
    " authenticate the LDAP server's certificate for SSL / TLS connections.");
DEFINE_bool(ldap_passwords_in_clear_ok, false, "If set, will allow LDAP passwords "
    "to be sent in the clear (without TLS/SSL) over the network.  This option should not "
    "be used in production environments" );
DEFINE_bool(ldap_allow_anonymous_binds, false, "(Advanced) If true, LDAP authentication "
    "with a blank password (an 'anonymous bind') is allowed by Impala.");
DEFINE_bool(ldap_manual_config, false, "Obsolete; Ignored");
DEFINE_string(ldap_domain, "", "If set, Impala will try to bind to LDAP with a name of "
    "the form <userid>@<ldap_domain>");
DEFINE_string(ldap_baseDN, "", "If set, Impala will try to bind to LDAP with a name of "
    "the form uid=<userid>,<ldap_baseDN>");
DEFINE_string(ldap_bind_pattern, "", "If set, Impala will try to bind to LDAP with a name"
     " of <ldap_bind_pattern>, but where the string #UID is replaced by the user ID. Use"
     " to control the bind name precisely; do not set --ldap_domain or --ldap_baseDN with"
     " this option");
DEFINE_string(internal_principals_whitelist, "hdfs", "(Advanced) Comma-separated list of "
    " additional usernames authorized to access Impala's internal APIs. Defaults to "
    "'hdfs' which is the system user that in certain deployments must access "
    "catalog server APIs.");

// TODO: Remove this flag and the old kerberos code in a compatibility-breaking release.
// (IMPALA-5893)
DEFINE_bool(use_kudu_kinit, true, "If true, Impala will programatically perform kinit "
    "by calling into the libkrb5 library using the provided APIs. If false, it will fork "
    "off a kinit process.");

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

// Path to the file based credential cache that we pass to the KRB5CCNAME environment
// variable.
static const string KRB5CCNAME_PATH = "/tmp/krb5cc_impala_internal";

// Constants for the two Sasl mechanisms we support
static const string KERBEROS_MECHANISM = "GSSAPI";
static const string PLAIN_MECHANISM = "PLAIN";

// Required prefixes for ldap URIs:
static const string LDAP_URI_PREFIX = "ldap://";
static const string LDAPS_URI_PREFIX = "ldaps://";

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

// This callback is only called when we're providing LDAP authentication. This "check
// pass" callback is our hook to ask the real LDAP server if we're allowed to log in or
// not. We can be thought of as a proxy for LDAP logins - the user gives their password
// to us, and we pass it to the real LDAP server.
//
// Note that this method uses ldap_sasl_bind_s(), which does *not* provide any security
// to the connection between Impala and the LDAP server. You must either set --ldap_tls,
// or have a URI which has "ldaps://" as the scheme in order to get a secure connection.
// Use --ldap_ca_certificate to specify the location of the certificate used to confirm
// the authenticity of the LDAP server certificate.
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
  if (passlen == 0 && !FLAGS_ldap_allow_anonymous_binds) {
    // Disable anonymous binds.
    return SASL_FAIL;
  }

  LDAP* ld;
  int rc = ldap_initialize(&ld, FLAGS_ldap_uri.c_str());
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "Could not initialize connection with LDAP server ("
                 << FLAGS_ldap_uri << "). Error: " << ldap_err2string(rc);
    return SASL_FAIL;
  }

  // Force the LDAP version to 3 to make sure TLS is supported.
  int ldap_ver = 3;
  ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &ldap_ver);

  // If -ldap_tls is turned on, and the URI is ldap://, issue a STARTTLS operation.
  // Note that we'll ignore -ldap_tls when using ldaps:// because we've already
  // got a secure connection (and the LDAP server will reject the STARTTLS).
  if (FLAGS_ldap_tls && (FLAGS_ldap_uri.find(LDAP_URI_PREFIX) == 0)) {
    int tls_rc = ldap_start_tls_s(ld, NULL, NULL);
    if (tls_rc != LDAP_SUCCESS) {
      LOG(WARNING) << "Could not start TLS secure connection to LDAP server ("
                   << FLAGS_ldap_uri << "). Error: " << ldap_err2string(tls_rc);
      ldap_unbind_ext(ld, NULL, NULL);
      return SASL_FAIL;
    }
    VLOG(2) << "Started TLS connection with LDAP server: " << FLAGS_ldap_uri;
  }

  // Map the user string into an acceptable LDAP "DN" (distinguished name)
  string user_str = user;
  if (!FLAGS_ldap_domain.empty()) {
    // Append @domain if there isn't already an @ in the user string.
    if (user_str.find("@") == string::npos) {
      user_str = Substitute("$0@$1", user_str, FLAGS_ldap_domain);
    }
  } else if (!FLAGS_ldap_baseDN.empty()) {
    user_str = Substitute("uid=$0,$1", user_str, FLAGS_ldap_baseDN);
  } else if (!FLAGS_ldap_bind_pattern.empty()) {
    user_str = FLAGS_ldap_bind_pattern;
    replace_all(user_str, "#UID", user);
  }

  // Map the password into a credentials structure
  struct berval cred;
  cred.bv_val = const_cast<char*>(pass);
  cred.bv_len = passlen;

  VLOG_QUERY << "Trying simple LDAP bind for: " << user_str;

  rc = ldap_sasl_bind_s(ld, user_str.c_str(), LDAP_SASL_SIMPLE, &cred,
      NULL, NULL, NULL);
  // Free ld
  ldap_unbind_ext(ld, NULL, NULL);
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "LDAP authentication failure for " << user_str
                 << " : " << ldap_err2string(rc);
    return SASL_FAIL;
  }

  VLOG_QUERY << "LDAP bind successful";

  return SASL_OK;
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
  SaslAuthProvider* internal_auth_provider;
  if (context == NULL) {
    internal_auth_provider = static_cast<SaslAuthProvider*>(
        AuthManager::GetInstance()->GetInternalAuthProvider());
  } else {
    // Branch should only be taken for testing, where context is used to inject an auth
    // provider.
    internal_auth_provider = static_cast<SaslAuthProvider*>(context);
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

// This callback could be used to authorize or restrict access to certain
// users.  Currently it is used to log a message that we successfully
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
static int SaslAuthorizeExternal(sasl_conn_t* conn, void* context,
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

// When operating as a Kerberos client (internal connections only), we need to
// 'kinit' as the principal.  A thread is created and calls this function for
// that purpose, and to periodically renew the ticket as well.
//
// first_kinit: Used to communicate success/failure of the initial kinit call to
//              the parent thread
// Return: Only if the first call to 'kinit' fails
void SaslAuthProvider::RunKinit(Promise<Status>* first_kinit) {

  // Pass the path to the key file and the principal.
  const string kinit_cmd = Substitute("kinit -k -t $0 $1 2>&1",
      keytab_file_, principal_);

  bool first_time = true;
  std::random_device rd;
  mt19937 generator(rd());
  uniform_int<> dist(0, 300);

  while (true) {
    LOG(INFO) << "Registering " << principal_ << ", keytab file " << keytab_file_;
    string kinit_output;
    bool success = RunShellProcess(kinit_cmd, &kinit_output);

    if (!success) {
      const string& err_msg = Substitute(
          "Failed to obtain Kerberos ticket for principal: $0. $1", principal_,
          kinit_output);
      if (first_time) {
        first_kinit->Set(Status(err_msg));
        return;
      } else {
        LOG(ERROR) << err_msg;
      }
    } else if (first_time) {
      first_time = false;
      first_kinit->Set(Status::OK());
    }

    // Sleep for the renewal interval, minus a random time between 0-5 minutes to help
    // avoid a storm at the KDC. Additionally, never sleep less than a minute to
    // reduce KDC stress due to frequent renewals.
    SleepForMs(1000 * max((60 * FLAGS_kerberos_reinit_interval) - dist(generator), 60));
  }
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
    if (!FLAGS_principal.empty()) {
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
      KERB_EXT_CALLBACKS[1].proc = (int (*)())&SaslAuthorizeExternal;
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
      LDAP_EXT_CALLBACKS[1].proc = (int (*)())&SaslAuthorizeExternal;
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

Status SaslAuthProvider::InitKerberos(const string& principal,
    const string& keytab_file) {

  principal_ = principal;
  keytab_file_ = keytab_file;
  // The logic here is that needs_kinit_ is false unless we are the internal
  // auth provider and we support kerberos.
  needs_kinit_ = is_internal_;

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
  DCHECK(!FLAGS_principal.empty());

  RETURN_IF_ERROR(CheckReplayCacheDirPermissions());

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

  // We want to set a custom location for the impala credential cache.
  // Usually, it's /tmp/krb5cc_xxx where xxx is the UID of the process.  This
  // is normally fine, but if you're not running impala daemons as user
  // 'impala', the kinit we perform is going to blow away credentials for the
  // current user.  Not setting this isn't technically fatal, so ignore errors.
  (void) setenv("KRB5CCNAME", "/tmp/krb5cc_impala_internal", 1);

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
      LOG(WARNING) << "Failed to set KRB5_TRACE; --krb5_debuf_file not enabled for "
          "back-end code";
      krb5_debug_fail = true;
    }
    if (!EnvAppend("JAVA_TOOL_OPTIONS", "sun.security.krb5.debug", "true").ok()) {
      LOG(WARNING) << "Failed to set JAVA_TOOL_OPTIONS; --krb5_debuf_file not enabled "
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

Status SaslAuthProvider::Start() {
  // True for kerberos internal use
  if (needs_kinit_) {
    DCHECK(is_internal_);
    DCHECK(!principal_.empty());
    if (FLAGS_use_kudu_kinit) {
      // Starts a thread that periodically does a 'kinit'. The thread lives as long as the
      // process does.
      KUDU_RETURN_IF_ERROR(kudu::security::InitKerberosForServer(principal_, keytab_file_,
          KRB5CCNAME_PATH, false), "Could not init kerberos");
    } else {
      Promise<Status> first_kinit;
      stringstream thread_name;
      thread_name << "kinit-" << principal_;
      RETURN_IF_ERROR(Thread::Create("authentication", thread_name.str(),
          &SaslAuthProvider::RunKinit, this, &first_kinit, &kinit_thread_));
      LOG(INFO) << "Waiting for Kerberos ticket for principal: " << principal_;
      RETURN_IF_ERROR(first_kinit.Get());
    }
    LOG(INFO) << "Kerberos ticket granted to " << principal_;
  }

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

Status SaslAuthProvider::GetServerTransportFactory(
    boost::shared_ptr<TTransportFactory>* factory) {
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

Status SaslAuthProvider::WrapClientTransport(const string& hostname,
    boost::shared_ptr<TTransport> raw_transport, const string& service_name,
    boost::shared_ptr<TTransport>* wrapped_transport) {

  boost::shared_ptr<sasl::TSasl> sasl_client;
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

  // This function is called immediately prior to sasl_client_start(), and so
  // can be used to log an "I'm beginning authentication for this principal"
  // message.  Unfortunately, there are no hooks for us at this level to say
  // that we successfully authenticated as a client.
  VLOG_RPC << "Initiating client connection using principal " << principal_;

  return Status::OK();
}

Status NoAuthProvider::GetServerTransportFactory(
    boost::shared_ptr<TTransportFactory>* factory) {
  // No Sasl - yawn.  Here, have a regular old buffered transport.
  factory->reset(new ThriftServer::BufferedTransportFactory());
  return Status::OK();
}

Status NoAuthProvider::WrapClientTransport(const string& hostname,
    boost::shared_ptr<TTransport> raw_transport, const string& dummy_service,
    boost::shared_ptr<TTransport>* wrapped_transport) {
  // No Sasl - yawn.  Don't do any transport wrapping for clients.
  *wrapped_transport = raw_transport;
  return Status::OK();
}

Status AuthManager::Init() {
  ssl_socket_factory_.reset(new TSSLSocketFactory());

  bool use_ldap = false;
  const string excl_msg = "--$0 and --$1 are mutually exclusive "
      "and should not be set together";

  // Get all of the flag validation out of the way
  if (FLAGS_enable_ldap_auth) {
    use_ldap = true;

    if (!FLAGS_ldap_domain.empty()) {
      if (!FLAGS_ldap_baseDN.empty()) {
        return Status(Substitute(excl_msg, "ldap_domain", "ldap_baseDN"));
      }
      if (!FLAGS_ldap_bind_pattern.empty()) {
        return Status(Substitute(excl_msg, "ldap_domain", "ldap_bind_pattern"));
      }
    } else if (!FLAGS_ldap_baseDN.empty()) {
      if (!FLAGS_ldap_bind_pattern.empty()) {
        return Status(Substitute(excl_msg, "ldap_baseDN", "ldap_bind_pattern"));
      }
    }

    if (FLAGS_ldap_uri.empty()) {
      return Status("--ldap_uri must be supplied when --ldap_enable_auth is set");
    }

    if ((FLAGS_ldap_uri.find(LDAP_URI_PREFIX) != 0) &&
        (FLAGS_ldap_uri.find(LDAPS_URI_PREFIX) != 0)) {
      return Status(Substitute("--ldap_uri must start with either $0 or $1",
              LDAP_URI_PREFIX, LDAPS_URI_PREFIX ));
    }

    LOG(INFO) << "Using LDAP authentication with server " << FLAGS_ldap_uri;

    if (!FLAGS_ldap_tls && (FLAGS_ldap_uri.find(LDAPS_URI_PREFIX) != 0)) {
      if (FLAGS_ldap_passwords_in_clear_ok) {
        LOG(WARNING) << "LDAP authentication is being used, but without TLS. "
                     << "ALL PASSWORDS WILL GO OVER THE NETWORK IN THE CLEAR.";
      } else {
        return Status("LDAP authentication specified, but without TLS. "
                      "Passwords would go over the network in the clear. "
                      "Enable TLS with --ldap_tls or use an ldaps:// URI. "
                      "To override this is non-production environments, "
                      "specify --ldap_passwords_in_clear_ok");
      }
    } else if (FLAGS_ldap_ca_certificate.empty()) {
      LOG(WARNING) << "LDAP authentication is being used with TLS, but without "
                   << "an --ldap_ca_certificate file, the identity of the LDAP "
                   << "server cannot be verified.  Network communication (and "
                   << "hence passwords) could be intercepted by a "
                   << "man-in-the-middle attack";
    }
  }

  if (FLAGS_principal.empty() && !FLAGS_be_principal.empty()) {
    return Status("A back end principal (--be_principal) was supplied without "
        "also supplying a regular principal (--principal). Either --principal "
        "must be supplied alone, in which case it applies to all communication, "
        "or --principal and --be_principal must be supplied together, in which "
        "case --principal is used in external communication and --be_principal "
        "is used in internal (back-end) communication.");
  }

  // When acting as a client, or as a server on internal connections:
  string kerberos_internal_principal;
  // When acting as a server on external connections:
  string kerberos_external_principal;

  bool use_kerberos = IsKerberosEnabled();
  if (use_kerberos) {
    RETURN_IF_ERROR(GetInternalKerberosPrincipal(&kerberos_internal_principal));
    RETURN_IF_ERROR(GetExternalKerberosPrincipal(&kerberos_external_principal));
    DCHECK(!kerberos_internal_principal.empty());
    DCHECK(!kerberos_external_principal.empty());

    RETURN_IF_ERROR(InitKerberosEnv());
  }

  // This is written from the perspective of the daemons - thus "internal"
  // means "I am used for communication with other daemons, both as a client
  // and as a server".  "External" means that "I am used when being a server
  // for clients that are external - that is, they aren't daemons - like the
  // impala shell, odbc, jdbc, etc.
  //
  // Flags     | Internal | External
  // --------- | -------- | --------
  // None      | NoAuth   | NoAuth
  // LDAP only | NoAuth   | Sasl(ldap)
  // Kerb only | Sasl(be) | Sasl(fe)
  // Both      | Sasl(be) | Sasl(fe+ldap)

  // Set up the internal auth provider as per above.  Since there's no LDAP on
  // the client side, this is just a check for the "back end" kerberos
  // principal.
  if (use_kerberos) {
    SaslAuthProvider* sap = NULL;
    internal_auth_provider_.reset(sap = new SaslAuthProvider(true));
    RETURN_IF_ERROR(sap->InitKerberos(kerberos_internal_principal,
        FLAGS_keytab_file));
    LOG(INFO) << "Internal communication is authenticated with Kerberos";
  } else {
    internal_auth_provider_.reset(new NoAuthProvider());
    LOG(INFO) << "Internal communication is not authenticated";
  }
  RETURN_IF_ERROR(internal_auth_provider_->Start());

  // Set up the external auth provider as per above.  Either a "front end"
  // principal or ldap tells us to use a SaslAuthProvider, and we fill in
  // details from there.
  if (use_ldap || use_kerberos) {
    SaslAuthProvider* sap = NULL;
    external_auth_provider_.reset(sap = new SaslAuthProvider(false));
    if (use_kerberos) {
      RETURN_IF_ERROR(sap->InitKerberos(kerberos_external_principal,
          FLAGS_keytab_file));
      LOG(INFO) << "External communication is authenticated with Kerberos";
    }
    if (use_ldap) {
      sap->InitLdap();
      LOG(INFO) << "External communication is authenticated with LDAP";
    }
  } else {
    external_auth_provider_.reset(new NoAuthProvider());
    LOG(INFO) << "External communication is not authenticated";
  }
  RETURN_IF_ERROR(external_auth_provider_->Start());

  return Status::OK();
}

AuthProvider* AuthManager::GetExternalAuthProvider() {
  DCHECK(external_auth_provider_.get() != NULL);
  return external_auth_provider_.get();
}

AuthProvider* AuthManager::GetInternalAuthProvider() {
  DCHECK(internal_auth_provider_.get() != NULL);
  return internal_auth_provider_.get();
}

}
