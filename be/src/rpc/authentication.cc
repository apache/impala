// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rpc/authentication.h"

#include <stdio.h>
#include <signal.h>
#include <boost/algorithm/string.hpp>
#include <boost/thread/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <gutil/strings/substitute.h>
#include <string>
#include <vector>
#include <thrift/Thrift.h>
#include <transport/TSasl.h>
#include <transport/TSaslServerTransport.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

// Make sure ldap_simple_bind_s() is available.
#define LDAP_DEPRECATED 1
#include <ldap.h>

#include "rpc/auth-provider.h"
#include "transport/TSaslClientTransport.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/network-util.h"
#include "util/os-util.h"
#include "util/promise.h"
#include "util/thread.h"
#include "util/time.h"

using namespace apache::thrift;
using namespace std;
using namespace strings;
using namespace boost;
using namespace boost::random;

DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(be_principal);
DECLARE_string(krb_credential_cache);

DEFINE_int32(kerberos_reinit_interval, 60,
    "Interval, in minutes, between kerberos ticket renewals. Each renewal will request "
    "a ticket with a lifetime that is at least 2x the renewal interval.");
DEFINE_string(sasl_path, "/usr/lib/sasl2:/usr/lib64/sasl2:/usr/local/lib/sasl2:"
    "/usr/lib/x86_64-linux-gnu/sasl2", "Colon separated list of paths to look for SASL "
    "security library plugins.");
DEFINE_bool(enable_ldap_auth, false,
    "If true, use LDAP authentication for client connections");

DEFINE_string(ldap_uri, "", "The URI of the LDAP server to authenticate users against");
DEFINE_bool(ldap_tls, false, "If true, use the secure TLS protocol to connect to the LDAP"
    " server");
DEFINE_string(ldap_ca_certificate, "", "The full path to the certificate file used to"
    " authenticate the LDAP server's certificate for SSL / TLS connections.");
DEFINE_bool(ldap_manual_config, false, "(Advanced) If true, use a custom SASL config file"
    " to configure access to an LDAP server.");

namespace impala {

// Array of default callbacks for the Sasl library. Initialised in InitAuth().
static vector<sasl_callback_t> SASL_CALLBACKS;

// Pattern for hostname substitution.
static const string HOSTNAME_PATTERN = "_HOST";

// Constants for the two Sasl mechanisms we support
static const std::string KERBEROS_MECHANISM = "GSSAPI";
static const std::string PLAIN_MECHANISM = "PLAIN";

// Passed to the SaslGetOption() callback by SASL to indicate that the callback is for
// LDAP, not any other mechanism.
static const void* LDAP_AUTH_CONTEXT = reinterpret_cast<void*>(1);

// Our dummy plugin needs a standard name.
static const string IMPALA_AUXPROP_PLUGIN = "impala-auxprop";

AuthManager* AuthManager::auth_manager_ = new AuthManager();

// Output Sasl messages.
// context: not used.
// level: logging level.
// message: message to output;
static int SaslLogCallback(void* context, int level,  const char* message) {
  if (message == NULL) return SASL_BADPARAM;

  switch (level) {
  case SASL_LOG_NONE:
    break;
  case SASL_LOG_ERR:
  case SASL_LOG_FAIL:
    LOG(ERROR) << "SASL message: " << message;
    break;
  case SASL_LOG_WARN:
    LOG(WARNING) << "SASL message: " << message;
    break;
  case SASL_LOG_NOTE:
    LOG(INFO) << "SASL message: " << message;
    break;
  case SASL_LOG_DEBUG:
    VLOG(1) << "SASL message: " << message;
    break;
  case SASL_LOG_TRACE:
  case SASL_LOG_PASS:
    VLOG(3) << "SASL message: " << message;
    break;
  }

  return SASL_OK;
}

// Called by the SASL library during LDAP authentication. This is where we 'cheat' and use
// this callback to confirm the user's credentials without implementing a fully-fledged
// auxprop plugin.
//
// Note that this method uses ldap_simple_bind_s(), which does *not* provide any security
// to the connection between Impala and the LDAP server. You must either set --ldap_tls,
// or have a URI which has "ldaps://" as the scheme in order to get a secure connection.
// Use --ldap_ca_certificate to specify the location of the certificate used to confirm
// the authenticity of the LDAP server certificate.
int SaslLdapCheckPass(sasl_conn_t* conn, void* context, const char* user,
    const char *pass, unsigned passlen, struct propctx *propctx) {
  LDAP* ld;
  int rc = ldap_initialize(&ld, FLAGS_ldap_uri.c_str());
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "Could not initialise connection with LDAP server ("
                 << FLAGS_ldap_uri << "). Error: " << ldap_err2string(rc);
    return SASL_FAIL;
  }

  // Force the LDAP version to 3 to make sure TLS is supported.
  int ldap_ver = 3;
  ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &ldap_ver);

  if (FLAGS_ldap_tls) {
    int tls_rc = ldap_start_tls_s(ld, NULL, NULL);
    if (tls_rc != LDAP_SUCCESS) {
      LOG(WARNING) << "Could not start TLS secure connection to LDAP server ("
                   << FLAGS_ldap_uri << "). Error: " << ldap_err2string(tls_rc);
      ldap_unbind(ld);
      return SASL_FAIL;
    }
    LOG(INFO) << "Started TLS connection with LDAP server: " << FLAGS_ldap_uri;
  }

  string pass_str(pass, passlen);
  rc = ldap_simple_bind_s(ld, user, pass_str.c_str());
  // Free ld
  ldap_unbind(ld);
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "LDAP bind failed: " << ldap_err2string(rc);
    return SASL_FAIL;
  }

  return SASL_OK;
}

// Get Sasl option.
// context: not used
// plugin_name: name of plugin for which an option is being requested.
// option: option requested
// result: value for option
// len: length of the result
// Return SASL_FAIL if the option is not handled, this does not fail the handshake.
static int SaslGetOption(void* context, const char* plugin_name, const char* option,
    const char** result, unsigned* len) {
  // Tell SASL to use our dummy plugin if we are using LDAP. If --ldap_manual_config is
  // set, users are expecting to override these settings so we don't respond to the SASL
  // request.
  if (strcmp("auxprop_plugin", option) == 0 && context == LDAP_AUTH_CONTEXT
      && !FLAGS_ldap_manual_config) {
    *result = IMPALA_AUXPROP_PLUGIN.c_str();
    if (len != NULL) *len = strlen(*result);
    return SASL_OK;
  }

  if (plugin_name == NULL) {
    // Return the logging level that we want the sasl library to use.
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
    }

    // Options can default so don't complain.
    VLOG(3) << "SaslGetOption: Unknown option: " << option;
    return SASL_FAIL;
  }

  if (strcmp(KERBEROS_MECHANISM.c_str(), plugin_name) == 0) {
    // Return the path to our keytab file.
    // TODO: why is this never called?
    // TODO: Always returns FLAGS_keytab_file, even though technically we might have
    // different keytabs for different principals. Ok for now, because in practice we
    // never have different keytabs.
    if (strcmp("keytab", option) == 0) {
        *result = FLAGS_keytab_file.c_str();
        if (len != NULL) *len = strlen(*result);
        return SASL_OK;
    }
    VLOG(3) << "SaslGetOption: Unknown option: " << option << " for plugin_name: "
            << plugin_name;
    return SASL_FAIL;
  }

  VLOG(3) << "SaslGetOption: Unknown plugin: " << plugin_name << " : " << option;
  return SASL_FAIL;

}

// Sasl Authorize callback.
// Can be used to restrict access.  Currently used for diagnostics.
// requsted_user, rlen: The user requesting access and string length.
// auth_identity, alen: The identity (principal) and length.
// default_realm, urlen: Realm of the user and length.
// propctx: properties requested.
static int SaslAuthorize(sasl_conn_t* conn, void* context, const char* requested_user,
    unsigned rlen, const char* auth_identity, unsigned alen, const char* def_realm,
    unsigned urlen, struct propctx* propctx) {

  string user(requested_user, rlen);
  string auth(auth_identity, alen);
  string realm(def_realm, urlen);
  VLOG_CONNECTION << "SASL authorize. User: " << user << " for: " << auth << " from "
                  << realm;
  return SASL_OK;
}

// Sasl Get Path callback.
// Returns the list of possible places for the plugins might be.
// Places we know they might be:
// UBUNTU:          /usr/lib/sasl2
// CENTOS:          /usr/lib64/sasl2
// custom install:  /usr/local/lib/sasl2
static int SaslGetPath(void* context, const char** path) {
  *path = FLAGS_sasl_path.c_str();
  return SASL_OK;
}

void KerberosAuthProvider::RunKinit(Promise<Status>* first_kinit) {
  // Minumum lifetime to request for each ticket renewal.
  static const int MIN_TICKET_LIFETIME_IN_MINS = 1440;
  LOG(INFO) << "Kerberos credential cache: " << credential_cache_;

  // Set the ticket lifetime to an arbitrarily large value or 2x the renewal interval,
  // whichever is larger. The KDC will automatically fall back to using the maximum
  // allowed allowed value if a longer lifetime is requested, so it is okay to be greedy
  // here.
  int ticket_lifetime_mins =
      max(MIN_TICKET_LIFETIME_IN_MINS, FLAGS_kerberos_reinit_interval * 2);

  // Pass the path to the key file and the principal. Make the ticket renewable.
  // Calling kinit -R ensures the ticket makes it to the cache, and should be a separate
  // call to kinit.
  const string kinit_cmd = Substitute("kinit -r $0m -c $1 -k -t $2 $3 2>&1",
      ticket_lifetime_mins, credential_cache_, keytab_path_, principal_);

  bool first_time = true;
  int failures_since_renewal = 0;
  while (true) {
    LOG(INFO) << "Registering " << principal_ << ", key_tab file " << keytab_path_;
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
    }

    if (success) {
      if (first_time) {
        first_time = false;
        first_kinit->Set(Status::OK);
      }
      failures_since_renewal = 0;
      // Workaround for Kerberos 1.8.1 - wait a short time, before requesting a renewal of
      // the ticket-granting ticket. The sleep time is >1s, to force the system clock to
      // roll-over o a new second, avoiding a race between grant and renewal.
      SleepForMs(1500);
      string krenew_output;
      if (RunShellProcess("kinit -R", &krenew_output)) {
        LOG(INFO) << "Successfully renewed Keberos ticket";
      } else {
        // We couldn't renew the ticket so just report the error. Existing connections
        // are ok and we'll try to renew the ticket later.
        ++failures_since_renewal;
        LOG(ERROR) << "Failed to extend Kerberos ticket. Error: " << krenew_output
                   << ". Failure count: " << failures_since_renewal;
      }
    }
    // Sleep for the renewal interval, minus a random time between 0-5 minutes to help
    // avoid a storm at the KDC. Additionally, never sleep less than a minute to
    // reduce KDC stress due to frequent renewals.
    mt19937 generator;
    uniform_int<> dist(0, 300);
    SleepForMs(1000 * max((60 * FLAGS_kerberos_reinit_interval) - dist(generator), 60));
  }
}

Status InitAuth(const string& appname) {
  // Application-wide set of callbacks.
  SASL_CALLBACKS.resize(5);
  SASL_CALLBACKS[0].id = SASL_CB_LOG;
  SASL_CALLBACKS[0].proc = (int (*)())&SaslLogCallback;
  SASL_CALLBACKS[0].context = NULL;
  SASL_CALLBACKS[1].id = SASL_CB_GETOPT;
  SASL_CALLBACKS[1].proc = (int (*)())&SaslGetOption;
  SASL_CALLBACKS[1].context = NULL;
  SASL_CALLBACKS[2].id = SASL_CB_PROXY_POLICY;
  SASL_CALLBACKS[2].proc = (int (*)())&SaslAuthorize;
  SASL_CALLBACKS[2].context = NULL;
  SASL_CALLBACKS[3].id = SASL_CB_GETPATH;
  SASL_CALLBACKS[3].proc = (int (*)())&SaslGetPath;
  SASL_CALLBACKS[3].context = NULL;
  SASL_CALLBACKS[SASL_CALLBACKS.size() - 1].id = SASL_CB_LIST_END;

  try {
    // We assume all impala processes are both server and client.
    sasl::TSaslServer::SaslInit(&SASL_CALLBACKS[0], appname);
    sasl::TSaslClient::SaslInit(&SASL_CALLBACKS[0]);
  } catch (sasl::SaslServerImplException& e) {
    stringstream err_msg;
    err_msg << "Could not initialize Sasl library: " << e.what();
    return Status(err_msg.str());
  }

  RETURN_IF_ERROR(AuthManager::GetInstance()->Init());
  return Status::OK;
}

KerberosAuthProvider::KerberosAuthProvider(const string& principal,
    const string& keytab_path, const string& credential_cache_path, bool needs_kinit)
    : principal_(principal), keytab_path_(keytab_path),
      credential_cache_(credential_cache_path),needs_kinit_(needs_kinit) {
}

Status KerberosAuthProvider::Start() {
  // The "keytab" callback is never called.  Set the file name in the environment.
  if (setenv("KRB5_KTNAME", keytab_path_.c_str(), 1)) {
    stringstream ss;
    ss << "Kerberos could not set KRB5_KTNAME: " << GetStrErrMsg();
    return Status(ss.str());
  }

  // Replace the string _HOST with our hostname.
  size_t off = principal_.find(HOSTNAME_PATTERN);
  if (off != string::npos) {
    string hostname;
    RETURN_IF_ERROR(GetHostname(&hostname));
    principal_.replace(off, HOSTNAME_PATTERN.size(), hostname);
  }

  vector<string> names;
  split(names, principal_, is_any_of("/@"));

  if (names.size() != 3) {
    stringstream ss;
    ss << "Kerberos principal should of the form: <service>/<hostname>@<realm> - got: "
       << principal_;
    return Status(ss.str());
  }

  service_name_ = names[0];
  hostname_ = names[1];
  // Realm (names[2]) is unused.

  if (needs_kinit_) {
    Promise<Status> first_kinit;
    stringstream thread_name;
    thread_name << "kinit-" << principal_;
    kinit_thread_.reset(new Thread("authentication", thread_name.str(),
        &KerberosAuthProvider::RunKinit, this, &first_kinit));
    LOG(INFO) << "Waiting for Kerberos ticket for principal: " << principal_;
    RETURN_IF_ERROR(first_kinit.Get());
    LOG(INFO) << "Kerberos ticket granted to " << principal_;
  }

  return Status::OK;
}

Status KerberosAuthProvider::GetServerTransportFactory(
    shared_ptr<TTransportFactory>* factory) {
  // The string should be service/hostname@realm

  try {
    // TSaslServerTransport::Factory doesn't actually do anything with the properties
    // argument, so we pass in an empty map
    map<string, string> sasl_props;
    factory->reset(new TSaslServerTransport::Factory(
        KERBEROS_MECHANISM, service_name_, hostname_, 0, sasl_props, SASL_CALLBACKS));
  } catch (const TException& e) {
    LOG(ERROR) << "Failed to create a Kerberos transport factory: " << e.what();
    return Status(e.what());
  }

  return Status::OK;
}

Status KerberosAuthProvider::WrapClientTransport(const string& hostname,
    shared_ptr<TTransport> raw_transport, const string& service_name,
    shared_ptr<TTransport>* wrapped_transport) {
  shared_ptr<sasl::TSasl> sasl_client;
  map<string, string> props;
  // We do not set this.
  string auth_id;

  try {
    string service = service_name.empty() ? service_name_ : service_name;
    sasl_client.reset(new sasl::TSaslClient(KERBEROS_MECHANISM, auth_id, service,
        hostname, props, &SASL_CALLBACKS[0]));
  } catch (sasl::SaslClientImplException& e) {
    LOG(ERROR) << "Failed to create a GSSAPI/SASL client: " << e.what();
    return Status(e.what());
  }

  wrapped_transport->reset(new TSaslClientTransport(sasl_client, raw_transport));
  return Status::OK;
}


// This is the one required method to implement a SASL 'auxprop' plugin. It's just a
// no-op; the work is done in SaslLdapCheckPass();
void ImpalaAuxpropLookup(void* glob_context, sasl_server_params_t* sparams,
    unsigned int flags, const char* user, unsigned ulen) {
  // NO-OP
}

// Singleton structure used to register auxprop plugin with SASL
static sasl_auxprop_plug_t impala_auxprop_plugin = {
  0, // feature flag
  0, // 'spare'
  NULL, // global plugin state
  NULL, // Free global state callback
  &ImpalaAuxpropLookup, // Auxprop lookup method
  const_cast<char*>(IMPALA_AUXPROP_PLUGIN.c_str()), // Name of plugin
  NULL // Store property callback
};

// Called by SASL during registration of the auxprop plugin.
int ImpalaAuxpropInit(const sasl_utils_t* utils, int max_version, int* out_version,
    sasl_auxprop_plug_t** plug, const char* plugname) {
  LOG(INFO) << "Initialising Impala SASL plugin: " << plugname;
  *plug = &impala_auxprop_plugin;
  *out_version = max_version;
  return SASL_OK;
}

Status LdapAuthProvider::Start() {
  sasl_callbacks_.resize(6);
  sasl_callbacks_[0].id = SASL_CB_LOG;
  sasl_callbacks_[0].proc = (int (*)())&SaslLogCallback;
  sasl_callbacks_[0].context = NULL;
  sasl_callbacks_[1].id = SASL_CB_GETOPT;
  sasl_callbacks_[1].proc = (int (*)())&SaslGetOption;
  sasl_callbacks_[1].context = (void*)1;
  sasl_callbacks_[2].id = SASL_CB_PROXY_POLICY;
  sasl_callbacks_[2].proc = (int (*)())&SaslAuthorize;
  sasl_callbacks_[2].context = NULL;
  sasl_callbacks_[3].id = SASL_CB_GETPATH;
  sasl_callbacks_[3].proc = (int (*)())&SaslGetPath;
  sasl_callbacks_[3].context = NULL;
  // This is where LDAP / PLAIN differs from other SASL mechanisms: we use the checkpass
  // callback to do authentication.
  sasl_callbacks_[4].id = SASL_CB_SERVER_USERDB_CHECKPASS;
  sasl_callbacks_[4].proc = (int (*)())&SaslLdapCheckPass;
  sasl_callbacks_[4].context = NULL;
  sasl_callbacks_[sasl_callbacks_.size() - 1].id = SASL_CB_LIST_END;

  if (!FLAGS_ldap_ca_certificate.empty()) {
    int set_rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_CACERTFILE,
        FLAGS_ldap_ca_certificate.c_str());
    if (set_rc != LDAP_SUCCESS) {
      return Status(Substitute("Could not set location of LDAP server cert: $0",
          ldap_err2string(set_rc)));
    }
  } else {
    LOG(INFO) << "No certificate file specified for LDAP, will not check certificate for"
              <<" authentication";
    int val = LDAP_OPT_X_TLS_ALLOW;
    int set_rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_REQUIRE_CERT,
        reinterpret_cast<void*>(&val));
    if (set_rc != LDAP_SUCCESS) {
      return Status(Substitute(
          "Could not disable certificate requirement for LDAP server: $0",
          ldap_err2string(set_rc)));
    }
  }

  return Status::OK;
}

Status LdapAuthProvider::GetServerTransportFactory(
    shared_ptr<TTransportFactory>* factory) {
  // TSaslServerTransport::Factory doesn't actually do anything with the properties
  // argument, so we pass in an empty map
  map<string, string> sasl_props;
  // If overriding the hard-coded LDAP configuration, use the default callbacks,
  // otherwise use those specific to LDAP.
  factory->reset(
      new TSaslServerTransport::Factory(PLAIN_MECHANISM, "", "", 0, sasl_props,
          FLAGS_ldap_manual_config ? SASL_CALLBACKS : sasl_callbacks_));
  return Status::OK;
}

Status LdapAuthProvider::WrapClientTransport(const string& hostname,
  shared_ptr<TTransport> raw_transport, const string& dummy_service,
  shared_ptr<TTransport>* wrapped_transport) {
  *wrapped_transport = raw_transport;
  return Status::OK;
}

Status NoAuthProvider::GetServerTransportFactory(shared_ptr<TTransportFactory>* factory) {
  factory->reset(new TBufferedTransportFactory());
  return Status::OK;
}

Status NoAuthProvider::WrapClientTransport(const string& hostname,
    shared_ptr<TTransport> raw_transport, const string& dummy_service,
    shared_ptr<TTransport>* wrapped_transport) {
  *wrapped_transport = raw_transport;
  return Status::OK;
}

Status AuthManager::Init() {
  if (FLAGS_krb_credential_cache.empty()) {
    FLAGS_krb_credential_cache = Substitute("/tmp/krb5_impala_$0_$1",
        google::ProgramInvocationShortName(), getpid());
  }

  // Client-side uses LDAP if enabled, else Kerberos if FLAGS_principal is set, otherwise
  // a NoAuthProvider.
  if (FLAGS_enable_ldap_auth) {
    client_auth_provider_.reset(new LdapAuthProvider());
    // We only want to register the plugin once for the whole application, so don't
    // register inside the LdapAuthProvider() instructor.
    int rc = sasl_auxprop_add_plugin(IMPALA_AUXPROP_PLUGIN.c_str(), &ImpalaAuxpropInit);
    if (rc != SASL_OK) return Status(sasl_errstring(rc, NULL, NULL));
  } else if (!FLAGS_principal.empty()) {
    client_auth_provider_.reset(new KerberosAuthProvider(FLAGS_principal,
            FLAGS_keytab_file, FLAGS_krb_credential_cache, false));
  } else {
    client_auth_provider_.reset(new NoAuthProvider());
  }

  // Server-side uses Kerberos if either FLAGS_principal or the specific
  // FLAGS_be_principal is set, otherwise a NoAuthProvider.
  if (!FLAGS_principal.empty() || !FLAGS_be_principal.empty()) {
    if (FLAGS_be_principal.empty()) FLAGS_be_principal = FLAGS_principal;

    // Should init as this principal as well, in order to allow connections to other
    // backend services.
    server_auth_provider_.reset(new KerberosAuthProvider(FLAGS_be_principal,
            FLAGS_keytab_file, FLAGS_krb_credential_cache, true));
  } else {
    server_auth_provider_.reset(new NoAuthProvider());
  }

  RETURN_IF_ERROR(server_auth_provider_->Start());
  RETURN_IF_ERROR(client_auth_provider_->Start());

  return Status::OK;
}

AuthProvider* AuthManager::GetClientFacingAuthProvider() {
  DCHECK(client_auth_provider_.get() != NULL);
  return client_auth_provider_.get();
}

AuthProvider* AuthManager::GetServerFacingAuthProvider() {
  DCHECK(server_auth_provider_.get() != NULL);
  return server_auth_provider_.get();
}

}
