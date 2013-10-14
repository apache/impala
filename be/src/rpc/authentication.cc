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
#include <string>
#include <vector>

#include <transport/TSasl.h>
#include <transport/TSaslServerTransport.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "rpc/auth-provider.h"
#include "transport/TSaslClientTransport.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/network-util.h"
#include "util/thread.h"

using namespace std;
using namespace boost;
using namespace boost::random;

DECLARE_string(keytab_file);
DECLARE_string(principal);

DEFINE_int32(kerberos_reinit_interval, 60,
    "Interval, in minutes, between kerberos ticket renewals. Each renewal will request "
    "a ticket with a lifetime that is at least 2x the renewal interval.");
DEFINE_string(sasl_path, "/usr/lib/sasl2:/usr/lib64/sasl2:/usr/local/lib/sasl2:"
    "/usr/lib/x86_64-linux-gnu/sasl2", "Colon separated list of paths to look for SASL "
    "security library plugins.");
DEFINE_bool(enable_ldap_auth, false,
    "If true, use LDAP authentication for client connections");

namespace impala {

// Array of callbacks for the Sasl library.
static vector<sasl_callback_t> SASL_CALLBACKS;

// Pattern for hostname substitution.
static const string HOSTNAME_PATTERN = "_HOST";

// Constants for the two Sasl  mechanisms we support
static const std::string KERBEROS_MECHANISM = "GSSAPI";
static const std::string PLAIN_MECHANISM = "PLAIN";

mutex KerberosAuthProvider::kinit_lock_;
scoped_ptr<Thread> KerberosAuthProvider::kinit_thread_;

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

// Get Sasl option.
// context: not used
// plugin_name: name of plugin for which an option is being requested.
// option: option requested
// result: value for option
// len: length of the result
// Return SASL_FAIL if the option is not handled, this does not fail the handshake.
static int SaslGetOption(void* context, const char* plugin_name, const char* option,
    const char** result, unsigned* len) {
  // Handle Sasl Library options
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
    if (strcmp("keytab", option) == 0) {
        *result = FLAGS_keytab_file.c_str();
        if (len != NULL) *len = strlen(*result);
        return SASL_OK;
    }
    VLOG(3) << "SaslGetOption: Unknown option: " << option;
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

// Periodically call kinit to get a ticket granting ticket from the kerberos server.
// This is kept in the kerberos cache associated with this process.
void KerberosAuthProvider::RunKinit() {
  // Minumum lifetime to request for each ticket renewal.
  static const int MIN_TICKET_LIFETIME_IN_MINS = 1440;
  stringstream sysstream;

  // Set the ticket lifetime to an arbitrarily large value or 2x the renewal interval,
  // whichever is larger. The KDC will automatically fall back to using the maximum
  // allowed allowed value if a longer lifetime is requested, so it is okay to be greedy
  // here.
  int ticket_lifetime =
      max(MIN_TICKET_LIFETIME_IN_MINS, FLAGS_kerberos_reinit_interval * 2);

  // Pass the path to the key file and the principal. Make the ticket renewable.
  // Calling kinit -R ensures the ticket makes it to the cache.
  sysstream << "kinit -r " << ticket_lifetime
            << "m -k -t " << FLAGS_keytab_file << " " << FLAGS_principal
            << " 2>&1 && kinit -R 2>&1";

  bool started = false;
  int failures = 0;
  string kreturn;

  while (true) {
    LOG(INFO) << "Registering " << FLAGS_principal << " key_tab file "
              << FLAGS_keytab_file;
    bool succeeded = false;
    FILE* fp = popen(sysstream.str().c_str(), "r");
    if (fp == NULL) {
      kreturn = "Failed to execute kinit";
    } else {
      // Read the first 1024 bytes of any output so we have some idea of what
      // happened on failure.
      char buf[1024];
      size_t len = fread(buf, 1, 1024, fp);
      kreturn.assign(buf, len);
      // pclose() returns -1 on error. non-zero return codes are used for
      // other information. We only care about errors, so ignore those.
      if (pclose(fp) != -1) succeeded = true;
    }

    if (!succeeded) {
      string error_msg = GetStrErrMsg();
      if (!started) {
        LOG(ERROR) << "Exiting: failed to register with kerberos: " << error_msg
                   << " '" << kreturn << "'";
        exit(1);
      }
      // We couldn't renew the ticket so just report the error. Existing connections
      // are ok and we'll try to renew the ticket later.
      ++failures;
      LOG(ERROR) << "Failed to extend kerberos ticket: '" << kreturn
                 << "' " << error_msg << ". Failure count: " << failures;
    } else {
      VLOG_CONNECTION << "kinit returned: '" << kreturn << "'";
    }
    started = true;
    // Sleep for the renewal interval, minus a random time between 0-5 minutes to help
    // avoid a storm at the KDC. Additionally, never sleep less than a minute to
    // reduce KDC stress due to frequent renewals.
    mt19937 generator;
    uniform_int<> dist(0, 300);
    sleep(max((60 * FLAGS_kerberos_reinit_interval) - dist(generator), 60));
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
  SASL_CALLBACKS[4].id = SASL_CB_LIST_END;

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
    const string& keytab_path) : principal_(principal), keytab_path_(keytab_path) {

}

Status KerberosAuthProvider::Start() {
  // The "keytab" callback is never called.  Set the file name in the environment.
  if (setenv("KRB5_KTNAME", keytab_path_.c_str(), 1)) {
    string error_msg = GetStrErrMsg();
    stringstream ss;
    ss << "Kerberos could not set KRB5_KTNAME: " << error_msg;
    LOG(ERROR) << ss;
    return Status(ss.str());
  }

  // Replace the string _HOST with our hostname.
  size_t off = FLAGS_principal.find(HOSTNAME_PATTERN);
  if (off != string::npos) {
    string hostname;
    Status status = GetHostname(&hostname);
    if (!status.ok()) {
      status.AddErrorMsg("InitKerberos call to GetHostname failed.");
      return status;
    }
    FLAGS_principal.replace(off, HOSTNAME_PATTERN.size(), hostname);
  }

  off = FLAGS_principal.find("/");
  if (off == string::npos) {
    stringstream ss;
    ss << "--principal must contain '/': " << FLAGS_principal;
    LOG(ERROR) << ss;
    return Status(ss.str());
  }
  service_name_ = FLAGS_principal.substr(0, off);

  // Only start Kinit thread  if we're the first initialisation of this auth provider
  lock_guard<mutex> l(kinit_lock_);
  if (kinit_thread_.get() == NULL) {
    kinit_thread_.reset(
        new Thread("authentication", "kinit", &KerberosAuthProvider::RunKinit, this));
  }

  return Status::OK;
}

Status KerberosAuthProvider::GetServerTransportFactory(
    shared_ptr<TTransportFactory>* factory) {
  // The string should be service/hostname@realm
  vector<string> names;
  split(names, principal_, is_any_of("/@"));

  if (names.size() != 3) {
    stringstream ss;
    ss << "Kerberos principal should of the form: <service>/<hostname>@<realm> - got: "
       << principal_;
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }

  try {
    // TSaslServerTransport::Factory doesn't actually do anything with the properties
    // argument, so we pass in an empty map
    map<string, string> sasl_props;
    factory->reset(new TSaslServerTransport::Factory(
        KERBEROS_MECHANISM, names[0], names[1], 0, sasl_props, SASL_CALLBACKS));
  } catch (TTransportException& e) {
    LOG(ERROR) << "Failed to create a Kerberos transport factory: " << e.what();
    return Status(e.what());
  }

  return Status::OK;
}

Status KerberosAuthProvider::WrapClientTransport(const string& hostname,
    shared_ptr<TTransport> raw_transport, shared_ptr<TTransport>* wrapped_transport) {
  shared_ptr<sasl::TSasl> sasl_client;
  map<string, string> props;
  // We do not set this.
  string auth_id;

  try {
    sasl_client.reset(new sasl::TSaslClient(KERBEROS_MECHANISM, auth_id, service_name_,
        hostname, props, &SASL_CALLBACKS[0]));
  } catch (sasl::SaslClientImplException& e) {
    LOG(ERROR) << "Failed to create a GSSAPI/SASL client: " << e.what();
    return Status(e.what());
  }

  wrapped_transport->reset(new TSaslClientTransport(sasl_client, raw_transport));
  return Status::OK;
}

Status LdapAuthProvider::GetServerTransportFactory(
    shared_ptr<TTransportFactory>* factory) {
  try {
    // TSaslServerTransport::Factory doesn't actually do anything with the properties
    // argument, so we pass in an empty map
    map<string, string> sasl_props;
    factory->reset(new TSaslServerTransport::Factory(PLAIN_MECHANISM, "", "", 0,
        sasl_props, SASL_CALLBACKS));
  } catch (sasl::SaslClientImplException& e) {
    LOG(ERROR) << "Kerberos client create failed: " << e.what();
    return Status(e.what());
  }
  return Status::OK;
}

Status LdapAuthProvider::WrapClientTransport(const string& hostname,
  shared_ptr<TTransport> raw_transport, shared_ptr<TTransport>* wrapped_transport) {
  *wrapped_transport = raw_transport;
  return Status::OK;
}

Status NoAuthProvider::GetServerTransportFactory(shared_ptr<TTransportFactory>* factory) {
  factory->reset(new TBufferedTransportFactory());
  return Status::OK;
}

Status NoAuthProvider::WrapClientTransport(const string& hostname,
    shared_ptr<TTransport> raw_transport, shared_ptr<TTransport>* wrapped_transport) {
  *wrapped_transport = raw_transport;
  return Status::OK;
}

Status AuthManager::Init() {
  // If principal is set, client-side uses Kerberos. The same is true server-side *unless*
  // --enable_ldap_auth is true. If neither are set, we use a NoAuthProvider for both.
  if (FLAGS_enable_ldap_auth) {
    client_auth_provider_.reset(new LdapAuthProvider());
    RETURN_IF_ERROR(client_auth_provider_->Start());
  }

  if (!FLAGS_principal.empty()) {
    server_auth_provider_.reset(
        new KerberosAuthProvider(FLAGS_principal, FLAGS_keytab_file));
    RETURN_IF_ERROR(server_auth_provider_->Start());
  } else {
    server_auth_provider_.reset(new NoAuthProvider());
    RETURN_IF_ERROR(server_auth_provider_->Start());
  }

  return Status::OK;
}

AuthProvider* AuthManager::GetClientFacingAuthProvider() {
  if (client_auth_provider_.get() != NULL) return client_auth_provider_.get();
  return GetServerFacingAuthProvider();
}

AuthProvider* AuthManager::GetServerFacingAuthProvider() {
  DCHECK(server_auth_provider_.get() != NULL);
  return server_auth_provider_.get();
}

}
