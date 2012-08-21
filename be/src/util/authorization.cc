// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <stdio.h>
#include <unistd.h>
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

#include "authorization.h"

using namespace std;
using namespace boost;
using namespace boost::random;

DECLARE_string(keytab_file);
DECLARE_string(principal);
DEFINE_int32(kerberos_ticket_life, 0, \
  "Number of minutes to request for a ticket lifetime." \
  "0 implies twice the reinit interval");
DEFINE_int32(kerberos_reinit_interval, 60, \
    "Number of minutes between reestablishing our ticket with the kerberos server");

namespace impala {

// Array of callbacks for the sasl library.
static vector<sasl_callback_t> callbacks;

// Pattern for hostname substitution.
static string HOSTNAME_PATTERN = "_HOST";

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
    LOG(ERROR) << "Kerberos: " << message;
    break;

  case SASL_LOG_WARN:
    LOG(WARNING) << "Kerberos: " << message;
    break;

  case SASL_LOG_NOTE:
    LOG(INFO) << "Kerberos: " << message;
    break;

  case SASL_LOG_DEBUG:
    VLOG(1) << "Kerberos: " << message;
    break;

  case SASL_LOG_TRACE:
  case SASL_LOG_PASS:
    VLOG(3) << "Kerberos: " << message;
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
  VLOG(3) << "SaslGetOption: Unknown plugin: " << plugin_name;
  return SASL_FAIL;

}

// Sasl Authorize callback.
// Can be used to restrict access.  Currently used for diagnostics.
// TODO: This does not actually get called.
// requsted_user, rlen: The user requesting access and string length.
// auth_identity, alen: The identity (principal) and length.
// default_realm, urlen: Realm of the user and length.
// propctx: properties requested.
static int SaslAuthorize(sasl_conn_t* conn, void* context,
    const char* requested_user, unsigned rlen, const char* auth_identity, unsigned alen,
    const char* def_realm, unsigned urlen, struct propctx* propctx) {

  string user(requested_user, rlen);
  string auth(auth_identity, alen);
  string realm(def_realm, urlen);
  VLOG_CONNECTION << "Kerberos User: " << user << " for: " << auth << " from " << realm;

  return SASL_OK;
}

// Periodically call kinit to get a ticket granting ticket from the kerberos server.
// This is kept in the kerberos cache associated with this process.
static void RunKinit() {
  stringstream sysstream;

  if (FLAGS_kerberos_ticket_life == 0) {
    FLAGS_kerberos_ticket_life = 2 * FLAGS_kerberos_reinit_interval;
  }
  // Pass the path to the key file and the principal. Make the ticket renewable.
  // Calling kinit -R ensures the ticket makes it to the cache.
  sysstream << "kinit -r " << FLAGS_kerberos_ticket_life
            << "m -k -t " << FLAGS_keytab_file << " " << FLAGS_principal
            << " 2>&1 && kinit -R 2>&1";

  bool started = false;
  int failures = 0;
  while (true) {
    LOG(INFO) << "Registering "
              << FLAGS_principal << " key_tab file " << FLAGS_keytab_file;
    FILE* fp = popen(sysstream.str().c_str(), "r");
    if (fp == NULL) {
      LOG(ERROR) << "Exiting: failed to execute kinit: " << strerror(errno);
      exit(1);
    }
    // Read the first 1024 bytes of any output so we have some idea of what
    // happened on failure.
    char buf[1024];
    size_t len = fread(buf, 1, 1024, fp);
    string kreturn(buf, len);
    int ret = pclose(fp);
    if (ret != 0) {
      if (!started) {
        LOG(ERROR) << "Exiting: failed to register with kerberos: '" << kreturn << "'";
        exit(1);
      }
      // Just report the problem, existing report the error.  Existing connections
      // are ok and we can talk to HDFS until our ticket expires.
      ++failures;
      LOG(ERROR) << "Failed to extend kerberos ticket: '" << kreturn
                 << "' errno " << errno << ". Failure count: " << failures;
    } else {
      VLOG_CONNECTION << "kinit returned: '" << kreturn << "'";
    }
    started = true;
    // Sleep for some number of minutes.  Avoid a storm at the KDC, randomize over
    // a 5 minute interval.
    mt19937 generator;
    uniform_int<> dist(0, 300);
    sleep((60 * FLAGS_kerberos_reinit_interval) + dist(generator));
  }
}

Status InitKerberos(const string& appname) {
  callbacks.resize(4);
  callbacks[0].id = SASL_CB_LOG;
  callbacks[0].proc = (int (*)())&SaslLogCallback;
  callbacks[0].context = NULL;
  callbacks[1].id = SASL_CB_GETOPT;
  callbacks[1].proc = (int (*)())&SaslGetOption;
  callbacks[1].context = NULL;
  callbacks[2].id = SASL_CB_PROXY_POLICY;
  callbacks[2].proc = (int (*)())&SaslAuthorize;
  callbacks[2].context = NULL;
  callbacks[2].id = SASL_CB_LIST_END;

  // Replace the string _HOST with our hostname.
  size_t off = FLAGS_principal.find(HOSTNAME_PATTERN);
  if (off != string::npos) {
    string hostname = GetHostname();
    if (hostname.empty()) {
      stringstream ss;
      ss << "InitKerberos call to gethostname failed: errno " << errno;
      LOG(ERROR) << ss;
      return Status(ss.str());
    }
    FLAGS_principal.replace(off, HOSTNAME_PATTERN.size(), hostname);
  }

  try {
    sasl::TSaslServer::SaslInit(&callbacks[0], appname);
  } catch (sasl::SaslServerImplException&  e) {
    LOG(ERROR) << "Could not initialize Sasl library: " << e.what();
    return Status(e.what());
  }

  // Run kinit every hour or as configured till we exit.
  thread krun(RunKinit);
  return Status::OK;
}

Status GetKerberosTransportFactory(const string& principal,
   const string& key_tab_file, shared_ptr<TTransportFactory>* factory) {

  // The "keytab" callback is never called.  Set the file name in the environment.
  if (setenv("KRB5_KTNAME", key_tab_file.c_str(), 1)) {
    stringstream ss;
    ss << "Kerberos could not set KRB5_KTNAME: errno " << errno;
    LOG(ERROR) << ss;
    return Status(ss.str());
  }

  // The string should be service/hostname@realm
  vector<string> names;
  split(names, principal, is_any_of("/@"));

  if (names.size() != 3) {
    stringstream ss;
    ss << "Kerberos principal should of the form: <service>/<hostname>@<realm> - got: "
        << principal;
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }

  // TODO: What properties do we support? In meantime we pass an empty map.
  map<string, string> props;

  try {
    factory->reset(new TSaslServerTransport::Factory(
        KERBEROS_MECHANISM, names[0], names[1], 0, props, callbacks));
  } catch (TTransportException& e) {
    LOG(ERROR) << "Kerberos transport factory failed: " << e.what();
    return Status(e.what());
  }

  return Status::OK;
}

Status GetTSaslClient(const string& service,
                      const string& hostname, shared_ptr<sasl::TSasl>* saslClient) {
  map<string, string> props;
  // We do not set this.
  string auth_id;

  try {
    saslClient->reset(new sasl::TSaslClient(
        KERBEROS_MECHANISM, auth_id, service, hostname, props, &callbacks[0]));
  } catch (sasl::SaslClientImplException& e) {
    LOG(ERROR) << "Kerberos client create failed: " << e.what();
    return Status(e.what());
  }

  return Status::OK;
}

string GetHostname() {
  char name[HOST_NAME_MAX];
  string ret_name;
  int ret = gethostname(name, HOST_NAME_MAX);
  if (ret == 0) {
    ret_name = string(name);
  } else {
    LOG(WARNING) << "Could not get hostname: errno: " << errno;
  }
  return ret_name;
}
}
