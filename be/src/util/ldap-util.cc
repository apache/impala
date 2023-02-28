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

#include "util/ldap-util.h"

#include <ldap.h>
#include <gflags/gflags.h>
#include <gutil/strings/util.h>

#include "common/logging.h"
#include "common/names.h"
#include "kudu/util/flag_tags.h"
#include "util/ldap-search-bind.h"
#include "util/ldap-simple-bind.h"
#include "util/os-util.h"

DEFINE_string(ldap_uri, "", "The URI of the LDAP server to authenticate users against");
DEFINE_bool(ldap_tls, false,
    "If true, use the secure TLS protocol to connect to the LDAP server");
DEFINE_bool(ldap_search_bind_authentication, false,
    "If set to true, LDAP search bind authentication will be used instead of the "
    "default simple bind.");

DEFINE_bool(ldap_passwords_in_clear_ok, false,
    "If set, will allow LDAP passwords to be sent in the clear (without TLS/SSL) over "
    "the network.  This option should not be used in production environments");
DEFINE_bool(ldap_allow_anonymous_binds, false,
    "(Advanced) If true, LDAP authentication with a blank password "
    "(an 'anonymous bind') is allowed by Impala.");

DEFINE_string(ldap_bind_dn, "",
    "Distinguished name of the user to bind as when doing user or group searches. Only "
    "required if user or group filters are being used and the LDAP server is not "
    "configured to allow anonymous searches.");
DEFINE_string(ldap_bind_password_cmd, "",
    "A Unix command whose output returns the password to use with --ldap_bind_dn. The "
    "output of the command will be truncated to 1024 bytes and trimmed of trailing "
    "whitespace.");
DEFINE_bool(allow_custom_ldap_filters_with_kerberos_auth, false,
    "If set, will allow custom LDAP user and group filters even if Kerberos "
    "authentication is enabled. Disabled by default.");
TAG_FLAG(ldap_bind_password_cmd, sensitive);

DECLARE_string(ldap_ca_certificate);
DECLARE_string(ldap_user_filter);
DECLARE_string(ldap_group_filter);
DECLARE_string(principal);
DECLARE_bool(skip_external_kerberos_auth);

using namespace strings;

namespace impala {

// Required prefixes for ldap URIs:
const string LDAP_URI_PREFIX = "ldap://";
const string LDAPS_URI_PREFIX = "ldaps://";

Status ImpalaLdap::CreateLdap(std::unique_ptr<ImpalaLdap>* ldap,
    const std::string& user_filter, const std::string& group_filter) {
  if (FLAGS_ldap_search_bind_authentication) {
    ldap->reset(new LdapSearchBind());
  } else {
    ldap->reset(new LdapSimpleBind());
  }
  RETURN_IF_ERROR(ldap->get()->ValidateFlags());
  RETURN_IF_ERROR(ldap->get()->Init(user_filter, group_filter));
  return Status::OK();
}

Status ImpalaLdap::ValidateFlags() {
  if (FLAGS_ldap_uri.empty()) {
    return Status("--ldap_uri must be supplied when --ldap_enable_auth is set");
  }

  if ((FLAGS_ldap_uri.find(LDAP_URI_PREFIX) != 0)
      && (FLAGS_ldap_uri.find(LDAPS_URI_PREFIX) != 0)) {
    return Status(Substitute(
        "--ldap_uri must start with either $0 or $1", LDAP_URI_PREFIX, LDAPS_URI_PREFIX));
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

  if ((!FLAGS_ldap_user_filter.empty() || !FLAGS_ldap_group_filter.empty())
      && (!FLAGS_principal.empty() && !FLAGS_skip_external_kerberos_auth
          && !FLAGS_allow_custom_ldap_filters_with_kerberos_auth)) {
    return Status("LDAP user and group filters may not be used if Kerberos auth is "
                  "turned on for external connections.");
  }

  return Status::OK();
}

Status ImpalaLdap::Init(const string& user_filter, const string& group_filter) {
  if (!FLAGS_ldap_bind_password_cmd.empty()) {
    if (!RunShellProcess(
            FLAGS_ldap_bind_password_cmd, &bind_password_, true, {"JAVA_TOOL_OPTIONS"})) {
      return Status(
          Substitute("ldap_bind_password_cmd failed with output: '$0'", bind_password_));
    }
  }
  return Status::OK();
}

bool ImpalaLdap::Bind(
    const string& user_dn, const char* pass, unsigned passlen, LDAP** ld) {
  if (passlen == 0 && !FLAGS_ldap_allow_anonymous_binds) {
    // Disable anonymous binds.
    LOG(WARNING) << "LDAP anonymous bind is disabled in Impala and the user:" << user_dn
                 << " tries to authenticate with blank password. To allow anonymous "
                 << "binds configure --ldap_allow_anonymous_binds=true";
    return false;
  }

  int rc = ldap_initialize(ld, FLAGS_ldap_uri.c_str());
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "Could not initialize connection with LDAP server (" << FLAGS_ldap_uri
                 << "). Error: " << ldap_err2string(rc);
    return false;
  }

  // Force the LDAP version to 3 to make sure TLS is supported.
  int ldap_ver = 3;
  ldap_set_option(*ld, LDAP_OPT_PROTOCOL_VERSION, &ldap_ver);

  // If -ldap_tls is turned on, and the URI is ldap://, issue a STARTTLS operation.
  // Note that we'll ignore -ldap_tls when using ldaps:// because we've already
  // got a secure connection (and the LDAP server will reject the STARTTLS).
  if (FLAGS_ldap_tls && (FLAGS_ldap_uri.find(LDAP_URI_PREFIX) == 0)) {
    int tls_rc = ldap_start_tls_s(*ld, nullptr, nullptr);
    if (tls_rc != LDAP_SUCCESS) {
      LOG(WARNING) << "Could not start TLS secure connection to LDAP server ("
                   << FLAGS_ldap_uri << "). Error: " << ldap_err2string(tls_rc);
      ldap_unbind_ext(*ld, nullptr, nullptr);
      return false;
    }
    VLOG(2) << "Started TLS connection with LDAP server: " << FLAGS_ldap_uri;
  }

  // Map the password into a credentials structure
  struct berval cred;
  cred.bv_val = const_cast<char*>(pass);
  cred.bv_len = passlen;

  rc = ldap_sasl_bind_s(
      *ld, user_dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr);
  // Free ld
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "LDAP authentication failure for " << user_dn << " : "
                 << ldap_err2string(rc);
    ldap_unbind_ext(*ld, nullptr, nullptr);
    return false;
  }

  return true;
}

vector<string> ImpalaLdap::LdapSearchObject(LDAP* ld, const char* base_dn,
    const char* filter) {
  vector<string> result_dns;
  LDAPMessage* result_msg;
  // Search through LDAP starting at a base of 'base_dn' and including the entire subtree
  // below it while applying 'filter'. This should return a list of all entries
  // encountered in the search that have the given user as a member.
  int rc = ldap_search_ext_s(ld, base_dn, LDAP_SCOPE_SUBTREE, filter, nullptr, false,
      nullptr, nullptr, nullptr, LDAP_MAXINT, &result_msg);
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "LDAP search failed with base DN=" << base_dn << " and filter="
                 << filter << " : " << ldap_err2string(rc);
    ldap_msgfree(result_msg);
    return result_dns;
  }

  // Iterate through the result objects and retrieve the DN from the result.
  for (LDAPMessage* msg = ldap_first_message(ld, result_msg); msg != nullptr;
       msg = ldap_next_message(ld, msg)) {
    int msg_type = ldap_msgtype(msg);
    switch (msg_type) {
      case LDAP_RES_SEARCH_ENTRY:
        char* entry_dn;
        if ((entry_dn = ldap_get_dn(ld, msg)) != nullptr) {
          result_dns.push_back(string(entry_dn));
          ldap_memfree(entry_dn);
        } else {
          LOG(WARNING) << "LDAP search error for " << filter << " with DN=" << base_dn
                       << ": Was not able to get DN from search result.";
        }
        break;
      case LDAP_RES_SEARCH_REFERENCE: {
        LOG(WARNING) << "LDAP search error for " << filter << " with DN=" << base_dn
                     << ": Following of referrals not supported, ignoring.";
        char** referrals;
        int parse_rc = ldap_parse_reference(ld, msg, &referrals, nullptr, 0);
        if (parse_rc != LDAP_SUCCESS) {
          LOG(WARNING) << "Was unable to parse LDAP search reference result: "
                       << ldap_err2string(parse_rc);
          break;
        }

        if (referrals != nullptr) {
          for (int i = 0; referrals[i] != nullptr; ++i) {
            LOG(WARNING) << "Got search reference: " << referrals[i];
          }
          ber_memvfree((void**)referrals);
        }
        break;
      }
      case LDAP_RES_SEARCH_RESULT:
        // Indicates the end of the messages in the result. Nothing to do.
        break;
    }
  }
  ldap_msgfree(result_msg);

  return result_dns;
}

} // namespace impala
