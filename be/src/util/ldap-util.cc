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
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/split.h>
#include <gutil/strings/util.h>

#include "common/logging.h"

#include "common/names.h"

DEFINE_string(ldap_uri, "", "The URI of the LDAP server to authenticate users against");
DEFINE_bool(ldap_tls, false, "If true, use the secure TLS protocol to connect to the LDAP"
    " server");

DEFINE_bool(ldap_passwords_in_clear_ok, false, "If set, will allow LDAP passwords "
    "to be sent in the clear (without TLS/SSL) over the network.  This option should not "
    "be used in production environments" );
DEFINE_bool(ldap_allow_anonymous_binds, false, "(Advanced) If true, LDAP authentication "
    "with a blank password (an 'anonymous bind') is allowed by Impala.");

DEFINE_string(ldap_domain, "", "If set, Impala will try to bind to LDAP with a name of "
    "the form <userid>@<ldap_domain>");
DEFINE_string(ldap_baseDN, "", "If set, Impala will try to bind to LDAP with a name of "
    "the form uid=<userid>,<ldap_baseDN>");
DEFINE_string(ldap_bind_pattern, "", "If set, Impala will try to bind to LDAP with a name"
     " of <ldap_bind_pattern>, but where the string #UID is replaced by the user ID. Use"
     " to control the bind name precisely; do not set --ldap_domain or --ldap_baseDN with"
     " this option");

DEFINE_string(ldap_group_dn_pattern, "", "Colon separated list of patterns for the "
    "'distinguished name' used to search for groups in the directory. Each pattern may "
    "contain a '%s' which will be substituted with each group name from "
    "--ldap_group_filter when doing group searches.");
DEFINE_string(ldap_group_membership_key, "member",
    "The LDAP attribute on group entries that indicates its members.");
DEFINE_string(ldap_group_class_key, "groupOfNames",
    "The LDAP objectClass each of the groups in --ldap_group_filter implements in LDAP.");

DECLARE_string(ldap_ca_certificate);

using boost::algorithm::replace_all;
using namespace strings;

namespace impala {

// Required prefixes for ldap URIs:
static const string LDAP_URI_PREFIX = "ldap://";
static const string LDAPS_URI_PREFIX = "ldaps://";

Status ImpalaLdap::ValidateFlags() {
  const string excl_msg = "--$0 and --$1 are mutually exclusive "
    "and should not be set together";

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
  return Status::OK();
}

Status ImpalaLdap::Init(const std::string& user_filter, const std::string& group_filter) {
  if (!user_filter.empty()) {
    user_filter_ = Split(user_filter, ",");
  }

  if (!group_filter.empty()) {
    if (FLAGS_ldap_group_dn_pattern.empty()) {
      return Status("In order to apply an LDAP group filter, --ldap_group_dn_pattern "
                    "must be specified.");
    }
    group_filter_ = Split(group_filter, ",");
    vector<string> group_dns = Split(FLAGS_ldap_group_dn_pattern, ":");

    // Build the list of DNs to search for groups by iterating through the
    // DN patterns and replacing the optional '%s' with each group name, if present.
    for (const string& group_dn_pattern : group_dns) {
      if (group_dn_pattern.find("%s") != std::string::npos) {
        for (const string& group : group_filter_) {
          group_filter_dns_.push_back(
              StringReplace(group_dn_pattern, "%s", group, /* replace_all */ false));
        }
      } else {
        group_filter_dns_.push_back(group_dn_pattern);
      }
    }
  }

  return Status::OK();
}

bool ImpalaLdap::LdapCheckPass(
    const char* user, const char* pass, unsigned passlen, string do_as_user) {
  if (passlen == 0 && !FLAGS_ldap_allow_anonymous_binds) {
    // Disable anonymous binds.
    return false;
  }

  LDAP* ld;
  int rc = ldap_initialize(&ld, FLAGS_ldap_uri.c_str());
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "Could not initialize connection with LDAP server (" << FLAGS_ldap_uri
                 << "). Error: " << ldap_err2string(rc);
    return false;
  }

  // Force the LDAP version to 3 to make sure TLS is supported.
  int ldap_ver = 3;
  ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &ldap_ver);

  // If -ldap_tls is turned on, and the URI is ldap://, issue a STARTTLS operation.
  // Note that we'll ignore -ldap_tls when using ldaps:// because we've already
  // got a secure connection (and the LDAP server will reject the STARTTLS).
  if (FLAGS_ldap_tls && (FLAGS_ldap_uri.find(LDAP_URI_PREFIX) == 0)) {
    int tls_rc = ldap_start_tls_s(ld, nullptr, nullptr);
    if (tls_rc != LDAP_SUCCESS) {
      LOG(WARNING) << "Could not start TLS secure connection to LDAP server ("
                   << FLAGS_ldap_uri << "). Error: " << ldap_err2string(tls_rc);
      ldap_unbind_ext(ld, nullptr, nullptr);
      return false;
    }
    VLOG(2) << "Started TLS connection with LDAP server: " << FLAGS_ldap_uri;
  }

  string user_dn = ConstructUserDN(user);

  // Map the password into a credentials structure
  struct berval cred;
  cred.bv_val = const_cast<char*>(pass);
  cred.bv_len = passlen;

  VLOG_QUERY << "Trying simple LDAP bind for: " << user_dn;

  rc = ldap_sasl_bind_s(
      ld, user_dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr);
  // Free ld
  if (rc != LDAP_SUCCESS) {
    LOG(WARNING) << "LDAP authentication failure for " << user_dn << " : "
                 << ldap_err2string(rc);
    ldap_unbind_ext(ld, nullptr, nullptr);
    return false;
  }

  VLOG_QUERY << "LDAP bind successful";

  string filter_user = do_as_user == "" ? user : do_as_user;
  if (!user_filter_.empty() && user_filter_.count(filter_user) != 1) {
    LOG(WARNING) << "LDAP authentication failure for " << user_dn << ". Bind was "
                 << "successful but user is not in the authorized user list.";
    ldap_unbind_ext(ld, nullptr, nullptr);
    return false;
  }

  string filter_user_dn = do_as_user == nullptr ? user_dn : ConstructUserDN(do_as_user);
  if (!group_filter_.empty()) {
    if (!CheckGroupMembership(ld, filter_user_dn)) {
      LOG(WARNING) << "LDAP authentication failure for " << user_dn << ". Bind was "
                   << "successful but user is not in any of the required groups.";
      ldap_unbind_ext(ld, nullptr, nullptr);
      return false;
    }
  }
  ldap_unbind_ext(ld, nullptr, nullptr);

  return true;
}

bool ImpalaLdap::CheckGroupMembership(LDAP* ld, const string& user_dn) {
  // Construct a filter that will search for LDAP entries that represent groups
  // (determined by having the group class key) and that contain the user trying to
  // authenticate (determined by having a membership entry matching the user).
  string filter = Substitute("(&(objectClass=$0)($1=$2))", FLAGS_ldap_group_class_key,
      FLAGS_ldap_group_membership_key, user_dn);
  VLOG(2) << "Searching for groups with filter: " << filter;

  for (const string& group_dn : group_filter_dns_) {
    LDAPMessage* result;
    // Search through LDAP starting at a base of 'group_dn' and including the entire
    // subtree below it while applying 'filter'. This should return a list of all group
    // entries encountered in the search that have the given user as a member.
    int rc = ldap_search_ext_s(ld, group_dn.c_str(), LDAP_SCOPE_SUBTREE, filter.c_str(),
        nullptr, false, nullptr, nullptr, nullptr, LDAP_MAXINT, &result);
    if (rc != LDAP_SUCCESS) {
      LOG(WARNING) << "LDAP search failed for " << filter << " with DN=" << group_dn
                   << ": " << ldap_err2string(rc);
      ldap_msgfree(result);
      continue;
    }

    for (LDAPMessage* msg = ldap_first_message(ld, result); msg != nullptr;
         msg = ldap_next_message(ld, msg)) {
      int msg_type = ldap_msgtype(msg);
      switch (msg_type) {
        case LDAP_RES_SEARCH_ENTRY:
          char* dn;
          if ((dn = ldap_get_dn(ld, msg)) != nullptr) {
            string short_name = GetShortName(dn);
            if (group_filter_.count(short_name) == 1) {
              ldap_memfree(dn);
              ldap_msgfree(result);
              return true;
            }
            ldap_memfree(dn);
          } else {
            LOG(WARNING) << "LDAP search error for " << filter << " with DN=" << group_dn
                         << ": Was not able to get DN from search result.";
          }
          break;
        case LDAP_RES_SEARCH_REFERENCE: {
          LOG(WARNING) << "LDAP search error for " << filter << " with DN=" << group_dn
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
    ldap_msgfree(result);
  }

  return false;
}

string ImpalaLdap::GetShortName(const string& rdn) {
  vector<string> attributes = Split(rdn, delimiter::Limit(",", 1));
  vector<string> value = Split(attributes[0], delimiter::Limit("=", 1));
  return value[1];
}

string ImpalaLdap::ConstructUserDN(const std::string& user) {
  string user_dn = user;
  if (!FLAGS_ldap_domain.empty()) {
    // Append @domain if there isn't already an @ in the user string.
    if (user_dn.find("@") == string::npos) {
      user_dn = Substitute("$0@$1", user_dn, FLAGS_ldap_domain);
    }
  } else if (!FLAGS_ldap_baseDN.empty()) {
    user_dn = Substitute("uid=$0,$1", user_dn, FLAGS_ldap_baseDN);
  } else if (!FLAGS_ldap_bind_pattern.empty()) {
    user_dn = FLAGS_ldap_bind_pattern;
    replace_all(user_dn, "#UID", user);
  }
  return user_dn;
}

} // namespace impala
