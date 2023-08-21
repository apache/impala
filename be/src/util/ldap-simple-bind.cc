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

#include "ldap-simple-bind.h"

#include <ldap.h>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/split.h>
#include <gutil/strings/util.h>

#include "common/logging.h"
#include "common/names.h"
#include "util/os-util.h"

DEFINE_string(ldap_domain, "",
    "If set, Impala will try to bind to LDAP with a name of the form "
    "<userid>@<ldap_domain>");
DEFINE_string(ldap_baseDN, "",
    "If set, Impala will try to bind to LDAP with a name of the form "
    "uid=<userid>,<ldap_baseDN>");
DEFINE_string(ldap_bind_pattern, "",
    "If set, Impala will try to bind to LDAP with a name of <ldap_bind_pattern>, but "
    "where the string #UID is replaced by the user ID. Use to control the bind name "
    "precisely; do not set --ldap_domain or --ldap_baseDN with this option");

DEFINE_string(ldap_group_dn_pattern, "",
    "Colon separated list of patterns for the 'distinguished name' used to search for "
    "groups in the directory. Each pattern may contain a '%s' which will be substituted "
    "with each group name from --ldap_group_filter when doing group searches.");
DEFINE_string(ldap_group_membership_key, "member",
    "The LDAP attribute on group entries that indicates its members.");
DEFINE_string(ldap_group_class_key, "groupOfNames",
    "The LDAP objectClass each "
    "of the groups in --ldap_group_filter implements in LDAP.");

DECLARE_string(ldap_bind_dn);

using boost::algorithm::replace_all;
using namespace strings;

namespace impala {

Status LdapSimpleBind::ValidateFlags() {
  RETURN_IF_ERROR(ImpalaLdap::ValidateFlags());

  const string excl_msg = "--$0 and --$1 are mutually exclusive and should not be set "
                          "together";

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

  return Status::OK();
}

Status LdapSimpleBind::Init(const string& user_filter, const string& group_filter) {
  RETURN_IF_ERROR(ImpalaLdap::Init(user_filter, group_filter));

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
      if (group_dn_pattern.find("%s") != string::npos) {
        for (const string& group : group_filter_) {
          group_base_dns_.push_back(
              StringReplace(group_dn_pattern, "%s", group, /* replace_all */ false));
        }
      } else {
        group_base_dns_.push_back(group_dn_pattern);
      }
    }
  }

  return Status::OK();
}

bool LdapSimpleBind::LdapCheckPass(const char* user, const char* pass, unsigned passlen) {
  string user_dn = ConstructUserDN(user);
  LDAP* ld;
  VLOG(1) << "Trying simple LDAP bind for: " << user_dn;
  bool success = Bind(user_dn, pass, passlen, &ld);
  if (success) {
    ldap_unbind_ext(ld, nullptr, nullptr);
    VLOG(2) << "LDAP bind successful";
  }
  return success;
}

bool LdapSimpleBind::LdapCheckFilters(string username) {
  if (user_filter_.empty() && group_filter_.empty()) return true;

  VLOG(2) << "Checking LDAP filters for " << username;
  if (username.empty()) {
    LOG(WARNING) << "Failed to check LDAP filters: username empty.";
    return false;
  }

  LDAP* ld;
  bool success =
      Bind(FLAGS_ldap_bind_dn, bind_password_.c_str(), bind_password_.size(), &ld);
  if (!success) return false;

  if (!user_filter_.empty() && user_filter_.count(username) != 1) {
    LOG(WARNING) << "LDAP authentication failure for " << username << ". Bind was "
                 << "successful but user is not in the authorized user list.";
    ldap_unbind_ext(ld, nullptr, nullptr);
    return false;
  }

  if (!group_filter_.empty()) {
    string filter_user_dn = ConstructUserDN(username);
    if (!CheckGroupMembership(ld, filter_user_dn)) {
      LOG(WARNING) << "LDAP authentication failure for " << username << ". Bind was "
                   << "successful but user is not in any of the required groups.";
      ldap_unbind_ext(ld, nullptr, nullptr);
      return false;
    }
  }
  ldap_unbind_ext(ld, nullptr, nullptr);
  VLOG(2) << "LDAP filter check for " << username << " was successful.";
  return true;
}

bool LdapSimpleBind::CheckGroupMembership(LDAP* ld, const string& user_dn) {
  // Construct a filter that will search for LDAP entries that represent groups
  // (determined by having the group class key) and that contain the user trying to
  // authenticate (determined by having a membership entry matching the user).
  string filter = Substitute("(&(objectClass=$0)($1=$2))", FLAGS_ldap_group_class_key,
      FLAGS_ldap_group_membership_key, user_dn);

  VLOG(2) << "Searching for groups with filter: " << filter;
  for (const string& group_dn : group_base_dns_) {
    vector<string> dns = LdapSearchObject(ld, group_dn.c_str(), filter.c_str());
    // Retrieve the value part of the first attribute in the provided relative DN.
    for(const string& dn : dns) {
      vector<string> attributes = Split(dn, delimiter::Limit(",", 1));
      vector<string> short_name = Split(attributes[0], delimiter::Limit("=", 1));
      if (short_name.size() > 1 && group_filter_.count(short_name[1]) == 1) {
        return true;
      }
    }
  }

  return false;
}

string LdapSimpleBind::ConstructUserDN(const string& user) {
  string user_dn = user;
  if (!FLAGS_ldap_domain.empty()) {
    // Append @domain if there isn't already an @ in the user string.
    if (user_dn.find('@') == string::npos) {
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
