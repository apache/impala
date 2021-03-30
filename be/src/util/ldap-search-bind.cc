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

#include "ldap-search-bind.h"

#include <ldap.h>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>

#include "common/logging.h"
#include "common/names.h"
#include "util/os-util.h"

DEFINE_string(ldap_user_search_basedn, "",
    "The 'distinguished name' DN that will be used to search for the authenticating "
    "user, this field is required for search bind authentication.");
DEFINE_string(ldap_group_search_basedn, "",
    "The 'distinguished name' DN that will be used to search for the authenticating "
    "group. If left empty, group checks will not be performed.");

DECLARE_string(ldap_bind_dn);
DECLARE_string(ldap_user_filter);
DECLARE_string(ldap_group_filter);

using boost::algorithm::replace_all;
using std::string;

namespace impala {

// Permitted patterns in the user/group filter
const string USER_NAME_PATTERN = "{0}";
const string USER_DN_PATTERN = "{1}";
// Default ldap filters
const string DEFAULT_USER_FILTER = "(&(objectClass=user)(sAMAccountName={0}))";
const string DEFAULT_GROUP_FILTER = "(&(objectClass=group)(member={1})";

Status LdapSearchBind::ValidateFlags() {
  RETURN_IF_ERROR(ImpalaLdap::ValidateFlags());

  if (FLAGS_ldap_user_search_basedn.empty()) {
    return Status("LDAP Search bind authentication has been configured, "
                  "but the --ldap_user_search_basedn is empty, please configure it.");
  }
  if (FLAGS_ldap_user_filter.empty()) {
    LOG(WARNING) << "LDAP Search bind authentication has been configured, but the "
                    "--ldap_user_filter is empty, the default filter will be used: "
                 << DEFAULT_USER_FILTER;
  }

  return Status::OK();
}

Status LdapSearchBind::Init(const string& user_filter, const string& group_filter) {
  RETURN_IF_ERROR(ImpalaLdap::Init(user_filter, group_filter));

  user_filter_ = (!user_filter.empty()) ? user_filter : DEFAULT_USER_FILTER;
  group_filter_ = (!group_filter.empty()) ? group_filter : DEFAULT_GROUP_FILTER;

  if (!FLAGS_ldap_group_search_basedn.empty() && group_filter_.empty()) {
    LOG(WARNING) << "LDAP Search bind authentication has been configured with group "
                    "filter, but group filter is empty, the default filter will be used: "
                 << DEFAULT_USER_FILTER;
  }
  if (FLAGS_ldap_group_search_basedn.empty() && !group_filter.empty()) {
    LOG(WARNING) << "LDAP Search bind authentication has been configured with group "
                    "filter and empty group base dn, group search will be skipped.";
  }

  return Status::OK();
}

bool LdapSearchBind::LdapCheckPass(const char* user, const char* pass, unsigned passlen) {
  // Bind with the bind user for the ldap search
  LDAP* bind_user_ld;
  VLOG(2) << "Trying LDAP bind with bind user for user search";
  bool success = Bind(
      FLAGS_ldap_bind_dn, bind_password_.c_str(), bind_password_.size(), &bind_user_ld);
  if (!success) return false;
  VLOG(2) << "LDAP bind successful";

  // Replace the USER_NAME_PATTERN in the filter with the username
  string filter = string(user_filter_);
  replace_all(filter, USER_NAME_PATTERN, user);

  // Execute the LDAP search and try to retrieve the user dn
  VLOG(1) << "Trying LDAP user search for: " << user;
  string user_dn = LdapSearchObject(
      bind_user_ld, FLAGS_ldap_user_search_basedn.c_str(), filter.c_str());
  ldap_unbind_ext(bind_user_ld, nullptr, nullptr);
  if (user_dn.empty()) return false;
  VLOG(2) << "LDAP search successful";

  // Bind with the found user and provided pass
  LDAP* user_ld;
  VLOG(2) << "Trying LDAP bind with: " << user_dn;
  success = Bind(user_dn, pass, passlen, &user_ld);
  if (success) {
    ldap_unbind_ext(user_ld, nullptr, nullptr);
    VLOG(2) << "LDAP bind successful";
  }

  return success;
}

bool LdapSearchBind::LdapCheckFilters(string username) {
  if (FLAGS_ldap_group_search_basedn.empty()) return true;

  // Bind with the bind user for the ldap search
  LDAP* ld;
  VLOG(2) << "Trying LDAP bind with bind user for group search";
  bool success =
      Bind(FLAGS_ldap_bind_dn, bind_password_.c_str(), bind_password_.size(), &ld);
  if (!success) return false;
  VLOG(2) << "LDAP bind successful";

  // Substitute the USER_NAME_PATTERN and USER_DN_PATTERN patterns for the group search.
  // USER_DN_PATTERN requires to determine the user dn and therefore an additional LDAP
  // search.
  string group_filter = group_filter_;
  replace_all(group_filter, USER_NAME_PATTERN, username);
  if (group_filter.find(USER_DN_PATTERN) != string::npos) {
    string user_filter = user_filter_;
    replace_all(user_filter, USER_NAME_PATTERN, username);
    string user_dn =
        LdapSearchObject(ld, FLAGS_ldap_user_search_basedn.c_str(), user_filter.c_str());
    if (user_dn.empty()) {
      ldap_unbind_ext(ld, nullptr, nullptr);
      return false;
    }
    replace_all(group_filter, USER_DN_PATTERN, user_dn);
  }

  // Execute LDAP search for the group
  VLOG_QUERY << "Trying LDAP group search for: " << username;
  string filter_user_dn =
      LdapSearchObject(ld, FLAGS_ldap_group_search_basedn.c_str(), group_filter.c_str());
  ldap_unbind_ext(ld, nullptr, nullptr);
  if (filter_user_dn.empty()) return false;
  VLOG(2) << "LDAP group search successful";

  return true;
}

} // namespace impala
