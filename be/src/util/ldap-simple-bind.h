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

#pragma once

#include "util/ldap-util.h"

namespace impala {

// Utility class for checking usernames and passwords in LDAP with simple bind mechanism.
class LdapSimpleBind : public ImpalaLdap {
 public:
  /// Validates the class specific GFlag configurations.
  Status ValidateFlags() override;

  /// 'user_filter' and 'group_filter' are optional comma separated lists specifying what
  /// users and groups are allowed to authenticate.
  Status Init(const std::string& user_filter,
      const std::string& group_filter) override WARN_UNUSED_RESULT;

  /// Attempts to authenticate to LDAP using the given username and password, applying the
  /// user filters as appropriate. 'passlen' is the length of the password.
  /// Returns true if authentication is successful.
  ///
  /// Note that this method uses ldap_sasl_bind_s(), which does *not* provide any security
  /// to the connection between Impala and the LDAP server. You must either set
  /// --ldap_tls or have a URI which has "ldaps://" as the scheme in order to get a secure
  /// connection. Use --ldap_ca_certificate to specify the location of the certificate
  /// used to confirm the authenticity of the LDAP server certificate.
  bool LdapCheckPass(const char* username, const char* password,
      unsigned passlen) override WARN_UNUSED_RESULT;

  /// Returns true if 'username' passes the LDAP user and group filters, if configured.
  bool LdapCheckFilters(std::string username) override WARN_UNUSED_RESULT;

 private:
  /// If non-empty, only users in this set can successfully authenticate.
  std::unordered_set<std::string> user_filter_;

  /// If non-empty, only users who belong to groups in this set can successfully
  /// authenticate.
  std::unordered_set<std::string> group_filter_;

  /// The base DNs to use when preforming group searches.
  std::vector<std::string> group_base_dns_;

  /// Searches LDAP to determine if 'user_str' belong to one of the groups in
  /// 'group_filter_'. Returns true if so.
  bool CheckGroupMembership(LDAP* ld, const std::string& user_str);

  /// Maps the user string into an acceptable LDAP "DN" (distinguished name) based on the
  /// values of the LDAP config flags.
  std::string ConstructUserDN(const std::string& user);
};

} // namespace impala
