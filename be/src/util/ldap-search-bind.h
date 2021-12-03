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

// Utility class for checking user names and passwords in LDAP with search bind mechanism.
class LdapSearchBind : public ImpalaLdap {
 public:
  /// Validates the class specific GFlag configurations.
  Status ValidateFlags() override;

  /// 'user_filter' and 'group_filter' are optional comma separated lists specifying what
  /// users and groups are allowed to authenticate.
  Status Init(const std::string& user_filter,
      const std::string& group_filter) override WARN_UNUSED_RESULT;

  /// Attempts to authenticate to LDAP using the given username and password, applying the
  /// user or group filters as appropriate. 'passlen' is the length of the password.
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
  /// LDAP filter that will be applied when searching for the user that is authenticating.
  /// The string can contain '{0}' which will be replaced with the username.
  std::string user_filter_;

  /// LDAP filter that will be applied when searching for the group of the user.
  /// The string can contain '{0}' which will be replaced with the username or '{1}'
  /// which will be replace with the user dn. '{1}' requires an extra ldap search for
  /// the user dn. If empty, the group check will be skipped.
  std::string group_filter_;

  /// The LDAP filter specification assigns special meaning to the following characters:
  /// '*', '(', ')', '\', 'NUL'. These 5 characters should be escaped with the backslash
  /// escape character, followed by the two character ASCII hexadecimal representation of
  /// the character.
  /// This function should only be called on internal properties, currently these are
  /// the results of '{0}' and '{1}' resolution. Impala should be configured with properly
  /// escaped filters apart from '{0}' and '{1}'.
  std::string EscapeFilterProperty(string property);
};

} // namespace impala
