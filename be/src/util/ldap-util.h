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

#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"

struct ldap;
typedef struct ldap LDAP;

namespace impala {

class Status;

// Utility class for checking usernames and passwords in LDAP.
class ImpalaLdap {
 public:
  static Status ValidateFlags();

  /// 'user_filter' and 'group_filter' are optional comma separated lists specifying what
  /// users and groups are allowed to authenticate.
  Status Init(
      const std::string& user_filter, const std::string& group_filter) WARN_UNUSED_RESULT;

  /// Attempts to authenticate to LDAP using the given username and password, applying the
  /// user or group filters as approrpriate. 'passlen' is the length of the password.
  /// Returns true if authentication is successful.
  ///
  /// Note that this method uses ldap_sasl_bind_s(), which does *not* provide any security
  /// to the connection between Impala and the LDAP server. You must either set
  /// --ldap_tls or have a URI which has "ldaps://" as the scheme in order to get a secure
  /// connection. Use --ldap_ca_certificate to specify the location of the certificate
  /// used to confirm the authenticity of the LDAP server certificate.
  bool LdapCheckPass(
      const char* username, const char* password, unsigned passlen) WARN_UNUSED_RESULT;

 private:
  /// If non-empty, only users in this set can successfully authenticate.
  std::unordered_set<std::string> user_filter_;

  /// If non-empty, only users who belong to groups in this set can successfully
  /// authenticate.
  std::unordered_set<std::string> group_filter_;
  /// The base DNs to use when preforming group searches.
  std::vector<std::string> group_filter_dns_;

  /// Searches LDAP to determine if 'user_str' belong to one of the groups in
  /// 'group_filter_'. Returns true if so.
  bool CheckGroupMembership(LDAP* ld, const std::string& user_str);

  /// Returns the value part of the first attribute in the provided relative DN.
  static std::string GetShortName(const std::string& rdn);
};

} // namespace impala
