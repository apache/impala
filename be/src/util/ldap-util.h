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

// Abstract base class for utility classes to authenticate users through LDAP.
class ImpalaLdap {
 public:
  /// Factory method that returns an initialized LDAP object based on the GFlag
  /// configurations and the parameters.
  static Status CreateLdap(std::unique_ptr<ImpalaLdap>* ldap,
      const std::string& user_filter, const std::string& group_filter);

  /// Attempts to authenticate to LDAP using the given username and password, applying the
  /// user filters as appropriate. 'passlen' is the length of the password.
  /// Returns true if authentication is successful.
  virtual bool LdapCheckPass(const char* username, const char* password,
      unsigned passlen) WARN_UNUSED_RESULT = 0;

  /// Returns true if 'username' passes the LDAP group filters, if configured.
  virtual bool LdapCheckFilters(std::string username) WARN_UNUSED_RESULT = 0;

  virtual ~ImpalaLdap() = default;

 protected:
  /// The output of --ldap_bind_password_cmd, if specified. It stores the bind user's
  /// password.
  std::string bind_password_;

  /// Validates the class specific GFlag configurations.
  virtual Status ValidateFlags();

  /// Initializes the object specific configurations, currently used to initialize
  /// 'user_filter' and 'group_filter'.
  virtual Status Init(const std::string& user_filter,
      const std::string& group_filter) WARN_UNUSED_RESULT = 0;

  /// Attempts a bind with 'user' and 'pass'. Returns true if successful and the handle is
  /// returned in 'ldap', in which case the caller must call 'ldap_unbind_ext' on 'ldap'.
  bool Bind(const std::string& user_dn, const char* pass, unsigned passlen,
      LDAP** ldap) WARN_UNUSED_RESULT;

  /// This method binds with the configured bind dn and bind password and attempts an
  /// ldap search for objects on the 'base_dn' with the provided 'filter'.
  /// It returns the DN of the object when succeeds or empty string if fails. Only
  /// allows one matching entry.
  std::vector<std::string> LdapSearchObject(LDAP* ld, const char* base_dn,
      const char* filter);
};

} // namespace impala
