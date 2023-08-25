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
#include <gflags/gflags_declare.h>

#include "common/status.h"
#include "service/impala-server.h"

DECLARE_bool(skip_external_kerberos_auth);
DECLARE_bool(skip_internal_kerberos_auth);
DECLARE_string(principal);

namespace impala {

class TSessionState;

/// Returns a reference to the "effective user" from the specified session. Queries
/// are run and authorized on behalf of the effective user. When a delegated_user is
/// specified (is not empty), the effective user is the delegated_user. This is because
/// the connected_user is acting as a "proxy user" for the delegated_user. When
/// delegated_user is empty, the effective user is the connected user.
const std::string& GetEffectiveUser(const TSessionState& session);

/// Same behavior as the function above with different input parameter type.
const std::string& GetEffectiveUser(const ImpalaServer::SessionState& session);

/// Checks if 'user' can access the runtime profile or execution summary of a
/// statement by comparing 'user' with the user that run the statement, 'effective_user',
/// and checking if 'effective_user' is authorized to access the profile, as indicated by
/// 'has_access'. An error Status is returned if 'user' is not authorized to
/// access the runtime profile or execution summary.
Status CheckProfileAccess(const std::string& user, const std::string& effective_user,
    bool has_access);

/// Returns the internal kerberos principal. The internal kerberos principal is the
/// principal that is used for backend connections only.
/// 'out_principal' will contain the principal string. Must only be called if Kerberos
/// is enabled.
Status GetInternalKerberosPrincipal(std::string* out_principal);

/// Returns the external kerberos principal. The external kerberos principal is the
/// principal that is used for client connections only.
/// 'out_principal' will contain the principal string. Must only be called if Kerberos
/// is enabled.
Status GetExternalKerberosPrincipal(std::string* out_principal);

/// Splits the kerberos principal 'principal' which should be of the format:
/// "<service>/<hostname>@<realm>", and fills in the respective out parameters.
/// If 'principal' is not of the above format, an error status is returned.
Status ParseKerberosPrincipal(const std::string& principal, std::string* service_name,
    std::string* hostname, std::string* realm);

// Takes a Kerberos principal (either user/hostname@realm or user@realm)
// and returns the username part.
string GetShortUsernameFromKerberosPrincipal(const string& principal);

/// Returns true if kerberos is enabled.
inline bool IsKerberosEnabled() {
  return !FLAGS_principal.empty();
}

/// Returns true if kerberos is enabled for incoming connections on internal services.
inline bool IsInternalKerberosEnabled() {
  return IsKerberosEnabled() && !FLAGS_skip_internal_kerberos_auth;
}

/// Returns true if kerberos is enabled for incoming connections on external services.
inline bool IsExternalKerberosEnabled() {
  return IsKerberosEnabled() && !FLAGS_skip_external_kerberos_auth;
}
} // namespace impala
