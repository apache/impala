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

#include "util/auth-util.h"

#include <ostream>

#include <boost/algorithm/string/classification.hpp>

#include "common/logging.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"
#include "kudu/security/init.h"
#include "kudu/util/status.h"
#include "util/kudu-status-util.h"
#include "util/network-util.h"

using namespace std;
using boost::algorithm::is_any_of;

DECLARE_string(principal);
DECLARE_string(be_principal);

namespace impala {

// Pattern for hostname substitution.
static const string HOSTNAME_PATTERN = "_HOST";

const string& GetEffectiveUser(const TSessionState& session) {
  if (session.__isset.delegated_user && !session.delegated_user.empty()) {
    return session.delegated_user;
  }
  return session.connected_user;
}

const string& GetEffectiveUser(const ImpalaServer::SessionState& session) {
  return session.do_as_user.empty() ? session.connected_user : session.do_as_user;
}

Status CheckProfileAccess(const string& user, const string& effective_user,
    bool has_access) {
  if (user.empty() || (user == effective_user && has_access)) return Status::OK();
  stringstream ss;
  ss << "User " << user << " is not authorized to access the runtime profile or "
     << "execution summary.";
  return Status(ss.str());
}

// Replaces _HOST with the hostname if it occurs in the principal string.
Status ReplacePrincipalHostFormat(string* out_principal) {
  // Replace the string _HOST in principal with our hostname.
  size_t off = out_principal->find(HOSTNAME_PATTERN);
  if (off != string::npos) {
    string hostname;
    RETURN_IF_ERROR(GetHostname(&hostname));
    out_principal->replace(off, HOSTNAME_PATTERN.size(), hostname);
  }
  return Status::OK();
}

Status GetExternalKerberosPrincipal(string* out_principal) {
  DCHECK(IsKerberosEnabled());

  *out_principal = FLAGS_principal;
  DCHECK(!out_principal->empty());
  RETURN_IF_ERROR(ReplacePrincipalHostFormat(out_principal));
  return Status::OK();
}

Status GetInternalKerberosPrincipal(string* out_principal) {
  DCHECK(IsKerberosEnabled());

  *out_principal = !FLAGS_be_principal.empty() ? FLAGS_be_principal : FLAGS_principal;
  DCHECK(!out_principal->empty());
  RETURN_IF_ERROR(ReplacePrincipalHostFormat(out_principal));
  return Status::OK();
}

Status ParseKerberosPrincipal(const string& principal, string* service_name,
    string* hostname, string* realm) {
  KUDU_RETURN_IF_ERROR(kudu::security::Krb5ParseName(principal, service_name,
      hostname, realm), strings::Substitute("bad principal format $0", principal));
  return Status::OK();
}

string GetShortUsernameFromKerberosPrincipal(const string& principal) {
  size_t end_idx = min(principal.find('/'), principal.find('@'));
  string short_user(
      end_idx == string::npos || end_idx == 0 ?
      principal : principal.substr(0, end_idx));
  return short_user;
}

}
