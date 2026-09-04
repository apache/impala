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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "util/jwt-util.h"

namespace impala {

/// Manages multiple OAuth / JWT identity providers. Each configured server has its own
/// JWTHelper instance for signature verification.
class OAuthServersManager {
 public:
  /// Initializes OAuth servers from --oauth_servers and legacy JWT/OAuth flags.
  Status Init();

  /// Decodes and validates a bearer token and extracts the username.
  Status AuthenticateBearerToken(
      const std::string& token, std::string* username_out) const;

  /// Builds the RFC6750 WWW-Authenticate response header for bearer-token failures.
  static std::string BearerAuthFailureHeader(const Status& status);

  bool empty() const {
    DCHECK(jwt_helpers_);
    return jwt_helpers_->empty();
  }

  size_t size() const {
    DCHECK(jwt_helpers_);
    return jwt_helpers_->size();
  }

 private:
  /// Verifies the token signature against configured servers. On success, also
  /// extracts the username claim from the matching server if 'username_out' is
  /// non-null.
  Status Verify(
      const JWTHelper::JWTDecodedToken* decoded_token, std::string* username_out) const;

  /// Finds the first server (starting from 'start_idx') that can verify the token.
  /// If none matches, sets 'matched_server_idx_out' to size().
  Status FindMatchingServer(const JWTHelper::JWTDecodedToken* decoded_token,
      size_t start_idx, size_t* matched_server_idx_out) const;

  struct OAuthServerVerifier {
    JWTHelper jwt_helper;
    std::string username_claim;
  };

  /// Extracts the username using the username claim from the server at 'server_idx'.
  Status GetUsername(const JWTHelper::JWTDecodedToken* decoded_token, size_t server_idx,
      std::string* username_out) const;

  using JwtHelpers = std::vector<OAuthServerVerifier>;
  std::unique_ptr<JwtHelpers> jwt_helpers_;
};

} // namespace impala
