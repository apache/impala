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

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace impala {

/// Configuration for a single OAuth / JWT identity provider, as specified in the
/// --oauth_servers JSON flag (IMPALA-14799).
struct OAuthServerConfig {
  static constexpr int32_t DEFAULT_JWKS_PULL_TIMEOUT_SECS = 10;
  static constexpr int32_t DEFAULT_JWKS_UPDATE_FREQUENCY_SECS = 60;

  std::string ca_cert_file_path;
  bool verify_server_cert = true;
  std::string jwks_uri;
  bool is_local_jwks = false;
  int32_t jwks_pull_timeout_secs = DEFAULT_JWKS_PULL_TIMEOUT_SECS;
  int32_t jwks_update_frequency_secs = DEFAULT_JWKS_UPDATE_FREQUENCY_SECS;
  std::string username_claim = "username";
};

/// Builds the full list of OAuth server configs from --oauth_servers plus any legacy
/// jwks_* / oauth_* startup flags.
Status BuildOAuthServerConfigs(std::vector<OAuthServerConfig>* configs_out);

} // namespace impala
