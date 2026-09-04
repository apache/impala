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

#include "util/oauth-servers-manager.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "util/oauth-server-config.h"

#include "common/names.h"

using namespace strings;

namespace impala {

string OAuthServersManager::BearerAuthFailureHeader(const Status& status) {
  const string error_message =
      status.GetDetail().empty() ? status.msg().msg() : status.GetDetail();
  return Substitute("WWW-Authenticate: Bearer error=\"invalid_token\","
      "error_description=\"$0 \"", error_message);
}

Status OAuthServersManager::Init() {
  vector<OAuthServerConfig> configs;
  RETURN_IF_ERROR(BuildOAuthServerConfigs(&configs));
  if (configs.empty()) {
    return Status("No OAuth servers are configured for token verification");
  }
  jwt_helpers_ = std::make_unique<JwtHelpers>();
  jwt_helpers_->reserve(configs.size());

  for (OAuthServerConfig& config : configs) {
    OAuthServerVerifier verifier;
    Status status = verifier.jwt_helper.Init(config.jwks_uri, config.verify_server_cert,
        config.ca_cert_file_path, config.is_local_jwks, config.jwks_pull_timeout_secs,
        config.jwks_update_frequency_secs);
    if (!status.ok()) return status;
    verifier.username_claim = std::move(config.username_claim);
    jwt_helpers_->emplace_back(std::move(verifier));
  }
  return Status::OK();
}

Status OAuthServersManager::AuthenticateBearerToken(
    const string& token, string* username_out) const {
  DCHECK(jwt_helpers_);
  DCHECK(username_out != nullptr);
  username_out->clear();
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  RETURN_IF_ERROR(JWTHelper::Decode(token, decoded_token));
  return Verify(decoded_token.get(), username_out);
}

Status OAuthServersManager::Verify(const JWTHelper::JWTDecodedToken* decoded_token,
    string* username_out) const {
  DCHECK(jwt_helpers_);
  DCHECK(!jwt_helpers_->empty());
  DCHECK(decoded_token != nullptr);

  Status last_error;
  bool attempted_signature_verification = false;
  for (size_t next_idx = 0; next_idx < jwt_helpers_->size();) {
    size_t matched_server_idx = size();
    RETURN_IF_ERROR(FindMatchingServer(decoded_token, next_idx, &matched_server_idx));
    if (matched_server_idx == size()) break;
    attempted_signature_verification = true;
    Status status = jwt_helpers_->at(matched_server_idx).jwt_helper.Verify(decoded_token);
    if (status.ok()) {
      if (username_out != nullptr) {
        RETURN_IF_ERROR(GetUsername(decoded_token, matched_server_idx, username_out));
      }
      return Status::OK();
    }
    last_error = status;
    next_idx = matched_server_idx + 1;
  }
  if (attempted_signature_verification) return last_error;
  return Status(TErrorCode::JWT_VERIFY_FAILED, "Verification failed, no matching key");
}

Status OAuthServersManager::FindMatchingServer(
    const JWTHelper::JWTDecodedToken* decoded_token, size_t start_idx,
    size_t* matched_server_idx_out) const {
  DCHECK(jwt_helpers_);
  DCHECK(decoded_token != nullptr);
  DCHECK(matched_server_idx_out != nullptr);

  for (size_t i = start_idx; i < jwt_helpers_->size(); ++i) {
    bool can_verify = true;
    RETURN_IF_ERROR(jwt_helpers_->at(i).jwt_helper.CanVerify(decoded_token, &can_verify));
    if (can_verify) {
      *matched_server_idx_out = i;
      return Status::OK();
    }
  }
  *matched_server_idx_out = size();
  return Status::OK();
}

Status OAuthServersManager::GetUsername(const JWTHelper::JWTDecodedToken* decoded_token,
    size_t server_idx, string* username_out) const {
  DCHECK(jwt_helpers_);
  DCHECK(decoded_token != nullptr);
  DCHECK(username_out != nullptr);
  DCHECK_LT(server_idx, jwt_helpers_->size());
  return JWTHelper::GetCustomClaimUsername(
      decoded_token, jwt_helpers_->at(server_idx).username_claim, *username_out);
}

} // namespace impala
