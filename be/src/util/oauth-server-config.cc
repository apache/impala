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

#include "util/oauth-server-config.h"

#include <strings.h>
#include <memory>

#include <curl/curl.h>
#include <gflags/gflags.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "common/logging.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace strings;

DECLARE_string(jwks_file_path);
DECLARE_string(jwks_url);
DECLARE_bool(jwt_token_auth);
DECLARE_bool(jwks_verify_server_certificate);
DECLARE_string(jwks_ca_certificate);
DECLARE_int32(jwks_update_frequency_s);
DECLARE_int32(jwks_pulling_timeout_s);
DECLARE_string(jwt_custom_claim_username);

DECLARE_string(oauth_jwks_file_path);
DECLARE_string(oauth_jwks_url);
DECLARE_bool(oauth_token_auth);
DECLARE_bool(oauth_jwks_verify_server_certificate);
DECLARE_string(oauth_jwks_ca_certificate);
DECLARE_int32(oauth_jwks_update_frequency_s);
DECLARE_int32(oauth_jwks_pulling_timeout_s);
DECLARE_string(oauth_jwt_custom_claim_username);

DEFINE_string(oauth_servers, "",
    "JSON array of OAuth server configurations for JWT/OAuth token verification. Each "
    "element may specify caCertFilePath, verifyServerCert, jwksFilePath, jwksUrl, "
    "jwksProxy, jwksProxyCaCertFilePath, jwksProxyPinnedPublicKey, "
    "jwksPullTimeoutSecs, jwksUpdateFrequencySecs, usernameClaim, audienceClaims, and "
    "issuerClaims. jwksProxy is the proxy used to download the JWKS from jwksUrl, e.g. "
    "'https://proxy.example.com:3128' or 'socks5h://proxy.example.com:1080', in any "
    "format accepted by libcurl's CURLOPT_PROXY except that it must not contain a user "
    "name or password. Hosts listed in the 'no_proxy' environment variable bypass the "
    "proxy. The TLS certificate of an https proxy is verified according to "
    "verifyServerCert against the CA certificates in jwksProxyCaCertFilePath, which "
    "defaults to caCertFilePath. If jwksProxyCaCertFilePath is empty, the CA "
    "certificates in --trusted_certificate_file are used if that flag is set, otherwise "
    "the default CA bundle. It must also match jwksProxyPinnedPublicKey if set, which is "
    "either a path to a PEM or DER public key file or 'sha256//<base64 hash>' as "
    "accepted by libcurl's CURLOPT_PROXY_PINNEDPUBLICKEY, and requires an https "
    "jwksProxy.");

namespace impala {

namespace {

bool IsLegacyJwtConfigFlagSpecified() {
  return FLAGS_jwt_token_auth
      && (!FLAGS_jwks_file_path.empty() || !FLAGS_jwks_url.empty());
}

bool IsLegacyOAuthConfigFlagSpecified() {
  return FLAGS_oauth_token_auth
      && (!FLAGS_oauth_jwks_file_path.empty() || !FLAGS_oauth_jwks_url.empty());
}

Status SetJwksSource(const string& jwks_file_path, const string& jwks_url,
    OAuthServerConfig* config) {
  DCHECK(config != nullptr);
  if (!jwks_file_path.empty() && !jwks_url.empty()) {
    return Status("oauth_servers entry cannot specify both jwksFilePath and jwksUrl");
  }
  if (jwks_file_path.empty() && jwks_url.empty()) {
    return Status("oauth_servers entry must specify either jwksFilePath or jwksUrl");
  }
  if (!jwks_file_path.empty()) {
    config->jwks_uri = jwks_file_path;
    config->is_local_jwks = true;
  } else if (!jwks_url.empty()) {
    config->jwks_uri = jwks_url;
    config->is_local_jwks = false;
  }
  return Status::OK();
}

bool HasUrlPart(CURLU* url, CURLUPart part) {
  char* value = nullptr;
  bool has_part = curl_url_get(url, part, &value, 0) == CURLUE_OK;
  curl_free(value);
  return has_part;
}

// The proxy URL is parsed with libcurl's URL API using the same flags that libcurl uses
// for CURLOPT_PROXY. The value is never included in error messages.
Status ValidateJwksProxy(const OAuthServerConfig& config) {
  if (config.jwks_proxy.empty()) {
    if (!config.jwks_proxy_pinned_public_key.empty()) {
      return Status(
          "oauth_servers entry field 'jwksProxyPinnedPublicKey' requires 'jwksProxy'");
    }
    return Status::OK();
  }
  if (config.is_local_jwks) {
    return Status("oauth_servers entry field 'jwksProxy' requires 'jwksUrl'");
  }
  std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> url(
      curl_url(), &curl_url_cleanup);
  if (url == nullptr) return Status("Failed to allocate a libcurl URL handle");
  if (curl_url_set(url.get(), CURLUPART_URL, config.jwks_proxy.c_str(),
          CURLU_NON_SUPPORT_SCHEME | CURLU_GUESS_SCHEME) != CURLUE_OK) {
    return Status("oauth_servers entry field 'jwksProxy' is not a valid URL");
  }
  // The --oauth_servers flag is logged and shown on the debug web UI.
  if (HasUrlPart(url.get(), CURLUPART_USER)
      || HasUrlPart(url.get(), CURLUPART_PASSWORD)) {
    return Status("oauth_servers entry field 'jwksProxy' must not contain a user name "
        "or password");
  }
  if (!config.jwks_proxy_pinned_public_key.empty()) {
    char* scheme = nullptr;
    bool is_https = curl_url_get(url.get(), CURLUPART_SCHEME, &scheme, 0) == CURLUE_OK
        && strcasecmp(scheme, "https") == 0;
    curl_free(scheme);
    if (!is_https) {
      return Status("oauth_servers entry field 'jwksProxyPinnedPublicKey' requires an "
          "https 'jwksProxy'");
    }
  }
  return Status::OK();
}

Status ValidateJwksSettings(const OAuthServerConfig& config) {
  if (config.jwks_pull_timeout_secs <= 0) {
    return Status("oauth_servers entry field 'jwksPullTimeoutSecs' must be > 0");
  }
  if (config.jwks_update_frequency_secs <= 0) {
    return Status("oauth_servers entry field 'jwksUpdateFrequencySecs' must be > 0");
  }
  return ValidateJwksProxy(config);
}

Status ReadOptionalStringField(const rapidjson::Value& obj, const char* field_name,
    std::string* value_out) {
  if (!obj.HasMember(field_name)) return Status::OK();
  const rapidjson::Value& field = obj[field_name];
  if (!field.IsString()) {
    return Status(Substitute("oauth_servers entry field '$0' must be a string",
        field_name));
  }
  *value_out = field.GetString();
  return Status::OK();
}

Status ReadOptionalBoolField(const rapidjson::Value& obj, const char* field_name,
    bool* value_out) {
  if (!obj.HasMember(field_name)) return Status::OK();
  const rapidjson::Value& field = obj[field_name];
  if (!field.IsBool()) {
    return Status(Substitute("oauth_servers entry field '$0' must be a boolean",
        field_name));
  }
  *value_out = field.GetBool();
  return Status::OK();
}

Status ReadOptionalIntField(const rapidjson::Value& obj, const char* field_name,
    int32_t* value_out) {
  if (!obj.HasMember(field_name)) return Status::OK();
  const rapidjson::Value& field = obj[field_name];
  if (!field.IsInt()) {
    return Status(Substitute("oauth_servers entry field '$0' must be an integer",
        field_name));
  }
  *value_out = field.GetInt();
  return Status::OK();
}

Status ReadOptionalStringArrayField(const rapidjson::Value& obj, const char* field_name,
    vector<string>* values_out) {
  DCHECK(values_out != nullptr);
  if (!obj.HasMember(field_name)) return Status::OK();
  const rapidjson::Value& field = obj[field_name];
  if (!field.IsArray()) {
    return Status(Substitute("oauth_servers entry field '$0' must be an array",
        field_name));
  }
  for (rapidjson::SizeType i = 0; i < field.Size(); ++i) {
    if (!field[i].IsString()) {
      return Status(Substitute("oauth_servers entry field '$0' must be an array of "
          "strings", field_name));
    }
    // Preserve backward compatibility: empty entries are treated as unset values.
    if (field[i].GetStringLength() > 0) values_out->push_back(field[i].GetString());
  }
  return Status::OK();
}

Status ParseOAuthServerObject(
    const rapidjson::Value& obj, OAuthServerConfig* config_out) {
  if (!obj.IsObject()) {
    return Status("Each oauth_servers entry must be a JSON object");
  }
  OAuthServerConfig config;
  string jwks_file_path;
  string jwks_url;
  RETURN_IF_ERROR(
      ReadOptionalStringField(obj, "caCertFilePath", &config.ca_cert_file_path));
  RETURN_IF_ERROR(
      ReadOptionalBoolField(obj, "verifyServerCert", &config.verify_server_cert));
  RETURN_IF_ERROR(ReadOptionalStringField(obj, "jwksFilePath", &jwks_file_path));
  RETURN_IF_ERROR(ReadOptionalStringField(obj, "jwksUrl", &jwks_url));
  RETURN_IF_ERROR(SetJwksSource(jwks_file_path, jwks_url, &config));
  RETURN_IF_ERROR(ReadOptionalStringField(obj, "jwksProxy", &config.jwks_proxy));
  config.jwks_proxy_ca_certificate = config.ca_cert_file_path;
  RETURN_IF_ERROR(ReadOptionalStringField(
      obj, "jwksProxyCaCertFilePath", &config.jwks_proxy_ca_certificate));
  RETURN_IF_ERROR(ReadOptionalStringField(
      obj, "jwksProxyPinnedPublicKey", &config.jwks_proxy_pinned_public_key));
  RETURN_IF_ERROR(ReadOptionalIntField(
      obj, "jwksPullTimeoutSecs", &config.jwks_pull_timeout_secs));
  RETURN_IF_ERROR(ReadOptionalIntField(
      obj, "jwksUpdateFrequencySecs", &config.jwks_update_frequency_secs));
  RETURN_IF_ERROR(ReadOptionalStringField(obj, "usernameClaim", &config.username_claim));
  RETURN_IF_ERROR(ReadOptionalStringArrayField(
      obj, "audienceClaims", &config.audience_claims));
  RETURN_IF_ERROR(ReadOptionalStringArrayField(
      obj, "issuerClaims", &config.issuer_claims));
  RETURN_IF_ERROR(ValidateJwksSettings(config));
  if (config.username_claim.empty()) {
    return Status("oauth_servers entry field 'usernameClaim' must not be empty");
  }
  *config_out = config;
  return Status::OK();
}

Status BuildLegacyJwtServerConfig(OAuthServerConfig* config_out) {
  DCHECK(config_out != nullptr);
  OAuthServerConfig config;
  string jwks_file_path = FLAGS_jwks_file_path;
  string jwks_url = FLAGS_jwks_url;
  if (!jwks_file_path.empty() && !jwks_url.empty()) jwks_url.clear();
  config.ca_cert_file_path = FLAGS_jwks_ca_certificate;
  config.verify_server_cert = FLAGS_jwks_verify_server_certificate;
  RETURN_IF_ERROR(SetJwksSource(jwks_file_path, jwks_url, &config));
  config.jwks_pull_timeout_secs = FLAGS_jwks_pulling_timeout_s;
  config.jwks_update_frequency_secs = FLAGS_jwks_update_frequency_s;
  config.username_claim = FLAGS_jwt_custom_claim_username;
  *config_out = std::move(config);
  return Status::OK();
}

Status BuildLegacyOAuthServerConfig(OAuthServerConfig* config_out) {
  DCHECK(config_out != nullptr);
  OAuthServerConfig config;
  string jwks_file_path = FLAGS_oauth_jwks_file_path;
  string jwks_url = FLAGS_oauth_jwks_url;
  if (!jwks_file_path.empty() && !jwks_url.empty()) jwks_url.clear();
  config.ca_cert_file_path = FLAGS_oauth_jwks_ca_certificate;
  config.verify_server_cert = FLAGS_oauth_jwks_verify_server_certificate;
  RETURN_IF_ERROR(SetJwksSource(jwks_file_path, jwks_url, &config));
  config.jwks_pull_timeout_secs = FLAGS_oauth_jwks_pulling_timeout_s;
  config.jwks_update_frequency_secs = FLAGS_oauth_jwks_update_frequency_s;
  config.username_claim = FLAGS_oauth_jwt_custom_claim_username;
  *config_out = std::move(config);
  return Status::OK();
}

void WarnIfFlagSet(const char* flag_name, bool is_set) {
  if (is_set) {
    LOG(WARNING) << "Startup flag --" << flag_name
                 << " is deprecated. Use --oauth_servers instead.";
  }
}

Status ParseOAuthServersJson(
    const string& oauth_servers_json, vector<OAuthServerConfig>* configs_out) {
  DCHECK(configs_out != nullptr);
  if (oauth_servers_json.empty()) return Status::OK();

  rapidjson::Document doc;
  doc.Parse(oauth_servers_json.data(), oauth_servers_json.size());
  if (doc.HasParseError()) {
    return Status(Substitute("Failed to parse --oauth_servers JSON: $0 at offset $1",
        rapidjson::GetParseError_En(doc.GetParseError()), doc.GetErrorOffset()));
  }
  if (!doc.IsArray()) {
    return Status("--oauth_servers must be a JSON array");
  }
  for (rapidjson::SizeType i = 0; i < doc.Size(); ++i) {
    OAuthServerConfig config;
    RETURN_IF_ERROR(ParseOAuthServerObject(doc[i], &config));
    configs_out->push_back(config);
  }
  return Status::OK();
}

void WarnOnDeprecatedOAuthFlags() {
  WarnIfFlagSet("jwks_file_path", !FLAGS_jwks_file_path.empty());
  WarnIfFlagSet("jwks_url", !FLAGS_jwks_url.empty());
  WarnIfFlagSet("jwks_verify_server_certificate",
      !FLAGS_jwks_verify_server_certificate);
  WarnIfFlagSet("jwks_ca_certificate", !FLAGS_jwks_ca_certificate.empty());
  WarnIfFlagSet("jwks_update_frequency_s", FLAGS_jwks_update_frequency_s != 60);
  WarnIfFlagSet("jwks_pulling_timeout_s", FLAGS_jwks_pulling_timeout_s != 10);
  WarnIfFlagSet("jwt_custom_claim_username",
      FLAGS_jwt_custom_claim_username != "username");
  WarnIfFlagSet("oauth_jwks_file_path", !FLAGS_oauth_jwks_file_path.empty());
  WarnIfFlagSet("oauth_jwks_url", !FLAGS_oauth_jwks_url.empty());
  WarnIfFlagSet("oauth_jwks_verify_server_certificate",
      !FLAGS_oauth_jwks_verify_server_certificate);
  WarnIfFlagSet("oauth_jwks_ca_certificate", !FLAGS_oauth_jwks_ca_certificate.empty());
  WarnIfFlagSet("oauth_jwks_update_frequency_s",
      FLAGS_oauth_jwks_update_frequency_s != 60);
  WarnIfFlagSet("oauth_jwks_pulling_timeout_s",
      FLAGS_oauth_jwks_pulling_timeout_s != 10);
  WarnIfFlagSet("oauth_jwt_custom_claim_username",
      FLAGS_oauth_jwt_custom_claim_username != "username");
}

} // anonymous namespace

Status BuildOAuthServerConfigs(vector<OAuthServerConfig>* configs_out) {
  DCHECK(configs_out != nullptr);
  configs_out->clear();
  WarnOnDeprecatedOAuthFlags();

  RETURN_IF_ERROR(ParseOAuthServersJson(FLAGS_oauth_servers, configs_out));
  if (IsLegacyJwtConfigFlagSpecified()) {
    OAuthServerConfig config;
    RETURN_IF_ERROR(BuildLegacyJwtServerConfig(&config));
    configs_out->push_back(std::move(config));
  }
  if (IsLegacyOAuthConfigFlagSpecified()) {
    OAuthServerConfig config;
    RETURN_IF_ERROR(BuildLegacyOAuthServerConfig(&config));
    configs_out->push_back(std::move(config));
  }
  return Status::OK();
}

} // namespace impala
