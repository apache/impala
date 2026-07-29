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

// The functions in this file are specifically not cross-compiled to IR because there
// is no signifcant performance benefit to be gained.

#include <set>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <curl/curl.h>

#include "exprs/ai-functions.inline.h"

using namespace impala_udf;
using boost::algorithm::trim;
using std::any_of;
using std::istringstream;
using std::set;
using std::string_view;

DEFINE_string(ai_endpoint, "https://api.openai.com/v1/chat/completions",
    "The default API endpoint for an external AI engine.");
DEFINE_validator(ai_endpoint, [](const char* name, const string& endpoint) {
  return (impala::AiFunctions::is_api_endpoint_valid(endpoint) &&
      impala::AiFunctions::is_api_endpoint_supported(endpoint));
});

DEFINE_string(ai_model, "gpt-4", "The default AI model used by an external AI engine.");

DEFINE_string(ai_api_key_jceks_secret, "",
    "The jceks secret key used for extracting the api key from configured keystores. "
    "'hadoop.security.credential.provider.path' in core-site must be configured to "
    "include the keystore storing the corresponding secret.");

DEFINE_string(ai_additional_platforms, "",
    "A comma-separated list of additional platforms allowed for Impala to access via "
    "the AI api, formatted as 'site1,site2'.");

DEFINE_int32(ai_connection_timeout_s, 10,
    "(Advanced) The time in seconds for connection timed out when communicating with an "
    "external AI engine");
TAG_FLAG(ai_api_key_jceks_secret, sensitive);

namespace impala {

// static class members
const string AiFunctions::AI_GENERATE_TXT_JSON_PARSE_ERROR = "Invalid Json";
const string AiFunctions::AI_GENERATE_TXT_INVALID_PROTOCOL_ERROR =
    "Invalid Protocol, use https";
const string AiFunctions::AI_GENERATE_TXT_UNSUPPORTED_ENDPOINT_ERROR =
    "Unsupported Endpoint";
const string AiFunctions::AI_GENERATE_TXT_INVALID_PROMPT_ERROR =
    "Invalid Prompt, cannot be null or empty";
const string AiFunctions::AI_GENERATE_TXT_MSG_OVERRIDE_FORBIDDEN_ERROR =
    "Invalid override, 'messages' cannot be overriden";
const string AiFunctions::AI_GENERATE_TXT_N_OVERRIDE_FORBIDDEN_ERROR =
    "Invalid override, 'n' must be of integer type and have value 1";
string AiFunctions::ai_api_key_;
const char* AiFunctions::OPEN_AI_REQUEST_FIELD_CONTENT_TYPE_HEADER =
    "Content-Type: application/json";
const char* AiFunctions::OPEN_AI_REQUEST_AUTH_HEADER =
    "Authorization: Bearer ";
const char* AiFunctions::AZURE_OPEN_AI_REQUEST_AUTH_HEADER =
    "api-key: ";

// other constants
static const StringVal NULL_STRINGVAL = StringVal::null();
static const char* AI_API_ENDPOINT_PREFIX = "https://";
static const char* OPEN_AI_AZURE_ENDPOINT = "openai.azure.com";
static const char* OPEN_AI_PUBLIC_ENDPOINT = "api.openai.com";
// OPEN AI specific constants
static const char* OPEN_AI_RESPONSE_FIELD_CHOICES = "choices";
static const char* OPEN_AI_RESPONSE_FIELD_MESSAGE = "message";
static const char* OPEN_AI_RESPONSE_FIELD_CONTENT = "content";

// Helper to extract the hostname from a URL using libcurl.
// Returns empty string if invalid.
// The input 'endpoint' must be null-terminated (\0).
static string ExtractHost(const string_view& endpoint) {
  string final_host = "";
  // Reject any endpoint embedded NULL bytes (\0).
  if (UNLIKELY(endpoint.find('\0') != string_view::npos)) {
    LOG(WARNING) << "Rejected AI endpoint containing embedded null byte(s).";
    return final_host;
  }
  // Use the libcurl URL parser to extract the host name from the endpoint.
  CURLU* url_handle = curl_url();
  if (UNLIKELY(url_handle == nullptr)) {
    LOG(WARNING) << "Failed to allocate libcurl handle extracting host from endpoint";
    return final_host;
  }
  CURLUcode rc = curl_url_set(url_handle, CURLUPART_URL, endpoint.data(), 0);
  if (LIKELY(rc == CURLUE_OK)) {
    char* host = nullptr;
    rc = curl_url_get(url_handle, CURLUPART_HOST, &host, 0);
    if (LIKELY(rc == CURLUE_OK)) {
      final_host = host;
      curl_free(host);
    } else {
      DCHECK(host == nullptr) << "libcurl should not set host pointer on failure";
      LOG(WARNING) << "Failed to curl_url_get(). CURLUcode: " << rc;
    }
  } else {
    LOG(WARNING) << "Failed to curl_url_set(). CURLUcode: " << rc;
  }
  curl_url_cleanup(url_handle);
  return final_host;
}

// Helper to strictly validate a host against a target domain.
bool IsHostMatch(const string_view& host, const string_view& target_domain) {
  if (host.length() == target_domain.length()) {
    return strncasecmp(host.data(), target_domain.data(), target_domain.length()) == 0;
  } else if (host.length() > target_domain.length() + 1) {
    // Allow subdomains by checking for a preceding dot.
    // Length +1 ensures there is at least one character before the dot
    // (e.g., 'a.domain.com').
    if (host[host.length() - target_domain.length() - 1] == '.') {
      return boost::algorithm::iends_with(host, target_domain);
    }
  }
  return false;
}

/**
 * Singleton class for managing the additional AI platforms endpoints.
 * The additional platforms are loaded and parsed once to optimize for efficiency.
 */
class AIAdditionalPlatforms {
 public:
  // Singleton accessor.
  static AIAdditionalPlatforms& GetInstance() {
    static AIAdditionalPlatforms instance;
    return instance;
  }

  // Prevent copying.
  AIAdditionalPlatforms(const AIAdditionalPlatforms&) = delete;
  AIAdditionalPlatforms& operator=(const AIAdditionalPlatforms&) = delete;

  // Check if the extracted host matches any of the additional platforms.
  bool IsGeneralSite(const string_view& host) const {
    return any_of(additional_platforms.begin(), additional_platforms.end(),
        [&host](const string& site) {
          return IsHostMatch(host, site);
        });
  }

  // For testing.
  void Reset() {
    additional_platforms.clear();
    ParseAdditionalSites();
  }

 private:
  AIAdditionalPlatforms() { ParseAdditionalSites(); }

  // Parse additional platforms from the flag ai_additional_platforms.
  void ParseAdditionalSites() {
    const string& ai_additional_platforms = FLAGS_ai_additional_platforms;

    if (!ai_additional_platforms.empty()) {
      istringstream stream(ai_additional_platforms);
      string site;
      LOG(INFO) << "Loading AI platform additional platforms: "
                << ai_additional_platforms;

      while (getline(stream, site, ',')) {
        trim(site);
        if (!site.empty()) {
          additional_platforms.insert(site);
          LOG(INFO) << "Loaded AI platform additional site: " << site;
        }
      }
    }
  }

  // Storage of AI additional platforms;
  set<string> additional_platforms;
};

bool AiFunctions::is_api_endpoint_valid(const string_view& endpoint) {
  // Simple validation for endpoint. It should start with https://
  return (strncaseprefix(endpoint.data(), endpoint.size(), AI_API_ENDPOINT_PREFIX,
              strlen(AI_API_ENDPOINT_PREFIX))
      != nullptr);
}

bool AiFunctions::is_api_endpoint_supported(const string_view& endpoint) {
  return GetAiPlatformFromEndpoint(endpoint, false)
      != AiFunctions::AI_PLATFORM::UNSUPPORTED;
}

AiFunctions::AI_PLATFORM AiFunctions::GetAiPlatformFromEndpoint(
    const string_view& endpoint, bool dry_run) {
  if (UNLIKELY(dry_run)) AIAdditionalPlatforms::GetInstance().Reset();

  // Validate the canonical host.
  string host = ExtractHost(endpoint);
  if (host.empty()) return AiFunctions::AI_PLATFORM::UNSUPPORTED;

  if (IsHostMatch(host, OPEN_AI_PUBLIC_ENDPOINT)) {
    return AiFunctions::AI_PLATFORM::OPEN_AI;
  }
  if (IsHostMatch(host, OPEN_AI_AZURE_ENDPOINT)) {
    return AiFunctions::AI_PLATFORM::AZURE_OPEN_AI;
  }
  if (AIAdditionalPlatforms::GetInstance().IsGeneralSite(host)) {
    return AI_PLATFORM::GENERAL;
  }

  return AiFunctions::AI_PLATFORM::UNSUPPORTED;
}

StringVal AiFunctions::copyErrorMessage(FunctionContext* ctx, const string& errorMsg) {
  return StringVal::CopyFrom(ctx,
      reinterpret_cast<const uint8_t*>(errorMsg.c_str()),
      errorMsg.length());
}

string AiFunctions::AiGenerateTextParseOpenAiResponse(const string_view& response) {
  rapidjson::Document document;
  document.Parse(response.data(), response.size());
  // Check for parse errors
  if (document.HasParseError()) {
    LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR << ": " << response;
    return AI_GENERATE_TXT_JSON_PARSE_ERROR;
  }
  // Check if the "choices" array exists and is not empty
  if (!document.HasMember(OPEN_AI_RESPONSE_FIELD_CHOICES)
      || !document[OPEN_AI_RESPONSE_FIELD_CHOICES].IsArray()
      || document[OPEN_AI_RESPONSE_FIELD_CHOICES].Empty()) {
    LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR << ": " << response;
    return AI_GENERATE_TXT_JSON_PARSE_ERROR;
  }

  // Access the first element of the "choices" array
  const rapidjson::Value& firstChoice = document[OPEN_AI_RESPONSE_FIELD_CHOICES][0];

  // Check if the "message" object exists
  if (!firstChoice.HasMember(OPEN_AI_RESPONSE_FIELD_MESSAGE)
      || !firstChoice[OPEN_AI_RESPONSE_FIELD_MESSAGE].IsObject()) {
    LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR << ": " << response;
    return AI_GENERATE_TXT_JSON_PARSE_ERROR;
  }

  // Access the "content" field within "message"
  const rapidjson::Value& message = firstChoice[OPEN_AI_RESPONSE_FIELD_MESSAGE];
  if (!message.HasMember(OPEN_AI_RESPONSE_FIELD_CONTENT)
      || !message[OPEN_AI_RESPONSE_FIELD_CONTENT].IsString()) {
    LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR << ": " << response;
    return AI_GENERATE_TXT_JSON_PARSE_ERROR;
  }

  return message[OPEN_AI_RESPONSE_FIELD_CONTENT].GetString();
}

template <bool fastpath>
StringVal AiFunctions::AiGenerateTextHelper(FunctionContext* ctx,
    const StringVal& endpoint, const StringVal& prompt, const StringVal& model,
    const StringVal& auth_credential, const StringVal& platform_params,
    const StringVal& impala_options) {
  string endpoint_str;
  string_view endpoint_sv(FLAGS_ai_endpoint);
  // endpoint validation
  if (!fastpath && endpoint.ptr != nullptr && endpoint.len != 0) {
    // Copy the StringVal to a std::string to ensure it is null-terminated (\0).
    endpoint_str = string(reinterpret_cast<char*>(endpoint.ptr), endpoint.len);
    endpoint_sv = endpoint_str;
    // Simple validation for endpoint. It should start with https://
    if (!is_api_endpoint_valid(endpoint_sv)) {
      LOG(ERROR) << "AI Generate Text: \ninvalid protocol: " << endpoint_sv;
      return StringVal(AI_GENERATE_TXT_INVALID_PROTOCOL_ERROR.c_str());
    }
  }

  AI_PLATFORM platform = GetAiPlatformFromEndpoint(endpoint_sv);
  switch(platform) {
    case AI_PLATFORM::OPEN_AI:
      return AiGenerateTextInternal<fastpath, AI_PLATFORM::OPEN_AI>(ctx, endpoint_sv,
          prompt, model, auth_credential, platform_params, impala_options, false);
    case AI_PLATFORM::AZURE_OPEN_AI:
      return AiGenerateTextInternal<fastpath, AI_PLATFORM::AZURE_OPEN_AI>(ctx,
          endpoint_sv, prompt, model, auth_credential, platform_params, impala_options,
          false);
    case AI_PLATFORM::GENERAL:
      return AiGenerateTextInternal<fastpath, AI_PLATFORM::GENERAL>(ctx, endpoint_sv,
          prompt, model, auth_credential, platform_params, impala_options, false);
    default:
      if (fastpath) {
        DCHECK(false) << "Default endpoint " << FLAGS_ai_endpoint << " must be supported";
      }
      LOG(ERROR) << "AI Generate Text: \nunsupported endpoint: " << endpoint_sv;
      return StringVal(AI_GENERATE_TXT_UNSUPPORTED_ENDPOINT_ERROR.c_str());
  }
}

StringVal AiFunctions::AiGenerateText(FunctionContext* ctx, const StringVal& endpoint,
    const StringVal& prompt, const StringVal& model, const StringVal& auth_credential,
    const StringVal& platform_params, const StringVal& impala_options) {
  return AiGenerateTextHelper<false>(
      ctx, endpoint, prompt, model, auth_credential, platform_params, impala_options);
}

StringVal AiFunctions::AiGenerateTextDefault(
  FunctionContext* ctx, const StringVal& prompt) {
  return AiGenerateTextHelper<true>(ctx, NULL_STRINGVAL, prompt, NULL_STRINGVAL,
      NULL_STRINGVAL, NULL_STRINGVAL, NULL_STRINGVAL);
}

} // namespace impala
