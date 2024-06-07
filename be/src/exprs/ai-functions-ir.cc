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

#include <gutil/strings/util.h>

#include "exprs/ai-functions.inline.h"

using namespace impala_udf;

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

bool AiFunctions::is_api_endpoint_valid(const std::string_view& endpoint) {
  // Simple validation for endpoint. It should start with https://
  return (strncaseprefix(endpoint.data(), endpoint.size(), AI_API_ENDPOINT_PREFIX,
              sizeof(AI_API_ENDPOINT_PREFIX))
      != nullptr);
}

bool AiFunctions::is_api_endpoint_supported(const std::string_view& endpoint) {
  // Only OpenAI endpoints are supported.
  return (
      gstrncasestr(endpoint.data(), OPEN_AI_AZURE_ENDPOINT, endpoint.size()) != nullptr ||
      gstrncasestr(endpoint.data(), OPEN_AI_PUBLIC_ENDPOINT, endpoint.size()) != nullptr);
}

AiFunctions::AI_PLATFORM AiFunctions::GetAiPlatformFromEndpoint(
    const std::string_view& endpoint) {
  // Only OpenAI endpoints are supported.
  if (gstrncasestr(endpoint.data(), OPEN_AI_PUBLIC_ENDPOINT, endpoint.size()) != nullptr)
    return AiFunctions::AI_PLATFORM::OPEN_AI;
  if (gstrncasestr(endpoint.data(), OPEN_AI_AZURE_ENDPOINT, endpoint.size()) != nullptr)
    return AiFunctions::AI_PLATFORM::AZURE_OPEN_AI;
  return AiFunctions::AI_PLATFORM::UNSUPPORTED;
}

StringVal AiFunctions::copyErrorMessage(FunctionContext* ctx, const string& errorMsg) {
  return StringVal::CopyFrom(ctx,
      reinterpret_cast<const uint8_t*>(errorMsg.c_str()),
      errorMsg.length());
}

string AiFunctions::AiGenerateTextParseOpenAiResponse(
    const std::string_view& response) {
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
    const StringVal& api_key_jceks_secret, const StringVal& params) {
  std::string_view endpoint_sv(FLAGS_ai_endpoint);
  // endpoint validation
  if (!fastpath && endpoint.ptr != nullptr && endpoint.len != 0) {
    endpoint_sv = std::string_view(reinterpret_cast<char*>(endpoint.ptr), endpoint.len);
    // Simple validation for endpoint. It should start with https://
    if (!is_api_endpoint_valid(endpoint_sv)) {
      LOG(ERROR) << "AI Generate Text: \ninvalid protocol: " << endpoint_sv;
      return StringVal(AI_GENERATE_TXT_INVALID_PROTOCOL_ERROR.c_str());
    }
  }
  AI_PLATFORM platform = GetAiPlatformFromEndpoint(endpoint_sv);
  switch(platform) {
    case AI_PLATFORM::OPEN_AI:
      return AiGenerateTextInternal<fastpath, AI_PLATFORM::OPEN_AI>(
          ctx, endpoint_sv, prompt, model, api_key_jceks_secret, params, false);
    case AI_PLATFORM::AZURE_OPEN_AI:
      return AiGenerateTextInternal<fastpath, AI_PLATFORM::AZURE_OPEN_AI>(
          ctx, endpoint_sv, prompt, model, api_key_jceks_secret, params, false);
    default:
      if (fastpath) {
        DCHECK(false) << "Default endpoint " << FLAGS_ai_endpoint << "must be supported";
      }
      LOG(ERROR) << "AI Generate Text: \nunsupported endpoint: " << endpoint_sv;
      return StringVal(AI_GENERATE_TXT_UNSUPPORTED_ENDPOINT_ERROR.c_str());
  }
}

StringVal AiFunctions::AiGenerateText(FunctionContext* ctx, const StringVal& endpoint,
    const StringVal& prompt, const StringVal& model,
    const StringVal& api_key_jceks_secret, const StringVal& params) {
  return AiGenerateTextHelper<false>(
      ctx, endpoint, prompt, model, api_key_jceks_secret, params);
}

StringVal AiFunctions::AiGenerateTextDefault(
  FunctionContext* ctx, const StringVal& prompt) {
  return AiGenerateTextHelper<true>(
      ctx, NULL_STRINGVAL, prompt, NULL_STRINGVAL, NULL_STRINGVAL, NULL_STRINGVAL);
}

} // namespace impala
