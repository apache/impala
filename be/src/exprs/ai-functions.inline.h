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

#include <gutil/strings/util.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/compiler-util.h"
#include "exprs/ai-functions.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "runtime/exec-env.h"
#include "service/frontend.h"

using namespace rapidjson;
using namespace impala_udf;

DECLARE_string(ai_endpoint);
DECLARE_string(ai_model);
DECLARE_string(ai_api_key_jceks_secret);
DECLARE_int32(ai_connection_timeout_s);

namespace impala {

#define RETURN_STRINGVAL_IF_ERROR(ctx, stmt)               \
  do {                                                     \
    const ::impala::Status& _status = (stmt);              \
    if (UNLIKELY(!_status.ok())) {                         \
      return copyErrorMessage(ctx, _status.msg().msg());   \
    }                                                      \
  } while (false)

// Impala Ai Functions Options Constants.
static const char* IMPALA_AI_API_STANDARD_FIELD = "api_standard";
static const char* IMPALA_AI_CREDENTIAL_TYPE_FIELD = "credential_type";
static const char* IMPALA_AI_PAYLOAD_FIELD = "payload";
static const char* IMPALA_AI_API_STANDARD_OPENAI = "openai";
static const char* IMPALA_AI_CREDENTIAL_TYPE_PLAIN = "plain";
static const char* IMPALA_AI_CREDENTIAL_TYPE_JCEKS = "jceks";
static const int MAX_CUSTOM_PAYLOAD_LENGTH = 5 * 1024 * 1024; // 5MB

static const size_t IMPALA_AI_API_STANDARD_OPENAI_LEN =
    std::strlen(IMPALA_AI_API_STANDARD_OPENAI);
static const size_t IMPALA_AI_CREDENTIAL_TYPE_PLAIN_LEN =
    std::strlen(IMPALA_AI_CREDENTIAL_TYPE_PLAIN);
static const size_t IMPALA_AI_CREDENTIAL_TYPE_JCEKS_LEN =
    std::strlen(IMPALA_AI_CREDENTIAL_TYPE_JCEKS);

template <AiFunctions::AI_PLATFORM platform>
Status getAuthorizationHeader(string& authHeader, const std::string_view& api_key,
    const AiFunctions::AiFunctionsOptions& ai_options) {
  const char* header_prefix = nullptr;
  switch (platform) {
    case AiFunctions::AI_PLATFORM::OPEN_AI:
      header_prefix = AiFunctions::OPEN_AI_REQUEST_AUTH_HEADER;
      break;
    case AiFunctions::AI_PLATFORM::AZURE_OPEN_AI:
      header_prefix = AiFunctions::AZURE_OPEN_AI_REQUEST_AUTH_HEADER;
      break;
    case AiFunctions::AI_PLATFORM::GENERAL:
      // For the general platform, only support OPEN_AI api standard for now.
      if (ai_options.api_standard == AiFunctions::API_STANDARD::OPEN_AI) {
        header_prefix = AiFunctions::OPEN_AI_REQUEST_AUTH_HEADER;
        break;
      }
    default:
      DCHECK(false) << "AiGenerateTextInternal should only be called for Supported "
                       "Platforms and Standard";
      return Status(AiFunctions::AI_GENERATE_TXT_UNSUPPORTED_ENDPOINT_ERROR);
  }
  DCHECK(header_prefix != nullptr);
  authHeader = header_prefix;
  authHeader.append(api_key);
  return Status::OK();
}

static void ParseImpalaOptions(const StringVal& options, Document& document,
    AiFunctions::AiFunctionsOptions& result) {
  // If options is NULL or empty, return with defaults.
  if (options.is_null || options.len == 0) return;

  if (document.Parse(reinterpret_cast<const char*>(options.ptr), options.len)
          .HasParseError()) {
    std::stringstream ss;
    ss << "Error parsing impala options: " << reinterpret_cast<const char*>(options.ptr)
       << ", error code: " << document.GetParseError() << ", offset input "
       << document.GetErrorOffset();
    throw std::runtime_error(ss.str());
  }
  // Check for "api_standard" field.
  if (document.HasMember(IMPALA_AI_API_STANDARD_FIELD)
      && document[IMPALA_AI_API_STANDARD_FIELD].IsString()) {
    const char* api_standard_value = document[IMPALA_AI_API_STANDARD_FIELD].GetString();
    if (gstrncasestr(IMPALA_AI_API_STANDARD_OPENAI, api_standard_value,
            IMPALA_AI_API_STANDARD_OPENAI_LEN) != nullptr) {
      result.api_standard = AiFunctions::API_STANDARD::OPEN_AI;
    } else {
      result.api_standard = AiFunctions::API_STANDARD::UNSUPPORTED;
    }
  }

  // Check for "credential_type" field.
  if (document.HasMember(IMPALA_AI_CREDENTIAL_TYPE_FIELD)
      && document[IMPALA_AI_CREDENTIAL_TYPE_FIELD].IsString()) {
    const char* credential_type_value =
        document[IMPALA_AI_CREDENTIAL_TYPE_FIELD].GetString();
    if (gstrncasestr(IMPALA_AI_CREDENTIAL_TYPE_PLAIN, credential_type_value,
            IMPALA_AI_CREDENTIAL_TYPE_PLAIN_LEN) != nullptr) {
      result.credential_type = AiFunctions::CREDENTIAL_TYPE::PLAIN;
    } else if (gstrncasestr(IMPALA_AI_CREDENTIAL_TYPE_JCEKS, credential_type_value,
                   IMPALA_AI_CREDENTIAL_TYPE_JCEKS_LEN) != nullptr) {
      result.credential_type = AiFunctions::CREDENTIAL_TYPE::JCEKS;
    }
  }

  // Check for "payload" field.
  if (document.HasMember(IMPALA_AI_PAYLOAD_FIELD)
      && document[IMPALA_AI_PAYLOAD_FIELD].IsString()) {
    const char* payload_value = document[IMPALA_AI_PAYLOAD_FIELD].GetString();
    result.ai_custom_payload = std::string_view(payload_value);
    // Check if payload exceeds the maximum allowed length of custom payload.
    if (result.ai_custom_payload.length() > MAX_CUSTOM_PAYLOAD_LENGTH) {
      std::stringstream ss;
      ss << "Error: custom payload can't be longer than " << MAX_CUSTOM_PAYLOAD_LENGTH
         << " bytes. Current length: " << result.ai_custom_payload.length();
      result.ai_custom_payload = std::string_view();
      throw std::runtime_error(ss.str());
    }
  }
}

template <bool fastpath, AiFunctions::AI_PLATFORM platform>
StringVal AiFunctions::AiGenerateTextInternal(FunctionContext* ctx,
    const std::string_view& endpoint_sv, const StringVal& prompt, const StringVal& model,
    const StringVal& auth_credential, const StringVal& platform_params,
    const StringVal& impala_options, const bool dry_run) {
  // Generate the header for the POST request
  vector<string> headers;
  headers.emplace_back(OPEN_AI_REQUEST_FIELD_CONTENT_TYPE_HEADER);
  string authHeader;
  AiFunctions::AiFunctionsOptions ai_options;
  Document impala_options_document;

  if (!fastpath) {
    try {
      ParseImpalaOptions(impala_options, impala_options_document, ai_options);
    } catch (const std::runtime_error& e) {
      LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR << ": " << e.what();
      return StringVal(AI_GENERATE_TXT_JSON_PARSE_ERROR.c_str());
    }
  }

  if (!fastpath && auth_credential.ptr != nullptr && auth_credential.len != 0) {
    if (ai_options.credential_type == CREDENTIAL_TYPE::PLAIN) {
      // Use the credential as a plain text token.
      std::string_view token(
          reinterpret_cast<char*>(auth_credential.ptr), auth_credential.len);
      RETURN_STRINGVAL_IF_ERROR(
          ctx, getAuthorizationHeader<platform>(authHeader, token, ai_options));
    } else {
      DCHECK(ai_options.credential_type == CREDENTIAL_TYPE::JCEKS);
      // Use the credential as JCEKS secret and fetch API key.
      string api_key;
      string api_key_secret(
          reinterpret_cast<char*>(auth_credential.ptr), auth_credential.len);
      RETURN_STRINGVAL_IF_ERROR(ctx,
          ExecEnv::GetInstance()->frontend()->GetSecretFromKeyStore(
              api_key_secret, &api_key));
      RETURN_STRINGVAL_IF_ERROR(
          ctx, getAuthorizationHeader<platform>(authHeader, api_key, ai_options));
    }
  } else {
    RETURN_STRINGVAL_IF_ERROR(
        ctx, getAuthorizationHeader<platform>(authHeader, ai_api_key_, ai_options));
  }
  headers.emplace_back(authHeader);

  string payload_str;
  if (!fastpath && !ai_options.ai_custom_payload.empty()) {
    payload_str =
        string(ai_options.ai_custom_payload.data(), ai_options.ai_custom_payload.size());
  } else {
    // Generate the payload for the POST request
    Document payload;
    payload.SetObject();
    Document::AllocatorType& payload_allocator = payload.GetAllocator();
    // Azure Open AI endpoint doesn't expect model as a separate param since it's
    // embedded in the endpoint. The 'deployment_name' below maps to a model.
    // https://<resource_name>.openai.azure.com/openai/deployments/<deployment_name>/..
    if (platform != AI_PLATFORM::AZURE_OPEN_AI) {
      if (!fastpath && model.ptr != nullptr && model.len != 0) {
        payload.AddMember("model",
            rapidjson::StringRef(reinterpret_cast<char*>(model.ptr), model.len),
            payload_allocator);
      } else {
        payload.AddMember("model",
            rapidjson::StringRef(FLAGS_ai_model.c_str(), FLAGS_ai_model.length()),
            payload_allocator);
      }
    }
    Value message_array(rapidjson::kArrayType);
    Value message(rapidjson::kObjectType);
    message.AddMember("role", "user", payload_allocator);
    if (prompt.ptr == nullptr || prompt.len == 0) {
      return StringVal(AI_GENERATE_TXT_INVALID_PROMPT_ERROR.c_str());
    }
    message.AddMember("content",
        rapidjson::StringRef(reinterpret_cast<char*>(prompt.ptr), prompt.len),
        payload_allocator);
    message_array.PushBack(message, payload_allocator);
    payload.AddMember("messages", message_array, payload_allocator);
    // Override additional platform params.
    // Caution: 'payload' might reference data owned by 'overrides'.
    // To ensure valid access, place 'overrides' outside the 'if'
    // statement before using 'payload'.
    Document overrides;
    if (!fastpath && platform_params.ptr != nullptr && platform_params.len != 0) {
      overrides.Parse(reinterpret_cast<char*>(platform_params.ptr), platform_params.len);
      if (overrides.HasParseError()) {
        LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR << ": error code "
                     << overrides.GetParseError() << ", offset input "
                     << overrides.GetErrorOffset();
        return StringVal(AI_GENERATE_TXT_JSON_PARSE_ERROR.c_str());
      }
      for (auto& m : overrides.GetObject()) {
        if (payload.HasMember(m.name.GetString())) {
          if (m.name == "messages") {
            LOG(WARNING)
                << AI_GENERATE_TXT_JSON_PARSE_ERROR
                << ": 'messages' is constructed from 'prompt', cannot be overridden";
            return StringVal(AI_GENERATE_TXT_MSG_OVERRIDE_FORBIDDEN_ERROR.c_str());
          } else {
            payload[m.name.GetString()] = m.value;
          }
        } else {
          if ((m.name == "n") && !(m.value.IsInt() && m.value.GetInt() == 1)) {
            LOG(WARNING) << AI_GENERATE_TXT_JSON_PARSE_ERROR
                         << ": 'n' must be of integer type and have value 1";
            return StringVal(AI_GENERATE_TXT_N_OVERRIDE_FORBIDDEN_ERROR.c_str());
          }
          payload.AddMember(m.name, m.value, payload_allocator);
        }
      }
    }
    // Convert payload into string for POST request
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    payload.Accept(writer);
    payload_str = string(buffer.GetString(), buffer.GetSize());
  }
  DCHECK(!payload_str.empty());
  VLOG(2) << "AI Generate Text: \nendpoint: " << endpoint_sv
          << " \npayload: " << payload_str;
  if (UNLIKELY(dry_run)) {
    std::stringstream post_request;
    post_request << endpoint_sv;
    for (auto& header : headers) {
      post_request << "\n" << header;
    }
    post_request << "\n" << payload_str;
    return StringVal::CopyFrom(ctx,
        reinterpret_cast<const uint8_t*>(post_request.str().data()),
        post_request.str().length());
  }
  // Send request to external AI API endpoint
  kudu::EasyCurl curl;
  curl.set_timeout(kudu::MonoDelta::FromSeconds(FLAGS_ai_connection_timeout_s));
  curl.set_fail_on_http_error(true);
  kudu::faststring resp;
  kudu::Status status;
  if (fastpath) {
    DCHECK_EQ(std::string_view(FLAGS_ai_endpoint), endpoint_sv);
    status = curl.PostToURL(FLAGS_ai_endpoint, payload_str, &resp, headers);
  } else {
    std::string endpoint_str{endpoint_sv};
    status = curl.PostToURL(endpoint_str, payload_str, &resp, headers);
  }
  VLOG(2) << "AI Generate Text: \noriginal response: " << resp.ToString();
  if (UNLIKELY(!status.ok())) return copyErrorMessage(ctx, status.ToString());
  // Parse the JSON response string
  std::string response = AiGenerateTextParseOpenAiResponse(
      std::string_view(reinterpret_cast<char*>(resp.data()), resp.size()));
  VLOG(2) << "AI Generate Text: \nresponse: " << response;
  StringVal result(ctx, response.length());
  if (UNLIKELY(result.is_null)) return StringVal::null();
  memcpy(result.ptr, response.data(), response.length());
  return result;
}

} // namespace impala
