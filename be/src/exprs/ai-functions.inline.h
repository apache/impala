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

template <bool fastpath>
StringVal AiFunctions::AiGenerateTextInternal(FunctionContext* ctx,
    const StringVal& endpoint, const StringVal& prompt, const StringVal& model,
    const StringVal& api_key_jceks_secret, const StringVal& params, const bool dry_run) {
  std::string_view endpoint_sv(FLAGS_ai_endpoint);
  // endpoint validation
  if (!fastpath && endpoint.ptr != nullptr && endpoint.len != 0) {
    endpoint_sv = std::string_view(reinterpret_cast<char*>(endpoint.ptr), endpoint.len);
    // Simple validation for endpoint. It should start with https://
    if (!is_api_endpoint_valid(endpoint_sv)) {
      LOG(ERROR) << "AI Generate Text: \ninvalid protocol: " << endpoint_sv;
      return StringVal(AI_GENERATE_TXT_INVALID_PROTOCOL_ERROR.c_str());
    }
    // Only OpenAI endpoints are supported.
    if (!is_api_endpoint_supported(endpoint_sv)) {
      LOG(ERROR) << "AI Generate Text: \nunsupported endpoint: " << endpoint_sv;
      return StringVal(AI_GENERATE_TXT_UNSUPPORTED_ENDPOINT_ERROR.c_str());
    }
  }
  // Generate the header for the POST request
  vector<string> headers;
  headers.emplace_back(OPEN_AI_REQUEST_FIELD_CONTENT_TYPE_HEADER);
  if (!fastpath && api_key_jceks_secret.ptr != nullptr && api_key_jceks_secret.len != 0) {
    string api_key;
    string api_key_secret(
        reinterpret_cast<char*>(api_key_jceks_secret.ptr), api_key_jceks_secret.len);
    Status status = ExecEnv::GetInstance()->frontend()->GetSecretFromKeyStore(
        api_key_secret, &api_key);
    if (!status.ok()) {
      return StringVal::CopyFrom(ctx,
          reinterpret_cast<const uint8_t*>(status.msg().msg().c_str()),
          status.msg().msg().length());
    }
    headers.emplace_back("Authorization: Bearer " + api_key);
  } else {
    headers.emplace_back("Authorization: Bearer " + ai_api_key_);
  }
  // Generate the payload for the POST request
  Document payload;
  payload.SetObject();
  Document::AllocatorType& payload_allocator = payload.GetAllocator();
  if (!fastpath && model.ptr != nullptr && model.len != 0) {
    payload.AddMember("model",
        rapidjson::StringRef(reinterpret_cast<char*>(model.ptr), model.len),
        payload_allocator);
  } else {
    payload.AddMember("model",
        rapidjson::StringRef(FLAGS_ai_model.c_str(), FLAGS_ai_model.length()),
        payload_allocator);
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
  // Override additional params.
  // Caution: 'payload' might reference data owned by 'overrides'.
  // To ensure valid access, place 'overrides' outside the 'if'
  // statement before using 'payload'.
  Document overrides;
  if (!fastpath && params.ptr != nullptr && params.len != 0) {
    overrides.Parse(reinterpret_cast<char*>(params.ptr), params.len);
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
          LOG(WARNING)
              << AI_GENERATE_TXT_JSON_PARSE_ERROR
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
  string payload_str(buffer.GetString(), buffer.GetSize());
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
  if (!status.ok()) {
    string msg = status.ToString();
    return StringVal::CopyFrom(
        ctx, reinterpret_cast<const uint8_t*>(msg.c_str()), msg.size());
  }
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
