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

#include <string_view>

#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::StringVal;

class AiFunctions {
 public:
  static const string AI_GENERATE_TXT_JSON_PARSE_ERROR;
  static const string AI_GENERATE_TXT_INVALID_PROTOCOL_ERROR;
  static const string AI_GENERATE_TXT_UNSUPPORTED_ENDPOINT_ERROR;
  static const string AI_GENERATE_TXT_INVALID_PROMPT_ERROR;
  static const string AI_GENERATE_TXT_MSG_OVERRIDE_FORBIDDEN_ERROR;
  static const string AI_GENERATE_TXT_N_OVERRIDE_FORBIDDEN_ERROR;
  static const char* OPEN_AI_REQUEST_FIELD_CONTENT_TYPE_HEADER;
  static const char* OPEN_AI_REQUEST_AUTH_HEADER;
  static const char* AZURE_OPEN_AI_REQUEST_AUTH_HEADER;
  enum class AI_PLATFORM {
    /// Unsupported platform
    UNSUPPORTED,
    /// OpenAI public platform
    OPEN_AI,
    /// Azure OpenAI platform
    AZURE_OPEN_AI,
    /// General AI platform
    GENERAL
  };
  enum class API_STANDARD {
    /// Unsupported standard
    UNSUPPORTED,
    /// OpenAI standard
    OPEN_AI
  };
  enum class CREDENTIAL_TYPE {
    /// Input credentials will be treated as plain text.
    PLAIN,
    /// Input credentials will be treated as a jceks secret.
    JCEKS
  };
  struct AiFunctionsOptions {
    // Default of api standard is OPEN_AI
    AiFunctions::API_STANDARD api_standard = AiFunctions::API_STANDARD::OPEN_AI;
    // Default of credential type is JCEKS.
    AiFunctions::CREDENTIAL_TYPE credential_type = AiFunctions::CREDENTIAL_TYPE::JCEKS;
    // Only valid when a customized payload is included in the request.
    std::string_view ai_custom_payload;
  };
  /// Sends a prompt to the input AI endpoint using the input model, authentication
  /// credential and optional platform params and impala options.
  /// platform_params (optional) are additional AI platform specific parameters included
  /// in the request sent to the AI model.
  /// impala_options (optional) are Impala API specific options i.e AiFunctionsOptions.
  static StringVal AiGenerateText(FunctionContext* ctx, const StringVal& endpoint,
      const StringVal& prompt, const StringVal& model, const StringVal& auth_credential,
      const StringVal& platform_params, const StringVal& impala_options);
  /// Sends a prompt to the default endpoint and uses the default model, default
  /// api-key and default platform params and impala options.
  static StringVal AiGenerateTextDefault(FunctionContext* ctx, const StringVal& prompt);
  /// Set the ai_api_key_ member.
  static void set_api_key(string& api_key) { ai_api_key_ = api_key; }
  /// Validate api end point.
  static bool is_api_endpoint_valid(const std::string_view& endpoint);
  /// Check if endpoint is supported
  static bool is_api_endpoint_supported(const std::string_view& endpoint);

 private:
  /// The default api_key used for communicating with external APIs.
  static std::string ai_api_key_;
  /// Internal function which implements the logic of parsing user input and sending
  /// request to the external API endpoint. If 'dry_run' is set, the POST request is
  /// returned. 'dry_run' mode is used only for unit tests.
  template <bool fastpath, AI_PLATFORM platform>
  static StringVal AiGenerateTextInternal(FunctionContext* ctx,
      const std::string_view& endpoint, const StringVal& prompt, const StringVal& model,
      const StringVal& auth_credential, const StringVal& platform_params,
      const StringVal& impala_options, const bool dry_run);
  /// Helper function for calling AiGenerateTextInternal with common code for both
  /// fastpath and regular path.
  template <bool fastpath>
  static StringVal AiGenerateTextHelper(FunctionContext* ctx, const StringVal& endpoint,
      const StringVal& prompt, const StringVal& model, const StringVal& auth_credential,
      const StringVal& platform_params, const StringVal& impala_options);
  /// Internal helper function for parsing OPEN AI's API response. Input parameter is the
  /// json representation of the OPEN AI's API response.
  static std::string AiGenerateTextParseOpenAiResponse(
      const std::string_view& reponse);
  /// Helper function for getting AI Platform from the endpoint
  static AI_PLATFORM GetAiPlatformFromEndpoint(
      const std::string_view& endpoint, const bool dry_run = false);
  /// Helper functions for deep copying error message
  static StringVal copyErrorMessage(FunctionContext* ctx, const string& errorMsg);

  friend class ExprTest_AiFunctionsTest_Test;
  friend class ExprTest_AiFunctionsTestAdditionalSites_Test;
};

} // namespace impala
