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

#include <functional>
#include <string>
#include <string_view>

#include <rapidjson/document.h>

#include "common/status.h"

namespace impala {

// Calls the configured AI endpoint with Impala options JSON and returns
// raw response text.
Status CallAiGenerateText(std::string_view impala_options_json, std::string* response);

// Callback used by the AI loop to execute a named tool call and return tool output JSON.
using AiToolExecutor = std::function<Status(
    std::string_view tool_name, std::string_view tool_args_json, std::string* tool_json)>;

// Runs iterative AI analysis, allowing the model to issue tool calls until it returns
// final analysis text.
Status RunAiAgentWithToolExecutor(std::string_view initial_summary_json,
    const AiToolExecutor& tool_executor, std::string* analysis);

// Redacts profile input, runs AI analysis with profile tools, and unredacts the result.
Status GenerateAiAnalysisFromProfile(
    const rapidjson::Value& profile_json, std::string* analysis);

// Internal helper wrappers exposed for offline unit testing.
namespace test {
bool ParseJsonObjectFromResponse(
    std::string_view response, rapidjson::Document* response_doc);
const rapidjson::Value* FindAssistantMessageInResponse(
    const rapidjson::Document& response_doc);
bool TryExtractAssistantContentText(
    const rapidjson::Value* assistant_message, std::string* assistant_content);
bool TryAcquireAiAnalysisSlot();
void ReleaseAiAnalysisSlot();
} // namespace test

} // namespace impala
