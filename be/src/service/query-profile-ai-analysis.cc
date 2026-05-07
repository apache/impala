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

#include "service/query-profile-ai-analysis.h"
#include "service/query-profile-redaction.h"
#include "service/query-profile-parsing-tools.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <rapidjson/document.h>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "exprs/ai-functions.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "udf/udf-internal.h"
#include "util/gflag-validator-util.h"
#include "util/json-util.h"
#include "util/parse-util.h"
#include "util/scope-exit-trigger.h"

DECLARE_string(ai_model);

static bool ValidateQueryProfileAiAnalysisSizeLimitPercent(
    const char* flagname, const std::string& value) {
  bool is_percent = false;
  int64_t parsed = impala::ParseUtil::ParseMemSpec(value, &is_percent, 100);
  if (is_percent && parsed > 0 && parsed <= 100) return true;
  LOG(ERROR) << "Invalid value for --" << flagname << ": '" << value
             << "'. Must be a percentage in range (0%, 100%].";
  return false;
}

DEFINE_int64(query_profile_ai_analysis_profile_size_limit_bytes,
    256L * 1024L * 1024L,
    "Maximum profile size in bytes allowed for query-profile AI analysis modules.");
DEFINE_validator(query_profile_ai_analysis_profile_size_limit_bytes, gt_zero);

DEFINE_string(query_profile_ai_analysis_profile_size_limit_percent, "1%",
    "Maximum profile size as percentage of process_mem_limit for query-profile AI "
    "analysis modules (e.g. 5%).");
DEFINE_validator(query_profile_ai_analysis_profile_size_limit_percent,
    ValidateQueryProfileAiAnalysisSizeLimitPercent);

DEFINE_int32(query_profile_ai_analysis_max_concurrent_runs, 3,
    "Maximum number of concurrent query-profile AI analysis runs.");
DEFINE_validator(query_profile_ai_analysis_max_concurrent_runs, ge_zero);

using impala_udf::FunctionContext;
using impala_udf::StringVal;
using rapidjson::Document;
using rapidjson::Value;
using namespace std;

namespace impala {

static constexpr int MAX_AGENT_ITERATIONS = 12;
static constexpr size_t MAX_TOOL_RESULT_CHARS = 100000;
static mutex num_profiles_being_analyzed_lock;
static int32_t num_profiles_being_analyzed = 0;
static constexpr const char JSON_KEY_ARGUMENTS[] = "arguments";
static constexpr const char JSON_KEY_CHOICES[] = "choices";
static constexpr const char JSON_KEY_CONTENT[] = "content";
static constexpr const char JSON_KEY_ERROR[] = "error";
static constexpr const char JSON_KEY_FUNCTION[] = "function";
static constexpr const char JSON_KEY_ID[] = "id";
static constexpr const char JSON_KEY_MESSAGE[] = "message";
static constexpr const char JSON_KEY_MESSAGES[] = "messages";
static constexpr const char JSON_KEY_MODEL[] = "model";
static constexpr const char JSON_KEY_NAME[] = "name";
static constexpr const char JSON_KEY_OUTPUT_TEXT[] = "output_text";
static constexpr const char JSON_KEY_PARALLEL_TOOL_CALLS[] = "parallel_tool_calls";
static constexpr const char JSON_KEY_PAYLOAD[] = "payload";
static constexpr const char JSON_KEY_RESULT[] = "result";
static constexpr const char JSON_KEY_ROLE[] = "role";
static constexpr const char JSON_KEY_SUMMARY[] = "summary";
static constexpr const char JSON_KEY_TEXT[] = "text";
static constexpr const char JSON_KEY_TOOL_CALL_ID[] = "tool_call_id";
static constexpr const char JSON_KEY_TOOL_CALLS[] = "tool_calls";
static constexpr const char JSON_KEY_TOOL_CHOICE[] = "tool_choice";
static constexpr const char JSON_KEY_TOOL_NAME[] = "tool_name";
static constexpr const char JSON_KEY_TOOLS[] = "tools";
static constexpr const char JSON_KEY_VALUE[] = "value";

static constexpr const char ROLE_ASSISTANT[] = "assistant";
static constexpr const char ROLE_SYSTEM[] = "system";
static constexpr const char ROLE_TOOL[] = "tool";
static constexpr const char ROLE_USER[] = "user";
static constexpr const char TOOL_CHOICE_AUTO[] = "auto";

// Attempts to reserve one slot for a profile AI analysis run.
static bool TryAcquireAiAnalysisSlot() {
  lock_guard<mutex> l(num_profiles_being_analyzed_lock);
  if (num_profiles_being_analyzed
      >= FLAGS_query_profile_ai_analysis_max_concurrent_runs) {
    return false;
  }
  ++num_profiles_being_analyzed;
  return true;
}

static void ReleaseAiAnalysisSlot() {
  lock_guard<mutex> l(num_profiles_being_analyzed_lock);
  if (num_profiles_being_analyzed <= 0) {
    DCHECK_GT(num_profiles_being_analyzed, 0);
    return;
  }
  --num_profiles_being_analyzed;
}

// Computes the profile size limit passed to profile redaction/parsing:
// min(configured byte limit, configured percentage of process_mem_limit).
static Status ComputeProfileAnalysisSizeLimitBytes(int64_t* profile_size_limit_bytes) {
  if (profile_size_limit_bytes == nullptr) {
    return Status("profile size limit output pointer cannot be null");
  }
  const int64_t byte_limit = FLAGS_query_profile_ai_analysis_profile_size_limit_bytes;

  ExecEnv* exec_env = ExecEnv::GetInstance();
  if (exec_env == nullptr || exec_env->process_mem_tracker() == nullptr) {
    *profile_size_limit_bytes = byte_limit;
    return Status::OK();
  }
  const int64_t process_mem_limit = exec_env->process_mem_tracker()->limit();
  if (process_mem_limit <= 0) {
    *profile_size_limit_bytes = byte_limit;
    return Status::OK();
  }

  bool is_percent = false;
  int64_t percent_limit_bytes = ParseUtil::ParseMemSpec(
      FLAGS_query_profile_ai_analysis_profile_size_limit_percent, &is_percent,
      process_mem_limit);
  if (!is_percent || percent_limit_bytes <= 0) {
    return Status(strings::Substitute(
        "Invalid --query_profile_ai_analysis_profile_size_limit_percent: '$0'",
        FLAGS_query_profile_ai_analysis_profile_size_limit_percent));
  }
  *profile_size_limit_bytes = min(byte_limit, percent_limit_bytes);
  return Status::OK();
}

static constexpr char SYSTEM_INSTRUCTION[] =
    "You are an Impala engineer who is analyzing a query profile to identify "
    "performance bottlenecks. Out of the potential query bottleneck sources "
    "identified, prove or disprove each one using evidence from the Impala Query "
    "Profile until you are left with the only answer that is plausible. Never make "
    "any assumptions when thinking, instead always make a hypothesis and confirm it "
    "using hard evidence before moving on. Drill down into profile details as needed "
    "and feel free to perform multiple rounds of tool calls. Avoid finalizing root "
    "cause from overview/summary tools alone. Ensure that the plan is optimal for "
    "the given query. Ensure that the statistics are computed and available. "
    "Evaluate any sources of bad memory and cardinality estimates and consider them "
    "during overall bottleneck analysis. Your response should include all the "
    "hypotheses you create along the way along with a piece of evidence from the profile "
    "that justifies why each hypothesis and your final conclusion was true or false."
    "\n\n"
    "Use native tool calling for every non-final step. If you need more "
    "evidence, you MUST return tool_calls (not prose) and you may include "
    "multiple independent tool calls in the same response. Do not describe "
    "intended tool calls in text. Only provide final plain-text "
    "analysis (not Markdown) when no additional tools are needed.";

static const char* ANALYSIS_PROMPT = "Summarize this Impala Query Profile, identify any "
                                     "performance bottlenecks, and identify "
                                     "the sources of these bottlenecks.";

// OpenAI-style tool definitions
static constexpr char PROFILE_TOOLS_JSON[] = R"PT([
  {
    "type": "function",
    "function": {
      "name": "get_summary",
      "description": "Get the Summary section of the Impala query profile, )PT"
R"PT(including query metadata, options, and top-level fields.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_timeline",
      "description": "Get the Query Timeline section detailing the execution schedule.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_compilation",
      "description": "Get the Query Compilation section timeline.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_execution_profile",
      "description": "Get the complete Execution Profile section including )PT"
R"PT(per-node profiles.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_fragments_overview",
      "description": "Get a condensed array of all fragments and their )PT"
R"PT(execution summaries to triage bottlenecks.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_fragment",
      "description": "Get ONLY the 'Averaged Fragment' section for a )PT"
R"PT(specific fragment. Best for high-level analysis of a fragment's operators.",
      "parameters": {
        "type": "object",
        "properties": {
          "fragment_id": {
            "type": "string",
            "description": "Fragment ID to retrieve (MUST be a string, e.g., 'F00', '03')"
          }
        },
        "required": [
          "fragment_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_all_fragments",
      "description": "Get all block variants (Averaged, Coordinator, and )PT"
R"PT(standard Fragment blocks) for a fragment_id.",
      "parameters": {
        "type": "object",
        "properties": {
          "fragment_id": {
            "type": "string",
            "description": "Fragment ID to retrieve (MUST be a string, e.g., 'F00', '03')"
          }
        },
        "required": [
          "fragment_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_fragment_instances",
      "description": "Get the individual backend execution 'Instance' )PT"
R"PT(blocks for a specific fragment. Use this to look for data skew or )PT"
R"PT(straggler nodes within a fragment.",
      "parameters": {
        "type": "object",
        "properties": {
          "fragment_id": {
            "type": "string",
            "description": "Fragment ID to retrieve instances for (MUST be a )PT"
R"PT(string, e.g., 'F00', '03')"
          }
        },
        "required": [
          "fragment_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_operator",
      "description": "Fuzzy search to find an operator by node_id. )PT"
R"PT(Returns the best match found in the profile.",
      "parameters": {
        "type": "object",
        "properties": {
          "node_id": {
            "type": "string",
            "description": "Numeric Node ID to retrieve (MUST be a string, )PT"
R"PT(e.g., '00', '04', '11')"
          },
          "fragment_id": {
            "type": "string",
            "description": "Optional Fragment ID to scope the search (e.g., 'F03')"
          }
        },
        "required": [
          "node_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_average_operator",
      "description": "Get ONLY the averaged metric block for a specific )PT"
R"PT(operator across all hosts. This is the recommended primary tool for )PT"
R"PT(analyzing operator performance.",
      "parameters": {
        "type": "object",
        "properties": {
          "node_id": {
            "type": "string",
            "description": "Numeric Node ID to retrieve (MUST be a string, )PT"
R"PT(e.g., '00', '04', '11')"
          }
        },
        "required": [
          "node_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_all_operators",
      "description": "Get an array of operator metrics from EVERY )PT"
R"PT(individual instance executing this node_id.",
      "parameters": {
        "type": "object",
        "properties": {
          "node_id": {
            "type": "string",
            "description": "Numeric Node ID to retrieve (MUST be a )PT"
R"PT(string, e.g., '00', '04')"
          },
          "fragment_id": {
            "type": "string",
            "description": "Fragment ID the node belongs to (MUST be a )PT"
R"PT(string, e.g., 'F03')"
          }
        },
        "required": [
          "node_id",
          "fragment_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_specific_operator",
      "description": "Get operator metrics for one exact backend )PT"
R"PT(instance to diagnose a straggler.",
      "parameters": {
        "type": "object",
        "properties": {
          "node_id": {
            "type": "string",
            "description": "Numeric Node ID to retrieve (MUST be a )PT"
R"PT(string, e.g., '00', '04')"
          },
          "fragment_id": {
            "type": "string",
            "description": "Fragment ID to scope search (MUST be a string, e.g., 'F03')"
          },
          "instance_id": {
            "type": "string",
            "description": "Full Instance identifier (e.g., )PT"
R"PT('4a4f3a5058de2282:4959cbc300000005')"
          }
        },
        "required": [
          "node_id",
          "fragment_id",
          "instance_id"
        ]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_scan_nodes_summary",
      "description": "Get a condensed summary of all SCAN nodes (HDFS, )PT"
R"PT(S3, KUDU) in the query.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_per_node_profiles",
      "description": "Get the Per Node Profiles section containing )PT"
R"PT(host-level resource utilization.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_query_options",
      "description": "Get parsed key/value pairs of the query options )PT"
R"PT((e.g. MT_DOP, RUNTIME_FILTERS) used.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_tables_queried",
      "description": "Get an array of the fully qualified tables queried.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_resource_estimates",
      "description": "Get peak memory and resource estimation metadata from the profile.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "get_analyzed_query",
      "description": "Get the expanded, analyzed SQL query text )PT"
R"PT(containing rewritten joins and predicates.",
      "parameters": {
        "type": "object",
        "properties": {},
        "required": []
      }
    }
  }
])PT";

struct ChatMessage {
  string role;
  string content;
  string tool_call_id;
  string tool_calls_json;

  ChatMessage() = default;
  ChatMessage(string role, string content, string tool_call_id, string tool_calls_json)
      : role(move(role)),
        content(move(content)),
        tool_call_id(move(tool_call_id)),
        tool_calls_json(move(tool_calls_json)) {}
};

// Truncates oversized tool output so model payloads stay bounded.
static void TruncateToolOutputIfNeeded(string* tool_json) {
  DCHECK(tool_json != nullptr);
  if (tool_json->size() <= MAX_TOOL_RESULT_CHARS) return;
  tool_json->resize(MAX_TOOL_RESULT_CHARS);
  tool_json->append("\n... [TRUNCATED]");
}

static Value MakeJsonStringValue(string_view value, Document::AllocatorType& alloc) {
  Value json_value;
  json_value.SetString(
      value.data(), static_cast<rapidjson::SizeType>(value.size()), alloc);
  return json_value;
}

static const Document& GetParsedProfileToolsDocument() {
  static const Document parsed_tools_doc = [] {
    Document doc;
    doc.Parse(PROFILE_TOOLS_JSON,
        static_cast<rapidjson::SizeType>(sizeof(PROFILE_TOOLS_JSON) - 1));
    return doc;
  }();
  return parsed_tools_doc;
}

// Parses a model JSON response and tolerates non-JSON prefixes/suffixes.
static bool ParseJsonObjectFromResponse(string_view response, Document* response_doc) {
  DCHECK(response_doc != nullptr);
  if (response_doc == nullptr) return false;
  response_doc->Parse(response.data(), response.size());
  if (!response_doc->HasParseError() && response_doc->IsObject()) return true;

  const size_t first_brace = response.find('{');
  const size_t last_brace = response.rfind('}');
  if (first_brace == string_view::npos || last_brace == string_view::npos
      || last_brace <= first_brace) {
    return false;
  }
  const string_view candidate =
      response.substr(first_brace, last_brace - first_brace + 1);
  response_doc->Parse(candidate.data(), candidate.size());
  return !response_doc->HasParseError() && response_doc->IsObject();
}

// Returns the assistant message object from common response envelope shapes.
static const Value* FindAssistantMessageInResponse(const Document& response_doc) {
  if (response_doc.HasMember(JSON_KEY_ROLE) && response_doc[JSON_KEY_ROLE].IsString()) {
    return &response_doc;
  }
  if (response_doc.HasMember(JSON_KEY_TOOL_CALLS)
      || response_doc.HasMember(JSON_KEY_CONTENT)) {
    return &response_doc;
  }
  if (response_doc.HasMember(JSON_KEY_MESSAGE)
      && response_doc[JSON_KEY_MESSAGE].IsObject()) {
    return &response_doc[JSON_KEY_MESSAGE];
  }
  if (response_doc.HasMember(JSON_KEY_CHOICES)
      && response_doc[JSON_KEY_CHOICES].IsArray()) {
    const Value& choices = response_doc[JSON_KEY_CHOICES];
    if (!choices.Empty() && choices[0].IsObject()
        && choices[0].HasMember(JSON_KEY_MESSAGE)
        && choices[0][JSON_KEY_MESSAGE].IsObject()) {
      return &choices[0][JSON_KEY_MESSAGE];
    }
  }
  return nullptr;
}

// Extracts assistant content text from string or content-part array formats.
static bool TryExtractAssistantContentText(
    const Value* assistant_message, string* assistant_content) {
  DCHECK(assistant_content != nullptr);
  if (assistant_message == nullptr) return false;
  if (assistant_content == nullptr) return false;
  assistant_content->clear();
  if (!assistant_message->HasMember(JSON_KEY_CONTENT)) return false;

  const Value& content = (*assistant_message)[JSON_KEY_CONTENT];
  if (content.IsString()) {
    assistant_content->assign(content.GetString(), content.GetStringLength());
    return true;
  }
  if (!content.IsArray()) return false;

  for (rapidjson::SizeType i = 0; i < content.Size(); ++i) {
    const Value& part = content[i];
    if (!part.IsObject()) continue;

    const Value* text_field = nullptr;
    if (part.HasMember(JSON_KEY_TEXT) && part[JSON_KEY_TEXT].IsString()) {
      text_field = &part[JSON_KEY_TEXT];
    } else if (part.HasMember(JSON_KEY_OUTPUT_TEXT)
        && part[JSON_KEY_OUTPUT_TEXT].IsString()) {
      text_field = &part[JSON_KEY_OUTPUT_TEXT];
    } else if (part.HasMember(JSON_KEY_TEXT) && part[JSON_KEY_TEXT].IsObject()
        && part[JSON_KEY_TEXT].HasMember(JSON_KEY_VALUE)
        && part[JSON_KEY_TEXT][JSON_KEY_VALUE].IsString()) {
      text_field = &part[JSON_KEY_TEXT][JSON_KEY_VALUE];
    }
    if (text_field == nullptr) continue;
    if (!assistant_content->empty()) assistant_content->append("\n");
    assistant_content->append(text_field->GetString(), text_field->GetStringLength());
  }
  return !assistant_content->empty();
}

namespace test {

bool ParseJsonObjectFromResponse(
    string_view response, Document* response_doc) {
  return impala::ParseJsonObjectFromResponse(response, response_doc);
}

const Value* FindAssistantMessageInResponse(const Document& response_doc) {
  return impala::FindAssistantMessageInResponse(response_doc);
}

bool TryExtractAssistantContentText(
    const Value* assistant_message, string* assistant_content) {
  return impala::TryExtractAssistantContentText(assistant_message, assistant_content);
}

bool TryAcquireAiAnalysisSlot() {
  return impala::TryAcquireAiAnalysisSlot();
}

void ReleaseAiAnalysisSlot() {
  impala::ReleaseAiAnalysisSlot();
}

} // namespace test

// Builds the model request payload from the current conversation state.
static Status BuildAgentPayloadJson(
    const vector<ChatMessage>& conversation, string* payload_json) {
  if (payload_json == nullptr) return Status("payload output pointer cannot be null");

  const Document& parsed_tools_doc = GetParsedProfileToolsDocument();
  if (parsed_tools_doc.HasParseError() || !parsed_tools_doc.IsArray()) {
    return Status("Failed to parse PROFILE_TOOLS_JSON for AI analysis");
  }

  Document payload_doc;
  payload_doc.SetObject();
  Document::AllocatorType& alloc = payload_doc.GetAllocator();
  payload_doc.AddMember(JSON_KEY_MODEL, Value(FLAGS_ai_model.c_str(), alloc), alloc);
  Value tools_value(parsed_tools_doc, alloc);
  payload_doc.AddMember(JSON_KEY_TOOLS, tools_value, alloc);
  payload_doc.AddMember(JSON_KEY_TOOL_CHOICE, TOOL_CHOICE_AUTO, alloc);
  payload_doc.AddMember(JSON_KEY_PARALLEL_TOOL_CALLS, true, alloc);

  Value messages(rapidjson::kArrayType);
  {
    Value system_msg(rapidjson::kObjectType);
    system_msg.AddMember(JSON_KEY_ROLE, ROLE_SYSTEM, alloc);
    system_msg.AddMember(
        JSON_KEY_CONTENT, rapidjson::StringRef(SYSTEM_INSTRUCTION), alloc);
    messages.PushBack(system_msg, alloc);
  }

  for (const ChatMessage& msg : conversation) {
    string_view role = msg.role;
    if (role != ROLE_USER && role != ROLE_ASSISTANT
        && role != ROLE_SYSTEM && role != ROLE_TOOL) {
      role = ROLE_USER;
    }
    Value json_msg(rapidjson::kObjectType);
    json_msg.AddMember(JSON_KEY_ROLE, MakeJsonStringValue(role, alloc), alloc);
    if (role == ROLE_ASSISTANT && !msg.tool_calls_json.empty()) {
      Document tool_calls_doc;
      tool_calls_doc.Parse(msg.tool_calls_json.c_str());
      if (!tool_calls_doc.HasParseError() && tool_calls_doc.IsArray()) {
        Value tool_calls_value(tool_calls_doc, alloc);
        json_msg.AddMember(JSON_KEY_TOOL_CALLS, tool_calls_value, alloc);
      }
      if (msg.content.empty()) {
        Value null_content;
        null_content.SetNull();
        json_msg.AddMember(JSON_KEY_CONTENT, null_content, alloc);
      } else {
        json_msg.AddMember(
            JSON_KEY_CONTENT, MakeJsonStringValue(msg.content, alloc), alloc);
      }
    } else {
      json_msg.AddMember(
          JSON_KEY_CONTENT, MakeJsonStringValue(msg.content, alloc), alloc);
      if (role == ROLE_TOOL && !msg.tool_call_id.empty()) {
        json_msg.AddMember(
            JSON_KEY_TOOL_CALL_ID, MakeJsonStringValue(msg.tool_call_id, alloc), alloc);
      }
    }
    messages.PushBack(json_msg, alloc);
  }
  payload_doc.AddMember(JSON_KEY_MESSAGES, messages, alloc);

  *payload_json = JsonToString(payload_doc);
  return Status::OK();
}

// Calls ai_generate_text with the provided serialized options payload.
Status CallAiGenerateText(string_view impala_options_json, string* response) {
  if (response == nullptr) return Status("response output pointer cannot be null");
  MemTracker tracker(-1, "QueryProfileAiAnalysis",
      ExecEnv::GetInstance()->process_mem_tracker());
  MemPool udf_pool(&tracker);
  MemPool result_pool(&tracker);
  const auto cleanup = MakeScopeExitTrigger([&]() {
    result_pool.FreeAll();
    udf_pool.FreeAll();
    tracker.Close();
  });
  FunctionContext::TypeDesc str_type;
  str_type.type = FunctionContext::TYPE_STRING;
  str_type.precision = 0;
  str_type.scale = 0;
  str_type.len = 0;
  vector<FunctionContext::TypeDesc> arg_types(7, str_type);
  FunctionContext* ctx = FunctionContextImpl::CreateContext(
      nullptr, &udf_pool, &result_pool, str_type, arg_types);
  if (ctx == nullptr) {
    return Status("Failed to create function context for AI analysis");
  }
  const auto ctx_cleanup = MakeScopeExitTrigger([&]() {
    ctx->impl()->Close();
    delete ctx;
  });

  const StringVal null_arg = StringVal::null();
  StringVal impala_options_val(reinterpret_cast<uint8_t*>(
                                   const_cast<char*>(impala_options_json.data())),
      impala_options_json.size());
  StringVal result = AiFunctions::AiGenerateText(
      ctx, null_arg, null_arg, null_arg, null_arg, null_arg, impala_options_val);
  if (ctx->has_error()) {
    string err =
        ctx->error_msg() == nullptr ? "Unknown AI function error" : ctx->error_msg();
    return Status(err);
  }
  if (result.is_null) {
    return Status("AI function returned null response");
  }
  response->assign(reinterpret_cast<char*>(result.ptr), result.len);
  return Status::OK();
}

// Runs an iterative tool-calling loop until a final analysis response is produced.
Status RunAiAgentWithToolExecutor(string_view initial_summary_json,
    const AiToolExecutor& tool_executor, string* analysis) {
  if (analysis == nullptr) return Status("analysis output pointer cannot be null");
  if (!tool_executor) return Status("tool executor cannot be empty");

  vector<ChatMessage> conversation;
  conversation.reserve(MAX_AGENT_ITERATIONS * 2);
  string user_message;
  user_message.reserve(
      strlen(ANALYSIS_PROMPT) + strlen("\n\nInitial profile context:\n")
      + initial_summary_json.size());
  user_message.append(ANALYSIS_PROMPT)
      .append("\n\nInitial profile context:\n")
      .append(initial_summary_json.data(), initial_summary_json.size());
  conversation.emplace_back(ROLE_USER, move(user_message), "", "");

  string last_response;
  for (int iteration = 0; iteration < MAX_AGENT_ITERATIONS; ++iteration) {
    string payload_json;
    RETURN_IF_ERROR(BuildAgentPayloadJson(conversation, &payload_json));

    Document impala_options_doc;
    impala_options_doc.SetObject();
    Document::AllocatorType& impala_options_alloc = impala_options_doc.GetAllocator();
    impala_options_doc.AddMember(JSON_KEY_PAYLOAD,
        rapidjson::StringRef(payload_json.c_str(),
            static_cast<rapidjson::SizeType>(payload_json.size())),
        impala_options_alloc);
    const string impala_options_json = JsonToString(impala_options_doc);

    string model_response;
    RETURN_IF_ERROR(CallAiGenerateText(impala_options_json, &model_response));
    last_response = move(model_response);

    Document response_doc;
    if (ParseJsonObjectFromResponse(last_response, &response_doc)) {
      const Value* assistant_message = FindAssistantMessageInResponse(response_doc);
      if (assistant_message != nullptr
          && assistant_message->HasMember(JSON_KEY_TOOL_CALLS)
          && (*assistant_message)[JSON_KEY_TOOL_CALLS].IsArray()) {
        const Value& tool_calls = (*assistant_message)[JSON_KEY_TOOL_CALLS];
        if (tool_calls.Empty()) {
          string assistant_content;
          if (TryExtractAssistantContentText(assistant_message, &assistant_content)) {
            conversation.emplace_back(ROLE_ASSISTANT, assistant_content, "", "");
            *analysis = move(assistant_content);
          } else {
            *analysis = last_response;
          }
          return Status::OK();
        }

        string assistant_content;
        TryExtractAssistantContentText(assistant_message, &assistant_content);
        conversation.emplace_back(ROLE_ASSISTANT, move(assistant_content), "",
            JsonToString(tool_calls));
        int executed_tool_calls = 0;
        for (rapidjson::SizeType i = 0; i < tool_calls.Size(); ++i) {
          const Value& tool_call = tool_calls[i];
          if (!tool_call.IsObject()) continue;
          if (!tool_call.HasMember(JSON_KEY_FUNCTION)
              || !tool_call[JSON_KEY_FUNCTION].IsObject()) {
            continue;
          }
          const Value& function = tool_call[JSON_KEY_FUNCTION];
          if (!function.HasMember(JSON_KEY_NAME) || !function[JSON_KEY_NAME].IsString()) {
            continue;
          }

          const Value& tool_name_value = function[JSON_KEY_NAME];
          const string_view tool_name(
              tool_name_value.GetString(), tool_name_value.GetStringLength());
          string_view args_json;
          if (function.HasMember(JSON_KEY_ARGUMENTS)
              && function[JSON_KEY_ARGUMENTS].IsString()) {
            const Value& arguments = function[JSON_KEY_ARGUMENTS];
            args_json = string_view(arguments.GetString(), arguments.GetStringLength());
          }

          string tool_call_id;
          if (tool_call.HasMember(JSON_KEY_ID) && tool_call[JSON_KEY_ID].IsString()) {
            const Value& id = tool_call[JSON_KEY_ID];
            tool_call_id.assign(id.GetString(), id.GetStringLength());
          }
          if (tool_call_id.empty()) {
            LOG(WARNING) << "Skipping tool call '" << tool_name
                         << "' because model response is missing tool_call id.";
            string retry_message = strings::Substitute(
                "The previous tool call for '$0' was ignored because tool_call id "
                "was missing. Retry with a valid tool_call id.",
                StringPiece(tool_name.data(), static_cast<int>(tool_name.size())));
            conversation.emplace_back(ROLE_USER, move(retry_message), "", "");
            continue;
          }

          string tool_json;
          Status tool_status = tool_executor(tool_name, args_json, &tool_json);
          if (!tool_status.ok()) {
            LOG(WARNING) << "Tool call '" << tool_name << "' failed: "
                         << tool_status.GetDetail();
            Document tool_error_doc;
            tool_error_doc.SetObject();
            Document::AllocatorType& error_alloc = tool_error_doc.GetAllocator();
            tool_error_doc.AddMember(
                JSON_KEY_ERROR, Value(tool_status.GetDetail().c_str(), error_alloc),
                error_alloc);
            tool_json = JsonToString(tool_error_doc);
          }
          TruncateToolOutputIfNeeded(&tool_json);
          conversation.emplace_back(ROLE_TOOL, move(tool_json), move(tool_call_id), "");
          ++executed_tool_calls;
        }
        if (executed_tool_calls == 0) {
          *analysis = "AI analysis could not execute requested tool calls.";
          return Status::OK();
        }
        continue;
      }

      string assistant_content;
      if (assistant_message != nullptr
          && TryExtractAssistantContentText(assistant_message, &assistant_content)) {
        conversation.emplace_back(ROLE_ASSISTANT, assistant_content, "", "");
        *analysis = move(assistant_content);
        return Status::OK();
      }
    }
    *analysis = last_response;
    return Status::OK();
  }

  Document response_doc;
  if (ParseJsonObjectFromResponse(last_response, &response_doc)) {
    const Value* assistant_message = FindAssistantMessageInResponse(response_doc);
    if (assistant_message != nullptr && assistant_message->HasMember(JSON_KEY_TOOL_CALLS)
        && (*assistant_message)[JSON_KEY_TOOL_CALLS].IsArray()
        && !(*assistant_message)[JSON_KEY_TOOL_CALLS].Empty()) {
      *analysis = "AI analysis did not complete within the tool-calling iteration "
                  "limit. Please retry.";
      return Status::OK();
    }
  }
  *analysis = last_response.empty() ? "AI analysis did not complete."
                                    : move(last_response);
  return Status::OK();
}

// Redacts profile input, runs AI analysis with profile tools, then unredacts output.
Status GenerateAiAnalysisFromProfile(const Value& profile_json, string* analysis) {
  if (analysis == nullptr) return Status("analysis output pointer cannot be null");
  if (FLAGS_query_profile_ai_analysis_max_concurrent_runs == 0) {
    return Status("Query profile AI analysis is disabled.");
  }
  if (!profile_json.IsObject()) {
    return Status("Query profile input must be a valid JSON object");
  }
  if (!TryAcquireAiAnalysisSlot()) {
    return Status(strings::Substitute(
        "Too many concurrent query-profile AI analysis runs. Limit: $0. "
        "Please retry.",
        FLAGS_query_profile_ai_analysis_max_concurrent_runs));
  }
  const auto slot_release = MakeScopeExitTrigger([]() { ReleaseAiAnalysisSlot(); });

  int64_t profile_size_limit_bytes = 0;
  RETURN_IF_ERROR(ComputeProfileAnalysisSizeLimitBytes(&profile_size_limit_bytes));

  QueryProfileRedactor redactor(profile_size_limit_bytes);
  RETURN_IF_ERROR(redactor.Redact(profile_json));
  const size_t redacted_profile_size_bytes =
      JsonToString(redactor.redacted_profile_json()).size();

  QueryProfileToolExecutor profile_tool_executor;
  RETURN_IF_ERROR(CreateQueryProfileToolExecutorForProfile(
      redactor.redacted_profile_json(), &profile_tool_executor,
      profile_size_limit_bytes, redacted_profile_size_bytes));

  Document summary_tool_output_doc;
  RETURN_IF_ERROR(profile_tool_executor("get_summary", "", &summary_tool_output_doc));
  Document initial_summary_doc;
  initial_summary_doc.SetObject();
  initial_summary_doc.AddMember(
      JSON_KEY_SUMMARY,
      Value(summary_tool_output_doc, initial_summary_doc.GetAllocator()),
      initial_summary_doc.GetAllocator());
  const string initial_summary_json = JsonToString(initial_summary_doc);

  RETURN_IF_ERROR(RunAiAgentWithToolExecutor(initial_summary_json,
      [&profile_tool_executor](
          string_view tool_name, string_view tool_args_json, string* tool_json) {
        if (tool_json == nullptr) {
          return Status("tool output pointer cannot be null");
        }
        Document tool_result_doc;
        RETURN_IF_ERROR(
            profile_tool_executor(tool_name, tool_args_json, &tool_result_doc));
        Document tool_doc;
        tool_doc.SetObject();
        tool_doc.AddMember(JSON_KEY_TOOL_NAME,
            MakeJsonStringValue(tool_name, tool_doc.GetAllocator()),
            tool_doc.GetAllocator());
        tool_doc.AddMember(JSON_KEY_RESULT,
            Value(tool_result_doc, tool_doc.GetAllocator()), tool_doc.GetAllocator());
        *tool_json = JsonToString(tool_doc);
        return Status::OK();
      }, analysis));
  *analysis = redactor.Unredact(*analysis);
  return Status::OK();
}

} // namespace impala
