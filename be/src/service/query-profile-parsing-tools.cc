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

#include "service/query-profile-parsing-tools.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <regex>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <re2/re2.h>

#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "service/query-profile-size-limit-util.h"
#include "util/debug-util.h"
#include "util/json-util.h"
#include "util/pretty-printer.h"
#include "util/string-util.h"

using namespace std;
using strings::Substitute;
using rapidjson::Document;
using rapidjson::Value;

namespace impala {

static constexpr const char CONTENTS_KEY[] = "contents";
static constexpr const char PROFILE_NAME_KEY[] = "profile_name";
static constexpr const char CHILD_PROFILES_KEY[] = "child_profiles";
static constexpr const char INFO_STRINGS_KEY[] = "info_strings";
static constexpr const char NUM_CHILDREN_KEY[] = "num_children";
static constexpr const char VALUE_KEY[] = "_value";
static constexpr const char SUMMARY_KEY[] = "Summary";
static constexpr const char QUERY_TYPE_KEY[] = "Query Type";
static const string QUERY_TYPE_QUERY = PrintValue(TStmtType::QUERY);
static constexpr const char EVENT_SEQUENCES_KEY[] = "event_sequences";
static constexpr const char EVENTS_KEY[] = "events";
static constexpr const char EXECUTION_PROFILE_PREFIX[] = "Execution Profile";
static constexpr const char PER_NODE_PROFILES_KEY[] = "Per Node Profiles";
static constexpr const char ERROR_KEY[] = "error";
static constexpr const char INSTANCE_TOKEN[] = "Instance";
static constexpr const char FRAGMENT_ID_KEY[] = "fragment_id";
static constexpr const char NODE_ID_KEY[] = "node_id";
static constexpr const char INSTANCE_ID_KEY[] = "instance_id";
static constexpr const char MISSING_ARG_ERROR_MSG[] =
    "tool '$0' requires string argument '$1'";
static constexpr const char TOOL_UNKNOWN_PREFIX[] = "Unknown tool: ";
static constexpr int64_t DEFAULT_PARSING_PROFILE_SIZE_LIMIT_MAX_BYTES =
    256L * 1024L * 1024L;
static constexpr int64_t DEFAULT_PARSING_PROFILE_SIZE_LIMIT_PERCENTAGE = 1;

// Matches fragment ids like "F00", "F12", etc.
static const regex FID_RE(R"(F\d+)", regex::optimize);
// Captures reservation text after "Max Per-Host Resource Reservation:".
static const regex RES_RE(
    R"(Max Per-Host Resource Reservation:\s*(.+))", regex::optimize);
// Captures estimate text after "Per-Host Resource Estimates:".
static const regex EST_RE(R"(Per-Host Resource Estimates:\s*(.+))", regex::optimize);
// Captures the analyzed SQL between "Analyzed query:" and the first fragment section.
static const re2::RE2 ANALYZED_RE(
    R"(Analyzed query:\s*([\s\S]*?)\n\nF\d+:PLAN FRAGMENT)");
// Matches scan node keys like "1:SCAN ..." or "...SCAN_NODE" (case-insensitive).
static const regex SCAN_NODE_RE(
    R"((?:^\d+:\s*SCAN\b)|(?:\b[A-Z_]*SCAN_NODE\b))",
    regex::optimize | regex::icase | regex::nosubs);
// Captures query id from keys like "Query (id=...)".
static const regex QUERY_ID_RE(R"(^Query \(id=([^)]+)\):?$)", regex::optimize);
// Matches compact fragment prefixes like "F00" or "F12:" at string start.
static const regex FRAG_SHORT_RE(R"(^F\d+(\b|:))", regex::optimize | regex::nosubs);
// Captures hour components in duration strings like "3h".
static const regex H_RE(R"((\d+)h)", regex::optimize);
// Captures minute components in duration strings like "15m" (not "ms").
static const regex M_RE(R"((\d+)m(?!s))", regex::optimize);
// Captures second components in duration strings like "12s".
static const regex S_RE(R"((\d+)s)", regex::optimize);
// Captures millisecond components in duration strings like "123.4ms".
static const regex MS_RE(R"((\d+(?:\.\d+)?)ms)", regex::optimize);
// Extracts one operator-timing row: optional node id, operator name, hosts, instances,
// avg time token, and max time token.
static const regex ROW_RE(
    R"(^\s*(?:(\d+):)?)"
    R"(([A-Z][A-Z _/\-]+(?:\[[^\]]+\])?)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+))",
    regex::optimize);
// Matches and removes a leading fragment prefix like "F00:" from plan lines.
static const regex FRAGMENT_PREFIX_RE(R"(^\s*F\d+:\s*)", regex::optimize);

// Deep-copies a JSON value into a target allocator.
static Value CloneValue(const Value& src, Document::AllocatorType& alloc) {
  return Value(src, alloc);
}

static Value MakeJsonString(string_view text, Document::AllocatorType& alloc) {
  return Value(text.data(), static_cast<rapidjson::SizeType>(text.size()), alloc);
}

static constexpr string_view AVERAGED_FRAGMENT_PREFIX = "Averaged Fragment ";
static constexpr string_view FRAGMENT_PREFIX = "Fragment ";
static constexpr string_view COORDINATOR_FRAGMENT_PREFIX = "Coordinator Fragment ";

static string NormalizeFragmentId(string_view fragment_id) {
  string normalized(fragment_id);
  if (!normalized.empty() && normalized[0] != 'F' && IsAllDigits(normalized)) {
    normalized = "F" + (normalized.size() == 1 ? "0" + normalized : normalized);
  }
  return normalized;
}

static bool MatchesFragmentPrefix(
    string_view key, string_view prefix, string_view normalized_fragment_id) {
  if (normalized_fragment_id.empty()) return false;
  const size_t normalized_start = prefix.size();
  if (key.size() < normalized_start + normalized_fragment_id.size()) return false;
  if (key.compare(0, prefix.size(), prefix) != 0) return false;
  if (key.compare(normalized_start, normalized_fragment_id.size(),
          normalized_fragment_id) != 0) {
    return false;
  }
  const size_t after_id_idx = normalized_start + normalized_fragment_id.size();
  if (after_id_idx == key.size()) return true;
  return key[after_id_idx] == ' '
      || key[after_id_idx] == ':'
      || key[after_id_idx] == '[';
}

static int FragmentMatchRank(string_view key, string_view normalized_fragment_id) {
  if (normalized_fragment_id.empty()) return 5;
  if (key == normalized_fragment_id) return 0;
  if (MatchesFragmentPrefix(key, AVERAGED_FRAGMENT_PREFIX, normalized_fragment_id)) {
    return 1;
  }
  if (MatchesFragmentPrefix(key, FRAGMENT_PREFIX, normalized_fragment_id)) return 2;
  if (MatchesFragmentPrefix(
          key, COORDINATOR_FRAGMENT_PREFIX, normalized_fragment_id)) {
    return 3;
  }
  if (key.find(normalized_fragment_id) != string::npos) return 4;
  return 5;
}

// Adds a key/value child to an object; duplicate keys are accumulated as arrays.
static void AddChild(Value& parent_obj, const char* key, Value&& value,
    Document::AllocatorType& alloc) {
  DCHECK(parent_obj.IsObject()) << "AddChild parent must be an object";
  if (!parent_obj.IsObject()) return;
  if (!parent_obj.HasMember(key)) {
    parent_obj.AddMember(Value(key, alloc), std::move(value), alloc);
    return;
  }

  Value& existing = parent_obj[key];
  if (!existing.IsArray()) {
    Value arr(rapidjson::kArrayType);
    arr.PushBack(Value(existing, alloc), alloc);
    existing = move(arr);
  }
  existing.PushBack(std::move(value), alloc);
}

// Converts profile keys/children arrays into a normalized JSON object tree.
static Status ConvertJsonProfile(
    const Value& profile, Value* out, Document::AllocatorType& alloc) {
  DCHECK(out != nullptr);
  if (out == nullptr) return Status("output profile pointer cannot be null");
  out->SetObject();
  for (auto it = profile.MemberBegin(); it != profile.MemberEnd(); ++it) {
    const char* key = it->name.GetString();
    if (strcmp(key, PROFILE_NAME_KEY) == 0
        || strcmp(key, CHILD_PROFILES_KEY) == 0
        || strcmp(key, INFO_STRINGS_KEY) == 0
        || strcmp(key, NUM_CHILDREN_KEY) == 0) {
      continue;
    }
    out->AddMember(Value(it->name, alloc), CloneValue(it->value, alloc), alloc);
  }

  if (profile.HasMember(INFO_STRINGS_KEY)) {
    if (!profile[INFO_STRINGS_KEY].IsArray()) {
      return Status("'info_strings' must be an array");
    }
    for (const auto& entry : profile[INFO_STRINGS_KEY].GetArray()) {
      if (!entry.IsObject() || !entry.HasMember("key")
          || !entry["key"].IsString()) {
        continue;
      }
      const char* key = entry["key"].GetString();
      if (entry.HasMember("value")) {
        AddChild(*out, key, CloneValue(entry["value"], alloc), alloc);
      } else {
        AddChild(*out, key, Value("", alloc), alloc);
      }
    }
  }

  if (profile.HasMember(CHILD_PROFILES_KEY)) {
    if (!profile[CHILD_PROFILES_KEY].IsArray()) {
      return Status("'child_profiles' must be an array");
    }
    for (const auto& child : profile[CHILD_PROFILES_KEY].GetArray()) {
      if (!child.IsObject() || !child.HasMember(PROFILE_NAME_KEY)
          || !child[PROFILE_NAME_KEY].IsString()) {
        continue;
      }
      const char* child_name = child[PROFILE_NAME_KEY].GetString();
      Value child_value(rapidjson::kObjectType);
      RETURN_IF_ERROR(ConvertJsonProfile(child, &child_value, alloc));
      AddChild(*out, child_name, std::move(child_value), alloc);
    }
  }
  return Status::OK();
}

static Status ParseProfile(const string& profile_text, ParsedProfile* out) {
  if (out == nullptr) return Status("parsed profile output pointer cannot be null");
  out->SetObject();
  Document source_json;
  source_json.Parse(
      profile_text.data(), static_cast<rapidjson::SizeType>(profile_text.size()));
  if (source_json.HasParseError()) {
    return Status("Query profile input must be a valid JSON object: parse error '" +
        string(rapidjson::GetParseError_En(source_json.GetParseError()))
        + "' (error code " + std::to_string(static_cast<int>(source_json.GetParseError()))
        + ") at offset " + std::to_string(source_json.GetErrorOffset()));
  }
  if (!source_json.IsObject()) {
    return Status("Query profile input must be a valid JSON object");
  }

  Document::AllocatorType& alloc = out->GetAllocator();
  const Value* root_profile = nullptr;
  if (source_json.HasMember(CONTENTS_KEY) && source_json[CONTENTS_KEY].IsObject()) {
    root_profile = &source_json[CONTENTS_KEY];
  } else if (source_json.HasMember(PROFILE_NAME_KEY)
      && source_json[PROFILE_NAME_KEY].IsString()) {
    root_profile = &source_json;
  }
  if (root_profile != nullptr && root_profile->HasMember(PROFILE_NAME_KEY)
      && (*root_profile)[PROFILE_NAME_KEY].IsString()) {
    const Value& profile_name = (*root_profile)[PROFILE_NAME_KEY];
    Value converted(rapidjson::kObjectType);
    RETURN_IF_ERROR(ConvertJsonProfile(*root_profile, &converted, alloc));
    out->AddMember(Value(profile_name, alloc), converted, alloc);
    return Status::OK();
  }

  return Status("Query profile JSON must include either 'contents.profile_name' "
                "or top-level 'profile_name'");
}

// Initializes query selection metadata from the parsed profile document.
QueryProfileToolAccessor::QueryProfileToolAccessor(ParsedProfile parsed)
    : parsed_(move(parsed)) {
  if (!parsed_.IsObject()) return;

  int query_type_query_index = 0;
  bool selected_query_type_query = false;
  string first_query_key;
  for (auto it = parsed_.MemberBegin(); it != parsed_.MemberEnd(); ++it) {
    const string_view key(it->name.GetString(), it->name.GetStringLength());
    if (key.find("Query (id=") != 0 || !it->value.IsObject()) continue;
    if (first_query_key.empty()) first_query_key.assign(key.data(), key.size());

    const string_view query_type = GetQueryType(it->value);
    if (query_type == QUERY_TYPE_QUERY && query_key_.empty()) {
      query_key_.assign(key.data(), key.size());
      selected_query_type_query = true;
    }
    if (query_type == QUERY_TYPE_QUERY) {
      if (key == query_key_) query_type_query_scope_idx_ = query_type_query_index;
      ++query_type_query_index;
    }
  }
  if (query_key_.empty()) query_key_ = first_query_key;
  if (!query_key_.empty()) {
    smatch match;
    if (regex_match(query_key_, match, QUERY_ID_RE)) query_id_ = match[1].str();
    DCHECK(!query_id_.empty()) << "failed to extract query id from query key: "
                               << query_key_;
    if (!query_id_.empty()) {
      execution_profile_key_ = string(EXECUTION_PROFILE_PREFIX) + " " + query_id_;
    }
  }
  if (!selected_query_type_query) query_type_query_scope_idx_ = 0;

  const Value* scoped_exec = GetScopedExecutionProfile();
  if (scoped_exec == nullptr || !scoped_exec->IsObject()) return;

  unordered_map<string, int> used;
  vector<pair<string, const Value*>> gathered_fragments;
  GatherFragments(*scoped_exec, &gathered_fragments);
  for (const auto& item : gathered_fragments) {
    const string& key = item.first;
    const int count = ++used[key];
    string unique_key = key;
    if (count > 1) {
      unique_key.append(" [").append(std::to_string(count)).append("]");
    }
    all_fragments_.emplace_back(move(unique_key), item.second);
  }
  if (all_fragments_.empty()) return;

  unordered_map<string, pair<string, const Value*>> best_by_fragment_id;
  unordered_map<string, int> best_score;
  for (const auto& item : all_fragments_) {
    smatch match;
    string bucket = item.first;
    if (regex_search(item.first, match, FID_RE)) bucket = match[0].str();
    // Multiple sections can map to the same fragment id (for example averaged vs
    // coordinator variants). Keep the richest object to preserve most details.
    const int score = item.second != nullptr ? DictComplexity(*item.second) : 0;
    const auto score_it = best_score.find(bucket);
    if (score_it == best_score.end() || score > score_it->second) {
      best_score.insert_or_assign(bucket, score);
      best_by_fragment_id.insert_or_assign(bucket, item);
    }
  }
  for (auto& entry : best_by_fragment_id) {
    fragments_.emplace(move(entry.second));
  }
}

// Returns the Summary section for the selected query.
Value QueryProfileToolAccessor::GetSummary(Document::AllocatorType& alloc) const {
  const Value* summary = GetSummaryPtr();
  if (summary != nullptr) return CloneValue(*summary, alloc);
  return Value(rapidjson::kObjectType);
}

// Returns the execution timeline section.
Value QueryProfileToolAccessor::GetTimeline(Document::AllocatorType& alloc) const {
  return BuildEventTimelineSection(/*sequence_idx=*/1, alloc);
}

// Returns the compilation timeline section.
Value QueryProfileToolAccessor::GetCompilation(Document::AllocatorType& alloc) const {
  return BuildEventTimelineSection(/*sequence_idx=*/0, alloc);
}

// Returns the scoped execution profile with per-node details merged.
Value QueryProfileToolAccessor::GetExecutionProfile(
    Document::AllocatorType& alloc) const {
  const Value* scoped = GetScopedExecutionProfile();
  if (scoped != nullptr && scoped->IsObject()) {
    Value out = CloneValue(*scoped, alloc);
    if (!out.HasMember(PER_NODE_PROFILES_KEY)) {
      Value pnp = GetPerNodeProfiles(alloc);
      if (pnp.IsObject() && !pnp.ObjectEmpty()) {
        out.AddMember(Value(PER_NODE_PROFILES_KEY, alloc), pnp, alloc);
      }
    }
    return out;
  }
  DCHECK(false) << "profile is missing execution profile section for query " << query_id_;
  return Value(rapidjson::kObjectType);
}

// Returns a condensed list of all fragments with summary metadata.
Value QueryProfileToolAccessor::GetFragmentsOverview(
    Document::AllocatorType& alloc) const {
  Value arr(rapidjson::kArrayType);
  for (const auto& fragment : fragments_) {
    const string& key = fragment.first;
    const Value* frag_data = fragment.second;
    Value info(rapidjson::kObjectType);
    smatch match;
    if (regex_search(key, match, FID_RE)) {
      info.AddMember(
          Value(FRAGMENT_ID_KEY, alloc),
          Value(match[0].str().c_str(), alloc), alloc);
    } else {
      info.AddMember(Value(FRAGMENT_ID_KEY, alloc), Value("", alloc), alloc);
    }

    info.AddMember("full_key", Value(key.c_str(), alloc), alloc);
    if (frag_data->IsObject()) {
      int instance_count = 0;
      for (auto it = frag_data->MemberBegin(); it != frag_data->MemberEnd(); ++it) {
        const string_view member_key(it->name.GetString(), it->name.GetStringLength());
        if (member_key.find(INSTANCE_TOKEN) != string::npos) ++instance_count;
      }
      if (instance_count > 0) info.AddMember("instance_count", instance_count, alloc);
      if (frag_data->HasMember(VALUE_KEY) && (*frag_data)[VALUE_KEY].IsString()) {
        const Value& summary_value = (*frag_data)[VALUE_KEY];
        const string_view summary(
            summary_value.GetString(), summary_value.GetStringLength());
        info.AddMember("summary", MakeJsonString(summary, alloc), alloc);
        AddOperatorRows(summary, &info, alloc);
      }
    } else if (frag_data->IsString()) {
      const string_view summary(frag_data->GetString(), frag_data->GetStringLength());
      info.AddMember("summary", MakeJsonString(summary, alloc), alloc);
      AddOperatorRows(summary, &info, alloc);
    }
    arr.PushBack(info, alloc);
  }
  return arr;
}

// Returns the averaged fragment object for a fragment id.
Value QueryProfileToolAccessor::GetFragment(
    string_view fragment_id, Document::AllocatorType& alloc) const {
  const string normalized = NormalizeFragmentId(fragment_id);
  const Value* best_match = nullptr;
  string_view best_key;
  for (const auto& fragment : all_fragments_) {
    if (fragment.second == nullptr) continue;
    const string_view key(fragment.first);
    const bool is_match = normalized.empty()
        ? key.find(AVERAGED_FRAGMENT_PREFIX) == 0
        : MatchesFragmentPrefix(key, AVERAGED_FRAGMENT_PREFIX, normalized);
    if (!is_match) continue;
    if (best_match == nullptr || key < best_key) {
      best_match = fragment.second;
      best_key = key;
    }
  }

  if (best_match != nullptr) return CloneValue(*best_match, alloc);
  Value err(rapidjson::kObjectType);
  err.AddMember(
      Value(ERROR_KEY, alloc), Value("averaged fragment not found", alloc), alloc);
  return err;
}

Value QueryProfileToolAccessor::GetAllFragments(
    string_view fragment_id, Document::AllocatorType& alloc) const {
  const string normalized = NormalizeFragmentId(fragment_id);
  multiset<const pair<string, const Value*>*, FragmentPtrLess> matches;
  for (const auto& fragment : all_fragments_) {
    if (fragment.second == nullptr) continue;
    if (!normalized.empty()
        && FragmentMatchRank(fragment.first, normalized) == 5) {
      continue;
    }
    matches.emplace(&fragment);
  }
  Value out(rapidjson::kArrayType);
  for (const auto* match : matches) {
    Value row(rapidjson::kObjectType);
    row.AddMember("fragment", MakeJsonString(match->first, alloc), alloc);
    row.AddMember("section", CloneValue(*match->second, alloc), alloc);
    out.PushBack(row, alloc);
  }
  return out;
}

// Returns all instances nested under a specific fragment.
Value QueryProfileToolAccessor::GetFragmentInstances(
      string_view fragment_id, Document::AllocatorType& alloc) const {
  const string normalized = NormalizeFragmentId(fragment_id);
  Value out(rapidjson::kObjectType);
  unordered_set<string_view> seen_instance_keys;
  auto add_instances_from_fragment = [&](const Value& fragment) {
    if (!fragment.IsObject()) return;
    for (auto it = fragment.MemberBegin(); it != fragment.MemberEnd(); ++it) {
      const string_view key(it->name.GetString(), it->name.GetStringLength());
      if (key.find(INSTANCE_TOKEN) == string::npos) continue;
      if (!seen_instance_keys.insert(key).second) continue;
      out.AddMember(Value(it->name, alloc), Value(it->value, alloc), alloc);
    }
  };
  for (const auto& fragment : all_fragments_) {
    if (fragment.second == nullptr) continue;
    const string_view key(fragment.first);
    if (!MatchesFragmentPrefix(key, FRAGMENT_PREFIX, normalized)
        && !MatchesFragmentPrefix(
            key, COORDINATOR_FRAGMENT_PREFIX, normalized)) {
      continue;
    }
    add_instances_from_fragment(*fragment.second);
  }
  if (!out.ObjectEmpty()) return out;

  for (const auto& fragment : all_fragments_) {
    if (fragment.second == nullptr) continue;
    if (FragmentMatchRank(fragment.first, normalized) == 5) continue;
    add_instances_from_fragment(*fragment.second);
  }
  return out;
}

// Returns a matching operator section, optionally scoped to a fragment.
Value QueryProfileToolAccessor::GetOperator(string_view node_id, string_view fragment_id,
      Document::AllocatorType& alloc) const {
  const Value* search_data = &parsed_;
  Document tmp;
  tmp.SetObject();
  Value fragment_holder;
  if (!fragment_id.empty()) {
    fragment_holder = GetFragmentInstances(fragment_id, tmp.GetAllocator());
    if (fragment_holder.IsObject() && !fragment_holder.ObjectEmpty()) {
      search_data = &fragment_holder;
    } else {
      fragment_holder = GetFragment(fragment_id, tmp.GetAllocator());
      if (!fragment_holder.IsObject() || fragment_holder.HasMember(ERROR_KEY)) {
        Value err(rapidjson::kObjectType);
        err.AddMember(Value(ERROR_KEY, alloc), Value("fragment not found", alloc), alloc);
        return err;
      }
      search_data = &fragment_holder;
    }
  }
  const Value* found = SearchOperator(*search_data, node_id);
  if (found == nullptr) {
    Value err(rapidjson::kObjectType);
    err.AddMember(Value(ERROR_KEY, alloc), Value("operator not found", alloc), alloc);
    return err;
  }
  return CloneValue(*found, alloc);
}

Value QueryProfileToolAccessor::GetAveragedOperator(
    string_view node_id, Document::AllocatorType& alloc) const {
  const Value* best_match = nullptr;
  string_view best_fragment_key;
  string_view best_node_key;
  for (const auto& fragment : all_fragments_) {
    const string_view fragment_key(fragment.first);
    if (fragment.second == nullptr || !fragment.second->IsObject()) continue;
    if (fragment_key.find(AVERAGED_FRAGMENT_PREFIX) != 0) continue;
    string_view node_key;
    const Value* match = SearchOperator(*fragment.second, node_id, &node_key);
    if (match == nullptr) continue;
    if (best_match == nullptr
        || fragment_key < best_fragment_key
        || (fragment_key == best_fragment_key && node_key < best_node_key)) {
      best_match = match;
      best_fragment_key = fragment_key;
      best_node_key = node_key;
    }
  }
  if (best_match == nullptr) {
    Value err(rapidjson::kObjectType);
    err.AddMember(Value(ERROR_KEY, alloc), Value("averaged operator not found", alloc),
        alloc);
    return err;
  }
  Value out(rapidjson::kObjectType);
  out.AddMember("fragment", MakeJsonString(best_fragment_key, alloc), alloc);
  out.AddMember("operator", MakeJsonString(best_node_key, alloc), alloc);
  out.AddMember("section", CloneValue(*best_match, alloc), alloc);
  return out;
}

Value QueryProfileToolAccessor::GetAllOperators(
    string_view node_id, string_view fragment_id, Document::AllocatorType& alloc) const {
  const string normalized_fragment = NormalizeFragmentId(fragment_id);
  auto is_instance_fragment = [](string_view key) {
    return key.find(FRAGMENT_PREFIX) == 0 || key.find(COORDINATOR_FRAGMENT_PREFIX) == 0;
  };
  auto is_requested_fragment = [&](string_view key) {
    if (normalized_fragment.empty()) return true;
    return MatchesFragmentPrefix(key, FRAGMENT_PREFIX, normalized_fragment)
        || MatchesFragmentPrefix(key, COORDINATOR_FRAGMENT_PREFIX, normalized_fragment);
  };

  Value arr(rapidjson::kArrayType);
  for (const auto& fragment : all_fragments_) {
    const string_view fragment_key(fragment.first);
    if (fragment.second == nullptr || !fragment.second->IsObject()) continue;
    if (!is_instance_fragment(fragment_key)
        || !is_requested_fragment(fragment_key)) {
      continue;
    }
    const Value& fragment_value = *fragment.second;
    for (auto it = fragment_value.MemberBegin(); it != fragment_value.MemberEnd(); ++it) {
      const string_view instance_key(it->name.GetString(), it->name.GetStringLength());
      if (instance_key.find(INSTANCE_TOKEN) == string::npos
          || !it->value.IsObject()) {
        continue;
      }
      string_view operator_key;
      const Value* section = SearchOperator(it->value, node_id, &operator_key);
      if (section == nullptr) continue;
      Value row(rapidjson::kObjectType);
      row.AddMember("fragment", MakeJsonString(fragment_key, alloc), alloc);
      row.AddMember("instance", MakeJsonString(instance_key, alloc), alloc);
      row.AddMember("operator", MakeJsonString(operator_key, alloc), alloc);
      row.AddMember("section", CloneValue(*section, alloc), alloc);
      arr.PushBack(row, alloc);
    }
  }
  return arr;
}

Value QueryProfileToolAccessor::GetSpecificOperator(string_view node_id,
    string_view fragment_id, string_view instance_id,
    Document::AllocatorType& alloc) const {
  Value all = GetAllOperators(node_id, fragment_id, alloc);
  if (!all.IsArray() || all.Empty()) {
    Value err(rapidjson::kObjectType);
    err.AddMember(Value(ERROR_KEY, alloc), Value("operator not found", alloc), alloc);
    return err;
  }
  if (instance_id.empty()) {
    return Value(move(all.GetArray()[0]));
  }
  for (auto& row : all.GetArray()) {
    if (!row.IsObject() || !row.HasMember("instance")
        || !row["instance"].IsString()) {
      continue;
    }
    const Value& instance_value = row["instance"];
    const string_view instance(
        instance_value.GetString(), instance_value.GetStringLength());
    if (instance.find(instance_id) != string::npos) return Value(move(row));
  }
  Value err(rapidjson::kObjectType);
  err.AddMember(
      Value(ERROR_KEY, alloc),
      Value("specific operator instance not found", alloc), alloc);
  return err;
}

// Returns compact metrics for scan nodes across fragments.
Value QueryProfileToolAccessor::GetScanNodesSummary(
    Document::AllocatorType& alloc) const {
  Value arr(rapidjson::kArrayType);
  unordered_set<pair<string_view, string_view>, StringViewPairHash> seen;
  auto add_scan_summary = [&](string_view fragment_key, string_view node_key,
                              const Value* node_value) {
    if (!seen.emplace(fragment_key, node_key).second) return;
    Value summary(rapidjson::kObjectType);
    summary.AddMember("fragment", MakeJsonString(fragment_key, alloc), alloc);
    summary.AddMember("node", MakeJsonString(node_key, alloc), alloc);
    if (node_value != nullptr && node_value->IsObject()) {
      for (auto it = node_value->MemberBegin(); it != node_value->MemberEnd(); ++it) {
        const string_view key(it->name.GetString(), it->name.GetStringLength());
        if (key.find("Rows") != string::npos || key.find("Bytes") != string::npos
            || key.find("Time") != string::npos || key.find("Memory") != string::npos
            || key.find("Peak") != string::npos) {
          summary.AddMember(Value(it->name, alloc), Value(it->value, alloc), alloc);
        }
      }
    }
    arr.PushBack(summary, alloc);
  };

  for (const auto& fragment : fragments_) {
    vector<pair<string_view, const Value*>> nodes;
    FindNodesByPattern(*fragment.second, "SCAN", &nodes);
    for (const auto& node : nodes) {
      add_scan_summary(fragment.first, node.first, node.second);
    }
  }
  if (arr.Empty()) {
    vector<pair<string_view, const Value*>> nodes;
    Value pnp = GetPerNodeProfiles(alloc);
    if (pnp.IsObject()) FindNodesByPattern(pnp, "SCAN", &nodes);
    if (nodes.empty()) FindNodesByPattern(parsed_, "SCAN", &nodes);
    for (const auto& node : nodes) {
      add_scan_summary("", node.first, node.second);
    }
  }
  return arr;
}

// Returns the Per Node Profiles section when available.
Value QueryProfileToolAccessor::GetPerNodeProfiles(
    Document::AllocatorType& alloc) const {
  const Value* exec = GetScopedExecutionProfile();
  if (exec != nullptr && exec->IsObject()
      && exec->HasMember(PER_NODE_PROFILES_KEY)
      && (*exec)[PER_NODE_PROFILES_KEY].IsObject()) {
    return CloneValue((*exec)[PER_NODE_PROFILES_KEY], alloc);
  }
  return Value(rapidjson::kObjectType);
}

// Parses query options into a normalized key/value object.
Value QueryProfileToolAccessor::GetQueryOptions(Document::AllocatorType& alloc) const {
  Value obj(rapidjson::kObjectType);
  const Value* summary = GetSummaryPtr();
  if (summary == nullptr) return obj;
  static const vector<const char*> option_keys = {
      "Query Options (set by configuration)",
      "Query Options (set by configuration and planner)"};
  for (const auto* option_key : option_keys) {
    if (!summary->HasMember(option_key) || !(*summary)[option_key].IsString()) continue;
    const Value& options_value = (*summary)[option_key];
    const string_view options(
        options_value.GetString(), options_value.GetStringLength());
    size_t item_start = 0;
    while (item_start < options.size()) {
      const size_t comma = options.find(',', item_start);
      const size_t item_end = comma == string_view::npos ? options.size() : comma;
      string_view option = options.substr(item_start, item_end - item_start);
      item_start = comma == string_view::npos ? options.size() : comma + 1;
      if (option.empty()) continue;

      const size_t eq = option.find('=');
      if (eq == string_view::npos) continue;
      const string_view key = TrimWhiteSpace(option.substr(0, eq));
      const string_view value = TrimWhiteSpace(option.substr(eq + 1));
      if (key.empty()) continue;

      Value key_json(rapidjson::kStringType);
      key_json.SetString(
          key.data(), static_cast<rapidjson::SizeType>(key.size()), alloc);
      if (obj.HasMember(key_json)) {
        obj[key_json].SetString(
            value.data(), static_cast<rapidjson::SizeType>(value.size()), alloc);
      } else {
        obj.AddMember(
            MakeJsonString(key, alloc),
            MakeJsonString(value, alloc), alloc);
      }
    }
  }
  return obj;
}

// Returns the list of queried tables from Summary metadata.
Value QueryProfileToolAccessor::GetTablesQueried(Document::AllocatorType& alloc) const {
  Value arr(rapidjson::kArrayType);
  const Value* summary = GetSummaryPtr();
  if (summary == nullptr || !summary->HasMember("Tables Queried")
      || !(*summary)["Tables Queried"].IsString()) {
    return arr;
  }
  const Value& tables_value = (*summary)["Tables Queried"];
  const string_view tables(tables_value.GetString(), tables_value.GetStringLength());
  size_t table_start = 0;
  while (table_start < tables.size()) {
    const size_t comma = tables.find(',', table_start);
    const size_t table_end = comma == string_view::npos ? tables.size() : comma;
    const string_view table =
        TrimWhiteSpace(tables.substr(table_start, table_end - table_start));
    table_start = comma == string_view::npos ? tables.size() : comma + 1;
    if (!table.empty()) {
      arr.PushBack(MakeJsonString(table, alloc), alloc);
    }
  }
  return arr;
}

// Returns reservation and estimate fields from summary/plan text.
Value QueryProfileToolAccessor::GetResourceEstimates(
    Document::AllocatorType& alloc) const {
  Value obj(rapidjson::kObjectType);
  const Value* summary = GetSummaryPtr();
  if (summary != nullptr && summary->IsObject()) {
    if (summary->HasMember("Max Per-Host Resource Reservation")) {
      obj.AddMember("reservation",
          CloneValue((*summary)["Max Per-Host Resource Reservation"], alloc), alloc);
    }
    if (summary->HasMember("Per-Host Resource Estimates")) {
      obj.AddMember("estimates",
          CloneValue((*summary)["Per-Host Resource Estimates"], alloc), alloc);
    }
    if (summary->HasMember("Dedicated Coordinator Resource Estimate")) {
      obj.AddMember("coordinator",
          CloneValue((*summary)["Dedicated Coordinator Resource Estimate"], alloc),
          alloc);
    }
  }
  if (!obj.HasMember("reservation")) {
    if (summary != nullptr && summary->HasMember("Plan")
        && (*summary)["Plan"].IsString()) {
      const Value& plan_value = (*summary)["Plan"];
      const string_view plan(plan_value.GetString(), plan_value.GetStringLength());
      match_results<string_view::const_iterator> match;
      if (regex_search(plan.begin(), plan.end(), match, RES_RE) && match.size() > 1) {
        const string_view reservation =
            TrimWhiteSpace(string_view(match[1].first, match[1].length()));
        if (!reservation.empty()) {
          obj.AddMember("reservation", MakeJsonString(reservation, alloc), alloc);
        }
      }
    }
  }
  if (!obj.HasMember("estimates")) {
    if (summary != nullptr && summary->HasMember("Plan")
        && (*summary)["Plan"].IsString()) {
      const Value& plan_value = (*summary)["Plan"];
      const string_view plan(plan_value.GetString(), plan_value.GetStringLength());
      match_results<string_view::const_iterator> match;
      if (regex_search(plan.begin(), plan.end(), match, EST_RE) && match.size() > 1) {
        const string_view estimates =
            TrimWhiteSpace(string_view(match[1].first, match[1].length()));
        if (!estimates.empty()) {
          obj.AddMember("estimates", MakeJsonString(estimates, alloc), alloc);
        }
      }
    }
  }
  return obj;
}

// Extracts analyzed query SQL text from the plan field.
string QueryProfileToolAccessor::GetAnalyzedQuery() const {
  const Value* summary = GetSummaryPtr();
  if (summary != nullptr && summary->HasMember("Plan") && (*summary)["Plan"].IsString()) {
    const Value& plan_value = (*summary)["Plan"];
    re2::StringPiece plan(plan_value.GetString(), plan_value.GetStringLength());
    re2::StringPiece match;
    if (re2::RE2::PartialMatch(plan, ANALYZED_RE, &match)) {
      const string_view analyzed =
          TrimWhiteSpace(string_view(match.data(), match.length()));
      return string(analyzed);
    }
  }
  return string();
}

// Reads query type from the Summary section.
string_view QueryProfileToolAccessor::GetQueryType(const Value& query_obj) const {
  if (!query_obj.IsObject() || !query_obj.HasMember(SUMMARY_KEY)
      || !query_obj[SUMMARY_KEY].IsObject()) {
    return string_view();
  }
  const Value& summary = query_obj[SUMMARY_KEY];
  if (!summary.HasMember(QUERY_TYPE_KEY) || !summary[QUERY_TYPE_KEY].IsString()) {
    return string_view();
  }
  const Value& query_type = summary[QUERY_TYPE_KEY];
  return string_view(query_type.GetString(), query_type.GetStringLength());
}

// Selects the scoped value for the chosen query index.
const Value* QueryProfileToolAccessor::SelectScopedValue(const Value& value) const {
  if (!value.IsArray()) return &value;
  const rapidjson::SizeType idx =
      static_cast<rapidjson::SizeType>(query_type_query_scope_idx_);
  if (idx < value.Size()) return &value[idx];
  if (!value.Empty()) return &value[0];
  return nullptr;
}

// Returns the best execution profile section for the selected query.
const Value* QueryProfileToolAccessor::GetScopedExecutionProfile() const {
  if (!query_key_.empty()) {
    DCHECK(!query_id_.empty())
        << "selected query key is missing query id: " << query_key_;
  }
  const Value* query_data = GetQueryData();
  if (query_data != nullptr && query_data->IsObject()) {
    for (auto it = query_data->MemberBegin(); it != query_data->MemberEnd(); ++it) {
      const string_view key(it->name.GetString(), it->name.GetStringLength());
      if (key.find(EXECUTION_PROFILE_PREFIX) == 0) {
        const Value* scoped = SelectScopedValue(it->value);
        if (scoped != nullptr && scoped->IsObject()) return scoped;
      }
    }
  }
  if (!execution_profile_key_.empty()) {
    if (parsed_.HasMember(execution_profile_key_.c_str())
        && parsed_[execution_profile_key_.c_str()].IsObject()) {
      return &parsed_[execution_profile_key_.c_str()];
    }
  }
  return nullptr;
}

// Computes a recursive complexity score for candidate objects.
int QueryProfileToolAccessor::DictComplexity(const Value& v) const {
  if (v.IsObject()) {
    int total = static_cast<int>(v.MemberCount());
    for (auto it = v.MemberBegin(); it != v.MemberEnd(); ++it) {
      total += DictComplexity(it->value);
    }
    return total;
  }
  if (v.IsArray()) {
    int total = static_cast<int>(v.Size());
    for (const auto& item : v.GetArray()) total += DictComplexity(item);
    return total;
  }
  return 1;
}

// Returns the selected top-level query object.
const Value* QueryProfileToolAccessor::GetQueryData() const {
  if (query_key_.empty() || !parsed_.HasMember(query_key_.c_str())) return nullptr;
  return &parsed_[query_key_.c_str()];
}

// Returns the selected query Summary object.
const Value* QueryProfileToolAccessor::GetSummaryPtr() const {
  const Value* query_data = GetQueryData();
  if (query_data != nullptr && query_data->IsObject()
      && query_data->HasMember(SUMMARY_KEY)
      && (*query_data)[SUMMARY_KEY].IsObject()) {
    return &(*query_data)[SUMMARY_KEY];
  }
  DCHECK(false) << "profile is missing summary section for query " << query_id_;
  return nullptr;
}

// Builds timeline output for either compilation or execution sequence.
Value QueryProfileToolAccessor::BuildEventTimelineSection(
      int sequence_idx, Document::AllocatorType& alloc) const {
  Value out(rapidjson::kObjectType);
  const Value* summary = GetSummaryPtr();
  if (summary == nullptr || !summary->IsObject()
      || !summary->HasMember(EVENT_SEQUENCES_KEY)
      || !(*summary)[EVENT_SEQUENCES_KEY].IsArray()) {
    return out;
  }
  const Value& sequences = (*summary)[EVENT_SEQUENCES_KEY];
  if (sequence_idx < 0 || sequence_idx >= static_cast<int>(sequences.Size())
      || !sequences[sequence_idx].IsObject()) {
    return out;
  }
  const Value& sequence = sequences[sequence_idx];
  if (!sequence.HasMember(EVENTS_KEY) || !sequence[EVENTS_KEY].IsArray()) {
    return out;
  }

  int64_t prev_ts = 0;
  int64_t first_ts = -1;
  int64_t last_ts = -1;
  for (const auto& event : sequence[EVENTS_KEY].GetArray()) {
    if (!event.IsObject() || !event.HasMember("label") || !event["label"].IsString()
        || !event.HasMember("timestamp")) {
      continue;
    }
    int64_t ts = -1;
    if (event["timestamp"].IsInt64()) ts = event["timestamp"].GetInt64();
    if (event["timestamp"].IsInt()) ts = event["timestamp"].GetInt();
    if (ts < 0) continue;
    if (first_ts < 0) first_ts = ts;
    const int64_t delta = (prev_ts == 0) ? ts : (ts - prev_ts);
    prev_ts = ts;
    last_ts = ts;
    const string ts_str = PrettyPrinter::Print(ts, TUnit::TIME_NS);
    const string delta_str = PrettyPrinter::Print(delta, TUnit::TIME_NS);
    string formatted;
    formatted.reserve(ts_str.size() + delta_str.size() + 4);
    formatted.append(ts_str);
    formatted.append(" (");
    formatted.append(delta_str);
    formatted.push_back(')');
    out.AddMember(Value(event["label"], alloc), Value(formatted.c_str(), alloc), alloc);
  }
  if (last_ts >= 0 && first_ts >= 0) {
    const int64_t total = last_ts - first_ts;
    const string total_str = PrettyPrinter::Print(total, TUnit::TIME_NS);
    out.AddMember(
        Value(VALUE_KEY, alloc), Value(total_str.c_str(), alloc), alloc);
  }
  return out;
}

// Checks whether a key appears to represent a fragment block.
bool QueryProfileToolAccessor::IsFragmentKey(string_view key) const {
  if (key.find("- ") == 0) return false;
  return key.find("Fragment F") != string::npos
      || regex_search(key.begin(), key.end(), FRAG_SHORT_RE);
}

// Recursively gathers fragment object members as key/value pointers.
void QueryProfileToolAccessor::GatherFragments(
      const Value& data, vector<pair<string, const Value*>>* out) const {
  if (!data.IsObject()) return;
  for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
    const string_view key(it->name.GetString(), it->name.GetStringLength());
    if (IsFragmentKey(key)) out->emplace_back(string(key), &it->value);
    if (it->value.IsObject()) GatherFragments(it->value, out);
    if (it->value.IsArray()) {
      for (const auto& item : it->value.GetArray()) {
        if (item.IsObject()) GatherFragments(item, out);
      }
    }
  }
}

int QueryProfileToolAccessor::ScoreNodeKeyMatch(
    string_view key, string_view node_id) const {
  if (node_id.empty()) return 0;
  if (key.size() == node_id.size() && key == node_id) return 3;

  const bool numeric_node_id = IsAllDigits(node_id);
  bool has_loose_substring_match = false;
  size_t pos = key.find(node_id);
  while (pos != string_view::npos) {
    has_loose_substring_match = true;
    const bool has_left = pos > 0;
    const size_t right_idx = pos + node_id.size();
    const bool has_right = right_idx < key.size();
    const bool left_is_alnum = has_left
        && isalnum(static_cast<unsigned char>(key[pos - 1])) != 0;
    const bool right_is_alnum = has_right
        && isalnum(static_cast<unsigned char>(key[right_idx])) != 0;
    if (!left_is_alnum && !right_is_alnum) return 2;
    pos = key.find(node_id, pos + 1);
  }

  // Avoid returning "1" for keys like "10" or "21" when no token-boundary match exists.
  if (numeric_node_id) return 0;
  return has_loose_substring_match ? 1 : 0;
}

int QueryProfileToolAccessor::ScoreOperatorKeyMatch(
    string_view key, string_view node_id) const {
  if (node_id.empty()) return 0;
  const bool has_embedded_operator_id = key.find("(id=") != string::npos;
  size_t numeric_prefix_len = 0;
  while (numeric_prefix_len < key.size()
      && isdigit(static_cast<unsigned char>(key[numeric_prefix_len])) != 0) {
    ++numeric_prefix_len;
  }
  const bool has_numeric_operator_prefix =
      numeric_prefix_len > 0 && numeric_prefix_len < key.size()
      && key[numeric_prefix_len] == ':';
  if (!has_embedded_operator_id && !has_numeric_operator_prefix) return 0;
  if (has_embedded_operator_id && IsAllDigits(node_id)) {
    const string exact_operator_id = "(id=" + string(node_id) + ")";
    return key.find(exact_operator_id) != string::npos ? 3 : 0;
  }
  return ScoreNodeKeyMatch(key, node_id);
}

void QueryProfileToolAccessor::FindBestOperatorMatch(const Value& data,
    string_view node_id, string_view* best_key, const Value** best_match, int* best_score,
    int* best_complexity) const {
  DCHECK(best_key != nullptr);
  DCHECK(best_match != nullptr);
  DCHECK(best_score != nullptr);
  DCHECK(best_complexity != nullptr);
  if (best_key == nullptr || best_match == nullptr || best_score == nullptr
      || best_complexity == nullptr) {
    return;
  }
  if (data.IsArray()) {
    for (const auto& item : data.GetArray()) {
      FindBestOperatorMatch(
          item, node_id, best_key, best_match, best_score, best_complexity);
      if (*best_score >= 3) return;
    }
    return;
  }
  if (!data.IsObject()) return;
  for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
    const string_view key(it->name.GetString(), it->name.GetStringLength());
    const int score = ScoreOperatorKeyMatch(key, node_id);
    if (score > 0) {
      const int complexity = DictComplexity(it->value);
      if (score > *best_score
          || (score == *best_score
              && (complexity > *best_complexity
                  || (complexity == *best_complexity
                      && (best_key->empty() || key < *best_key))))) {
        *best_key = key;
        *best_match = &it->value;
        *best_score = score;
        *best_complexity = complexity;
        if (*best_score >= 3) return;
      }
    }
    FindBestOperatorMatch(
        it->value, node_id, best_key, best_match, best_score, best_complexity);
    if (*best_score >= 3) return;
  }
}

const Value* QueryProfileToolAccessor::SearchOperator(
    const Value& data, string_view node_id, string_view* matched_key) const {
  if (matched_key != nullptr) *matched_key = string_view();
  const Value* best_match = nullptr;
  string_view best_key;
  int best_score = 0;
  int best_complexity = -1;
  FindBestOperatorMatch(
      data, node_id, &best_key, &best_match, &best_score, &best_complexity);
  if (matched_key != nullptr) *matched_key = best_key;
  return best_match;
}

// Recursively finds nodes whose key matches the provided pattern.
void QueryProfileToolAccessor::FindNodesByPattern(
    const Value& data, string_view pattern,
    vector<pair<string_view, const Value*>>* out)
    const {
  string normalized_pattern(pattern);
  transform(normalized_pattern.begin(), normalized_pattern.end(),
      normalized_pattern.begin(),
      [](unsigned char c) { return toupper(c); });
  FindNodesByPatternNormalized(data, normalized_pattern, out);
}

void QueryProfileToolAccessor::FindNodesByPatternNormalized(
    const Value& data, const string& normalized_pattern,
    vector<pair<string_view, const Value*>>* out) const {
  if (data.IsArray()) {
    for (const auto& item : data.GetArray()) {
      FindNodesByPatternNormalized(item, normalized_pattern, out);
    }
    return;
  }
  if (!data.IsObject()) return;
  for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
    const string_view key(it->name.GetString(), it->name.GetStringLength());
    string upper_key(key);
    transform(upper_key.begin(), upper_key.end(), upper_key.begin(),
        [](unsigned char c) { return toupper(c); });
    bool match = false;
    if (normalized_pattern == "SCAN") {
      match = regex_search(key.begin(), key.end(), SCAN_NODE_RE);
    } else {
      match = upper_key.find(normalized_pattern) != string::npos;
    }
    if (match) out->emplace_back(key, &it->value);
    FindNodesByPatternNormalized(it->value, normalized_pattern, out);
  }
}

// Parses duration strings (h/m/s/ms) into milliseconds.
double QueryProfileToolAccessor::ParseDurationMs(string_view value) const {
  if (value.empty()) return -1.0;
  double total_ms = 0.0;
  bool matched = false;
  match_results<string_view::const_iterator> match;
  if (regex_search(value.begin(), value.end(), match, H_RE)) {
    total_ms += atof(string(match[1].first, match[1].second).c_str()) * 3600000.0;
    matched = true;
  }
  if (regex_search(value.begin(), value.end(), match, M_RE)) {
    total_ms += atof(string(match[1].first, match[1].second).c_str()) * 60000.0;
    matched = true;
  }
  if (regex_search(value.begin(), value.end(), match, S_RE)) {
    total_ms += atof(string(match[1].first, match[1].second).c_str()) * 1000.0;
    matched = true;
  }
  if (regex_search(value.begin(), value.end(), match, MS_RE)) {
    total_ms += atof(string(match[1].first, match[1].second).c_str());
    matched = true;
  }
  return matched ? total_ms : -1.0;
}

// Extracts operator timing rows from fragment summary text.
void QueryProfileToolAccessor::AddOperatorRows(
      string_view text, Value* info, Document::AllocatorType& alloc) const {
  Value rows(rapidjson::kArrayType);
  const auto parse_int = [](string_view number, int* out) -> bool {
    if (out == nullptr || number.empty()) return false;
    int parsed = 0;
    const char* begin = number.data();
    const char* end = begin + number.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc() || result.ptr != end) return false;
    *out = parsed;
    return true;
  };
  for (const string_view line : SplitLines(text)) {
    string normalized(line);
    StripWhiteSpace(&normalized);
    while (!normalized.empty() && (normalized[0] == '|' || normalized[0] == '-')) {
      normalized.erase(0, 1);
      StripWhiteSpace(&normalized);
    }
    normalized = regex_replace(normalized, FRAGMENT_PREFIX_RE, "");
    smatch match;
    if (!regex_search(normalized, match, ROW_RE)) continue;
    Value row(rapidjson::kObjectType);
    const auto submatch_view = [&normalized, &match](int idx) -> string_view {
      if (!match[idx].matched) return string_view();
      const size_t pos = static_cast<size_t>(match.position(idx));
      const size_t len = static_cast<size_t>(match.length(idx));
      return string_view(normalized.data() + pos, len);
    };
    const string_view node_id = submatch_view(1);
    const string_view op_name = submatch_view(2);
    const string_view hosts_text = submatch_view(3);
    const string_view instances_text = submatch_view(4);
    const string_view avg_time = submatch_view(5);
    const string_view max_time = submatch_view(6);
    int hosts = 0;
    int instances = 0;
    if (!parse_int(hosts_text, &hosts)
        || !parse_int(instances_text, &instances)) continue;
    row.AddMember(NODE_ID_KEY,
        match[1].matched ? MakeJsonString(node_id, alloc) : Value("", alloc),
        alloc);
    row.AddMember("operator", MakeJsonString(op_name, alloc), alloc);
    row.AddMember("hosts", hosts, alloc);
    row.AddMember("instances", instances, alloc);
    row.AddMember("avg_time", MakeJsonString(avg_time, alloc), alloc);
    row.AddMember("max_time", MakeJsonString(max_time, alloc), alloc);
    const double avg_ms = ParseDurationMs(avg_time);
    const double max_ms = ParseDurationMs(max_time);
    if (avg_ms > 0 && max_ms > 0) {
      row.AddMember("max_vs_avg_ratio", max_ms / avg_ms, alloc);
    }
    rows.PushBack(row, alloc);
  }
  if (!rows.Empty()) info->AddMember("operator_timing_rows", rows, alloc);
}

// Dispatches a tool call name/args to the corresponding query-profile accessor.
static Status ExecuteTool(string_view tool_name, const Value& args,
    const QueryProfileToolAccessor& profile, Document::AllocatorType& alloc,
    Value* out_val) {
  DCHECK(out_val != nullptr);
  if (out_val == nullptr) return Status("tool output value pointer cannot be null");
  if (tool_name == "get_summary") {
    *out_val = profile.GetSummary(alloc);
    return Status::OK();
  }
  if (tool_name == "get_timeline") {
    *out_val = profile.GetTimeline(alloc);
    return Status::OK();
  }
  if (tool_name == "get_compilation") {
    *out_val = profile.GetCompilation(alloc);
    return Status::OK();
  }
  if (tool_name == "get_execution_profile") {
    *out_val = profile.GetExecutionProfile(alloc);
    return Status::OK();
  }
  if (tool_name == "get_fragments_overview") {
    *out_val = profile.GetFragmentsOverview(alloc);
    return Status::OK();
  }
  if (tool_name == "get_fragment") {
    string_view fragment_id;
    if (args.HasMember(FRAGMENT_ID_KEY) && args[FRAGMENT_ID_KEY].IsString()) {
      fragment_id = string_view(
          args[FRAGMENT_ID_KEY].GetString(), args[FRAGMENT_ID_KEY].GetStringLength());
    }
    *out_val = profile.GetFragment(fragment_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_all_fragments"
      || tool_name == "all_fragments"
      || tool_name == "AllFragments") {
    string_view fragment_id;
    if (args.HasMember(FRAGMENT_ID_KEY) && args[FRAGMENT_ID_KEY].IsString()) {
      fragment_id = string_view(
          args[FRAGMENT_ID_KEY].GetString(), args[FRAGMENT_ID_KEY].GetStringLength());
    }
    *out_val = profile.GetAllFragments(fragment_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_fragment_instances") {
    string_view fragment_id;
    if (args.HasMember(FRAGMENT_ID_KEY) && args[FRAGMENT_ID_KEY].IsString()) {
      fragment_id = string_view(
          args[FRAGMENT_ID_KEY].GetString(), args[FRAGMENT_ID_KEY].GetStringLength());
    }
    *out_val = profile.GetFragmentInstances(fragment_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_operator") {
    string_view node_id;
    string_view fragment_id;
    if (args.HasMember(NODE_ID_KEY) && args[NODE_ID_KEY].IsString()) {
      node_id = string_view(
          args[NODE_ID_KEY].GetString(), args[NODE_ID_KEY].GetStringLength());
    }
    if (args.HasMember(FRAGMENT_ID_KEY) && args[FRAGMENT_ID_KEY].IsString()) {
      fragment_id = string_view(
          args[FRAGMENT_ID_KEY].GetString(), args[FRAGMENT_ID_KEY].GetStringLength());
    }
    *out_val = profile.GetOperator(node_id, fragment_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_average_operator") {
    string_view node_id;
    if (args.HasMember(NODE_ID_KEY) && args[NODE_ID_KEY].IsString()) {
      node_id = string_view(
          args[NODE_ID_KEY].GetString(), args[NODE_ID_KEY].GetStringLength());
    } else {
      return Status(Substitute(MISSING_ARG_ERROR_MSG, string(tool_name), NODE_ID_KEY));
    }
    *out_val = profile.GetAveragedOperator(node_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_all_operators") {
    string_view node_id;
    string_view fragment_id;
    if (args.HasMember(NODE_ID_KEY) && args[NODE_ID_KEY].IsString()) {
      node_id = string_view(
          args[NODE_ID_KEY].GetString(), args[NODE_ID_KEY].GetStringLength());
    } else {
      return Status(Substitute(MISSING_ARG_ERROR_MSG, string(tool_name), NODE_ID_KEY));
    }
    if (args.HasMember(FRAGMENT_ID_KEY) && args[FRAGMENT_ID_KEY].IsString()) {
      fragment_id = string_view(
          args[FRAGMENT_ID_KEY].GetString(), args[FRAGMENT_ID_KEY].GetStringLength());
    } else {
      return Status(
          Substitute(MISSING_ARG_ERROR_MSG, string(tool_name), FRAGMENT_ID_KEY));
    }
    *out_val = profile.GetAllOperators(node_id, fragment_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_specific_operator") {
    string_view node_id;
    string_view fragment_id;
    string_view instance_id;
    if (args.HasMember(NODE_ID_KEY) && args[NODE_ID_KEY].IsString()) {
      node_id = string_view(
          args[NODE_ID_KEY].GetString(), args[NODE_ID_KEY].GetStringLength());
    } else {
      return Status(Substitute(MISSING_ARG_ERROR_MSG, string(tool_name), NODE_ID_KEY));
    }
    if (args.HasMember(FRAGMENT_ID_KEY) && args[FRAGMENT_ID_KEY].IsString()) {
      fragment_id = string_view(
          args[FRAGMENT_ID_KEY].GetString(), args[FRAGMENT_ID_KEY].GetStringLength());
    } else {
      return Status(
          Substitute(MISSING_ARG_ERROR_MSG, string(tool_name), FRAGMENT_ID_KEY));
    }
    if (args.HasMember(INSTANCE_ID_KEY) && args[INSTANCE_ID_KEY].IsString()) {
      instance_id = string_view(
          args[INSTANCE_ID_KEY].GetString(), args[INSTANCE_ID_KEY].GetStringLength());
    } else {
      return Status(
          Substitute(MISSING_ARG_ERROR_MSG, string(tool_name), INSTANCE_ID_KEY));
    }
    *out_val = profile.GetSpecificOperator(node_id, fragment_id, instance_id, alloc);
    return Status::OK();
  }
  if (tool_name == "get_scan_nodes_summary") {
    *out_val = profile.GetScanNodesSummary(alloc);
    return Status::OK();
  }
  if (tool_name == "get_per_node_profiles") {
    *out_val = profile.GetPerNodeProfiles(alloc);
    return Status::OK();
  }
  if (tool_name == "get_query_options") {
    *out_val = profile.GetQueryOptions(alloc);
    return Status::OK();
  }
  if (tool_name == "get_tables_queried") {
    *out_val = profile.GetTablesQueried(alloc);
    return Status::OK();
  }
  if (tool_name == "get_resource_estimates") {
    *out_val = profile.GetResourceEstimates(alloc);
    return Status::OK();
  }
  if (tool_name == "get_analyzed_query") {
    out_val->SetObject();
    const string analyzed_query = profile.GetAnalyzedQuery();
    out_val->AddMember("analyzed_query", MakeJsonString(analyzed_query, alloc), alloc);
    return Status::OK();
  }
  return Status(TOOL_UNKNOWN_PREFIX + string(tool_name));
}

// Public API: parses one profile and returns a reusable query-profile tool executor.
Status CreateQueryProfileToolExecutorForProfile(
    const string& profile_text, QueryProfileToolExecutor* tool_executor,
    int64_t profile_size_limit_bytes) {
  if (tool_executor == nullptr) {
    return Status("tool executor pointer cannot be null");
  }
  if (profile_text.empty()) return Status("Query profile is empty.");
  const int64_t effective_profile_size_limit_bytes = profile_size_limit_bytes > 0
      ? profile_size_limit_bytes
      : ComputeDefaultProfileSizeLimitBytes(
            DEFAULT_PARSING_PROFILE_SIZE_LIMIT_MAX_BYTES,
            DEFAULT_PARSING_PROFILE_SIZE_LIMIT_PERCENTAGE);
  if (profile_text.size() > static_cast<size_t>(effective_profile_size_limit_bytes)) {
    LOG(WARNING) << "Query profile parsing failed because input size "
                 << profile_text.size()
                 << " bytes exceeds configured profile size limit "
                 << effective_profile_size_limit_bytes << " bytes";
    return Status("Query profile size exceeds configured parsing profile size limit");
  }

  ParsedProfile parsed;
  RETURN_IF_ERROR(ParseProfile(profile_text, &parsed));
  auto profile = make_shared<QueryProfileToolAccessor>(move(parsed));

  *tool_executor =
      [profile](string_view tool_name, string_view tool_args_json,
          string* tool_output_json) {
        if (tool_output_json == nullptr) {
          return Status("tool output pointer cannot be null");
        }
        if (tool_name.empty()) return Status("tool_name cannot be empty.");

        Document args_doc;
        if (!tool_args_json.empty()) {
          args_doc.Parse(tool_args_json.data(),
              static_cast<rapidjson::SizeType>(tool_args_json.size()));
          if (args_doc.HasParseError()) {
            const int parse_error_code =
                static_cast<int>(args_doc.GetParseError());
            return Status("tool_args_json must be a valid JSON object: parse error '" +
                string(rapidjson::GetParseError_En(args_doc.GetParseError()))
                + "' (error code " + std::to_string(parse_error_code)
                + ") at offset " + std::to_string(args_doc.GetErrorOffset()));
          }
          if (!args_doc.IsObject()) {
            return Status("tool_args_json must be a valid JSON object");
          }
        } else {
          args_doc.SetObject();
        }

        Document out_doc;
        Value out;
        RETURN_IF_ERROR(
            ExecuteTool(tool_name, args_doc, *profile, out_doc.GetAllocator(), &out));
        *tool_output_json = JsonToString(out);
        return Status::OK();
      };
  return Status::OK();
}

} // namespace impala
