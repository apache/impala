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

#include "service/query-profile-redaction.h"
#include "service/query-profile-size-limit-util.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arpa/inet.h>
#include <boost/algorithm/string.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>

#include "common/logging.h"

using namespace std;
using rapidjson::Document;
using rapidjson::Value;

namespace impala {

static constexpr const char* REDACTED_SQL_STATEMENT = "[REDACTED_SQL_STATEMENT]";
static constexpr int64_t DEFAULT_REDACTION_PROFILE_SIZE_LIMIT_MAX_BYTES =
    256L * 1024L * 1024L;
static constexpr int64_t DEFAULT_REDACTION_PROFILE_SIZE_LIMIT_PERCENTAGE = 1;
// Matches hostnames followed by a port (e.g. coordinator.example.com:22000).
static const re2::RE2 HOST_WITH_PORT_RE(
    R"(\b([A-Za-z][A-Za-z0-9-]*(?:\.[A-Za-z0-9-]+)*)\:(\d{2,5})\b)");
// Matches the analyzed query subsection embedded in the textual plan.
static const re2::RE2 ANALYZED_RE(
    R"(Analyzed query:\s*([\s\S]*?)\n\nF\d+:PLAN FRAGMENT)");
// Matches fully-qualified identifiers with at least one dot (e.g. db.tbl.col).
static const re2::RE2 QUALIFIED_ID_RE(
    R"(\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\b)");
// Matches table tokens following FROM/JOIN clauses.
static const re2::RE2 FROM_JOIN_TABLE_RE(
    R"((?i:\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_\.]*)\b))");
// Matches snake_case identifiers that are candidates for column tokens.
static const re2::RE2 SNAKE_CASE_ID_RE(
    R"(\b([A-Za-z_][A-Za-z0-9_]*_[A-Za-z0-9_]*)\b)");
// Matches e-mail addresses.
static const re2::RE2 EMAIL_RE(
    R"(([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}))");
// Matches user/userid key-value pairs like user=alice.
static const re2::RE2 USER_KV_RE(
    R"((?i:\b(?:user|uid)=([A-Za-z0-9._@-]+)\b))");
// Matches IPv4 addresses.
static const re2::RE2 IPV4_RE(
    R"((\b(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3})"
    R"((?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b))");
// Collects IPv6-like candidates; inet_pton() validates true IPv6 values later.
static const re2::RE2 IPV6_CANDIDATE_RE(
    R"(([0-9A-Fa-f:]*:[0-9A-Fa-f:]+))");

// Builds deterministic SQL placeholders while keeping the first token stable.
static string BuildRedactedSqlPlaceholder(size_t index) {
  if (index <= 1) return REDACTED_SQL_STATEMENT;
  return string("[REDACTED_SQL_STATEMENT_") + std::to_string(index) + "]";
}

// Collects unique regex matches from text using the first capture group.
static vector<string> CollectRegexMatchesInternal(
    const string_view& text, const re2::RE2& pattern) {
  unordered_set<string_view> seen_matches;
  vector<string> results;
  re2::StringPiece remaining(text.data(), text.size());
  re2::StringPiece token_match;
  while (re2::RE2::FindAndConsume(&remaining, pattern, &token_match)) {
    const string_view token(token_match.data(), token_match.size());
    if (!seen_matches.insert(token).second) continue;
    results.emplace_back(token);
  }
  return results;
}

// Validates an IPv6 candidate with inet_pton() to avoid false positives like timestamps.
static bool IsValidIpv6Address(const string& candidate) {
  unsigned char address[16];
  return inet_pton(AF_INET6, candidate.c_str(), address) == 1;
}

// Adds a prefixed alias set into a global map while tracking reverse lookups.
template <typename TokenCollection>
static size_t AddAliasesByPrefix(const TokenCollection& tokens, const string_view& prefix,
    unordered_map<string, string>* global_aliases,
    unordered_map<string, string>* alias_to_original) {
  DCHECK(global_aliases != nullptr);
  DCHECK(alias_to_original != nullptr);
  size_t idx = 0;
  size_t inserted_count = 0;
  for (const string& token : tokens) {
    if (token.empty() || global_aliases->find(token) != global_aliases->end()) continue;
    string alias(prefix);
    alias.push_back('_');
    string idx_string = std::to_string(++idx);
    if (idx_string.size() < 3) alias.append(3 - idx_string.size(), '0');
    alias.append(idx_string);
    global_aliases->emplace(token, alias);
    alias_to_original->emplace(move(alias), token);
    ++inserted_count;
  }
  return inserted_count;
}

// Materializes replacement map entries as string_views and sorts them
// deterministically for safe replacement order.
static vector<pair<string_view, string_view>> GetSortedReplacementEntries(
    const unordered_map<string, string>& replacements) {
  vector<pair<string_view, string_view>> entries;
  entries.reserve(replacements.size());
  for (const auto& entry : replacements) {
    entries.emplace_back(entry.first, entry.second);
  }
  // Sort longer source tokens first so nested/overlapping names are replaced atomically
  // (e.g. replacing "table.column" before "table") and avoid partial matches.
  sort(entries.begin(), entries.end(),
      [](const auto& a, const auto& b) {
        if (a.first.size() != b.first.size()) return a.first.size() > b.first.size();
        return a.first < b.first;
      });
  return entries;
}

// Returns true if the character is part of a SQL identifier token.
static inline bool IsIdentifierChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

// Applies pre-sorted replacements in a single left-to-right pass.
static Status ApplyAliasEntries(const vector<pair<string_view, string_view>>& entries,
    const string_view& text, string* output) {
  DCHECK(output != nullptr);
  if (entries.empty()) {
    *output = string(text);
    return Status::OK();
  }

  string result;
  result.reserve(text.size());

  size_t i = 0;
  while (i < text.size()) {
    bool matched = false;
    for (const auto& [from, to] : entries) {
      if (from.empty() || text.size() - i < from.size()) continue;
      if (text.compare(i, from.size(), from) != 0) continue;

      bool start_boundary_ok = (i == 0) || !IsIdentifierChar(text[i - 1]);
      const size_t after_idx = i + from.size();
      const bool end_boundary_ok =
          (after_idx == text.size()) || !IsIdentifierChar(text[after_idx]);
      if (!start_boundary_ok || !end_boundary_ok) continue;

      result.append(to.data(), to.size());
      i += from.size();
      matched = true;
      break;
    }
    if (!matched) {
      result.push_back(text[i]);
      ++i;
    }
  }

  *output = move(result);
  return Status::OK();
}

// Applies a full alias map to a text blob in a single left-to-right pass.
static Status ApplyAliasMap(const unordered_map<string, string>& alias_map,
    const string_view& text, string* output) {
  DCHECK(output != nullptr);
  return ApplyAliasEntries(GetSortedReplacementEntries(alias_map), text, output);
}

// Extracts the analyzed query section from a plan text block.
static string ExtractAnalyzedQueryFromPlanText(const string_view& plan_text) {
  re2::StringPiece input(plan_text.data(), plan_text.size());
  string extracted_query;
  if (!re2::RE2::PartialMatch(input, ANALYZED_RE, &extracted_query)) {
    return string();
  }
  return boost::algorithm::trim_copy(extracted_query);
}

// Parses an info_strings entry and returns validated key/value pointers.
static pair<const char*, const char*> ParseInfoStringEntry(const Value& entry) {
  if (!entry.IsObject() || !entry.HasMember("key") || !entry["key"].IsString()
      || !entry.HasMember("value") || !entry["value"].IsString()) {
    return {nullptr, nullptr};
  }
  return {entry["key"].GetString(), entry["value"].GetString()};
}

// Recursive helper used by CollectIdentifierContextsFromJsonProfile().
static void CollectIdentifierContextsFromJsonProfileImpl(
    const Value& node, vector<string>* contexts) {
  DCHECK(contexts != nullptr);
  if (node.IsArray()) {
    for (const auto& item : node.GetArray()) {
      CollectIdentifierContextsFromJsonProfileImpl(item, contexts);
    }
    return;
  }
  if (!node.IsObject()) return;
  for (auto it = node.MemberBegin(); it != node.MemberEnd(); ++it) {
    if (strcmp(it->name.GetString(), "info_strings") == 0) {
      if (!it->value.IsArray()) continue;
      for (const auto& entry : it->value.GetArray()) {
        const auto [key, value] = ParseInfoStringEntry(entry);
        if (key == nullptr || value == nullptr) continue;
        if (strcmp(key, "Select Columns") == 0 || strcmp(key, "Where Columns") == 0
            || strcmp(key, "Join Columns") == 0
            || strcmp(key, "Analyzed query") == 0) {
          if (*value != '\0') contexts->emplace_back(value);
        } else if (strcmp(key, "Plan") == 0) {
          string analyzed_query = ExtractAnalyzedQueryFromPlanText(value);
          if (!analyzed_query.empty()) contexts->emplace_back(move(analyzed_query));
        }
      }
      continue;
    }
    CollectIdentifierContextsFromJsonProfileImpl(it->value, contexts);
  }
}

// Recursively collects profile text contexts that contain query identifiers.
static vector<string> CollectIdentifierContextsFromJsonProfile(const Value& node) {
  vector<string> contexts;
  CollectIdentifierContextsFromJsonProfileImpl(node, &contexts);
  return contexts;
}

// Recursive helper used by CollectInfoStringValuesByKeys().
static void CollectInfoStringValuesByKeysImpl(const Value& node,
    const unordered_set<string_view>& target_keys, vector<string>* values) {
  DCHECK(values != nullptr);
  if (node.IsArray()) {
    for (const auto& item : node.GetArray()) {
      CollectInfoStringValuesByKeysImpl(item, target_keys, values);
    }
    return;
  }
  if (!node.IsObject()) return;

  for (auto it = node.MemberBegin(); it != node.MemberEnd(); ++it) {
    if (strcmp(it->name.GetString(), "info_strings") == 0) {
      if (!it->value.IsArray()) continue;
      for (const auto& entry : it->value.GetArray()) {
        const auto [key, value] = ParseInfoStringEntry(entry);
        if (key == nullptr || value == nullptr) continue;
        if (target_keys.find(key) == target_keys.end()) continue;
        if (*value != '\0') values->emplace_back(value);
      }
      continue;
    }
    CollectInfoStringValuesByKeysImpl(it->value, target_keys, values);
  }
}

// Recursively collects info_strings values whose keys match the target set.
static vector<string> CollectInfoStringValuesByKeys(
    const Value& node, const unordered_set<string_view>& target_keys) {
  vector<string> values;
  CollectInfoStringValuesByKeysImpl(node, target_keys, &values);
  return values;
}

// Recursive helper used by CollectStringValuesFromJson().
static void CollectStringValuesFromJsonImpl(
    const Value& node, vector<string_view>* values) {
  DCHECK(values != nullptr);
  if (node.IsString()) {
    values->emplace_back(node.GetString(), node.GetStringLength());
  } else if (node.IsArray()) {
    for (const auto& item : node.GetArray()) {
      CollectStringValuesFromJsonImpl(item, values);
    }
  } else if (node.IsObject()) {
    for (auto it = node.MemberBegin(); it != node.MemberEnd(); ++it) {
      CollectStringValuesFromJsonImpl(it->value, values);
    }
  }
}

// Collects all string values from the profile JSON DOM.
static vector<string_view> CollectStringValuesFromJson(const Value& node) {
  vector<string_view> values;
  CollectStringValuesFromJsonImpl(node, &values);
  return values;
}

// Collects unique regex matches across a sequence of text inputs.
static vector<string> CollectRegexMatchesFromTexts(
    const vector<string_view>& texts, const re2::RE2& pattern) {
  unordered_set<string> seen;
  vector<string> results;
  for (const string_view& text : texts) {
    vector<string> matches = CollectRegexMatchesInternal(text, pattern);
    for (string& token : matches) {
      if (!seen.emplace(token).second) continue;
      results.emplace_back(move(token));
    }
  }
  return results;
}

// Collects unique IPv6 matches across a sequence of text inputs.
static vector<string> CollectIpv6MatchesFromTexts(const vector<string_view>& texts) {
  vector<string> candidates = CollectRegexMatchesFromTexts(texts, IPV6_CANDIDATE_RE);
  vector<string> results;
  for (string& candidate : candidates) {
    if (!IsValidIpv6Address(candidate)) continue;
    results.emplace_back(move(candidate));
  }
  return results;
}

class CountingOutputStream {
 public:
  using Ch = char;
  void Put(char) { ++size_bytes_; }
  void Flush() {}
  char Peek() const { return '\0'; }
  char Take() { return '\0'; }
  size_t Tell() const { return size_bytes_; }
  char* PutBegin() { return nullptr; }
  size_t PutEnd(char*) { return 0; }

 private:
  size_t size_bytes_ = 0;
};

// Estimates serialized JSON size in bytes directly from a DOM value.
static size_t EstimateSerializedJsonSize(const Value& value) {
  CountingOutputStream counting_output_stream;
  rapidjson::Writer<CountingOutputStream> writer(counting_output_stream);
  const bool write_success = value.Accept(writer);
  if (UNLIKELY(!write_success)) {
    LOG(WARNING) << "JSON size estimation failed. The estimated size "
                 << counting_output_stream.Tell() << " bytes is incomplete.";
  }
  DCHECK(write_success) << "JSON size estimation failed";
  return counting_output_stream.Tell();
}

// Extracts hostnames from specific profile sections that include host:port lists.
static vector<string> ExtractHostTokensFromPerHostSections(const Value& source_json) {
  static const unordered_set<string_view> HOST_SECTION_KEYS = {
      "Per Host Min Memory Reservation"};
  vector<string> host_tokens;
  vector<string> host_sections =
      CollectInfoStringValuesByKeys(source_json, HOST_SECTION_KEYS);
  for (const string& section : host_sections) {
    vector<string> section_hosts =
        CollectRegexMatchesInternal(section, HOST_WITH_PORT_RE);
    host_tokens.insert(host_tokens.end(), make_move_iterator(section_hosts.begin()),
        make_move_iterator(section_hosts.end()));
  }
  return host_tokens;
}

// Extracts candidate table and column tokens from SQL-like context strings.
static pair<vector<string>, vector<string>> ExtractTableAndColumnTokens(
    const vector<string>& contexts) {
  unordered_set<string_view> table_set;
  unordered_set<string_view> column_set;
  unordered_set<string_view> table_leaf_set;
  vector<string> table_tokens;
  vector<string> column_tokens;

  for (const string& context : contexts) {
    re2::StringPiece qualified_remaining(context.data(), context.size());
    re2::StringPiece qualified_match;
    while (re2::RE2::FindAndConsume(&qualified_remaining, QUALIFIED_ID_RE,
        &qualified_match)) {
      const string_view fq(qualified_match.data(), qualified_match.size());
      const size_t last_dot = fq.rfind('.');
      if (last_dot == string_view::npos || last_dot == 0
          || last_dot + 1 >= fq.size()) {
        continue;
      }
      const string_view table = fq.substr(0, last_dot);
      const string_view col = fq.substr(last_dot + 1);
      if (table_set.insert(table).second) table_tokens.emplace_back(table);
      if (column_set.insert(col).second) column_tokens.emplace_back(col);
      const size_t table_last_dot = table.rfind('.');
      table_leaf_set.insert(
          table_last_dot == string_view::npos ? table : table.substr(table_last_dot + 1));
    }

    re2::StringPiece table_remaining(context.data(), context.size());
    re2::StringPiece table_match;
    while (re2::RE2::FindAndConsume(&table_remaining, FROM_JOIN_TABLE_RE, &table_match)) {
      const string_view table(table_match.data(), table_match.size());
      if (table_set.insert(table).second) table_tokens.emplace_back(table);
      const size_t table_last_dot = table.rfind('.');
      table_leaf_set.insert(
          table_last_dot == string_view::npos ? table : table.substr(table_last_dot + 1));
    }

    re2::StringPiece snake_case_remaining(context.data(), context.size());
    re2::StringPiece snake_case_match;
    while (re2::RE2::FindAndConsume(&snake_case_remaining, SNAKE_CASE_ID_RE,
        &snake_case_match)) {
      const string_view token(snake_case_match.data(), snake_case_match.size());
      if (table_set.find(token) != table_set.end()) continue;
      if (table_leaf_set.find(token) != table_leaf_set.end()) continue;
      if (column_set.insert(token).second) column_tokens.emplace_back(token);
    }
  }
  return {table_tokens, column_tokens};
}

// Builds deterministic alias maps for profile redaction and unredaction.
static Status BuildRedactionAliasMaps(const Value& source_json,
    unordered_map<string, string>* global_aliases,
    unordered_map<string, string>* alias_to_original) {
  DCHECK(global_aliases != nullptr);
  DCHECK(alias_to_original != nullptr);
  global_aliases->clear();
  alias_to_original->clear();
  const vector<string_view> profile_strings = CollectStringValuesFromJson(source_json);

  const vector<string> sql_statements =
      CollectInfoStringValuesByKeys(source_json, {"Sql Statement"});
  for (size_t idx = 0; idx < sql_statements.size(); ++idx) {
    const string& sql_statement = sql_statements[idx];
    if (sql_statement.empty()) continue;
    const string placeholder = BuildRedactedSqlPlaceholder(idx + 1);
    global_aliases->emplace(sql_statement, placeholder);
    alias_to_original->emplace(placeholder, sql_statement);
  }

  vector<string> user_values = CollectInfoStringValuesByKeys(
      source_json, {"User", "Connected User", "Delegated User"});
  vector<string> emails = CollectRegexMatchesFromTexts(profile_strings, EMAIL_RE);
  vector<string> user_kvs = CollectRegexMatchesFromTexts(profile_strings, USER_KV_RE);
  user_values.insert(user_values.end(), make_move_iterator(emails.begin()),
      make_move_iterator(emails.end()));
  user_values.insert(user_values.end(), make_move_iterator(user_kvs.begin()),
      make_move_iterator(user_kvs.end()));
  const size_t username_count =
      AddAliasesByPrefix(user_values, "user", global_aliases, alias_to_original);

  vector<string> all_ip_tokens = CollectRegexMatchesFromTexts(profile_strings, IPV4_RE);
  vector<string> ipv6_tokens = CollectIpv6MatchesFromTexts(profile_strings);
  all_ip_tokens.insert(all_ip_tokens.end(), make_move_iterator(ipv6_tokens.begin()),
      make_move_iterator(ipv6_tokens.end()));
  const size_t ip_count =
      AddAliasesByPrefix(all_ip_tokens, "ip", global_aliases, alias_to_original);

  vector<string> contexts = CollectIdentifierContextsFromJsonProfile(source_json);
  auto [table_tokens, column_tokens] = ExtractTableAndColumnTokens(contexts);
  const size_t table_count =
      AddAliasesByPrefix(table_tokens, "table", global_aliases, alias_to_original);
  const size_t column_count =
      AddAliasesByPrefix(column_tokens, "column", global_aliases, alias_to_original);

  const vector<string> host_tokens = ExtractHostTokensFromPerHostSections(source_json);
  const size_t host_count =
      AddAliasesByPrefix(host_tokens, "host", global_aliases, alias_to_original);

  VLOG(1) << "Query profile redaction complete. Extracted items : "
          << "SQL statements: " << sql_statements.size() << ", "
          << "Users: " << username_count << ", "
          << "IPs: " << ip_count << ", "
          << "Tables: " << table_count << ", "
          << "Columns: " << column_count << ", "
          << "Hosts: " << host_count << ". "
          << "Total aliases generated: " << alias_to_original->size();

  return Status::OK();
}

// Recursively applies alias replacements in-place across a JSON value tree.
static Status RedactJsonValueInPlace(
    const vector<pair<string_view, string_view>>& alias_entries,
    Value* json_value, Document::AllocatorType& alloc) {
  DCHECK(json_value != nullptr);
  if (alias_entries.empty()) return Status::OK();

  if (json_value->IsString()) {
    const string_view original_value(
        json_value->GetString(), json_value->GetStringLength());
    string redacted_value;
    RETURN_IF_ERROR(ApplyAliasEntries(alias_entries, original_value, &redacted_value));
    if (redacted_value.size() == original_value.size()
        && memcmp(redacted_value.data(), original_value.data(),
               original_value.size())
            == 0) {
      return Status::OK();
    }
    json_value->SetString(redacted_value.data(),
        static_cast<rapidjson::SizeType>(redacted_value.size()), alloc);
    return Status::OK();
  }

  if (json_value->IsArray()) {
    for (auto& element : json_value->GetArray()) {
      RETURN_IF_ERROR(RedactJsonValueInPlace(alias_entries, &element, alloc));
    }
    return Status::OK();
  }

  if (!json_value->IsObject()) return Status::OK();
  for (auto member = json_value->MemberBegin(); member != json_value->MemberEnd();
       ++member) {
    const string_view original_key(
        member->name.GetString(), member->name.GetStringLength());
    string redacted_key;
    RETURN_IF_ERROR(ApplyAliasEntries(alias_entries, original_key, &redacted_key));
    if (redacted_key.size() != original_key.size()
        || memcmp(redacted_key.data(), original_key.data(), original_key.size()) != 0) {
      member->name.SetString(redacted_key.data(),
          static_cast<rapidjson::SizeType>(redacted_key.size()), alloc);
    }
    RETURN_IF_ERROR(RedactJsonValueInPlace(alias_entries, &member->value, alloc));
  }
  return Status::OK();
}

// Restores aliased placeholders in text using a reverse alias map.
static string UnredactTextWithAliases(
    const string_view& text, const unordered_map<string, string>& alias_to_original) {
  if (alias_to_original.empty()) return string(text);
  string unredacted;
  Status status = ApplyAliasMap(alias_to_original, text, &unredacted);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to unredact profile text: " << status.GetDetail();
    return string(text);
  }
  return unredacted;
}

static Status RedactSourceJson(const Value& source_json, int64_t profile_size_limit_bytes,
    unordered_map<string, string>* alias_to_original, Document* redacted_profile_json) {
  DCHECK(alias_to_original != nullptr);
  DCHECK(redacted_profile_json != nullptr);

  const size_t profile_size_bytes = EstimateSerializedJsonSize(source_json);
  if (profile_size_bytes > static_cast<size_t>(profile_size_limit_bytes)) {
    LOG(WARNING) << "Profile redaction failed because input size " << profile_size_bytes
                 << " bytes exceeds configured profile size limit "
                 << profile_size_limit_bytes << " bytes";
    return Status("Query profile size exceeds configured redaction profile size limit");
  }

  unordered_map<string, string> global_aliases;
  RETURN_IF_ERROR(
      BuildRedactionAliasMaps(source_json, &global_aliases, alias_to_original));
  const auto alias_entries = GetSortedReplacementEntries(global_aliases);
  redacted_profile_json->CopyFrom(source_json, redacted_profile_json->GetAllocator());
  RETURN_IF_ERROR(
      RedactJsonValueInPlace(alias_entries, redacted_profile_json,
          redacted_profile_json->GetAllocator()));
  DCHECK(redacted_profile_json->IsObject());
  return Status::OK();
}

QueryProfileRedactor::QueryProfileRedactor(int64_t profile_size_limit_bytes)
    : profile_size_limit_bytes_(
          profile_size_limit_bytes > 0
              ? profile_size_limit_bytes
              : ComputeDefaultProfileSizeLimitBytes(
                    DEFAULT_REDACTION_PROFILE_SIZE_LIMIT_MAX_BYTES,
                    DEFAULT_REDACTION_PROFILE_SIZE_LIMIT_PERCENTAGE)) {}

Status QueryProfileRedactor::Redact(const Value& profile_json) {
  DCHECK(redacted_profile_json_.IsNull()) << "Cannot call Redact function more than once";
  if (!profile_json.IsObject()) {
    LOG(WARNING)
        << "Profile redaction failed because input JSON root is not an object";
    return Status("Query profile input must be a valid JSON object");
  }
  RETURN_IF_ERROR(RedactSourceJson(profile_json, profile_size_limit_bytes_,
      &alias_to_original_, &redacted_profile_json_));
  redacted_profile_size_bytes_ = EstimateSerializedJsonSize(redacted_profile_json_);
  return Status::OK();
}

string QueryProfileRedactor::Unredact(const string_view& text) const {
  DCHECK(redacted_profile_json_.IsObject())
      << "Redact function has not been called, no profile to unredact";
  return UnredactTextWithAliases(text, alias_to_original_);
}

namespace test {
vector<string> CollectRegexMatches(string_view text, const re2::RE2& pattern) {
  return CollectRegexMatchesInternal(text, pattern);
}

vector<string> CollectStringValuesFromJsonForTest(const Value& node) {
  vector<string_view> values = impala::CollectStringValuesFromJson(node);
  vector<string> result;
  result.reserve(values.size());
  for (const string_view value : values) result.emplace_back(value);
  return result;
}

vector<string> CollectRegexMatchesFromTextsForTest(
    const vector<string>& texts, const string& pattern, size_t group_index) {
  DCHECK_LE(group_index, 1);
  vector<string_view> text_views;
  text_views.reserve(texts.size());
  for (const string& text : texts) text_views.emplace_back(text);
  string wrapped_pattern = pattern;
  if (group_index == 0) wrapped_pattern = "(" + pattern + ")";
  const re2::RE2 compiled_pattern(wrapped_pattern);
  DCHECK(compiled_pattern.ok()) << "invalid regex pattern: " << wrapped_pattern;
  return impala::CollectRegexMatchesFromTexts(text_views, compiled_pattern);
}

vector<string> CollectIpv6MatchesFromTextsForTest(const vector<string>& texts) {
  vector<string_view> text_views;
  text_views.reserve(texts.size());
  for (const string& text : texts) text_views.emplace_back(text);
  return impala::CollectIpv6MatchesFromTexts(text_views);
}

size_t EstimateSerializedJsonSizeForTest(const Value& value) {
  return impala::EstimateSerializedJsonSize(value);
}
} // namespace test

} // namespace impala
