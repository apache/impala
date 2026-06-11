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
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>

#include "common/logging.h"
#include "util/json-util.h"

using namespace std;
using rapidjson::Document;
using rapidjson::StringBuffer;
using rapidjson::Value;
using rapidjson::Writer;

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

// Builds deterministic SQL placeholders while keeping the first token stable.
static string BuildRedactedSqlPlaceholder(size_t index) {
  if (index <= 1) return REDACTED_SQL_STATEMENT;
  return string("[REDACTED_SQL_STATEMENT_") + std::to_string(index) + "]";
}

// Escapes a string so it can be safely matched within JSON-serialized text.
static string JsonEscapeString(const string_view& input) {
  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  writer.String(input.data(), static_cast<rapidjson::SizeType>(input.size()));
  const char* p = buffer.GetString();
  const size_t n = buffer.GetSize();
  const size_t start = n >= 1 && p[0] == '"' ? 1 : 0;
  const size_t end = n >= 1 && p[n - 1] == '"' ? n - 1 : n;
  if (end < start) return "";
  return string(p + start, end - start);
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

// Collects valid IPv6 tokens from text by scanning contiguous hex/colon ranges.
static vector<string> CollectIpv6Matches(const string_view& text) {
  unordered_set<string_view> seen_matches;
  vector<string> results;

  auto is_ipv6_char = [](char c) -> bool {
    return std::isxdigit(static_cast<unsigned char>(c)) || c == ':';
  };

  size_t pos = 0;
  while (pos < text.size()) {
    if (!is_ipv6_char(text[pos])) {
      ++pos;
      continue;
    }

    const size_t start = pos;
    bool has_colon = false;
    while (pos < text.size() && is_ipv6_char(text[pos])) {
      if (text[pos] == ':') has_colon = true;
      ++pos;
    }
    if (!has_colon) continue;

    const string_view candidate(text.data() + start, pos - start);
    if (!seen_matches.insert(candidate).second) continue;
    string candidate_text(candidate);
    if (!IsValidIpv6Address(candidate_text)) continue;
    results.emplace_back(move(candidate_text));
  }

  return results;
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

// Returns true if text[idx] is the escaped character in a JSON escape sequence.
// Example: for "\\n", idx should point to 'n'.
static inline bool IsEscapedJsonChar(const string_view& text, size_t idx) {
  if (idx == 0 || idx >= text.size()) return false;
  if (text[idx - 1] != '\\') return false;
  switch (text[idx]) {
    case '"':
    case '\\':
    case '/':
    case 'b':
    case 'f':
    case 'n':
    case 'r':
    case 't':
      return true;
    case 'u': {
      if (idx + 4 >= text.size()) return false;
      for (size_t i = idx + 1; i <= idx + 4; ++i) {
        if (!std::isxdigit(static_cast<unsigned char>(text[i]))) return false;
      }
      return true;
    }
    default:
      return false;
  }
}

// Applies a full alias map to a text blob in a single left-to-right pass.
static Status ApplyAliasMap(const unordered_map<string, string>& alias_map,
    const string_view& text, string* output) {
  DCHECK(output != nullptr);
  if (alias_map.empty()) {
    *output = string(text);
    return Status::OK();
  }

  const auto entries = GetSortedReplacementEntries(alias_map);

  string result;
  result.reserve(text.size());

  size_t i = 0;
  while (i < text.size()) {
    bool matched = false;
    for (const auto& [from, to] : entries) {
      if (from.empty() || text.size() - i < from.size()) continue;
      if (text.compare(i, from.size(), from) != 0) continue;

      bool start_boundary_ok = (i == 0) || !IsIdentifierChar(text[i - 1]);
      if (!start_boundary_ok && IsEscapedJsonChar(text, i - 1)) {
        start_boundary_ok = true;
      }
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

// Redacts sensitive profile values and optionally records alias-to-original mappings.
static Status RedactQueryProfileWithAliases(const string_view& profile_text,
    const Value& source_json,
    unordered_map<string, string>& alias_to_original, string* redacted) {
  DCHECK(redacted != nullptr);
  unordered_map<string, string> global_aliases;

  const vector<string> sql_statements =
      CollectInfoStringValuesByKeys(source_json, {"Sql Statement"});
  for (size_t idx = 0; idx < sql_statements.size(); ++idx) {
    const string& sql_statement = sql_statements[idx];
    if (sql_statement.empty()) continue;
    const string placeholder = BuildRedactedSqlPlaceholder(idx + 1);
    const string escaped_sql = JsonEscapeString(sql_statement);
    global_aliases.emplace(escaped_sql, placeholder);
    global_aliases.emplace(sql_statement, placeholder);
    // Unredaction runs against serialized JSON text, so restore escaped representation.
    alias_to_original.emplace(placeholder, escaped_sql);
  }

  vector<string> user_values = CollectInfoStringValuesByKeys(
      source_json, {"User", "Connected User", "Delegated User"});
  vector<string> emails = CollectRegexMatchesInternal(profile_text, EMAIL_RE);
  vector<string> user_kvs = CollectRegexMatchesInternal(profile_text, USER_KV_RE);
  user_values.insert(user_values.end(), make_move_iterator(emails.begin()),
      make_move_iterator(emails.end()));
  user_values.insert(user_values.end(), make_move_iterator(user_kvs.begin()),
      make_move_iterator(user_kvs.end()));
  const size_t username_count =
      AddAliasesByPrefix(user_values, "user", &global_aliases, &alias_to_original);

  vector<string> all_ip_tokens = CollectRegexMatchesInternal(profile_text, IPV4_RE);
  vector<string> ipv6_tokens = CollectIpv6Matches(profile_text);
  all_ip_tokens.insert(all_ip_tokens.end(), make_move_iterator(ipv6_tokens.begin()),
      make_move_iterator(ipv6_tokens.end()));
  const size_t ip_count =
      AddAliasesByPrefix(all_ip_tokens, "ip", &global_aliases, &alias_to_original);

  vector<string> contexts = CollectIdentifierContextsFromJsonProfile(source_json);
  auto [table_tokens, column_tokens] = ExtractTableAndColumnTokens(contexts);
  const size_t table_count =
      AddAliasesByPrefix(table_tokens, "table", &global_aliases, &alias_to_original);
  const size_t column_count =
      AddAliasesByPrefix(column_tokens, "column", &global_aliases, &alias_to_original);

  const vector<string> host_tokens = ExtractHostTokensFromPerHostSections(source_json);
  const size_t host_count =
      AddAliasesByPrefix(host_tokens, "host", &global_aliases, &alias_to_original);

  Status status = ApplyAliasMap(global_aliases, profile_text, redacted);
  if (!status.ok()) return status;

  VLOG(1) << "Query profile redaction complete. Extracted items : "
          << "SQL statements: " << sql_statements.size() << ", "
          << "Users: " << username_count << ", "
          << "IPs: " << ip_count << ", "
          << "Tables: " << table_count << ", "
          << "Columns: " << column_count << ", "
          << "Hosts: " << host_count << ". "
          << "Total aliases generated: " << alias_to_original.size();

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

  const string profile_text = JsonToString(source_json);
  if (profile_text.size() > static_cast<size_t>(profile_size_limit_bytes)) {
    LOG(WARNING) << "Profile redaction failed because input size " << profile_text.size()
                 << " bytes exceeds configured profile size limit "
                 << profile_size_limit_bytes << " bytes";
    return Status("Query profile size exceeds configured redaction profile size limit");
  }

  alias_to_original->clear();
  string redacted_profile_text;
  RETURN_IF_ERROR(RedactQueryProfileWithAliases(
      profile_text, source_json, *alias_to_original, &redacted_profile_text));

  redacted_profile_json->Parse(redacted_profile_text.data(),
      static_cast<rapidjson::SizeType>(redacted_profile_text.size()));
  if (redacted_profile_json->HasParseError()) {
    LOG(WARNING) << "Profile redaction failed while parsing redacted output JSON with "
                 << "RapidJSON error code: "
                 << redacted_profile_json->GetParseError() << " at offset: "
                 << redacted_profile_json->GetErrorOffset();
    return Status("Redacted query profile must be a valid JSON object");
  }
  if (!redacted_profile_json->IsObject()) {
    LOG(WARNING)
        << "Profile redaction failed because redacted JSON root is not an object";
    return Status("Redacted query profile must be a valid JSON object");
  }
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
  return RedactSourceJson(profile_json, profile_size_limit_bytes_,
      &alias_to_original_, &redacted_profile_json_);
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
} // namespace test

} // namespace impala
