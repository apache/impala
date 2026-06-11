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

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <rapidjson/document.h>
#include <re2/re2.h>

#include "common/status.h"

namespace impala {

// Redacts sensitive query-profile fields from a JSON profile blob and supports
// deterministic unredaction for follow-up text processing.
//
// Usage/lifecycle:
// 1) Create a QueryProfileRedactor instance.
// 2) Call Redact(profile_json) exactly once per instance.
// 3) Read redacted_profile_json() for redacted output.
// 4) Call Unredact(text) on any derived text that may contain aliases to map
//    aliases back to their original values.
//
// Redact() builds a deterministic alias map (original -> alias) and also stores
// the reverse map (alias -> original) used by Unredact(). SQL values are
// restored using their JSON-escaped representation because unredaction is
// applied against serialized JSON text.
//
// Redaction targets currently include:
// - SQL statements from "Sql Statement" fields:
//     "SELECT * FROM t" -> "[REDACTED_SQL_STATEMENT]"
//     second distinct statement -> "[REDACTED_SQL_STATEMENT_2]"
// - User identity tokens (profile user fields, e-mail addresses, user=...):
//     "alice@example.com" -> "user_001"
// - IP addresses (IPv4 + validated IPv6):
//     "10.10.10.10" -> "ip_001"
// - Table identifiers extracted from "Select Columns", "Where Columns",
//   "Join Columns", and "Analyzed query" profile sections:
//     "original_table_name" -> "table_001"
// - Column identifiers extracted from "Select Columns", "Where Columns",
//   "Join Columns", and "Analyzed query" profile sections:
//     "customer_id" -> "column_001"
// - Hostnames from host:port profile sections:
//     "coordinator.example.com" -> "host_001"
class QueryProfileRedactor {
 public:
  // If profile_size_limit_bytes is <= 0, a default limit is chosen as:
  // min(256 MB, 1% of process_mem_limit).
  // If process_mem_limit is unavailable, 256 MB is used.
  explicit QueryProfileRedactor(int64_t profile_size_limit_bytes = -1);

  Status Redact(const rapidjson::Value& profile_json);
  std::string Unredact(const std::string_view& text) const;
  const rapidjson::Document& redacted_profile_json() const {
    return redacted_profile_json_;
  }

 private:
  int64_t profile_size_limit_bytes_;
  rapidjson::Document redacted_profile_json_;
  std::unordered_map<std::string, std::string> alias_to_original_;
};

// Internal helper wrappers exposed for offline unit testing.
namespace test {
std::vector<std::string> CollectRegexMatches(
    std::string_view text, const re2::RE2& pattern);
} // namespace test

} // namespace impala
