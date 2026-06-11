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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <rapidjson/document.h>

#include "common/status.h"

namespace impala {

typedef rapidjson::Document ParsedProfile;

/// Provides read-only, tool-oriented accessors over a parsed query profile JSON tree.
///
/// Purpose:
/// - Encapsulates query-profile traversal/normalization logic behind stable helper
///   methods used by query profile tools (summary, fragments, nodes, timelines, etc.).
/// - Handles profile-shape differences (for example duplicated keys collected as arrays)
///   and query scoping decisions internally, so callers can request high-level sections.
///
/// Usage:
/// - Preferred path: call CreateQueryProfileToolExecutorForProfile() with a profile
///   JSON DOM value, then invoke tools through the returned QueryProfileToolExecutor.
/// - Direct path: construct QueryProfileToolAccessor with a ParsedProfile when the
///   profile is already parsed and validated by the caller.
///
/// Input format:
/// - Accepts profile data normalized by ParseProfile(). The source JSON must be either:
///   1) a root object containing "contents" (object) whose "profile_name" is a string, or
///   2) a root object with top-level "profile_name" (string).
/// - Profile sections are expected to be nested JSON objects/arrays built from the
///   profile text export.
/// - High-level accepted input shape:
///   {
///     "contents": {
///       "profile_name": "Query (id=...)",
///       "child_profiles": [ ... ],
///       "info_strings": [ {"key": "...", "value": ...}, ... ]
///     }
///   }
///
/// Output behavior:
/// - Accessor methods return JSON values (mostly objects or arrays depending on method).
/// - Methods that cannot find a section generally return an empty JSON object/array.
/// - Some lookup methods (for example fragment/node lookups) return an object containing
///   an "error" field when a specific requested item is not found.
class QueryProfileToolAccessor {
 public:
  explicit QueryProfileToolAccessor(ParsedProfile parsed);
  QueryProfileToolAccessor(const QueryProfileToolAccessor&) = delete;
  QueryProfileToolAccessor& operator=(const QueryProfileToolAccessor&) = delete;
  QueryProfileToolAccessor(QueryProfileToolAccessor&&) = delete;
  QueryProfileToolAccessor& operator=(QueryProfileToolAccessor&&) = delete;
  // Returns the selected query's "Summary" section as-is.
  // Example: {"Query State":"FINISHED","Plan":"..."}.
  rapidjson::Value GetSummary(rapidjson::Document::AllocatorType& alloc) const;
  // Returns execution event timings (labels -> "absolute (delta)").
  // Example: {"Planning":"2.3ms (2.3ms)","Submit for admission":"5.1ms (2.8ms)"}.
  rapidjson::Value GetTimeline(rapidjson::Document::AllocatorType& alloc) const;
  // Returns compilation event timings using the same shape as GetTimeline().
  rapidjson::Value GetCompilation(rapidjson::Document::AllocatorType& alloc) const;
  // Returns the selected "Execution Profile..." subtree.
  rapidjson::Value GetExecutionProfile(rapidjson::Document::AllocatorType& alloc) const;
  // Returns one entry per selected fragment with derived metadata.
  // Example row: {"fragment_id":"F00","full_key":"Fragment F00","instance_count":12}.
  rapidjson::Value GetFragmentsOverview(rapidjson::Document::AllocatorType& alloc) const;
  // Returns one averaged fragment object for a fragment id (e.g. "F00" or "00").
  rapidjson::Value GetFragment(std::string_view fragment_id,
      rapidjson::Document::AllocatorType& alloc) const;
  // Returns all matching fragment variants for a fragment id.
  rapidjson::Value GetAllFragments(
      std::string_view fragment_id, rapidjson::Document::AllocatorType& alloc) const;
  // Returns only instance children from the selected fragment.
  // Example keys: "Instance 1234...","Instance 5678...".
  rapidjson::Value GetFragmentInstances(
      std::string_view fragment_id, rapidjson::Document::AllocatorType& alloc) const;
  // Returns one best-matching operator section, optionally scoped to a fragment.
  // Example node_id: "45", "HDFS_SCAN_NODE", "HDFS_SCAN_NODE (id=45)".
  rapidjson::Value GetOperator(std::string_view node_id, std::string_view fragment_id,
      rapidjson::Document::AllocatorType& alloc) const;
  // Returns one best-matching averaged operator section for the node id.
  rapidjson::Value GetAveragedOperator(
      std::string_view node_id, rapidjson::Document::AllocatorType& alloc) const;
  // Returns all per-instance operator sections for the node id, including fragment
  // and instance(host) context.
  rapidjson::Value GetAllOperators(std::string_view node_id, std::string_view fragment_id,
      rapidjson::Document::AllocatorType& alloc) const;
  // Returns one specific per-instance operator section for the node id.
  // If instance_id is empty, returns the first deterministic match.
  rapidjson::Value GetSpecificOperator(std::string_view node_id,
      std::string_view fragment_id, std::string_view instance_id,
      rapidjson::Document::AllocatorType& alloc) const;
  // Returns compact metrics for scan nodes across fragments.
  rapidjson::Value GetScanNodesSummary(rapidjson::Document::AllocatorType& alloc) const;
  // Returns the "Per Node Profiles" object when present.
  rapidjson::Value GetPerNodeProfiles(rapidjson::Document::AllocatorType& alloc) const;
  // Parses summary options into {"option":"value"} form.
  // Example source: "MT_DOP=4, RUNTIME_FILTER_MODE=GLOBAL".
  rapidjson::Value GetQueryOptions(rapidjson::Document::AllocatorType& alloc) const;
  // Returns Summary "Tables Queried" as a string array.
  // Example: ["db.fact_sales","db.dim_date"].
  rapidjson::Value GetTablesQueried(rapidjson::Document::AllocatorType& alloc) const;
  // Returns reservation/estimate/coordinator resource fields when available.
  rapidjson::Value GetResourceEstimates(rapidjson::Document::AllocatorType& alloc) const;
  // Extracts the SQL text under "Analyzed query:" from the Plan field.
  std::string GetAnalyzedQuery() const;

 private:
  using FragmentEntry = std::pair<std::string, const rapidjson::Value*>;
  struct FragmentEntryLess {
    bool operator()(const FragmentEntry& lhs, const FragmentEntry& rhs) const {
      return lhs.first < rhs.first;
    }
  };
  struct FragmentPtrLess {
    bool operator()(const FragmentEntry* lhs, const FragmentEntry* rhs) const {
      if (lhs->first != rhs->first) return lhs->first < rhs->first;
      return lhs < rhs;
    }
  };

  std::string_view GetQueryType(const rapidjson::Value& query_obj) const;
  const rapidjson::Value* SelectScopedValue(const rapidjson::Value& value) const;
  const rapidjson::Value* GetScopedExecutionProfile() const;
  int DictComplexity(const rapidjson::Value& v) const;
  const rapidjson::Value* GetQueryData() const;
  const rapidjson::Value* GetSummaryPtr() const;
  rapidjson::Value BuildEventTimelineSection(
      int sequence_idx, rapidjson::Document::AllocatorType& alloc) const;
  bool IsFragmentKey(std::string_view key) const;
  void GatherFragments(const rapidjson::Value& data,
      std::vector<std::pair<std::string, const rapidjson::Value*>>* out) const;
  int ScoreNodeKeyMatch(std::string_view key, std::string_view node_id) const;
  int ScoreOperatorKeyMatch(std::string_view key, std::string_view node_id) const;
  void FindBestOperatorMatch(const rapidjson::Value& data, std::string_view node_id,
      std::string_view* best_key, const rapidjson::Value** best_match, int* best_score,
      int* best_complexity) const;
  const rapidjson::Value* SearchOperator(const rapidjson::Value& data,
      std::string_view node_id, std::string_view* matched_key = nullptr) const;
  void FindNodesByPattern(const rapidjson::Value& data, std::string_view pattern,
      std::vector<std::pair<std::string_view, const rapidjson::Value*>>* out) const;
  void FindNodesByPatternNormalized(const rapidjson::Value& data,
      const std::string& normalized_pattern,
      std::vector<std::pair<std::string_view, const rapidjson::Value*>>* out) const;
  double ParseDurationMs(std::string_view value) const;
  void AddOperatorRows(std::string_view text, rapidjson::Value* info,
      rapidjson::Document::AllocatorType& alloc) const;

  // Parsed root profile after ParseProfile() normalization.
  // Example top-level key: "Query (id=084f...):".
  ParsedProfile parsed_;
  // All fragment-like blocks discovered in the scoped execution profile.
  // Example key/value:
  //   key   = "Fragment F00 [2]"
  //   value = {"Instance 8b4...": {...}, "_value":"..." }
  std::vector<FragmentEntry> all_fragments_;
  // Deduplicated "best" fragment blocks, one representative per fragment id.
  // This keeps the most information-dense object when multiple keys map to
  // the same id.
  std::set<FragmentEntry, FragmentEntryLess> fragments_;
  struct StringViewPairHash {
    std::size_t operator()(
        const std::pair<std::string_view, std::string_view>& value) const {
      return std::hash<std::string_view>()(value.first)
          ^ (std::hash<std::string_view>()(value.second) << 1);
    }
  };
  // Selected query object key in parsed_.
  // Example: "Query (id=084f0b6e6340f5d9:6dd77f6f00000000):".
  std::string query_key_;
  // Query id extracted from query_key_.
  // Example: "084f0b6e6340f5d9:6dd77f6f00000000".
  std::string query_id_;
  // Cached top-level execution profile key derived from query_id_.
  std::string execution_profile_key_;
  // Index used when a section appears as an array across query scopes.
  // For QUERY statements, this tracks the matching query-type scope index.
  int query_type_query_scope_idx_ = 0;
};

using QueryProfileToolExecutor = std::function<Status(
    std::string_view tool_name, std::string_view tool_args_json,
    rapidjson::Document* tool_output_doc)>;

// Parses one already-materialized profile JSON object and returns a reusable tool
// executor.
//
// If profile_size_limit_bytes is <= 0, a default limit is chosen as:
// min(256 MB, 1% of process_mem_limit). If process_mem_limit is unavailable,
// 256 MB is used.
//
// If profile_size_bytes is 0, the function computes it by serializing
// profile_json, then rejects parsing when the effective size exceeds the
// effective profile size limit.
Status CreateQueryProfileToolExecutorForProfile(
    const rapidjson::Value& profile_json, QueryProfileToolExecutor* tool_executor,
    int64_t profile_size_limit_bytes = -1, size_t profile_size_bytes = 0);

// Internal helper wrappers exposed for offline unit testing.
namespace test {
double ParseDurationMs(std::string_view value);
} // namespace test

} // namespace impala
