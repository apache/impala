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

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/posix_time/time_parsers.hpp>
#include <gflags/gflags.h>

#include "common/object-pool.h"
#include "gutil/strings/strip.h"
#include "util/os-info.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/progress-util.h"
#include "util/runtime-profile.h"
#include "util/string-parser.h"

#include "common/names.h"

static const char* USAGE =
    "Utility to decode an Impala profile log or WebUI thrift profile download"
    " from standard input.\n"
    "\n"
    "The input is consumed from standard input and each successfully parsed profile"
    " is pretty-printed to standard output.\n"
    "\n"
    "Usage:\n"
    "  impala-profile-tool < impala_profile_log_1.1-1607057366897\n"
    "  impala-profile-tool < thrift_profile_<query id>.txt\n"
    "\n"
    "The following options are supported:\n"
    "Output options:\n"
    "--profile_format={text,json,prettyjson,summary}: controls\n"
    "   text (default): pretty-print in the standard human readable format\n"
    "   json: output as JSON with one profile per line. Compatible with jsonlines.org\n"
    "   prettyjson: output as pretty-printed JSON array with one element per object\n"
    "   summary: output one TSV row per profile with query-level summary fields\n"
    "--summary_text_length=<integer>: maximum text field length in summary output."
    " If <= 0, the full text is used\n"
    "--profile_verbosity={0,1,2,3,4,minimal,legacy,default,extended,full}: control"
    " verbosity of profile output. If not set, picks based on profile version\n"
    "\n"
    "Filtering options:\n"
    "--query_id=<query id>: given an impala query ID, only process profiles with this"
    " query id\n"
    "--min_timestamp=<Unix epoch milliseconds>: only process profiles at or"
    " after this timestamp\n"
    "--max_timestamp=<Unix epoch milliseconds>: only process profiles at or"
    " before this timestamp\n"
    "Filtering options only apply to profile log entries that include timestamp and"
    " query id metadata.\n";

DEFINE_string(
    profile_format, "text",
    "Profile format to output: either text, json, prettyjson or summary");
DEFINE_string(profile_verbosity, "", "Verbosity of profile output. Must be one of "
    "{0,1,2,3,4,minimal,legacy,default,extended,full}. If not set, picks based on "
    "version of each input profile.");
DEFINE_string(query_id, "", "Query ID to output profiles for");
DEFINE_int64(min_timestamp, -1,
    "Minimum timestamp in Unix epoch milliseconds (inclusive) to output profiles for");
DEFINE_int64(max_timestamp, -1,
    "Maximum timestamp in Unix epoch milliseconds (inclusive) to output profiles for");
DEFINE_int32(summary_text_length, 250,
    "Maximum text field length in summary output. If <= 0, the full text is used.");

using namespace impala;

using boost::algorithm::to_lower_copy;
using google::DescribeOneFlag;
using google::GetCommandLineFlagInfoOrDie;
using std::cerr;
using std::cin;
using std::cout;
using std::istringstream;
using std::ostream;

static const char* const SUMMARY_HEADER[] = {"Query ID", "User", "Default Db",
    "Query Type", "Start Time", "End Time", "Duration", "Queued Duration",
    "Mem Usage", "Mem Estimate", "Scan Progress", "Query Progress", "Bytes Read",
    "Bytes Sent", "Bytes Spilled", "State", "Query Status", "# rows fetched",
    "Resource Pool", "Statement"};
static const char* const SUMMARY_VALUE_UNAVAILABLE = "N/A";

static bool StartsWith(const string& str, const string& prefix) {
  return str.compare(0, prefix.size(), prefix) == 0;
}

static string GetProfileQueryId(const TRuntimeProfileTree& tree) {
  if (tree.nodes.empty()) return SUMMARY_VALUE_UNAVAILABLE;

  const string& root_name = tree.nodes[0].name;
  const string prefix = "Query (id=";
  const string suffix = ")";
  if (!StartsWith(root_name, prefix)
      || root_name.size() <= prefix.size() + suffix.size()) {
    return SUMMARY_VALUE_UNAVAILABLE;
  }
  size_t suffix_pos = root_name.size() - suffix.size();
  if (root_name.compare(suffix_pos, suffix.size(), suffix) != 0) {
    return SUMMARY_VALUE_UNAVAILABLE;
  }
  return root_name.substr(prefix.size(),
      root_name.size() - prefix.size() - suffix.size());
}

static const TRuntimeProfileNode* FindNodeByName(
    const TRuntimeProfileTree& tree, const string& name) {
  for (const TRuntimeProfileNode& node : tree.nodes) {
    if (node.name == name) return &node;
  }
  return nullptr;
}

static const TRuntimeProfileNode* FindNodeByPrefix(
    const TRuntimeProfileTree& tree, const string& prefix) {
  for (const TRuntimeProfileNode& node : tree.nodes) {
    if (StartsWith(node.name, prefix)) return &node;
  }
  return nullptr;
}

static string GetInfoString(
    const TRuntimeProfileNode* node, const string& key,
    const string& default_value = "") {
  if (node == nullptr) return default_value;
  auto it = node->info_strings.find(key);
  return it == node->info_strings.end() || it->second.empty() ? default_value :
      it->second;
}

static bool FindCounterValue(
    const TRuntimeProfileNode* node, const string& name, int64_t* value) {
  if (node == nullptr) return false;
  for (const TCounter& counter : node->counters) {
    if (counter.name == name) {
      *value = counter.value;
      return true;
    }
  }
  return false;
}

static bool IsAggregatedProfile(const TRuntimeProfileTree& tree) {
  return tree.__isset.profile_version && tree.profile_version >= 2;
}

static bool SumAggregatedCounterValues(
    const TRuntimeProfileTree& tree, const string& name, int64_t* value) {
  if (!IsAggregatedProfile(tree)) return false;
  *value = 0;
  bool found = false;
  // TRuntimeProfileTree::nodes is a single flat list containing every node in
  // the profile tree, laid out in pre-order traversal order.
  for (const TRuntimeProfileNode& node : tree.nodes) {
    if (node.__isset.aggregated && node.aggregated.__isset.counters) {
      for (const TAggCounter& counter : node.aggregated.counters) {
        if (counter.name != name) continue;
        size_t num_values = std::min(counter.values.size(), counter.has_value.size());
        for (size_t i = 0; i < num_values; ++i) {
          if (counter.has_value[i]) {
            *value += counter.values[i];
            found = true;
          }
        }
      }
    }
  }
  return found;
}

static size_t NextSiblingIndex(
    const vector<TRuntimeProfileNode>& nodes, size_t node_idx) {
  size_t next_idx = node_idx;
  int64_t nodes_remaining = 1;
  while (nodes_remaining > 0 && next_idx < nodes.size()) {
    nodes_remaining += nodes[next_idx].num_children;
    ++next_idx;
    --nodes_remaining;
  }
  return next_idx;
}

static bool SumScratchBytesWrittenForHosts(
    const vector<TRuntimeProfileNode>& nodes, size_t per_node_idx, int64_t* value) {
  bool found = false;
  const TRuntimeProfileNode& per_node = nodes[per_node_idx];
  size_t host_idx = per_node_idx + 1;
  for (int child = 0; child < per_node.num_children && host_idx < nodes.size();
       ++child) {
    int64_t counter_value = 0;
    if (FindCounterValue(&nodes[host_idx], "ScratchBytesWritten", &counter_value)) {
      *value += counter_value;
      found = true;
    }
    host_idx = NextSiblingIndex(nodes, host_idx);
  }
  return found;
}

static bool SumScratchBytesWrittenForPerNodeProfiles(
    const TRuntimeProfileTree& tree, int64_t* value) {
  *value = 0;
  bool found = false;
  for (size_t i = 0; i < tree.nodes.size(); ++i) {
    const TRuntimeProfileNode& node = tree.nodes[i];
    if (!StartsWith(node.name, "Execution Profile ")) continue;

    size_t child_idx = i + 1;
    for (int child = 0; child < node.num_children && child_idx < tree.nodes.size();
         ++child) {
      size_t next_child_idx = NextSiblingIndex(tree.nodes, child_idx);
      if (tree.nodes[child_idx].name == "Per Node Profiles") {
        found |= SumScratchBytesWrittenForHosts(tree.nodes, child_idx, value);
      }
      child_idx = next_child_idx;
    }
  }
  return found;
}

static bool SumCounterValuesInSubtreePrefixRecursive(
    const vector<TRuntimeProfileNode>& nodes, size_t* node_idx, bool in_matching_subtree,
    const string& prefix, const string& counter_name, int64_t* value) {
  const TRuntimeProfileNode& node = nodes[*node_idx];
  bool current_in_matching_subtree = in_matching_subtree || StartsWith(node.name, prefix);
  bool found = false;
  if (current_in_matching_subtree) {
    int64_t counter_value = 0;
    if (FindCounterValue(&node, counter_name, &counter_value)) {
      *value += counter_value;
      found = true;
    }
  }

  ++(*node_idx);
  for (int i = 0; i < node.num_children && *node_idx < nodes.size(); ++i) {
    found |= SumCounterValuesInSubtreePrefixRecursive(
        nodes, node_idx, current_in_matching_subtree, prefix, counter_name, value);
  }
  return found;
}

static bool SumCounterValuesInSubtreePrefix(const TRuntimeProfileTree& tree,
    const string& prefix, const string& counter_name, int64_t* value) {
  *value = 0;
  bool found = false;
  size_t node_idx = 0;
  while (node_idx < tree.nodes.size()) {
    found |= SumCounterValuesInSubtreePrefixRecursive(
        tree.nodes, &node_idx, false, prefix, counter_name, value);
  }
  return found;
}

static bool SumScanRangesComplete(const TRuntimeProfileTree& tree, int64_t* value) {
  if (IsAggregatedProfile(tree)) {
    return SumAggregatedCounterValues(tree, "ScanRangesComplete", value);
  }
  // V1 profiles include per-instance scan counters alongside averaged counters.
  return SumCounterValuesInSubtreePrefix(tree, "Instance ", "ScanRangesComplete", value);
}

static bool SumScratchBytesWritten(const TRuntimeProfileTree& tree, int64_t* value) {
  return SumScratchBytesWrittenForPerNodeProfiles(tree, value);
}

static bool GetEventTime(
    const TEventSequence& sequence, const string& label, int64_t* timestamp) {
  size_t num_events = std::min(sequence.labels.size(), sequence.timestamps.size());
  for (size_t i = 0; i < num_events; ++i) {
    if (sequence.labels[i] == label) {
      *timestamp = sequence.timestamps[i];
      return true;
    }
  }
  return false;
}

static bool GetTimelineDuration(
    const TRuntimeProfileNode* node, const string& name, int64_t* duration) {
  if (node == nullptr || !node->__isset.event_sequences) return false;
  for (const TEventSequence& sequence : node->event_sequences) {
    if (sequence.name != name || sequence.timestamps.empty()) continue;
    *duration = *std::max_element(sequence.timestamps.begin(), sequence.timestamps.end());
    return true;
  }
  return false;
}

static bool ParseTimestamp(const string& timestamp,
    boost::posix_time::ptime* parsed_timestamp) {
  if (timestamp.empty()) return false;

  string ts = timestamp;
  size_t dot = ts.find('.', 19);
  if (dot != string::npos && ts.size() > dot + 7) ts.resize(dot + 7);
  try {
    *parsed_timestamp = boost::posix_time::time_from_string(ts);
    return true;
  } catch (...) {
    return false;
  }
}

static string QueryDuration(const TRuntimeProfileNode* summary_node) {
  boost::posix_time::ptime start_time;
  boost::posix_time::ptime end_time;
  if (ParseTimestamp(GetInfoString(summary_node, "Start Time"), &start_time)
      && ParseTimestamp(GetInfoString(summary_node, "End Time"), &end_time)) {
    boost::posix_time::time_duration duration = end_time - start_time;
    if (!duration.is_negative()) {
      return PrettyPrinter::Print(duration.total_microseconds() * 1000, TUnit::TIME_NS);
    }
  }

  int64_t duration = 0;
  if (!GetTimelineDuration(summary_node, "Query Timeline", &duration)) {
    return SUMMARY_VALUE_UNAVAILABLE;
  }
  return PrettyPrinter::Print(duration, TUnit::TIME_NS);
}

static string QueuedDuration(const TRuntimeProfileNode* summary_node) {
  if (summary_node == nullptr || !summary_node->__isset.event_sequences) {
    return SUMMARY_VALUE_UNAVAILABLE;
  }
  for (const TEventSequence& sequence : summary_node->event_sequences) {
    if (sequence.name != "Query Timeline") continue;
    int64_t submitted = 0;
    int64_t admitted = 0;
    if (GetEventTime(sequence, "Submit for admission", &submitted)
        && GetEventTime(sequence, "Completed admission", &admitted)
        && admitted >= submitted) {
      return PrettyPrinter::Print(admitted - submitted, TUnit::TIME_NS);
    }
  }
  return SUMMARY_VALUE_UNAVAILABLE;
}

static bool ParseBytesValue(const string& value, int64_t* bytes) {
  string mem_spec = value;
  mem_spec.erase(std::remove_if(mem_spec.begin(), mem_spec.end(), [](char c) {
    return std::isspace(static_cast<unsigned char>(c));
  }), mem_spec.end());
  if (mem_spec.empty() || StartsWith(mem_spec, "-")) return false;

  bool is_percent = false;
  int64_t parsed_bytes = ParseUtil::ParseMemSpec(mem_spec, &is_percent, 0);
  if (is_percent || parsed_bytes < 0) return false;
  *bytes = parsed_bytes;
  return true;
}

static bool SumFileFormatScanRanges(const string& info_string, int multiplier,
    int64_t* total_scan_ranges) {
  bool found = false;
  size_t pos = 0;
  while (pos < info_string.size()) {
    size_t colon = info_string.find(':', pos);
    if (colon == string::npos) break;
    const char* count_start = info_string.c_str() + colon + 1;
    char* count_end = nullptr;
    int64_t count = std::strtoll(count_start, &count_end, 10);
    if (count_end != count_start && count > 0) {
      *total_scan_ranges += count * multiplier;
      found = true;
      pos = count_end - info_string.c_str();
    } else {
      pos = colon + 1;
    }
  }
  return found;
}

static bool SumFileFormatScanRangesForParentRecursive(
    const vector<TRuntimeProfileNode>& nodes, size_t* node_idx, bool parent_matches,
    const string& parent_name, int64_t* total_scan_ranges) {
  const TRuntimeProfileNode& node = nodes[*node_idx];
  bool found = false;
  if (parent_matches) {
    string file_formats = GetInfoString(&node, "File Formats");
    if (!file_formats.empty()) {
      found |= SumFileFormatScanRanges(file_formats, 1, total_scan_ranges);
    }
  }

  bool current_matches = StartsWith(node.name, parent_name);
  ++(*node_idx);
  for (int i = 0; i < node.num_children && *node_idx < nodes.size(); ++i) {
    found |= SumFileFormatScanRangesForParentRecursive(
        nodes, node_idx, current_matches, parent_name, total_scan_ranges);
  }
  return found;
}

static bool SumFileFormatScanRangesForParent(
    const TRuntimeProfileTree& tree, const string& parent_name, int64_t* value) {
  *value = 0;
  bool found = false;
  size_t node_idx = 0;
  while (node_idx < tree.nodes.size()) {
    found |= SumFileFormatScanRangesForParentRecursive(
        tree.nodes, &node_idx, false, parent_name, value);
  }
  return found;
}

static bool SumAggregatedFileFormatScanRanges(
    const TRuntimeProfileTree& tree, int64_t* value) {
  if (!IsAggregatedProfile(tree)) return false;
  *value = 0;
  bool found = false;
  for (const TRuntimeProfileNode& node : tree.nodes) {
    if (!node.__isset.aggregated || !node.aggregated.__isset.info_strings) continue;
    auto file_formats = node.aggregated.info_strings.find("File Formats");
    if (file_formats == node.aggregated.info_strings.end()) continue;
    for (const auto& entry : file_formats->second) {
      found |= SumFileFormatScanRanges(
          entry.first, static_cast<int>(entry.second.size()), value);
    }
  }
  return found;
}

static bool SumTotalScanRanges(const TRuntimeProfileTree& tree, int64_t* value) {
  if (IsAggregatedProfile(tree)) return SumAggregatedFileFormatScanRanges(tree, value);
  return SumFileFormatScanRangesForParent(tree, "Instance ", value);
}

static bool ParsePerNodeMemoryUsage(const string& info_string, int64_t* total_bytes) {
  *total_bytes = 0;
  bool found = false;
  size_t pos = 0;
  while (pos < info_string.size()) {
    size_t open = info_string.find('(', pos);
    if (open == string::npos) break;
    size_t close = info_string.find(')', open + 1);
    if (close == string::npos) break;
    int64_t bytes = 0;
    if (ParseBytesValue(info_string.substr(open + 1, close - open - 1), &bytes)) {
      found = true;
      *total_bytes += bytes;
    }
    pos = close + 1;
  }
  return found;
}

static string MemoryUsage(const TRuntimeProfileNode* execution_node) {
  int64_t total_bytes = 0;
  // Archived profiles keep query-level peak memory as this coordinator info string.
  if (ParsePerNodeMemoryUsage(
          GetInfoString(execution_node, "Per Node Peak Memory Usage"), &total_bytes)) {
    return PrettyPrinter::Print(total_bytes, TUnit::BYTES);
  }
  return SUMMARY_VALUE_UNAVAILABLE;
}

static string MemoryEstimate(const TRuntimeProfileNode* summary_node) {
  return GetInfoString(
      summary_node, "Cluster Memory Admitted", SUMMARY_VALUE_UNAVAILABLE);
}

static string ScanProgress(const TRuntimeProfileTree& tree, const string& query_state) {
  int64_t completed_scan_ranges = 0;
  if (!SumScanRangesComplete(tree, &completed_scan_ranges) || completed_scan_ranges < 0) {
    return SUMMARY_VALUE_UNAVAILABLE;
  }

  int64_t total_scan_ranges = 0;
  if (SumTotalScanRanges(tree, &total_scan_ranges) && total_scan_ranges > 0) {
    return ProgressToString(completed_scan_ranges, total_scan_ranges);
  }

  // Older profile logs may not keep total scan ranges, but finished queries are 100%
  // done.
  if (query_state == "FINISHED" && completed_scan_ranges > 0) {
    return ProgressToString(completed_scan_ranges, completed_scan_ranges);
  }
  return SUMMARY_VALUE_UNAVAILABLE;
}

static string QueryProgress(
    const TRuntimeProfileNode* execution_node, const string& query_state) {
  int64_t num_fragment_instances = 0;
  if (query_state == "FINISHED"
      && FindCounterValue(
          execution_node, "NumFragmentInstances", &num_fragment_instances)
      && num_fragment_instances > 0) {
    return ProgressToString(num_fragment_instances, num_fragment_instances);
  }
  return SUMMARY_VALUE_UNAVAILABLE;
}

static string CounterValue(
    const TRuntimeProfileNode* node, const string& counter_name, TUnit::type unit) {
  int64_t value = 0;
  if (!FindCounterValue(node, counter_name, &value)) return SUMMARY_VALUE_UNAVAILABLE;
  return PrettyPrinter::Print(value, unit);
}

static string BytesSpilled(const TRuntimeProfileTree& tree) {
  int64_t value = 0;
  if (!SumScratchBytesWritten(tree, &value)) return SUMMARY_VALUE_UNAVAILABLE;
  return PrettyPrinter::Print(value, TUnit::BYTES);
}

static string RowsFetched(const TRuntimeProfileNode* impala_server_node) {
  int64_t rows_fetched = 0;
  if (!FindCounterValue(impala_server_node, "NumRowsFetched", &rows_fetched)) {
    return SUMMARY_VALUE_UNAVAILABLE;
  }
  return std::to_string(rows_fetched);
}

static string SanitizeTsvValue(string value) {
  for (char& c : value) {
    if (c == '\t' || c == '\n' || c == '\r') c = ' ';
  }
  return value;
}

static string TruncateSummaryValue(string value) {
  if (FLAGS_summary_text_length > 0 && value.size() > FLAGS_summary_text_length) {
    value = value.substr(0, FLAGS_summary_text_length) + "...";
  }
  return value;
}

static void PrintTsvRow(const vector<string>& values, ostream* out) {
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) *out << "\t";
    *out << SanitizeTsvValue(values[i]);
  }
  *out << "\n";
}

static void PrintSummaryHeader(ostream* out) {
  size_t num_columns = sizeof(SUMMARY_HEADER) / sizeof(SUMMARY_HEADER[0]);
  PrintTsvRow(vector<string>(SUMMARY_HEADER, SUMMARY_HEADER + num_columns), out);
}

static void PrintSummaryRow(
    const string& query_id, const TRuntimeProfileTree& profile_tree, ostream* out) {
  const TRuntimeProfileNode* summary_node = FindNodeByName(profile_tree, "Summary");
  const TRuntimeProfileNode* impala_server_node =
      FindNodeByName(profile_tree, "ImpalaServer");
  const TRuntimeProfileNode* execution_node =
      FindNodeByPrefix(profile_tree, "Execution Profile ");
  string query_state = GetInfoString(summary_node, "Query State");
  string output_query_id = GetProfileQueryId(profile_tree);
  if (output_query_id == SUMMARY_VALUE_UNAVAILABLE && !query_id.empty()) {
    output_query_id = query_id;
  }

  vector<string> values = {output_query_id,
      GetInfoString(summary_node, "User", SUMMARY_VALUE_UNAVAILABLE),
      GetInfoString(summary_node, "Default Db", SUMMARY_VALUE_UNAVAILABLE),
      GetInfoString(summary_node, "Query Type", SUMMARY_VALUE_UNAVAILABLE),
      GetInfoString(summary_node, "Start Time", SUMMARY_VALUE_UNAVAILABLE),
      GetInfoString(summary_node, "End Time", SUMMARY_VALUE_UNAVAILABLE),
      QueryDuration(summary_node),
      QueuedDuration(summary_node),
      MemoryUsage(execution_node),
      MemoryEstimate(summary_node),
      ScanProgress(profile_tree, query_state),
      QueryProgress(execution_node, query_state),
      CounterValue(execution_node, "TotalBytesRead", TUnit::BYTES),
      CounterValue(execution_node, "TotalBytesSent", TUnit::BYTES),
      BytesSpilled(profile_tree),
      query_state.empty() ? SUMMARY_VALUE_UNAVAILABLE : query_state,
      TruncateSummaryValue(GetInfoString(
          summary_node, "Query Status", SUMMARY_VALUE_UNAVAILABLE)),
      RowsFetched(impala_server_node),
      GetInfoString(summary_node, "Request Pool", SUMMARY_VALUE_UNAVAILABLE),
      TruncateSummaryValue(GetInfoString(
          summary_node, "Sql Statement", SUMMARY_VALUE_UNAVAILABLE))};
  PrintTsvRow(values, out);
}

static bool ParseTimestamp(const string& timestamp_str, int64_t* timestamp) {
  StringParser::ParseResult result;
  *timestamp = StringParser::StringToInt<int64_t>(
      timestamp_str.c_str(), timestamp_str.length(), &result);
  return result == StringParser::PARSE_SUCCESS;
}

int main(int argc, char** argv) {
  google::SetUsageMessage(USAGE);
  google::ParseCommandLineFlags(&argc, &argv, true);

  string profile_format = to_lower_copy(FLAGS_profile_format);
  if (profile_format != "text" && profile_format != "json"
      && profile_format != "prettyjson" && profile_format != "summary") {
    cerr << "Invalid --profile_format value: '" << profile_format << "'\n\n"
         << DescribeOneFlag(GetCommandLineFlagInfoOrDie("profile_format"));
    return 1;
  }
  RuntimeProfileBase::Verbosity configured_verbosity =
      RuntimeProfileBase::Verbosity::LEGACY;
  if (FLAGS_profile_verbosity != ""
      && !RuntimeProfileBase::ParseVerbosity(
             FLAGS_profile_verbosity, &configured_verbosity)) {
    cerr << "Invalid --profile_verbosity value: '" << FLAGS_profile_verbosity << "'\n\n"
         << DescribeOneFlag(GetCommandLineFlagInfoOrDie("profile_verbosity"));
    return 1;
  }

  // Init OsInfo for StopWatch used in MemPool.
  // TODO: try using a fake MemPool that invokes malloc/free directly.
  OsInfo::Init();
  if (profile_format == "summary") PrintSummaryHeader(&cout);
  if (profile_format == "prettyjson") cout << "[\n";
  int errors = 0;
  int profiles_emitted = 0;
  string line;
  int lineno = 1;
  // Read profile log or WebUI thrift profile lines from stdin.
  for (; getline(cin, line); ++lineno) {
    // Profile logs prefix each encoded profile with timestamp and query id. WebUI
    // thrift profile downloads contain only the encoded profile.
    istringstream liness(line);
    string timestamp_str, query_id, encoded_profile;
    liness >> timestamp_str >> query_id >> encoded_profile;
    const bool has_log_metadata = !liness.fail();
    if (!has_log_metadata) {
      encoded_profile = line;
      StripWhiteSpace(&encoded_profile);
    }

    // Skip decoding entries that don't match our parameters.
    if (has_log_metadata && FLAGS_query_id != "" && FLAGS_query_id != query_id) {
      continue;
    }
    if (has_log_metadata && (FLAGS_min_timestamp != -1 || FLAGS_max_timestamp != -1)) {
      int64_t timestamp;
      if (!ParseTimestamp(timestamp_str, &timestamp)) {
        cerr << "Error parsing profile log timestamp prefix on line " << lineno
             << ": '" << timestamp_str << "'. Expected Unix epoch milliseconds; "
             << "timestamp prefixes are parsed only when "
             << "--min_timestamp/--max_timestamp filtering is enabled.\n";
        ++errors;
        continue;
      }
      if ((FLAGS_min_timestamp != -1 && timestamp < FLAGS_min_timestamp)
          || (FLAGS_max_timestamp != -1 && timestamp > FLAGS_max_timestamp)) {
        continue;
      }
    }

    if (profile_format == "summary") {
      TRuntimeProfileTree profile_tree;
      Status status =
          RuntimeProfile::DeserializeFromArchiveString(encoded_profile, &profile_tree);
      if (!status.ok()) {
        cerr << "Error parsing entry " << lineno << ": " << status.GetDetail() << "\n";
        ++errors;
        continue;
      }
      PrintSummaryRow(query_id, profile_tree, &cout);
      ++profiles_emitted;
      continue;
    }

    ObjectPool pool;
    RuntimeProfile* profile;
    Status status = RuntimeProfile::CreateFromArchiveString(
        encoded_profile, &pool, &profile);
    if (!status.ok()) {
      cerr << "Error parsing entry " << lineno << ": " << status.GetDetail() << "\n";
      ++errors;
      continue;
    }

    // Default verbosity depends on version - preserve legacy output for V1 profiles.
    RuntimeProfileBase::Verbosity verbosity = configured_verbosity;
    if (FLAGS_profile_verbosity == "") {
      // Assign default verbosity based on the execution profile's type
      RuntimeProfile* exec_profile = (RuntimeProfile*)
          profile->GetChildByPrefix("Execution Profile");
      if (exec_profile != nullptr) {
        const::string* profile_type = exec_profile->GetInfoString("Profile Type");
        if (profile_type != nullptr) {
          if (*profile_type == "UNAGGREGATED") {
            verbosity = RuntimeProfile::Verbosity::LEGACY;
          } else {
            verbosity = RuntimeProfile::Verbosity::DEFAULT;
          }
        }
      }
    }

    if (profile_format == "text") {
      profile->PrettyPrint(verbosity, &cout);
    } else if (profile_format == "json") {
      CHECK_EQ("json", profile_format);
      rapidjson::Document json_profile(rapidjson::kObjectType);
      profile->ToJson(verbosity, &json_profile);
      RuntimeProfile::JsonProfileToString(json_profile, /*pretty=*/false, &cout);
      cout << "\n"; // Each JSON document gets a separate line.
    } else if (profile_format == "prettyjson") {
      CHECK_EQ("prettyjson", profile_format);
      rapidjson::Document json_profile(rapidjson::kObjectType);
      profile->ToJson(verbosity, &json_profile);
      if (profiles_emitted > 0) cout << ",\n";
      RuntimeProfile::JsonProfileToString(json_profile, /*pretty=*/true, &cout);
      cout << "\n"; // Each JSON document starts on a new line.
    }
    ++profiles_emitted;
  }
  if (profile_format == "prettyjson") cout << "]\n";
  if (cin.bad()) {
    cerr << "Error reading line " << lineno << "\n";
    ++errors;
  }

  if (errors > 0) {
    cerr << "Encountered " << errors << " parse errors" << "\n";
    return 1;
  }
  return 0;
}
