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

namespace cpp impala
namespace java org.apache.impala.thrift

include "ExecStats.thrift"
include "Metrics.thrift"
include "Types.thrift"

// NOTE: This file and the includes above define the format of Impala query profiles. As
// newer versions of Impala should be able to read profiles written by older versions,
// some best practices must be followed when making changes to the structures below:
//
// - Only append new values at the end of enums.
// - Only add new fields at the end of structures, and always make them optional.
// - Don't remove fields.
// - Don't change the numbering of fields.

// Represents the different formats a runtime profile can be represented in.
enum TRuntimeProfileFormat {
  // Pretty printed.
  STRING = 0

  // The thrift profile, serialized, compressed, and encoded. Used for the query log.
  // See RuntimeProfile::SerializeToArchiveString.
  BASE64 = 1

  // TRuntimeProfileTree.
  THRIFT = 2

  // JSON profile
  JSON = 3
}

// Counter data
struct TCounter {
  1: required string name
  2: required Metrics.TUnit unit
  3: required i64 value
}

// Aggregated version of TCounter, belonging to a TAggregatedRuntimeProfileNode.
// Lists have TAggregatedRuntimeProfileNode.num_instances entries.
struct TAggCounter {
  1: required string name
  2: required Metrics.TUnit unit
  // True if a value was set for this instance.
  3: required list<bool> has_value
  // The actual values. values[i] holds the value if has_value[i] == true, and is ignored
  // if has_value[i] == false.
  4: required list<i64> values
}

// Thrift version of RuntimeProfile::EventSequence - list of (label, timestamp) pairs
// which represent an ordered sequence of events.
struct TEventSequence {
  1: required string name
  2: required list<i64> timestamps
  3: required list<string> labels
}

// Aggregated version of TEventSequence.
struct TAggEventSequence {
  1: required string name
  // One entry per unique label.
  2: required list<string> label_dict

  // The below lists have one entry per instance.

  // The label of each event, represented as the index in label_dict.
  3: required list<list<i32>> label_idxs
  4: required list<list<i64>> timestamps
}

// Struct to contain data sampled at even time intervals (e.g. ram usage every
// N seconds).
// values[0] represents the value when the counter stated (e.g. fragment started)
// values[1] is the value at period_ms (e.g. 500 ms later)
// values[2] is the value at 2 * period_ms (e.g. 1sec since start)
// This can be used to reconstruct a time line for a particular counter.
struct TTimeSeriesCounter {
  1: required string name
  2: required Metrics.TUnit unit

  // Period of intervals in ms
  3: required i32 period_ms

  // The sampled values.
  4: required list<i64> values

  // The index of the first value in this series (this is equal to the total number of
  // values contained in previous updates for this counter). Values > 0 mean that this
  // series contains an interval of a larger series. For values > 0, period_ms should be
  // ignored, as chunked counters don't resample their values.
  5: optional i64 start_index
}

// Aggregated version of TTimeSeriesCounter
struct TAggTimeSeriesCounter {
  1: required string name
  2: required Metrics.TUnit unit

  // The below lists have one entry per instance, with each list entry containing
  // the equivalent values to the similarly-named field in TTimeSeriesCounter.
  3: required list<i32> period_ms

  4: required list<list<i64>> values

  5: required list<i64> start_index
}

// Thrift version of RuntimeProfile::SummaryStatsCounter.
struct TSummaryStatsCounter {
  1: required string name
  2: required Metrics.TUnit unit
  3: required i64 sum
  4: required i64 total_num_values
  5: required i64 min_value
  6: required i64 max_value
}

// Aggregated version of TSummaryStatsCounter, belonging to a
// TAggregatedRuntimeProfileNode.
// Lists have TAggregatedRuntimeProfileNode.num_instances entries.
struct TAggSummaryStatsCounter {
  1: required string name
  2: required Metrics.TUnit unit

  // True if a value was set for this instance.
  3: required list<bool> has_value

  // The actual values of the summary stats counter, with each field stored in a separate
  // list. The ith element of each list holds a valid value if has_value[i] == true, and
  // is ignored if has_value[i] == false.
  4: required list<i64> sum
  5: required list<i64> total_num_values
  6: required list<i64> min_value
  7: required list<i64> max_value
}

// Metadata to help identify what entity the profile node corresponds to.
union TRuntimeProfileNodeMetadata {
  // Set if this node corresponds to a plan node.
  1: Types.TPlanNodeId plan_node_id

  // Set if this node corresponds to a data sink.
  2: Types.TDataSinkId data_sink_id
}

struct TAggregatedRuntimeProfileNode {
  // Number of instances that were included in the stats in this profile.
  1: optional i32 num_instances

  // Names of the profiles that contributed to this aggregated profile. This is only
  // set at the root of the aggregated profile tree. Nodes below that root will have
  // the same number of instances but the names of the input profiles are not duplicated.
  // The indices of these instances are used in many of the aggregated profile elements.
  // E.g. indices in TAggCounter.values line up with these indices, and the indices used
  // as map values in info_strings correspond to the indices here.
  2: optional list<string> input_profiles

  // The nesting of these counters is determined by 'child_counters_map' in the parent
  // TRuntimeProfileNode.
  3: optional list<TAggCounter> counters

  // Info strings stored in a dense representation. The first map key is the info string
  // key. The second map key is a distinct value of that key. The value is a list of
  // input indices that had the value. This means that the common case, where many
  // instances have identical strings, can be represented very compactly.
  //
  // E.g. the "ExecOption" and "Table Name" fields for 10 HDFS_SCAN_NODE instances could
  // be represented as:
  // {
  //   "Table Name": {"tpch.lineitem": [0,1,2,3,4,5,6,7,8,9]},
  //   "ExecOption": {
  //      "ExecOption: TEXT Codegen Enabled, Codegen enabled: 2 out of 2":
  //       [0,1,2,3,4,6,7,8,9],
  //      "ExecOption: TEXT Codegen Enabled, Codegen enabled: 3 out of 3":
  //       [5]
  //   }
  // }
  4: optional map<string, map<string, list<i32>>> info_strings

  // List of summary stats counters.
  5: optional list<TAggSummaryStatsCounter> summary_stats_counters

  // List of event sequences.
  6: optional list<TAggEventSequence> event_sequences

  // List of time series counters.
  7: optional list<TAggTimeSeriesCounter> time_series_counters
}


// A single node in the runtime profile tree. This can either be an unaggregated node,
// which has the singular counter values from the original RuntimeProfile object, or
// it can be an aggregated node, which was merged together from one or more unaggregated
// input nodes.
struct TRuntimeProfileNode {
  1: required string name
  2: required i32 num_children

  // In a unaggregated profile, individual counters.
  // In an aggregated profile, these are the averaged counters only.
  // The nesting of these counters is determined by 'child_counters_map' in the parent.
  3: required list<TCounter> counters

  // Legacy field. May contain the node ID for plan nodes.
  // Replaced by node_metadata, which contains richer metadata.
  4: required i64 metadata

  // indicates whether the child will be printed with extra indentation;
  // corresponds to indent param of RuntimeProfile::AddChild()
  5: required bool indent

  // ======================================================================
  // BEGIN: unaggregated state.
  // These fields represent unaggregated profile state only.
  // These fields are not used in aggregated profile nodes, i.e. profile V2,
  // unless otherwise noted.
  // ======================================================================
  // map of key,value info strings that capture any kind of additional information
  // about the profiled object
  6: required map<string, string> info_strings

  // Auxiliary structure to capture the info strings display order when printed
  7: required list<string> info_strings_display_order

  // map from parent counter name to child counter name. This applies to both
  // 'counters' and 'aggregated.counters'.
  8: required map<string, set<string>> child_counters_map

  // List of event sequences that capture ordered events in a query's lifetime
  9: optional list<TEventSequence> event_sequences

  // List of time series counters
  10: optional list<TTimeSeriesCounter> time_series_counters

  // ======================================================================
  // END: unaggregated state.
  // ======================================================================

  // List of summary stats counters.
  // Used in both unaggregated profiles and aggregated profiles. In aggregated profiles,
  // summarizes stats from all input profiles.
  11: optional list<TSummaryStatsCounter> summary_stats_counters

  // Metadata about the entity that this node refers to.
  12: optional TRuntimeProfileNodeMetadata node_metadata

  // Aggregated profile counters - contains the same counters as above, except transposed
  // so that one TRuntimeProfileNode contains counters for all input profiles.
  13: optional TAggregatedRuntimeProfileNode aggregated
}

// A flattened tree of runtime profiles, obtained by an pre-order traversal. The contents
// of the profile vary between different version.
//
// Version 1:
// The fully-expanded runtime profile tree generated by the query is included. For every
// fragment, each fragment instance has a separate profile subtree and an averaged profile
// for the fragment is also included with averaged counter values.
//
// Version 2 (experimental):
// Different from version 1, there is only a single aggregated profile tree for each
// fragment. All nodes in this tree use the aggregated profile representation. Otherwise
// the structure of the profile is the same.
// TODO: IMPALA-9382 - update when non-experimental
struct TRuntimeProfileTree {
  1: required list<TRuntimeProfileNode> nodes
  2: optional ExecStats.TExecSummary exec_summary

  // The version of the runtime profile representation. Different versions may have
  // different invariants or information. Thrift structures for new versions need to
  // remain readable by readers with old versions of the thrift file, but may remove
  // information.
  // Version 1: this field is unset
  // Version 2: this field is set to 2
  // TODO: IMPALA-9846: document which versions of Impala generate which version.
  3: optional i32 profile_version
}

// A list of TRuntimeProfileTree structures.
struct TRuntimeProfileForest {
  1: required list<TRuntimeProfileTree> profile_trees
  2: optional TRuntimeProfileTree host_profile
}
