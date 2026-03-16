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

//
// This file contains all structs, enums, etc., that uses in HBO.

namespace py impala_thrift_gen.HBO
namespace cpp impala
namespace java org.apache.impala.thrift

include "Exprs.thrift"

// Defines the type of statistics tracked by HBO
// TODO: Add more stats, e.g., peak memory, cpu usage, avg row size, runtime filter
// effectiveness, etc.
enum THboStatsType {
  CARDINALITY = 0
}

// Canonicalization strategies for HBO hash keys.
enum TCanonicalizationStrategy {
  EXPR_REWRITE = 0
  IGNORE_PARTITION_CONSTANTS = 1
}

// Input statistics for one leaf scan (e.g. one table) in a plan node run.
struct TScanInputStats {
  // Number of input rows from HMS stats for the selected partitions (or -1 if unknown).
  1: optional i64 input_rows
  // Catalog version of the table. Only valid for ScanNode.
  2: optional i64 catalog_version
  // Only valid for file based table scans
  3: optional i64 num_input_files
  4: optional i64 input_file_size
}

struct TPlanNodeRun {
  // Per-scan input stats for all leaf ScanNodes contributing to this run.
  1: optional list<TScanInputStats> scan_input_stats

  // TODO: track CPU usage, execution time, etc.
  2: optional i64 num_rows
  3: optional i64 mem_usage
}

struct TPlanNodeRunWithKeys {
  1: required TPlanNodeRun run
  // The HBO hash keys of the PlanNode generated for different canonicalization
  // strategies.
  2: required map<TCanonicalizationStrategy, string> hash_keys
  3: required THboStatsType stats_type
}

// Execution stats extracted from a query
struct THistoricalStatsUpdate {
  // List of plan node execution stats with their corresponding HBO hash strings for
  // different canonicalization strategies.
  1: optional list<TPlanNodeRunWithKeys> plan_node_runs
}
