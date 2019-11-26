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

#include "util/summary-util.h"

#include <vector>
#include <boost/lexical_cast.hpp>

#include "common/logging.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/table-printer.h"

#include "common/names.h"

using namespace impala;

// Helper function for PrintExecSummary() that walks the exec summary recursively.
// Output for this node is appended to *result. Each value in *result should contain
// the statistics for a single exec summary node.
// node_idx is an in/out parameter. It is called with the idx (into exec_summary_.nodes)
// for the current node and on return, will contain the id of the next node.
void PrintExecSummary(const TExecSummary& exec_summary, int indent_level,
    int new_indent_level, int* node_idx,
    vector<vector<string>>* result) {
  DCHECK_LT(*node_idx, exec_summary.nodes.size());
  const TPlanNodeExecSummary& node = exec_summary.nodes[*node_idx];
  const TExecStats& est_stats = node.estimated_stats;

  TExecStats agg_stats;
  TExecStats max_stats;

#define COMPUTE_MAX_SUM_STATS(NAME)\
  agg_stats.NAME += node.exec_stats[i].NAME;\
  max_stats.NAME = std::max(max_stats.NAME, node.exec_stats[i].NAME)

  // Compute avg and max of each used stat (cpu_time_ns is unused in the summary output).
  for (int i = 0; i < node.exec_stats.size(); ++i) {
    COMPUTE_MAX_SUM_STATS(latency_ns);
    COMPUTE_MAX_SUM_STATS(cardinality);
    COMPUTE_MAX_SUM_STATS(memory_used);
  }
#undef COMPUTE_MAX_SUM_STATS

  int64_t avg_time = node.exec_stats.size() == 0 ? 0 :
      agg_stats.latency_ns / node.exec_stats.size();

  // Print the level to indicate nesting with "|--"
  stringstream label_ss;
  if (indent_level != 0) {
    label_ss << "|";
    for (int i = 0; i < indent_level - 1; ++i) {
      label_ss << "  |";
    }
    label_ss << (new_indent_level ? "--" : "  ");
  }

  label_ss << node.label;

  vector<string> row;
  row.push_back(label_ss.str());
  row.push_back(lexical_cast<string>(node.num_hosts));
  row.push_back(lexical_cast<string>(node.exec_stats.size())); // Num instances
  row.push_back(PrettyPrinter::Print(avg_time, TUnit::TIME_NS));
  row.push_back(PrettyPrinter::Print(max_stats.latency_ns, TUnit::TIME_NS));
  if (node.node_id == -1) {
    // Cardinality stats are not valid for sinks.
    row.push_back("");
    row.push_back("");
  } else {
    row.push_back(PrettyPrinter::Print(
        node.is_broadcast ? max_stats.cardinality : agg_stats.cardinality, TUnit::UNIT));
    row.push_back(PrettyPrinter::Print(est_stats.cardinality, TUnit::UNIT));
  }
  row.push_back(PrettyPrinter::Print(max_stats.memory_used, TUnit::BYTES));
  row.push_back(PrettyPrinter::Print(est_stats.memory_used, TUnit::BYTES));
  // Node "details" may contain exprs which should be redacted.
  row.push_back(RedactCopy(node.label_detail));
  result->push_back(row);

  map<int, int>::const_iterator child_fragment_idx_it =
      exec_summary.exch_to_sender_map.find(*node_idx);
  if (child_fragment_idx_it != exec_summary.exch_to_sender_map.end()) {
    int child_fragment_indent_level = indent_level + node.num_children;
    bool child_fragment_new_indent_level = node.num_children > 0;
    int child_fragment_id = child_fragment_idx_it->second;
    PrintExecSummary(exec_summary, child_fragment_indent_level,
        child_fragment_new_indent_level, &child_fragment_id, result);
  }
  ++*node_idx;
  if (node.num_children == 0) return;

  // Print the non-left children to the stream first.
  vector<vector<string>> child0_result;
  PrintExecSummary(exec_summary, indent_level, false, node_idx, &child0_result);

  for (int i = 1; i < node.num_children; ++i) {
    PrintExecSummary(exec_summary, indent_level + 1, true, node_idx, result);
  }
  for (int i = 0; i < child0_result.size(); ++i) {
    result->push_back(child0_result[i]);
  }
}

string impala::PrintExecSummary(const TExecSummary& exec_summary) {
  // Bail if Coordinator::InitExecProfile() has not been called.
  if (!exec_summary.__isset.nodes) return "";

  TablePrinter printer;
  printer.set_max_output_width(1000);
  printer.AddColumn("Operator", true);
  printer.AddColumn("#Hosts", false);
  printer.AddColumn("#Inst", false);
  printer.AddColumn("Avg Time", false);
  printer.AddColumn("Max Time", false);
  printer.AddColumn("#Rows", false);
  printer.AddColumn("Est. #Rows", false);
  printer.AddColumn("Peak Mem", false);
  printer.AddColumn("Est. Peak Mem", false);
  printer.AddColumn("Detail", true);

  vector<vector<string>> rows;
  int node_idx = 0;
  ::PrintExecSummary(exec_summary, 0, false, &node_idx, &rows);
  for (int i = 0; i < rows.size(); ++i) {
    printer.AddRow(rows[i]);
  }
  return printer.ToString("\n");
}
