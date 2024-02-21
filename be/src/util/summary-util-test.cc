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

#include "testutil/gtest-util.h"

#include "gen-cpp/ExecStats_types.h"
#include "util/summary-util.h"

namespace impala {

static const string expected_str = "\n"
"Operator              #Hosts  #Inst   Avg Time   Max Time  #Rows  Est. #Rows   Peak Mem "
" Est. Peak Mem  Detail                                    \n"
"----------------------------------------------------------------------------------------"
"----------------------------------------------------------\n"
"F01:ROOT                   1      1  857.074us  857.074us                       4.01 MB "
"       4.00 MB                                            \n"
"01:EXCHANGE                1      1  269.934us  269.934us     99           7   88.00 KB "
"      16.00 KB  UNPARTITIONED                             \n"
"F00:EXCHANGE SENDER        3      3  332.506us  338.449us                       7.95 KB "
"      96.00 KB                                            \n"
"00:SCAN HDFS               3      3    1s328ms    1s331ms     99           7  360.00 KB "
"      64.00 MB  default.test_query_log_beeswax_1707938440 ";

static TExecStats buildExecStats(int64_t latency, int64_t mem_used,
    int64_t cardinality) {
  TExecStats stat;

  if (latency > -1) {
    stat.__set_latency_ns(latency);
  }
  stat.__set_memory_used(mem_used);
  stat.__set_cardinality(cardinality);

  return stat;
}

static TPlanNodeExecSummary buildPlanNode(TPlanNodeId node_id, TFragmentIdx fragment_idx,
    string label, string detail, int32_t num_hosts, int32_t num_children,
    bool is_broadcast, TExecStats estimates) {
  TPlanNodeExecSummary node;

  node.__set_node_id(node_id);
  node.__set_fragment_idx(fragment_idx);
  node.__set_label(label);
  node.__set_label_detail(detail);
  node.__set_num_children(num_children);
  node.__set_estimated_stats(estimates);
  if (is_broadcast) {
    node.__set_is_broadcast(is_broadcast);
  }
  node.__set_num_hosts(num_hosts);

  return node;
}

// Constructs a simple exec summary and ensures the text table is generated correctly for
// that exec summary.
TEST(PrintTableTest, HappyPath) {
  TExecSummary input;

  TPlanNodeExecSummary node = buildPlanNode(-1, 0, "F01:ROOT", "", 1, 1,
      false, buildExecStats(-1, 4194304, -1));
  node.exec_stats.push_back(buildExecStats(857074, 4202496, -1));
  node.__isset.exec_stats = true;
  input.nodes.push_back(node);

  node = buildPlanNode(1, 0, "01:EXCHANGE", "UNPARTITIONED", 1, 0,
      true, buildExecStats(-1, 16384, 7));
  node.exec_stats.push_back(buildExecStats(269934, 90112, 99));
  node.__isset.exec_stats = true;
  input.nodes.push_back(node);

  node = buildPlanNode(-1, 1, "F00:EXCHANGE SENDER", "", 3, 1,
      false, buildExecStats(-1, 98304, -1));
  node.exec_stats.push_back(buildExecStats(338449, 6862, -1));
  node.exec_stats.push_back(buildExecStats(333098, 8139, -1));
  node.exec_stats.push_back(buildExecStats(325971, 8139, -1));
  node.__isset.exec_stats = true;
  input.nodes.push_back(node);

  node = buildPlanNode(0, 1, "00:SCAN HDFS",
      "default.test_query_log_beeswax_1707938440", 3, 0, false,
      buildExecStats(-1, 67108864, 7));
  node.exec_stats.push_back(buildExecStats(1331010710, 368640, 39));
  node.exec_stats.push_back(buildExecStats(1326558546, 279552, 30));
  node.exec_stats.push_back(buildExecStats(1327758097, 283648, 30));
  node.__isset.exec_stats = true;
  input.nodes.push_back(node);

  input.__isset.nodes = true;
  input.exch_to_sender_map.emplace(1, 2);
  input.__isset.exch_to_sender_map = true;

  string actual = PrintExecSummary(input);
  EXPECT_EQ(expected_str, actual);
}

} // namespace impala