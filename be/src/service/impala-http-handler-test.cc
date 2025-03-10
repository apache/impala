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

#include "service/impala-http-handler.h"

#include "testutil/gtest-util.h"

#include "common/names.h"

#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using namespace impala;
using namespace std;
using namespace rapidjson;

namespace impala {

class ImpalaHttpHandlerTest : public testing::Test {
  static TPlanNodeId node_id;
  static const string PLAN_SCHEMA_JSON;

 public:
  static void validatePlanSchema(const Document& plan_document) {
    Document sd;
    ASSERT_FALSE(sd.Parse(PLAN_SCHEMA_JSON).HasParseError());
    SchemaDocument schema(sd);

    SchemaValidator validator(schema);
    if (!plan_document.Accept(validator)) {
      StringBuffer sb;
      validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
      printf("Invalid schema: %s\n", sb.GetString());
      printf("Invalid keyword: %s\n", validator.GetInvalidSchemaKeyword());
      sb.Clear();
      validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
      printf("Invalid document: %s\n", sb.GetString());
      FAIL();
    }
  }

  static TPlanNode createTPlanNode(TPlanNodeId id, const string& label,
      const string& label_detail, size_t num_children) {
    TPlanNode node;
    node.__set_node_id(id);
    node.__set_label(label);
    node.__set_label_detail(label_detail);
    node.__set_num_children(num_children);
    return node;
  }

  static TPlanFragment createTPlanFragment(TFragmentIdx id) {
    TPlanFragment plan_fragment;
    plan_fragment.__set_idx(id);
    return plan_fragment;
  }

  static void addStatsToSummary(int baseline, TPlanNodeExecSummary* summary) {
    TExecStats stats;
    // Set some dummy values as stats.
    stats.__set_cardinality((baseline + 1) * 2);
    stats.__set_latency_ns((baseline + 1) * 2000);
    summary->exec_stats.push_back(stats);
  }

  static TPlanNodeExecSummary createTPlanNodeExecSummary(TPlanNodeId id) {
    TPlanNodeExecSummary node_summary;
    node_summary.__set_node_id(id);
    // Set some dummy values for the summary.
    if (id % 2 == 0) {
      node_summary.__set_is_broadcast(true);
    }
    addStatsToSummary(id, &node_summary);
    if (id % 3 == 0) {
      addStatsToSummary(id + 1, &node_summary);
    }
    return node_summary;
  }

  // Should be kept synced with the logic in createTPlanNodeExecSummary.
  static void checkPlanNodeStats(const Value& plan_node, TPlanNodeId id) {
    ASSERT_TRUE(plan_node.IsObject());
    int output_card = (id + 1) * 2;
    if (id % 3 == 0) {
      if (id % 2 == 0) {
        output_card = (id + 2) * 2;
      } else {
        output_card += (id + 2) * 2;
      }
    }
    EXPECT_EQ(plan_node["output_card"].GetInt(), output_card);

    if (id % 3 == 0) {
      EXPECT_EQ(plan_node["num_instances"].GetInt(), 2);
    } else {
      EXPECT_EQ(plan_node["num_instances"].GetInt(), 1);
    }

    if (id % 2 == 0) {
      EXPECT_TRUE(plan_node["is_broadcast"].GetBool());
    } else {
      EXPECT_FALSE(plan_node.HasMember("is_broadcast"));
    }

    int max_time = id % 3 == 0 ? (id + 2) * 2000 : (id + 1) * 2000;

    EXPECT_EQ(plan_node["max_time_val"].GetInt(), max_time);
    EXPECT_EQ(
        plan_node["max_time"].GetString(), std::to_string(max_time / 1000) + ".000us");

    EXPECT_TRUE(plan_node["children"].IsArray());
  }

  static void addNodeToFragment(const string& label, const string& label_detail,
      size_t num_children, TPlanFragment* fragment, TExecSummary* summary) {
    if (fragment->__isset.plan) {
      fragment->plan.nodes.push_back(
          createTPlanNode(node_id, label, label_detail, num_children));
    } else {
      TPlan plan;
      plan.nodes.push_back(createTPlanNode(node_id, label, label_detail, num_children));
      fragment->__set_plan(plan);
    }
    summary->nodes.push_back(createTPlanNodeExecSummary(node_id));
    node_id++;
  }

  static TDataSink createStreamSink(TPlanNodeId dest_node_id) {
    TDataSink sink;
    TDataStreamSink stream_sink;
    stream_sink.__set_dest_node_id(dest_node_id);
    sink.__set_type(TDataSinkType::DATA_STREAM_SINK);
    sink.__set_stream_sink(stream_sink);
    return sink;
  }

  static TDataSink createPlanRootSink() {
    TDataSink sink;
    TPlanRootSink plan_root_sink;
    sink.__set_type(TDataSinkType::PLAN_ROOT_SINK);
    sink.__set_plan_root_sink(plan_root_sink);
    return sink;
  }

  static TDataSink createHdfsTableSink() {
    TDataSink sink;
    TTableSink table_sink;
    THdfsTableSink hdfs_table_sink;
    table_sink.__set_type(TTableSinkType::HDFS);
    table_sink.__set_hdfs_table_sink(hdfs_table_sink);
    sink.__set_type(TDataSinkType::TABLE_SINK);
    sink.__set_table_sink(table_sink);
    return sink;
  }

  static void addSinkToFragment(
      const TDataSink& sink, TPlanFragment* fragment, TExecSummary* summary) {
    fragment->__set_output_sink(sink);
    // sinks are stored in summary with -1 as node_id
    summary->nodes.push_back(createTPlanNodeExecSummary(-1));
  }

  ImpalaHttpHandlerTest() { node_id = 0; }
};

TPlanNodeId ImpalaHttpHandlerTest::node_id;
const string ImpalaHttpHandlerTest::PLAN_SCHEMA_JSON = R"({
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "plan_nodes": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "label": {
            "type": "string"
          },
          "label_detail": {
            "type": "string"
          },
          "output_card": {
            "type": "integer"
          },
          "num_instances": {
            "type": "integer"
          },
          "is_broadcast" : {
            "const": "true"
          },
          "max_time": {
            "type": "string"
          },
          "max_time_val": {
            "type": "integer"
          },
          "avg_time": {
            "type": "string"
          },
          "children": {
            "$ref": "#/properties/plan_nodes"
          },
          "data_stream_target": {
            "type": "string"
          },
          "join_build_target": {
            "type": "string"
          }
        },
        "required": [
          "label",
          "label_detail",
          "output_card",
          "num_instances",
          "max_time",
          "max_time_val",
          "avg_time",
          "children"
        ],
        "additionalProperties": false
      }
    }
  },
  "required": [ "plan_nodes" ],
  "additionalProperties": false
})";

// Prepares query plan for a select statement with two fragments, each with one node.
// The first fragment has an exchange node and the second fragment has a scan node.
//
// F01:PLAN FRAGMENT
// |
// PLAN-ROOT SINK
// |
// 01:EXCHANGE
// |
// F00:PLAN FRAGMENT
// 00:SCAN HDFS
//
// Expected JSON output for the plan will look like this:
// {
//   "plan_nodes": [
//     {
//       "label": "01:EXCHANGE",
//       "label_detail": "UNPARTITIONED",
//       "output_card": 4,
//       "num_instances": 2,
//       "is_broadcast": true,
//       "max_time": "4.000us",
//       "max_time_val": 4000,
//       "avg_time": "3.000us",
//       "children": []
//     },
//     {
//       "label": "00:SCAN HDFS",
//       "label_detail": "default.table",
//       "output_card": 4,
//       "num_instances": 1,
//       "max_time": "4.000us",
//       "max_time_val": 4000,
//       "avg_time": "4.000us",
//       "children": [],
//       "data_stream_target": "01:EXCHANGE"
//     }
//   ]
// }
static void prepareSelectStatement(const vector<string>& labels,
    const vector<string>& label_details, vector<TPlanFragment>* fragments,
    TExecSummary* summary) {
  ASSERT_EQ(labels.size(), 2);
  ASSERT_EQ(label_details.size(), 2);

  // F01:PLAN FRAGMENT
  TPlanFragment fragment01 = ImpalaHttpHandlerTest::createTPlanFragment(0);
  TDataSink plan_root_sink = ImpalaHttpHandlerTest::createPlanRootSink();
  ImpalaHttpHandlerTest::addSinkToFragment(plan_root_sink, &fragment01, summary);
  // 01:EXCHANGE
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(0), label_details.at(0), 0, &fragment01, summary);
  fragments->push_back(fragment01);

  // F00:PLAN FRAGMENT
  TPlanFragment fragment00 = ImpalaHttpHandlerTest::createTPlanFragment(1);
  TDataSink stream_sink =
      ImpalaHttpHandlerTest::createStreamSink(fragment01.plan.nodes[0].node_id);
  ImpalaHttpHandlerTest::addSinkToFragment(stream_sink, &fragment00, summary);
  // 00:SCAN HDFS
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(1), label_details.at(1), 0, &fragment00, summary);
  fragments->push_back(fragment00);
}

// Prepares query plan for a CTAS statement with two fragments, each with one node.
// Very similar to prepareSelectStatement(), but the first fragment has a table sink.
//
// F01:PLAN FRAGMENT
// |
// WRITE TO HDFS
// |
// 01:EXCHANGE
// |
// F00:PLAN FRAGMENT
// 00:SCAN HDFS
//
// Expected JSON output for the plan:
// {
//   "plan_nodes": [
//     {
//       "label": "01:EXCHANGE",
//       "label_detail": "UNPARTITIONED",
//       "output_card": 4,
//       "num_instances": 2,
//       "is_broadcast": true,
//       "max_time": "4.000us",
//       "max_time_val": 4000,
//       "avg_time": "3.000us",
//       "children": []
//     },
//     {
//       "label": "00:SCAN HDFS",
//       "label_detail": "default.table",
//       "output_card": 4,
//       "num_instances": 1,
//       "max_time": "4.000us",
//       "max_time_val": 4000,
//       "avg_time": "4.000us",
//       "children": [],
//       "data_stream_target": "01:EXCHANGE"
//     }
//   ]
// }
static void prepareCreateTableAsSelectStatement(const vector<string>& labels,
    const vector<string>& label_details, vector<TPlanFragment>* fragments,
    TExecSummary* summary) {
  ASSERT_EQ(labels.size(), 2);
  ASSERT_EQ(label_details.size(), 2);

  // F01:PLAN FRAGMENT
  TPlanFragment fragment01 = ImpalaHttpHandlerTest::createTPlanFragment(0);
  TDataSink table_sink = ImpalaHttpHandlerTest::createHdfsTableSink();
  ImpalaHttpHandlerTest::addSinkToFragment(table_sink, &fragment01, summary);
  // 01:EXCHANGE
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(0), label_details.at(0), 0, &fragment01, summary);
  fragments->push_back(fragment01);

  // F00:PLAN FRAGMENT
  TPlanFragment fragment00 = ImpalaHttpHandlerTest::createTPlanFragment(1);
  TDataSink stream_sink =
      ImpalaHttpHandlerTest::createStreamSink(fragment01.plan.nodes[0].node_id);
  ImpalaHttpHandlerTest::addSinkToFragment(stream_sink, &fragment00, summary);
  // 00:SCAN HDFS
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(1), label_details.at(1), 0, &fragment00, summary);
  fragments->push_back(fragment00);
}

// Prepares query plan for a join statement with one fragment and the join node having
// two children.
//
// F02:PLAN FRAGMENT
// |
// PLAN-ROOT SINK
// |
// 04:EXCHANGE
// |
// F00:PLAN FRAGMENT
// 02:HASH JOIN
// |
// |--03:EXCHANGE
// |  |
// |  F01:PLAN FRAGMENT
// |  00:SCAN HDFS
// |
// 01:SCAN HDFS
//
// Expected JSON output for the plan:
//
// {
//   "plan_nodes": [
//     {
//       "label": "04:EXCHANGE",
//       "label_detail": "UNPARTITIONED",
//       "output_card": 4,
//       "num_instances": 2,
//       "is_broadcast": true,
//       "max_time": "4.000us",
//       "max_time_val": 4000,
//       "avg_time": "3.000us",
//       "children": []
//     },
//     {
//       "label": "02:HASH_JOIN",
//       "label_detail": "INNER JOIN, BROADCAST",
//       "output_card": 4,
//       "num_instances": 1,
//       "max_time": "4.000us",
//       "max_time_val": 4000,
//       "avg_time": "4.000us",
//       "children": [
//         {
//           "label": "01:SCAN HDFS",
//           "label_detail": "default.table1 t1",
//           "output_card": 6,
//           "num_instances": 1,
//           "is_broadcast": true,
//           "max_time": "6.000us",
//           "max_time_val": 6000,
//           "avg_time": "6.000us",
//           "children": []
//         },
//         {
//           "label": "03:EXCHANGE",
//           "label_detail": "BROADCAST",
//           "output_card": 18,
//           "num_instances": 2,
//           "max_time": "10.000us",
//           "max_time_val": 10000,
//           "avg_time": "9.000us",
//           "children": []
//         }
//       ],
//       "data_stream_target": "04:EXCHANGE"
//     },
//     {
//       "label": "00:SCAN HDFS",
//       "label_detail": "default.table2 t2",
//       "output_card": 10,
//       "num_instances": 1,
//       "is_broadcast": true,
//       "max_time": "10.000us",
//       "max_time_val": 10000,
//       "avg_time": "10.000us",
//       "children": [],
//       "data_stream_target": "03:EXCHANGE"
//     }
//   ]
// }
static void prepareJoinStatement(const vector<string>& labels,
    const vector<string>& label_details, vector<TPlanFragment>* fragments,
    TExecSummary* summary) {
  ASSERT_EQ(labels.size(), 5);
  ASSERT_EQ(label_details.size(), 5);

  // F02:PLAN FRAGMENT
  TPlanFragment fragment02 = ImpalaHttpHandlerTest::createTPlanFragment(2);
  // 04:EXCHANGE
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(0), label_details.at(0), 0, &fragment02, summary);
  TDataSink plan_root_sink = ImpalaHttpHandlerTest::createPlanRootSink();
  ImpalaHttpHandlerTest::addSinkToFragment(plan_root_sink, &fragment02, summary);
  fragments->push_back(fragment02);

  // F00:PLAN FRAGMENT
  TPlanFragment fragment00 = ImpalaHttpHandlerTest::createTPlanFragment(0);
  TDataSink stream_sink1 =
      ImpalaHttpHandlerTest::createStreamSink(fragment02.plan.nodes[0].node_id);
  ImpalaHttpHandlerTest::addSinkToFragment(stream_sink1, &fragment00, summary);
  // 02:HASH_JOIN
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(1), label_details.at(1), 2, &fragment00, summary);
  // 01:SCAN HDFS
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(2), label_details.at(2), 0, &fragment00, summary);
  // 03:EXCHANGE
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(3), label_details.at(3), 0, &fragment00, summary);
  fragments->push_back(fragment00);

  // F01:PLAN FRAGMENT
  TPlanFragment fragment01 = ImpalaHttpHandlerTest::createTPlanFragment(1);
  // data stream sink pointing to the 03:EXCHANGE node
  TDataSink stream_sink2 =
      ImpalaHttpHandlerTest::createStreamSink(fragment00.plan.nodes[2].node_id);
  ImpalaHttpHandlerTest::addSinkToFragment(stream_sink2, &fragment01, summary);
  // 00:SCAN HDFS
  ImpalaHttpHandlerTest::addNodeToFragment(
      labels.at(4), label_details.at(4), 0, &fragment01, summary);
  fragments->push_back(fragment01);
}

} // namespace impala

TEST_F(ImpalaHttpHandlerTest, SelectStatement) {
  const vector<string> SELECT_LABELS = {"01:EXCHANGE", "00:SCAN HDFS"};
  const vector<string> SELECT_LABEL_DETAILS = {"UNPARTITIONED", "default.table"};
  vector<TPlanFragment> fragments;
  TExecSummary summary;

  prepareSelectStatement(SELECT_LABELS, SELECT_LABEL_DETAILS, &fragments, &summary);

  Document document;
  Value value(kObjectType);

  PlanToJson(fragments, summary, &document, &value);
  document.CopyFrom(value, document.GetAllocator());

  validatePlanSchema(document);

  auto array = document["plan_nodes"].GetArray();
  EXPECT_EQ(array.Size(), 2);
  for (size_t i = 0; i < array.Size(); ++i) {
    EXPECT_EQ(array[i]["label"].GetString(), SELECT_LABELS.at(i));
    EXPECT_EQ(array[i]["label_detail"].GetString(), SELECT_LABEL_DETAILS.at(i));

    checkPlanNodeStats(array[i], i);

    EXPECT_EQ(array[i]["children"].Size(), 0);
  }
}

TEST_F(ImpalaHttpHandlerTest, CreateTableAsSelectStatement) {
  const vector<string> CTAS_LABELS = {"01:EXCHANGE", "00:SCAN HDFS"};
  const vector<string> CTAS_LABEL_DETAILS = {"UNPARTITIONED", "default.table"};
  vector<TPlanFragment> fragments;
  TExecSummary summary;

  prepareCreateTableAsSelectStatement(
      CTAS_LABELS, CTAS_LABEL_DETAILS, &fragments, &summary);

  Document document;
  Value value(kObjectType);

  PlanToJson(fragments, summary, &document, &value);
  document.CopyFrom(value, document.GetAllocator());

  validatePlanSchema(document);

  auto array = document["plan_nodes"].GetArray();
  EXPECT_EQ(array.Size(), 2);
  for (size_t i = 0; i < array.Size(); ++i) {
    EXPECT_EQ(array[i]["label"].GetString(), CTAS_LABELS.at(i));
    EXPECT_EQ(array[i]["label_detail"].GetString(), CTAS_LABEL_DETAILS.at(i));

    checkPlanNodeStats(array[i], i);

    EXPECT_EQ(array[i]["children"].Size(), 0);
  }
}

TEST_F(ImpalaHttpHandlerTest, JoinStatement) {
  const vector<string> JOIN_LABELS = {
      "04:EXCHANGE", "02:HASH_JOIN", "01:SCAN HDFS", "03:EXCHANGE", "00:SCAN HDFS"};
  const vector<string> JOIN_LABEL_DETAILS = {"UNPARTITIONED", "INNER JOIN, BROADCAST",
      "default.table1 t1", "BROADCAST", "default.table2 t2"};
  vector<TPlanFragment> fragments;
  TExecSummary summary;

  prepareJoinStatement(JOIN_LABELS, JOIN_LABEL_DETAILS, &fragments, &summary);

  Document document;
  Value value(kObjectType);

  PlanToJson(fragments, summary, &document, &value);
  document.CopyFrom(value, document.GetAllocator());

  validatePlanSchema(document);

  auto array = document["plan_nodes"].GetArray();
  EXPECT_EQ(array.Size(), 3);

  // Check the 04:EXCHANGE node
  auto& exch04_node = array[0];
  EXPECT_EQ(exch04_node["label"].GetString(), JOIN_LABELS[0]);
  EXPECT_EQ(exch04_node["label_detail"].GetString(), JOIN_LABEL_DETAILS[0]);

  checkPlanNodeStats(exch04_node, 0);
  EXPECT_EQ(exch04_node["children"].Size(), 0);

  // Check the 02:HASH_JOIN node
  auto& join_node = array[1];
  EXPECT_EQ(join_node["label"].GetString(), JOIN_LABELS[1]);
  EXPECT_EQ(join_node["label_detail"].GetString(), JOIN_LABEL_DETAILS[1]);

  checkPlanNodeStats(join_node, 1);
  EXPECT_TRUE(join_node.HasMember("data_stream_target"));
  EXPECT_EQ(join_node["data_stream_target"], JOIN_LABELS[0]);

  // Check the two children of join node
  auto children = join_node["children"].GetArray();
  EXPECT_EQ(children.Size(), 2);

  for (size_t i = 0; i < children.Size(); ++i) {
    EXPECT_EQ(children[i]["label"].GetString(), JOIN_LABELS.at(i + 2));
    EXPECT_EQ(children[i]["label_detail"].GetString(), JOIN_LABEL_DETAILS.at(i + 2));

    checkPlanNodeStats(children[i], i + 2);

    EXPECT_EQ(children[i]["children"].Size(), 0);
  }

  // Check the 00:SCAN HDFS node
  auto& scan_node = array[2];
  EXPECT_EQ(scan_node["label"].GetString(), JOIN_LABELS[4]);
  EXPECT_EQ(scan_node["label_detail"].GetString(), JOIN_LABEL_DETAILS[4]);

  checkPlanNodeStats(scan_node, 4);
  EXPECT_EQ(scan_node["children"].Size(), 0);
  EXPECT_TRUE(join_node.HasMember("data_stream_target"));
  EXPECT_EQ(scan_node["data_stream_target"].GetString(), JOIN_LABELS[3]);
}