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

#include "service/query-profile-parsing-tools.h"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "testutil/gtest-util.h"

using std::string;
using std::string_view;
namespace fs = std::filesystem;

namespace impala {

namespace {

constexpr const char* PROFILE_FILE_TPCDS_72 = "tpcds_72_local_run_profile.json";
constexpr const char* PROFILE_FILE_TPCDS_1_NUM_NODES_0 =
    "tpcds_1_local_run_num_nodes_0_profile.json";
constexpr const char* PROFILE_FILE_TPCDS_1_MT_DOP_0 =
    "tpcds_1_local_run_mt_dop_0_profile.json";
constexpr const char* PROFILE_FILE_TPCDS_1_MT_DOP_12 =
    "tpcds_1_local_run_mt_dop_12_profile.json";
constexpr const char* UPDATE_GOLDENS_ENV_VAR =
    "UPDATE_QUERY_PROFILE_AI_ANALYSIS_TOOL_GOLDENS";
constexpr const char* TESTDATA_BASE_RELATIVE_PATH = "testdata/impala-profiles";

string ReadFileToString(const fs::path& path) {
  std::ifstream in(path);
  if (!in.is_open()) return "";
  std::stringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

fs::path GetProfilesBasePath(const char* impala_home) {
  return fs::path(impala_home) / TESTDATA_BASE_RELATIVE_PATH;
}

string LoadTestProfileText(
    const char* impala_home, string_view profile_file, fs::path* profile_path) {
  if (profile_path == nullptr) return "";
  *profile_path = GetProfilesBasePath(impala_home) / string(profile_file);
  return ReadFileToString(*profile_path);
}

bool ShouldUpdateGoldens() {
  return std::getenv(UPDATE_GOLDENS_ENV_VAR) != nullptr;
}

const char* GetImpalaHome() {
  return std::getenv("IMPALA_HOME");
}

void ExpectRejectedNonJsonProfileInput(const Status& status) {
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "valid JSON object");
}

void ParseProfileJsonOrDie(
    const string& profile_text, rapidjson::Document* profile_json) {
  ASSERT_NE(profile_json, nullptr);
  profile_json->Parse(
      profile_text.data(), static_cast<rapidjson::SizeType>(profile_text.size()));
  ASSERT_FALSE(profile_json->HasParseError());
}

bool WriteStringToFile(const fs::path& path, const string& contents) {
  std::ofstream out(path);
  if (!out.is_open()) return false;
  out << contents;
  return out.good();
}

string FormatJson(string_view json_text, bool pretty) {
  rapidjson::Document doc;
  doc.Parse(
      json_text.data(), static_cast<rapidjson::SizeType>(json_text.size()));
  if (doc.HasParseError()) return string(json_text);
  rapidjson::StringBuffer buffer;
  if (pretty) {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', 2);
    doc.Accept(writer);
  } else {
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
  }
  return string(buffer.GetString(), buffer.GetSize());
}

string FormatJson(const rapidjson::Value& value, bool pretty) {
  rapidjson::StringBuffer buffer;
  if (pretty) {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', 2);
    value.Accept(writer);
  } else {
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
  }
  return string(buffer.GetString(), buffer.GetSize());
}

TEST(QueryProfileParsingToolsTest, ToolMethodsReturnExpectedProfileSections) {
  const char* impala_home = GetImpalaHome();
  ASSERT_NE(nullptr, impala_home);

  const fs::path golden_dir =
      fs::path(impala_home)
      / "testdata/impala-profiles/query-profile-parsing-tools-expected";
  const bool update_goldens = ShouldUpdateGoldens();
  if (update_goldens) {
    fs::create_directories(golden_dir);
  }

  struct ToolCase {
    string_view tool_name;
    string_view args_json;
    string_view golden_file;
  };

  const std::array<ToolCase, 18> tools = {{
      {"get_summary", "{}", "get_summary.json"},
      {"get_timeline", "{}", "get_timeline.json"},
      {"get_compilation", "{}", "get_compilation.json"},
      {"get_execution_profile", "{}", "get_execution_profile.json"},
      {"get_fragments_overview", "{}", "get_fragments_overview.json"},
      {"get_fragment", "{\"fragment_id\":\"F00\"}", "get_fragment_F00.json"},
      {"get_all_fragments", "{\"fragment_id\":\"F00\"}", "get_all_fragments_F00.json"},
      {"get_fragment_instances", "{\"fragment_id\":\"F00\"}",
          "get_fragment_instances_F00.json"},
      {"get_operator", "{\"node_id\":\"HDFS_SCAN_NODE\",\"fragment_id\":\"F00\"}",
          "get_node_HDFS_SCAN_NODE_F00.json"},
      {"get_average_operator", "{\"node_id\":\"HDFS_SCAN_NODE\"}",
          "get_average_plan_node_HDFS_SCAN_NODE.json"},
      {"get_all_operators", "{\"node_id\":\"HDFS_SCAN_NODE\",\"fragment_id\":\"F00\"}",
          "get_all_plan_nodes_HDFS_SCAN_NODE_F00.json"},
      {"get_specific_operator",
          "{\"node_id\":\"HDFS_SCAN_NODE\",\"fragment_id\":\"F00\","
          "\"instance_id\":\"00000001\"}",
          "get_specific_plan_node_HDFS_SCAN_NODE_F00_instance_00000001.json"},
      {"get_scan_nodes_summary", "{}", "get_scan_nodes_summary.json"},
      {"get_per_node_profiles", "{}", "get_per_node_profiles.json"},
      {"get_query_options", "{}", "get_query_options.json"},
      {"get_tables_queried", "{}", "get_tables_queried.json"},
      {"get_resource_estimates", "{}", "get_resource_estimates.json"},
      {"get_analyzed_query", "{}", "get_analyzed_query.json"},
  }};

  struct ProfileCase {
    string_view profile_file;
    string_view golden_subdir;
    string_view specific_operator_instance_id;
  };

  const std::array<ProfileCase, 4> profiles = {{
      {PROFILE_FILE_TPCDS_72, "tpcds_72_local_run_profile", "00000001"},
      {PROFILE_FILE_TPCDS_1_NUM_NODES_0, "tpcds_1_local_run_num_nodes_0_profile",
          "00000000"},
      {PROFILE_FILE_TPCDS_1_MT_DOP_0, "tpcds_1_local_run_mt_dop_0_profile",
          "00000001"},
      {PROFILE_FILE_TPCDS_1_MT_DOP_12, "tpcds_1_local_run_mt_dop_12_profile",
          "00000001"},
  }};

  for (const ProfileCase& profile : profiles) {
    fs::path profile_path;
    const string profile_text =
        LoadTestProfileText(impala_home, profile.profile_file, &profile_path);
    ASSERT_FALSE(profile_text.empty()) << "failed to read " << profile_path;
    rapidjson::Document profile_json;
    ParseProfileJsonOrDie(profile_text, &profile_json);

    QueryProfileToolExecutor tool_executor;
    Status executor_status =
        CreateQueryProfileToolExecutorForProfile(
            profile_json, &tool_executor, /*profile_size_limit_bytes=*/-1,
            profile_text.size());
    ASSERT_TRUE(executor_status.ok())
        << "profile=" << profile.profile_file
        << " status=" << executor_status.GetDetail();

    fs::path profile_golden_dir = golden_dir;
    if (!profile.golden_subdir.empty()) {
      profile_golden_dir /= string(profile.golden_subdir);
    }
    if (update_goldens) fs::create_directories(profile_golden_dir);

    for (const ToolCase& tc : tools) {
      string tool_args(tc.args_json);
      string golden_file(tc.golden_file);
      if (tc.tool_name == "get_specific_operator") {
        const string default_instance_id = "00000001";
        const string instance_id(profile.specific_operator_instance_id);
        if (instance_id != default_instance_id) {
          const size_t args_pos = tool_args.find(default_instance_id);
          if (args_pos != string::npos) {
            tool_args.replace(args_pos, default_instance_id.size(), instance_id);
          }
          const size_t file_pos = golden_file.find(default_instance_id);
          if (file_pos != string::npos) {
            golden_file.replace(file_pos, default_instance_id.size(), instance_id);
          }
        }
      }

      rapidjson::Document output_doc;
      const Status status = tool_executor(tc.tool_name, tool_args, &output_doc);
      ASSERT_TRUE(status.ok()) << "profile=" << profile.profile_file
                               << " tool=" << tc.tool_name
                               << " status=" << status.GetDetail();
      ASSERT_FALSE(output_doc.IsNull())
          << "profile=" << profile.profile_file << " tool=" << tc.tool_name;

      const fs::path golden_path = profile_golden_dir / golden_file;
      if (update_goldens) {
        const string pretty_json = FormatJson(output_doc, /*pretty=*/true);
        ASSERT_TRUE(WriteStringToFile(golden_path, pretty_json))
            << "failed to write " << golden_path;
        continue;
      }

      const string expected_json = ReadFileToString(golden_path);
      ASSERT_FALSE(expected_json.empty())
          << "missing golden output for profile=" << profile.profile_file
          << " tool=" << tc.tool_name
          << " at " << golden_path
          << ". Re-run test with UPDATE_QUERY_PROFILE_AI_ANALYSIS_TOOL_GOLDENS=1";
      EXPECT_EQ(FormatJson(expected_json, /*pretty=*/false),
          FormatJson(output_doc, /*pretty=*/false))
          << "profile=" << profile.profile_file
          << " tool=" << tc.tool_name << " output mismatch";
    }
  }
}

TEST(QueryProfileParsingToolsTest, ToolMethodsRejectNonJsonProfileInput) {
  QueryProfileToolExecutor tool_executor;
  rapidjson::Document profile_json;
  profile_json.SetString("not-a-json-object", profile_json.GetAllocator());
  const Status status =
      CreateQueryProfileToolExecutorForProfile(profile_json, &tool_executor);
  ExpectRejectedNonJsonProfileInput(status);
}

TEST(QueryProfileParsingToolsTest, ToolExecutorRejectsInputLargerThanConfiguredLimit) {
  const string profile_text = R"({
  "profile_name": "Query (id=oversized-query-id):",
  "child_profiles": [],
  "info_strings": [
    {"key": "Summary", "value": {"Query Type": "QUERY"}}
  ],
  "num_children": 0
})";
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);

  QueryProfileToolExecutor tool_executor;
  const Status status = CreateQueryProfileToolExecutorForProfile(
      profile_json, &tool_executor, /*profile_size_limit_bytes=*/32,
      profile_text.size());
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(
      status.GetDetail(), "configured parsing profile size limit");
}

TEST(
    QueryProfileParsingToolsTest,
    ToolExecutorRespectsConfiguredProfileSizeLimitOverride) {
  const string profile_text = R"({
  "profile_name": "Query (id=override-query-id):",
  "child_profiles": [],
  "info_strings": [
    {
      "key": "Summary",
      "value": {
        "Query Type": "QUERY",
        "Query State": "FINISHED"
      }
    }
  ],
  "num_children": 0
})";
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);

  QueryProfileToolExecutor tool_executor;
  const Status create_status = CreateQueryProfileToolExecutorForProfile(
      profile_json, &tool_executor, /*profile_size_limit_bytes=*/1024,
      profile_text.size());
  ASSERT_TRUE(create_status.ok()) << create_status.GetDetail();

  rapidjson::Document output_doc;
  const Status execute_status = tool_executor("get_summary", "{}", &output_doc);
  ASSERT_TRUE(execute_status.ok()) << execute_status.GetDetail();
  const string output_json = FormatJson(output_doc, /*pretty=*/false);
  EXPECT_STR_CONTAINS(output_json, "Query State");
}

TEST(QueryProfileParsingToolsTest,
    GetOperatorPrefersTokenBoundaryMatchForShortNumericIdsInAveragedFragment) {
  constexpr const char* profile_text = R"json(
{
  "profile_name": "Query (id=test-query-id):",
  "child_profiles": [
    {
      "profile_name": "Execution Profile test-query-id",
      "child_profiles": [
        {
          "profile_name": "Averaged Fragment F00",
          "info_strings": [
            {"key": "10:SCAN HDFS", "value": {"id": "wrong"}},
            {"key": "1:SCAN HDFS", "value": {"id": "expected"}}
          ],
          "child_profiles": []
        }
      ],
      "info_strings": [],
      "num_children": 1
    }
  ],
  "info_strings": [
    {"key": "Summary", "value": {"Query Type": "QUERY"}}
  ],
  "num_children": 1
}
)json";
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);

  QueryProfileToolExecutor tool_executor;
  const Status create_status =
      CreateQueryProfileToolExecutorForProfile(
          profile_json, &tool_executor, /*profile_size_limit_bytes=*/-1,
          string_view(profile_text).size());
  ASSERT_TRUE(create_status.ok()) << create_status.GetDetail();

  rapidjson::Document node_doc;
  const Status run_status = tool_executor(
      "get_operator", R"({"node_id":"1","fragment_id":"F00"})", &node_doc);
  ASSERT_TRUE(run_status.ok()) << run_status.GetDetail();

  const string node_json = FormatJson(node_doc, /*pretty=*/false);
  ASSERT_TRUE(node_doc.IsObject()) << node_json;
  ASSERT_TRUE(node_doc.HasMember("id")) << node_json;
  ASSERT_TRUE(node_doc["id"].IsString()) << node_json;
  EXPECT_STREQ("expected", node_doc["id"].GetString());
}

TEST(
    QueryProfileParsingToolsTest,
    OperatorToolsProvideAveragedAllAndSpecificNodeViewsWithContext) {
  constexpr const char* profile_text = R"json(
{
  "profile_name": "Query (id=test-operator-id):",
  "child_profiles": [
    {
      "profile_name": "Execution Profile test-operator-id",
      "child_profiles": [
        {
          "profile_name": "Averaged Fragment F00",
          "info_strings": [
            {
              "key": "HDFS_SCAN_NODE (id=45)",
              "value": {"Table Name": "avg_table"}
            }
          ],
          "child_profiles": [],
          "num_children": 0
        },
        {
          "profile_name": "Fragment F00",
          "info_strings": [
            {
              "key": "Instance inst1 (host=host-a:27000)",
              "value": {
                "HDFS_SCAN_NODE (id=45)": {"Table Name": "table_a"}
              }
            },
            {
              "key": "Instance inst2 (host=host-b:27000)",
              "value": {
                "HDFS_SCAN_NODE (id=45)": {"Table Name": "table_b"}
              }
            }
          ],
          "child_profiles": [],
          "num_children": 0
        }
      ],
      "info_strings": [],
      "num_children": 2
    }
  ],
  "info_strings": [
    {"key": "Summary", "value": {"Query Type": "QUERY"}}
  ],
  "num_children": 1
}
)json";
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);

  QueryProfileToolExecutor tool_executor;
  const Status create_status =
      CreateQueryProfileToolExecutorForProfile(
          profile_json, &tool_executor, /*profile_size_limit_bytes=*/-1,
          string_view(profile_text).size());
  ASSERT_TRUE(create_status.ok()) << create_status.GetDetail();

  rapidjson::Document averaged_doc;
  const Status averaged_status =
      tool_executor("get_average_operator", R"({"node_id":"45"})", &averaged_doc);
  ASSERT_TRUE(averaged_status.ok()) << averaged_status.GetDetail();
  const string averaged_json = FormatJson(averaged_doc, /*pretty=*/false);
  ASSERT_TRUE(averaged_doc.IsObject()) << averaged_json;
  ASSERT_TRUE(averaged_doc.HasMember("fragment")) << averaged_json;
  ASSERT_TRUE(averaged_doc["fragment"].IsString()) << averaged_json;
  EXPECT_STR_CONTAINS(averaged_doc["fragment"].GetString(), "Averaged Fragment F00");
  ASSERT_TRUE(averaged_doc.HasMember("operator")) << averaged_json;
  ASSERT_TRUE(averaged_doc["operator"].IsString()) << averaged_json;
  EXPECT_STR_CONTAINS(averaged_doc["operator"].GetString(), "(id=45)");
  ASSERT_TRUE(averaged_doc.HasMember("section")) << averaged_json;
  ASSERT_TRUE(averaged_doc["section"].IsObject()) << averaged_json;
  ASSERT_TRUE(averaged_doc["section"].HasMember("Table Name")) << averaged_json;
  ASSERT_TRUE(averaged_doc["section"]["Table Name"].IsString()) << averaged_json;
  EXPECT_STREQ("avg_table", averaged_doc["section"]["Table Name"].GetString());

  rapidjson::Document all_doc;
  const Status all_status = tool_executor(
      "get_all_operators", R"({"node_id":"45","fragment_id":"F00"})", &all_doc);
  ASSERT_TRUE(all_status.ok()) << all_status.GetDetail();
  const string all_json = FormatJson(all_doc, /*pretty=*/false);
  ASSERT_TRUE(all_doc.IsArray()) << all_json;
  ASSERT_EQ(2, all_doc.Size()) << all_json;
  for (const auto& row : all_doc.GetArray()) {
    ASSERT_TRUE(row.IsObject()) << all_json;
    ASSERT_TRUE(row.HasMember("fragment")) << all_json;
    ASSERT_TRUE(row.HasMember("instance")) << all_json;
    ASSERT_TRUE(row.HasMember("operator")) << all_json;
    ASSERT_TRUE(row.HasMember("section")) << all_json;
    ASSERT_TRUE(row["instance"].IsString()) << all_json;
    ASSERT_TRUE(row["operator"].IsString()) << all_json;
    ASSERT_TRUE(row["section"].IsObject()) << all_json;
    EXPECT_STR_CONTAINS(row["operator"].GetString(), "(id=45)");
    EXPECT_STR_CONTAINS(row["instance"].GetString(), "host=");
  }

  rapidjson::Document specific_doc;
  const Status specific_status = tool_executor("get_specific_operator",
      R"({"node_id":"45","fragment_id":"F00","instance_id":"inst2"})", &specific_doc);
  ASSERT_TRUE(specific_status.ok()) << specific_status.GetDetail();
  const string specific_json = FormatJson(specific_doc, /*pretty=*/false);
  ASSERT_TRUE(specific_doc.IsObject()) << specific_json;
  ASSERT_TRUE(specific_doc.HasMember("instance")) << specific_json;
  ASSERT_TRUE(specific_doc["instance"].IsString()) << specific_json;
  EXPECT_STR_CONTAINS(specific_doc["instance"].GetString(), "inst2");
  ASSERT_TRUE(specific_doc.HasMember("section")) << specific_json;
  ASSERT_TRUE(specific_doc["section"].IsObject()) << specific_json;
  ASSERT_TRUE(specific_doc["section"].HasMember("Table Name")) << specific_json;
  ASSERT_TRUE(specific_doc["section"]["Table Name"].IsString()) << specific_json;
  EXPECT_STREQ("table_b", specific_doc["section"]["Table Name"].GetString());

  rapidjson::Document fragment_doc;
  const Status fragment_status =
      tool_executor("get_fragment", R"({"fragment_id":"F00"})", &fragment_doc);
  ASSERT_TRUE(fragment_status.ok()) << fragment_status.GetDetail();
  const string fragment_json = FormatJson(fragment_doc, /*pretty=*/false);
  ASSERT_TRUE(fragment_doc.IsObject()) << fragment_json;
  ASSERT_TRUE(fragment_doc.HasMember("HDFS_SCAN_NODE (id=45)")) << fragment_json;
  ASSERT_TRUE(fragment_doc["HDFS_SCAN_NODE (id=45)"].IsObject()) << fragment_json;
  ASSERT_TRUE(
      fragment_doc["HDFS_SCAN_NODE (id=45)"].HasMember("Table Name")) << fragment_json;
  ASSERT_TRUE(
      fragment_doc["HDFS_SCAN_NODE (id=45)"]["Table Name"].IsString()) << fragment_json;
  EXPECT_STREQ(
      "avg_table", fragment_doc["HDFS_SCAN_NODE (id=45)"]["Table Name"].GetString());

  rapidjson::Document all_fragments_doc;
  const Status all_fragments_status =
      tool_executor("AllFragments", R"({"fragment_id":"F00"})", &all_fragments_doc);
  ASSERT_TRUE(all_fragments_status.ok()) << all_fragments_status.GetDetail();
  const string all_fragments_json = FormatJson(all_fragments_doc, /*pretty=*/false);
  ASSERT_TRUE(all_fragments_doc.IsArray()) << all_fragments_json;
  ASSERT_EQ(2, all_fragments_doc.Size()) << all_fragments_json;
  for (const auto& row : all_fragments_doc.GetArray()) {
    ASSERT_TRUE(row.IsObject()) << all_fragments_json;
    ASSERT_TRUE(row.HasMember("fragment")) << all_fragments_json;
    ASSERT_TRUE(row.HasMember("section")) << all_fragments_json;
    ASSERT_TRUE(row["fragment"].IsString()) << all_fragments_json;
    ASSERT_TRUE(row["section"].IsObject()) << all_fragments_json;
  }

}

TEST(QueryProfileParsingToolsTest,
    ParseDurationMsParsesMixedUnitsIncludingDecimalMilliseconds) {
  EXPECT_EQ(3723004.0, test::ParseDurationMs("1h2m3s4ms"));
  EXPECT_EQ(3723001.34, test::ParseDurationMs("1h2m3s1.34ms"));
  EXPECT_EQ(120125.0, test::ParseDurationMs("2m0.125s"));
  EXPECT_EQ(-1.0, test::ParseDurationMs("not-a-duration"));
}

} // namespace

} // namespace impala
