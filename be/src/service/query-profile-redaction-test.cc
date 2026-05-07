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

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "gutil/strings/substitute.h"
#include "testutil/gtest-util.h"

using std::string;
using strings::Substitute;

namespace impala {

static constexpr const char* PROFILE_FILE = "tpcds_72_local_run_profile.json";
static constexpr const char* UPDATE_GOLDENS_ENV_VAR =
    "UPDATE_QUERY_PROFILE_AI_ANALYSIS_TOOL_GOLDENS";
static constexpr const char* TESTDATA_BASE_RELATIVE_PATH = "testdata/impala-profiles";
static constexpr const char* NON_JSON_PROFILE_INPUT = "not a json query profile";

static string ReadFileToString(const string& path) {
  std::ifstream in(path);
  if (!in.is_open()) return "";
  std::stringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

static string GetProfilesBasePath(const char* impala_home) {
  return Substitute("$0/$1", impala_home, TESTDATA_BASE_RELATIVE_PATH);
}

static string LoadTestProfileText(const char* impala_home, string* profile_path) {
  if (profile_path == nullptr) return "";
  const string profiles_base = GetProfilesBasePath(impala_home);
  *profile_path = Substitute("$0/$1", profiles_base, PROFILE_FILE);
  return ReadFileToString(*profile_path);
}

static bool ShouldUpdateGoldens() {
  return std::getenv(UPDATE_GOLDENS_ENV_VAR) != nullptr;
}

static const char* GetImpalaHome() {
  return std::getenv("IMPALA_HOME");
}

static void ExpectRejectedNonJsonProfileInput(const Status& status) {
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "valid JSON object");
}

static bool WriteStringToFile(const string& path, const string& contents) {
  std::ofstream out(path);
  if (!out.is_open()) return false;
  out << contents;
  return out.good();
}

TEST(QueryProfileRedactionTest, RedactedProfileMatchesGoldenForTpcds72) {
  const char* impala_home = GetImpalaHome();
  ASSERT_NE(nullptr, impala_home);
  string profile_path;
  const string profile_text = LoadTestProfileText(impala_home, &profile_path);
  ASSERT_FALSE(profile_text.empty()) << "failed to read " << profile_path;

  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_text));
  const string& redacted_text = redactor.redacted_profile_text();
  ASSERT_FALSE(redacted_text.empty());
  const string unredacted_text = redactor.Unredact(redacted_text);
  EXPECT_EQ(profile_text, unredacted_text);

  const string golden_dir = Substitute(
      "$0/testdata/impala-profiles/query-profile-redaction-expected",
      impala_home);
  const string golden_path =
      Substitute("$0/tpcds_72_local_run_profile.redacted.json", golden_dir);
  const bool update_goldens = ShouldUpdateGoldens();
  if (update_goldens) {
    std::filesystem::create_directories(golden_dir);
    ASSERT_TRUE(WriteStringToFile(golden_path, redacted_text))
        << "failed to write " << golden_path;
    return;
  }

  const string expected_redacted_text = ReadFileToString(golden_path);
  ASSERT_FALSE(expected_redacted_text.empty())
      << "missing redaction golden at " << golden_path
      << ". Re-run test with UPDATE_QUERY_PROFILE_AI_ANALYSIS_TOOL_GOLDENS=1";
  EXPECT_EQ(expected_redacted_text, redacted_text);
}

TEST(QueryProfileRedactionTest, RedactionRejectsNonJsonProfileInput) {
  QueryProfileRedactor redactor;
  Status status = redactor.Redact(NON_JSON_PROFILE_INPUT);
  ExpectRejectedNonJsonProfileInput(status);
}

TEST(QueryProfileRedactionTest, RedactionRejectsEmptyAndMalformedJsonInput) {
  QueryProfileRedactor empty_redactor;
  ExpectRejectedNonJsonProfileInput(empty_redactor.Redact(""));

  QueryProfileRedactor malformed_redactor;
  ExpectRejectedNonJsonProfileInput(
      malformed_redactor.Redact(R"({"info_strings":[{"key":"x","value":}])"));
}

TEST(QueryProfileRedactionTest, RedactionRejectsInputLargerThanConfiguredLimit) {
  const string profile_text = R"({
  "info_strings": [
    {
      "key": "Sql Statement",
      "value": "select very_large_value from tiny_limit_table"
    }
  ]
})";
  QueryProfileRedactor redactor(/*profile_size_limit_bytes=*/32);
  const Status status = redactor.Redact(profile_text);
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(
      status.GetDetail(), "configured redaction profile size limit");
}

TEST(QueryProfileRedactionTest, RedactionRespectsConfiguredProfileSizeLimitOverride) {
  const string profile_text = R"({
  "info_strings": [
    {
      "key": "Sql Statement",
      "value": "select value_col from override_limit_table"
    }
  ]
})";
  QueryProfileRedactor redactor(/*profile_size_limit_bytes=*/1024);
  ASSERT_OK(redactor.Redact(profile_text));
  ASSERT_FALSE(redactor.redacted_profile_text().empty());
}

TEST(QueryProfileRedactionTest, UnredactionLeavesUnrelatedTextUntouched) {
  const string profile_text = R"({
  "info_strings": [
    {
      "key": "Analyzed query",
      "value": "select db_one.tbl_one.col_one from db_one.tbl_one"
    }
  ]
})";
  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_text));

  const string unrelated_text = "totally unrelated text that has no aliases";
  EXPECT_EQ(unrelated_text, redactor.Unredact(unrelated_text));
}

TEST(QueryProfileRedactionTest, RegexDrivenRedactionsAreCoveredAndReversible) {
  const string profile_text =
      "{\n"
      "  \"info_strings\": [\n"
      "    {\n"
      "      \"key\": \"Sql Statement\",\n"
      "      \"value\": \"select raw_value from pii_db.customer_table where "
      "owner='ops-team@example.com'\"\n"
      "    },\n"
      "    {\n"
      "      \"key\": \"User\",\n"
      "      \"value\": \"primary_user\"\n"
      "    },\n"
      "    {\n"
      "      \"key\": \"Analyzed query\",\n"
      "      \"value\": \"select sales_db.order_table.customer_id, snake_case_col "
      "from sales_db.order_table join dim_db.customer_dim on "
      "sales_db.order_table.customer_id = dim_db.customer_dim.customer_id "
      "where extra_snake_col = 1\"\n"
      "    },\n"
      "    {\n"
      "      \"key\": \"Plan\",\n"
      "      \"value\": \"Prefix text\\nAnalyzed query: "
      "select misc_db.misc_table.misc_col from misc_db.misc_table\\n\\n"
      "F00:PLAN FRAGMENT\"\n"
      "    },\n"
      "    {\n"
      "      \"key\": \"Per Host Min Memory Reservation\",\n"
      "      \"value\": \"worker-a.example.com:22000(2.00 GB) "
      "worker-b.example.com:23000(3.00 GB)\"\n"
      "    },\n"
      "    {\n"
      "      \"key\": \"Custom\",\n"
      "      \"value\": \"uid=service_user owner=ops-team@example.com "
      "source_ip=10.20.30.40 source_ipv6=2001:db8::7\"\n"
      "    }\n"
      "  ]\n"
      "}";

  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_text));
  const string& redacted_text = redactor.redacted_profile_text();
  ASSERT_FALSE(redacted_text.empty());

  EXPECT_STR_CONTAINS(redacted_text, "[REDACTED_SQL_STATEMENT]");
  EXPECT_STR_CONTAINS(redacted_text, "user_");
  EXPECT_STR_CONTAINS(redacted_text, "ip_");
  EXPECT_STR_CONTAINS(redacted_text, "table_");
  EXPECT_STR_CONTAINS(redacted_text, "column_");
  EXPECT_STR_CONTAINS(redacted_text, "host_");

  EXPECT_EQ(string::npos, redacted_text.find("ops-team@example.com"));
  EXPECT_EQ(string::npos, redacted_text.find("10.20.30.40"));
  EXPECT_EQ(string::npos, redacted_text.find("2001:db8::7"));
  EXPECT_EQ(string::npos, redacted_text.find("worker-a.example.com"));
  EXPECT_EQ(string::npos, redacted_text.find("sales_db.order_table"));
  EXPECT_EQ(string::npos, redacted_text.find("snake_case_col"));

  EXPECT_EQ(profile_text, redactor.Unredact(redacted_text));
}

TEST(QueryProfileRedactionTest, UnredactionDoesNotCascadeAliasReplacements) {
  const string profile_text =
      "{\n"
      "  \"info_strings\": [\n"
      "    {\n"
      "      \"key\": \"Analyzed query\",\n"
      "      \"value\": \"select decoy_table_002.id from decoy_table_002 "
      "join original_target on decoy_table_002.id = original_target.id\"\n"
      "    }\n"
      "  ]\n"
      "}";
  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_text));
  const string& redacted_text = redactor.redacted_profile_text();
  EXPECT_STR_CONTAINS(redacted_text, "table_001");
  EXPECT_STR_CONTAINS(redacted_text, "table_002");
  EXPECT_EQ(profile_text, redactor.Unredact(redacted_text));
}

TEST(QueryProfileRedactionTest, RedactionDoesNotReplaceInsideLargerIdentifiers) {
  const string profile_text = R"({
  "info_strings": [
    {
      "key": "Analyzed query",
      "value": "select foo_table.id from foo_table where foo_table.id = 1"
    },
    {
      "key": "Custom",
      "value": "unrelated_token=foo_tablex"
    }
  ]
})";
  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_text));

  const string& redacted_text = redactor.redacted_profile_text();
  EXPECT_STR_CONTAINS(redacted_text, "from table_");
  EXPECT_STR_CONTAINS(redacted_text, "foo_tablex");
  EXPECT_EQ(string::npos, redacted_text.find("table_001x"));
  EXPECT_EQ(profile_text, redactor.Unredact(redacted_text));
}

} // namespace impala
