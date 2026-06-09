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

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <rapidjson/document.h>

#include "gutil/strings/substitute.h"
#include "testutil/gtest-util.h"
#include "util/json-util.h"

using std::string;
using strings::Substitute;

namespace impala {

static constexpr const char* PROFILE_FILE = "tpcds_72_local_run_profile.json";
static constexpr const char* UPDATE_GOLDENS_ENV_VAR =
    "UPDATE_QUERY_PROFILE_AI_ANALYSIS_TOOL_GOLDENS";
static constexpr const char* TESTDATA_BASE_RELATIVE_PATH = "testdata/impala-profiles";
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

static void ParseProfileJsonOrDie(const string& profile_text,
    rapidjson::Document* profile_json) {
  ASSERT_NE(profile_json, nullptr);
  profile_json->Parse(
      profile_text.data(), static_cast<rapidjson::SizeType>(profile_text.size()));
  ASSERT_FALSE(profile_json->HasParseError());
  ASSERT_TRUE(profile_json->IsObject());
}

static string CanonicalizeJsonTextOrDie(const string& json_text) {
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(json_text, &profile_json);
  return JsonToString(profile_json);
}

static string RedactedProfileText(const QueryProfileRedactor& redactor) {
  return JsonToString(redactor.redacted_profile_json());
}

static bool ContainsStandaloneIdentifierToken(
    const string& text, const string& token) {
  if (token.empty()) return false;
  size_t pos = 0;
  while ((pos = text.find(token, pos)) != string::npos) {
    const bool start_boundary_ok = pos == 0
        || (!std::isalnum(static_cast<unsigned char>(text[pos - 1]))
               && text[pos - 1] != '_');
    const size_t token_end = pos + token.size();
    const bool end_boundary_ok = token_end == text.size()
        || (!std::isalnum(static_cast<unsigned char>(text[token_end]))
               && text[token_end] != '_');
    if (start_boundary_ok && end_boundary_ok) return true;
    pos = token_end;
  }
  return false;
}

static bool ContainsString(const std::vector<string>& values, const string& value) {
  return std::find(values.begin(), values.end(), value) != values.end();
}

TEST(QueryProfileRedactionTest, RedactedProfileMatchesGoldenForTpcds72) {
  const char* impala_home = GetImpalaHome();
  ASSERT_NE(nullptr, impala_home);
  string profile_path;
  const string profile_text = LoadTestProfileText(impala_home, &profile_path);
  ASSERT_FALSE(profile_text.empty()) << "failed to read " << profile_path;
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);

  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_json));
  const string redacted_text = RedactedProfileText(redactor);
  ASSERT_FALSE(redacted_text.empty());

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
  EXPECT_EQ(CanonicalizeJsonTextOrDie(expected_redacted_text),
      CanonicalizeJsonTextOrDie(redacted_text));
}

TEST(QueryProfileRedactionTest, RedactionRejectsNonObjectProfileInput) {
  QueryProfileRedactor redactor;
  rapidjson::Document profile_json;
  profile_json.SetString("not-a-json-object", profile_json.GetAllocator());
  Status status = redactor.Redact(profile_json);
  ExpectRejectedNonJsonProfileInput(status);
}

TEST(QueryProfileRedactionTest, RedactionRejectsNullAndArrayInput) {
  QueryProfileRedactor null_redactor;
  rapidjson::Document null_profile_json;
  null_profile_json.SetNull();
  ExpectRejectedNonJsonProfileInput(null_redactor.Redact(null_profile_json));

  QueryProfileRedactor array_redactor;
  rapidjson::Document array_profile_json;
  array_profile_json.SetArray();
  ExpectRejectedNonJsonProfileInput(array_redactor.Redact(array_profile_json));
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
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);
  QueryProfileRedactor redactor(/*profile_size_limit_bytes=*/32);
  const Status status = redactor.Redact(profile_json);
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
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);
  QueryProfileRedactor redactor(/*profile_size_limit_bytes=*/1024);
  ASSERT_OK(redactor.Redact(profile_json));
  ASSERT_TRUE(redactor.redacted_profile_json().IsObject());
  ASSERT_FALSE(RedactedProfileText(redactor).empty());
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
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);
  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_json));

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
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);

  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_json));
  const string redacted_text = RedactedProfileText(redactor);
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

  const string redacted_summary =
      "sql=[REDACTED_SQL_STATEMENT] user=user_001 ip=ip_001 "
      "host=host_001 table=table_001 column=column_001";
  const string unredacted_summary = redactor.Unredact(redacted_summary);
  EXPECT_EQ(string::npos, unredacted_summary.find("[REDACTED_SQL_STATEMENT]"));
  EXPECT_EQ(string::npos, unredacted_summary.find("user_001"));
  EXPECT_EQ(string::npos, unredacted_summary.find("ip_001"));
  EXPECT_EQ(string::npos, unredacted_summary.find("host_001"));
  EXPECT_EQ(string::npos, unredacted_summary.find("table_001"));
  EXPECT_EQ(string::npos, unredacted_summary.find("column_001"));
  EXPECT_STR_CONTAINS(
      unredacted_summary, "select raw_value from pii_db.customer_table");
  EXPECT_STR_CONTAINS(unredacted_summary, "primary_user");
  EXPECT_STR_CONTAINS(unredacted_summary, "10.20.30.40");
  EXPECT_STR_CONTAINS(unredacted_summary, "worker-a.example.com");
  EXPECT_STR_CONTAINS(unredacted_summary, "sales_db.order_table");
  EXPECT_STR_CONTAINS(unredacted_summary, "customer_id");
}

TEST(QueryProfileRedactionTest, CollectStringValuesFromJsonCollectsNestedValues) {
  const string json_text = R"({
  "root": "value_one",
  "nested": {
    "key": "value_two",
    "arr": ["value_three", {"leaf": "value_four"}]
  },
  "other": ["value_five", {"deep": ["value_six"]}]
})";
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(json_text, &profile_json);

  const std::vector<string> values =
      test::CollectStringValuesFromJsonForTest(profile_json);
  EXPECT_EQ(6, values.size());
  EXPECT_TRUE(ContainsString(values, "value_one"));
  EXPECT_TRUE(ContainsString(values, "value_two"));
  EXPECT_TRUE(ContainsString(values, "value_three"));
  EXPECT_TRUE(ContainsString(values, "value_four"));
  EXPECT_TRUE(ContainsString(values, "value_five"));
  EXPECT_TRUE(ContainsString(values, "value_six"));
}

TEST(QueryProfileRedactionTest, CollectRegexMatchesFromTextsHandlesCapturesAndDedup) {
  const std::vector<string> texts = {
      "owner=ops-team@example.com uid=service_user",
      "owner=ops-team@example.com uid=service_user",
      "owner=backup@example.com user=service_user"};
  const std::vector<string> email_matches =
      test::CollectRegexMatchesFromTextsForTest(
          texts, R"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})");
  EXPECT_EQ(2, email_matches.size());
  EXPECT_TRUE(ContainsString(email_matches, "ops-team@example.com"));
  EXPECT_TRUE(ContainsString(email_matches, "backup@example.com"));

  const std::vector<string> user_matches = test::CollectRegexMatchesFromTextsForTest(
      texts, R"(\b(?:user|uid)=([A-Za-z0-9._@-]+)\b)", /*group_index=*/1);
  EXPECT_EQ(1, user_matches.size());
  EXPECT_EQ("service_user", user_matches[0]);
}

TEST(QueryProfileRedactionTest, CollectIpv6MatchesFromTextsFiltersInvalidCandidates) {
  const std::vector<string> texts = {
      "valid=2001:db8::7 invalid_time=12:34 invalid_short=abcd:ef01",
      "valid=fe80::1 duplicate=2001:db8::7"};
  const std::vector<string> ipv6_matches =
      test::CollectIpv6MatchesFromTextsForTest(texts);
  EXPECT_EQ(2, ipv6_matches.size());
  EXPECT_TRUE(ContainsString(ipv6_matches, "2001:db8::7"));
  EXPECT_TRUE(ContainsString(ipv6_matches, "fe80::1"));
  EXPECT_FALSE(ContainsString(ipv6_matches, "12:34"));
  EXPECT_FALSE(ContainsString(ipv6_matches, "abcd:ef01"));
}

TEST(QueryProfileRedactionTest, EstimateSerializedJsonSizeMatchesJsonSerialization) {
  const string json_text = R"({
  "plain": "abc",
  "escaped": "a\"b\nc",
  "arr": [1, true, null, {"x": "y\tz"}],
  "double_values": [
    3.1631581491853288,
    0.00013492935534459787,
    1.2345e-20,
    -9.8765E+17
  ]
})";
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(json_text, &profile_json);

  EXPECT_EQ(JsonToString(profile_json).size(),
      test::EstimateSerializedJsonSizeForTest(profile_json));
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
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);
  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_json));
  const string redacted_text = RedactedProfileText(redactor);
  EXPECT_STR_CONTAINS(redacted_text, "table_001");
  EXPECT_STR_CONTAINS(redacted_text, "table_002");

  const string unredacted_text = redactor.Unredact("table_001 table_002");
  EXPECT_STR_CONTAINS(unredacted_text, "decoy_table_002");
  EXPECT_STR_CONTAINS(unredacted_text, "original_target");
  EXPECT_FALSE(ContainsStandaloneIdentifierToken(unredacted_text, "table_001"));
  EXPECT_FALSE(ContainsStandaloneIdentifierToken(unredacted_text, "table_002"));
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
  rapidjson::Document profile_json;
  ParseProfileJsonOrDie(profile_text, &profile_json);
  QueryProfileRedactor redactor;
  ASSERT_OK(redactor.Redact(profile_json));

  const string redacted_text = RedactedProfileText(redactor);
  EXPECT_STR_CONTAINS(redacted_text, "from table_");
  EXPECT_STR_CONTAINS(redacted_text, "foo_tablex");
  EXPECT_EQ(string::npos, redacted_text.find("table_001x"));

  const string unredacted_text = redactor.Unredact("table_001 table_001x");
  EXPECT_STR_CONTAINS(unredacted_text, "foo_table");
  EXPECT_STR_CONTAINS(unredacted_text, "table_001x");
}

TEST(QueryProfileRedactionTest,
    RegexCollectorsCaptureUserKvValuesAndEmailTokens) {
  const re2::RE2 user_kv_re(R"((?i:\b(?:user|uid)=([A-Za-z0-9._@-]+)\b))");
  const re2::RE2 email_re(R"(([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}))");

  const auto user_kv_matches = test::CollectRegexMatches(
      "uid=service_user UID=service_user user=analytics", user_kv_re);
  ASSERT_EQ(2, user_kv_matches.size());
  EXPECT_EQ("service_user", user_kv_matches[0]);
  EXPECT_EQ("analytics", user_kv_matches[1]);

  const auto email_matches = test::CollectRegexMatches(
      "owner=ops-team@example.com cc=ops-team@example.com "
      "alt=alerts@example.org", email_re);
  ASSERT_EQ(2, email_matches.size());
  EXPECT_EQ("ops-team@example.com", email_matches[0]);
  EXPECT_EQ("alerts@example.org", email_matches[1]);
}

} // namespace impala
