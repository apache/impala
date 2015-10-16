// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "redactor.h"
#include "redactor.detail.h"

#include <gtest/gtest.h>

#include "redactor-test-utils.h"

namespace impala {

using std::string;
using strings::Substitute;

extern std::vector<Rule>* g_rules;

TEST(ParserTest, FileNotFound) {
  TempRulesFile rules_file("");
  rules_file.Delete();
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "No such file");
}

TEST(ParserTest, EmptyFile) {
  TempRulesFile rules_file("");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_EQ(0, g_rules->size());
  ASSERT_UNREDACTED("foo33");

  rules_file.OverwriteContents(" \t\n ");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "Text only contains white space");
}

TEST(ParserTest, DescriptionPropertyIgnored) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"foo\", \"replace\": \"bar\", \"description\": \"def\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_REDACTED_EQ("foo", "bar");
}

TEST(ParserTest, InvalidJson) {
  TempRulesFile rules_file(
      "\"version\": 100,"
      "\"rules\": ["
      "  {\"search\": \"[0-9]\", \"replace\": \"#\"}"
      "]");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "either an object or array at root");

  rules_file.OverwriteContents(
      "[{"
      "  \"version\": 1.0,"
      "  \"rules\": ["
      "    {\"search\": \"[0-9]\", \"replace\": \"#\"}"
      "  ]"
      "}]");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "root element must be a JSON Object");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"ules\": ["
      "    {\"search\": \"[0-9]\", \"replace\": \"#\"}"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "unexpected property 'ules'");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"earch\": \"[0-9]\", \"replace\": \"#\"}"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "unexpected property 'earch'");

  rules_file.OverwriteContents("{!@#$}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "Name of an object member must be a string");
}

TEST(ParserTest, BadVersion) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 100,"
      "  \"rules\": ["
      "    {\"search\": \"[0-9]\", \"replace\": \"#\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "only version 1");

  int bad_value_count = 6;
  string bad_values[][2] = {
    {"1.0", "Float"},
    {"true", "Bool"},
    {"false", "Bool"},
    {"\"string\"", "String"},
    {"[]", "Array"},
    {"{}", "Object"},
  };
  ASSERT_EQ(sizeof(string) * bad_value_count * 2, sizeof(bad_values));
  for (int i = 0; i < bad_value_count; ++i) {
    rules_file.OverwriteContents(Substitute("{ \"version\": $0 }", bad_values[i][0]));
    error = SetRedactionRulesFromFile(rules_file.name());
    ASSERT_ERROR_MESSAGE_CONTAINS(
        error, Substitute("must be an Integer but is a $0", bad_values[i][1]).c_str());
  }

  rules_file.OverwriteContents(
      "{"
      "  \"rules\": ["
      "    {\"search\": \"[0-9]\", \"replace\": \"#\"}"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "version is required");
}

TEST(ParserTest, BadRegex) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"[0-9\", \"replace\": \"#\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "missing ]");
}

TEST(ParserTest, BadCaseSensitivtyValue) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"[0-9\", \"replace\": \"#\", \"caseSensitive\": 1}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error, "must be of type Bool");
}

TEST(ParserTest, RuleNumberInErrorMessage) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"[0-9]\", \"replace\": \"#\"},"
      "    {\"search\": \"[0-\", \"replace\": \"error\"},"
      "    {\"search\": \"[A-Z]\", \"replace\": \"_\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_ERROR_MESSAGE_CONTAINS(error,
      "Error parsing redaction rule #2; search regex is invalid; missing ]");
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
