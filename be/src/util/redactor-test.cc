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

#include "redactor.h"
#include "redactor.detail.h"

#include <cstdlib>  // rand
#include <cstdio>  // file stuff
#include <pthread.h>
#include <unistd.h>  // cpu info

#include "redactor-test-utils.h"
#include "testutil/gtest-util.h"

namespace impala {

using std::string;
extern std::vector<Rule>* g_rules;

void* MultiThreadWorkload(void* unused) {
  unsigned int rand_seed = RandSeed();
  int buffer_size = 10000 + rand_r(&rand_seed) % 1000;
  char buffer[buffer_size];
  string message;
  for (int i = 0; i < 100; ++i) {
    RandomlyFillString(buffer, buffer_size);
    message = buffer;
    Redact(&message);
    if ((buffer_size - 1) != message.length()) {
      ADD_FAILURE() << "Message length changed; new size is " << message.length();
      return NULL;
    }
    for (int c = 0; c < buffer_size - 1; ++c) {
      if ('0' <= message[c] && message[c] <= '9') {
        ADD_FAILURE() << "Number " << message[c] << " should be replaced with #";
        return NULL;
      }
      if (message[c] < ' ' || '~' < message[c]) {
        ADD_FAILURE() << "Unexpected char " << message[c];
        return NULL;
      }
    }
    if (message[buffer_size - 1] != '\0') {
      ADD_FAILURE() << "Missing string terminator";
      return NULL;
    }
  }
  return NULL;
}

TEST(RedactorTest, NoTrigger) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"foo\", \"replace\": \"bar\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_EQ(1, g_rules->size());
  ASSERT_EQ("", g_rules->begin()->trigger);
  ASSERT_EQ("foo", g_rules->begin()->search_pattern.pattern());
  ASSERT_EQ("bar", g_rules->begin()->replacement);
  ASSERT_UNREDACTED("baz");
  ASSERT_REDACTED_EQ("foo", "bar");
  ASSERT_REDACTED_EQ("foo bar foo baz", "bar bar bar baz");
  ASSERT_REDACTED_EQ("foo\nbar\nfoo baz", "bar\nbar\nbar baz");
}

TEST(RedactorTest, Trigger) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"trigger\": \"baz\", \"search\": \"foo\", \"replace\": \"bar\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_EQ(1, g_rules->size());
  ASSERT_EQ("baz", g_rules->begin()->trigger);
  ASSERT_UNREDACTED("foo");
  ASSERT_REDACTED_EQ("foo bar foo baz", "bar bar bar baz");
}

TEST(RedactorTest, MultiTrigger) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"\\\\d+\", \"replace\": \"#\"},"
      "    {\"trigger\": \"baz\", \"search\": \"foo\", \"replace\": \"bar\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_EQ(2, g_rules->size());
  ASSERT_REDACTED_EQ("foo33", "foo#");
  ASSERT_REDACTED_EQ("foo foo baz!3", "bar bar baz!#");
}

TEST(RedactorTest, CaseSensitivityProperty) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"(C|d)+\", \"replace\": \"_\", \"caseSensitive\": false}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_UNREDACTED("123");
  ASSERT_REDACTED_EQ("abcD Cd c D d C", "ab_ _ _ _ _ _");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {"
      "      \"trigger\": \"BaZ\","
      "      \"caseSensitive\": false,"
      "      \"search\": \"bAz\","
      "      \"replace\": \"bar\""
      "    }"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_REDACTED_EQ("bAz bar", "bar bar");
  ASSERT_REDACTED_EQ("BAz bar", "bar bar");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {"
      "      \"trigger\": \"FOO\","
      "      \"caseSensitive\": false,"
      "      \"search\": \"foO\","
      "      \"replace\": \"BAR\""
      "    }"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_REDACTED_EQ("fOO bar", "BAR bar");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"(Xy)+\", \"replace\": \"$\", \"caseSensitive\": true}"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_UNREDACTED("xY");
  ASSERT_REDACTED_EQ("Xy", "$");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {"
      "      \"trigger\": \"Sensitive\","
      "      \"caseSensitive\": true,"
      "      \"search\": \"SsS\","
      "      \"replace\": \"sss\""
      "    }"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_UNREDACTED("SsS");
  ASSERT_UNREDACTED("sensitive SsS");
  ASSERT_UNREDACTED("Sensitive sss");
  ASSERT_REDACTED_EQ("Sensitive SsS", "Sensitive sss");

  rules_file.OverwriteContents(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {"
      "      \"trigger\": \"QQQ\","
      "      \"search\": \"qQq\","
      "      \"replace\": \"QqQ\""
      "    }"
      "  ]"
      "}");
  error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_UNREDACTED("qQq");
  ASSERT_UNREDACTED("QQQ");
  ASSERT_UNREDACTED("QQq qQq");
  ASSERT_REDACTED_EQ("QQQ qQq", "QQQ QqQ");
}

TEST(RedactorTest, SingleTriggerMultiRule) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"trigger\": \"baz\", \"search\": \"\\\\d+\", \"replace\": \"#\"},"
      "    {\"trigger\": \"baz\", \"search\": \"foo\", \"replace\": \"bar\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_EQ(2, g_rules->size());
  ASSERT_UNREDACTED("foo33");
  ASSERT_REDACTED_EQ("foo foo baz!3", "bar bar baz!#");
}

TEST(RedactorTest, RuleOrder) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"trigger\": \"barC\", \"search\": \".*\", \"replace\": \"Z\"},"
      "    {\"search\": \"1\", \"replace\": \"2\"},"
      "    {\"search\": \"1\", \"replace\": \"3\"},"
      "    {\"trigger\": \"foo\", \"search\": \"2\", \"replace\": \"A\"},"
      "    {\"trigger\": \"bar\", \"search\": \"2\", \"replace\": \"1\"},"
      "    {\"search\": \"1\", \"replace\": \"4\"},"
      "    {\"search\": \"1\", \"replace\": \"5\"},"
      "    {\"trigger\": \"foo\", \"search\": \"A\", \"replace\": \"C\"},"
      "    {\"trigger\": \"bar\", \"search\": \"5\", \"replace\": \"1\"},"
      "    {\"trigger\": \"barC\", \"search\": \".*\", \"replace\": \"D\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_EQ(10, g_rules->size());
  ASSERT_UNREDACTED("foo");
  ASSERT_REDACTED_EQ("1", "2");
  ASSERT_REDACTED_EQ("foo1", "fooC");
  ASSERT_REDACTED_EQ("bar1", "bar4");
  ASSERT_REDACTED_EQ("bar5", "bar1");
  ASSERT_REDACTED_EQ("foobar1", "D");
}

TEST(RedactorTest, InputSize) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"[0-9]\", \"replace\": \"#\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_UNREDACTED("");
  int buffer_size = 10000;
  char buffer[buffer_size];
  RandomlyFillString(buffer, buffer_size);
  string message(buffer);
  Redact(&message);
  ASSERT_EQ(buffer_size - 1, message.length());
  for (int i = 0; i < buffer_size; ++i) {
    ASSERT_TRUE(message[i] < '0' || '9' < message[i])
        << "Number " << message[i] << " should be replaced with #";
  }
}

TEST(RedactorTest, ChangeInputSize) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"[A-Z]\", \"replace\": \"\"},"
      "    {\"trigger\": \"reduce\", \"search\": \"[0-9]+\", \"replace\": \"#\"},"
      "    {\"trigger\": \"add\", \"search\": \"[0-9]\", \"replace\": \"####\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);
  ASSERT_REDACTED_EQ("AAAAAAA", "");
  ASSERT_REDACTED_EQ("reduce1234", "reduce#");
  ASSERT_REDACTED_EQ("add1234", "add################");
}

TEST(RedactorTest, MultiThreaded) {
  TempRulesFile rules_file(
      "{"
      "  \"version\": 1,"
      "  \"rules\": ["
      "    {\"search\": \"0\", \"replace\": \"#\"},"
      "    {\"search\": \"1\", \"replace\": \"#\"},"
      "    {\"search\": \"2\", \"replace\": \"#\"},"
      "    {\"search\": \"3\", \"replace\": \"#\"},"
      "    {\"search\": \"4\", \"replace\": \"#\"},"
      "    {\"search\": \"5\", \"replace\": \"#\"},"
      "    {\"search\": \"6\", \"replace\": \"#\"},"
      "    {\"search\": \"7\", \"replace\": \"#\"},"
      "    {\"trigger\": \"8\", \"search\": \"8\", \"replace\": \"#\"},"
      "    {\"trigger\": \"9\", \"search\": \"9\", \"replace\": \"#\"}"
      "  ]"
      "}");
  string error = SetRedactionRulesFromFile(rules_file.name());
  ASSERT_EQ("", error);

  int processor_count = sysconf(_SC_NPROCESSORS_ONLN);
  int worker_count = 2 * processor_count;
  pthread_t worker_ids[worker_count];
  for (int i = 0; i < worker_count; ++i) {
    int status = pthread_create(worker_ids + i, NULL, MultiThreadWorkload, NULL);
    ASSERT_EQ(0, status);
  }
  for (int i = 0; i < worker_count; ++i) {
    int status = pthread_join(worker_ids[i], NULL);
    ASSERT_EQ(0, status);
  }
}

}
