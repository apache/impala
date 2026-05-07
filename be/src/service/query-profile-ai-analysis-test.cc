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

#include "service/query-profile-ai-analysis.h"

#include <atomic>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <rapidjson/document.h>

#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"

using std::string;
using std::string_view;
using std::vector;

DECLARE_int32(query_profile_ai_analysis_max_concurrent_runs);

namespace impala {

namespace {

using test::FindAssistantMessageInResponse;
using test::ParseJsonObjectFromResponse;
using test::ReleaseAiAnalysisSlot;
using test::TryAcquireAiAnalysisSlot;
using test::TryExtractAssistantContentText;

TEST(QueryProfileAiAnalysisHelpersTest, ParseJsonObjectFromNoisyResponse) {
  constexpr string_view noisy_response =
      "model debug prefix\n"
      "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"final\"}}]}\n"
      "model suffix";

  rapidjson::Document response_doc;
  EXPECT_TRUE(ParseJsonObjectFromResponse(noisy_response, &response_doc));
  ASSERT_TRUE(response_doc.IsObject());
  ASSERT_TRUE(response_doc.HasMember("choices"));
  ASSERT_TRUE(response_doc["choices"].IsArray());
}

TEST(QueryProfileAiAnalysisHelpersTest, ParseJsonObjectFromResponseRejectsNonJsonText) {
  constexpr string_view non_json_response = "there is no json object in this response";
  rapidjson::Document response_doc;
  EXPECT_FALSE(ParseJsonObjectFromResponse(non_json_response, &response_doc));
}

TEST(QueryProfileAiAnalysisHelpersTest, ExtractAssistantContentFromNestedArrayParts) {
  constexpr string_view response = R"json(
prefix
{"choices":[{"message":{"content":[
  {"text":"first"},
  {"output_text":"second"},
  {"text":{"value":"third"}},
  {"text":null},
  7,
  {"foo":"bar"}
]}}]}
suffix
)json";

  rapidjson::Document response_doc;
  ASSERT_TRUE(ParseJsonObjectFromResponse(response, &response_doc));
  const rapidjson::Value* assistant_message =
      FindAssistantMessageInResponse(response_doc);
  ASSERT_NE(nullptr, assistant_message);

  string assistant_content = "stale";
  EXPECT_TRUE(TryExtractAssistantContentText(assistant_message, &assistant_content));
  EXPECT_EQ("first\nsecond\nthird", assistant_content);
}

TEST(QueryProfileAiAnalysisHelpersTest, ExtractAssistantContentHandlesStringContent) {
  rapidjson::Document response_doc;
  response_doc.Parse(R"json({"role":"assistant","content":"plain content"})json");
  ASSERT_FALSE(response_doc.HasParseError());

  const rapidjson::Value* assistant_message =
      FindAssistantMessageInResponse(response_doc);
  ASSERT_NE(nullptr, assistant_message);

  string assistant_content;
  EXPECT_TRUE(TryExtractAssistantContentText(assistant_message, &assistant_content));
  EXPECT_EQ("plain content", assistant_content);
}

TEST(QueryProfileAiAnalysisHelpersTest,
    ExtractAssistantContentRejectsNullOrAbnormalContent) {
  rapidjson::Document assistant_message;
  assistant_message.SetObject();
  rapidjson::Document::AllocatorType& alloc = assistant_message.GetAllocator();
  rapidjson::Value null_content;
  null_content.SetNull();
  assistant_message.AddMember("content", null_content, alloc);

  string assistant_content = "previous";
  EXPECT_FALSE(TryExtractAssistantContentText(&assistant_message, &assistant_content));
  EXPECT_TRUE(assistant_content.empty());

  rapidjson::Document object_content_message;
  object_content_message.SetObject();
  rapidjson::Value object_content(rapidjson::kObjectType);
  object_content.AddMember("unexpected", 1, object_content_message.GetAllocator());
  object_content_message.AddMember(
      "content", object_content, object_content_message.GetAllocator());

  assistant_content = "previous";
  EXPECT_FALSE(TryExtractAssistantContentText(
      &object_content_message, &assistant_content));
  EXPECT_TRUE(assistant_content.empty());
}

TEST(QueryProfileAiAnalysisHelpersTest, FindAssistantMessageRejectsAbnormalEnvelope) {
  rapidjson::Document response_doc;
  response_doc.Parse(R"json({"choices":[{"message":"not-an-object"}]})json");
  ASSERT_FALSE(response_doc.HasParseError());

  const rapidjson::Value* assistant_message =
      FindAssistantMessageInResponse(response_doc);
  EXPECT_EQ(nullptr, assistant_message);
}

TEST(QueryProfileAiAnalysisHelpersTest, MaxConcurrentSlotsMultithreaded) {
  const auto scoped_max_concurrent_runs = ScopedFlagSetter<int32_t>::Make(
      &FLAGS_query_profile_ai_analysis_max_concurrent_runs, 4);
  constexpr int num_threads = 10;
  std::atomic<int> successful_acquires(0);

  vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&successful_acquires]() {
      if (TryAcquireAiAnalysisSlot()) {
        successful_acquires.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(FLAGS_query_profile_ai_analysis_max_concurrent_runs,
      successful_acquires.load());
  EXPECT_FALSE(TryAcquireAiAnalysisSlot());

  for (int i = 0; i < successful_acquires.load(); ++i) ReleaseAiAnalysisSlot();
}

TEST(QueryProfileAiAnalysisDeathTest, ReleaseSlotCannotDropBelowZero) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  constexpr int32_t kMaxConcurrentRuns = 1;
  const auto scoped_max_concurrent_runs = ScopedFlagSetter<int32_t>::Make(
      &FLAGS_query_profile_ai_analysis_max_concurrent_runs, kMaxConcurrentRuns);
  ASSERT_TRUE(TryAcquireAiAnalysisSlot());
  ReleaseAiAnalysisSlot();
  IMPALA_ASSERT_DEBUG_DEATH(
      ReleaseAiAnalysisSlot(), "impala::ReleaseAiAnalysisSlot\\(\\)");
}

} // namespace

} // namespace impala
