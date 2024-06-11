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

#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include <random>

#include <boost/bind.hpp>

#include "common/object-pool.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/container-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"
#include "util/thread.h"

#include "common/names.h"

DECLARE_bool(gen_experimental_profile);
DECLARE_int32(status_report_interval_ms);
DECLARE_int32(periodic_counter_update_period_ms);

using std::mt19937;
using std::shuffle;

namespace impala {

/// Return true if this is one of the counters automatically added to profiles,
/// e.g. TotalTime.
static bool IsDefaultCounter(const string& counter_name) {
  return counter_name == "TotalTime" || counter_name == "InactiveTotalTime";
}

TEST(CountersTest, Basic) {
  ObjectPool pool;
  RuntimeProfile* profile_a = RuntimeProfile::Create(&pool, "ProfileA");
  RuntimeProfile* profile_a1 = RuntimeProfile::Create(&pool, "ProfileA1");
  RuntimeProfile* profile_a2 = RuntimeProfile::Create(&pool, "ProfileAb");

  TRuntimeProfileTree thrift_profile;

  profile_a->AddChild(profile_a1);
  profile_a->AddChild(profile_a2);

  // Test Empty
  profile_a->ToThrift(&thrift_profile);
  EXPECT_EQ(thrift_profile.nodes.size(), 3);
  thrift_profile.nodes.clear();

  RuntimeProfile::Counter* counter_a;
  RuntimeProfile::Counter* counter_b;
  RuntimeProfile::Counter* counter_merged;

  // Updating/setting counter
  counter_a = profile_a->AddCounter("A", TUnit::UNIT);
  EXPECT_TRUE(counter_a != NULL);
  counter_a->Add(10);
  counter_a->Add(-5);
  EXPECT_EQ(counter_a->value(), 5);
  counter_a->Set(1);
  EXPECT_EQ(counter_a->value(), 1);

  counter_b = profile_a2->AddCounter("B", TUnit::BYTES);
  EXPECT_TRUE(counter_b != NULL);

  // Update status to be included in ExecSummary
  TExecSummary exec_summary;
  TStatus status;
  status.__set_status_code(TErrorCode::CANCELLED);
  exec_summary.__set_status(status);
  profile_a->SetTExecSummary(exec_summary);

  // Serialize/deserialize to thrift
  profile_a->ToThrift(&thrift_profile);
  RuntimeProfile* from_thrift = RuntimeProfile::CreateFromThrift(&pool, thrift_profile);
  counter_merged = from_thrift->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);
  EXPECT_TRUE(from_thrift->GetCounter("Not there") == NULL);
  EXPECT_TRUE(from_thrift->GetCounter("Not there") == nullptr);
  TExecSummary exec_summary_result;
  from_thrift->GetExecSummary(&exec_summary_result);
  EXPECT_EQ(exec_summary_result.status, status);

  // Serialize/deserialize to archive string
  string archive_str;
  EXPECT_OK(profile_a->SerializeToArchiveString(&archive_str));
  TRuntimeProfileTree deserialized_thrift_profile;
  EXPECT_OK(RuntimeProfile::DeserializeFromArchiveString(
      archive_str, &deserialized_thrift_profile));
  RuntimeProfile* deserialized_profile =
      RuntimeProfile::CreateFromThrift(&pool, deserialized_thrift_profile);
  counter_merged = deserialized_profile->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);
  EXPECT_TRUE(deserialized_profile->GetCounter("Not there") == NULL);
  EXPECT_TRUE(deserialized_profile->GetCounter("Not there") == nullptr);
  deserialized_profile->GetExecSummary(&exec_summary_result);
  EXPECT_EQ(exec_summary_result.status, status);

  // Serialize/deserialize to compressed binary
  vector<uint8_t> compressed;
  EXPECT_OK(profile_a->Compress(&compressed));
  RuntimeProfile* deserialized_profile2;
  EXPECT_OK(
      RuntimeProfile::DecompressToProfile(compressed, &pool, &deserialized_profile2));
  counter_merged = deserialized_profile2->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);
  EXPECT_TRUE(deserialized_profile2->GetCounter("Not there") == NULL);
  EXPECT_TRUE(deserialized_profile2->GetCounter("Not there") == nullptr);
  deserialized_profile2->GetExecSummary(&exec_summary_result);
  EXPECT_EQ(exec_summary_result.status, status);

  // Averaged
  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "Merged", 2, true);
  averaged_profile->UpdateAggregatedFromInstance(from_thrift, 0);
  counter_merged = averaged_profile->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);

  // Update again, there should be no change.
  averaged_profile->UpdateAggregatedFromInstance(from_thrift, 0);
  EXPECT_EQ(counter_merged->value(), 1);

  counter_a = profile_a2->AddCounter("A", TUnit::UNIT);
  counter_a->Set(3);
  averaged_profile->UpdateAggregatedFromInstance(profile_a2, 1);
  EXPECT_EQ(counter_merged->value(), 2);

  // Update
  RuntimeProfile* updated_profile = RuntimeProfile::Create(&pool, "Updated");
  updated_profile->Update(thrift_profile);
  RuntimeProfile::Counter* counter_updated = updated_profile->GetCounter("A");
  EXPECT_EQ(counter_updated->value(), 1);

  // Update 2 more times, counters should stay the same
  updated_profile->Update(thrift_profile);
  updated_profile->Update(thrift_profile);
  EXPECT_EQ(counter_updated->value(), 1);
}

void ValidateCounter(RuntimeProfileBase* profile, const string& name, int64_t value) {
  RuntimeProfile::Counter* counter = profile->GetCounter(name);
  EXPECT_TRUE(counter != NULL);
  EXPECT_EQ(counter->value(), value);
}

TEST(CountersTest, MergeAndUpdate) {
  // Create two trees.  Each tree has two children, one of which has the
  // same name in both trees.  Merging the two trees should result in 3
  // children, with the counters from the shared child aggregated.

  ObjectPool pool;
  RuntimeProfile* profile1 = RuntimeProfile::Create(&pool, "Parent1");
  RuntimeProfile* p1_child1 = RuntimeProfile::Create(&pool, "Child1");
  RuntimeProfile* p1_child2 = RuntimeProfile::Create(&pool, "Child2");
  profile1->AddChild(p1_child1);
  profile1->AddChild(p1_child2);

  RuntimeProfile* profile2 = RuntimeProfile::Create(&pool, "Parent2");
  RuntimeProfile* p2_child1 = RuntimeProfile::Create(&pool, "Child1");
  RuntimeProfile* p2_child3 = RuntimeProfile::Create(&pool, "Child3");
  profile2->AddChild(p2_child1);
  profile2->AddChild(p2_child3);

  // Create parent level counters
  RuntimeProfile::Counter* parent1_shared =
      profile1->AddCounter("Parent Shared", TUnit::UNIT);
  RuntimeProfile::Counter* parent2_shared =
      profile2->AddCounter("Parent Shared", TUnit::UNIT);
  RuntimeProfile::Counter* parent1_only =
      profile1->AddCounter("Parent 1 Only", TUnit::UNIT);
  RuntimeProfile::Counter* parent2_only =
      profile2->AddCounter("Parent 2 Only", TUnit::UNIT);
  parent1_shared->Add(1);
  parent2_shared->Add(3);
  parent1_only->Add(2);
  parent2_only->Add(5);

  // Create child level counters
  RuntimeProfile::Counter* p1_c1_shared =
    p1_child1->AddCounter("Child1 Shared", TUnit::UNIT);
  RuntimeProfile::Counter* p1_c1_only =
    p1_child1->AddCounter("Child1 Parent 1 Only", TUnit::UNIT);
  RuntimeProfile::Counter* p1_c2 =
    p1_child2->AddCounter("Child2", TUnit::UNIT);
  RuntimeProfile::Counter* p2_c1_shared =
    p2_child1->AddCounter("Child1 Shared", TUnit::UNIT);
  RuntimeProfile::Counter* p2_c1_only =
    p1_child1->AddCounter("Child1 Parent 2 Only", TUnit::UNIT);
  RuntimeProfile::Counter* p2_c3 =
    p2_child3->AddCounter("Child3", TUnit::UNIT);
  p1_c1_shared->Add(10);
  p1_c1_only->Add(50);
  p2_c1_shared->Add(20);
  p2_c1_only->Add(100);
  p2_c3->Add(30);
  p1_c2->Add(40);

  // Merge the two and validate
  TRuntimeProfileTree tprofile1;
  profile1->ToThrift(&tprofile1);
  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "merged", 2, true);
  averaged_profile->UpdateAggregatedFromInstance(profile1, 0);
  averaged_profile->UpdateAggregatedFromInstance(profile2, 1);
  EXPECT_EQ(5, averaged_profile->num_counters());
  ValidateCounter(averaged_profile, "Parent Shared", 2);
  ValidateCounter(averaged_profile, "Parent 1 Only", 2);
  ValidateCounter(averaged_profile, "Parent 2 Only", 5);

  vector<RuntimeProfileBase*> children;
  averaged_profile->GetChildren(&children);
  EXPECT_EQ(children.size(), 3);

  for (int i = 0; i < 3; ++i) {
    RuntimeProfileBase* profile = children[i];
    if (profile->name().compare("Child1") == 0) {
      EXPECT_EQ(5, profile->num_counters());
      ValidateCounter(profile, "Child1 Shared", 15);
      ValidateCounter(profile, "Child1 Parent 1 Only", 50);
      ValidateCounter(profile, "Child1 Parent 2 Only", 100);
    } else if (profile->name().compare("Child2") == 0) {
      EXPECT_EQ(3, profile->num_counters());
      ValidateCounter(profile, "Child2", 40);
    } else if (profile->name().compare("Child3") == 0) {
      EXPECT_EQ(3, profile->num_counters());
      ValidateCounter(profile, "Child3", 30);
    } else {
      FAIL();
    }
  }

  // make sure we can print
  stringstream dummy;
  averaged_profile->PrettyPrint(&dummy);

  // Update profile2 w/ profile1 and validate
  profile2->Update(tprofile1);
  EXPECT_EQ(5, profile2->num_counters());
  ValidateCounter(profile2, "Parent Shared", 1);
  ValidateCounter(profile2, "Parent 1 Only", 2);
  ValidateCounter(profile2, "Parent 2 Only", 5);

  profile2->GetChildren(&children);
  EXPECT_EQ(children.size(), 3);

  for (int i = 0; i < 3; ++i) {
    RuntimeProfileBase* profile = children[i];
    if (profile->name().compare("Child1") == 0) {
      EXPECT_EQ(5, profile->num_counters());
      ValidateCounter(profile, "Child1 Shared", 10);
      ValidateCounter(profile, "Child1 Parent 1 Only", 50);
      ValidateCounter(profile, "Child1 Parent 2 Only", 100);
    } else if (profile->name().compare("Child2") == 0) {
      EXPECT_EQ(3, profile->num_counters());
      ValidateCounter(profile, "Child2", 40);
    } else if (profile->name().compare("Child3") == 0) {
      EXPECT_EQ(3, profile->num_counters());
      ValidateCounter(profile, "Child3", 30);
    } else {
      FAIL();
    }
  }

  // make sure we can print
  profile2->PrettyPrint(&dummy);
}

// Regression test for IMPALA-6694 - child order isn't preserved if a child
// is prepended between updates.
TEST(CountersTest, MergeAndUpdateChildOrder) {
  ObjectPool pool;
  // Add Child2 first.
  RuntimeProfile* profile1 = RuntimeProfile::Create(&pool, "Parent");
  RuntimeProfile* p1_child2 = RuntimeProfile::Create(&pool, "Child2");
  profile1->AddChild(p1_child2);
  TRuntimeProfileTree tprofile1_v1, tprofile1_v2, tprofile1_v3;
  profile1->ToThrift(&tprofile1_v1);

  // Update averaged and deserialized profiles from the serialized profile.
  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "merged", 2, true);
  RuntimeProfile* deserialized_profile = RuntimeProfile::Create(&pool, "Parent");
  averaged_profile->UpdateAggregatedFromInstance(profile1, 0);
  deserialized_profile->Update(tprofile1_v1);

  std::vector<RuntimeProfileBase*> tmp_children;
  averaged_profile->GetChildren(&tmp_children);
  EXPECT_EQ(1, tmp_children.size());
  EXPECT_EQ("Child2", tmp_children[0]->name());
  deserialized_profile->GetChildren(&tmp_children);
  EXPECT_EQ(1, tmp_children.size());
  EXPECT_EQ("Child2", tmp_children[0]->name());

  // Prepend Child1 and update profiles.
  RuntimeProfile* p1_child1 = RuntimeProfile::Create(&pool, "Child1");
  profile1->PrependChild(p1_child1);
  profile1->ToThrift(&tprofile1_v2);
  averaged_profile->UpdateAggregatedFromInstance(profile1, 0);
  deserialized_profile->Update(tprofile1_v2);

  averaged_profile->GetChildren(&tmp_children);
  EXPECT_EQ(2, tmp_children.size());
  EXPECT_EQ("Child1", tmp_children[0]->name());
  EXPECT_EQ("Child2", tmp_children[1]->name());
  deserialized_profile->GetChildren(&tmp_children);
  EXPECT_EQ(2, tmp_children.size());
  EXPECT_EQ("Child1", tmp_children[0]->name());
  EXPECT_EQ("Child2", tmp_children[1]->name());

  // Test that changes in order of children is handled gracefully by preserving the
  // order from the previous update. Sorting puts the children in descending total time
  // order.
  p1_child1->total_time_counter()->Set(1);
  p1_child2->total_time_counter()->Set(2);
  profile1->SortChildrenByTotalTime();
  profile1->GetChildren(&tmp_children);
  EXPECT_EQ("Child2", tmp_children[0]->name());
  EXPECT_EQ("Child1", tmp_children[1]->name());
  profile1->ToThrift(&tprofile1_v3);
  averaged_profile->UpdateAggregatedFromInstance(profile1, 0);
  deserialized_profile->Update(tprofile1_v2);

  // The previous order of children that were already present is preserved.
  averaged_profile->GetChildren(&tmp_children);
  EXPECT_EQ(2, tmp_children.size());
  EXPECT_EQ("Child1", tmp_children[0]->name());
  EXPECT_EQ("Child2", tmp_children[1]->name());
  deserialized_profile->GetChildren(&tmp_children);
  EXPECT_EQ(2, tmp_children.size());
  EXPECT_EQ("Child1", tmp_children[0]->name());
  EXPECT_EQ("Child2", tmp_children[1]->name());

  // Make sure we can print the profiles.
  stringstream dummy;
  averaged_profile->PrettyPrint(&dummy);
  deserialized_profile->PrettyPrint(&dummy);
}

TEST(CountersTest, TotalTimeCounters) {
  ObjectPool pool;

  // Set up a three layer profile: parent -> child1 -> child2
  RuntimeProfile* parent = RuntimeProfile::Create(&pool, "Parent");
  RuntimeProfile* child1 = RuntimeProfile::Create(&pool, "Child1");
  RuntimeProfile* child2 = RuntimeProfile::Create(&pool, "Child2");
  child1->AddChild(child2);
  parent->AddChild(child1);

  // Part 1: Test accumulation of time up from child2 to child1 to parent
  // One millisecond passes in child2
  int64_t one_milli_ns = 1 * NANOS_PER_MICRO * MICROS_PER_MILLI;
  child2->total_time_counter()->Add(1 * NANOS_PER_MICRO * MICROS_PER_MILLI);
  parent->ComputeTimeInProfile();
  EXPECT_EQ(child2->total_time(), one_milli_ns);
  EXPECT_EQ(child2->local_time(), one_milli_ns);

  // Child1 is a parent of child2, so it is expected to contain at least as much time
  // as in child2. In this case, it is equal. However, none of the time is local.
  EXPECT_EQ(child1->total_time(), one_milli_ns);
  EXPECT_EQ(child1->local_time(), 0);

  // The parent is in the same situation as child1
  EXPECT_EQ(parent->total_time(), one_milli_ns);
  EXPECT_EQ(parent->local_time(), 0);

  // Time now accumulates up to child1
  child1->total_time_counter()->Add(child2->total_time());
  parent->ComputeTimeInProfile();

  // This doesn't change anything for anyone
  EXPECT_EQ(child2->total_time(), one_milli_ns);
  EXPECT_EQ(child2->local_time(), one_milli_ns);
  EXPECT_EQ(child1->total_time(), one_milli_ns);
  EXPECT_EQ(child1->local_time(), 0);
  EXPECT_EQ(parent->total_time(), one_milli_ns);
  EXPECT_EQ(parent->local_time(), 0);

  // Time now accumulates up to parent
  parent->total_time_counter()->Add(child1->total_time());
  parent->ComputeTimeInProfile();

  // This doesn't change anything for the parent
  EXPECT_EQ(child2->total_time(), one_milli_ns);
  EXPECT_EQ(child2->local_time(), one_milli_ns);
  EXPECT_EQ(child1->total_time(), one_milli_ns);
  EXPECT_EQ(child1->local_time(), 0);
  EXPECT_EQ(parent->total_time(), one_milli_ns);
  EXPECT_EQ(parent->local_time(), 0);

  // Part 2: Time accumulated in middle child
  // Add 1ms to the middle child
  child1->total_time_counter()->Add(one_milli_ns);
  parent->ComputeTimeInProfile();

  // Child2 did not change
  EXPECT_EQ(child2->total_time(), one_milli_ns);
  EXPECT_EQ(child2->local_time(), one_milli_ns);

  // Child1 has 1ms more of total time and local time
  EXPECT_EQ(child1->total_time(), 2 * one_milli_ns);
  EXPECT_EQ(child1->local_time(), one_milli_ns);

  // Parent has more total time, but no local time
  EXPECT_EQ(parent->total_time(), 2 * one_milli_ns);
  EXPECT_EQ(parent->local_time(), 0);

  // Accumulate the middle child up to the parent
  parent->total_time_counter()->Add(one_milli_ns);
  parent->ComputeTimeInProfile();

  // Doesn't change anything
  EXPECT_EQ(child2->total_time(), one_milli_ns);
  EXPECT_EQ(child2->local_time(), one_milli_ns);
  EXPECT_EQ(child1->total_time(), 2 * one_milli_ns);
  EXPECT_EQ(child1->local_time(), one_milli_ns);
  EXPECT_EQ(parent->total_time(), 2 * one_milli_ns);
  EXPECT_EQ(parent->local_time(), 0);

  // Part 3: Time accumulated at parent
  // Add 1ms to the parent
  parent->total_time_counter()->Add(one_milli_ns);
  parent->ComputeTimeInProfile();

  // Child1 and child2 don't change
  EXPECT_EQ(child2->total_time(), one_milli_ns);
  EXPECT_EQ(child2->local_time(), one_milli_ns);
  EXPECT_EQ(child1->total_time(), 2 * one_milli_ns);
  EXPECT_EQ(child1->local_time(), one_milli_ns);

  // Parent has 1ms more total time and local time
  EXPECT_EQ(parent->total_time(), 3 * one_milli_ns);
  EXPECT_EQ(parent->local_time(), one_milli_ns);
}

TEST(CountersTest, HighWaterMarkCounters) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  RuntimeProfile::HighWaterMarkCounter* bytes_counter =
      profile->AddHighWaterMarkCounter("bytes", TUnit::BYTES);

  bytes_counter->Set(10);
  EXPECT_EQ(bytes_counter->current_value(), 10);
  EXPECT_EQ(bytes_counter->value(), 10);

  bytes_counter->Add(5);
  EXPECT_EQ(bytes_counter->current_value(), 15);
  EXPECT_EQ(bytes_counter->value(), 15);

  bytes_counter->Set(5);
  EXPECT_EQ(bytes_counter->current_value(), 5);
  EXPECT_EQ(bytes_counter->value(), 15);

  bytes_counter->Add(3);
  EXPECT_EQ(bytes_counter->current_value(), 8);
  EXPECT_EQ(bytes_counter->value(), 15);

  bool success = bytes_counter->TryAdd(20, 30);
  EXPECT_TRUE(success);
  EXPECT_EQ(bytes_counter->current_value(), 28);
  EXPECT_EQ(bytes_counter->value(), 28);

  success = bytes_counter->TryAdd(5, 30);
  EXPECT_FALSE(success);
  EXPECT_EQ(bytes_counter->current_value(), 28);
  EXPECT_EQ(bytes_counter->value(), 28);
}

TEST(CountersTest, SummaryStatsCounters) {
  ObjectPool pool;
  RuntimeProfile* profile1 = RuntimeProfile::Create(&pool, "Profile 1");
  RuntimeProfile::SummaryStatsCounter* summary_stats_counter_1 =
    profile1->AddSummaryStatsCounter("summary_stats", TUnit::UNIT);

  EXPECT_EQ(summary_stats_counter_1->value(), 0);
  EXPECT_EQ(summary_stats_counter_1->MinValue(), numeric_limits<int64_t>::max());
  EXPECT_EQ(summary_stats_counter_1->MaxValue(), numeric_limits<int64_t>::min());

  summary_stats_counter_1->UpdateCounter(10);
  EXPECT_EQ(summary_stats_counter_1->value(), 10);
  EXPECT_EQ(summary_stats_counter_1->MinValue(), 10);
  EXPECT_EQ(summary_stats_counter_1->MaxValue(), 10);

  // Check that the average stays the same when updating with the same number.
  summary_stats_counter_1->UpdateCounter(10);
  EXPECT_EQ(summary_stats_counter_1->value(), 10);
  EXPECT_EQ(summary_stats_counter_1->MinValue(), 10);
  EXPECT_EQ(summary_stats_counter_1->MaxValue(), 10);

  summary_stats_counter_1->UpdateCounter(40);
  EXPECT_EQ(summary_stats_counter_1->value(), 20);
  EXPECT_EQ(summary_stats_counter_1->MinValue(), 10);
  EXPECT_EQ(summary_stats_counter_1->MaxValue(), 40);

  // Verify an update with 0. This should still change the average as the number of
  // samples increase
  summary_stats_counter_1->UpdateCounter(0);
  EXPECT_EQ(summary_stats_counter_1->value(), 15);
  EXPECT_EQ(summary_stats_counter_1->MinValue(), 0);
  EXPECT_EQ(summary_stats_counter_1->MaxValue(), 40);

  // Verify a negative update..
  summary_stats_counter_1->UpdateCounter(-40);
  EXPECT_EQ(summary_stats_counter_1->value(), 4);
  EXPECT_EQ(summary_stats_counter_1->MinValue(), -40);
  EXPECT_EQ(summary_stats_counter_1->MaxValue(), 40);

  RuntimeProfile* profile2 = RuntimeProfile::Create(&pool, "Profile 2");
  RuntimeProfile::SummaryStatsCounter* summary_stats_counter_2 =
    profile2->AddSummaryStatsCounter("summary_stats", TUnit::UNIT);

  summary_stats_counter_2->UpdateCounter(100);
  EXPECT_EQ(summary_stats_counter_2->value(), 100);
  EXPECT_EQ(summary_stats_counter_2->MinValue(), 100);
  EXPECT_EQ(summary_stats_counter_2->MaxValue(), 100);

  TRuntimeProfileTree tprofile1;
  profile1->ToThrift(&tprofile1);

  // Merge profile1 and profile2 and check that profile2 is overwritten.
  profile2->Update(tprofile1);
  EXPECT_EQ(summary_stats_counter_2->value(), 4);
  EXPECT_EQ(summary_stats_counter_2->MinValue(), -40);
  EXPECT_EQ(summary_stats_counter_2->MaxValue(), 40);

}

// Helper for the AggregateSummaryStats that verifies the encoded event sequence
// in the thrift representation when it was merged into the profile at instance offset
// 'offset'.
static void VerifyThriftSummaryStats(
    const TRuntimeProfileNode& tnode, int offset, int total_instances) {
  const int NUM_VALID_INSTANCES = 3;
  DCHECK_LE(offset + NUM_VALID_INSTANCES, total_instances);
  ASSERT_TRUE(tnode.__isset.aggregated);

  const TAggregatedRuntimeProfileNode& agg_node = tnode.aggregated;
  ASSERT_TRUE(agg_node.__isset.summary_stats_counters);

  const TAggSummaryStatsCounter& tcounter = agg_node.summary_stats_counters[0];
  EXPECT_EQ("test ss", tcounter.name);
  EXPECT_EQ(TUnit::UNIT, tcounter.unit);

  EXPECT_LE(offset + NUM_VALID_INSTANCES, tcounter.has_value.size());
  EXPECT_LE(offset + NUM_VALID_INSTANCES, tcounter.sum.size());
  EXPECT_LE(offset + NUM_VALID_INSTANCES, tcounter.total_num_values.size());
  EXPECT_LE(offset + NUM_VALID_INSTANCES, tcounter.min_value.size());
  EXPECT_LE(offset + NUM_VALID_INSTANCES, tcounter.max_value.size());

  for (int i = 0; i < total_instances; ++i) {
    if (i < offset || i >= offset + NUM_VALID_INSTANCES) {
      EXPECT_FALSE(tcounter.has_value[i]);
      continue;
    }
    EXPECT_TRUE(tcounter.has_value[i]);
    int min_val = i - offset;
    EXPECT_EQ(min_val * 2 + 1, tcounter.sum[i]);
    EXPECT_EQ(2, tcounter.total_num_values[i]);
    EXPECT_EQ(min_val, tcounter.min_value[i]);
    EXPECT_EQ(min_val + 1, tcounter.max_value[i]);
  }
}

// Test handling of event sequences in the aggregated profile.
TEST(CountersTest, AggregateSummaryStats) {
  auto cert = ScopedFlagSetter<bool>::Make(&FLAGS_gen_experimental_profile, true);
  const int NUM_PROFILES = 3;
  // Create a profile with event sequences with some shared event keys.
  ObjectPool pool;
  RuntimeProfile* profiles[NUM_PROFILES];
  RuntimeProfile::SummaryStatsCounter* counters[NUM_PROFILES];
  for (int i = 0; i < NUM_PROFILES; ++i) {
    profiles[i] = RuntimeProfile::Create(&pool, "Profile");
    counters[i] = profiles[i]->AddSummaryStatsCounter("test ss", TUnit::UNIT);
    counters[i]->UpdateCounter(i);
    counters[i]->UpdateCounter(i + 1);
  }

  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "Merged", 3, true);
  for (int i = 0; i < NUM_PROFILES; ++i) {
    averaged_profile->UpdateAggregatedFromInstance(profiles[i], i);
  }

  TRuntimeProfileTree ttree;
  averaged_profile->ToThrift(&ttree);
  VerifyThriftSummaryStats(ttree.nodes[0], 0, NUM_PROFILES);

  // Test merging into another averaged profile at an offset
  const int NUM_UNINIT_PROFILES = 2;
  const int OFFSET = 1;
  AggregatedRuntimeProfile* averaged_profile2 = AggregatedRuntimeProfile::Create(
      &pool, "Merged 2", NUM_PROFILES + NUM_UNINIT_PROFILES, true);
  averaged_profile2->UpdateAggregatedFromInstances(ttree, OFFSET);
  TRuntimeProfileTree ttree2;
  averaged_profile2->ToThrift(&ttree2);
  VerifyThriftSummaryStats(ttree2.nodes[0], OFFSET, NUM_PROFILES + NUM_UNINIT_PROFILES);
}

TEST(CountersTest, DerivedCounters) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  RuntimeProfile::Counter* bytes_counter =
      profile->AddCounter("bytes", TUnit::BYTES);
  RuntimeProfile::Counter* ticks_counter =
      profile->AddCounter("ticks", TUnit::TIME_NS);
  // set to 1 sec
  ticks_counter->Set(1000L * 1000L * 1000L);

  RuntimeProfile::DerivedCounter* throughput_counter =
      profile->AddDerivedCounter("throughput", TUnit::BYTES,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_counter, ticks_counter));

  bytes_counter->Set(10);
  EXPECT_EQ(throughput_counter->value(), 10);
  bytes_counter->Set(20);
  EXPECT_EQ(throughput_counter->value(), 20);
  ticks_counter->Set(ticks_counter->value() / 2);
  EXPECT_EQ(throughput_counter->value(), 40);
}

TEST(CountersTest, AverageSetCounters) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  RuntimeProfile::Counter* bytes_1_counter =
      profile->AddCounter("bytes 1", TUnit::BYTES);
  RuntimeProfile::Counter* bytes_2_counter =
      profile->AddCounter("bytes 2", TUnit::BYTES);

  bytes_1_counter->Set(10);
  RuntimeProfile::AveragedCounter bytes_avg(TUnit::BYTES, 2);
  bytes_avg.UpdateCounter(bytes_1_counter, 0);
  // Avg of 10L
  EXPECT_EQ(bytes_avg.value(), 10);
  bytes_1_counter->Set(20L);
  bytes_avg.UpdateCounter(bytes_1_counter, 0);
  // Avg of 20L
  EXPECT_EQ(bytes_avg.value(), 20);
  bytes_2_counter->Set(40L);
  bytes_avg.UpdateCounter(bytes_2_counter, 1);
  // Avg of 20L and 40L
  EXPECT_EQ(bytes_avg.value(), 30);
  bytes_2_counter->Set(30L);
  bytes_avg.UpdateCounter(bytes_2_counter, 1);
  // Avg of 20L and 30L
  EXPECT_EQ(bytes_avg.value(), 25);

  RuntimeProfile::Counter* double_1_counter =
      profile->AddCounter("double 1", TUnit::DOUBLE_VALUE);
  RuntimeProfile::Counter* double_2_counter =
      profile->AddCounter("double 2", TUnit::DOUBLE_VALUE);
  double_1_counter->Set(1.0f);
  RuntimeProfile::AveragedCounter double_avg(TUnit::DOUBLE_VALUE, 2);
  double_avg.UpdateCounter(double_1_counter, 0);
  // Avg of 1.0f
  EXPECT_EQ(double_avg.double_value(), 1.0f);
  double_1_counter->Set(2.0f);
  double_avg.UpdateCounter(double_1_counter, 0);
  // Avg of 2.0f
  EXPECT_EQ(double_avg.double_value(), 2.0f);
  double_2_counter->Set(4.0f);
  double_avg.UpdateCounter(double_2_counter, 1);
  // Avg of 2.0f and 4.0f
  EXPECT_EQ(double_avg.double_value(), 3.0f);
  double_2_counter->Set(3.0f);
  double_avg.UpdateCounter(double_2_counter, 1);
  // Avg of 2.0f and 3.0f
  EXPECT_EQ(double_avg.double_value(), 2.5f);
}

TEST(CountersTest, AveragedCounterStats) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  // Average 100 input counters with values 100-199.
  const int NUM_COUNTERS = 100;
  vector<RuntimeProfile::Counter*> counters;
  for (int i = 0; i < NUM_COUNTERS; ++i) {
    counters.push_back(
        profile->AddCounter(Substitute("c$0", i), TUnit::BYTES));
    counters.back()->Set(100 + i);
  }
  // Randomize counter order - computed stats shouldn't depend on order.
  mt19937 rng;
  RandTestUtil::SeedRng("RUNTIME_PROFILE_TEST_SEED", &rng);
  shuffle(counters.begin(), counters.end(), rng);

  RuntimeProfile::AveragedCounter bytes_avg(TUnit::BYTES, NUM_COUNTERS);
  for (int i = 0; i < NUM_COUNTERS; ++i) {
    bytes_avg.UpdateCounter(counters[i], i);
  }
  RuntimeProfile::AveragedCounter::Stats<int64_t> stats = bytes_avg.GetStats<int64_t>();
  EXPECT_EQ(NUM_COUNTERS, stats.num_vals);
  EXPECT_EQ(100, stats.min);
  EXPECT_EQ(199, stats.max);
  EXPECT_EQ(149, stats.mean);
  EXPECT_EQ(149, stats.p50);
  EXPECT_EQ(174, stats.p75);
  EXPECT_EQ(189, stats.p90);
  EXPECT_EQ(194, stats.p95);

  // Round-trip via thrift and confirm values are all the same.
  TAggCounter tcounter;
  bytes_avg.ToThrift("", &tcounter);
  RuntimeProfile::AveragedCounter bytes_avg2(
      TUnit::BYTES, tcounter.has_value, tcounter.values);
  stats = bytes_avg2.GetStats<int64_t>();
  EXPECT_EQ(NUM_COUNTERS, stats.num_vals);
  EXPECT_EQ(100, stats.min);
  EXPECT_EQ(199, stats.max);
  EXPECT_EQ(149, stats.mean);
  EXPECT_EQ(149, stats.p50);
  EXPECT_EQ(174, stats.p75);
  EXPECT_EQ(189, stats.p90);
  EXPECT_EQ(194, stats.p95);
}

TEST(CountersTest, InfoStringTest) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  EXPECT_TRUE(profile->GetInfoString("Key") == NULL);

  profile->AddInfoString("Key", "Value");
  const string* value = profile->GetInfoString("Key");
  EXPECT_TRUE(value != NULL);
  EXPECT_EQ(*value, "Value");

  // Convert it to thrift
  TRuntimeProfileTree tprofile;
  profile->ToThrift(&tprofile);

  // Convert it back
  RuntimeProfile* from_thrift = RuntimeProfile::CreateFromThrift(
      &pool, tprofile);
  value = from_thrift->GetInfoString("Key");
  EXPECT_TRUE(value != NULL);
  EXPECT_EQ(*value, "Value");

  // Test update.
  RuntimeProfile* update_dst_profile = RuntimeProfile::Create(&pool, "Profile2");
  update_dst_profile->Update(tprofile);
  value = update_dst_profile->GetInfoString("Key");
  EXPECT_TRUE(value != NULL);
  EXPECT_EQ(*value, "Value");

  // Update the original profile, convert it to thrift and update from the dst
  // profile
  profile->AddInfoString("Key", "NewValue");
  profile->AddInfoString("Foo", "Bar");
  EXPECT_EQ(*profile->GetInfoString("Key"), "NewValue");
  EXPECT_EQ(*profile->GetInfoString("Foo"), "Bar");
  profile->ToThrift(&tprofile);

  update_dst_profile->Update(tprofile);
  EXPECT_EQ(*update_dst_profile->GetInfoString("Key"), "NewValue");
  EXPECT_EQ(*update_dst_profile->GetInfoString("Foo"), "Bar");
}

// Helper for the AggregateInfoStrings that verifies the encoded event sequence
// in the thrift representation when it was merged into the profile at instance offset
// 'offset'.
static void VerifyThriftInfoStrings(
    const TRuntimeProfileNode& tnode, int offset, int total_instances) {
  ASSERT_TRUE(tnode.__isset.aggregated);
  const int NUM_VALID_INSTANCES = 3;
  DCHECK_LE(offset + NUM_VALID_INSTANCES, total_instances);

  const TAggregatedRuntimeProfileNode& agg_node = tnode.aggregated;
  ASSERT_TRUE(agg_node.__isset.info_strings);
  const map<string, map<string, vector<int32_t>>>& info_strings = agg_node.info_strings;
  auto it = info_strings.find("shared");
  EXPECT_TRUE(it != info_strings.end());
  EXPECT_EQ(1, it->second.size()) << "Only one distinct value for shared";

  // Same value should be present in all instances, i.e. all indices should be present.
  auto it2 = it->second.find("same value");
  EXPECT_TRUE(it2 != it->second.end());
  EXPECT_EQ(it2->second, vector<int32_t>({offset + 0, offset + 1, offset + 2}));

  // Distinct value should have different value per instance.
  it = info_strings.find("distinct");
  EXPECT_TRUE(it != info_strings.end());
  EXPECT_EQ(NUM_VALID_INSTANCES, it->second.size()) << "One distinct value per instance";
  it2 = it->second.find("val0");
  EXPECT_TRUE(it2 != it->second.end());
  EXPECT_EQ(it2->second, vector<int32_t>({offset + 0}));
  it2 = it->second.find("val1");
  EXPECT_TRUE(it2 != it->second.end());
  EXPECT_EQ(it2->second, vector<int32_t>({offset + 1}));
  it2 = it->second.find("val2");
  EXPECT_TRUE(it2 != it->second.end());
  EXPECT_EQ(it2->second, vector<int32_t>({offset + 2}));
}

// Test handling of event sequences in the aggregated profile.
TEST(CountersTest, AggregateInfoStrings) {
  auto cert = ScopedFlagSetter<bool>::Make(&FLAGS_gen_experimental_profile, true);
  const int NUM_PROFILES = 3;
  // Create a profile with info strings that are shared across instances and then
  // distinct across instances to test that they are deduplicated appropriately.
  ObjectPool pool;
  RuntimeProfile* profiles[NUM_PROFILES];
  for (int i = 0; i < NUM_PROFILES; ++i) {
    profiles[i] = RuntimeProfile::Create(&pool, "Profile");
    profiles[i]->AddInfoString("shared", "same value");
    profiles[i]->AddInfoString("distinct", Substitute("val$0", i));
  }

  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "Merged", 3, true);
  for (int i = 0; i < NUM_PROFILES; ++i) {
    averaged_profile->UpdateAggregatedFromInstance(profiles[i], i);
  }

  TRuntimeProfileTree ttree;
  averaged_profile->ToThrift(&ttree);
  VerifyThriftInfoStrings(ttree.nodes[0], 0, NUM_PROFILES);

  // Test merging into another averaged profile at an offset
  const int NUM_UNINIT_PROFILES = 2;
  const int OFFSET = 1;
  AggregatedRuntimeProfile* averaged_profile2 = AggregatedRuntimeProfile::Create(
      &pool, "Merged 2", NUM_PROFILES + NUM_UNINIT_PROFILES, true);
  averaged_profile2->UpdateAggregatedFromInstances(ttree, OFFSET);
  TRuntimeProfileTree ttree2;
  averaged_profile2->ToThrift(&ttree2);
  VerifyThriftInfoStrings(ttree2.nodes[0], OFFSET, NUM_PROFILES + NUM_UNINIT_PROFILES);
}

TEST(CountersTest, RateCounters) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");

  RuntimeProfile::Counter* bytes_counter =
      profile->AddCounter("bytes", TUnit::BYTES);

  RuntimeProfile::Counter* rate_counter =
      profile->AddRateCounter("RateCounter", bytes_counter);
  EXPECT_TRUE(rate_counter->unit() == TUnit::BYTES_PER_SECOND);

  EXPECT_EQ(rate_counter->value(), 0);
  // set to 100MB.  Use bigger units to avoid truncating to 0 after divides.
  bytes_counter->Set(100L * 1024L * 1024L);

  // Wait one second.
  sleep(1);

  int64_t rate = rate_counter->value();

  // Stop the counter so it no longer gets updates
  profile->StopPeriodicCounters();

  // The rate counter is not perfectly accurate.  Currently updated at 500ms intervals,
  // we should have seen somewhere between 1 and 3 updates (33 - 200 MB/s)
  EXPECT_GT(rate, 66 * 1024 * 1024);
  EXPECT_LE(rate, 200 * 1024 * 1024);

  // Wait another second.  The counter has been removed. So the value should not be
  // changed (much).
  sleep(2);

  rate = rate_counter->value();
  EXPECT_GT(rate, 66 * 1024 * 1024);
  EXPECT_LE(rate, 200 * 1024 * 1024);
}

TEST(CountersTest, BucketCounters) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");

  RuntimeProfile::Counter* unit_counter =
      profile->AddCounter("unit", TUnit::UNIT);

  // Set the unit to 1 before sampling
  unit_counter->Set(1);

  // Create the bucket counters and start sampling
  vector<RuntimeProfile::Counter*>* buckets =
      profile->AddBucketingCounters(unit_counter, 2);

  // Wait two seconds.
  sleep(2);

  // Stop sampling
  profile->StopPeriodicCounters();

  // TODO: change the value to double
  // The value of buckets[0] should be zero and buckets[1] should be 1.
  double val0 = (*buckets)[0]->double_value();
  double val1 = (*buckets)[1]->double_value();
  EXPECT_EQ(0, val0);
  EXPECT_EQ(100, val1);

  // Wait another second.  The counter has been removed. So the value should not be
  // changed (much).
  sleep(2);
  EXPECT_EQ(val0, (*buckets)[0]->double_value());
  EXPECT_EQ(val1, (*buckets)[1]->double_value());
}

TEST(CountersTest, EventSequences) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  RuntimeProfile::EventSequence* seq = profile->AddEventSequence("event sequence");
  seq->MarkEvent("aaaa");
  seq->MarkEvent("bbbb");
  seq->MarkEvent("cccc");

  vector<RuntimeProfile::EventSequence::Event> events;
  seq->GetEvents(&events);
  EXPECT_EQ(3, events.size());

  uint64_t last_timestamp = 0;
  string last_string = "";
  for (const RuntimeProfile::EventSequence::Event& ev: events) {
    EXPECT_TRUE(ev.second >= last_timestamp);
    last_timestamp = ev.second;
    EXPECT_TRUE(ev.first > last_string);
    last_string = ev.first;
  }

  TRuntimeProfileTree thrift_profile;
  profile->ToThrift(&thrift_profile);
  EXPECT_TRUE(thrift_profile.nodes[0].__isset.event_sequences);
  EXPECT_EQ(1, thrift_profile.nodes[0].event_sequences.size());

  RuntimeProfile* reconstructed_profile =
      RuntimeProfile::CreateFromThrift(&pool, thrift_profile);

  last_timestamp = 0;
  last_string = "";
  EXPECT_EQ(NULL, reconstructed_profile->GetEventSequence("doesn't exist"));
  seq = reconstructed_profile->GetEventSequence("event sequence");
  EXPECT_TRUE(seq != NULL);
  seq->GetEvents(&events);
  EXPECT_EQ(3, events.size());
  for (const RuntimeProfile::EventSequence::Event& ev: events) {
    EXPECT_TRUE(ev.second >= last_timestamp);
    last_timestamp = ev.second;
    EXPECT_TRUE(ev.first > last_string);
    last_string = ev.first;
  }
}

static void CheckAscending(const vector<int64_t>& v) {
  if (v.empty()) return;
  int64_t prev = v[0];
  for (int i = 1; i < v.size(); ++i) {
    EXPECT_LE(prev, v[i]);
    prev = v[i];
  }
}

// Helper for the AggregateEventSequences that verifies the encoded event sequence
// in the thrift representation when it was merged into the profile at instance offset
// 'offset'.
static void VerifyThriftEventSequences(
    const TRuntimeProfileNode& tnode, int offset, int total_instances) {
  ASSERT_TRUE(tnode.__isset.aggregated);
  const int NUM_VALID_INSTANCES = 3;
  DCHECK_LE(offset + NUM_VALID_INSTANCES, total_instances);

  const TAggregatedRuntimeProfileNode& agg_node = tnode.aggregated;
  ASSERT_TRUE(agg_node.__isset.event_sequences);
  ASSERT_EQ(1, agg_node.event_sequences.size());
  // Check that the dictionary encoding worked.
  const TAggEventSequence& tseq = agg_node.event_sequences[0];
  EXPECT_EQ("event sequence", tseq.name);
  EXPECT_EQ("aaaa", tseq.label_dict[0]);
  EXPECT_EQ("bbbb", tseq.label_dict[1]);
  EXPECT_EQ("cccc", tseq.label_dict[2]);
  EXPECT_EQ("dddd", tseq.label_dict[3]);

  // Validate that the right number of instances are present and that the invalid
  // instances do not have any data associated with them.
  EXPECT_EQ(total_instances, tseq.label_idxs.size());
  EXPECT_EQ(total_instances, tseq.timestamps.size());
  for (int i = 0; i < total_instances; ++i) {
    if (i < offset || i >= offset + NUM_VALID_INSTANCES) {
      EXPECT_EQ(0, tseq.label_idxs[i].size());
      EXPECT_EQ(0, tseq.timestamps[i].size());
    }
  }

  // Validate the label/timestamp values for the valid instances.
  EXPECT_EQ(3, tseq.label_idxs[offset + 0].size());
  EXPECT_EQ(0, tseq.label_idxs[offset + 0][0]);
  EXPECT_EQ(1, tseq.label_idxs[offset + 0][1]);
  EXPECT_EQ(2, tseq.label_idxs[offset + 0][2]);
  EXPECT_EQ(3, tseq.timestamps[offset + 0].size());
  CheckAscending(tseq.timestamps[offset + 0]);

  EXPECT_EQ(2, tseq.label_idxs[offset + 1].size());
  EXPECT_EQ(0, tseq.label_idxs[offset + 1][0]);
  EXPECT_EQ(2, tseq.label_idxs[offset + 1][1]);
  EXPECT_EQ(2, tseq.timestamps[offset + 1].size());
  CheckAscending(tseq.timestamps[offset + 1]);

  EXPECT_EQ(3, tseq.label_idxs[offset + 2].size());
  EXPECT_EQ(0, tseq.label_idxs[offset + 2][0]);
  EXPECT_EQ(3, tseq.label_idxs[offset + 2][1]);
  EXPECT_EQ(1, tseq.label_idxs[offset + 2][2]);
  EXPECT_EQ(3, tseq.timestamps[offset + 2].size());
  CheckAscending(tseq.timestamps[offset + 2]);
}

// Test handling of event sequences in the aggregated profile.
TEST(CountersTest, AggregateEventSequences) {
  auto cert = ScopedFlagSetter<bool>::Make(&FLAGS_gen_experimental_profile, true);
  const int NUM_PROFILES = 3;
  // Create a profile with event sequences with some shared event keys.
  ObjectPool pool;
  RuntimeProfile* profiles[NUM_PROFILES];
  RuntimeProfile::EventSequence* seqs[NUM_PROFILES];
  for (int i = 0; i < NUM_PROFILES; ++i) {
    profiles[i] = RuntimeProfile::Create(&pool, "Profile");
    seqs[i] = profiles[i]->AddEventSequence("event sequence");
    seqs[i]->MarkEvent("aaaa");
  }
  seqs[0]->MarkEvent("bbbb");
  seqs[0]->MarkEvent("cccc");
  seqs[1]->MarkEvent("cccc");
  seqs[2]->MarkEvent("dddd");
  seqs[2]->MarkEvent("bbbb");

  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "Merged", 3, true);
  for (int i = 0; i < NUM_PROFILES; ++i) {
    averaged_profile->UpdateAggregatedFromInstance(profiles[i], i);
  }

  TRuntimeProfileTree ttree;
  averaged_profile->ToThrift(&ttree);
  VerifyThriftEventSequences(ttree.nodes[0], 0, NUM_PROFILES);

  // Test merging into another averaged profile at an offset
  const int NUM_UNINIT_PROFILES = 2;
  const int OFFSET = 1;
  AggregatedRuntimeProfile* averaged_profile2 = AggregatedRuntimeProfile::Create(
      &pool, "Merged 2", NUM_PROFILES + NUM_UNINIT_PROFILES, true);
  averaged_profile2->UpdateAggregatedFromInstances(ttree, OFFSET);
  TRuntimeProfileTree ttree2;
  averaged_profile2->ToThrift(&ttree2);
  VerifyThriftEventSequences(ttree2.nodes[0], OFFSET, NUM_PROFILES + NUM_UNINIT_PROFILES);
}

TEST(CountersTest, UpdateEmptyEventSequence) {
  // IMPALA-6824: This test makes sure that adding events to an empty event sequence does
  // not crash.
  ObjectPool pool;

  // Create the profile to send in the update and add some events.
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  RuntimeProfile::EventSequence* seq = profile->AddEventSequence("event sequence");
  seq->Start();
  // Sleep for 10ms to make sure the events are logged at a time > 0.
  SleepForMs(10);
  seq->MarkEvent("aaaa");
  seq->MarkEvent("bbbb");

  vector<RuntimeProfile::EventSequence::Event> events;
  seq->GetEvents(&events);
  EXPECT_EQ(2, events.size());

  TRuntimeProfileTree thrift_profile;
  profile->ToThrift(&thrift_profile);

  // Create the profile that will be updated and add the empty event sequence to it.
  RuntimeProfile* updated_profile = RuntimeProfile::Create(&pool, "Updated Profile");
  seq = updated_profile->AddEventSequence("event sequence");
  updated_profile->Update(thrift_profile);

  // Verify that the events have been updated successfully.
  events.clear();
  seq->GetEvents(&events);
  EXPECT_EQ(2, events.size());
}

void ValidateSampler(const StreamingSampler<int, 10>& sampler, int expected_num,
    int expected_period, int expected_delta) {
  const int* samples = NULL;
  int num_samples;
  int period;

  samples = sampler.GetSamples(&num_samples, &period);
  EXPECT_TRUE(samples != NULL);
  EXPECT_EQ(num_samples, expected_num);
  EXPECT_EQ(period, expected_period);

  for (int i = 0; i < expected_num - 1; ++i) {
    EXPECT_EQ(samples[i] + expected_delta, samples[i + 1]) << i;
  }
}

TEST(CountersTest, StreamingSampler) {
  StreamingSampler<int, 10> sampler(500);

  int idx = 0;
  for (int i = 0; i < 3; ++i) {
    sampler.AddSample(idx++, 500);
  }
  ValidateSampler(sampler, 3, 500, 1);

  for (int i = 0; i < 3; ++i) {
    sampler.AddSample(idx++, 500);
  }
  ValidateSampler(sampler, 6, 500, 1);

  for (int i = 0; i < 3; ++i) {
    sampler.AddSample(idx++, 500);
  }
  ValidateSampler(sampler, 9, 500, 1);

  // Added enough to cause a collapse
  for (int i = 0; i < 3; ++i) {
    sampler.AddSample(idx++, 500);
  }
  // Added enough to cause a collapse
  ValidateSampler(sampler, 6, 1000, 2);

  for (int i = 0; i < 3; ++i) {
    sampler.AddSample(idx++, 500);
  }
  ValidateSampler(sampler, 7, 1000, 2);
}

// Test class to test ConcurrentStopWatch and RuntimeProfile::ConcurrentTimerCounter
// don't double count in multithread environment.
class TimerCounterTest {
 public:
  TimerCounterTest()
    : timercounter_(TUnit::TIME_NS) {}

  struct DummyWorker {
    thread* thread_handle;
    AtomicBool done;

    DummyWorker()
      : thread_handle(NULL), done(false) {}

    ~DummyWorker() {
      Stop();
    }

    DummyWorker(const DummyWorker& dummy_worker)
      : thread_handle(dummy_worker.thread_handle), done(dummy_worker.done.Load()) {}

    void Stop() {
      if (!done.Load() && thread_handle != NULL) {
        done.Store(true);
        thread_handle->join();
        delete thread_handle;
        thread_handle = NULL;
      }
    }
  };

  void Run(DummyWorker* worker) {
    SCOPED_CONCURRENT_STOP_WATCH(&csw_);
    SCOPED_CONCURRENT_COUNTER(&timercounter_);
    while (!worker->done.Load()) {
      SleepForMs(10);
      // Each test case should be no more than one second.
      // Consider test failed if timer is more than 3 seconds.
      if (csw_.TotalRunningTime() > 6000000000) {
        FAIL();
      }
    }
  }

  // Start certain number of worker threads. If interval is set, it will add some delay
  // between creating worker thread.
  void StartWorkers(int num, int interval) {
    workers_.reserve(num);
    for (int i = 0; i < num; ++i) {
      workers_.push_back(DummyWorker());
      DummyWorker& worker = workers_.back();
      worker.thread_handle = new thread(&TimerCounterTest::Run, this, &worker);
      SleepForMs(interval);
    }
  }

  // Stop specified thread by index. if index is -1, stop all threads
  void StopWorkers(int thread_index = -1) {
    if (thread_index >= 0) {
      workers_[thread_index].Stop();
    } else {
      for (int i = 0; i < workers_.size(); ++i) {
        workers_[i].Stop();
      }
    }
  }

  void Reset() {
    workers_.clear();
  }

  // Allow some timer inaccuracy (30ms) since thread join could take some time.
  static const int MAX_TIMER_ERROR_NS = 30000000;
  vector<DummyWorker> workers_;
  ConcurrentStopWatch csw_;
  RuntimeProfile::ConcurrentTimerCounter timercounter_;
};

void ValidateTimerValue(const TimerCounterTest& timer, int64_t start) {
  int64_t expected_value = MonotonicStopWatch::Now() - start;
  int64_t stopwatch_value = timer.csw_.TotalRunningTime();
  EXPECT_GE(stopwatch_value, expected_value - TimerCounterTest::MAX_TIMER_ERROR_NS);
  EXPECT_LE(stopwatch_value, expected_value + TimerCounterTest::MAX_TIMER_ERROR_NS);

  int64_t timer_value = timer.timercounter_.value();
  EXPECT_GE(timer_value, expected_value - TimerCounterTest::MAX_TIMER_ERROR_NS);
  EXPECT_LE(timer_value, expected_value + TimerCounterTest::MAX_TIMER_ERROR_NS);
}

void ValidateLapTime(TimerCounterTest* timer, int64_t expected_value) {
  int64_t stopwatch_value = timer->csw_.LapTime();
  EXPECT_GE(stopwatch_value, expected_value - TimerCounterTest::MAX_TIMER_ERROR_NS);
  EXPECT_LE(stopwatch_value, expected_value + TimerCounterTest::MAX_TIMER_ERROR_NS);

  int64_t timer_value = timer->timercounter_.LapTime();
  EXPECT_GE(timer_value, expected_value - TimerCounterTest::MAX_TIMER_ERROR_NS);
  EXPECT_LE(timer_value, expected_value + TimerCounterTest::MAX_TIMER_ERROR_NS);
}

TEST(TimerCounterTest, CountersTestOneThread) {
  TimerCounterTest tester;
  int64_t start = MonotonicStopWatch::Now();
  tester.StartWorkers(1, 0);
  SleepForMs(500);
  ValidateTimerValue(tester, start);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
}

TEST(TimerCounterTest, CountersTestTwoThreads) {
  TimerCounterTest tester;
  int64_t start = MonotonicStopWatch::Now();
  tester.StartWorkers(2, 10);
  SleepForMs(500);
  ValidateTimerValue(tester, start);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
}

TEST(TimerCounterTest, CountersTestRandom) {
  TimerCounterTest tester;
  int64_t start = MonotonicStopWatch::Now();
  ValidateTimerValue(tester, start);
  // First working period
  tester.StartWorkers(5, 10);
  ValidateTimerValue(tester, start);
  SleepForMs(400);
  tester.StopWorkers(2);
  ValidateTimerValue(tester, start);
  SleepForMs(100);
  tester.StopWorkers(4);
  ValidateTimerValue(tester, start);
  SleepForMs(600);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
  tester.Reset();

  ValidateLapTime(&tester, MonotonicStopWatch::Now() - start);
  int64_t first_run_end = MonotonicStopWatch::Now();
  // Adding some idle time. concurrent stopwatch and timer should not count the idle time.
  SleepForMs(200);
  start += MonotonicStopWatch::Now() - first_run_end;

  // Second working period
  tester.StartWorkers(2, 0);
  // We just get lap time after first run finish. so at start of second run, expect lap time == 0
  ValidateLapTime(&tester, 0);
  int64_t lap_time_start = MonotonicStopWatch::Now();
  SleepForMs(200);
  ValidateTimerValue(tester, start);
  SleepForMs(200);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
  ValidateLapTime(&tester, MonotonicStopWatch::Now() - lap_time_start);
}

// Don't run TestAddClearRace against TSAN builds as it is expected to have race
// conditions.
#ifndef THREAD_SANITIZER

TEST(TimeSeriesCounterTest, TestAddClearRace) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  int i = 0;
  // Return and increment i
  auto f = [&i]() { return i++; };
  RuntimeProfile::TimeSeriesCounter* counter =
      profile->AddChunkedTimeSeriesCounter("Counter", TUnit::UNIT, f);
  // Sleep 1 second for some values to accumulate.
  sleep(1);
  int num_samples, period;
  counter->GetSamplesTest(&num_samples, &period);
  EXPECT_GT(num_samples, 0);

  // Wait for more values to show up
  sleep(1);

  // Stop the counters. The rest of the test assumes that no new values will be added.
  profile->StopPeriodicCounters();

  // Clear the counter
  profile->ClearChunkedTimeSeriesCounters();

  // Check that clearing multiple times doesn't affect valued that have not been
  // retrieved.
  profile->ClearChunkedTimeSeriesCounters();

  // Make sure that it still has values in it.
  counter->GetSamplesTest(&num_samples, &period);
  EXPECT_GT(num_samples, 0);

  // Clear it again
  profile->ClearChunkedTimeSeriesCounters();

  // Make sure the values are gone.
  counter->GetSamplesTest(&num_samples, &period);
  EXPECT_EQ(num_samples, 0);
}

#endif

/// Stops the periodic counter updater in 'profile' and then clears the samples in
/// 'counter'.
void StopAndClearCounter(RuntimeProfile* profile,
    RuntimeProfile::TimeSeriesCounter* counter) {
  // There's a race between adding the counter and calling StopPeriodicCounters so we
  // sleep here to make sure we exercise the code that handles the race.
  sleep(1);
  profile->StopPeriodicCounters();

  // Reset the counter state by reading and clearing its samples.
  int num_samples = 0;
  int result_period_unused = 0;
  counter->GetSamplesTest(&num_samples, &result_period_unused);
  ASSERT_GT(num_samples, 0);
  profile->ClearChunkedTimeSeriesCounters();
  // Ensure clean state.
  counter->GetSamplesTest(&num_samples, &result_period_unused);
  ASSERT_EQ(num_samples, 0);
}

/// Tests that ChunkedTimeSeriesCounters are bounded by a maximum size.
TEST(TimeSeriesCounterTest, TestMaximumSize) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");

  const int test_period = FLAGS_periodic_counter_update_period_ms;

  // Add a counter with a sample function that counts up, starting from 0.
  int value = 0;
  auto sample_fn = [&value]() { return value++; };
  RuntimeProfile::TimeSeriesCounter* counter =
      profile->AddChunkedTimeSeriesCounter("TestCounter", TUnit::UNIT, sample_fn);

  // Stop counter updates from interfering with the rest of the test.
  StopAndClearCounter(profile, counter);

  // Reset value after previous values have been retrieved.
  value = 0;

  int64_t max_size = 10 * FLAGS_status_report_interval_ms / test_period;
  for (int i = 0; i < 10 + max_size; ++i) counter->AddSample(test_period);

  int num_samples = 0;
  int result_period = 0;
  // Retrieve and validate samples.
  const int64_t* samples = counter->GetSamplesTest(&num_samples, &result_period);
  ASSERT_EQ(num_samples, max_size);
  // No resampling happens with ChunkedTimeSeriesCounter.
  ASSERT_EQ(result_period, test_period);

  // First 10 samples have been truncated
  ASSERT_EQ(samples[0], 10);
}

// Helper for the AggregateTimeSeries that verifies the encoded event sequence
// in the thrift representation when it was merged into the profile at instance offset
// 'offset'.
static void VerifyThriftTimeSeries(
    const TRuntimeProfileNode& tnode, int offset, int total_instances) {
  ASSERT_TRUE(tnode.__isset.aggregated);
  const int NUM_VALID_INSTANCES = 3;
  DCHECK_LE(offset + NUM_VALID_INSTANCES, total_instances);

  const TAggregatedRuntimeProfileNode& agg_node = tnode.aggregated;
  ASSERT_TRUE(agg_node.__isset.time_series_counters);

  const TAggTimeSeriesCounter& tcounter = agg_node.time_series_counters[0];
  EXPECT_EQ("TestCounter", tcounter.name);
  const int test_period = FLAGS_periodic_counter_update_period_ms;
  for (int i = 0; i < total_instances; ++i) {
    if (i < offset || i >= offset + NUM_VALID_INSTANCES) {
      EXPECT_EQ(0, tcounter.period_ms[i]);
      EXPECT_EQ(0, tcounter.values[i].size());
      EXPECT_EQ(0, tcounter.start_index[i]);
      continue;
    }
    EXPECT_EQ(test_period, tcounter.period_ms[i]);
    EXPECT_GE(tcounter.start_index[i], 0);
    EXPECT_EQ(vector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), tcounter.values[i]);
  }
}

// Test handling of event sequences in the aggregated profile.
TEST(TimeSeriesCounterTest, AggregateTimeSeries) {
  auto cert = ScopedFlagSetter<bool>::Make(&FLAGS_gen_experimental_profile, true);
  const int NUM_PROFILES = 3;
  // Create a profile with event sequences with some shared event keys.
  ObjectPool pool;
  RuntimeProfile* profiles[NUM_PROFILES];
  RuntimeProfile::TimeSeriesCounter* counters[NUM_PROFILES];
  const int test_period = FLAGS_periodic_counter_update_period_ms;
  for (int i = 0; i < NUM_PROFILES; ++i) {
    profiles[i] = RuntimeProfile::Create(&pool, "Profile");
    // Add a counter with a sample function that counts up, starting from 0.
    int value = 0;
    auto sample_fn = [&value]() { return value++; };
    counters[i] =
        profiles[i]->AddChunkedTimeSeriesCounter("TestCounter", TUnit::UNIT, sample_fn);

    // Stop counter updates from interfering with the rest of the test.
    StopAndClearCounter(profiles[i], counters[i]);

    // Reset value after previous values have been retrieved.
    value = 0;

    for (int j = 0; j < 10; ++j) counters[i]->AddSample(test_period);
  }

  AggregatedRuntimeProfile* averaged_profile =
      AggregatedRuntimeProfile::Create(&pool, "Merged", 3, true);
  for (int i = 0; i < NUM_PROFILES; ++i) {
    averaged_profile->UpdateAggregatedFromInstance(profiles[i], i);
  }
  TRuntimeProfileTree ttree;
  averaged_profile->ToThrift(&ttree);
  VerifyThriftTimeSeries(ttree.nodes[0], 0, NUM_PROFILES);

  // Test merging into another averaged profile at an offset
  const int NUM_UNINIT_PROFILES = 2;
  const int OFFSET = 1;
  AggregatedRuntimeProfile* averaged_profile2 = AggregatedRuntimeProfile::Create(
      &pool, "Merged 2", NUM_PROFILES + NUM_UNINIT_PROFILES, true);
  averaged_profile2->UpdateAggregatedFromInstances(ttree, OFFSET);
  TRuntimeProfileTree ttree2;
  averaged_profile2->ToThrift(&ttree2);
  VerifyThriftTimeSeries(ttree2.nodes[0], OFFSET, NUM_PROFILES + NUM_UNINIT_PROFILES);
}

// Helper for the TAggCounter that verifies the encoded counter in the thrift
// representation when it was merged into the profile at instance offset 'offset'.
static void VerifyThriftCounters(
    const TRuntimeProfileNode& tnode, int offset, int total_instances) {
  ASSERT_TRUE(tnode.__isset.aggregated);
  const int NUM_VALID_INSTANCES = 3;
  DCHECK_LE(offset + NUM_VALID_INSTANCES, total_instances);

  const TAggregatedRuntimeProfileNode& agg_node = tnode.aggregated;
  ASSERT_TRUE(agg_node.__isset.time_series_counters);

  const TAggCounter& tcounter = agg_node.counters[2];
  EXPECT_EQ("simple_counter", tcounter.name);
  EXPECT_EQ(TUnit::BYTES, tcounter.unit);
  for (int i = 0; i < total_instances; ++i) {
    if (i < offset || i >= offset + NUM_VALID_INSTANCES) {
      EXPECT_EQ(false, tcounter.has_value[i]);
      EXPECT_EQ(0, tcounter.values[i]);
      continue;
    }
    EXPECT_EQ(true, tcounter.has_value[i]);
    EXPECT_EQ((i - offset + 1) * 11, tcounter.values[i]);
  }
}

// Test handling aggregation of two profile update, where the second profile update is a
// partial update.
TEST(CountersTest, PartialUpdate) {
  auto cert = ScopedFlagSetter<bool>::Make(&FLAGS_gen_experimental_profile, true);
  const int NUM_PROFILES = 3;
  // Create a profile with event sequences with some shared event keys.
  ObjectPool pool;
  RuntimeProfile* profiles[NUM_PROFILES];

  // Create Profiles and Counters.
  RuntimeProfile::Counter* counters[NUM_PROFILES];
  for (int i = 0; i < NUM_PROFILES; ++i) {
    profiles[i] = RuntimeProfile::Create(&pool, strings::Substitute("Profile $0", i));
    counters[i] = profiles[i]->AddCounter("simple_counter", TUnit::BYTES);
    counters[i]->Set((i + 1) * (i > 0 ? 11 : 5));
  }

  // Create SummaryStatsCounters.
  RuntimeProfile::SummaryStatsCounter* ss_counters[NUM_PROFILES];
  for (int i = 0; i < NUM_PROFILES; ++i) {
    ss_counters[i] = profiles[i]->AddSummaryStatsCounter("test ss", TUnit::UNIT);
    ss_counters[i]->UpdateCounter(i);
    if (i > 0) ss_counters[i]->UpdateCounter(i + 1);
  }

  // Create InfoStrings.
  for (int i = 0; i < NUM_PROFILES; ++i) {
    profiles[i]->AddInfoString("shared", "same value");
    if (i > 0) profiles[i]->AddInfoString("distinct", Substitute("val$0", i));
  }

  // Create EventSequences.
  RuntimeProfile::EventSequence* seqs[NUM_PROFILES];
  for (int i = 0; i < NUM_PROFILES; ++i) {
    seqs[i] = profiles[i]->AddEventSequence("event sequence");
    seqs[i]->MarkEvent("aaaa");
  }
  seqs[0]->MarkEvent("bbbb");
  seqs[1]->MarkEvent("cccc");
  seqs[2]->MarkEvent("dddd");
  seqs[2]->MarkEvent("bbbb");

  // Create TimeSeriesCounters.
  RuntimeProfile::TimeSeriesCounter* ts_counters[NUM_PROFILES];
  const int test_period = FLAGS_periodic_counter_update_period_ms;
  // Add a counter with a sample function that counts up, starting from 0.
  int ts_value = 0;
  for (int i = NUM_PROFILES - 1; i >= 0; --i) {
    auto sample_fn = [&ts_value]() { return ts_value++; };
    ts_counters[i] =
        profiles[i]->AddChunkedTimeSeriesCounter("TestCounter", TUnit::UNIT, sample_fn);

    // Stop counter updates from interfering with the rest of the test.
    StopAndClearCounter(profiles[i], ts_counters[i]);

    // Reset value after previous values have been retrieved.
    ts_value = 0;

    for (int j = 0; j < (i == 0 ? 9 : 10); ++j) ts_counters[i]->AddSample(test_period);
  }

  // Update 1 has instance #1 and #2 reporting final update, while instance #0 reporting
  // its first update.
  AggregatedRuntimeProfile* aggregated_profile_1 =
      AggregatedRuntimeProfile::Create(&pool, "Update 1", NUM_PROFILES, true);
  for (int i = 0; i < NUM_PROFILES; ++i) {
    aggregated_profile_1->UpdateAggregatedFromInstance(profiles[i], i);
  }
  TRuntimeProfileTree ttree1;
  aggregated_profile_1->ToThrift(&ttree1);

  // Update 2 has only instance #0 reporting its final update, which has 1 more sample in
  // its counter, event sequence, and time series counter.
  counters[0]->Set(11);
  ss_counters[0]->UpdateCounter(1);
  profiles[0]->AddInfoString("distinct", "val0");
  seqs[0]->MarkEvent("cccc");
  ts_counters[0]->AddSample(test_period);
  AggregatedRuntimeProfile* aggregated_profile_2 =
      AggregatedRuntimeProfile::Create(&pool, "Update 2", NUM_PROFILES, true);
  aggregated_profile_2->UpdateAggregatedFromInstance(profiles[0], 0);
  TRuntimeProfileTree ttree2;
  aggregated_profile_2->ToThrift(&ttree2);

  // Test merging both update into larger aggregated profile (size of 9) at an offset 3.
  const int MERGE_SIZE = NUM_PROFILES * 3;
  const int OFFSET = NUM_PROFILES;
  AggregatedRuntimeProfile* merged_profile =
      AggregatedRuntimeProfile::Create(&pool, "Merged", MERGE_SIZE, true);
  merged_profile->UpdateAggregatedFromInstances(ttree1, OFFSET);
  merged_profile->UpdateAggregatedFromInstances(ttree2, OFFSET);
  TRuntimeProfileTree ttree_merged;
  merged_profile->ToThrift(&ttree_merged);

  // Verify merged SummaryStats, InfoStrings, EventSequences, and TimeSeries.
  VerifyThriftCounters(ttree_merged.nodes[0], OFFSET, MERGE_SIZE);
  VerifyThriftSummaryStats(ttree_merged.nodes[0], OFFSET, MERGE_SIZE);
  VerifyThriftInfoStrings(ttree_merged.nodes[0], OFFSET, MERGE_SIZE);
  VerifyThriftEventSequences(ttree_merged.nodes[0], OFFSET, MERGE_SIZE);
  VerifyThriftTimeSeries(ttree_merged.nodes[0], OFFSET, MERGE_SIZE);

  // Verify that all profile name match.
  ASSERT_EQ(ttree_merged.nodes[0].aggregated.num_instances, MERGE_SIZE);
  for (int i = 0; i < NUM_PROFILES; ++i) {
    ASSERT_STREQ(profiles[i]->name().c_str(),
        ttree_merged.nodes[0].aggregated.input_profiles[i + OFFSET].c_str());
  }
}

/// Test parameter class that helps to test time series resampling during profile pretty
/// printing with a varying number of test samples.
struct TimeSeriesTestParam {
  TimeSeriesTestParam(int num_samples, vector<const char*> expected)
    : num_samples(num_samples), expected(expected) {}
  int num_samples;
  vector<const char*> expected;

  // Used by gtest to print values of this struct
  friend std::ostream& operator<<(std::ostream& os, const TimeSeriesTestParam& p) {
    return os << "num_samples: " << p.num_samples << endl;
  }
};

class TimeSeriesCounterResampleTest : public testing::TestWithParam<TimeSeriesTestParam> {
};

/// Tests that pretty-printing a ChunkedTimeSeriesCounter limits the number or printed
/// samples to 64 or lower.
TEST_P(TimeSeriesCounterResampleTest, TestPrettyPrint) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");

  const TimeSeriesTestParam& param = GetParam();
  FLAGS_periodic_counter_update_period_ms = 500;
  const int test_period = FLAGS_periodic_counter_update_period_ms;

  // Add a counter with a sample function that counts up, starting from 0.
  int value = 0;
  auto sample_fn = [&value]() { return value++; };
  // We increase the value of this flag to allow the counter to store enough samples.
  FLAGS_status_report_interval_ms = 50000;
  RuntimeProfile::TimeSeriesCounter* counter =
      profile->AddChunkedTimeSeriesCounter("TestCounter", TUnit::UNIT, sample_fn);

  // Stop counter updates from interfering with the rest of the test.
  StopAndClearCounter(profile, counter);

  // Reset value after previous values have been retrieved.
  value = 0;
  for (int i = 0; i < param.num_samples; ++i) counter->AddSample(test_period);

  int num_samples = 0;
  int result_period = 0;
  // Retrieve and validate samples.
  const int64_t* samples = counter->GetSamplesTest(&num_samples, &result_period);
  ASSERT_EQ(num_samples, param.num_samples);
  // No resampling happens with ChunkedTimeSeriesCounter.
  ASSERT_EQ(result_period, test_period);

  for (int i = 0; i < param.num_samples; ++i) ASSERT_EQ(samples[i], i);

  stringstream pretty;
  profile->PrettyPrint(&pretty);
  const string pretty_str = pretty.str();

  for (const char* e : param.expected) EXPECT_STR_CONTAINS(pretty_str, e);
}

INSTANTIATE_TEST_SUITE_P(VariousNumbers, TimeSeriesCounterResampleTest,
    ::testing::Values(
    TimeSeriesTestParam(64, {"TestCounter (500.000ms): 0, 1, 2, 3", "61, 62, 63"}),

    TimeSeriesTestParam(65, {"TestCounter (1s000ms): 0, 2, 4, 6,",
    "60, 62, 64 (Showing 33 of 65 values from Thrift Profile)"}),

    TimeSeriesTestParam(80, {"TestCounter (1s000ms): 0, 2, 4, 6,",
    "74, 76, 78 (Showing 40 of 80 values from Thrift Profile)"}),

    TimeSeriesTestParam(127, {"TestCounter (1s000ms): 0, 2, 4, 6,",
    "122, 124, 126 (Showing 64 of 127 values from Thrift Profile)"}),

    TimeSeriesTestParam(128, {"TestCounter (1s000ms): 0, 2, 4, 6,",
    "122, 124, 126 (Showing 64 of 128 values from Thrift Profile)"}),

    TimeSeriesTestParam(129, {"TestCounter (1s500ms): 0, 3, 6, 9,",
    "120, 123, 126 (Showing 43 of 129 values from Thrift Profile)"})
    ));

// Tests that the __isset field for TRuntimeProfileNode.node_metadata is set correctly
// (IMPALA-8252).
TEST(ToThrift, NodeMetadataIsSetCorrectly) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  TRuntimeProfileTree thrift_profile;
  profile->ToThrift(&thrift_profile);
  // Profile is empty, expect 0 nodes
  EXPECT_EQ(thrift_profile.nodes.size(), 1);
  EXPECT_FALSE(thrift_profile.nodes[0].__isset.node_metadata);

  // Set the plan node ID and make sure that the field is marked correctly
  profile->SetPlanNodeId(1);
  profile->ToThrift(&thrift_profile);
  EXPECT_TRUE(thrift_profile.nodes[0].__isset.node_metadata);
}

TEST(ToJson, RuntimeProfileToJsonTest) {
  ObjectPool pool;
  RuntimeProfile* profile_a = RuntimeProfile::Create(&pool, "ProfileA");
  RuntimeProfile* profile_a1 = RuntimeProfile::Create(&pool, "ProfileA1");
  RuntimeProfile* profile_ab = RuntimeProfile::Create(&pool, "ProfileAb");
  RuntimeProfile::Counter* counter_a;

  // Initialize for further validation
  profile_a->AddChild(profile_a1);
  profile_a->AddChild(profile_ab);
  profile_a->AddInfoString("Key", "Value");

  counter_a = profile_a->AddCounter("A", TUnit::UNIT);
  counter_a->Set(1);
  RuntimeProfile::HighWaterMarkCounter* high_water_counter =
      profile_a->AddHighWaterMarkCounter("high_water_counter", TUnit::BYTES);
  high_water_counter->Set(10);
  high_water_counter->Add(10);
  high_water_counter->Set(10);

  RuntimeProfile::SummaryStatsCounter* summary_stats_counter =
      profile_a->AddSummaryStatsCounter("summary_stats_counter", TUnit::TIME_NS);
  summary_stats_counter->UpdateCounter(10);
  summary_stats_counter->UpdateCounter(20);

  // Serialize to json
  rapidjson::Document doc(rapidjson::kObjectType);
  profile_a->ToJson(&doc);
  rapidjson::Value& content = doc["contents"];

  // Check profile correct
  EXPECT_EQ("ProfileA", content["profile_name"]);
  EXPECT_EQ("ProfileA1", content["child_profiles"][0]["profile_name"]);
  EXPECT_EQ("ProfileAb", content["child_profiles"][1]["profile_name"]);

  // Check Info String correct
  EXPECT_EQ(1, content["info_strings"].Size());
  EXPECT_EQ("Key", content["info_strings"][0]["key"]);
  EXPECT_EQ("Value", content["info_strings"][0]["value"]);

  // Check counter value matches
  EXPECT_EQ(4, content["counters"].Size());
  for (auto& itr : content["counters"].GetArray()) {
    // check normal Counter
    if (itr["counter_name"] == "A") {
      EXPECT_EQ(1, itr["value"].GetInt());
      EXPECT_EQ("UNIT", itr["unit"]);
    }// check HighWaterMarkCounter
    else if (itr["counter_name"] == "high_water_counter") {
      EXPECT_EQ(20, itr["value"].GetInt());
      EXPECT_EQ("BYTES", itr["unit"]);
    } else {
      EXPECT_TRUE(IsDefaultCounter(itr["counter_name"].GetString()))
          << itr["counter_name"].GetString();
    }
  }

  // Check SummaryStatsCounter
  EXPECT_EQ(1, content["summary_stats_counters"].Size());
  for (auto& itr : content["summary_stats_counters"].GetArray()) {
    if (itr["counter_name"] == "summary_stats_counter") {
      EXPECT_EQ(10, itr["min"].GetInt());
      EXPECT_EQ(20, itr["max"].GetInt());
      EXPECT_EQ(15, itr["avg"].GetInt());
      EXPECT_EQ(2, itr["num_of_samples"].GetInt());
      EXPECT_EQ("TIME_NS", itr["unit"]);
    }
  }
}

// Test when some fields are not set. ToJson will not add them as a member
TEST(ToJson, EmptyTest) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");

  // Serialize to json
  rapidjson::Document doc(rapidjson::kObjectType);
  profile->ToJson(&doc);
  rapidjson::Value& content = doc["contents"];

  EXPECT_EQ("Profile", content["profile_name"]);
  EXPECT_TRUE(content.HasMember("num_children"));

  // Empty profile should not have following members
  EXPECT_TRUE(!content.HasMember("info_strings"));
  EXPECT_TRUE(!content.HasMember("event_sequences"));
  EXPECT_TRUE(!content.HasMember("summary_stats_counters"));
  EXPECT_TRUE(!content.HasMember("time_series_counters"));
  EXPECT_TRUE(!content.HasMember("child_profiles"));

  // Only default counters should be present.
  EXPECT_EQ(2, content["counters"].Size());
  for (auto& itr : content["counters"].GetArray()) {
    EXPECT_TRUE(IsDefaultCounter(itr["counter_name"].GetString()))
        << itr["counter_name"].GetString();
  }
}

TEST(ToJson, EventSequenceToJsonTest) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");
  RuntimeProfile::EventSequence* seq = profile->AddEventSequence("event sequence");
  seq->MarkEvent("aaaa");
  seq->MarkEvent("bbbb");
  seq->MarkEvent("cccc");

  // Serialize to json
  rapidjson::Document doc(rapidjson::kObjectType);
  rapidjson::Value event_sequence_json(rapidjson::kObjectType);
  seq->ToJson(RuntimeProfile::Verbosity::DEFAULT, doc, &event_sequence_json);

  EXPECT_EQ(0, event_sequence_json["offset"].GetInt());

  uint64_t last_timestamp = 0;
  string last_string = "";
  for (auto& itr : event_sequence_json["events"].GetArray()) {
    EXPECT_TRUE(itr["timestamp"].GetInt() >= last_timestamp);
    last_timestamp = itr["timestamp"].GetInt();
    string label = string(itr["label"].GetString());
    EXPECT_TRUE(label > last_string);
    last_string = label;
  }
}

TEST(ToJson, TimeSeriesCounterToJsonTest) {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "Profile");

  // 1. TimeSeriesCounter should be empty
  rapidjson::Document doc(rapidjson::kObjectType);
  profile->ToJson(&doc);
  EXPECT_TRUE(!doc["contents"].HasMember("time_series_counters"));

  // 2. Check Serialize to json
  const int test_period = FLAGS_periodic_counter_update_period_ms;

  // Add a counter with a sample function that counts up, starting from 0.
  int value = 0;
  auto sample_fn = [&value]() { return value++; };

  // We increase the value of this flag to allow the counter to store enough samples.
  FLAGS_status_report_interval_ms = 50000;
  RuntimeProfile::TimeSeriesCounter* counter =
      profile->AddChunkedTimeSeriesCounter("TimeSeriesCounter", TUnit::UNIT, sample_fn);
  auto counter2 = static_cast<RuntimeProfile::SamplingTimeSeriesCounter*>(
      profile->AddSamplingTimeSeriesCounter("SamplingCounter", TUnit::UNIT, sample_fn));

  // Stop counter updates from interfering with the rest of the test.
  StopAndClearCounter(profile, counter);
  // ChunkedTimeSeriesCounters are stopped and cleared above.
  // But SamplingTimeSeriesCounter needs explicitly Reset() to back to the initial state.
  counter2->Reset();

  // Reset value after previous values have been retrieved.
  value = 0;
  for (int i = 0; i < 64; ++i) counter->AddSample(test_period);

  value = 0;
  for (int i = 0; i < 80; ++i) counter2->AddSample(test_period);

  profile->ToJson(&doc);
  EXPECT_EQ(doc["contents"]["time_series_counters"][1]["counter_name"],
      "TimeSeriesCounter");
  EXPECT_STR_CONTAINS(
      doc["contents"]["time_series_counters"][1]["data"].GetString(), "0,1,2,3,4");
  EXPECT_STR_CONTAINS(
      doc["contents"]["time_series_counters"][1]["data"].GetString(), "60,61,62,63");

  EXPECT_EQ(doc["contents"]["time_series_counters"][0]["counter_name"],
      "SamplingCounter");
  EXPECT_STR_CONTAINS(
      doc["contents"]["time_series_counters"][0]["data"].GetString(), "0,2,4,6");
  EXPECT_STR_CONTAINS(
      doc["contents"]["time_series_counters"][0]["data"].GetString(), "72,74,76,78");
}

} // namespace impala

