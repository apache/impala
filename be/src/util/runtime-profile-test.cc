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
#include <stdio.h>
#include <iostream>
#include <boost/bind.hpp>

#include "common/object-pool.h"
#include "testutil/gtest-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"
#include "util/streaming-sampler.h"
#include "util/thread.h"
#include "util/time.h"

#include "common/names.h"

namespace impala {

TEST(CountersTest, Basic) {
  ObjectPool pool;
  RuntimeProfile* profile_a = RuntimeProfile::Create(&pool, "ProfileA");
  RuntimeProfile* profile_a1 = RuntimeProfile::Create(&pool, "ProfileA1");
  RuntimeProfile* profile_a2 = RuntimeProfile::Create(&pool, "ProfileAb");

  TRuntimeProfileTree thrift_profile;

  profile_a->AddChild(profile_a1);
  profile_a->AddChild(profile_a2);

  // Test Empty
  profile_a->ToThrift(&thrift_profile.nodes);
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

  // Serialize/deserialize
  profile_a->ToThrift(&thrift_profile.nodes);
  RuntimeProfile* from_thrift = RuntimeProfile::CreateFromThrift(&pool, thrift_profile);
  counter_merged = from_thrift->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);
  EXPECT_TRUE(from_thrift->GetCounter("Not there") ==  NULL);

  // Averaged
  RuntimeProfile* averaged_profile = RuntimeProfile::Create(&pool, "Merged", true);
  averaged_profile->UpdateAverage(from_thrift);
  counter_merged = averaged_profile->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);

  // UpdateAverage again, there should be no change.
  averaged_profile->UpdateAverage(from_thrift);
  EXPECT_EQ(counter_merged->value(), 1);

  counter_a = profile_a2->AddCounter("A", TUnit::UNIT);
  counter_a->Set(3);
  averaged_profile->UpdateAverage(profile_a2);
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

void ValidateCounter(RuntimeProfile* profile, const string& name, int64_t value) {
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
  RuntimeProfile* averaged_profile = RuntimeProfile::Create(&pool, "merged", true);
  averaged_profile->UpdateAverage(profile1);
  averaged_profile->UpdateAverage(profile2);
  EXPECT_EQ(5, averaged_profile->num_counters());
  ValidateCounter(averaged_profile, "Parent Shared", 2);
  ValidateCounter(averaged_profile, "Parent 1 Only", 2);
  ValidateCounter(averaged_profile, "Parent 2 Only", 5);

  vector<RuntimeProfile*> children;
  averaged_profile->GetChildren(&children);
  EXPECT_EQ(children.size(), 3);

  for (int i = 0; i < 3; ++i) {
    RuntimeProfile* profile = children[i];
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
      EXPECT_TRUE(false);
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
    RuntimeProfile* profile = children[i];
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
      EXPECT_TRUE(false);
    }
  }

  // make sure we can print
  profile2->PrettyPrint(&dummy);
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
  RuntimeProfile::AveragedCounter bytes_avg(TUnit::BYTES);
  bytes_avg.UpdateCounter(bytes_1_counter);
  // Avg of 10L
  EXPECT_EQ(bytes_avg.value(), 10);
  bytes_1_counter->Set(20L);
  bytes_avg.UpdateCounter(bytes_1_counter);
  // Avg of 20L
  EXPECT_EQ(bytes_avg.value(), 20);
  bytes_2_counter->Set(40L);
  bytes_avg.UpdateCounter(bytes_2_counter);
  // Avg of 20L and 40L
  EXPECT_EQ(bytes_avg.value(), 30);
  bytes_2_counter->Set(30L);
  bytes_avg.UpdateCounter(bytes_2_counter);
  // Avg of 20L and 30L
  EXPECT_EQ(bytes_avg.value(), 25);

  RuntimeProfile::Counter* double_1_counter =
      profile->AddCounter("double 1", TUnit::DOUBLE_VALUE);
  RuntimeProfile::Counter* double_2_counter =
      profile->AddCounter("double 2", TUnit::DOUBLE_VALUE);
  double_1_counter->Set(1.0f);
  RuntimeProfile::AveragedCounter double_avg(TUnit::DOUBLE_VALUE);
  double_avg.UpdateCounter(double_1_counter);
  // Avg of 1.0f
  EXPECT_EQ(double_avg.double_value(), 1.0f);
  double_1_counter->Set(2.0f);
  double_avg.UpdateCounter(double_1_counter);
  // Avg of 2.0f
  EXPECT_EQ(double_avg.double_value(), 2.0f);
  double_2_counter->Set(4.0f);
  double_avg.UpdateCounter(double_2_counter);
  // Avg of 2.0f and 4.0f
  EXPECT_EQ(double_avg.double_value(), 3.0f);
  double_2_counter->Set(3.0f);
  double_avg.UpdateCounter(double_2_counter);
  // Avg of 2.0f and 3.0f
  EXPECT_EQ(double_avg.double_value(), 2.5f);
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
  StreamingSampler<int, 10> sampler;

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
    bool done;

    DummyWorker()
      : thread_handle(NULL), done(false) {}

    ~DummyWorker() {
      Stop();
    }

    void Stop() {
      if (!done && thread_handle != NULL) {
        done = true;
        thread_handle->join();
        delete thread_handle;
        thread_handle = NULL;
      }
    }
  };

  void Run(DummyWorker* worker) {
    SCOPED_CONCURRENT_STOP_WATCH(&csw_);
    SCOPED_CONCURRENT_COUNTER(&timercounter_);
    while (!worker->done) {
      SleepForMs(10);
      // Each test case should be no more than one second.
      // Consider test failed if timer is more than 3 seconds.
      if (csw_.TotalRunningTime() > 3000000000) {
        EXPECT_FALSE(false);
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

  // Allow some timer inaccuracy (15ms) since thread join could take some time.
  static const int MAX_TIMER_ERROR_NS = 15000000;
  vector<DummyWorker> workers_;
  ConcurrentStopWatch csw_;
  RuntimeProfile::ConcurrentTimerCounter timercounter_;
};

void ValidateTimerValue(const TimerCounterTest& timer, int64_t start) {
  int64_t expected_value = MonotonicNanos() - start;
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
  uint64_t start = MonotonicNanos();
  tester.StartWorkers(1, 0);
  SleepForMs(250);
  ValidateTimerValue(tester, start);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
}

TEST(TimerCounterTest, CountersTestTwoThreads) {
  TimerCounterTest tester;
  uint64_t start = MonotonicNanos();
  tester.StartWorkers(2, 5);
  SleepForMs(250);
  ValidateTimerValue(tester, start);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
}

TEST(TimerCounterTest, CountersTestRandom) {
  TimerCounterTest tester;
  uint64_t start = MonotonicNanos();
  ValidateTimerValue(tester, start);
  // First working period
  tester.StartWorkers(5, 5);
  ValidateTimerValue(tester, start);
  SleepForMs(200);
  tester.StopWorkers(2);
  ValidateTimerValue(tester, start);
  SleepForMs(50);
  tester.StopWorkers(4);
  ValidateTimerValue(tester, start);
  SleepForMs(300);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
  tester.Reset();

  ValidateLapTime(&tester, MonotonicNanos() - start);
  uint64_t first_run_end = MonotonicNanos();
  // Adding some idle time. concurrent stopwatch and timer should not count the idle time.
  SleepForMs(100);
  start += MonotonicNanos() - first_run_end;

  // Second working period
  tester.StartWorkers(2, 0);
  // We just get lap time after first run finish. so at start of second run, expect lap time == 0
  ValidateLapTime(&tester, 0);
  uint64_t lap_time_start = MonotonicNanos();
  SleepForMs(100);
  ValidateTimerValue(tester, start);
  SleepForMs(100);
  tester.StopWorkers(-1);
  ValidateTimerValue(tester, start);
  ValidateLapTime(&tester, MonotonicNanos() - lap_time_start);
}

}

IMPALA_TEST_MAIN();
