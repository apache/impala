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

// Test performance of RuntimeProfile operations on synthetic profiles.
//
// Before IMPALA-9399:
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// RuntimeProfile conversion: Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            ToThrift               30.4     30.4     30.8         1X         1X         1X
//
// After IMPALA-9399:
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// RuntimeProfile conversion: Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            ToThrift               83.4     84.6     84.9         1X         1X         1X

#include <iostream>

#include "common/object-pool.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/runtime-profile.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;

static ObjectPool profile_pool;
static RuntimeProfile* benchmark_profile = nullptr;

// Initialise *profile to a synthetic profile stored in 'profile_pool'.
// The tree should have the provided depth and fanout.
void InitProfile(int depth, int fanout, RuntimeProfile** profile) {
  *profile = RuntimeProfile::Create(&profile_pool, Substitute("level$0", depth));
  // Profiles tend to mainly have regular counters.
  const int NUM_REGULAR_COUNTERS = 20;
  // Include a smaller number of other profile items.
  const int NUM_OTHER_ITEMS = 5;
  for (int i = 0; i < NUM_REGULAR_COUNTERS; ++i) {
    (*profile)->AddCounter(Substitute("counter$0", i), TUnit::NONE);
  }
  RuntimeProfile::EventSequence* seq = (*profile)->AddEventSequence("timeline");
  for (int i = 0; i < NUM_OTHER_ITEMS; ++i) {
    (*profile)->AddInfoString(Substitute("info$0", i), Substitute("val$0", i));
    seq->MarkEvent(Substitute("event$0", i));
    RuntimeProfile::SummaryStatsCounter* stats =
        (*profile)->AddSummaryStatsCounter(Substitute("stats$0", i), TUnit::BYTES);
    stats->UpdateCounter(1);
    stats->UpdateCounter(2);
    stats->UpdateCounter(3);

    // Do not add time series or sampling counters, those are tricky to mock because they
    // are usually updated by a background thread.
  }
  if (depth == 0) return;
  for (int i = 0; i < fanout; ++i) {
    RuntimeProfile* child_profile;
    InitProfile(depth - 1, fanout, &child_profile);
    (*profile)->AddChild(child_profile, /*indent=*/ i % 2 == 0);
  }
}

void ToThriftBenchmark(int batch_size, void* dummy) {
  for (int i = 0; i < batch_size; ++i) {
    TRuntimeProfileTree tprofile;
    benchmark_profile->ToThrift(&tprofile);
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;

  InitProfile(5, 10, &benchmark_profile);

  Benchmark suite("RuntimeProfile conversion");
  suite.AddBenchmark("ToThrift", ToThriftBenchmark, nullptr);
  cout << suite.Measure() << endl;
  return 0;
}

