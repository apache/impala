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

#include <glog/logging.h>
#include <vector>

#include "cache-hash-table.h"
#include "cache-hash-table.inline.h"
#include "standard-hash-table.h"
#include "standard-hash-table.inline.h"
#include "tuple-types.h"
#include "runtime/mem-pool.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/hash-util.h"
#include "util/runtime-profile.h"
#include "util/stopwatch.h"

using namespace impala;

// Very basic hash aggregation prototype and test
// TODO: Generalize beyond hash aggregation, beyond hashing on the one column, etc.

CacheHashTable::CacheHashTable() {
  num_content_allocated_ = 0;
}

void CacheHashTable::BucketSizeDistribution() {
  std::vector<int> bucket_size;
  for (int i = 0; i < BUCKETS; ++i) {
    int size = buckets_[i].count;
    if (size >= bucket_size.size()) {
      // grow bucket_size to fit this size
      bucket_size.resize(size + 1, 0);
    }
    ++bucket_size[size];
  }

  std::stringstream distr;
  for (int i = 0; i < bucket_size.size(); ++i) {
    distr << i << ": " << bucket_size[i] << "\n";
  }
  LOG(INFO) << "Bucket Size Distribution\n" << distr.str();
}


// Update ht, which is doing a COUNT(*) GROUP BY id,
// by having it process the new tuple probe.
// Templatized on the type of hash table so we can reuse code without virtual calls.
template<typename T>
inline void Process(T* ht, const ProbeTuple* probe) {
  BuildTuple *existing = ht->Find(probe);
  if (existing != NULL) {
    ++existing->count;
  } else {
    BuildTuple build;
    build.id = probe->id;
    build.count = 1;
    ht->Insert(&build);
  }
}

// Test ht by aggregating input, which is an array of num_tuples ProbeTuples
// Templatized on the type of hash table so we can reuse code without virtual calls.
template<typename T>
uint64_t Test(T* ht, const ProbeTuple* input, uint64_t num_tuples)
{
  StopWatch time;
  time.Start();
  for (int i = 0; i < num_tuples; ++i) {
    Process<T>(ht, &input[i]);
  }
  time.Stop();
  return time.Ticks();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  CpuInfo::Init();

  srand(time(NULL));

  const int NUM_TUPLES = 100000000; //10^8
  const int NUM_BUILD_TUPLES = 4 * CacheHashTable::MaxBuildTuples() / 10;

  CacheHashTable cache_ht;
  StandardHashTable std_ht;

  ProbeTuple* input = GenTuples(NUM_TUPLES, NUM_BUILD_TUPLES);
  uint64_t cache_time = Test<CacheHashTable>(&cache_ht, input, NUM_TUPLES);
  LOG(ERROR) << "Cache-aware time: "
             << PrettyPrinter::Print(cache_time, TUnit::CPU_TICKS);
  uint64_t std_time = Test<StandardHashTable>(&std_ht, input, NUM_TUPLES);

  LOG(ERROR) << "Bucket-chained time: "
             << PrettyPrinter::Print(std_time, TUnit::CPU_TICKS);
  return 0;
}
