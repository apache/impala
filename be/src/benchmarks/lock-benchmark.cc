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

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <mutex>
#include <sstream>
#include <vector>
#include <boost/thread/thread.hpp>
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/spinlock.h"

#include "common/names.h"

using namespace impala;

// Benchmark for locking.
// Machine Info: Intel(R) Core(TM) i7-4770 CPU @ 3.40GHz
// locking:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       Unlocked 1-Total Threads               52.38                  1X
//         Atomic 1-Total Threads               17.84             0.3406X
//       SpinLock 1-Total Threads               8.923             0.1704X
//          Boost 1-Total Threads               6.055             0.1156X
//
//       Unlocked 4-Total Threads               91.46                  1X
//         Atomic 4-Total Threads                2.43            0.02657X
//       SpinLock 4-Total Threads              0.6329            0.00692X
//          Boost 4-Total Threads              0.2672           0.002922X
//
//       Unlocked 8-Total Threads               66.82                  1X
//         Atomic 8-Total Threads               2.406            0.03601X
//       SpinLock 8-Total Threads              0.4092           0.006124X
//          Boost 8-Total Threads              0.2477           0.003707X
//
//      Unlocked 12-Total Threads               64.48                  1X
//        Atomic 12-Total Threads               2.413            0.03743X
//      SpinLock 12-Total Threads              0.4085           0.006335X
//         Boost 12-Total Threads              0.2527           0.003918X
//
//      Unlocked 16-Total Threads               66.04                  1X
//        Atomic 16-Total Threads               2.397            0.03629X
//      SpinLock 16-Total Threads              0.4119           0.006237X
//         Boost 16-Total Threads               0.257           0.003892X
//
//      Unlocked 20-Total Threads               65.56                  1X
//        Atomic 20-Total Threads                2.39            0.03645X
//      SpinLock 20-Total Threads              0.4103           0.006259X
//         Boost 20-Total Threads              0.2558           0.003901X
//
//      Unlocked 24-Total Threads               65.14                  1X
//        Atomic 24-Total Threads               2.406            0.03694X
//      SpinLock 24-Total Threads              0.4087           0.006274X
//         Boost 24-Total Threads              0.2558           0.003926X

struct TestData {
  int num_producer_threads;
  int num_consumer_threads;
  int64_t num_produces;
  int64_t num_consumes;
  int64_t value;
};

mutex lock_;
SpinLock spinlock_;

typedef std::function<void (int64_t, int64_t*)> Fn;

void UnlockedConsumeThread(int64_t n, int64_t* value) {
  // volatile to prevent compile from collapsing this loop to *value -= n
  volatile int64_t* v = value;
  for (int64_t i = 0; i < n; ++i) {
    --(*v);
  }
}
void UnlockedProduceThread(int64_t n, int64_t* value) {
  // volatile to prevent compile from collapsing this loop to *value += n
  volatile int64_t* v = value;
  for (int64_t i = 0; i < n; ++i) {
    ++(*v);
  }
}

void AtomicConsumeThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    __sync_fetch_and_add(value, -1);
  }
}
void AtomicProduceThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    __sync_fetch_and_add(value, 1);
  }
}

void SpinLockConsumeThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    lock_guard<SpinLock> l(spinlock_);
    --(*value);
  }
}
void SpinLockProduceThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    lock_guard<SpinLock> l(spinlock_);
    ++(*value);
  }
}

void BoostConsumeThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    lock_guard<mutex> l(lock_);
    --(*value);
  }
}
void BoostProduceThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    lock_guard<mutex> l(lock_);
    ++(*value);
  }
}

void LaunchThreads(void* d, Fn consume_fn, Fn produce_fn, int64_t scale) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->value = 0;
  int64_t num_per_consumer = 0;
  int64_t num_per_producer = 0;
  if (data->num_consumer_threads > 0) {
    num_per_consumer = data->num_consumes / data->num_consumer_threads;
  }
  if (data->num_producer_threads > 0) {
    num_per_producer = data->num_produces / data->num_producer_threads;
  }
  num_per_producer *= scale;
  num_per_consumer *= scale;
  thread_group consumers, producers;
  for (int i = 0; i < data->num_consumer_threads; ++i) {
    consumers.add_thread(
        new thread(consume_fn, num_per_consumer, &data->value));
  }
  for (int i = 0; i < data->num_producer_threads; ++i) {
    consumers.add_thread(
        new thread(produce_fn, num_per_producer, &data->value));
  }
  consumers.join_all();
  producers.join_all();
}

void TestUnlocked(int batch_size, void* d) {
  LaunchThreads(d, UnlockedConsumeThread, UnlockedProduceThread, batch_size);
}

void TestAtomic(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  LaunchThreads(d, AtomicConsumeThread, AtomicProduceThread, batch_size);
  if (data->num_consumer_threads > 0) CHECK_EQ(data->value, 0);
}

void TestSpinLock(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  LaunchThreads(d, SpinLockConsumeThread, SpinLockProduceThread, batch_size);
  if (data->num_consumer_threads > 0) CHECK_EQ(data->value, 0);
}

void TestBoost(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  LaunchThreads(d, BoostConsumeThread, BoostProduceThread, batch_size);
  if (data->num_consumer_threads > 0) CHECK_EQ(data->value, 0);
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  int64_t N = 10000L;
  const int max_producers = 12;

  Benchmark suite("locking", /* micro = */ false);
  TestData data[max_producers + 1];
  for (int i = 0; i <= max_producers; i += 2) {
    if (i == 0) {
      // Single thread / no contention case.
      data[i].num_producer_threads = 1;
      data[i].num_consumer_threads = 0;
    } else {
      data[i].num_producer_threads = i;
      data[i].num_consumer_threads = i;
    }
    data[i].num_produces = N;
    data[i].num_consumes = N;

    stringstream suffix;
    stringstream name;
    suffix << " " << data[i].num_producer_threads + data[i].num_consumer_threads
           << "-Total Threads";

    name.str("");
    name << "Unlocked" << suffix.str();
    int baseline = suite.AddBenchmark(name.str(), TestUnlocked, &data[i], -1);

    name.str("");
    name << "Atomic" << suffix.str();
    suite.AddBenchmark(name.str(), TestAtomic, &data[i], baseline);

    name.str("");
    name << "SpinLock" << suffix.str();
    suite.AddBenchmark(name.str(), TestSpinLock, &data[i], baseline);

    name.str("");
    name << "Boost" << suffix.str();
    suite.AddBenchmark(name.str(), TestBoost, &data[i], baseline);
  }
  cout << suite.Measure() << endl;

  return 0;
}
