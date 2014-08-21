// Copyright 2012 Cloudera Inc.
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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>
#include <sstream>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/spinlock.h"

using namespace boost;
using namespace impala;
using namespace std;

// Benchmark for locking.
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// locking:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       Unlocked 2-Total Threads                45.5                  1X
//         Atomic 2-Total Threads               2.734            0.06009X
//       SpinLock 2-Total Threads               2.245            0.04934X
//          Boost 2-Total Threads              0.5453            0.01198X
// 
//       Unlocked 6-Total Threads               61.16                  1X
//         Atomic 6-Total Threads               2.875              0.047X
//       SpinLock 6-Total Threads               1.368            0.02236X
//          Boost 6-Total Threads              0.3173           0.005187X
// 
//      Unlocked 10-Total Threads               52.18                  1X
//        Atomic 10-Total Threads               2.061             0.0395X
//      SpinLock 10-Total Threads               1.236            0.02369X
//         Boost 10-Total Threads              0.3184           0.006101X
// 
//      Unlocked 14-Total Threads               54.18                  1X
//        Atomic 14-Total Threads               2.659            0.04907X
//      SpinLock 14-Total Threads               1.274            0.02351X
//         Boost 14-Total Threads              0.3252           0.006002X
// 
//      Unlocked 18-Total Threads               53.36                  1X
//        Atomic 18-Total Threads               1.952            0.03659X
//      SpinLock 18-Total Threads               1.308            0.02452X
//         Boost 18-Total Threads              0.3259           0.006109X
// 
//      Unlocked 22-Total Threads               56.91                  1X
//        Atomic 22-Total Threads               2.711            0.04764X
//      SpinLock 22-Total Threads               1.311            0.02303X
//         Boost 22-Total Threads              0.3254           0.005718X
struct TestData {
  int num_producer_threads;
  int num_consumer_threads;
  int64_t num_produces;
  int64_t num_consumes;
  int64_t value;
};
  
mutex lock_;
SpinLock spinlock_;
  
typedef function<void (int64_t, int64_t*)> Fn;

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
    ScopedSpinLock l(&spinlock_);
    --(*value);
  }
}
void SpinLockProduceThread(int64_t n, int64_t* value) {
  for (int64_t i = 0; i < n; ++i) {
    ScopedSpinLock l(&spinlock_);
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
  int64_t num_per_consumer = data->num_consumes / data->num_consumer_threads;
  int64_t num_per_producer = data->num_produces / data->num_producer_threads;
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
  CHECK_EQ(data->value, 0);
}

void TestSpinLock(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  LaunchThreads(d, SpinLockConsumeThread, SpinLockProduceThread, batch_size);
  CHECK_EQ(data->value, 0);
}

void TestBoost(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  LaunchThreads(d, BoostConsumeThread, BoostProduceThread, batch_size);
  CHECK_EQ(data->value, 0);
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  int64_t N = 10000L;
  const int max_producers = 12;
  
  Benchmark suite("locking");
  TestData data[max_producers];
  for (int i = 0; i < max_producers; i += 2) {
    data[i].num_producer_threads = i + 1;
    data[i].num_consumer_threads = i + 1;
    data[i].num_produces = N;
    data[i].num_consumes = N;

    stringstream suffix;
    stringstream name;
    suffix << " " << (i+1) * 2 << "-Total Threads";

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

