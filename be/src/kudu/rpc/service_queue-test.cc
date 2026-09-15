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

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DEFINE_int32(num_producers, 4,
             "Number of producer threads");

DEFINE_int32(num_consumers, 20,
             "Number of consumer threads");

DEFINE_int32(max_queue_size, 50,
             "Max queue length");

DEFINE_int32(consumer_delay_cycles, 0,
             "Number of CPU yield cycles for a consumer thread to run after "
             "each BlockingGet() call: this is to give the producer threads "
             "a chance to fill up the queue");

namespace kudu {
namespace rpc {

static atomic<uint32_t> inprogress;

static atomic<uint32_t> total;

template <typename Queue>
void ProducerThread(Queue* queue) {
  const int max_inprogress = FLAGS_max_queue_size - FLAGS_num_producers;
  while (true) {
    // For atomics, std::memory_relaxed is enough since producer threads
    // back off when hitting QUEUE_FULL anyway. As for the re-ordering concern,
    // it's addressed by the presence of a lock in the Put() call below.
    while (inprogress.load(std::memory_order_relaxed) > max_inprogress) {
      base::subtle::PauseCPU();
    }
    inprogress.fetch_add(1, std::memory_order_relaxed);
    InboundCall* call = new InboundCall(nullptr);
    InboundCall* evicted = nullptr;
    auto status = queue->Put(call, &evicted);
    if (status == QUEUE_FULL) {
      delete call;
      base::subtle::PauseCPU();
      continue;
    }

    if (PREDICT_FALSE(evicted != nullptr)) {
      delete evicted;
      base::subtle::PauseCPU();
      continue;
    }

    if (PREDICT_TRUE(status == QUEUE_SHUTDOWN)) {
      delete call;
      break;
    }
  }
}

template <typename Queue>
void ConsumerThread(Queue* queue) {
  const int32_t delay_cycles = FLAGS_consumer_delay_cycles;
  unique_ptr<InboundCall> call;
  while (queue->BlockingGet(&call)) {
    inprogress.fetch_sub(1, std::memory_order_relaxed);
    total.fetch_add(1, std::memory_order_relaxed);
    call.reset();
    for (int32_t cnt = delay_cycles; cnt > 0; --cnt) {
      base::subtle::PauseCPU();
    }
  }
}

TEST(TestServiceQueue, LifoServiceQueuePerf) {
  LifoServiceQueue queue(FLAGS_max_queue_size);
  vector<thread> producers;
  vector<thread> consumers;

  for (int i = 0; i < FLAGS_num_producers; i++) {
    producers.emplace_back(&ProducerThread<LifoServiceQueue>, &queue);
  }

  for (int i = 0; i < FLAGS_num_consumers; i++) {
    consumers.emplace_back(&ConsumerThread<LifoServiceQueue>, &queue);
  }

  int seconds = AllowSlowTests() ? 10 : 1;
  uint64_t total_sample = 0;
  uint64_t total_queue_len = 0;
  uint64_t total_idle_workers = 0;
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  int32_t before = total;

  for (int i = 0; i < seconds * 50; i++) {
    SleepFor(MonoDelta::FromMilliseconds(20));
    ++total_sample;
    total_queue_len += queue.estimated_queue_length();
    total_idle_workers += queue.estimated_idle_worker_count();
  }

  sw.stop();
  int32_t delta = total - before;

  queue.Shutdown();
  for (auto& p : producers) {
    p.join();
  }
  for (auto& c : consumers) {
    c.join();
  }

  float reqs_per_second = static_cast<float>(delta / sw.elapsed().wall_seconds());
  float user_cpu_micros_per_req = static_cast<float>(sw.elapsed().user / 1000.0 / delta);
  float sys_cpu_micros_per_req = static_cast<float>(sw.elapsed().system / 1000.0 / delta);

  LOG(INFO) << "Reqs/sec:         " << (int32_t)reqs_per_second;
  LOG(INFO) << "User CPU per req: " << user_cpu_micros_per_req << "us";
  LOG(INFO) << "Sys CPU per req:  " << sys_cpu_micros_per_req << "us";
  LOG(INFO) << "Avg rpc queue length: " << total_queue_len / static_cast<double>(total_sample);
  LOG(INFO) << "Avg idle workers:     " << total_idle_workers / static_cast<double>(total_sample);
}

} // namespace rpc
} // namespace kudu
