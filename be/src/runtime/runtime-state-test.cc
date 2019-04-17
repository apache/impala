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

#include <memory>

#include <boost/thread/thread.hpp>

#include "codegen/llvm-codegen.h"
#include "common/atomic.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

// Regression test for IMPALA-8270: MemTracker teardown order leading to
// use-after-free from LogUsage.
TEST(RuntimeStateTest, MemTrackerTeardown) {
  const int LOG_USAGE_THREADS = 4;
  const int CONSTRUCT_ITERS = 10;
  TestEnv test_env;
  ASSERT_OK(test_env.Init());
  // Poll LogUsage() from other threads to reproduce race.
  AtomicBool stop_threads(false);
  thread_group log_usage_threads;
  for (int i = 0; i < LOG_USAGE_THREADS; ++i) {
    log_usage_threads.add_thread(new thread([&test_env, &stop_threads]() {
      while (!stop_threads.Load()) {
        string usage = test_env.exec_env()->process_mem_tracker()->LogUsage(100);
      }
    }));
  }

  // Construct and tear down RuntimeState repeatedly to try to reproduce race.
  for (int i = 0; i < CONSTRUCT_ITERS; ++i) {
    TQueryCtx query_ctx;
    unique_ptr<RuntimeState> state(new RuntimeState(query_ctx, test_env.exec_env()));
    ASSERT_TRUE(state->instance_mem_tracker() != nullptr);
    state->ReleaseResources();
    state.reset();
  }
  stop_threads.Store(true);
  log_usage_threads.join_all();
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
