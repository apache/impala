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

#include <cstdlib>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/flags.h"
#include "kudu/util/minidump.h"
#include "kudu/util/signal.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

DEFINE_int32_hidden(test_timeout_after, 0,
             "Maximum total seconds allowed for all unit tests in the suite. Default: disabled");

DEFINE_int32_hidden(stress_cpu_threads, 0,
             "Number of threads to start that burn CPU in an attempt to "
             "stimulate race conditions");

namespace kudu {

// Start thread that kills the process if --test_timeout_after is exceeded before
// the tests complete.
static void CreateAndStartTimeoutThread() {
  if (FLAGS_test_timeout_after == 0) return;

  // KUDU-1995: if running death tests using EXPECT_EXIT()/ASSERT_EXIT(), LSAN
  // reports leaks in CreateAndStartTimeoutThread(). Adding a couple of scoped
  // leak check disablers as a workaround since right now it's not clear what
  // is going on exactly: LSAN does not report those leaks for tests which run
  // ASSERT_DEATH(). This does not seem harmful or hiding any potential leaks
  // since it's scoped and targeted only for this utility thread.
  debug::ScopedLeakCheckDisabler disabler;
  std::thread([=](){
      debug::ScopedLeakCheckDisabler disabler;
      SleepFor(MonoDelta::FromSeconds(FLAGS_test_timeout_after));
      // Dump a pstack to stdout.
      WARN_NOT_OK(PstackWatcher::DumpStacks(), "Unable to print pstack");

      // ...and abort.
      LOG(FATAL) << "Maximum unit test time exceeded (" << FLAGS_test_timeout_after << " sec)";
    }).detach();
}
} // namespace kudu


static void StartStressThreads() {
  for (int i = 0; i < FLAGS_stress_cpu_threads; i++) {
    std::thread([]{
        while (true) {
          // Do something which won't be optimized out.
          base::subtle::MemoryBarrier();
        }
      }).detach();
  }
}

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();

  // We don't use InitGoogleLoggingSafe() because gtest initializes glog, so we
  // need to block SIGUSR1 explicitly in order to test minidump generation.
  CHECK_OK(kudu::BlockSigUSR1());

  // Ignore SIGPIPE for all tests so that threads writing to TLS
  // sockets do not crash when writing to a closed socket. See KUDU-1910.
  kudu::IgnoreSigPipe();

  // InitGoogleTest() must precede ParseCommandLineFlags(), as the former
  // removes gtest-related flags from argv that would trip up the latter.
  ::testing::InitGoogleTest(&argc, argv);
  kudu::ParseCommandLineFlags(&argc, &argv, true);

  // Create the test-timeout timer.
  kudu::CreateAndStartTimeoutThread();

  StartStressThreads();

  // This is called by the KuduTest setup method, but in case we have
  // any tests that don't inherit from KuduTest, it's helpful to
  // cover our bases and call it here too.
  kudu::KuduTest::OverrideKrb5Environment();

  int ret = RUN_ALL_TESTS();

  return ret;
}
