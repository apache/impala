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

#ifndef IMPALA_COMMON_INIT_H
#define IMPALA_COMMON_INIT_H

#include "util/test-info.h"
#include "common/status.h"

// Using the first real-time signal available to initiate graceful shutdown.
// See "Real-time signals" section under signal(7)'s man page for more info.
#define IMPALA_SHUTDOWN_SIGNAL SIGRTMIN

namespace impala {

/// Returned by InitCommonRuntime(). Backend tests (including benchmarks) must keep it
/// alive in main() until all tests have run and any servers they own are shut down.
/// If InitCommonRuntime() started a JVM for a backend test, the destructor shuts it
/// down. Shutting down the JVM blocks until every thread attached to it has exited, so
/// a test that leaks such threads (e.g. by leaking a server) will hang in the
/// destructor. Set the environment variable IMPALA_BETEST_SKIP_JVM_SHUTDOWN to a
/// non-empty value other than "0" to skip the shutdown, e.g. to tell a hang at exit
/// apart from a hang in a test.
/// The destructor does nothing for daemons, FE tests and external frontends, where the
/// JVM is not owned by this process's main().
class [[nodiscard]] CommonRuntimeCtx {
 public:
  CommonRuntimeCtx(CommonRuntimeCtx&& other) noexcept : owns_jvm_(other.owns_jvm_) {
    other.owns_jvm_ = false;
  }
  CommonRuntimeCtx& operator=(CommonRuntimeCtx&&) = delete;
  CommonRuntimeCtx(const CommonRuntimeCtx&) = delete;
  CommonRuntimeCtx& operator=(const CommonRuntimeCtx&) = delete;
  ~CommonRuntimeCtx();

  /// Gives up responsibility for shutting down the JVM.
  void Release() { owns_jvm_ = false; }

 private:
  friend CommonRuntimeCtx InitCommonRuntime(int argc, char** argv, bool init_jvm,
      TestInfo::Mode m, bool external_fe);

  explicit CommonRuntimeCtx(bool owns_jvm) : owns_jvm_(owns_jvm) {}

  bool owns_jvm_;
};

/// Initialises logging, flags, and, if init_jvm is true, an embedded JVM.
/// Tests must indicate if they are a FE or BE test to output logs to the appropriate
/// logging directory, and enable special test-specific behavior.
/// Callers that want to override default gflags variables should do so before calling
/// this method. No logging should be performed until after this method returns.
/// Passing external_fe=true causes specific initialization steps to be skipped
/// that an external frontend will have already performed.
/// The result must be stored in a local variable in main() (see CommonRuntimeCtx).
/// Callers that do not want the JVM shut down (daemons) call Release() on it.
CommonRuntimeCtx InitCommonRuntime(int argc, char** argv, bool init_jvm,
    TestInfo::Mode m = TestInfo::NON_TEST, bool external_fe = false);

/// Starts background memory maintenance thread. Must be called after
/// RegisterMemoryMetrics(). This thread is needed for daemons to free memory and
/// refresh metrics but is not needed for standalone tests.
Status StartMemoryMaintenanceThread() WARN_UNUSED_RESULT;

/// Starts Impala shutdown signal handler thread. This thread is responsible for
/// synchronously handling the IMPALA_SHUTDOWN_SIGNAL signal and initiating graceful
/// shutdown when it is received. Must be called only after IMPALA_SHUTDOWN_SIGNAL is
/// blocked on all threads in the process and impala server has started.
Status StartImpalaShutdownSignalHandlerThread() WARN_UNUSED_RESULT;
}

#endif
