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

/// Initialises logging, flags, and, if init_jvm is true, an embedded JVM.
/// Tests must indicate if they are a FE or BE test to output logs to the appropriate
/// logging directory, and enable special test-specific behavior.
/// Callers that want to override default gflags variables should do so before calling
/// this method. No logging should be performed until after this method returns.
/// Passing external_fe=true causes specific initialization steps to be skipped
/// that an external frontend will have already performed.
void InitCommonRuntime(int argc, char** argv, bool init_jvm,
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
