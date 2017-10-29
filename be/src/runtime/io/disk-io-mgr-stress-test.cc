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

#include <gflags/gflags.h>

#include "common/init.h"
#include "runtime/io/disk-io-mgr-stress.h"
#include "common/init.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "util/string-parser.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

// Simple utility to run the disk io stress test.  A optional second parameter
// can be passed to control how long to run this test (0 for forever).

// TODO: make these configurable once we decide how to run BE tests with args
constexpr int DEFAULT_DURATION_SEC = 1;
const int NUM_DISKS = 5;
const int NUM_THREADS_PER_DISK = 5;
const int NUM_CLIENTS = 10;
const bool TEST_CANCELLATION = true;
const int64_t BUFFER_POOL_CAPACITY = 1024L * 1024L * 1024L * 4L;

DEFINE_int64(duration_sec, DEFAULT_DURATION_SEC,
    "Disk I/O Manager stress test duration in seconds. 0 means run indefinitely.");

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();

  if (FLAGS_duration_sec != 0) {
    printf("Running stress test for %ld seconds.\n", FLAGS_duration_sec);
  } else {
    printf("Running stress test indefinitely.\n");
  }

  TestEnv test_env;
  // Tests try to allocate arbitrarily small buffers. Ensure Buffer Pool allows it.
  test_env.SetBufferPoolArgs(DiskIoMgrStress::MIN_READ_BUFFER_SIZE, BUFFER_POOL_CAPACITY);
  Status status = test_env.Init();
  CHECK(status.ok()) << status.GetDetail();
  DiskIoMgrStress test(NUM_DISKS, NUM_THREADS_PER_DISK, NUM_CLIENTS, TEST_CANCELLATION);
  test.Run(FLAGS_duration_sec);
  return 0;
}
