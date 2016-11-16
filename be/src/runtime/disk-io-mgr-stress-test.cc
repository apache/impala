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

#include "runtime/disk-io-mgr-stress.h"
#include "util/cpu-info.h"
#include "util/string-parser.h"

#include "common/names.h"

using namespace impala;

// Simple utility to run the disk io stress test.  A optional second parameter
// can be passed to control how long to run this test (0 for forever).

// TODO: make these configurable once we decide how to run BE tests with args
const int DEFAULT_DURATION_SEC = 1;
const int NUM_DISKS = 5;
const int NUM_THREADS_PER_DISK = 5;
const int NUM_CLIENTS = 10;
const bool TEST_CANCELLATION = true;

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  CpuInfo::Init();
  OsInfo::Init();
  impala::InitThreading();
  int duration_sec = DEFAULT_DURATION_SEC;

  if (argc == 2) {
    StringParser::ParseResult status;
    duration_sec = StringParser::StringToInt<int>(argv[1], strlen(argv[1]), &status);
    if (status != StringParser::PARSE_SUCCESS) {
      printf("Invalid arg: %s\n", argv[1]);
      return 1;
    }
  }
  if (duration_sec != 0) {
    printf("Running stress test for %d seconds.\n", duration_sec);
  } else {
    printf("Running stress test indefinitely.\n");
  }
  DiskIoMgrStress test(NUM_DISKS, NUM_THREADS_PER_DISK, NUM_CLIENTS, TEST_CANCELLATION);
  test.Run(duration_sec);

  return 0;
}
