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

#include "common/daemon.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/logging.h"
#include "util/mem-info.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"
#include "util/thread.h"

DECLARE_string(hostname);

void impala::InitDaemon(int argc, char** argv) {
  // Set the default hostname. The user can override this with the hostname flag.
  GetHostname(&FLAGS_hostname);

  google::SetVersionString(impala::GetBuildVersion());
  google::ParseCommandLineFlags(&argc, &argv, true);
  impala::InitGoogleLoggingSafe(argv[0]);
  impala::InitThreading();

  LOG(INFO) << impala::GetVersionString();
  LOG(INFO) << "Using hostname: " << FLAGS_hostname;
  impala::LogCommandLineFlags();

  InitThriftLogging();
  CpuInfo::Init();
  DiskInfo::Init();
  MemInfo::Init();

  LOG(INFO) << CpuInfo::DebugString();
  LOG(INFO) << DiskInfo::DebugString();
  LOG(INFO) << MemInfo::DebugString();
}
