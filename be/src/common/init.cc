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

#include "common/init.h"

#include <google/heap-profiler.h>

#include "common/logging.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/logging-support.h"
#include "util/mem-info.h"
#include "util/network-util.h"
#include "util/os-info.h"
#include "runtime/timestamp-parse-util.h"
#include "rpc/authentication.h"
#include "rpc/thrift-util.h"
#include "util/thread.h"

DECLARE_string(hostname);
DECLARE_int32(logbufsecs);
DECLARE_bool(abort_on_config_error);
DECLARE_string(heap_profile_dir);
DECLARE_bool(enable_process_lifetime_heap_profiling);

using namespace boost;

// Thread that flushes glog every logbufsecs sec. glog flushes the log file only if
// logbufsecs has passed since the previous flush when a new log is written. That means
// that on a quiet system, logs will be buffered indefinitely.
shared_ptr<impala::Thread> glog_flusher;

// glog_flusher thread.
// Wakes up every logbufsecs sec to flush glog log file.
static void GlogFlushThread() {
  while(true) {
    sleep(FLAGS_logbufsecs);
    google::FlushLogFiles(google::GLOG_INFO);
  }
}

void impala::InitCommonRuntime(int argc, char** argv, bool init_jvm) {
  CpuInfo::Init();
  DiskInfo::Init();
  MemInfo::Init();
  OsInfo::Init();

  if (!CpuInfo::IsSupported(CpuInfo::SSE3)) {
    LOG(ERROR) << "CPU does not support the SSE3 instruction set, which is required."
               << " This could lead to instability.";
  }

  // Set the default hostname. The user can override this with the hostname flag.
  GetHostname(&FLAGS_hostname);

  google::SetVersionString(impala::GetBuildVersion());
  google::ParseCommandLineFlags(&argc, &argv, true);
  impala::InitGoogleLoggingSafe(argv[0]);
  impala::InitThreading();
  impala::TimestampParser::Init();
  EXIT_IF_ERROR(impala::InitAuth(argv[0]));

  // Initialize glog_flusher thread after InitGoogleLoggingSafe and InitThreading.
  glog_flusher.reset(new Thread("common", "glog-flush-thread", &GlogFlushThread));

  LOG(INFO) << impala::GetVersionString();
  LOG(INFO) << "Using hostname: " << FLAGS_hostname;
  impala::LogCommandLineFlags();

  InitThriftLogging();

  LOG(INFO) << CpuInfo::DebugString();
  LOG(INFO) << DiskInfo::DebugString();
  LOG(INFO) << MemInfo::DebugString();
  LOG(INFO) << OsInfo::DebugString();
  LOG(INFO) << "Process ID: " << getpid();

  if (init_jvm) {
    EXIT_IF_ERROR(JniUtil::Init());
    InitJvmLoggingSupport();
  }

  if (argc == -1) {
    // Should not be called. We need BuiltinsInit() so the builtin symbols are
    // not stripped.
    DCHECK(false);
    Expr::InitBuiltinsDummy();
  }

#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_dir.c_str());
  }
#endif
}
