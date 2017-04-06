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

#include "common/init.h"

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>

#include "common/logging.h"
#include "common/status.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "gutil/atomicops.h"
#include "rpc/authentication.h"
#include "rpc/thrift-util.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/decimal-value.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/timestamp-parse-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/decimal-util.h"
#include "util/disk-info.h"
#include "util/logging-support.h"
#include "util/mem-info.h"
#include "util/minidump.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/os-info.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/test-info.h"
#include "util/thread.h"
#include "util/time.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(hostname);
DECLARE_string(redaction_rules_file);
// TODO: renamed this to be more generic when we have a good CM release to do so.
DECLARE_int32(logbufsecs);
DECLARE_string(heap_profile_dir);
DECLARE_bool(enable_process_lifetime_heap_profiling);

DEFINE_int32(max_log_files, 10, "Maximum number of log files to retain per severity "
    "level. The most recent log files are retained. If set to 0, all log files are "
    "retained.");

DEFINE_int32(max_audit_event_log_files, 0, "Maximum number of audit event log files "
    "to retain. The most recent audit event log files are retained. If set to 0, "
    "all audit event log files are retained.");

DEFINE_int32(memory_maintenance_sleep_time_ms, 1000, "Sleep time in milliseconds "
    "between memory maintenance iterations");

DEFINE_int64(pause_monitor_sleep_time_ms, 500, "Sleep time in milliseconds for "
    "pause monitor thread.");

DEFINE_int64(pause_monitor_warn_threshold_ms, 10000, "If the pause monitor sleeps "
    "more than this time period, a warning is logged. If set to 0 or less, pause monitor"
    " is disabled.");

DEFINE_string(local_library_dir, "/tmp",
    "Scratch space for local fs operations. Currently used for copying "
    "UDF binaries locally from HDFS and also for initializing the timezone db");

// Defined by glog. This allows users to specify the log level using a glob. For
// example -vmodule=*scanner*=3 would enable full logging for scanners. If redaction
// is enabled, this option won't be allowed because some logging dumps table data
// in ways the authors of redaction rules can't anticipate.
DECLARE_string(vmodule);

// tcmalloc will hold on to freed memory. We will periodically release the memory back
// to the OS if the extra memory is too high. If the memory used by the application
// is less than this fraction of the total reserved memory, free it back to the OS.
static const float TCMALLOC_RELEASE_FREE_MEMORY_FRACTION = 0.5f;

using std::string;

// Log maintenance thread that runs periodically. It flushes glog every logbufsecs sec.
// glog only automatically flushes the log file if logbufsecs has passed since the
// previous flush when a new log is written. That means that on a quiet system, logs
// will be buffered indefinitely. It also rotates log files.
static scoped_ptr<impala::Thread> log_maintenance_thread;

// Memory Maintenance thread that runs periodically to free up memory. It does the
// following things every memory_maintenance_sleep_time_ms secs:
// 1) Releases BufferPool memory that is not currently in use.
// 2) Frees excess memory that TCMalloc has left in its pageheap.
static scoped_ptr<impala::Thread> memory_maintenance_thread;

// A pause monitor thread to monitor process pauses in impala daemons. The thread sleeps
// for a short interval of time (THREAD_SLEEP_TIME_MS), wakes up and calculates the actual
// time slept. If that exceeds PAUSE_WARN_THRESHOLD_MS, a warning is logged.
static scoped_ptr<impala::Thread> pause_monitor;

[[noreturn]] static void LogMaintenanceThread() {
  while (true) {
    sleep(FLAGS_logbufsecs);

    google::FlushLogFiles(google::GLOG_INFO);

    // No need to rotate log files in tests.
    if (impala::TestInfo::is_test()) continue;
    // Check for log rotation in every interval of the maintenance thread
    impala::CheckAndRotateLogFiles(FLAGS_max_log_files);
    // Check for audit event log rotation in every interval of the maintenance thread
    impala::CheckAndRotateAuditEventLogFiles(FLAGS_max_audit_event_log_files);
  }
}

[[noreturn]] static void MemoryMaintenanceThread() {
  while (true) {
    SleepForMs(FLAGS_memory_maintenance_sleep_time_ms);
    impala::ExecEnv* env = impala::ExecEnv::GetInstance();
    if (env == nullptr) continue; // ExecEnv may not have been created yet.
    BufferPool* buffer_pool = env->buffer_pool();
    if (buffer_pool != nullptr) buffer_pool->Maintenance();

#ifndef ADDRESS_SANITIZER
    // Required to ensure memory gets released back to the OS, even if tcmalloc doesn't do
    // it for us. This is because tcmalloc releases memory based on the
    // TCMALLOC_RELEASE_RATE property, which is not actually a rate but a divisor based
    // on the number of blocks that have been deleted. When tcmalloc does decide to
    // release memory, it removes a single span from the PageHeap. This means there are
    // certain allocation patterns that can lead to OOM due to not enough memory being
    // released by tcmalloc, even when that memory is no longer being used.
    // One example is continually resizing a vector which results in many allocations.
    // Even after the vector goes out of scope, all the memory will not be released
    // unless there are enough other deletions that are occurring in the system.
    // This can eventually lead to OOM/crashes (see IMPALA-818).
    // See: http://google-perftools.googlecode.com/svn/trunk/doc/tcmalloc.html#runtime
    size_t bytes_used = 0;
    size_t bytes_in_pageheap = 0;
    MallocExtension::instance()->GetNumericProperty(
        "generic.current_allocated_bytes", &bytes_used);
    MallocExtension::instance()->GetNumericProperty(
        "generic.heap_size", &bytes_in_pageheap);
    if (bytes_used < bytes_in_pageheap * TCMALLOC_RELEASE_FREE_MEMORY_FRACTION) {
      MallocExtension::instance()->ReleaseFreeMemory();
    }

    // When using tcmalloc, the process limit as measured by our trackers will
    // be out of sync with the process usage. Update the process tracker periodically.
    if (env != NULL && env->process_mem_tracker() != NULL) {
      env->process_mem_tracker()->RefreshConsumptionFromMetric();
    }
#endif
    // TODO: we should also update the process mem tracker with the reported JVM
    // mem usage.
  }
}

static void PauseMonitorLoop() {
  if (FLAGS_pause_monitor_warn_threshold_ms <= 0) return;
  int64_t time_before_sleep = MonotonicMillis();
  while (true) {
    SleepForMs(FLAGS_pause_monitor_sleep_time_ms);
    int64_t sleep_time = MonotonicMillis() - time_before_sleep;
    time_before_sleep += sleep_time;
    if (sleep_time > FLAGS_pause_monitor_warn_threshold_ms) {
      LOG(WARNING) << "A process pause was detected for approximately " <<
          PrettyPrinter::Print(sleep_time, TUnit::TIME_MS);
    }
  }
}

void impala::InitCommonRuntime(int argc, char** argv, bool init_jvm,
    TestInfo::Mode test_mode) {
  CpuInfo::Init();
  DiskInfo::Init();
  MemInfo::Init();
  OsInfo::Init();
  TestInfo::Init(test_mode);

  // Verify CPU meets the minimum requirements before calling InitGoogleLoggingSafe()
  // which might use SSSE3 instructions (see IMPALA-160).
  CpuInfo::VerifyCpuRequirements();

  // Set the default hostname. The user can override this with the hostname flag.
  GetHostname(&FLAGS_hostname);

  google::SetVersionString(impala::GetBuildVersion());
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (!FLAGS_redaction_rules_file.empty()) {
    if (VLOG_ROW_IS_ON || !FLAGS_vmodule.empty()) {
      CLEAN_EXIT_WITH_ERROR("Redaction cannot be used in combination with log level 3 or "
          "higher or the -vmodule option because these log levels may log data in "
          "ways redaction rules may not anticipate.");
    }
    const string& error_message = SetRedactionRulesFromFile(FLAGS_redaction_rules_file);
    if (!error_message.empty()) CLEAN_EXIT_WITH_ERROR(error_message);
  }
  impala::InitGoogleLoggingSafe(argv[0]);
  // Breakpad needs flags and logging to initialize.
  ABORT_IF_ERROR(RegisterMinidump(argv[0]));
  AtomicOps_x86CPUFeaturesInit();
  impala::InitThreading();
  impala::TimestampParser::Init();
  impala::SeedOpenSSLRNG();
  ABORT_IF_ERROR(impala::InitAuth(argv[0]));

  // Initialize maintenance_thread after InitGoogleLoggingSafe and InitThreading.
  log_maintenance_thread.reset(
      new Thread("common", "log-maintenance-thread", &LogMaintenanceThread));

  // Memory maintenance isn't necessary for frontend tests, and it's undesirable
  // to asynchronously free memory in backend tests that are testing memory
  // management behaviour.
  if (!impala::TestInfo::is_test()) {
    memory_maintenance_thread.reset(
        new Thread("common", "memory-maintenance-thread", &MemoryMaintenanceThread));
  }

  pause_monitor.reset(new Thread("common", "pause-monitor", &PauseMonitorLoop));

  LOG(INFO) << impala::GetVersionString();
  LOG(INFO) << "Using hostname: " << FLAGS_hostname;
  impala::LogCommandLineFlags();

  InitThriftLogging();

  LOG(INFO) << CpuInfo::DebugString();
  LOG(INFO) << DiskInfo::DebugString();
  LOG(INFO) << MemInfo::DebugString();
  LOG(INFO) << OsInfo::DebugString();
  LOG(INFO) << "Process ID: " << getpid();

  // Required for the FE's Catalog
  impala::LibCache::Init();
  Status fs_cache_init_status = impala::HdfsFsCache::Init();
  if (!fs_cache_init_status.ok()) CLEAN_EXIT_WITH_ERROR(fs_cache_init_status.GetDetail());

  if (init_jvm) {
    ABORT_IF_ERROR(JniUtil::Init());
    InitJvmLoggingSupport();
  }

  if (argc == -1) {
    // Should not be called. We need BuiltinsInit() so the builtin symbols are
    // not stripped.
    DCHECK(false);
    Expr::InitBuiltinsDummy();
  }

  if (impala::KuduIsAvailable()) impala::InitKuduLogging();

#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_dir.c_str());
  }
#endif
}
