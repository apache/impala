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

#include <csignal>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <third_party/lss/linux_syscall_support.h>

#include "common/global-flags.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/kudu-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/atomicops.h"
#include "gutil/strings/substitute.h"
#include "rpc/authentication.h"
#include "rpc/thrift-util.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/datetime-parse-util.h"
#include "runtime/decimal-value.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "service/impala-server.h"
#include "util/cgroup-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/decimal-util.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/logging-support.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/minidump.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/os-info.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/test-info.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/zip-util.h"

#include "common/names.h"

using namespace impala;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DECLARE_string(heap_profile_dir);
DECLARE_string(hostname);
// TODO: rename this to be more generic when we have a good CM release to do so.
DECLARE_int32(logbufsecs);
DECLARE_int32(max_log_files);
DECLARE_int32(max_minidumps);
DECLARE_string(redaction_rules_file);
DECLARE_string(reserved_words_version);
DECLARE_bool(symbolize_stacktrace);

DEFINE_int32(max_audit_event_log_files, 0, "Maximum number of audit event log files "
    "to retain. The most recent audit event log files are retained. If set to 0, "
    "all audit event log files are retained.");

DEFINE_int32(memory_maintenance_sleep_time_ms, 10000, "Sleep time in milliseconds "
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

using std::string;

// Log maintenance thread that runs periodically. It flushes glog every logbufsecs sec.
// glog only automatically flushes the log file if logbufsecs has passed since the
// previous flush when a new log is written. That means that on a quiet system, logs
// will be buffered indefinitely. It also rotates log files.
static unique_ptr<impala::Thread> log_maintenance_thread;

// Memory Maintenance thread that runs periodically to free up memory. It does the
// following things every memory_maintenance_sleep_time_ms secs:
// 1) Releases BufferPool memory that is not currently in use.
// 2) Frees excess memory that TCMalloc has left in its pageheap.
static unique_ptr<impala::Thread> memory_maintenance_thread;

// Shutdown signal handler thread that calls sigwait() on IMPALA_SHUTDOWN_SIGNAL and
// initiates a graceful shutdown with a virtually unlimited deadline (one year).
static unique_ptr<impala::Thread> shutdown_signal_handler_thread;

// A pause monitor thread to monitor process pauses in impala daemons. The thread sleeps
// for a short interval of time (THREAD_SLEEP_TIME_MS), wakes up and calculates the actual
// time slept. If that exceeds PAUSE_WARN_THRESHOLD_MS, a warning is logged.
static unique_ptr<impala::Thread> pause_monitor;

// Thread only used in backend tests to implement a test timeout.
static unique_ptr<impala::Thread> be_timeout_thread;

// Timeout after 2 hours - backend tests should generally run in minutes or tens of
// minutes at worst.
#if defined(UNDEFINED_SANITIZER_FULL)
static const int64_t BE_TEST_TIMEOUT_S = 60L * 60L * 4L;
#else
static const int64_t BE_TEST_TIMEOUT_S = 60L * 60L * 2L;
#endif

#ifdef CODE_COVERAGE_ENABLED
extern "C" { void __gcov_flush(); }
#endif

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
    // Check for minidump rotation in every interval of the maintenance thread. This is
    // necessary since an arbitrary number of minidumps can be written by sending SIGUSR1
    // to the process.
    impala::CheckAndRotateMinidumps(FLAGS_max_minidumps);
  }
}

[[noreturn]] static void MemoryMaintenanceThread() {
  while (true) {
    SleepForMs(FLAGS_memory_maintenance_sleep_time_ms);
    impala::ExecEnv* env = impala::ExecEnv::GetInstance();
    // ExecEnv may not have been created yet or this may be the catalogd or statestored,
    // which don't have ExecEnvs.
    if (env != nullptr) {
      BufferPool* buffer_pool = env->buffer_pool();
      if (buffer_pool != nullptr) buffer_pool->Maintenance();

      // The process limit as measured by our trackers may get out of sync with the
      // process usage if memory is allocated or freed without updating a MemTracker.
      // The metric is refreshed whenever memory is consumed or released via a MemTracker,
      // so on a system with queries executing it will be refreshed frequently. However
      // if the system is idle, we need to refresh the tracker occasionally since
      // untracked memory may be allocated or freed, e.g. by background threads.
      if (env->process_mem_tracker() != nullptr) {
        env->process_mem_tracker()->RefreshConsumptionFromMetric();
      }
    }
    // Periodically refresh values of the aggregate memory metrics to ensure they are
    // somewhat up-to-date.
    AggregateMemoryMetrics::Refresh();
  }
}

[[noreturn]] static void ImpalaShutdownSignalHandler() {
  sigset_t signals;
  CHECK_EQ(0, sigemptyset(&signals));
  CHECK_EQ(0, sigaddset(&signals, IMPALA_SHUTDOWN_SIGNAL));
  DCHECK(ExecEnv::GetInstance() != nullptr);
  DCHECK(ExecEnv::GetInstance()->impala_server() != nullptr);
  ImpalaServer* impala_server = ExecEnv::GetInstance()->impala_server();
  while (true) {
    int signal;
    int err = sigwait(&signals, &signal);
    CHECK(err == 0) << "sigwait(): " << GetStrErrMsg(err) << ": " << err;
    CHECK_EQ(IMPALA_SHUTDOWN_SIGNAL, signal);
    ShutdownStatusPB shutdown_status;
    const int ONE_YEAR_IN_SECONDS = 365 * 24 * 60 * 60;
    Status status = impala_server->StartShutdown(ONE_YEAR_IN_SECONDS, &shutdown_status);
    if (!status.ok()) {
      LOG(ERROR) << "Shutdown signal received but unable to initiate shutdown. Status: "
                 << status.GetDetail();
      continue;
    }
    LOG(INFO) << "Shutdown signal received. Current Shutdown Status: "
              << ImpalaServer::ShutdownStatusToString(shutdown_status);
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

// Signal handler for SIGTERM, that prints the message before doing an exit.
[[noreturn]] static void HandleSigTerm(int signum, siginfo_t* info, void* context) {
  const char* msg = "Caught signal: SIGTERM. Daemon will exit.\n";
  sys_write(STDOUT_FILENO, msg, strlen(msg));
#ifdef CODE_COVERAGE_ENABLED
  // On some systems __gcov_flush() only flushes a small subset of the coverage data.
  // If you run into this problem, there is a workaround that you can use at your own
  // risk: instead of calling __gcov_flush() and _exit(0) try to invoke exit(0) (no
  // underscore). You should only do this in your dev environment.
  __gcov_flush();
#endif
  // _exit() is async signal safe and is equivalent to the behaviour of the default
  // SIGTERM handler. exit() can run arbitrary code and is *not* safe to use here.
  _exit(0);
}

// Helper method that checks the return value of a syscall passed through
// 'syscall_ret_val'. If it indicates an error, it writes an error message to stderr along
// with the error string fetched via errno and calls exit().
void AbortIfError(const int syscall_ret_val, const string& msg) {
  if (syscall_ret_val == 0) return;
  cerr << Substitute("$0 Error: $1", msg, GetStrErrMsg());
  exit(1);
}

// Blocks the IMPALA_SHUTDOWN_SIGNAL signal. Should be called by the process before
// spawning any other threads to make sure it gets blocked in all threads and will only be
// caught by the thread waiting on it.
void BlockImpalaShutdownSignal() {
  const string error_msg = "Failed to block IMPALA_SHUTDOWN_SIGNAL for all threads.";
  sigset_t signals;
  AbortIfError(sigemptyset(&signals), error_msg);
  AbortIfError(sigaddset(&signals, IMPALA_SHUTDOWN_SIGNAL), error_msg);
  AbortIfError(pthread_sigmask(SIG_BLOCK, &signals, nullptr), error_msg);
}

void impala::InitCommonRuntime(int argc, char** argv, bool init_jvm,
    TestInfo::Mode test_mode) {
  srand(time(NULL));
  BlockImpalaShutdownSignal();

  CpuInfo::Init();
  DiskInfo::Init();
  MemInfo::Init();
  OsInfo::Init();
  TestInfo::Init(test_mode);

  // Verify CPU meets the minimum requirements before calling InitGoogleLoggingSafe()
  // which might use SSSE3 instructions (see IMPALA-160).
  CpuInfo::VerifyCpuRequirements();

  // Set the default hostname. The user can override this with the hostname flag.
  ABORT_IF_ERROR(GetHostname(&FLAGS_hostname));

#ifdef NDEBUG
  // Symbolize stacktraces by default in debug mode.
  FLAGS_symbolize_stacktrace = false;
# else
  FLAGS_symbolize_stacktrace = true;
#endif
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
  if (FLAGS_read_size < READ_SIZE_MIN_VALUE) {
    CLEAN_EXIT_WITH_ERROR(Substitute("read_size can not be lower than $0",
        READ_SIZE_MIN_VALUE));
  }
  if (FLAGS_reserved_words_version != "2.11.0" && FLAGS_reserved_words_version != "3.0.0")
  {
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid flag reserved_words_version. The value must"
        " be one of [\"2.11.0\", \"3.0.0\"], while the provided value is $0.",
        FLAGS_reserved_words_version));
  }
  impala::InitGoogleLoggingSafe(argv[0]);
  // Breakpad needs flags and logging to initialize.
  ABORT_IF_ERROR(RegisterMinidump(argv[0]));
#ifndef THREAD_SANITIZER
  AtomicOps_x86CPUFeaturesInit();
#endif
  impala::InitThreading();
  impala::datetime_parse_util::InitParseCtx();
  impala::SeedOpenSSLRNG();
  ABORT_IF_ERROR(impala::InitAuth(argv[0]));

  // Initialize maintenance_thread after InitGoogleLoggingSafe and InitThreading.
  Status thread_spawn_status = Thread::Create("common", "log-maintenance-thread",
      &LogMaintenanceThread, &log_maintenance_thread);
  if (!thread_spawn_status.ok()) CLEAN_EXIT_WITH_ERROR(thread_spawn_status.GetDetail());

  thread_spawn_status =
      Thread::Create("common", "pause-monitor", &PauseMonitorLoop, &pause_monitor);
  if (!thread_spawn_status.ok()) CLEAN_EXIT_WITH_ERROR(thread_spawn_status.GetDetail());

  // Implement timeout for backend tests.
  if (impala::TestInfo::is_be_test()) {
    thread_spawn_status = Thread::Create("common", "be-test-timeout-thread",
        []() {
          SleepForMs(BE_TEST_TIMEOUT_S * 1000L);
          LOG(FATAL) << "Backend test timed out after " << BE_TEST_TIMEOUT_S << "s";
        },
        &be_timeout_thread);
    if (!thread_spawn_status.ok()) CLEAN_EXIT_WITH_ERROR(thread_spawn_status.GetDetail());
  }

  PeriodicCounterUpdater::Init();

  LOG(INFO) << impala::GetVersionString();
  LOG(INFO) << "Using hostname: " << FLAGS_hostname;
  impala::LogCommandLineFlags();

  // When a process calls send(2) on a socket closed on the other end, linux generates
  // SIGPIPE. MSG_NOSIGNAL can be passed to send(2) to disable it, which thrift does. But
  // OpenSSL doesn't have place for this parameter so the signal must be disabled
  // manually.
  signal(SIGPIPE, SIG_IGN);
  InitThriftLogging();

  LOG(INFO) << CpuInfo::DebugString();
  LOG(INFO) << DiskInfo::DebugString();
  LOG(INFO) << MemInfo::DebugString();
  LOG(INFO) << OsInfo::DebugString();
  LOG(INFO) << CGroupUtil::DebugString();
  LOG(INFO) << "Process ID: " << getpid();
  LOG(INFO) << "Default AES cipher mode for spill-to-disk: "
            << EncryptionKey::ModeToString(EncryptionKey::GetSupportedDefaultMode());

  // Required for the FE's Catalog
  ABORT_IF_ERROR(impala::LibCache::Init());
  Status fs_cache_init_status = impala::HdfsFsCache::Init();
  if (!fs_cache_init_status.ok()) CLEAN_EXIT_WITH_ERROR(fs_cache_init_status.GetDetail());

  if (init_jvm) {
    ABORT_IF_ERROR(JniUtil::Init());
    InitJvmLoggingSupport();
    ABORT_IF_ERROR(JniUtil::InitJvmPauseMonitor());
    ZipUtil::InitJvm();
  }

  if (argc == -1) {
    // Should not be called. We need BuiltinsInit() so the builtin symbols are
    // not stripped.
    DCHECK(false);
    ScalarExprEvaluator::InitBuiltinsDummy();
  }

  if (impala::KuduIsAvailable()) impala::InitKuduLogging();

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_dir.c_str());
  }
#endif

  // Signal handler for handling the SIGTERM. We want to log a message when catalogd or
  // impalad or statestored is being shutdown using a SIGTERM.
  struct sigaction action;
  memset(&action, 0, sizeof(struct sigaction));
  action.sa_sigaction = &HandleSigTerm;
  action.sa_flags = SA_SIGINFO;
  if (sigaction(SIGTERM, &action, nullptr) == -1) {
    stringstream error_msg;
    error_msg << "Failed to register action for SIGTERM: " << GetStrErrMsg();
    CLEAN_EXIT_WITH_ERROR(error_msg.str());
  }
}

Status impala::StartMemoryMaintenanceThread() {
  DCHECK(AggregateMemoryMetrics::TOTAL_USED != nullptr) << "Mem metrics not registered.";
  return Thread::Create("common", "memory-maintenance-thread",
      &MemoryMaintenanceThread, &memory_maintenance_thread);
}

Status impala::StartImpalaShutdownSignalHandlerThread() {
  return Thread::Create("common", "shutdown-signal-handler", &ImpalaShutdownSignalHandler,
      &shutdown_signal_handler_thread);
}

#if defined(ADDRESS_SANITIZER)
// Default ASAN_OPTIONS. Override by setting environment variable $ASAN_OPTIONS.
extern "C" const char *__asan_default_options() {
  // IMPALA-2746: backend tests don't pass with leak sanitizer enabled.
  return "handle_segv=0 detect_leaks=0 allocator_may_return_null=1";
}
#endif

#if defined(THREAD_SANITIZER)
// Default TSAN_OPTIONS. Override by setting environment variable $TSAN_OPTIONS.
extern "C" const char *__tsan_default_options() {
  // Note that backend test should re-configure to halt_on_error=1
  return "halt_on_error=0 history_size=7";
}
#endif

// Default UBSAN_OPTIONS. Override by setting environment variable $UBSAN_OPTIONS.
#if defined(UNDEFINED_SANITIZER)
extern "C" const char *__ubsan_default_options() {
  return "print_stacktrace=1 suppressions=" UNDEFINED_SANITIZER_SUPPRESSIONS;
}
#endif
