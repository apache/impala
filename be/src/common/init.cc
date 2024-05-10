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
#include <regex>
#include <boost/filesystem.hpp>
#include <gperftools/heap-profiler.h>
#include <third_party/lss/linux_syscall_support.h>

#include "common/global-flags.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/kudu/kudu-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/string-functions.h"
#include "exprs/timezone_db.h"
#include "gutil/atomicops.h"
#include "gutil/strings/substitute.h"
#include "rpc/authentication.h"
#include "rpc/thrift-util.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "service/impala-server.h"
#include "util/cgroup-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/jni-util.h"
#include "util/logging-support.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/minidump.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/os-info.h"
#include "util/os-util.h"
#include "util/parse-util.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/test-info.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/zip-util.h"

#include "common/names.h"

using namespace impala;
namespace filesystem = boost::filesystem;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DECLARE_string(heap_profile_dir);
DECLARE_string(hostname);
DECLARE_bool(use_resolved_hostname);
// TODO: rename this to be more generic when we have a good CM release to do so.
DECLARE_int32(logbufsecs);
DECLARE_int32(max_log_files);
DECLARE_int32(max_minidumps);
DECLARE_string(redaction_rules_file);
DECLARE_bool(redirect_stdout_stderr);
DECLARE_string(re2_mem_limit);
DECLARE_string(reserved_words_version);
DECLARE_bool(symbolize_stacktrace);
DECLARE_string(debug_actions);
DECLARE_int64(thrift_rpc_max_message_size);
DECLARE_int64(thrift_external_rpc_max_message_size);

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

DEFINE_bool(jvm_automatic_add_opens, true,
    "Adds necessary --add-opens options for core Java modules necessary to correctly "
    "calculate catalog metadata cache object sizes.");

DEFINE_string_hidden(java_weigher, "auto",
    "Choose between 'jamm' (com.github.jbellis:jamm) and 'sizeof' (org.ehcache:sizeof) "
    "weighers for determining catalog metadata cache entry size. 'auto' uses 'sizeof' "
    "for Java 8 - 11, and 'jamm' for Java 15+.");

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

// Fault injection thread that is spawned if FLAGS_debug_actions has label
// 'LOG_MAINTENANCE_STDERR'.
static unique_ptr<impala::Thread> log_fault_inject_thread;

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

[[noreturn]] static void LogFaultInjectionThread() {
  const int64_t sleep_duration = 1;
  while (true) {
    sleep(sleep_duration);

    const int64_t now = MonotonicMillis();
    Status status = DebugAction(FLAGS_debug_actions, "LOG_MAINTENANCE_STDERR");
    if (!status.ok()) {
      // Fault injection activated. Print the error message several times to cerr.
      for (int i = 0; i < 128; i++) {
        std::cerr << now << " " << i << " "
                  << " LOG_MAINTENANCE_STDERR " << status.msg().msg() << endl;
      }

      // Check that impalad can always find INFO and ERROR log path.
      DCHECK(impala::HasLog(google::INFO));
      DCHECK(impala::HasLog(google::ERROR));
    }
  }
}

[[noreturn]] static void LogMaintenanceThread() {
  int64_t last_flush = MonotonicMillis();
  const int64_t sleep_duration = std::min(1, FLAGS_logbufsecs);
  while (true) {
    sleep(sleep_duration);

    const int64_t now = MonotonicMillis();
    bool max_log_file_exceeded = RedirectStdoutStderr() && impala::CheckLogSize(false);
    if ((now - last_flush) / 1000 < FLAGS_logbufsecs && !max_log_file_exceeded) {
      continue;
    }

    google::FlushLogFiles(google::GLOG_INFO);

    // Check log size again and force log rotation this time if they still big after
    // FlushLogFiles.
    if (max_log_file_exceeded && impala::CheckLogSize(true)) impala::ForceRotateLog();

    // No need to rotate log files in tests.
    if (impala::TestInfo::is_test()) continue;
    // Reattach stdout and stderr if necessary.
    if (impala::RedirectStdoutStderr()) impala::AttachStdoutStderr();
    // Check for log rotation in every interval of the maintenance thread
    impala::CheckAndRotateLogFiles(FLAGS_max_log_files);
    // Check for minidump rotation in every interval of the maintenance thread. This is
    // necessary since an arbitrary number of minidumps can be written by sending SIGUSR1
    // to the process.
    impala::CheckAndRotateMinidumps(FLAGS_max_minidumps);

    // update last_flush.
    last_flush = MonotonicMillis();
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
    Status status = impala_server->StartShutdown(-1, &shutdown_status);
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

// Returns Java major version, such as 8, 11, or 17.
static int GetJavaMajorVersion() {
  string cmd = "java";
  const char* java_home = getenv("JAVA_HOME");
  if (java_home != NULL) {
    cmd = (filesystem::path(java_home) / "bin" / "java").string();
  }
  cmd += " -version 2>&1";
  string msg;
  if (!RunShellProcess(cmd, &msg, false, {"JAVA_TOOL_OPTIONS"})) {
    LOG(INFO) << Substitute("Unable to determine Java version (default to 8): $0", msg);
    return 8;
  }

  // Find a version string in the first line.
  string first_line;
  std::getline(istringstream(msg), first_line);
  // Need to allow for a wide variety of formats for different JDK implementations.
  // Example: openjdk version "11.0.19" 2023-04-18
  std::regex java_version_pattern("\"([0-9]{1,3})\\.[0-9]+\\.[0-9]+[^\"]*\"");
  std::smatch matches;
  if (!std::regex_search(first_line, matches, java_version_pattern)) {
    LOG(INFO) << Substitute("Unable to determine Java version (default to 8): $0", msg);
    return 8;
  }
  DCHECK_EQ(matches.size(), 2);
  return std::stoi(matches.str(1));
}

// Append the javaagent arg to JAVA_TOOL_OPTIONS to load jamm.
static Status JavaAddJammAgent() {
  stringstream val_out;
  char* current_val_c = getenv("JAVA_TOOL_OPTIONS");
  if (current_val_c != NULL) {
    val_out << current_val_c << " ";
  }

  istringstream classpath {getenv("CLASSPATH")};
  string jamm_path, test_path;
  while (getline(classpath, test_path, ':')) {
    jamm_path = FileSystemUtil::FindFileInPath(test_path, "jamm-.*.jar");
    if (!jamm_path.empty()) break;
  }
  if (jamm_path.empty()) {
    return Status("Could not find jamm-*.jar in Java CLASSPATH");
  }
  val_out << "-javaagent:" << jamm_path;

  if (setenv("JAVA_TOOL_OPTIONS", val_out.str().c_str(), 1) < 0) {
    return Status(Substitute("Could not update JAVA_TOOL_OPTIONS: $0", GetStrErrMsg()));
  }
  return Status::OK();
}

// Append add-opens args to JAVA_TOOL_OPTIONS for ehcache and jamm.
static Status JavaAddOpens(bool useSizeOf) {
  if (!FLAGS_jvm_automatic_add_opens) return Status::OK();

  stringstream val_out;
  char* current_val_c = getenv("JAVA_TOOL_OPTIONS");
  if (current_val_c != NULL) {
    val_out << current_val_c;
  }

  for (const string& param : {
    // Needed for jamm and ehcache (jamm needs it to access lambdas)
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util.regex=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  }) {
    val_out << " " << param;
  }

  if (useSizeOf) {
    for (const string& param : {
      // Only needed for ehcache
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.module=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.ref=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio.charset=ALL-UNNAMED",
      "--add-opens=java.base/java.nio.file.attribute=ALL-UNNAMED",
      "--add-opens=java.base/java.security=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.jar=ALL-UNNAMED",
      "--add-opens=java.base/java.util.zip=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.math=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.module=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.perf=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.platform=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.platform.cgroupv1=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.util.jar=ALL-UNNAMED",
      "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.fs=ALL-UNNAMED",
      "--add-opens=jdk.dynalink/jdk.dynalink.beans=ALL-UNNAMED",
      "--add-opens=jdk.dynalink/jdk.dynalink.linker.support=ALL-UNNAMED",
      "--add-opens=jdk.dynalink/jdk.dynalink.linker=ALL-UNNAMED",
      "--add-opens=jdk.dynalink/jdk.dynalink.support=ALL-UNNAMED",
      "--add-opens=jdk.dynalink/jdk.dynalink=ALL-UNNAMED",
      "--add-opens=jdk.management.jfr/jdk.management.jfr=ALL-UNNAMED",
      "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED"
    }) {
      val_out << " " << param;
    }
  }

  if (setenv("JAVA_TOOL_OPTIONS", val_out.str().c_str(), 1) < 0) {
    return Status(Substitute("Could not update JAVA_TOOL_OPTIONS: $0", GetStrErrMsg()));
  }
  return Status::OK();
}

static Status InitializeJavaWeigher() {
  // This is set up so the default if things go wrong is to continue using ehcache.sizeof.
  int version = GetJavaMajorVersion();
  if (FLAGS_java_weigher == "auto") {
    // Update for backend-gflag-util.cc setting use_jamm_weigher.
    FLAGS_java_weigher = (version >= 15) ? "jamm" : "sizeof";
  }
  LOG(INFO) << "Using Java weigher " << FLAGS_java_weigher;

  if (FLAGS_java_weigher == "jamm") {
    RETURN_IF_ERROR(JavaAddJammAgent());
  }
  if (version >= 9) {
    // add-opens is only supported in Java 9+.
    RETURN_IF_ERROR(JavaAddOpens(FLAGS_java_weigher != "jamm"));
  }
  return Status::OK();
}

static Status JavaSetProcessName(string name) {
  string current_val;
  char* current_val_c = getenv("JAVA_TOOL_OPTIONS");
  if (current_val_c != NULL) {
    current_val = current_val_c;
  }

  if (!current_val.empty() && current_val.find("-Dsun.java.command") != string::npos) {
    LOG(WARNING) << "Overriding sun.java.command in JAVA_TOOL_OPTIONS to " << name;
  }

  stringstream val_out;
  if (!current_val.empty()) {
    val_out << current_val << " ";
  }
  // Set sun.java.command so jps reports the name correctly, and ThreadNameAnnotator can
  // use the process name for the main thread (and correctly restore the process name).
  val_out << "-Dsun.java.command=" << name;

  if (setenv("JAVA_TOOL_OPTIONS", val_out.str().c_str(), 1) < 0) {
    return Status(Substitute("Could not update JAVA_TOOL_OPTIONS: $0", GetStrErrMsg()));
  }
  return Status::OK();
}

void impala::InitCommonRuntime(int argc, char** argv, bool init_jvm,
    TestInfo::Mode test_mode, bool external_fe) {
  srand(time(NULL));
  BlockImpalaShutdownSignal();

  CpuInfo::Init();
  DiskInfo::Init();
  MemInfo::Init();
  OsInfo::Init();
  TestInfo::Init(test_mode);

  // Set the default hostname. The user can override this with the hostname flag.
  ABORT_IF_ERROR(GetHostname(&FLAGS_hostname));

#ifdef NDEBUG
  // Symbolize stacktraces by default in debug mode.
  FLAGS_symbolize_stacktrace = false;
# else
  FLAGS_symbolize_stacktrace = true;
#endif

  if (external_fe) {
    // Change defaults for flags when loaded as part of external frontend.
    // Write logs to stderr by default (otherwise logs get written to
    // FeSupport.INFO/ERROR).
    FLAGS_logtostderr = true;
    // Do not redirct stdout/stderr by default.
    FLAGS_redirect_stdout_stderr = false;
  }

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

  bool is_percent = false; // not used
  int64_t re2_mem_limit = ParseUtil::ParseMemSpec(FLAGS_re2_mem_limit, &is_percent, 0);
  if (re2_mem_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid mem limit for re2's regex engine: $0", FLAGS_re2_mem_limit));
  } else {
    StringFunctions::SetRE2MemLimit(re2_mem_limit);
  }

  if (FLAGS_reserved_words_version != "2.11.0" && FLAGS_reserved_words_version != "3.0.0")
  {
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid flag reserved_words_version. The value must"
        " be one of [\"2.11.0\", \"3.0.0\"], while the provided value is $0.",
        FLAGS_reserved_words_version));
  }

  // Enforce a minimum value for thrift_max_message_size, as configuring the limit to
  // a small value is very unlikely to work.
  if (!impala::TestInfo::is_test() && FLAGS_thrift_rpc_max_message_size > 0
      && FLAGS_thrift_rpc_max_message_size < ThriftDefaultMaxMessageSize()) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid $0: $1 is less than the minimum value of $2.",
            "thrift_rpc_max_message_size", FLAGS_thrift_rpc_max_message_size,
            ThriftDefaultMaxMessageSize()));
  }

  // Enforce a minimum value for thrift_external_max_message_size, as configuring the
  // limit to a small value is very unlikely to work.
  if (!impala::TestInfo::is_test() && FLAGS_thrift_external_rpc_max_message_size > 0
      && FLAGS_thrift_external_rpc_max_message_size < ThriftDefaultMaxMessageSize()) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid $0: $1 is less than the minimum value of $2.",
            "thrift_external_rpc_max_message_size",
            FLAGS_thrift_external_rpc_max_message_size, ThriftDefaultMaxMessageSize()));
  }

  impala::InitGoogleLoggingSafe(argv[0]);
  // Breakpad needs flags and logging to initialize.
  if (!external_fe) {
    ABORT_IF_ERROR(RegisterMinidump(argv[0]));
  }
  impala::InitThreading();
  impala::datetime_parse_util::SimpleDateFormatTokenizer::InitCtx();
  impala::SeedOpenSSLRNG();
  ABORT_IF_ERROR(impala::InitAuth(argv[0]));

  // Initialize maintenance_thread after InitGoogleLoggingSafe and InitThreading.
  Status thread_spawn_status = Thread::Create("common", "log-maintenance-thread",
      &LogMaintenanceThread, &log_maintenance_thread);
  if (!thread_spawn_status.ok()) CLEAN_EXIT_WITH_ERROR(thread_spawn_status.GetDetail());

  thread_spawn_status =
      Thread::Create("common", "pause-monitor", &PauseMonitorLoop, &pause_monitor);
  if (!thread_spawn_status.ok()) CLEAN_EXIT_WITH_ERROR(thread_spawn_status.GetDetail());

  // Initialize log fault injection if such debug action exist.
  if (strstr(FLAGS_debug_actions.c_str(), "LOG_MAINTENANCE_STDERR") != NULL) {
    thread_spawn_status = Thread::Create("common", "log-fault-inject-thread",
        &LogFaultInjectionThread, &log_fault_inject_thread);
    if (!thread_spawn_status.ok()) CLEAN_EXIT_WITH_ERROR(thread_spawn_status.GetDetail());
  }

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
  LOG(INFO) << "Using locale: " << std::locale("").name();
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

  // After initializing logging and printing the machine information, verify the
  // minimal CPU requirements and exit if they are not met.
  Status cpu_requirement_status = CpuInfo::EnforceCpuRequirements();
  if (!cpu_requirement_status.ok()) {
    CLEAN_EXIT_WITH_ERROR(cpu_requirement_status.GetDetail());
  }

  if (FLAGS_use_resolved_hostname) {
    IpAddr ip_address;
    Status status = HostnameToIpAddr(FLAGS_hostname, &ip_address);
    if (!status.ok()) CLEAN_EXIT_WITH_ERROR(status.GetDetail());
    LOG(INFO) << Substitute("Resolved hostname $0 to $1", FLAGS_hostname, ip_address);
    FLAGS_hostname = ip_address;
  }

  // Required for the FE's Catalog
  ABORT_IF_ERROR(impala::LibCache::Init(external_fe));
  Status fs_cache_init_status = impala::HdfsFsCache::Init();
  if (!fs_cache_init_status.ok()) CLEAN_EXIT_WITH_ERROR(fs_cache_init_status.GetDetail());

  if (init_jvm) {
    if (!external_fe) {
      ABORT_IF_ERROR(InitializeJavaWeigher());
      ABORT_IF_ERROR(JavaSetProcessName(filesystem::path(argv[0]).filename().string()));
      JniUtil::InitLibhdfs();
    }
    ABORT_IF_ERROR(JniUtil::Init());
    InitJvmLoggingSupport();
    if (!external_fe) {
      ABORT_IF_ERROR(JniUtil::InitJvmPauseMonitor());
    }
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

  if (external_fe || test_mode == TestInfo::FE_TEST) {
    // Explicitly load the timezone database for external FEs and FE tests.
    // Impala daemons load it through ImpaladMain
    ABORT_IF_ERROR(TimezoneDatabase::Initialize());
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
extern "C" const char* __tsan_default_options() {
  // Default TSAN_OPTIONS. Override by setting environment variable $TSAN_OPTIONS.
  // TSAN and Java don't play nicely together because JVM code is not instrumented with
  // TSAN. TSAN requires all libs to be compiled with '-fsanitize=thread' (see
  // https://github.com/google/sanitizers/wiki/ThreadSanitizerCppManual#non-instrumented-code),
  // which is not currently possible for Java code. See
  // https://wiki.openjdk.java.net/display/tsan/Main and JDK-8208520 for efforts to get
  // TSAN to run against Java code. The flag ignore_noninstrumented_modules tells TSAN to
  // ignore all interceptors called from any non-instrumented libraries (e.g. Java).
  return "ignore_noninstrumented_modules="
#if defined(THREAD_SANITIZER_FULL)
         "0 "
#else
         "1 "
#endif
         "halt_on_error=1 history_size=7 allocator_may_return_null=1 "
         "suppressions=" THREAD_SANITIZER_SUPPRESSIONS;
}
#endif

// Default UBSAN_OPTIONS. Override by setting environment variable $UBSAN_OPTIONS.
#if defined(UNDEFINED_SANITIZER)
extern "C" const char *__ubsan_default_options() {
  return "print_stacktrace=1 suppressions=" UNDEFINED_SANITIZER_SUPPRESSIONS;
}
#endif
