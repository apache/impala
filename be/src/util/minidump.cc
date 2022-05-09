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

#include "util/minidump.h"

#include <assert.h>
#include <boost/filesystem.hpp>
#include <client/linux/handler/exception_handler.h>
#include <common/linux/linux_libc_support.h>
#include <google_breakpad/common/minidump_format.h>
#include <third_party/lss/linux_syscall_support.h>
#include <ctime>
#include <fstream>
#include <glob.h>
#include <iomanip>
#include <map>
#include <signal.h>
#include <sstream>

#include "common/logging.h"
#include "common/thread-debug-info.h"
#include "common/version.h"
#include "util/debug-util.h"
#include "util/filesystem-util.h"
#include "util/time.h"

using namespace std;

using boost::filesystem::create_directories;
using boost::filesystem::is_regular_file;
using boost::filesystem::path;
using boost::filesystem::remove;

DECLARE_string(log_dir);
DECLARE_bool(enable_minidumps);
DECLARE_string(minidump_path);
DECLARE_int32(max_minidumps);
DECLARE_int32(minidump_size_limit_hint_kb);

namespace impala {

/// Breakpad ExceptionHandler. It registers its own signal handlers to write minidump
/// files during process crashes, but also can be used to write minidumps directly.
static google_breakpad::ExceptionHandler* minidump_exception_handler = NULL;

/// Test helper. True if minidumps should be enabled.
static bool minidumps_enabled = true;

/// Called by the exception handler before minidump is produced. Minidump is only written
/// if this returns true.
static bool FilterCallback(void* context) {
  return minidumps_enabled;
}

static void write_dump_threadinfo(int fd, ThreadDebugInfo* thread_info) {
  constexpr char thread_msg[] = "Minidump in thread ";
  // pid_t is signed, but valid process IDs must always be positive.
  const int64_t thread_id = thread_info->GetSystemThreadId();
  // 20 characters needed for UINT64_MAX.
  char thread_id_str[20];
  const unsigned int thread_id_len = my_uint_len(thread_id);
  my_uitos(thread_id_str, thread_id, thread_id_len);
  const char* thread_name = thread_info->GetThreadName();

  constexpr char query_msg[] = " running query ";
  const TUniqueId& query_id = thread_info->GetQueryId();
  char query_id_str[TUniqueIdBufferSize];
  PrintIdCompromised(query_id, query_id_str);

  constexpr char instance_msg[] = ", fragment instance ";
  const TUniqueId& instance_id = thread_info->GetInstanceId();
  // Format TUniqueId according to PrintId from util/debug-util.h
  char instance_id_str[TUniqueIdBufferSize];
  PrintIdCompromised(instance_id, instance_id_str);

  // Example:
  // > Minidump in thread [1790536]async-exec-thread running query 1a47cc1e2df94cb4:
  //   88dfa08200000000, fragment instance 0000000000000000:0000000000000000
  sys_write(fd, thread_msg, sizeof(thread_msg) / sizeof(thread_msg[0]) - 1);
  sys_write(fd, "[", 1);
  sys_write(fd, thread_id_str, thread_id_len);
  sys_write(fd, "]", 1);
  sys_write(fd, thread_name, my_strlen(thread_name));
  sys_write(fd, query_msg, sizeof(query_msg) / sizeof(query_msg[0]) - 1);
  sys_write(fd, query_id_str, TUniqueIdBufferSize);
  sys_write(fd, instance_msg, sizeof(instance_msg) / sizeof(instance_msg[0]) - 1);
  sys_write(fd, instance_id_str, TUniqueIdBufferSize);
  sys_write(fd, "\n", 1);
}

static void write_dump_path(int fd, const char* path) {
  // We use the linux syscall support methods from chromium here as per the
  // recommendation of the breakpad docs to avoid calling into other shared libraries.
  const char msg[] = "Wrote minidump to ";
  sys_write(fd, msg, sizeof(msg) / sizeof(msg[0]) - 1);
    // We use breakpad's reimplementation of strlen to avoid calling into libc.
  sys_write(fd, path, my_strlen(path));
  sys_write(fd, "\n", 1);
}

/// Callback for breakpad. It is called by breakpad whenever a minidump file has been
/// written and should not be called directly. It logs the event before breakpad crashes
/// the process. Due to the process being in a failed state we write to stdout/stderr and
/// let the surrounding redirection make sure the output gets logged. The calls might
/// still fail in unknown scenarios as the process is in a broken state. However we don't
/// rely on them as the minidump file has been written already.
bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor, void* context,
    bool succeeded) {
  // See if a file was written successfully.
  if (succeeded) {
    // Write to stdout/stderr, which will usually be captured in the INFO/ERROR log.
    ThreadDebugInfo* thread_info = GetThreadDebugInfo();
    if (thread_info != nullptr) {
      write_dump_threadinfo(STDOUT_FILENO, thread_info);
      write_dump_threadinfo(STDERR_FILENO, thread_info);
    } else {
      const char msg[] = "Minidump with no thread info available.\n";
      const int msg_len = sizeof(msg) / sizeof(msg[0]) - 1;
      sys_write(STDOUT_FILENO, msg, msg_len);
      sys_write(STDERR_FILENO, msg, msg_len);
    }

    const char* path = descriptor.path();
    write_dump_path(STDOUT_FILENO, path);
    write_dump_path(STDERR_FILENO, path);
  }
  // Return the value received in the call as described in the minidump documentation. If
  // this values is true, then no other handlers will be called. Breakpad will still crash
  // the process.
  return succeeded;
}

/// Signal handler to write a minidump file outside of crashes.
static void HandleSignal(int signum, siginfo_t* info, void* context) {
  const char* msg = "Caught signal: SIGUSR1\n";
  sys_write(STDOUT_FILENO, msg, strlen(msg));
  minidump_exception_handler->WriteMinidump(FLAGS_minidump_path, DumpCallback, NULL);
}

/// Register our signal handler to write minidumps on SIGUSR1. Will make us ignore the
/// signal if 'minidumps_enabled' is false.
static void SetupSigUSR1Handler(bool minidumps_enabled) {
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  if (minidumps_enabled) {
    DCHECK(minidump_exception_handler != NULL);
    sig_action.sa_sigaction = &HandleSignal;
    sig_action.sa_flags = SA_SIGINFO;
  } else {
    sig_action.sa_handler = SIG_IGN;
  }
  if (sigaction(SIGUSR1, &sig_action, nullptr) == -1) {
    stringstream error_msg;
    error_msg << "Failed to register action for SIGUSR1: " << GetStrErrMsg();
    CLEAN_EXIT_WITH_ERROR(error_msg.str());
  }
}

void CheckAndRotateMinidumps(int max_minidumps) {
  // Disable rotation if 0 or wrong input
  if (max_minidumps <= 0) return;

  // Search for minidumps. There could be multiple minidumps for a single second.
  multimap<int, path> timestamp_to_path;
  // Minidump filenames are created by breakpad in the following format, for example:
  // 7b57915b-ee6a-dbc5-21e59491-5c60a2cf.dmp.
  string pattern = FLAGS_minidump_path + "/*.dmp";
  glob_t result;
  glob(pattern.c_str(), GLOB_TILDE, NULL, &result);
  for (size_t i = 0; i < result.gl_pathc; ++i) {
    const path minidump_path(result.gl_pathv[i]);
    boost::system::error_code err;
    bool is_file = is_regular_file(minidump_path, err);
    // is_regular_file() calls stat() eventually, which can return errors, e.g. if the
    // file permissions prevented access or the path was wrong (see 'man 2 stat' for
    // details). In these cases we assume that the issue is out of our control and err on
    // the safe side by keeping the minidump around, hoping it will aid in debugging the
    // issue. The alternative, removing a ~2MB file, will probably not help much anyways.
    if (err) {
      LOG(WARNING) << "Failed to stat() file " << minidump_path << ": " << err;
      continue;
    }
    if (is_file) {
      ifstream stream(minidump_path.c_str(), std::ios::in | std::ios::binary);
      if (!stream.good()) {
        // Error opening file, probably broken, remove it.
        LOG(WARNING) << "Failed to open file " << minidump_path << ". Removing it.";
        stream.close();
        // Best effort, ignore error.
        remove(minidump_path.c_str(), err);
        continue;
      }
      // Read minidump header from file.
      MDRawHeader header;
      constexpr int header_size = sizeof(header);
      stream.read((char *)(&header), header_size);
      // Check for minidump header signature and version. We don't need to check for
      // endianness issues here since the file was written on the same machine. Ignore the
      // higher 16 bit of the version as per a comment in the breakpad sources.
      if (stream.gcount() != header_size || header.signature != MD_HEADER_SIGNATURE ||
          (header.version & 0x0000ffff) != MD_HEADER_VERSION) {
        LOG(WARNING) << "Found file in minidump folder, but it does not look like a "
            << "minidump file: " << minidump_path.string() << ". Removing it.";
        remove(minidump_path, err);
        if (err) {
          LOG(ERROR) << "Failed to delete file: " << minidump_path << "(error was: "
              << err << ")";
        }
        continue;
      }
      int timestamp = header.time_date_stamp;
      timestamp_to_path.emplace(timestamp, minidump_path);
    }
  }
  globfree(&result);

  // Remove oldest entries until max_minidumps are left.
  if (timestamp_to_path.size() <= max_minidumps) return;
  int files_to_delete = timestamp_to_path.size() - max_minidumps;
  DCHECK_GT(files_to_delete, 0);
  auto to_delete = timestamp_to_path.begin();
  for (int i = 0; i < files_to_delete; ++i, ++to_delete) {
    boost::system::error_code err;
    remove(to_delete->second, err);
    if (!err) {
      LOG(INFO) << "Removed old minidump file : " << to_delete->second;
    } else {
      LOG(ERROR) << "Failed to delete old minidump file: " << to_delete->second <<
        "(error was: " << err << ")";
    }
  }
}

Status RegisterMinidump(const char* cmd_line_path) {
  // Registration must only be called once.
  static bool registered = false;
  DCHECK(!registered);
  registered = true;

  if (!FLAGS_enable_minidumps || FLAGS_minidump_path.empty()) {
    SetupSigUSR1Handler(false);
    return Status::OK();
  }

  if (path(FLAGS_minidump_path).is_relative()) {
    path log_dir(FLAGS_log_dir);
    FLAGS_minidump_path = (log_dir / FLAGS_minidump_path).string();
  }

  // Add the daemon name to the path where minidumps will be written. This makes
  // identification easier and prevents name collisions between the files.
  path daemon = path(cmd_line_path).filename();
  FLAGS_minidump_path = (FLAGS_minidump_path / daemon).string();

  // Create the directory if it is not there. The minidump doesn't get written if there is
  // no directory.
  boost::system::error_code err;
  create_directories(FLAGS_minidump_path, err);
  if (err) {
    stringstream ss;
    ss << "Could not create minidump folder " << FLAGS_minidump_path << ". Error "
        << "was: " << err;
    return Status(ss.str());
  }

  google_breakpad::MinidumpDescriptor desc(FLAGS_minidump_path.c_str());

  // Limit filesize if configured.
  if (FLAGS_minidump_size_limit_hint_kb > 0) {
    size_t size_limit = 1024 * static_cast<int64_t>(FLAGS_minidump_size_limit_hint_kb);
    LOG(INFO) << "Setting minidump size limit to " << size_limit << ".";
    desc.set_size_limit(size_limit);
  }

  // Intentionally leaked. We want this to have the lifetime of the process.
  DCHECK(minidump_exception_handler == NULL);
  minidump_exception_handler = new google_breakpad::ExceptionHandler(
      desc, FilterCallback, DumpCallback, NULL, true, -1);

  // Setup signal handler for SIGUSR1.
  SetupSigUSR1Handler(true);

  return Status::OK();
}

bool EnableMinidumpsForTest(bool enabled) {
  bool old_value = minidumps_enabled;
  minidumps_enabled = enabled;
  return old_value;
}

}  // end ns impala
