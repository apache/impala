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

#include "common/logging.h"

#include <glob.h>
#include <stdio.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gutil/strings/substitute.h>

#include "common/thread-debug-info.h"
#include "kudu/util/flags.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/filesystem-util.h"
#include "util/logging-support.h"
#include "util/redactor.h"
#include "util/test-info.h"

#include "common/names.h"

DECLARE_string(redaction_rules_file);
DECLARE_string(log_filename);
DECLARE_bool(redirect_stdout_stderr);
DECLARE_int32(max_log_size);
DECLARE_bool(log_rotation_match_pid);

using boost::uuids::random_generator;
using impala::TUniqueId;

namespace {
bool logging_initialized = false;
// A 0 unique id, which indicates that one has not been set.
const TUniqueId ZERO_UNIQUE_ID;

string last_info_log_path = "";
string last_error_log_path = "";

// Prepends fragment id, when available. If unavailable, looks
// for query id. If unavailable, prepends nothing.
void PrependFragment(string* s, bool* changed) {
  impala::ThreadDebugInfo* tdi = impala::GetThreadDebugInfo();
  if (tdi != nullptr) {
    for (const auto& id : { tdi->GetInstanceId(), tdi->GetQueryId() }) {
      if (id == ZERO_UNIQUE_ID) continue;
      s->insert(0, PrintId(id) + "] ");
      if (changed != nullptr) *changed = true;
      return;
    }
  }
}

// Manipulates log messages by:
// - Applying redaction rules (if necessary)
// - Prepending fragment id (if available)
void MessageListener(string* s, bool* changed) {
  if (!FLAGS_redaction_rules_file.empty()) {
    impala::Redact(s, changed);
  }
  PrependFragment(s, changed);
}

}

mutex logging_mutex;

// Resolve 'symlink_path' into its 'canonical_path'.
// If 'symlink_path' is not a symlink, copy it to 'canonical_path'.
impala::Status ResolveLogSymlink(const string& symlink_path, string& canonical_path) {
  bool is_symbolic_link;
  string resolved_path;
  RETURN_IF_ERROR(impala::FileSystemUtil::IsSymbolicLink(
      symlink_path, &is_symbolic_link, &resolved_path));
  canonical_path = is_symbolic_link ? resolved_path : symlink_path;
  return impala::Status::OK();
}

// We specifically target the base file name created by glog.
// Glog's default base file name follow this pattern:
// "<program name>.<hostname>.<user name>.log.<severity level>.<date>-<time>.<pid>"
// IMPALA-12595: FLAGS_log_rotation_match_pid is added to control whether to match against
// PID or not.
inline string GlobPatternForLog(google::LogSeverity severity) {
  if (FLAGS_log_rotation_match_pid) {
    return strings::Substitute("$0/$1*.log.$2.*.$3", FLAGS_log_dir,
        google::ProgramInvocationShortName(), google::GetLogSeverityName(severity),
        getpid());
  } else {
    return strings::Substitute("$0/$1*.log.$2.*", FLAGS_log_dir,
        google::ProgramInvocationShortName(), google::GetLogSeverityName(severity));
  }
}

impala::Status GetLatestCanonicalLogPath(
    google::LogSeverity severity, std::string& log_path) {
  log_path = "";
  string path_pattern = GlobPatternForLog(severity);
  glob_t result;
  int glob_ret = glob(path_pattern.c_str(), GLOB_TILDE, NULL, &result);
  if (glob_ret != 0) {
    string msg;
    if (glob_ret == GLOB_NOMATCH) {
      msg = Substitute("No match found for glob pattern $0", path_pattern);
    } else if (glob_ret == GLOB_NOSPACE) {
      msg = Substitute("Running out of memory for glob pattern $0", path_pattern);
    } else if (glob_ret == GLOB_ABORTED) {
      msg = Substitute("Read error for glob pattern $0", path_pattern);
    } else {
      msg = Substitute(
          "glob failed in impala::GetLatestCanonicalLogPath on $0  with ret = $1",
          path_pattern, glob_ret);
    }
    globfree(&result);
    return impala::Status(impala::ErrorMsg(impala::TErrorCode::RUNTIME_ERROR, msg));
  }

  // Find the largest full file path which is also the latest.
  for (size_t i = 0; i < result.gl_pathc; ++i) {
    if (i == 0 || log_path.compare(result.gl_pathv[i]) < 0) {
      log_path = result.gl_pathv[i];
    }
  }
  globfree(&result);
  return impala::Status::OK();
}

bool impala::HasLog(google::LogSeverity severity) {
  string log_path;
  Status status = GetLatestCanonicalLogPath(severity, log_path);
  return status.ok();
}

// The main implementation of AttachStdoutStderr().
// Caller must hold lock over logging_mutex.
impala::Status AttachStdoutStderrLocked() {
  // Redirect stdout to INFO log and stderr to ERROR log.
  // Needs to be done after InitGoogleLogging, to get the INFO/ERROR file paths.
  string info_log_path, error_log_path;
  RETURN_IF_ERROR(GetLatestCanonicalLogPath(google::INFO, info_log_path));
  RETURN_IF_ERROR(GetLatestCanonicalLogPath(google::ERROR, error_log_path));

  bool info_log_rotated = last_info_log_path != info_log_path;
  bool error_log_rotated = last_error_log_path != error_log_path;

  if (info_log_rotated != error_log_rotated) {
    // Since we're not holding glog's log_mutex during log file lookup, it is
    // possible that glog rotates the INFO and ERROR log independently in between our
    // lookup. Log a warning and continue if this happens. The next
    // AttachStdoutStderrLocked call should redirect the stream to the newer file.
    LOG(WARNING) << "INFO and ERROR log is not rotated at the same time "
                 << "(info_log_rotated=" << info_log_rotated << ", "
                 << "error_log_rotated=" << error_log_rotated << "). "
                 << "STDOUT/STDERR might still write to an older log file.";
  }

  if (info_log_rotated) {
    // Print to stdout before redirecting so people looking for these logs in the standard
    // place know where to look.
    cout << "Redirecting stdout to " << info_log_path << endl;
    // TODO: how to handle these errors? Maybe abort the process?
    if (freopen(info_log_path.c_str(), "a", stdout) == NULL) {
      cout << "Could not redirect stdout: " << impala::GetStrErrMsg();
    }
    last_info_log_path = info_log_path;
  }

  if (error_log_rotated) {
    // Similar to stdout, do the same thing for stderr.
    cerr << "Redirecting stderr to " << error_log_path << endl;
    if (freopen(error_log_path.c_str(), "a", stderr) == NULL) {
      cerr << "Could not redirect stderr: " << impala::GetStrErrMsg();
    }
    last_error_log_path = error_log_path;
  }
  return impala::Status::OK();
}

void impala::InitGoogleLoggingSafe(const char* arg) {
  lock_guard<mutex> logging_lock(logging_mutex);
  if (logging_initialized) return;
  if (!FLAGS_log_filename.empty()) {
    for (int severity = google::INFO; severity <= google::FATAL; ++severity) {
      google::SetLogSymlink(severity, FLAGS_log_filename.c_str());
    }
  }

  // This forces our logging to use /tmp rather than looking for a
  // temporary directory if none is specified. This is done so that we
  // can reliably construct the log file name without duplicating the
  // complex logic that glog uses to guess at a temporary dir.
  if (FLAGS_log_dir.empty()) {
    FLAGS_log_dir = "/tmp";
  }

  // Don't double log to stderr on any threshold.
  FLAGS_stderrthreshold = google::FATAL + 1;

  if (RedirectStdoutStderr()) {
    // We will be redirecting stdout/stderr to INFO/LOG so override any glog settings
    // that log to stdout/stderr...
    FLAGS_logtostderr = false;
    FLAGS_alsologtostderr = false;
  }

  if (!FLAGS_logtostderr) {
    // Verify that a log file can be created in log_dir by creating a tmp file.
    stringstream ss;
    random_generator uuid_generator;
    ss << FLAGS_log_dir << "/" << "impala_test_log." << uuid_generator();
    const string file_name = ss.str();
    ofstream test_file(file_name.c_str());
    if (!test_file.is_open()) {
      stringstream error_msg;
      error_msg << "Could not open file in log_dir " << FLAGS_log_dir;
      perror(error_msg.str().c_str());
      // Unlock the mutex before exiting the program to avoid mutex d'tor assert.
      logging_mutex.unlock();
      exit(1);
    }
    remove(file_name.c_str());
  }

  google::InitGoogleLogging(arg);
  google::InstallLogMessageListenerFunction(MessageListener);

  // Needs to be done after InitGoogleLogging
  if (FLAGS_log_filename.empty()) {
    FLAGS_log_filename = google::ProgramInvocationShortName();
  }

  if (RedirectStdoutStderr()) {
    // The log files are created on first use, log something to each and flush before
    // redirecting.
    LOG(INFO) << "stdout will be logged to this file.";
    LOG(ERROR) << "stderr will be logged to this file.";
    google::FlushLogFiles(google::GLOG_INFO);
    Status status = AttachStdoutStderrLocked();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to attach STDOUT/STDERR: " << status.GetDetail();
    }
  }

  logging_initialized = true;
}

void impala::AttachStdoutStderr() {
  lock_guard<mutex> logging_lock(logging_mutex);
  Status status = AttachStdoutStderrLocked();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to attach STDOUT/STDERR: " << status.GetDetail();
  }
}

bool impala::CheckLogSize(bool log_error) {
  lock_guard<mutex> logging_lock(logging_mutex);
  // FLAGS_max_log_size is measured in megabytes. Thus, we convert it to bytes.
  uintmax_t max_log_size = FLAGS_max_log_size * 1024 * 1024;
  int log_to_check[2] = {google::INFO, google::ERROR};
  bool max_log_file_exceeded = false;
  Status status;
  int log_level = log_to_check[0];
  for (int i : log_to_check) {
    uintmax_t file_size = 0;
    log_level = i;
    string log_path;
    status = GetLatestCanonicalLogPath(log_level, log_path);
    if (!status.ok()) break;
    status = FileSystemUtil::ApproximateFileSize(log_path, file_size);
    if (!status.ok()) break;
    max_log_file_exceeded |= file_size > max_log_size;
  }
  if (!status.ok()) {
    if (log_error) {
      LOG(ERROR) << "Failed to check log file size for log level "
                 << (log_level == google::INFO ? "INFO" : "ERROR") << ": "
                 << status.GetDetail();
    }
    return false;
  }
  return max_log_file_exceeded;
}

void impala::ForceRotateLog() {
  lock_guard<mutex> logging_lock(logging_mutex);
  google::SetLogFilenameExtension(".cut");
  google::SetLogFilenameExtension("");
  LOG(INFO) << "INFO log rotated by Impala due to max_log_size exceeded.";
  LOG(ERROR) << "ERROR log rotated by Impala due to max_log_size exceeded.";
  google::FlushLogFiles(google::GLOG_INFO);
}

bool impala::RedirectStdoutStderr() {
  return FLAGS_redirect_stdout_stderr && !TestInfo::is_test();
}

void impala::GetFullLogFilename(google::LogSeverity severity, string* filename) {
  stringstream ss;
  ss << FLAGS_log_dir << "/" << FLAGS_log_filename << "."
     << google::GetLogSeverityName(severity);
  *filename = ss.str();
}

void impala::ShutdownLogging() {
  // This method may only correctly be called once (which this lock does not
  // enforce), but this lock protects against concurrent calls with
  // InitGoogleLoggingSafe
  lock_guard<mutex> logging_lock(logging_mutex);
  google::ShutdownGoogleLogging();
}

void impala::LogCommandLineFlags() {
  LOG(INFO) << "Flags (see also /varz are on debug webserver):" << endl
            << kudu::CommandlineFlagsIntoString(kudu::EscapeMode::NONE);

  vector<google::CommandLineFlagInfo> flags;
  google::GetAllFlags(&flags, true);
  stringstream ss;
  for (const auto& flag: flags) {
    if (flag.hidden) {
      string flag_value = CheckFlagAndRedact(flag, kudu::EscapeMode::NONE);
      ss << "--" << flag.name << "=" << flag_value << "\n";
    }
  }
  string experimental_flags = ss.str();
  if (!experimental_flags.empty()) {
    LOG(WARNING) << "Experimental flags:" << endl << experimental_flags;
  }
}

void impala::CheckAndRotateLogFiles(int max_log_files) {
  lock_guard<mutex> logging_lock(logging_mutex);
  // Ignore bad input or disable log rotation
  if (max_log_files <= 1) return;
  // Check log files for all severities
  for (int severity = 0; severity < google::NUM_SEVERITIES; ++severity) {
    // Build glob pattern for target severity
    // e.g. /tmp/impalad.*.log.INFO.*.5477
    string fname = GlobPatternForLog(severity);

    impala::LoggingSupport::DeleteOldLogs(fname, max_log_files);
  }
}

static const uint32_t ONE_BILLION = 1000000000;

// Print the value in base 10 by converting v into parts that are base
// 1 billion (large multiple of 10 that's easy to work with).
ostream& impala::operator<<(ostream& os, const __int128_t& val) {
  __int128_t v = val;
  if (v == 0) {
    os << "0";
    return os;
  }

  if (v < 0) {
    v = -v;
    os << "-";
  }

  // 1B^5 covers the range for __int128_t
  // parts[0] is the least significant place.
  uint32_t parts[5];
  int index = 0;
  while (v > 0) {
    parts[index++] = v % ONE_BILLION;
    v /= ONE_BILLION;
  }
  --index;

  // Accumulate into a temporary stringstream so format options on 'os' do
  // not mess up printing val.
  // TODO: This is likely pretty expensive with the string copies. We don't
  // do this in paths we care about currently but might need to revisit.
  stringstream ss;
  ss << parts[index];
  for (int i = index - 1; i >= 0; --i) {
    // The remaining parts need to be padded with leading zeros.
    ss << setfill('0') << setw(9) << parts[i];
  }
  os << ss.str();
  return os;
}
