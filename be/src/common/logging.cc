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

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cerrno>
#include <ctime>
#include <fstream>
#include <gutil/strings/substitute.h>
#include <iostream>
#include <map>
#include <sstream>
#include <stdio.h>

#include "common/logging.h"
#include "service/impala-server.h"
#include "util/error-util.h"
#include "util/logging-support.h"
#include "util/redactor.h"
#include "util/test-info.h"
#include "common/thread-debug-info.h"

#include "common/names.h"

DECLARE_string(redaction_rules_file);
DECLARE_string(log_filename);
DECLARE_bool(redirect_stdout_stderr);
DECLARE_string(audit_event_log_dir);

using boost::uuids::random_generator;
using impala::TUniqueId;

namespace {
bool logging_initialized = false;
// A 0 unique id, which indicates that one has not been set.
const TUniqueId ZERO_UNIQUE_ID;

// Prepends fragment id, when available. If unavailable, looks
// for query id. If unavailable, prepends nothing.
void PrependFragment(string* s, bool* changed) {
  impala::ThreadDebugInfo* tdi = impala::GetThreadDebugInfo();
  if (tdi != nullptr) {
    for (auto id : { tdi->GetInstanceId(), tdi->GetQueryId() }) {
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

void impala::InitGoogleLoggingSafe(const char* arg) {
  mutex::scoped_lock logging_lock(logging_mutex);
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

  if (FLAGS_redirect_stdout_stderr && !TestInfo::is_test()) {
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

  if (FLAGS_redirect_stdout_stderr && !TestInfo::is_test()) {
    // Needs to be done after InitGoogleLogging, to get the INFO/ERROR file paths.
    // Redirect stdout to INFO log and stderr to ERROR log
    string info_log_path, error_log_path;
    GetFullLogFilename(google::INFO, &info_log_path);
    GetFullLogFilename(google::ERROR, &error_log_path);

    // The log files are created on first use, log something to each before redirecting.
    LOG(INFO) << "stdout will be logged to this file.";
    LOG(ERROR) << "stderr will be logged to this file.";

    // Print to stderr/stdout before redirecting so people looking for these logs in
    // the standard place know where to look.
    cout << "Redirecting stdout to " << info_log_path << endl;
    cerr << "Redirecting stderr to " << error_log_path << endl;

    // TODO: how to handle these errors? Maybe abort the process?
    if (freopen(info_log_path.c_str(), "a", stdout) == NULL) {
      cout << "Could not redirect stdout: " << GetStrErrMsg();
    }
    if (freopen(error_log_path.c_str(), "a", stderr) == NULL) {
      cerr << "Could not redirect stderr: " << GetStrErrMsg();
    }
  }

  logging_initialized = true;
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
  mutex::scoped_lock logging_lock(logging_mutex);
  google::ShutdownGoogleLogging();
}

void impala::LogCommandLineFlags() {
  LOG(INFO) << "Flags (see also /varz are on debug webserver):" << endl
            << google::CommandlineFlagsIntoString();

  vector<google::CommandLineFlagInfo> flags;
  google::GetAllFlags(&flags, true);
  stringstream ss;
  for (const auto& flag: flags) {
    if (flag.hidden) {
      ss << "--" << flag.name << "=" << flag.current_value << "\n";
    }
  }
  string experimental_flags = ss.str();
  if (!experimental_flags.empty()) {
    LOG(WARNING) << "Experimental flags:" << endl << experimental_flags;
  }
}

void impala::CheckAndRotateLogFiles(int max_log_files) {
  // Ignore bad input or disable log rotation
  if (max_log_files <= 1) return;
  // Check log files for all severities
  for (int severity = 0; severity < google::NUM_SEVERITIES; ++severity) {
    // Build glob pattern for input
    // e.g. /tmp/impalad.*.INFO.*
    string fname = strings::Substitute("$0/$1.*.$2*", FLAGS_log_dir, FLAGS_log_filename,
        google::GetLogSeverityName(severity));

    impala::LoggingSupport::DeleteOldLogs(fname, max_log_files);
  }
}

void impala::CheckAndRotateAuditEventLogFiles(int max_log_files) {
  // Return if audit event logging is disabled
  if (FLAGS_audit_event_log_dir.empty()) return;
  // Ignore bad input or disable log rotation
  if (max_log_files <= 0) return;
  // Check audit event log files
  // Build glob pattern for input e.g. /tmp/impala_audit_event_log_1.0-*
  string fname = strings::Substitute(
      "$0/$1*", FLAGS_audit_event_log_dir, ImpalaServer::AUDIT_EVENT_LOG_FILE_PREFIX);

  impala::LoggingSupport::DeleteOldLogs(fname, max_log_files);
}
