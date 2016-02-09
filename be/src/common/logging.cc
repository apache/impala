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

#include "common/logging.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cerrno>
#include <ctime>
#include <fstream>
#include <glob.h>
#include <gutil/strings/substitute.h>
#include <iostream>
#include <map>
#include <sstream>
#include <stdio.h>
#include <sys/stat.h>

#include "common/logging.h"
#include "util/error-util.h"
#include "util/redactor.h"
#include "util/test-info.h"

#include "common/names.h"

DEFINE_string(log_filename, "",
    "Prefix of log filename - "
    "full path is <log_dir>/<log_filename>.[INFO|WARN|ERROR|FATAL]");
DEFINE_bool(redirect_stdout_stderr, true,
    "If true, redirects stdout/stderr to INFO/ERROR log.");

DECLARE_string(redaction_rules_file);

using boost::uuids::random_generator;

bool logging_initialized = false;

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
  if (!FLAGS_redaction_rules_file.empty()) {
    // This depends on a patched glog. The patch is at thirdparty/patches/glog.
    google::InstallLogMessageListenerFunction(impala::Redact);
  }

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
}

void impala::CheckAndRotateLogFiles(int max_log_files) {
  // Map capturing mtimes, oldest files first
  typedef map<time_t, string> LogFileMap;
  // Ignore bad input or disable log rotation
  if (max_log_files <= 1) return;
  // Check log files for all severities
  for (int severity = 0; severity < google::NUM_SEVERITIES; ++severity) {
    // Build glob pattern for input
    // e.g. /tmp/impalad.*.INFO.*
    string fname = strings::Substitute("$0/$1.*.$2*", FLAGS_log_dir, FLAGS_log_filename,
        google::GetLogSeverityName(severity));

    LogFileMap log_file_mtime;
    glob_t result;
    glob(fname.c_str(), GLOB_TILDE, NULL, &result);
    for (size_t i = 0; i < result.gl_pathc; ++i) {
      // Get the mtime for each match
      struct stat stat_val;
      if (stat(result.gl_pathv[i], &stat_val) != 0) {
        LOG(ERROR) << "Could not read last-modified-timestamp for log file "
                   << result.gl_pathv[i] << ", will not delete (error was: "
                   << strerror(errno) << ")";
        continue;
      }
      log_file_mtime[stat_val.st_mtime] = result.gl_pathv[i];
    }
    globfree(&result);

    // Iterate over the map and remove oldest log files first when too many
    // log files exist
    if (log_file_mtime.size() <= max_log_files) return;
    int files_to_delete = log_file_mtime.size() - max_log_files;
    DCHECK_GT(files_to_delete, 0);
    BOOST_FOREACH(LogFileMap::const_reference val, log_file_mtime) {
      if (unlink(val.second.c_str()) == 0) {
        LOG(INFO) << "Old log file deleted during log rotation: " << val.second;
      } else {
        LOG(ERROR) << "Failed to delete old log file: "
                   << val.second << "(error was: " << strerror(errno) << ")";
      }
      if (--files_to_delete == 0) break;
    }
  }
}
