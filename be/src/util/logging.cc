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

#include "util/logging.h"

#include <boost/thread/mutex.hpp>
#include <sstream>

#include "common/logging.h"

DEFINE_string(log_filename, "", 
    "Prefix of log filename - "
    "full path is <log_dir>/<log_filename>.[INFO|WARN|ERROR|FATAL]");

bool logging_initialized = false;

using namespace boost;
using namespace std;

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

  google::InitGoogleLogging(arg);

  // Needs to be done after InitGoogleLogging
  if (FLAGS_log_filename.empty()) {
    FLAGS_log_filename = google::ProgramInvocationShortName();
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
