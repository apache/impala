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

#include "util/simple-logger.h"

#include <mutex>
#include <regex>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "kudu/util/path_util.h"
#include "util/filesystem-util.h"
#include "util/logging-support.h"

#include "common/names.h"

using boost::filesystem::create_directories;
using boost::filesystem::exists;
using boost::filesystem::is_directory;
using boost::posix_time::microsec_clock;
using boost::posix_time::ptime;
using boost::posix_time::time_from_string;
using kudu::JoinPathSegments;
using namespace impala;

const ptime EPOCH = time_from_string("1970-01-01 00:00:00.000");

Status InitLoggingDir(const string& log_dir) {
  if (!exists(log_dir)) {
    LOG(INFO) << "Log directory does not exist, creating: " << log_dir;
    try {
      create_directories(log_dir);
    } catch (const std::exception& e) { // Explicit std:: to distinguish from boost::
      LOG(ERROR) << "Could not create log directory: "
                 << log_dir << ", " << e.what();
      return Status("Failed to create log directory");
    }
  }

  if (!is_directory(log_dir)) {
    LOG(ERROR) << "Log path is not a directory ("
               << log_dir << ")";
    return Status("Log path is not a directory");
  }
  return Status::OK();
}

string SimpleLogger::GenerateLogFileName() {
  stringstream ss;
  int64_t ms_since_epoch =
      (microsec_clock::universal_time() - EPOCH).total_milliseconds();
  ss << log_dir_ << "/" << log_file_name_prefix_ << ms_since_epoch;
  return ss.str();
}

SimpleLogger::SimpleLogger(const string& log_dir, const string& log_file_name_prefix,
    uint64_t max_entries_per_file, int max_log_files)
    : log_dir_(log_dir),
      log_file_name_prefix_(log_file_name_prefix),
      num_log_file_entries_(0),
      max_entries_per_file_(max_entries_per_file),
      max_log_files_(max_log_files) {
}

Status SimpleLogger::Init() {
  // Check that Init hasn't already been called by verifying the log_file_name_ is still
  // empty.
  DCHECK(!initialized_);
  DCHECK(log_file_name_.empty());
  RETURN_IF_ERROR(InitLoggingDir(log_dir_));
  log_file_name_ = GenerateLogFileName();
  RETURN_IF_ERROR(FlushInternal());
  LOG(INFO) << "Logging to: " << log_file_name_;
  // There may be preexisting files from a previous incarnation of this logger. For
  // example, if a service restarted and logged in the same locations. Proactively
  // enforce the limit on the number of files to keep the semantics clear.
  RotateLogFiles();
  initialized_ = true;
  return Status::OK();
}

Status SimpleLogger::AppendEntry(const std::string& entry) {
  DCHECK(initialized_);
  lock_guard<mutex> l(log_file_lock_);
  if (num_log_file_entries_ >= max_entries_per_file_) {
    num_log_file_entries_ = 0;
    // The log file is generated from a prefix and the timestamp in milliseconds. If
    // the timestamp hasn't changed, then we continue logging to the same file. In that
    // case, there is no need to enforce the limit on the number of log files.
    string next_log_file_name = GenerateLogFileName();
    if (next_log_file_name != log_file_name_) {
      log_file_name_ = next_log_file_name;
      RETURN_IF_ERROR(FlushInternal());
      RotateLogFiles();
    }
  }
  if (!log_file_.is_open()) return Status("Log file is not open: " + log_file_name_);
   // Not std::endl, since that causes an implicit flush
  log_file_ << entry << "\n";
  ++num_log_file_entries_;
  return Status::OK();
}

Status SimpleLogger::Flush() {
  DCHECK(initialized_);
  lock_guard<mutex> l(log_file_lock_);
  return FlushInternal();
}

Status SimpleLogger::FlushInternal() {
  if (log_file_.is_open()) {
    // flush() alone does not apparently fsync, but we actually want
    // the results to become visible, hence the close / reopen
    log_file_.flush();
    log_file_.close();
  }
  log_file_.open(log_file_name_.c_str(), std::ios_base::app | std::ios_base::out);
  if (!log_file_.is_open()) return Status("Could not open log file: " + log_file_name_);
  return Status::OK();
}

void SimpleLogger::RotateLogFiles() {
  string log_file_name = strings::Substitute("$0/$1*", log_dir_, log_file_name_prefix_);

  impala::LoggingSupport::DeleteOldLogs(log_file_name, max_log_files_);
}

Status SimpleLogger::GetLogFiles(const string& log_dir,
    const string& log_file_name_prefix, vector<string>* log_file_list) {
  string log_file_regex = Substitute("$0[0-9]+", log_file_name_prefix);
  DCHECK(log_file_list != nullptr);
  log_file_list->clear();

  vector<string> log_file_list_tmp;
  RETURN_IF_ERROR(FileSystemUtil::Directory::GetEntryNames(log_dir, &log_file_list_tmp,
      0, FileSystemUtil::Directory::DIR_ENTRY_REG, log_file_regex));

  std::sort(log_file_list_tmp.begin(), log_file_list_tmp.end());

  for (const string& filename : log_file_list_tmp) {
    log_file_list->push_back(JoinPathSegments(log_dir, filename));
  }

  return Status::OK();
}
