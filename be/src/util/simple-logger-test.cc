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

#include <boost/filesystem.hpp>
#include <fstream>
#include <glog/logging.h>
#include <iostream>
#include <regex>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "testutil/gtest-util.h"
#include "util/time.h"

#include "common/names.h"

using namespace impala;
using boost::filesystem::path;
using strings::Substitute;

static const string LOG_FILE_PREFIX = "log-file-prefix-";
static const int MAX_LOG_FILES = 5;
static const size_t MAX_ENTRIES_PER_FILE = 5;

class SimpleLoggerTest : public ::testing::Test {
 protected:
  SimpleLoggerTest() : tmp_dir_("/tmp" / boost::filesystem::unique_path()) {}

  path tmp_dir() const { return tmp_dir_; }

  void SetUp() override {
    boost::filesystem::create_directories(tmp_dir());
  }

  void TearDown() override {
    boost::filesystem::remove_all(tmp_dir());
  }

  // Create a SimpleLogger with standard log prefix, max entries per file, and
  // max files.
  unique_ptr<SimpleLogger> CreateSimpleLogger(path directory) {
    return std::make_unique<SimpleLogger>(directory.string(), LOG_FILE_PREFIX,
        MAX_ENTRIES_PER_FILE, MAX_LOG_FILES);
  }

 private:
  path tmp_dir_;
};

TEST_F(SimpleLoggerTest, CreateDirectories) {
  // Test that it can create multiple layers of subdirectories on initialization.
  path multilevel_subdir = tmp_dir() / "a" / "b" / "c";
  unique_ptr<SimpleLogger> logger = CreateSimpleLogger(multilevel_subdir);
  EXPECT_OK(logger->Init());
  logger.reset();

  // Test that it also works if the directory already exists
  logger = CreateSimpleLogger(tmp_dir().string());
  EXPECT_OK(logger->Init());
}

TEST_F(SimpleLoggerTest, PreexistingFiles) {
  // Test the reboot scenario where there are preexisting files
  path preexistingfiles_path = tmp_dir() / "preexistingfiles";

  // Create a logger with double the usual maximum number of log files to create the
  // preexisting files.
  unique_ptr<SimpleLogger> logger =
    make_unique<SimpleLogger>(preexistingfiles_path.string(), LOG_FILE_PREFIX,
        MAX_ENTRIES_PER_FILE, 2 * MAX_LOG_FILES);
  ASSERT_OK(logger->Init());

  // Create the preexisting files, then destroy the logger. This does not remove
  // the files.
  for (int i = 0; i < 2 * MAX_LOG_FILES; ++i) {
    for (int j = 0; j < MAX_ENTRIES_PER_FILE; ++j) {
      ASSERT_OK(logger->AppendEntry(Substitute("preexist_$0", i * MAX_LOG_FILES + j)));
    }
    // Filenames use time in milliseconds, so this sleep keeps the filenames distinct.
    SleepForMs(1);
  }
  ASSERT_OK(logger->Flush());
  logger.reset();

  // The directory and files outlive the logger.
  vector<string> log_files;
  Status status = SimpleLogger::GetLogFiles(preexistingfiles_path.string(),
      LOG_FILE_PREFIX, &log_files);
  EXPECT_OK(status);
  EXPECT_EQ(log_files.size(), 2 * MAX_LOG_FILES);

  // Create a logger with the usual, lower maximum number of log files. Init()
  // should enforce the limit and delete some of the older preexisting log files.
  // However, it does retain older log files within the limit.
  logger = CreateSimpleLogger(preexistingfiles_path);
  ASSERT_OK(logger->Init());

  status = SimpleLogger::GetLogFiles(preexistingfiles_path.string(), LOG_FILE_PREFIX,
      &log_files);
  EXPECT_OK(status);
  // The older files were deleted during Init()
  EXPECT_EQ(log_files.size(), MAX_LOG_FILES);
}

TEST_F(SimpleLoggerTest, SwitchFiles) {
  // Test that appending past the max number of log entries per file results in
  // switching to output to another file.
  path filerollover_path = tmp_dir() / "switchfiles";
  unique_ptr<SimpleLogger> logger = CreateSimpleLogger(filerollover_path);
  ASSERT_OK(logger->Init());

  for (int i = 0; i < MAX_ENTRIES_PER_FILE; ++i) {
    ASSERT_OK(logger->AppendEntry(Substitute("entry_$0", i)));
  }

  // Filenames use time in milliseconds, so this sleep keeps the filenames distinct.
  SleepForMs(1);

  // There should be one file at this point.
  vector<string> log_files;
  Status status = SimpleLogger::GetLogFiles(filerollover_path.string(), LOG_FILE_PREFIX,
      &log_files);
  ASSERT_OK(status);
  EXPECT_EQ(log_files.size(), 1);

  // Appending one more entry should result in a new file.
  ASSERT_OK(logger->AppendEntry("past_file_limit"));
  ASSERT_OK(logger->Flush());

  status = SimpleLogger::GetLogFiles(filerollover_path.string(), LOG_FILE_PREFIX,
      &log_files);
  ASSERT_OK(status);
  EXPECT_EQ(log_files.size(), 2);

  // First file contains only "entry_#" lines in the right order
  ifstream file1(log_files[0]);
  string line;
  int line_num = 0;
  while (getline(file1, line)) {
    EXPECT_EQ(line, Substitute("entry_$0", line_num));
    ++line_num;
  }
  EXPECT_EQ(line_num, MAX_ENTRIES_PER_FILE);

  // Second file contains only "past_file_limit"
  ifstream file2(log_files[1]);
  line_num = 0;
  while (getline(file2, line)) {
    EXPECT_EQ(line, "past_file_limit");
    ++line_num;
  }
  EXPECT_EQ(line_num, 1);
}

TEST_F(SimpleLoggerTest, LimitNumFiles) {
  // Test the limit on the total number of files. This appends entries until the first
  // file will be deleted and replaced with a new file.
  path maxfiles_path = tmp_dir() / "maxfiles";
  unique_ptr<SimpleLogger> logger = CreateSimpleLogger(maxfiles_path);
  ASSERT_OK(logger->Init());

  for (int i = 0; i < MAX_LOG_FILES; ++i) {
    for (int j = 0; j < MAX_ENTRIES_PER_FILE; ++j) {
      ASSERT_OK(logger->AppendEntry(Substitute("entry_$0", i * MAX_LOG_FILES + j)));
    }
    // Filenames use time in milliseconds, so this sleep keeps the filenames distinct.
    SleepForMs(1);
  }
  ASSERT_OK(logger->Flush());

  // Expectations:
  // There should be five files at this point. Each file contains 5 entries. The
  // messages start with "entry_0" and go through "entry_24".
  vector<string> log_files;
  Status status = SimpleLogger::GetLogFiles(maxfiles_path.string(), LOG_FILE_PREFIX,
      &log_files);
  EXPECT_EQ(log_files.size(), MAX_LOG_FILES);
  int line_num = 0;
  for (const string& log_file : log_files) {
    ifstream file(log_file);
    string line;
    while (getline(file, line)) {
      EXPECT_EQ(line, Substitute("entry_$0", line_num));
      ++line_num;
    }
  }

  // Appending one more entry should delete the first file and create a new file.
  ASSERT_OK(logger->AppendEntry("past_max_files"));
  ASSERT_OK(logger->Flush());

  // Expectations:
  // There should be 5 files at this point. The first four files contain 5 entries
  // with lines of the form entry_#. The numbers start at 5 and go through 24.
  status = SimpleLogger::GetLogFiles(maxfiles_path.string(), LOG_FILE_PREFIX,
      &log_files);
  line_num = MAX_ENTRIES_PER_FILE;
  for (int file_idx = 0; file_idx < log_files.size() - 1; ++file_idx) {
    ifstream file(log_files[file_idx]);
    string line;
    int lines_per_file = 0;
    while (getline(file, line)) {
      EXPECT_EQ(line, Substitute("entry_$0", line_num));
      ++line_num;
      ++lines_per_file;
    }
    EXPECT_EQ(lines_per_file, MAX_ENTRIES_PER_FILE);
  }
  ifstream last_file(log_files[log_files.size() - 1]);
  string line;
  line_num = 0;
  while (getline(last_file, line)) {
    EXPECT_EQ(line, "past_max_files");
    ++line_num;
  }
  EXPECT_EQ(line_num, 1);
}

TEST_F(SimpleLoggerTest, Blast) {
  // This tests the logger's behavior when there are high frequency updates.
  path blast_path = tmp_dir() / "blast";
  unique_ptr<SimpleLogger> logger = CreateSimpleLogger(blast_path);
  ASSERT_OK(logger->Init());

  for (int i = 0; i < 100000; ++i) {
    ASSERT_OK(logger->AppendEntry(Substitute("entry_$0", i)));
  }
  ASSERT_OK(logger->Flush());

  vector<string> log_files;
  Status status = SimpleLogger::GetLogFiles(blast_path.string(), LOG_FILE_PREFIX,
      &log_files);
  ASSERT_OK(status);
  // Debugging logging to help diagnosis for any problems.
  for (const string& log_file : log_files) {
    LOG(INFO) << "Log files after blast: " << log_file;
  }
  EXPECT_EQ(log_files.size(), MAX_LOG_FILES);

  // The expectation after the blast is that the entries are the last K written entries.
  // We don't know what K is, because the file naming uses milliseconds and can allow
  // more than the expected number of entries to be written. See IMPALA-9714. So, test
  // that the entry numbers are contiguous, increasing, and the final entry number is
  // the last entry written.
  int entry_number = -1;
  int total_lines = 0;
  // Regex to match the expect line. The parentheses are a grouping that lets us extract
  // the number separately.
  std::regex entry_regex = std::regex("entry_([0-9]+)");
  for (const string& log_file : log_files) {
    ifstream file(log_file);
    string line;
    while (getline(file, line)) {
      std::smatch match_result;
      if (std::regex_match(line, match_result, entry_regex)) {
        DCHECK_EQ(match_result.size(), 2);
        // Extract out just the number from the entry_* string and convert to an integer
        int cur_entry_number = std::stoi(match_result[1]);
        if (entry_number != -1) {
          EXPECT_EQ(cur_entry_number, entry_number + 1);
        }
        entry_number = cur_entry_number;
        ++total_lines;
      } else {
        EXPECT_TRUE(false) << "Regex did not match this line: " << line;
      }
    }
  }
  // The very last entry should be the last one we wrote.
  EXPECT_EQ(entry_number, 99999);
  // For debugging, print the number of lines seen
  LOG(INFO) << "Blast saw " << total_lines << " total lines.";
}
