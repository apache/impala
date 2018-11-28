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

#include "util/os-util.h"

#include <unistd.h>
#include <fstream>
#include <sstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "util/error-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::filesystem::exists;
using boost::algorithm::is_any_of;
using boost::algorithm::token_compress_on;
using boost::algorithm::split;
using boost::algorithm::trim_right;
using namespace impala;
using namespace strings;

// Ensure that Impala compiles on earlier kernels. If the target kernel does not support
// _SC_CLK_TCK, sysconf(_SC_CLK_TCK) will return -1.
#ifndef _SC_CLK_TCK
#define _SC_CLK_TCK 2
#endif

static const int64_t TICKS_PER_SEC = sysconf(_SC_CLK_TCK);

// Offsets into the ../stat file array of per-thread statistics
static const int64_t USER_TICKS = 13;
static const int64_t KERNEL_TICKS = 14;
static const int64_t IO_WAIT = 41;

// Largest offset we are interested in, to check we get a well formed stat file.
static const int64_t MAX_OFFSET = IO_WAIT;

Status impala::GetThreadStats(int64_t tid, ThreadStats* stats) {
  DCHECK(stats != NULL);
  if (TICKS_PER_SEC <= 0) return Status("ThreadStats not supported");

  stringstream proc_path;
  proc_path << "/proc/self/task/" << tid << "/stat";
  if (!exists(proc_path.str())) return Status("Thread path does not exist");

  ifstream proc_file(proc_path.str().c_str());
  if (!proc_file.is_open()) return Status("Could not open ifstream");

  string buffer((std::istreambuf_iterator<char>(proc_file)),
      std::istreambuf_iterator<char>());

  vector<string> splits;
  split(splits, buffer, is_any_of(" "), token_compress_on);
  if (splits.size() < MAX_OFFSET) return Status("Unrecognised /proc format");

  StringParser::ParseResult parse_result;
  int64_t tmp = StringParser::StringToInt<int64_t>(splits[USER_TICKS].c_str(),
      splits[USER_TICKS].size(), &parse_result);
  if (parse_result == StringParser::PARSE_SUCCESS) {
    stats->user_ns = tmp * (1e9 / TICKS_PER_SEC);
  }

  tmp = StringParser::StringToInt<int64_t>(splits[KERNEL_TICKS].c_str(),
      splits[KERNEL_TICKS].size(), &parse_result);
  if (parse_result == StringParser::PARSE_SUCCESS) {
    stats->kernel_ns = tmp * (1e9 / TICKS_PER_SEC);
  }

  tmp = StringParser::StringToInt<int64_t>(splits[IO_WAIT].c_str(),
      splits[IO_WAIT].size(), &parse_result);
  if (parse_result == StringParser::PARSE_SUCCESS) {
    stats->iowait_ns = tmp * (1e9 / TICKS_PER_SEC);
  }

  return Status::OK();
}

bool impala::RunShellProcess(const string& cmd, string* msg, bool do_trim,
    const std::set<std::string>& unset_environment) {
  DCHECK(msg != NULL);
  string unset_cmd = "";
  for (const auto& env: unset_environment) {
    unset_cmd += "unset " + env + "; ";
  }
  string new_cmd = Substitute("$0$1", unset_cmd, cmd);
  FILE* fp = popen(new_cmd.c_str(), "r");
  if (fp == NULL) {
    *msg = Substitute("Failed to execute shell cmd: '$0', error was: $1", new_cmd,
        GetStrErrMsg());
    return false;
  }
  // Read the first 1024 bytes of any output before pclose() so we have some idea of what
  // happened on failure.
  char buf[1024];
  size_t len = fread(buf, 1, 1024, fp);
  string output;
  output.assign(buf, len);

  // pclose() returns an encoded form of the sub-process' exit code.
  int status = pclose(fp);
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
    *msg = output;
    if (do_trim) trim_right(*msg);
    return true;
  }

  // pclose() doesn't set errno. We could redirect the cmd to get the stderr output as
  // well, but that might interfere with correct executions that happen to produce output
  // on stderr.
  *msg = Substitute("Shell cmd: '$0' exited with error status: '$1'. Stdout was: '$2'",
      cmd, WEXITSTATUS(status), output);
  return false;
}
