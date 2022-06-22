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

#include "util/process-state-info.h"

#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <dirent.h>
#include <gutil/strings/substitute.h>

#include "util/pretty-printer.h"
#include "util/debug-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::algorithm::trim;

using namespace strings;

namespace impala {

int ProcessStateInfo::GetInt(const string& state_key) const {
  ProcessStateMap::const_iterator it = process_state_map_.find(state_key);
  if (it != process_state_map_.end()) return atoi(it->second.c_str());
  return -1;
}

int64_t ProcessStateInfo::GetInt64(const string& state_key) const {
  ProcessStateMap::const_iterator it = process_state_map_.find(state_key);
  if (it != process_state_map_.end()) {
    StringParser::ParseResult result;
    int64_t state_value = StringParser::StringToInt<int64_t>(it->second.data(),
        it->second.size(), &result);
    if (result == StringParser::PARSE_SUCCESS) return state_value;
  }
  return -1;
}

string ProcessStateInfo::GetString(const string& state_key) const {
  ProcessStateMap::const_iterator it = process_state_map_.find(state_key);
  if (it != process_state_map_.end()) return it->second;
  return string();
}

int64_t ProcessStateInfo::GetBytes(const string& state_key) const {
  ProcessStateMap::const_iterator it = process_state_map_.find(state_key);
  if (it != process_state_map_.end()) {
    vector<string> fields;
    split(fields, it->second, is_any_of(" "), token_compress_on);
    // We expect state_value such as, e.g., '16129508', '16129508 kB', '16129508 mB'
    StringParser::ParseResult result;
    int64_t state_value = StringParser::StringToInt<int64_t>(fields[0].data(),
        fields[0].size(), &result);
    if (result == StringParser::PARSE_SUCCESS) {
      if (fields.size() < 2) return state_value;
      if (fields[1].compare("kB") == 0) return state_value * 1024L;
    }
  }
  return -1;
}

void ProcessStateInfo::ReadProcIO() {
  ifstream ioinfo("/proc/self/io", ios::in);
  string line;
  while (ioinfo.good() && !ioinfo.eof()) {
    getline(ioinfo, line);
    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    if (fields.size() < 2) continue;
    string key = fields[0].substr(0, fields[0].size() - 1);
    process_state_map_[Substitute("io/$0", key)] = fields[1];
  }

  if (ioinfo.is_open()) ioinfo.close();
}

void ProcessStateInfo::ReadProcSched() {
  ifstream schedinfo("/proc/self/sched", ios::in);
  string line;
  while (schedinfo.good() && !schedinfo.eof()) {
    getline(schedinfo, line);
    vector<string> fields;
    split(fields, line, is_any_of(":"), token_compress_on);
    if (fields.size() < 2) continue;
    trim(fields[0]);
    trim(fields[1]);
    process_state_map_[Substitute("sched/$0", fields[0])] = fields[1];
  }

  if (schedinfo.is_open()) schedinfo.close();
}

void ProcessStateInfo::ReadProcStatus() {
  ifstream statusinfo("/proc/self/status", ios::in);
  string line;
  while (statusinfo.good() && !statusinfo.eof()) {
    getline(statusinfo, line);
    vector<string> fields;
    split(fields, line, is_any_of("\t"), token_compress_on);
    if (fields.size() < 2) continue;
    trim(fields[1]);
    string key = fields[0].substr(0, fields[0].size() - 1);
    process_state_map_[Substitute("status/$0", key)] = fields[1];
  }

  if (statusinfo.is_open()) statusinfo.close();
}

void ProcessStateInfo::ReadProcFileDescriptorCount() {
  int fd_count = 0;
  DIR *dir_stream = opendir("/proc/self/fd");
  if (dir_stream != nullptr)
  {
    dirent *dir_entry;
    // readdir() is not thread-safe according to its man page, but in glibc
    // calling readdir() on different directory streams is thread-safe.
    // see: www.gnu.org/software/libc/manual/html_node/Reading_002fClosing-Directory.html
    while ((dir_entry = readdir(dir_stream)) != nullptr) {
      if(dir_entry->d_name[0] != '.') ++fd_count; // . and .. do not count
    }
    closedir(dir_stream);
  }
  process_state_map_["fd/count"] = std::to_string(fd_count);
}

ProcessStateInfo::ProcessStateInfo(bool get_extended_metrics)
  : have_extended_metrics_(get_extended_metrics) {
  ReadProcStatus();
  if (get_extended_metrics) {
    ReadProcIO();
    ReadProcSched();
    ReadProcFileDescriptorCount();
  }
}

string ProcessStateInfo::DebugString() const {
  stringstream stream;
  stream << "Process State: " << endl
         << "  Status: " << endl
         << "    Process ID: " << getpid() << endl
         << "    Thread Number: " << GetInt("status/Threads") << endl
         << "    VM Peak: "
         << PrettyPrinter::Print(GetBytes("status/VmPeak"), TUnit::BYTES) << endl
         << "    VM Size: "
         << PrettyPrinter::Print(GetVmSize(), TUnit::BYTES) << endl
         << "    VM Lock: "
         << PrettyPrinter::Print(GetBytes("status/VmLck"), TUnit::BYTES) << endl
         << "    VM Pin: "
         << PrettyPrinter::Print(GetBytes("status/VmPin"), TUnit::BYTES) << endl
         << "    VM HWM: "
         << PrettyPrinter::Print(GetBytes("status/VmHWM"), TUnit::BYTES) << endl
         << "    VM RSS: "
         << PrettyPrinter::Print(GetRss(), TUnit::BYTES) << endl
         << "    VM Data: "
         << PrettyPrinter::Print(GetBytes("status/VmData"), TUnit::BYTES) << endl
         << "    VM Stk: "
         << PrettyPrinter::Print(GetBytes("status/VmStk"), TUnit::BYTES) << endl
         << "    VM Exe: "
         << PrettyPrinter::Print(GetBytes("status/VmExe"), TUnit::BYTES) << endl
         << "    VM Lib: "
         << PrettyPrinter::Print(GetBytes("status/VmLib"), TUnit::BYTES) << endl
         << "    VM PTE: "
         << PrettyPrinter::Print(GetBytes("status/VmPTE"), TUnit::BYTES) << endl
         << "    VM Swap: "
         << PrettyPrinter::Print(GetBytes("status/VmSwap"), TUnit::BYTES) << endl
         << "    Cpus Allowed List: " << GetString("status/Cpus_allowed_list") << endl
         << "    Mems Allowed List: " << GetString("status/Mems_allowed_list") << endl;
  if (have_extended_metrics_) {
    stream << "  I/O: " << endl
           << "    Read: "
           << PrettyPrinter::Print(GetBytes("io/read_bytes"), TUnit::BYTES) << endl
           << "    Write: "
           << PrettyPrinter::Print(GetBytes("io/write_bytes"), TUnit::BYTES) << endl
           << "    Read I/O: " << GetInt64("io/syscr") << endl
           << "    Write I/O: " << GetInt64("io/syscw") << endl
           << "  Schedule: " << endl
           << "    Sum Execute Time: " << GetString("sched/se.sum_exec_runtime") << endl
           << "    Max Wait Time: " << GetString("sched/se.statistics.wait_max") << endl
           << "    Sum Wait Time: " << GetString("sched/se.statistics.wait_sum") << endl
           << "    Wait Count: " << GetInt64("sched/se.statistics.wait_count") << endl
           << "    Sum I/O Wait Time: " << GetString("sched/se.statistics.iowait_sum")
           << endl
           << "    I/O Wait Count: " << GetInt64("sched/se.statistics.iowait_count")
           << endl
           << "    Wakeup Count with cpu migration: "
           << GetInt64("sched/se.statistics.nr_wakeups_migrate") << endl
           << "    Switches: " << GetInt64("sched/nr_switches") << endl
           << "    Voluntary Switches: " << GetInt("sched/nr_voluntary_switches") << endl
           << "    Involuntary Switches: " << GetInt("sched/nr_involuntary_switches")
           << endl
           << "    Process Priority: " << GetInt("sched/prio") << endl
           << "  File Descriptors: " << endl
           << "    Number of File Descriptors: " << GetInt("fd/count") << endl;
  }
  stream << endl;
  return stream.str();
}

}
