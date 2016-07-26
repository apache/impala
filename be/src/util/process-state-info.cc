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

#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/algorithm/string.hpp>
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
  const string& path = Substitute("/proc/$0/io", getpid());
  ifstream ioinfo(path.c_str(), ios::in);
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

void ProcessStateInfo::ReadProcCgroup() {
  const string& path = Substitute("/proc/$0/cgroup", getpid());
  ifstream cgroupinfo(path.c_str(), ios::in);
  string line;
  while (cgroupinfo.good() && !cgroupinfo.eof()) {
    getline(cgroupinfo, line);
    vector<string> fields;
    split(fields, line, is_any_of(":"), token_compress_on);
    if (fields.size() < 3) continue;
    process_state_map_["cgroup/hierarchy_id"] = fields[0];
    process_state_map_["cgroup/subsystems"] = fields[1];
    process_state_map_["cgroup/control_group"] = fields[2];
    break;
  }

  if (cgroupinfo.is_open()) cgroupinfo.close();
}

void ProcessStateInfo::ReadProcSched() {
  const string& path = Substitute("/proc/$0/sched", getpid());
  ifstream schedinfo(path.c_str(), ios::in);
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
  const string& path = Substitute("/proc/$0/status", getpid());
  ifstream statusinfo(path.c_str(), ios::in);
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

void ProcessStateInfo::ReadProcFileDescriptorInfo() {
  const string& command = Substitute("ls -l /proc/$0/fd | awk '{print $$(NF-2), $$NF}'",
      getpid());
  FILE* fp = popen(command.c_str(), "r");
  if (fp) {
    int max_buffer = 1024;
    char buf[max_buffer];
    while (!feof(fp)) {
      if (fgets(buf, max_buffer, fp) != NULL) {
        string line;
        line.append(buf);
        vector<string> fields;
        split(fields, line, is_any_of(" "), token_compress_on);
        if (fields.size() < 2) continue;
        fd_desc_[atoi(fields[0].c_str())] = fields[1];
      }
    }
    pclose(fp);
  }
}

ProcessStateInfo::ProcessStateInfo() {
  ReadProcIO();
  ReadProcCgroup();
  ReadProcSched();
  ReadProcStatus();
  ReadProcFileDescriptorInfo();
}

string ProcessStateInfo::DebugString() const {
  stringstream stream;
  stream << "Process State: " << endl
         << "  I/O: " << endl
         << "    Read: "
         << PrettyPrinter::Print(GetBytes("io/read_bytes"), TUnit::BYTES) << endl
         << "    Write: "
         << PrettyPrinter::Print(GetBytes("io/write_bytes"), TUnit::BYTES) << endl
         << "    Read I/O: " << GetInt64("io/syscr") << endl
         << "    Write I/O: " << GetInt64("io/syscw") << endl
         << "  CGroups: " << endl
         << "    Hierarchy: " << GetString("cgroup/hierarchy_id") << endl
         << "    Subsystems: " << GetString("cgroup/subsystems") <<endl
         << "    Control Group: " << GetString("cgroup/control_group") << endl
         << "  Schedule: " << endl
         << "    Sum Execute Time: " << GetString("sched/se.sum_exec_runtime") << endl
         << "    Max Wait Time: " << GetString("sched/se.statistics.wait_max") << endl
         << "    Sum Wait Time: " << GetString("sched/se.statistics.wait_sum") << endl
         << "    Wait Count: " << GetInt64("sched/se.statistics.wait_count") << endl
         << "    Sum I/O Wait Time: "
         << GetString("sched/se.statistics.iowait_sum") << endl
         << "    I/O Wait Count: "
         << GetInt64("sched/se.statistics.iowait_count") << endl
         << "    Wakeup Count with cpu migration: "
         << GetInt64("sched/se.statistics.nr_wakeups_migrate") << endl
         << "    Switches: " << GetInt64("sched/nr_switches") << endl
         << "    Voluntary Switches: " << GetInt("sched/nr_voluntary_switches") << endl
         << "    Involuntary Switches: "
         << GetInt("sched/nr_involuntary_switches") << endl
         << "    Process Priority: " << GetInt("sched/prio") << endl
         << "  Status: " << endl
         << "    Process ID: " << getpid() << endl
         << "    Thread Number: " << GetInt("status/Threads") << endl
         << "    VM Peak: "
         << PrettyPrinter::Print(GetBytes("status/VmPeak"), TUnit::BYTES) << endl
         << "    VM Size: "
         << PrettyPrinter::Print(GetBytes("status/VmSize"), TUnit::BYTES) << endl
         << "    VM Lock: "
         << PrettyPrinter::Print(GetBytes("status/VmLck"), TUnit::BYTES) << endl
         << "    VM Pin: "
         << PrettyPrinter::Print(GetBytes("status/VmPin"), TUnit::BYTES) << endl
         << "    VM HWM: "
         << PrettyPrinter::Print(GetBytes("status/VmHWM"), TUnit::BYTES) << endl
         << "    VM RSS: "
         << PrettyPrinter::Print(GetBytes("status/VmRSS"), TUnit::BYTES) << endl
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
         << "    Mems Allowed List: " << GetString("status/Mems_allowed_list") << endl
         << "  File Descriptors: " << endl
         << "    Number of File Descriptors: " << fd_desc_.size() << endl;
  stream << endl;
  return stream.str();
}

}
