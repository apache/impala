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

#include "util/mem-info.h"
#include "util/debug-util.h"
#include "util/string-parser.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util/pretty-printer.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;

namespace impala {

bool MemInfo::initialized_ = false;
int64_t MemInfo::physical_mem_ = -1;
int32_t MemInfo::vm_overcommit_ = -1;
int64_t MemInfo::commit_limit_ = -1;

// Lines in meminfo have the form key colon whitespace value for example:
// MemTotal: 16129508 kB
int64_t ParseMemString(const char* val, size_t len) {
  StringParser::ParseResult result;
  int64_t mem_total_kb = StringParser::StringToInt<int64_t>(val,
      len, &result);
  if (result != StringParser::PARSE_SUCCESS) return -1;
  // Entries in /proc/meminfo are in KB.
  return mem_total_kb * 1024L;
}

void MemInfo::Init() {
  // Read overcommit settings
  ParseOvercommit();

  // Read from /proc/meminfo
  ifstream meminfo("/proc/meminfo", ios::in);
  string line;
  while (meminfo.good() && !meminfo.eof()) {
    getline(meminfo, line);
    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    // We expect lines such as, e.g., 'MemTotal: 16129508 kB'
    if (fields.size() < 3) continue;

    // Make sure that the format of the file does not change
    DCHECK_EQ(fields[2], "kB");

    if (fields[0].compare("MemTotal:") == 0) {
      physical_mem_ = ParseMemString(fields[1].data(), fields[1].size());
    } else if (fields[0].compare("CommitLimit:") == 0) {
      commit_limit_ = ParseMemString(fields[1].data(), fields[1].size());
    }
  }
  if (meminfo.is_open()) meminfo.close();

  if (physical_mem_ == -1) {
    LOG(WARNING) << "Could not determine amount of physical memory on this machine "
                 << "using /proc/meminfo.";
  }

  if (commit_limit_ == -1) {
    LOG(WARNING) << "Could not determine memory commit limit on this machine "
                 << "using /proc/meminfo.";
  }
  initialized_ = true;
}

void MemInfo::ParseOvercommit() {
  ifstream overcommit_s("/proc/sys/vm/overcommit_memory", ios::in);
  overcommit_s >> vm_overcommit_;
}

string MemInfo::DebugString() {
  DCHECK(initialized_);
  stringstream stream;
  stream << "Physical Memory: "
         << PrettyPrinter::Print(physical_mem_, TUnit::BYTES)
         << endl;
  return stream.str();
}

}
