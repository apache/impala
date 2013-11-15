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

using namespace boost;
using namespace std;

namespace impala {

bool MemInfo::initialized_ = false;
int64_t MemInfo::physical_mem_ = -1;

void MemInfo::Init() {
  // Read from /proc/meminfo
  ifstream meminfo("/proc/meminfo", ios::in);
  string line;
  while (meminfo.good() && !meminfo.eof()) {
    getline(meminfo, line);
    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    // We expect lines such as, e.g., 'MemTotal: 16129508 kB'
    if (fields.size() < 3) {
      continue;
    }
    if (fields[0].compare("MemTotal:") != 0) {
      continue;
    }
    StringParser::ParseResult result;
    int64_t mem_total_kb = StringParser::StringToInt<int64_t>(fields[1].data(),
        fields[1].size(), &result);
    if (result == StringParser::PARSE_SUCCESS) {
      // Entries in /proc/meminfo are in KB.
      physical_mem_ = mem_total_kb * 1024L;
    }
    break;
  }
  if (meminfo.is_open()) meminfo.close();

  if (physical_mem_ == -1) {
    LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
  }

  initialized_ = true;
}

string MemInfo::DebugString() {
  DCHECK(initialized_);
  stringstream stream;
  stream << "Physical Memory: "
         << PrettyPrinter::Print(physical_mem_, TCounterType::BYTES)
         << endl;
  return stream.str();
}

}
