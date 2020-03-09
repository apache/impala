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

#include "util/os-info.h"

#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <sstream>

#include <unistd.h>
#include <sys/stat.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/detail/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;

namespace impala {

bool OsInfo::initialized_ = false;
string OsInfo::os_distribution_ = "Unknown";
string OsInfo::os_version_ = "Unknown";
clockid_t OsInfo::fast_clock_ = CLOCK_MONOTONIC;
std::string OsInfo::clock_name_ =
    "Unknown clocksource, clockid_t defaulting to CLOCK_MONOTONIC";

// CLOCK_MONOTONIC_COARSE was added in Linux 2.6.32. For now we still want to support
// older kernels by falling back to CLOCK_MONOTONIC.
#ifdef CLOCK_MONOTONIC_COARSE
#define HAVE_CLOCK_MONOTONIC_COARSE true
#else
#define HAVE_CLOCK_MONOTONIC_COARSE false
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

void OsInfo::Init() {
  DCHECK(!initialized_);
  struct stat info;
  // Read from /etc/os-release
  if (stat("/etc/os-release", &info) == 0) {
    ifstream os_distribution("/etc/os-release", ios::in);
    string line;
    while (os_distribution.good() && !os_distribution.eof()) {
      getline(os_distribution, line);
      vector<string> fields;
      split(fields, line, is_any_of("="), token_compress_on);
      if (fields[0].compare("PRETTY_NAME") == 0) {
        os_distribution_ = fields[1].data();
        // remove quotes around os distribution
        os_distribution_.erase(
            remove(os_distribution_.begin(), os_distribution_.end(), '\"'),
            os_distribution_.end());
        break;
      }
    }
    if (os_distribution.is_open()) os_distribution.close();
  } else if (stat("/etc/redhat-release", &info) == 0) {
    // Only old distributions like centos 6, redhat 6
    ifstream os_distribution("/etc/redhat-release", ios::in);
    if (os_distribution.good()) getline(os_distribution, os_distribution_);
    if (os_distribution.is_open()) os_distribution.close();
  }

  // Read from /proc/version
  ifstream os_version("/proc/version", ios::in);
  if (os_version.good()) getline(os_version, os_version_);
  if (os_version.is_open()) os_version.close();

  // Read the current clocksource to see if CLOCK_MONOTONIC is known to be fast. "tsc" is
  // fast, while "xen" is slow (40 times slower than "tsc" on EC2). If CLOCK_MONOTONIC is
  // known to be slow, we use CLOCK_MONOTONIC_COARSE, which uses jiffies, with a
  // resolution measured in milliseconds, rather than nanoseconds.
  std::ifstream clocksource_file(
      "/sys/devices/system/clocksource/clocksource0/current_clocksource");
  if (clocksource_file.good()) {
    std::string clocksource;
    clocksource_file >> clocksource;
    clock_name_ = "clocksource: '" + clocksource + "', clockid_t: ";
    if (HAVE_CLOCK_MONOTONIC_COARSE && clocksource != "tsc") {
      clock_name_ += "CLOCK_MONOTONIC_COARSE";
      fast_clock_ = CLOCK_MONOTONIC_COARSE;
    } else {
      clock_name_ += "CLOCK_MONOTONIC";
      fast_clock_ = CLOCK_MONOTONIC;
    }
  }

  initialized_ = true;
}

string OsInfo::DebugString() {
  DCHECK(initialized_);
  stringstream stream;
  stream << "OS distribution: " << os_distribution_ << endl
         << "OS version: " << os_version_ << endl
         << "Clock: " << clock_name_ << endl;
  return stream.str();
}

} // namespace impala
