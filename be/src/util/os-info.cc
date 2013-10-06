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

#include "util/os-info.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

using namespace std;

namespace impala {

bool OsInfo::initialized_ = false;
string OsInfo::os_version_ = "Unknown";

void OsInfo::Init() {
  DCHECK(!initialized_);
  // Read from /proc/version
  ifstream version("/proc/version", ios::in);
  if (version.good()) getline(version, os_version_);
  if (version.is_open()) version.close();
  initialized_ = true;
}

string OsInfo::DebugString() {
  DCHECK(initialized_);
  stringstream stream;
  stream << "OS version: " << os_version_ << endl;
  return stream.str();
}

}
