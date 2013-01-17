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

#include "exec/serde-utils.inline.h"

#include <sstream>
#include <vector>

#include "common/status.h"

using namespace std;
using namespace impala;

string SerDeUtils::HexDump(const uint8_t* buf, int64_t length) {
  stringstream ss;
  ss << std::hex;
  for (int i = 0; i < length; ++i) {
    ss << static_cast<int>(buf[i]) << " ";
  }
  return ss.str();
}

string SerDeUtils::HexDump(const char* buf, int64_t length) {
  return HexDump(reinterpret_cast<const uint8_t*>(buf), length);
}

