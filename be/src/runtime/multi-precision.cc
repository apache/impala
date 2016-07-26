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

#include "runtime/multi-precision.h"

#include "common/logging.h"

#include "common/names.h"

namespace impala {

static const uint32_t ONE_BILLION = 1000000000;

// Print the value in base 10 by converting v into parts that are base
// 1 billion (large multiple of 10 that's easy to work with).
ostream& operator<<(ostream& os, const int128_t& val) {
  int128_t v = val;
  if (v == 0) {
    os << "0";
    return os;
  }

  if (v < 0) {
    v = -v;
    os << "-";
  }

  // 1B^5 covers the range for int128_t
  // parts[0] is the least significant place.
  uint32_t parts[5];
  int index = 0;
  while (v > 0) {
    parts[index++] = v % ONE_BILLION;
    v /= ONE_BILLION;
  }
  --index;

  // Accumulate into a temporary stringstream so format options on 'os' do
  // not mess up printing val.
  // TODO: This is likely pretty expensive with the string copies. We don't
  // do this in paths we care about currently but might need to revisit.
  stringstream ss;
  ss << parts[index];
  for (int i = index - 1; i >= 0; --i) {
    // The remaining parts need to be padded with leading zeros.
    ss << setfill('0') << setw(9) << parts[i];
  }
  os << ss.str();
  return os;
}

}
