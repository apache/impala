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

#include "util/sql-util.h"

#include<functional>
#include <string>
#include <sstream>
#include <unordered_map>

#include "common/compiler-util.h"

using namespace std;

namespace impala {

const unordered_map<char, char> CHARS_TO_ESCAPE = {
  {'\\', '\\'},
  {'\'', '\''},
  {'\n', 'n'},
  {';', ';'}
};

const string EscapeSql(const string& sql, const int64_t& max_escaped_len) noexcept {
  stringstream ret;
  function<bool()> pred;

  if (max_escaped_len == 0) {
    return ""; // <-- Note: early return
  }

  for (const auto& c : sql) {
    switch(c) {
      case '\n':
        ret << '\\';
        ret << 'n';
        break;
      case '\\':
      case '\'':
      case ';':
        ret << '\\';
      default:
        ret << c;
        break;
    }

    if (UNLIKELY(max_escaped_len > 0
        && static_cast<size_t>(ret.tellp() >= max_escaped_len))) {
      break;
    }
  }

  return ret.str();
} // function EscapeSql

} // namespace impala
