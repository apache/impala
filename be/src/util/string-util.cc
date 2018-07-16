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

#include <algorithm>

#include "gutil/strings/substitute.h"
#include "util/string-util.h"

#include "common/names.h"

namespace impala {

Status TruncateDown(const string& str, int32_t max_length, string* result) {
  DCHECK(result != nullptr);
  *result = str.substr(0, std::min(static_cast<int32_t>(str.length()), max_length));
  return Status::OK();
}

Status TruncateUp(const string& str, int32_t max_length, string* result) {
  DCHECK(result != nullptr);
  if (str.length() <= max_length) {
    *result = str;
    return Status::OK();
  }

  *result = str.substr(0, max_length);
  int i = max_length - 1;
  while (i > 0 && static_cast<int32_t>((*result)[i]) == -1) {
    (*result)[i] += 1;
    --i;
  }
  // We convert it to unsigned because signed overflow results in undefined behavior.
  unsigned char uch = static_cast<unsigned char>((*result)[i]);
  uch += 1;
  (*result)[i] = uch;
  if (i == 0 && (*result)[i] == 0) {
    return Status("TruncateUp() couldn't increase string.");
  }
  result->resize(i + 1);
  return Status::OK();
}

bool CommaSeparatedContains(const std::string& cs_list, const std::string& item) {
  size_t pos = 0;
  while (pos < cs_list.size()) {
    size_t comma_pos = cs_list.find(",", pos);
    if (comma_pos == string::npos) return cs_list.compare(pos, string::npos, item) == 0;
    if (cs_list.compare(pos, comma_pos - pos, item) == 0) return true;
    pos = comma_pos + 1;
  }
  return false;
}

bool EndsWith(const std::string& full_string, const std::string& end) {
  if (full_string.size() >= end.size()) {
    return (full_string.compare(full_string.size() - end.size(), end.size(),
        end) == 0);
  }
  return false;
}

const uint8_t* FindEndOfIdentifier(const uint8_t* start, const uint8_t* end) {
  if (start == end) return nullptr;
  uint8_t ch = *start++;
  if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_')) {
    return nullptr;
  }
  while (start != end) {
    ch = *start;
    if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') || ch == '_')) {
      return start;
    }
    ++start;
  }
  return end;
}

}
