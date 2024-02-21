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
#include "util/bit-util.h"
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
    size_t comma_pos = cs_list.find(',', pos);
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
  if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
      (ch >= '0' && ch <= '9') || ch == '_')) {
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

int FindUtf8PosForward(const uint8_t* ptr, const int len, int index) {
  DCHECK_GE(index, 0);
  int pos = 0;
  while (index > 0 && pos < len) {
    // Counting malformed UTF8 characters.
    while (!BitUtil::IsUtf8StartByte(ptr[pos]) && index > 0 && pos < len) {
      ++pos;
      --index;
    }
    if (index == 0 || pos == len) break;
    pos += BitUtil::NumBytesInUtf8Encoding(ptr[pos]);
    --index;
  }
  if (pos >= len) return len;
  return pos;
}

int FindUtf8PosBackward(const uint8_t* ptr, const int len, int index) {
  DCHECK_GE(index, 0);
  int pos = len - 1;
  int last_pos = len;
  while (pos >= 0) {
    // Point to the start byte of the last character.
    while (pos >= 0 && !BitUtil::IsUtf8StartByte(ptr[pos])) --pos;
    if (pos < 0) {
      // Can't find any legal characters. Count each byte from last_pos as one character.
      // Note that index is 0-based.
      if (index < last_pos) return last_pos - index - 1;
      return -1;
    }
    // Get bytes length of the located character.
    int bytes_len = BitUtil::NumBytesInUtf8Encoding(ptr[pos]);
    // If there are not enough bytes after the first byte, i.e. last_pos-pos < bytes_len,
    // we consider the bytes belong to a malformed character, and count them as one
    // character.
    int malformed_bytes = max(last_pos - pos - bytes_len, 0);
    if (index < malformed_bytes) {
      // Count each malformed bytes as one character.
      return last_pos - index - 1;
    }
    // We found a legal character and 'malformed_bytes' malformed characters.
    // At this point, index >= malformed_bytes. So the lowest value of the updated index
    // is -1, which means 'pos' points at what we want.
    index -= malformed_bytes + 1;
    if (index < 0) return pos;
    last_pos = pos;
    --pos;
  }
  DCHECK_EQ(pos, -1);
  return -1;
}

void StringStreamPop::move_back() {
  if (tellp() > 0) {
    seekp(-1, std::ios_base::cur);
  }
}

}
