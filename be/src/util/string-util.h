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

#ifndef IMPALA_UTIL_STRING_UTIL_H
#define IMPALA_UTIL_STRING_UTIL_H

#include <string>

#include "common/status.h"

namespace impala {

/// 'str' holds the minimum value of some string set. We need to truncate it
/// if it is longer than 'max_length'.
WARN_UNUSED_RESULT
Status TruncateDown(const std::string& str, int32_t max_length, std::string* result);

/// 'str' holds the maximum value of some string set. We want to truncate it
/// to only occupy 'max_length' bytes. We also want to guarantee that the truncated
/// value remains greater than all the strings in the original set, so we need
/// to increase it after truncation. E.g.: when 'max_length' == 3: AAAAAAA => AAB
/// Returns error if it cannot increase the string value, ie. all bytes are 0xFF.
WARN_UNUSED_RESULT
Status TruncateUp(const std::string& str, int32_t max_length, std::string* result);

/// Return true if the comma-separated string 'cs_list' contains 'item' as one of
/// the comma-separated values.
bool CommaSeparatedContains(const std::string& cs_list, const std::string& item);

/// Return true if a given string 'full_str' ends with the characters in the
/// 'end' string
bool EndsWith(const std::string& full_string, const std::string& end);

/// This function returns a pointer past the end of the longest identifier
/// that is a prefix of given string or NULL if the given string does not start with
/// one. The identifier must begin with an ASCII letter, underscore, or digit
/// and continues with ASCII letters, digits, or underscores. start and end means
/// the given string is located at [start, end)
const uint8_t* FindEndOfIdentifier(const uint8_t* start, const uint8_t* end);

/// Finds the N-th (0-based) Unicode character in the string, returns the start byte
/// position of it. The input string 'str_ptr' is viewed logically as sequence of Unicode
/// characters encoded in UTF8. 'index' is the index (0-based) of Unicode character whose
/// byte position in 'str_ptr' is to be found. If there are not enough characters, return
/// 'str_len'. E.g.
///  - If index is 0, returns 0.
///  - If the string is "你好" and index is 1, returns 3. (Each Chinese character is
///    encoded into 3 bytes in UTF-8, so the second character starts at pos 3).
///  - If the string is "你好" and index > 1, returns 6.
///  - If the string is "你好Bob" and index is 4, returns 8.
/// Error handling:
/// Bytes that don't belong any UTF8 characters are considered malformed UTF8 characters
/// as one byte per character. This is consistent with Hive since Hive replaces each of
/// those bytes to U+FFFD (REPLACEMENT CHARACTER). E.g. GenericUDFInstr calls
/// Text#toString(), which performs the replacement.
/// TODO(IMPALA-10761): Add query option to control the error handling.
int FindUtf8PosForward(const uint8_t* str_ptr, const int str_len, const int index);
inline int FindUtf8PosForward(const char* str_ptr, const int str_len, const int index) {
  return FindUtf8PosForward(reinterpret_cast<const uint8_t*>(str_ptr), str_len, index);
}

/// Counting backwards to find the last N-th Unicode character in the string, returns
/// the start byte position of it. The input string 'str_ptr' is viewed logically as
/// sequence of Unicode characters encoded in UTF8. 'index' is the backward index
/// (0-based) of Unicode character whose byte position in 'str_ptr' is to be found. E.g.
/// 0 means the last character, 1 means the second last one and so on. If there are not
/// enough characters, return -1. E.g.
///  - If the string is "你好" and index is 0, returns 3. (Each Chinese character is
///    encoded into 3 bytes in UTF-8, so the second character starts at pos 3).
///  - If the string is "你好Bob" and index is 1, returns 7.
///  - If the string is "你好Bob" and index > 4, return -1.
/// Error handling:
/// Bytes that don't belong any UTF8 characters are considered malformed UTF8 characters
/// as one byte per character. This is consistent with Hive since Hive replaces each of
/// those bytes to U+FFFD (REPLACEMENT CHARACTER). E.g. GenericUDFInstr calls
/// Text#toString(), which performs the replacement.
/// TODO(IMPALA-10761): Add query option to control the error handling.
int FindUtf8PosBackward(const uint8_t* str_ptr, const int str_len, const int index);
inline int FindUtf8PosBackward(const char* str_ptr, const int str_len, const int index) {
  return FindUtf8PosBackward(reinterpret_cast<const uint8_t*>(str_ptr), str_len, index);
}

/// Subclass of std::stringstream that adds functionality to allow overwriting the very
/// last character of the stream. The purpose of this additional functionality is to
/// enable comma delimited string building where the last instance of the comma needs to
/// be removed (for example when building a list of columns in a sql statement).
class StringStreamPop : public std::basic_stringstream<char> {
public:
  /// Directly modifies the underlying stream buffer seeking it backwards 1 position.
  /// Then, when additional characters are written, the character at the end of the stream
  /// is overwritten. Thus, to truly remove the character at the end of the stream
  /// requires writing at least one character to the stream after this function is called.
  void move_back();
};

}
#endif
