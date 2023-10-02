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

#pragma once

#include <string>
#include <unordered_map>

namespace impala {

/// Map of characters that will be escaped if found in a sql string and the character to
/// use as the replacement.
extern const std::unordered_map<char, char> CHARS_TO_ESCAPE;

constexpr char ESCAPE_CHAR = '\\';

/// Performs escaping on a sql string by adding a backslash before all instances of the
/// characters \, ', \n, and ; in a sql string. Double quotes are not escaped.
///
/// Parameters:
///   sql - String containing a sql statement to escape.
///   max_escaped_len - Optional, if specified, the string escaping will stop after this
///                     many characters. Note that this parameter limits the size of the
///                     returned escaped string instead of limiting the number of
///                     characters from the sql string that are processed. The returned
///                     string will have a length equal to this value except if the last
///                     character is escaped, then the returned string will be have a
///                     length one longer than this value.
///
/// Returns the escaped sql string.
const std::string EscapeSql(const std::string& sql,
    const std::int64_t& max_escaped_len = -1) noexcept;

} // namespace impala
