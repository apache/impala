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
}

#endif
