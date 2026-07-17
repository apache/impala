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

#include "util/utf8-util.h"

#include <simdutf.h>

namespace impala {

bool IsValidUtf8(const char* ptr, size_t len) {
  // An empty buffer is trivially valid UTF-8. Returning early also lets callers pass
  // (nullptr, 0) without depending on simdutf's handling of a null pointer.
  if (len == 0) return true;
  return simdutf::validate_utf8(ptr, len);
}

int64_t FindFirstInvalidUtf8(const char* ptr, size_t len) {
  if (len == 0) return -1;
  simdutf::result result = simdutf::validate_utf8_with_errors(ptr, len);
  // On error, 'count' is the byte offset of the error; on success it is the number of
  // validated bytes, which we report as "valid" (-1).
  if (result.error == simdutf::error_code::SUCCESS) return -1;
  return static_cast<int64_t>(result.count);
}

} // namespace impala
