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

#include <cstddef>
#include <cstdint>

namespace impala {

/// Returns true if the buffer [ptr, ptr + len) contains a valid UTF-8 encoded
/// string. An empty buffer (len == 0) is considered valid, in which case 'ptr'
/// may be null. Backed by the SIMD-accelerated simdutf library.
bool IsValidUtf8(const char* ptr, size_t len);

/// Returns the byte offset of the first invalid UTF-8 sequence in [ptr, ptr + len), or
/// -1 if the buffer is valid UTF-8 (an empty buffer is valid, in which case 'ptr' may be
/// null). Backed by the SIMD-accelerated simdutf library.
int64_t FindFirstInvalidUtf8(const char* ptr, size_t len);

} // namespace impala
