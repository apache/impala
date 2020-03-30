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

#include <cstdint>

#include "common/compiler-util.h"

namespace impala {

/// Utility classes for reading and writing to memory.


/// Utility class for writing to unaligned memory containing values of type T separated
/// by an arbitrary byte stride.
template <typename T>
struct StrideWriter {
  // The next element to write to. May not be aligned to the natural alignment of T.
  // Can be null for convenience, but the StrideWriter cannot be used in that case.
  T* current;

  // The stride in bytes between subsequent values.
  int64_t stride;

  explicit StrideWriter(T* current, int64_t stride) : current(current), stride(stride) {
  }

  /// No other functions can be called if false.
  ALWAYS_INLINE bool IsValid() const { return current != nullptr; }

  /// Set the next element to 'val' and advance 'current' to the next element.
  ALWAYS_INLINE void SetNext(T& val) {
    DCHECK(IsValid());
    // memcpy() is necessary because 'current' may not be aligned.
    memcpy(current, &val, sizeof(T));
    SkipNext(1);
  }

  /// Return a pointer to the current element and advance 'current' to the next element.
  ALWAYS_INLINE T* Advance() {
    DCHECK(IsValid());
    T* curr = current;
    SkipNext(1);
    return curr;
  }

  /// Set the next 'count' elements to 'val', advancing 'current' by 'count' values.
  void SetNext(T& val, int64_t count) {
    DCHECK(IsValid());
    for (int64_t i = 0; i < count; ++i) SetNext(val);
  }

  /// Advance 'current' by 'count' values.
  ALWAYS_INLINE void SkipNext(int64_t count) {
    DCHECK(IsValid());
    DCHECK_GE(count, 0);
    current = reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(current) + stride * count);
  }
};
} // namespace impala
