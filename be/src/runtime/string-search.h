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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "common/logging.h"
#include "runtime/string-value.h"

namespace impala {

/// TODO: This can be sped up with SIDD_CMP_EQUAL_ORDERED or at the very least rewritten
/// from published algorithms.
//
/// This is based on the Python search string function doing string search
/// (substring) using an optimized boyer-moore-horspool algorithm.

/// http://hg.python.org/cpython/file/6b6c79eba944/Objects/stringlib/fastsearch.h
///
/// Changes include using our own Bloom implementation, Impala's native StringValue string
/// type, and removing other search modes (e.g. FAST_COUNT).
class StringSearch {

 public:
  StringSearch() : pattern_(NULL), mask_(0) {}

  /// Initialize/Precompute a StringSearch object from the pattern
  StringSearch(const StringValue* pattern)
    : pattern_(pattern), mask_(0), skip_(0), rskip_(0) {
    StringValue::SimpleString pattern_s = pattern->ToSimpleString();
    // Special cases
    if (pattern_s.len <= 1) {
      return;
    }

    // Build compressed lookup table
    int mlast = pattern_s.len - 1;
    skip_ = mlast - 1;
    rskip_ = mlast - 1;

    // In the Python implementation, building the bloom filter happens at the
    // beginning of the Search operation. We build it during construction
    // instead, so that the same StringSearch instance can be reused multiple
    // times without rebuilding the bloom filter every time.
    for (int i = 0; i < mlast; ++i) {
      BloomAdd(pattern_s.ptr[i]);
      if (pattern_s.ptr[i] == pattern_s.ptr[mlast])
        skip_ = mlast - i - 1;
    }
    BloomAdd(pattern_s.ptr[mlast]);

    // The order of iteration doesn't have any effect on the bloom filter, but
    // it does on skip_. For this reason we need to calculate a separate rskip_
    // for reverse search.
    for (int i = mlast; i > 0; i--) {
      if (pattern_s.ptr[i] == pattern_s.ptr[0]) rskip_ = i - 1;
    }
  }

  /// Search for this pattern in str.
  ///   Returns the offset into str if the pattern exists
  ///   Returns -1 if the pattern is not found
  int Search(const StringValue* str) const {
    // Special cases
    if (str == NULL || pattern_ == NULL || pattern_->Len() == 0) {
      return -1;
    }
    StringValue::SimpleString pattern_s = pattern_->ToSimpleString();
    StringValue::SimpleString str_s = str->ToSimpleString();

    int mlast = pattern_s.len - 1;
    int w = str_s.len - pattern_s.len;
    int n = str_s.len;
    int m = pattern_s.len;
    const char* s = str_s.ptr;
    const char* p = pattern_s.ptr;

    // Special case if pattern->len == 1
    if (m == 1) {
      const char* result = reinterpret_cast<const char*>(memchr(s, p[0], n));
      if (result != NULL) return result - s;
      return -1;
    }

    // General case.
    int j;
    // TODO: the original code seems to have an off by one error. It is possible
    // to index at w + m which is the length of the input string. Checks have
    // been added to make sure that w + m < str->len.
    for (int i = 0; i <= w; i++) {
      // note: using mlast in the skip path slows things down on x86
      if (s[i+m-1] == p[m-1]) {
        // candidate match
        for (j = 0; j < mlast; j++)
          if (s[i+j] != p[j]) break;
        if (j == mlast) {
          return i;
        }
        // miss: check if next character is part of pattern
        if (i + m < n && !BloomQuery(s[i+m]))
          i = i + m;
        else
          i = i + skip_;
      } else {
        // skip: check if next character is part of pattern
        if (i + m < n && !BloomQuery(s[i+m])) {
          i = i + m;
        }
      }
    }
    return -1;
  }

  /// Search for this pattern in str backwards.
  ///   Returns the offset into str if the pattern exists
  ///   Returns -1 if the pattern is not found
  int RSearch(const StringValue* str) const {
    // Special cases
    if (str == NULL || pattern_ == NULL || pattern_->Len() == 0) {
      return -1;
    }
    StringValue::SimpleString pattern_s = pattern_->ToSimpleString();
    StringValue::SimpleString str_s = str->ToSimpleString();

    int mlast = pattern_s.len - 1;
    int w = str_s.len - pattern_s.len;
    int n = str_s.len;
    int m = pattern_s.len;
    const char* s = str_s.ptr;
    const char* p = pattern_s.ptr;

    // Special case if pattern->len == 1
    if (m == 1) {
      const char* result = reinterpret_cast<const char*>(memrchr(s, p[0], n));
      if (result != NULL) return result - s;
      return -1;
    }

    // General case.
    int j;
    for (int i = w; i >= 0; i--) {
      if (s[i] == p[0]) {
        // candidate match
        for (j = mlast; j > 0; j--)
          if (s[i + j] != p[j]) break;
        if (j == 0) {
          return i;
        }
        // miss: check if previous character is part of pattern
        if (i > 0 && !BloomQuery(s[i - 1]))
          i = i - m;
        else
          i = i - rskip_;
      } else {
        // skip: check if previous character is part of pattern
        if (i > 0 && !BloomQuery(s[i - 1])) {
          i = i - m;
        }
      }
    }
    return -1;
  }

 private:
  static const int BLOOM_WIDTH = 64;

  void BloomAdd(char c) {
    mask_ |= (1UL << (c & (BLOOM_WIDTH - 1)));
  }

  bool BloomQuery(char c) const {
    return mask_ & (1UL << (c & (BLOOM_WIDTH - 1)));
  }

  const StringValue* pattern_;
  int64_t mask_;
  int skip_;
  int rskip_;
};
}
