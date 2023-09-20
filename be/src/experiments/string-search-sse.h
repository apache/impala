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

#ifndef IMPALA_EXPERIMENTS_STRING_SEARCH_SSE_H
#define IMPALA_EXPERIMENTS_STRING_SEARCH_SSE_H

#include "common/compiler-util.h"
#include "common/logging.h"
#include "runtime/string-value.h"

namespace impala {

// This class implements strstr for non-null terminated strings.  It wraps the
// standard strstr function (which is sse4 optimized).
/// NOTE: Because this modifies the strings in place, you *cannot* pass it data
/// that was from the data section (e.g. StringSearchSSE(StringValue("abcd", 4));
/// TODO: this is still 25% slower than just calling strstr.  Look into why
/// TODO: this cannot be used with data containing nulls (I think)
class StringSearchSSE {
 public:
  /// Create a search needle for *null-terminated strings*.  This is more
  /// efficient for searching and should be used if either the needle already
  /// happens to be null terminated or the needle will be reused repeatedly, in
  /// which case the copy and null terminate is worth it.
  /// The caller owns the memory for the needle.
  StringSearchSSE(const char* needle) :
      needle_str_val_(NULL), needle_cstr_(needle) {
    needle_len_ = strlen(needle);
  }

  /// Create a search needle from a non-null terminated string.  The caller
  /// owns the memory for the needle.
  StringSearchSSE(StringValue* needle) :
      needle_str_val_(needle), needle_cstr_(NULL) {
    needle_len_ = needle->Len();
  }

  StringSearchSSE() : needle_str_val_(NULL), needle_cstr_(NULL), needle_len_(0) {}

  /// Search for needle in haystack.
  ///   Returns the offset into str if the needle exists
  ///   Returns -1 if the needle is not found
  /// str will be temporarily modified for the duration of the function
  int Search(const StringValue& haystack) const {
    // Edge cases
    StringValue::SimpleString haystack_s = haystack.ToSimpleString();
    if (UNLIKELY(haystack_s.len == 0 && needle_len_ == 0)) return 0;
    if (UNLIKELY(haystack_s.len == 0)) return -1;
    if (UNLIKELY(needle_len_ == 0)) return 0;
    if (UNLIKELY(haystack_s.len < needle_len_)) return -1;

    int result = -1;

    // temporarily null terminated input string
    char last_char_haystack = haystack_s.ptr[haystack_s.len - 1];
    haystack_s.ptr[haystack_s.len - 1] = '\0';

    // Use strchr if needle_len_ is 1
    if (needle_len_ == 1) {
      char c = (needle_str_val_ == NULL) ? needle_cstr_[0] : needle_str_val_->Ptr()[0];
      char* s = strchr(haystack_s.ptr, c);
      if (s != NULL) {
        result = s - haystack_s.ptr;
      } else if (last_char_haystack == c) {
        result = haystack_s.len - 1;
      }
      // Undo change to haystack
      haystack_s.ptr[haystack_s.len - 1] = last_char_haystack;
      return  result;
    }

    // needle is null terminated.  We just need to run strstr on the
    // null terminated haystack, and if there is no match, try a match
    // on the last needle_len chars.
    if (LIKELY(needle_cstr_ != NULL)) {
      char* s = strstr(haystack_s.ptr, needle_cstr_);
      // Undo change to haystack
      haystack_s.ptr[haystack_s.len - 1] = last_char_haystack;

      if (s != NULL) {
        result = s - haystack_s.ptr;
      } else {
        // If we didn't find a match, try the last needle->len chars
        char* end = haystack_s.ptr + haystack_s.len - needle_len_;
        bool match = true;
        for (int i = 0; i < needle_len_; ++i) {
          if (LIKELY(end[i] != needle_cstr_[i])) {
            match = false;
            break;
          }
        }
        if (UNLIKELY(match)) result = haystack_s.len - needle_len_;
      }
      return result;
    } else {
      // Needle is not null terminated.  Terminate it on the fly.
      const char last_char_needle = needle_str_val_->Ptr()[needle_len_ - 1];
      needle_str_val_->Ptr()[needle_len_ - 1] = '\0';

      int offset = 0;
      char* haystack_pos = haystack_s.ptr;
      while (offset <= haystack_s.len - needle_len_) {
        char* search = strstr(haystack_pos, needle_str_val_->Ptr());

        // If the shortened strings didn't match, then the full strings
        // must not match
        if (search == NULL) break;

        offset += (search - haystack_pos);

        // The match happened at the very end of string.  This is the case where:
        //   needle = "abc"  (null terminated to "ab")
        //   haystack is "aaabc" (null terminated to "aaab")
        // In this case, we just need to compare the last chars from both.
        if (offset == haystack_s.len - needle_len_) {
          if (last_char_needle == last_char_haystack) result = offset;
          break;
        } else {
          // If the shortened strings match, match the last character
          // Partial match within the string.  Compare the last and if doesn't
          // match, continue searching
          if (search[needle_len_ - 1] == last_char_needle) {
            result = offset;
            break;
          } else {
            // Advance the haystack.  This can be made more efficient by using
            // a boyer-moore like approach (instead of just advancing by one).
            haystack_pos = search + 1;
            ++offset;
          }
        }
      }

      // Undo change to haystack
      haystack_s.ptr[haystack_s.len - 1] = last_char_haystack;
      // Undo changes to needle
      needle_str_val_->Ptr()[needle_len_ - 1] = last_char_needle;
      return result;
    }
  }

 private:
  /// Only one of these two will be non-null.  Both are unowned.
  StringValue* needle_str_val_;
  const char* needle_cstr_;

  int needle_len_;
};

}

#endif
