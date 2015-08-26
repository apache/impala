// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_STRING_BUFFER_H
#define IMPALA_RUNTIME_STRING_BUFFER_H

#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

namespace impala {

/// Dynamic-sizable string (similar to std::string) but without as many
/// copies and allocations.
/// StringBuffer wraps a StringValue object with a pool and memory buffer length.
/// It supports a subset of the std::string functionality but will only allocate
/// bigger string buffers as necessary.  std::string tries to be immutable and will
/// reallocate very often.  std::string should be avoided in all hot paths.
class StringBuffer {
 public:
  /// C'tor for StringBuffer.  Memory backing the string will be allocated from
  /// the pool as necessary.  Can optionally be initialized from a StringValue.
  StringBuffer(MemPool* pool, StringValue* str = NULL)
      : pool_(pool), buffer_size_(0) {
    DCHECK(pool_ != NULL);
    if (str != NULL) {
      string_value_ = *str;
      buffer_size_ = str->len;
    }
  }

  /// Append 'str' to the current string, allocating a new buffer as necessary.
  void Append(const char* str, int len) {
    int new_len = len + string_value_.len;
    if (new_len > buffer_size_) {
      GrowBuffer(new_len);
    }
    memcpy(string_value_.ptr + string_value_.len, str, len);
    string_value_.len = new_len;
  }

  /// TODO: switch everything to uint8_t?
  void Append(const uint8_t* str, int len) {
    Append(reinterpret_cast<const char*>(str), len);
  }

  /// Assigns contents to StringBuffer
  void Assign(const char* str, int len) {
    Clear();
    Append(str, len);
  }

  /// Clear the underlying StringValue.  The allocated buffer can be reused.
  void Clear() {
    string_value_.len = 0;
  }

  /// Clears the underlying buffer and StringValue
  void Reset() {
    string_value_.len = 0;
    buffer_size_ = 0;
  }

  /// Returns whether the current string is empty
  bool Empty() const {
    return string_value_.len == 0;
  }

  /// Returns the length of the current string
  int Size() const {
    return string_value_.len;
  }

  /// Returns the underlying StringValue
  const StringValue& str() const {
    return string_value_;
  }

  /// Returns the buffer size
  int buffer_size() const {
    return buffer_size_;
  }

 private:
  /// Grows the buffer backing the string to be at least new_size, copying over the
  /// previous string data into the new buffer.
  void GrowBuffer(int new_len) {
    // TODO: Release/reuse old buffers somehow
    buffer_size_ = std::max(buffer_size_ * 2, new_len);
    DCHECK_LE(buffer_size_, StringValue::MAX_LENGTH);
    char* new_buffer = reinterpret_cast<char*>(pool_->Allocate(buffer_size_));
    if (string_value_.len > 0) {
      memcpy(new_buffer, string_value_.ptr, string_value_.len);
    }
    string_value_.ptr = new_buffer;
  }

  MemPool* pool_;
  StringValue string_value_;
  int buffer_size_;
};

}

#endif
