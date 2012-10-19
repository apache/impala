// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_STRING_BUFFER_H
#define IMPALA_RUNTIME_STRING_BUFFER_H

#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

namespace impala {

// Dynamic-sizable string (similar to std::string) but without as many
// copies and allocations.  
// StringBuffer wraps a StringValue object with a pool and memory buffer length.
// It supports a subset of the std::string functionality but will only allocate
// bigger string buffers as necessary.  std::string tries to be immutable and will
// reallocate very often.  std::string should be avoided in all hot paths.
class StringBuffer {
 public:
  // C'tor for StringBuffer.  Memory backing the string will be allocated from
  // the pool as necessary.  Can optionally be initialized from a StringValue.
  StringBuffer(MemPool* pool, StringValue* str = NULL) :
    pool_(pool),
    buffer_size_(0) {
    if (str != NULL) {
      string_value_ = *str;
      buffer_size_ = str->len;
    }
  }

  // Append 'str' to the current string, allocating a new buffer as necessary.
  void Append(const char* str, int len) {
    int new_len = len + string_value_.len;
    if (new_len > buffer_size_) {
      GrowBuffer(new_len);
    }
    memcpy(string_value_.ptr + string_value_.len, str, len);
    string_value_.len = new_len;
  }

  // TODO: switch everything to uint8_t?
  void Append(const uint8_t* str, int len) {
    Append(reinterpret_cast<const char*>(str), len);
  }

  // Assigns contents to StringBuffer
  void Assign(const char* str, int len) {
    Clear();
    Append(str, len);
  }

  // Clear the underlying StringValue
  void Clear() {
    string_value_.len = 0;
  }

  // Returns whether the current string is empty
  bool Empty() const {
    return string_value_.len == 0;
  }

  // Returns the length of the current string
  int Size() const {
    return string_value_.len;
  }

  // Returns the underlying StringValue
  const StringValue& str() const {
    return string_value_;
  }

  // Returns the buffer size
  int buffer_size() const {
    return buffer_size_;
  }

 private:
  // Grows the buffer backing the string to be at least new_size, copying
  // over the previous string data into the new buffer.
  // TODO: some kind of doubling strategy?
  void GrowBuffer(int new_len) {
    char* new_buffer = reinterpret_cast<char*>(pool_->Allocate(new_len));
    if (string_value_.len > 0) {
      memcpy(new_buffer, string_value_.ptr, string_value_.len);
    }
    string_value_.ptr = new_buffer;
    buffer_size_ = new_len;
  }

  MemPool* pool_;
  StringValue string_value_;
  int buffer_size_;
};

}

#endif
