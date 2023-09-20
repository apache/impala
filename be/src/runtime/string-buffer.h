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


#ifndef IMPALA_RUNTIME_STRING_BUFFER_H
#define IMPALA_RUNTIME_STRING_BUFFER_H

#include "common/status.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "util/ubsan.h"

namespace impala {

/// Dynamic-sizable string (similar to std::string) but without as many
/// copies and allocations.
/// StringBuffer is a buffer of char allocated from 'pool'. Current usage and size of the
/// buffer are tracked in 'len_' and 'buffer_size_' respectively. It supports a subset of
/// the std::string functionality but will only allocate bigger string buffers as
/// necessary. std::string tries to be immutable and will reallocate very often.
/// std::string should be avoided in all hot paths.
class StringBuffer {
 public:
  /// C'tor for StringBuffer.  Memory backing the string will be allocated from
  /// the pool as necessary. Can optionally be initialized from a StringValue.
  StringBuffer(MemPool* pool, StringValue* str = NULL)
      : pool_(pool), buffer_(NULL), len_(0), buffer_size_(0) {
    DCHECK(pool_ != NULL);
    if (str != NULL) {
      buffer_ = str->Ptr();
      len_ = buffer_size_ = str->Len();
    }
  }

  /// Append 'str' to the current string, allocating a new buffer as necessary.
  /// Return error status if memory limit is exceeded.
  Status Append(const char* str, int64_t str_len) WARN_UNUSED_RESULT {
    int64_t new_len = len_ + str_len;
    if (UNLIKELY(new_len > buffer_size_)) RETURN_IF_ERROR(GrowBuffer(new_len));
    Ubsan::MemCpy(buffer_ + len_, str, str_len);
    len_ += str_len;
    return Status::OK();
  }

  /// Wrapper around append() for input type 'uint8_t'.
  Status Append(const uint8_t* str, int64_t str_len) WARN_UNUSED_RESULT {
    return Append(reinterpret_cast<const char*>(str), str_len);
  }

  /// Clear the underlying StringValue. The allocated buffer can be reused.
  void Clear() { len_ = 0; }

  /// Reset the usage and size of the buffer. Note that the allocated buffer is
  /// retained but cannot be reused.
  void Reset() {
    len_ = 0;
    buffer_size_ = 0;
    buffer_ = NULL;
  }

  /// Returns true if no byte is consumed in the buffer.
  bool IsEmpty() const { return len_ == 0; }

  /// Grows the buffer to be at least 'new_size', copying over the previous data
  /// into the new buffer. The old buffer is not freed. Return an error status if
  /// growing the buffer will exceed memory limit.
  Status GrowBuffer(int64_t new_size) WARN_UNUSED_RESULT {
    if (LIKELY(new_size > buffer_size_)) {
      int64_t old_size = buffer_size_;
      buffer_size_ = std::max<int64_t>(buffer_size_ * 2, new_size);
      char* new_buffer = reinterpret_cast<char*>(pool_->TryAllocate(buffer_size_));
      if (UNLIKELY(new_buffer == NULL)) {
        string details =
            strings::Substitute("StringBuffer failed to grow buffer from $0 to $1 bytes.",
                old_size, buffer_size_);
        return pool_->mem_tracker()->MemLimitExceeded(NULL, details, buffer_size_);
      }
      if (LIKELY(len_ > 0)) memcpy(new_buffer, buffer_, len_);
      buffer_ = new_buffer;
    }
    return Status::OK();
  }

  /// Returns the number of bytes consumed in the buffer.
  int64_t len() const { return len_; }

  /// Returns the pointer to the buffer. Note that it's the caller's responsibility
  /// to not retain the pointer to 'buffer_' across call to Append() as the buffer_
  /// may be relocated in Append().
  char* buffer() const { return buffer_; }

  /// Returns the size of the buffer.
  int64_t buffer_size() const { return buffer_size_; }

 private:
  MemPool* pool_;
  char* buffer_;
  int64_t len_;         // number of bytes consumed in the buffer.
  int64_t buffer_size_; // size of the buffer.
};

}

#endif
