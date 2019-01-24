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

#ifndef IMPALA_RUNTIME_SCOPED_BUFFER_H
#define IMPALA_RUNTIME_SCOPED_BUFFER_H

#include "runtime/mem-tracker.h"

namespace impala {

/// A scoped memory allocation that is tracked against a MemTracker.
/// The allocation is automatically freed when the ScopedBuffer object goes out of scope.
/// NOTE: if multiple allocations share the same lifetime, prefer to use MemPool.
class ScopedBuffer {
 public:
  ScopedBuffer(MemTracker* mem_tracker) : mem_tracker_(mem_tracker),
      buffer_(NULL), bytes_(0) {}
  ~ScopedBuffer() { Release(); }

  /// Try to allocate a buffer of size 'bytes'. Returns false if MemTracker::TryConsume()
  /// or malloc() fails.
  /// Should not be called if a buffer is already allocated.
  bool TryAllocate(int64_t bytes) {
    DCHECK(buffer_ == NULL);
    DCHECK_GT(bytes, 0);
    if (!mem_tracker_->TryConsume(bytes)) return false;
    buffer_ = reinterpret_cast<uint8_t*>(malloc(bytes));
    if (UNLIKELY(buffer_ == NULL)) {
      mem_tracker_->Release(bytes);
      return false;
    }
    bytes_ = bytes;
    return true;
  }

  void Release() {
    if (buffer_ == NULL) return;
    free(buffer_);
    buffer_ = NULL;
    mem_tracker_->Release(bytes_);
    bytes_ = 0;
  }

  uint8_t* buffer() const { return buffer_; }

  int64_t Size() const { return bytes_; }

 private:
  MemTracker* mem_tracker_;
  uint8_t* buffer_;
  /// The current size of the allocated buffer, if not NULL.
  int64_t bytes_;
};

}

#endif
