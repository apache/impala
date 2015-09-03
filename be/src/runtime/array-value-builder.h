// Copyright 2015 Cloudera Inc.
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

#ifndef IMPALA_RUNTIME_ARRAY_VALUE_BUILDER_H
#define IMPALA_RUNTIME_ARRAY_VALUE_BUILDER_H

#include "runtime/array-value.h"
#include "runtime/tuple.h"

namespace impala {

/// Class for constructing an ArrayValue when the total size isn't known up-front. This
/// class handles allocating the buffer backing the array from a MemPool, and uses a
/// doubling strategy for growing the array.
class ArrayValueBuilder {
 public:
  // I did not pick this default for any meaningful reason, feel free to change!
  static const int DEFAULT_INITIAL_TUPLE_CAPACITY = 4;

  ArrayValueBuilder(ArrayValue* array_value, const TupleDescriptor& tuple_desc,
      MemPool* pool, int initial_tuple_capacity = DEFAULT_INITIAL_TUPLE_CAPACITY)
    : array_value_(array_value),
      tuple_desc_(tuple_desc),
      pool_(pool) {
    buffer_size_ = initial_tuple_capacity * tuple_desc_.byte_size();
    array_value_->ptr = pool_->TryAllocate(buffer_size_);
    if (array_value_->ptr == NULL) buffer_size_ = 0;
  }

  /// Returns memory to write new tuples to in *tuple_mem and returns the maximum number
  /// of tuples that may be written before calling CommitTuples(). After calling
  /// CommitTuples(), GetMemory() can be called again. Allocates if there is no free tuple
  /// memory left. Returns 0 if OOM.
  int GetFreeMemory(Tuple** tuple_mem) {
    if (tuple_desc_.byte_size() == 0) {
      // No tuple memory necessary, so caller can write as many tuples as 'num_tuples'
      // field can count
      return INT_MAX;
    }
    int64_t bytes_written = array_value_->ByteSize(tuple_desc_);
    DCHECK_GE(buffer_size_, bytes_written);
    if (buffer_size_ == bytes_written) {
      // Double tuple buffer
      int64_t new_buffer_size = max<int64_t>(buffer_size_ * 2, tuple_desc_.byte_size());
      // TODO: actual allocation limit is lower than INT_MAX - see IMPALA-1619.
      if (UNLIKELY(new_buffer_size > INT_MAX)) {
        LOG(INFO) << "Array allocation failure: failed to allocate " << new_buffer_size
                  << " bytes. Cannot allocate more than " << INT_MAX
                  << " bytes in a single allocation. Current buffer size: "
                  << buffer_size_ << ", num tuples: " << array_value_->num_tuples;
      }
      uint8_t* new_buf = pool_->TryAllocate(new_buffer_size);
      if (new_buf == NULL) {
        LOG(INFO) << "Array allocation failure: failed to allocate " << new_buffer_size
                  << " bytes. Current buffer size: " << buffer_size_
                  << ", num tuples: " << array_value_->num_tuples;
        *tuple_mem = NULL;
        return 0;
      }
      memcpy(new_buf, array_value_->ptr, bytes_written);
      array_value_->ptr = new_buf;
      buffer_size_ = new_buffer_size;
    }
    *tuple_mem = reinterpret_cast<Tuple*>(array_value_->ptr + bytes_written);
    int num_tuples = (buffer_size_ - bytes_written) / tuple_desc_.byte_size();
    DCHECK_EQ((buffer_size_ - bytes_written) % tuple_desc_.byte_size(), 0);
    DCHECK_GT(num_tuples, 0);
    return num_tuples;
  }

  /// Adds 'num_tuples' to the size of the array. 'num_tuples' must be <= the last value
  /// returned by GetMemory().
  void CommitTuples(int num_tuples) {
    array_value_->num_tuples += num_tuples;
    DCHECK_LE(array_value_->ByteSize(tuple_desc_), buffer_size_);
  }

  const TupleDescriptor& tuple_desc() const { return tuple_desc_; }
  MemPool* pool() const { return pool_; }

 private:
  ArrayValue* array_value_;

  /// The tuple desc for array_value_'s items
  const TupleDescriptor& tuple_desc_;

  /// The pool backing array_value_'s buffer
  MemPool* pool_;

  /// The current size of array_value_'s buffer in bytes, including any unused space
  /// (i.e. buffer_size_ is equal to or larger than array_value_->ByteSize()).
  int64_t buffer_size_;
};

}

#endif

