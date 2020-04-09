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

#ifndef IMPALA_RUNTIME_COLLECTION_VALUE_BUILDER_H
#define IMPALA_RUNTIME_COLLECTION_VALUE_BUILDER_H

#include "runtime/collection-value.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/ubsan.h"

namespace impala {

/// Class for constructing a CollectionValue when the total size isn't known
/// up-front. This class handles allocating the buffer backing the collection from a
/// MemPool, and uses a doubling strategy for growing the collection.
class CollectionValueBuilder {
 public:
  // I did not pick this default for any meaningful reason, feel free to change!
  static const int DEFAULT_INITIAL_TUPLE_CAPACITY = 4;

  CollectionValueBuilder(CollectionValue* coll_value, const TupleDescriptor& tuple_desc,
      MemPool* pool, RuntimeState* state,
      int64_t initial_tuple_capacity = DEFAULT_INITIAL_TUPLE_CAPACITY)
    : coll_value_(coll_value),
      tuple_desc_(tuple_desc),
      pool_(pool),
      state_(state),
      have_debug_action_(!state->query_options().debug_action.empty()) {
    buffer_size_ = initial_tuple_capacity * tuple_desc_.byte_size();
    coll_value_->ptr = pool_->TryAllocate(buffer_size_);
    if (coll_value_->ptr == NULL) buffer_size_ = 0;
  }

  /// Returns memory to write new tuples to in 'tuple_mem' and returns the maximum number
  /// of tuples that may be written before calling CommitTuples() in 'num_tuples'. After
  /// calling CommitTuples(), GetMemory() can be called again. Allocates if there is no
  /// free tuple memory left. Returns error status if memory limit is exceeded.
  Status GetFreeMemory(Tuple** tuple_mem, int* num_tuples) WARN_UNUSED_RESULT {
    if (tuple_desc_.byte_size() == 0) {
      // No tuple memory necessary, so caller can write as many tuples as 'num_tuples'
      // field can count.
      // TODO: num_tuples should be 64-bit. Need to update CollectionValue too.
      *num_tuples = INT_MAX;
    } else {
      int64_t bytes_written = coll_value_->ByteSize(tuple_desc_);
      DCHECK_GE(buffer_size_, bytes_written);
      if (buffer_size_ == bytes_written) {
        if (UNLIKELY(have_debug_action_)) {
          RETURN_IF_ERROR(
              DebugAction(state_->query_options(), "SCANNER_COLLECTION_ALLOC"));
        }
        // Double tuple buffer
        int64_t new_buffer_size =
            std::max<int64_t>(buffer_size_ * 2, tuple_desc_.byte_size());
        uint8_t* new_buf = pool_->TryAllocate(new_buffer_size);
        if (UNLIKELY(new_buf == NULL)) {
          *tuple_mem = NULL;
          *num_tuples = 0;
          std::string path = tuple_desc_.table_desc() == NULL ? "" :
              PrintPath(*tuple_desc_.table_desc(), tuple_desc_.tuple_path());
          return pool_->mem_tracker()->MemLimitExceeded(state_,
              ErrorMsg(TErrorCode::COLLECTION_ALLOC_FAILED, new_buffer_size,
              path, buffer_size_, coll_value_->num_tuples).msg(), new_buffer_size);
        }
        Ubsan::MemCpy(new_buf, coll_value_->ptr, bytes_written);
        coll_value_->ptr = new_buf;
        buffer_size_ = new_buffer_size;
      }
      *tuple_mem = reinterpret_cast<Tuple*>(coll_value_->ptr + bytes_written);
      *num_tuples = (buffer_size_ - bytes_written) / tuple_desc_.byte_size();
      DCHECK_EQ((buffer_size_ - bytes_written) % tuple_desc_.byte_size(), 0);
      DCHECK_GT(*num_tuples, 0);
    }
    return Status::OK();
  }

  /// Adds 'num_tuples' to the size of the collection. 'num_tuples' must be <= the last
  /// value returned by GetMemory().
  void CommitTuples(int num_tuples) {
    coll_value_->num_tuples += num_tuples;
    DCHECK_LE(coll_value_->ByteSize(tuple_desc_), buffer_size_);
  }

  const TupleDescriptor& tuple_desc() const { return tuple_desc_; }
  MemPool* pool() const { return pool_; }

 private:
  CollectionValue* coll_value_;

  /// The tuple desc for coll_value_'s items
  const TupleDescriptor& tuple_desc_;

  /// The pool backing coll_value_'s buffer
  MemPool* pool_;

  /// May be NULL. If non-NULL, used to log memory limit errors.
  RuntimeState* state_;

  /// Whether 'state_' has a debug action set. Used to reduce overhead of
  /// the check that is run once per collection.
  const bool have_debug_action_;

  /// The current size of coll_value_'s buffer in bytes, including any unused space
  /// (i.e. buffer_size_ is equal to or larger than coll_value_->ByteSize()).
  int64_t buffer_size_;
};

}

#endif
