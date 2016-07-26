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

#ifndef IMPALA_EXEC_PARQUET_SCRATCH_TUPLE_BATCH_H
#define IMPALA_EXEC_PARQUET_SCRATCH_TUPLE_BATCH_H

#include "runtime/descriptors.h"
#include "runtime/row-batch.h"

namespace impala {

/// Helper struct that holds a batch of tuples allocated from a mem pool, as well
/// as state associated with iterating over its tuples and transferring
/// them to an output batch in TransferScratchTuples().
struct ScratchTupleBatch {
  // Memory backing the batch of tuples. Allocated from batch's tuple data pool.
  uint8_t* tuple_mem;
  // Keeps track of the current tuple index.
  int tuple_idx;
  // Number of valid tuples in tuple_mem.
  int num_tuples;
  // Cached for convenient access.
  const int tuple_byte_size;

  // Helper batch for safely allocating tuple_mem from its tuple data pool using
  // ResizeAndAllocateTupleBuffer().
  RowBatch batch;

  ScratchTupleBatch(
      const RowDescriptor& row_desc, int batch_size, MemTracker* mem_tracker)
    : tuple_mem(NULL),
      tuple_idx(0),
      num_tuples(0),
      tuple_byte_size(row_desc.GetRowSize()),
      batch(row_desc, batch_size, mem_tracker) {
    DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);
  }

  Status Reset(RuntimeState* state) {
    tuple_idx = 0;
    num_tuples = 0;
    // Buffer size is not needed.
    int64_t buffer_size;
    RETURN_IF_ERROR(batch.ResizeAndAllocateTupleBuffer(state, &buffer_size, &tuple_mem));
    return Status::OK();
  }

  inline Tuple* GetTuple(int tuple_idx) const {
    return reinterpret_cast<Tuple*>(tuple_mem + tuple_idx * tuple_byte_size);
  }

  inline MemPool* mem_pool() { return batch.tuple_data_pool(); }
  inline int capacity() const { return batch.capacity(); }
  inline uint8_t* CurrTuple() const { return tuple_mem + tuple_idx * tuple_byte_size; }
  inline uint8_t* TupleEnd() const { return tuple_mem + num_tuples * tuple_byte_size; }
  inline bool AtEnd() const { return tuple_idx == num_tuples; }
};

}

#endif
