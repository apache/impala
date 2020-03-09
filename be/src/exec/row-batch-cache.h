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


#ifndef IMPALA_EXEC_ROW_BATCH_CACHE_H
#define IMPALA_EXEC_ROW_BATCH_CACHE_H

#include <memory>
#include <vector>

#include "runtime/row-batch.h"

namespace impala {

class TupleRow;
class RowDescriptor;
class MemTracker;

/// Simple cache of row batches.
class RowBatchCache {
 public:
  RowBatchCache(const RowDescriptor* row_desc, int batch_size)
    : row_desc_(row_desc), batch_size_(batch_size), next_row_batch_idx_(0) {}

  ~RowBatchCache() { DCHECK_EQ(0, row_batches_.size()); }

  /// Returns the next batch from the cache. Expands the cache if necessary.
  /// If a new batch is created, its memory is tracked against 'mem_tracker'.
  RowBatch* GetNextBatch(MemTracker* mem_tracker) {
    if (next_row_batch_idx_ >= row_batches_.size()) {
      // Expand the cache with a new row batch.
      row_batches_.push_back(
          std::make_unique<RowBatch>(row_desc_, batch_size_, mem_tracker));
    } else {
      // Reset batch from the cache before returning it.
      row_batches_[next_row_batch_idx_]->Reset();
    }
    return row_batches_[next_row_batch_idx_++].get();
  }

  /// Resets the cache such that subsequent calls to GetNextBatch() return batches from
  /// the beginning of the cache. All batches previously returned by GetNextBatch()
  /// are invalid.
  void Reset() { next_row_batch_idx_ = 0; }

  /// Delete and free resources associated with all cached batch objects. Must be called
  /// before the RowBatchCache is destroyed.
  void Clear() {
    row_batches_.clear(); // unique_ptr automatically calls all destructors.
  }

  /// Return the last batch returned from GetNextBatch() since Reset(), or NULL if
  /// GetNextBatch() has not yet been called.
  RowBatch* GetLastBatchReturned() {
    return next_row_batch_idx_ == 0 ? NULL : row_batches_[next_row_batch_idx_ - 1].get();
  }

 private:
  /// Parameters needed for creating row batches.
  const RowDescriptor* row_desc_;
  int batch_size_;

  /// List of cached row-batch objects. The row-batch objects are owned by this cache.
  std::vector<std::unique_ptr<RowBatch>> row_batches_;

  /// Index of next row batch to return in GetRowBatch().
  int next_row_batch_idx_;
};

}

#endif
