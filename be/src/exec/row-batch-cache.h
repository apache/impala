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


#ifndef IMPALA_EXEC_ROW_BATCH_CACHE_H
#define IMPALA_EXEC_ROW_BATCH_CACHE_H

#include <vector>

#include "runtime/row-batch.h"
#include "util/debug-util.h"

namespace impala {

class TupleRow;
class RowDescriptor;
class MemTracker;

/// Simple cache of row batches.
class RowBatchCache {
 public:
  RowBatchCache(const RowDescriptor& row_desc, int batch_size, MemTracker* mem_tracker)
    : row_desc_(row_desc),
      batch_size_(batch_size),
      mem_tracker_(mem_tracker),
      next_row_batch_idx_(0) {
  }

  /// Returns the next batch from the cache. Expands the cache if necessary.
  RowBatch* GetNextBatch() {
    if (next_row_batch_idx_ >= row_batches_.size()) {
      // Expand the cache with a new row batch.
      row_batches_.push_back(new RowBatch(row_desc_, batch_size_, mem_tracker_));
    } else {
      // Reset batch from the cache before returning it.
      row_batches_[next_row_batch_idx_]->Reset();
    }
    return row_batches_[next_row_batch_idx_++];
  }

  /// Resets the cache such that subsequent calls to GetNextBatch() return batches from
  /// the beginning of the cache. All batches previously returned by GetNextBatch()
  /// are invalid.
  void Reset() { next_row_batch_idx_ = 0; }

  ~RowBatchCache() {
    std::vector<RowBatch*>::iterator it;
    for (it = row_batches_.begin(); it != row_batches_.end(); ++it) delete *it;
    row_batches_.clear();
  }

 private:
  /// Parameters needed for creating row batches.
  const RowDescriptor& row_desc_;
  int batch_size_;
  MemTracker* mem_tracker_; // not owned

  /// List of cached row-batch objects. The row-batch objects are owned by this cache.
  std::vector<RowBatch*> row_batches_;

  /// Index of next row batch to return in GetRowBatch().
  int next_row_batch_idx_;
};

}

#endif
