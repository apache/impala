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


#ifndef IMPALA_EXEC_ROW_BATCH_LIST_H
#define IMPALA_EXEC_ROW_BATCH_LIST_H

#include <vector>
#include <string>

#include "runtime/row-batch.h"
#include "common/logging.h" // for DCHECK
#include "util/debug-util.h"

namespace impala {

class TupleRow;
class RowDescriptor;
class MemPool;

/// A simple list structure for RowBatches that provides an interface for
/// iterating over the TupleRows.
class RowBatchList {
 public:
  RowBatchList() : total_num_rows_(0) { }

  typedef std::vector<RowBatch*>::iterator BatchIterator;

  /// A simple iterator used to scan over all the rows stored in the list.
  class TupleRowIterator {
   public:
    /// Dummy constructor
    TupleRowIterator() : list_(NULL), row_idx_(0) { }

    /// Returns true if this iterator is at the end, i.e. GetRow() cannot be called.
    bool AtEnd() {
      return batch_it_ == list_->row_batches_.end();
    }

    /// Returns the current row. Callers must check the iterator is not AtEnd() before
    /// calling GetRow().
    TupleRow* GetRow() {
      DCHECK(!AtEnd());
      return (*batch_it_)->GetRow(row_idx_);
    }

    /// Moves the iterator to the next row. If the current row is the last row in the
    /// batch, advances to either the next non-empty batch or the end. No-op if the
    /// iterator is already at the end.
    void Next() {
      if (AtEnd()) return;
      DCHECK_GE((*batch_it_)->num_rows(), 0);
      if (++row_idx_ == (*batch_it_)->num_rows()) {
        ++batch_it_;
        SkipEmptyBatches();
        row_idx_ = 0;
      }
    }

   private:
    friend class RowBatchList;

    TupleRowIterator(RowBatchList* list)
      : list_(list),
        batch_it_(list->row_batches_.begin()),
        row_idx_(0) {
      SkipEmptyBatches();
    }

    void SkipEmptyBatches() {
      while (!AtEnd() && (*batch_it_)->num_rows() == 0) ++batch_it_;
    }

    RowBatchList* list_;

    /// The current batch. Either a batch with > 0 rows or the end() iterator.
    BatchIterator batch_it_;

    /// The index of the current row in the current batch. Always the index of a valid
    /// row if 'batch_it_' points to a valid batch.
    int64_t row_idx_;
  };

  /// Add the 'row_batch' to the list. The RowBatch* and all of its resources are owned
  /// by the caller.
  void AddRowBatch(RowBatch* row_batch) {
    row_batches_.push_back(row_batch);
    total_num_rows_ += row_batch->num_rows();
  }

  /// Resets the list.
  void Reset() {
    row_batches_.clear();
    total_num_rows_ = 0;
  }

  /// Transfers the resources of all contained row batches to `row_batch`.
  void TransferResourceOwnership(RowBatch* row_batch) {
    DCHECK(row_batch != NULL);
    for (int i = 0; i < row_batches_.size(); ++i) {
      row_batches_[i]->TransferResourceOwnership(row_batch);
    }
  }

  /// Outputs a debug string containing the contents of the list.
  std::string DebugString(const RowDescriptor& desc) {
    std::stringstream out;
    out << "RowBatchList(";
    out << "num_rows=" << total_num_rows_ << "; ";
    RowBatchList::TupleRowIterator it = Iterator();
    while (!it.AtEnd()) {
      out << " " << PrintRow(it.GetRow(), desc);
      it.Next();
    }
    out << " )";
    return out.str();
  }

  /// Returns the total number of rows in all row batches.
  int64_t total_num_rows() { return total_num_rows_; }

  /// Returns a new iterator over all the tuple rows.
  TupleRowIterator Iterator() {
    return TupleRowIterator(this);
  }

  /// Returns an iterator over the batches.
  BatchIterator BatchesBegin() { return row_batches_.begin(); }

  BatchIterator BatchesEnd() { return row_batches_.end(); }

 private:
  friend class TupleRowIterator;

  std::vector<RowBatch*> row_batches_;

  /// Total number of rows
  int64_t total_num_rows_;
};

}

#endif
