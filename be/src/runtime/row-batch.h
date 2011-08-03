// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_ROW_BATCH_H
#define IMPALA_RUNTIME_ROW_BATCH_H

#include <vector>
#include <cstring>
#include <glog/logging.h>

#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

class TupleDescriptor;

// A RowBatch encapsulates a batch of rows, each composed of a number of tuples. 
// The maximum number of rows is fixed at the time of construction, and the caller
// can add rows up to that capacity.
class RowBatch {
 public:
  // Create RowBatch for for num_rows rows of tuples specified by 'descriptors'.
  RowBatch(const std::vector<const TupleDescriptor*>& descriptors, int capacity)
    : num_rows_(0),
      capacity_(capacity),
      num_tuples_per_row_(descriptors.size()),
      tuple_ptrs_(new char[capacity_ * num_tuples_per_row_ * sizeof(Tuple*)]) {
    // assert(!descriptors.empty());
    // assert(num_rows > 0);
  }

  static const int INVALID_ROW_INDEX = -1;

  // Add a row of NULL tuples after the last committed row and return its index.
  // Returns INVALID_ROW_INDEX if the row batch is full.
  // Two consecutive AddRow() calls without a CommitLastRow() between them
  // have the same effect as a single call.
  int AddRow() {
    if (num_rows_ == capacity_) return INVALID_ROW_INDEX;
    bzero(reinterpret_cast<char*>(tuple_ptrs_)
        + num_rows_ * num_tuples_per_row_ * sizeof(Tuple*),
        num_tuples_per_row_ * sizeof(Tuple*));
    return num_rows_;
  }

  void CommitLastRow() {
    DCHECK(num_rows_ < capacity_);
    ++num_rows_;
  }

  // Returns true if row_batch has reached capacity, false otherwise
  bool IsFull() {
    return num_rows_ == capacity_;
  }

  TupleRow* GetRow(int row_idx) {
    // assert(row_idx >= 0 && row_idx < num_rows_);
    return reinterpret_cast<TupleRow*>(reinterpret_cast<char*>(tuple_ptrs_) +
        row_idx * num_tuples_per_row_ * sizeof(Tuple*));
  }

  // Add a mempool containing tuple data.
  void AddMemPool(MemPool* pool) {
    mem_pools_.push_back(pool);
  }

  int num_rows() const { return num_rows_; }
  int capacity() const { return capacity_; }

 private:
  int num_rows_;  // # of occupied rows
  int capacity_;  // maximum # of rows
  int num_tuples_per_row_;
  // TODO: replace w/ tr1 array?
  void* tuple_ptrs_;
  std::vector<MemPool*> mem_pools_;  // holding tuple data
};

}

#endif
