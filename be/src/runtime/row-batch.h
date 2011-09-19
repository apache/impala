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
// TODO: stick tuple_ptrs_ into a pool?
// TODO: GetRow(0) is still legal after Reset() (keep track of in-flight rows?)
class RowBatch {
 public:
  // Create RowBatch for for num_rows rows of tuples specified by 'descriptors'.
  RowBatch(const std::vector<TupleDescriptor*>& descriptors, int capacity)
    : has_in_flight_row_(false),
      num_rows_(0),
      capacity_(capacity),
      num_tuples_per_row_(descriptors.size()),
      tuple_ptrs_(new char[capacity_ * num_tuples_per_row_ * sizeof(Tuple*)]) {
    DCHECK(!descriptors.empty());
    DCHECK_GT(capacity, 0);
  }

  ~RowBatch() {
    delete [] tuple_ptrs_;
    for (int i = 0; i < mem_pools_.size(); ++i) {
      delete mem_pools_[i];
    }
  }

  static const int INVALID_ROW_INDEX = -1;

  // Add a row of NULL tuples after the last committed row and return its index.
  // Returns INVALID_ROW_INDEX if the row batch is full.
  // Two consecutive AddRow() calls without a CommitLastRow() between them
  // have the same effect as a single call.
  int AddRow() {
    if (num_rows_ == capacity_) return INVALID_ROW_INDEX;
    has_in_flight_row_ = true;
    bzero(tuple_ptrs_ + num_rows_ * num_tuples_per_row_ * sizeof(Tuple*),
          num_tuples_per_row_ * sizeof(Tuple*));
    return num_rows_;
  }

  void CommitLastRow() {
    DCHECK(num_rows_ < capacity_);
    ++num_rows_;
    has_in_flight_row_ = false;
  }

  // Returns true if row_batch has reached capacity, false otherwise
  bool IsFull() {
    return num_rows_ == capacity_;
  }

  TupleRow* GetRow(int row_idx) {
    DCHECK_GE(row_idx, 0);
    DCHECK_LT(row_idx, num_rows_ + (has_in_flight_row_ ? 1 : 0));
    return reinterpret_cast<TupleRow*>(
        tuple_ptrs_ + row_idx * num_tuples_per_row_ * sizeof(Tuple*));
  }

  void Reset() {
    num_rows_ = 0;
    for (int i = 0; i < mem_pools_.size(); ++i) {
      delete mem_pools_[i];
    }
    mem_pools_.clear();
  }

  // Transfer ownership of pool to this batch.
  void AddMemPool(std::auto_ptr<MemPool>* pool) {
    mem_pools_.push_back(pool->release());
  }

  // Release our mempools.
  void ReleaseMemPools(std::vector<MemPool*>* pools) {
    pools->insert(pools->end(), mem_pools_.begin(), mem_pools_.end());
    mem_pools_.clear();
    // make sure we can't access tuples after we gave up the pools holding the tuple data
    Reset();
  }

  // Transfer ownership of src's mempools to this batch.
  void AddMemPools(RowBatch* src) {
    mem_pools_.insert(
        mem_pools_.end(), src->mem_pools_.begin(), src->mem_pools_.end());
    src->mem_pools_.clear();
    // make sure we can't access src's tuples after it gave up the pools holding the
    // tuple data
    src->Reset();
  }

  void CopyRow(TupleRow* src, TupleRow* dest) {
    memcpy(dest, src, num_tuples_per_row_ * sizeof(Tuple*));
  }

  void ClearRow(TupleRow* row) {
    memset(row, 0, num_tuples_per_row_ * sizeof(Tuple*));
  }

  int num_rows() const { return num_rows_; }
  int capacity() const { return capacity_; }

 private:
  bool has_in_flight_row_;  // if true, last row hasn't been committed yet
  int num_rows_;  // # of committed rows
  int capacity_;  // maximum # of rows
  int num_tuples_per_row_;
  // TODO: replace w/ tr1 array?
  char* tuple_ptrs_;
  std::vector<MemPool*> mem_pools_;  // holding tuple data
};

}

#endif
