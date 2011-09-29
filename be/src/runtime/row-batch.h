// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_ROW_BATCH_H
#define IMPALA_RUNTIME_ROW_BATCH_H

#include <vector>
#include <cstring>
#include <glog/logging.h>
#include <boost/scoped_ptr.hpp>

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
      tuple_ptrs_(new char[capacity_ * num_tuples_per_row_ * sizeof(Tuple*)]),
      tuple_data_pool_(new MemPool()) {
    DCHECK(!descriptors.empty());
    DCHECK_GT(capacity, 0);
  }

  ~RowBatch() {
    delete [] tuple_ptrs_;
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
    tuple_data_pool_.reset(new MemPool());
  }

  MemPool* tuple_data_pool() { return tuple_data_pool_.get(); }

  // Transfer ownership of our tuple data to dest.
  void TransferTupleDataOwnership(RowBatch* dest) {
    dest->tuple_data_pool_->AcquireData(tuple_data_pool_.get(), false);
    // make sure we can't access our tuples after we gave up the pools holding the
    // tuple data
    Reset();
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

  // holding (some of the) data referenced by rows
  boost::scoped_ptr<MemPool> tuple_data_pool_;
};

}

#endif
