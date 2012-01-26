// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_ROW_BATCH_H
#define IMPALA_RUNTIME_ROW_BATCH_H

#include <vector>
#include <cstring>
#include <glog/logging.h>
#include <boost/scoped_ptr.hpp>

#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "gen-cpp/Data_types.h"  // scoped_ptr wants to see TRowBatch

namespace impala {

class TupleDescriptor;

// A RowBatch encapsulates a batch of rows, each composed of a number of tuples. 
// The maximum number of rows is fixed at the time of construction, and the caller
// can add rows up to that capacity.
// TODO: stick tuple_ptrs_ into a pool?
class RowBatch {
 public:
  // Create RowBatch for a maximum of 'capacity' rows of tuples specified
  // by 'row_desc'.
  RowBatch(const RowDescriptor& row_desc, int capacity)
    : has_in_flight_row_(false),
      is_self_contained_(false),
      num_rows_(0),
      capacity_(capacity),
      num_tuples_per_row_(row_desc.tuple_descriptors().size()),
      row_desc_(row_desc),
      tuple_ptrs_(new Tuple*[capacity_ * num_tuples_per_row_]),
      thrift_batch_(NULL),
      tuple_data_pool_(new MemPool()) {
    DCHECK_GT(capacity, 0);
  }

  // Populate a row batch from input_batch by turning input_batch's
  // tuple_data into the row batch's mempool and converting all offsets
  // in the data back into pointers. The row batch will be self-contained after the call
  // and takes ownership of input_batch; input_batch must not be deallocated after
  // calling this c'tor (it will be destroyed in ~RowBatch()).
  // TODO: figure out how to transfer the data from input_batch to this RowBatch
  // (so that we don't need to hang on to input_batch)
  RowBatch(const DescriptorTbl& desc_tbl, TRowBatch* input_batch);

  ~RowBatch();

  static const int INVALID_ROW_INDEX = -1;

  // Add a row of tuple pointers after the last committed row and return its index.
  // The row is uninitialized and each tuple of the row must be set.
  // Returns INVALID_ROW_INDEX if the row batch is full.
  // Two consecutive AddRow() calls without a CommitLastRow() between them
  // have the same effect as a single call.
  int AddRow() {
    if (num_rows_ == capacity_) return INVALID_ROW_INDEX;
    has_in_flight_row_ = true;
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
    return reinterpret_cast<TupleRow*>(tuple_ptrs_ + row_idx * num_tuples_per_row_);
  }

  void Reset() {
    num_rows_ = 0;
    has_in_flight_row_ = false;
    tuple_data_pool_.reset(new MemPool());
  }

  MemPool* tuple_data_pool() {
    return tuple_data_pool_.get();
  }

  // Transfer ownership of our tuple data to dest.
  void TransferTupleData(RowBatch* dest) {
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

  // Create a serialized version of this row batch in output_batch, attaching
  // all of the data it references (TRowBatch::tuple_data) to output_batch.tuple_data.
  // If an in-flight row is present in this row batch, it is ignored.
  // If this batch is self-contained, it simply does an in-place conversion of the
  // string pointers contained in the tuple data into offsets and resets the batch
  // after copying the data to TRowBatch.
  void Serialize(TRowBatch* output_batch);

  // utility function: return total tuple data size of 'batch'.
  static int GetBatchSize(TRowBatch* batch);

  int num_rows() const { return num_rows_; }
  int capacity() const { return capacity_; }

  // A self-contained row batch contains all of the tuple data it references
  // in tuple_data_pool_. The creator of the row batch needs to decide whether
  // it's self-contained (ie, this is not something that the row batch can
  // ascertain on its own).
  bool is_self_contained() const { return is_self_contained_; }
  void set_is_self_contained(bool v) { is_self_contained_ = v; }

  const RowDescriptor& row_desc() const { return row_desc_; }

 private:
  bool has_in_flight_row_;  // if true, last row hasn't been committed yet
  bool is_self_contained_;  // if true, contains all ref'd data in tuple_data_pool_
  int num_rows_;  // # of committed rows
  int capacity_;  // maximum # of rows
  int num_tuples_per_row_;
  RowDescriptor row_desc_;

  // array of pointers (w/ capacity_ * num_tuples_per_row_ elements)
  // TODO: replace w/ tr1 array?
  Tuple** tuple_ptrs_;

  // if set, contains data that we reference
  boost::scoped_ptr<TRowBatch> thrift_batch_;

  // holding (some of the) data referenced by rows
  boost::scoped_ptr<MemPool> tuple_data_pool_;
};

}

#endif
