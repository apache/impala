// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_ROW_BATCH_H
#define IMPALA_RUNTIME_ROW_BATCH_H

#include <vector>
#include <cstring>
#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/mem-pool.h"

namespace impala {

class TRowBatch;
class Tuple;
class TupleRow;
class TupleDescriptor;

// A RowBatch encapsulates a batch of rows, each composed of a number of tuples.
// The maximum number of rows is fixed at the time of construction, and the caller
// can add rows up to that capacity.
// The row batch reference a few different sources of memory.
//   1. TupleRow ptrs - this is always owned and managed by the row batch.
//   2. Tuple memory - this is allocated (or transferred to) the row batches tuple pool.
//   3. Auxillary tuple memory (e.g. string data) - this can either be stored externally
//      (don't copy strings) or from the tuple pool (strings are copied).  If external,
//      the data is in an io buffer that may not be attached to this row batch.  The
//      creator of that row batch has to make sure that the io buffer is not recycled
//      until all batches that reference the memory have been consumed.  
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
      tuple_data_pool_(new MemPool()) {
    tuple_ptrs_size_ = capacity_ * num_tuples_per_row_ * sizeof(Tuple*);
    tuple_ptrs_ = new Tuple*[capacity_ * num_tuples_per_row_];
    DCHECK_GT(capacity, 0);
  }

  // Populate a row batch from input_batch by copying input_batch's
  // tuple_data into the row batch's mempool and converting all offsets
  // in the data back into pointers. The row batch will be self-contained after the call.
  // TODO: figure out how to transfer the data from input_batch to this RowBatch
  // (so that we don't need to make yet another copy)
  RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch);

  // Releases all resources accumulated at this row batch.  This includes
  //  - tuple_ptrs
  //  - tuple mem pool data
  //  - buffer handles from the io mgr
  ~RowBatch();

  static const int INVALID_ROW_INDEX = -1;

  // Add n rows of tuple pointers after the last committed row and return its index.
  // The rows are uninitialized and each tuple of the row must be set.
  // Returns INVALID_ROW_INDEX if the row batch cannot fit n rows.
  // Two consecutive AddRow() calls without a CommitLastRow() between them
  // have the same effect as a single call.
  int AddRows(int n) {
    if (num_rows_ + n > capacity_) return INVALID_ROW_INDEX;
    has_in_flight_row_ = true;
    return num_rows_;
  }

  int AddRow() { return AddRows(1); }

  void CommitRows(int n) {
    DCHECK_LE(num_rows_ + n, capacity_);
    num_rows_ += n;
    has_in_flight_row_ = false;
  }
  
  void CommitLastRow() { CommitRows(1); }

  // Set function can be used to reduce the number of rows in the batch.  This is only
  // used in the limit case where more rows were added than necessary.
  void set_num_rows(int num_rows) {
    DCHECK_LE(num_rows, num_rows_);
    num_rows_ = num_rows;
  }

  // Returns true if row_batch has reached capacity or there are io buffers attached to
  // this batch, false otherwise
  // IO buffers are a scarce resource and must be recycled as quickly as possible to get
  // the best cpu/disk interleaving.
  bool IsFull() {
    return num_rows_ == capacity_ || !io_buffers_.empty();
  }

  int row_byte_size() {
    return num_tuples_per_row_ * sizeof(Tuple*);
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
    for (int i = 0; i < io_buffers_.size(); ++i) {
      io_buffers_[i]->Return();
    }
    io_buffers_.clear();
  }

  MemPool* tuple_data_pool() {
    return tuple_data_pool_.get();
  }

  void AddIoBuffer(DiskIoMgr::BufferDescriptor* buffer) {
    io_buffers_.push_back(buffer);
  }

  int num_io_buffers() {
    return io_buffers_.size();
  }

  // Transfer ownership of resources to dest.  This includes tuple data in mem
  // pool and io buffers.
  void TransferResourceOwnership(RowBatch* dest) {
    dest->tuple_data_pool_->AcquireData(tuple_data_pool_.get(), false);
    dest->io_buffers_.insert(dest->io_buffers_.begin(), 
        io_buffers_.begin(), io_buffers_.end());
    io_buffers_.clear();
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

  void ClearBatch() {
    memset(tuple_ptrs_, 0, capacity_ * num_tuples_per_row_ * sizeof(Tuple*));
  }

  // Swaps all of the row batch state with 'other'.  This is used for scan nodes
  // which produce RowBatches asynchronously.  Typically, an ExecNode is handed
  // a row batch to populate (pull model) but ScanNodes have multiple threads
  // which push row batches.  This function is used to swap the pushed row batch
  // contents with the row batch that's passed from the caller.
  // TODO: this is wasteful and makes a copy that's unnecessary.  Think about cleaning
  // this up.
  void Swap(RowBatch* other);

  // Create a serialized version of this row batch in output_batch, attaching
  // all of the data it references (TRowBatch::tuple_data) to output_batch.tuple_data.
  // If an in-flight row is present in this row batch, it is ignored.
  // If this batch is self-contained, it simply does an in-place conversion of the
  // string pointers contained in the tuple data into offsets and resets the batch
  // after copying the data to TRowBatch.
  void Serialize(TRowBatch* output_batch);

  // utility function: return total tuple data size of 'batch'.
  static int GetBatchSize(const TRowBatch& batch);

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
  // All members need to be handled in RowBatch::Swap()

  bool has_in_flight_row_;  // if true, last row hasn't been committed yet
  bool is_self_contained_;  // if true, contains all ref'd data in tuple_data_pool_
  int num_rows_;  // # of committed rows
  int capacity_;  // maximum # of rows

  int num_tuples_per_row_;
  RowDescriptor row_desc_;

  // array of pointers (w/ capacity_ * num_tuples_per_row_ elements)
  // TODO: replace w/ tr1 array?
  Tuple** tuple_ptrs_;
  int tuple_ptrs_size_;

  // holding (some of the) data referenced by rows
  boost::scoped_ptr<MemPool> tuple_data_pool_;

  std::vector<DiskIoMgr::BufferDescriptor*> io_buffers_;
};

}

#endif
