// Copyright 2012 Cloudera Inc.
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

class MemTracker;
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
// In order to minimize memory allocations, RowBatches and TRowBatches that have been
// serialized and sent over the wire should be reused (this prevents compression_scratch_
// from being needlessly reallocated).
// TODO: stick tuple_ptrs_ into a pool?
class RowBatch {
 public:
  // Create RowBatch for a maximum of 'capacity' rows of tuples specified
  // by 'row_desc'.
  // tracker cannot be NULL.
  RowBatch(const RowDescriptor& row_desc, int capacity, MemTracker* tracker);

  // Populate a row batch from input_batch by copying input_batch's
  // tuple_data into the row batch's mempool and converting all offsets
  // in the data back into pointers.
  // TODO: figure out how to transfer the data from input_batch to this RowBatch
  // (so that we don't need to make yet another copy)
  RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch,
      MemTracker* tracker);

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
    DCHECK_GE(n, 0);
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

  // Returns true if row_batch has reached capacity.
  bool IsFull() {
    return num_rows_ == capacity_;
  }

  // Returns true if the row batch has accumulated enough external memory (in MemPools
  // and io buffers).  This would be a trigger to compact the row batch or reclaim
  // the memory in some way.
  bool AtResourceLimit() {
    return io_buffers_.size() > MAX_IO_BUFFERS ||
           tuple_data_pool()->total_allocated_bytes() > MAX_MEM_POOL_SIZE;
  }

  int row_byte_size() {
    return num_tuples_per_row_ * sizeof(Tuple*);
  }

  // The total size of all data represented in this row batch (tuples and referenced
  // string data).
  int TotalByteSize();

  TupleRow* GetRow(int row_idx) {
    DCHECK(tuple_ptrs_ != NULL);
    DCHECK_GE(row_idx, 0);
    DCHECK_LT(row_idx, num_rows_ + (has_in_flight_row_ ? 1 : 0));
    return reinterpret_cast<TupleRow*>(tuple_ptrs_ + row_idx * num_tuples_per_row_);
  }

  MemPool* tuple_data_pool() { return tuple_data_pool_.get(); }
  int num_io_buffers() { return io_buffers_.size(); }

  // Resets the row batch, returning all resources it has accumulated.
  void Reset();

  // Add buffer to this row batch.
  void AddIoBuffer(DiskIoMgr::BufferDescriptor* buffer);

  // Transfer ownership of resources to dest.  This includes tuple data in mem
  // pool and io buffers.
  void TransferResourceOwnership(RowBatch* dest);

  void CopyRow(TupleRow* src, TupleRow* dest) {
    memcpy(dest, src, num_tuples_per_row_ * sizeof(Tuple*));
  }

  void ClearRow(TupleRow* row) {
    memset(row, 0, num_tuples_per_row_ * sizeof(Tuple*));
  }

  void ClearBatch() {
    memset(tuple_ptrs_, 0, capacity_ * num_tuples_per_row_ * sizeof(Tuple*));
  }

  // Acquires state from the 'src' row batch into this row batch. This includes all IO
  // buffers and tuple data.
  // This row batch must be empty and have the same row descriptor as the src batch.
  // This is used for scan nodes which produce RowBatches asynchronously.  Typically,
  // an ExecNode is handed a row batch to populate (pull model) but ScanNodes have
  // multiple threads which push row batches.
  // TODO: this is wasteful and makes a copy that's unnecessary.  Think about cleaning
  // this up.
  void AcquireState(RowBatch* src);

  // Create a serialized version of this row batch in output_batch, attaching all of the
  // data it references to output_batch.tuple_data. output_batch.tuple_data will be
  // snappy-compressed unless the compressed data is larger than the uncompressed
  // data. Use output_batch.is_compressed to determine whether tuple_data is compressed.
  // If an in-flight row is present in this row batch, it is ignored.
  // This function does not Reset().
  // Returns the uncompressed serialized size (this will be the true size of output_batch
  // if tuple_data is actually uncompressed).
  int Serialize(TRowBatch* output_batch);

  // Utility function: returns total size of batch.
  static int GetBatchSize(const TRowBatch& batch);

  int num_rows() const { return num_rows_; }
  int capacity() const { return capacity_; }

  const RowDescriptor& row_desc() const { return row_desc_; }

  // Allow the row batch to accumulate 32MBs before it is considered at the limit.
  // TODO: are these numbers reasonable?
  static const int MAX_IO_BUFFERS = 4;
  static const int MAX_MEM_POOL_SIZE = 32 * 1024 * 1024;

 private:
  MemTracker* mem_tracker_;  // not owned

  // All members below need to be handled in RowBatch::AcquireState()

  bool has_in_flight_row_;  // if true, last row hasn't been committed yet
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

  // String to write compressed tuple data to in Serialize().
  // This is a string so we can swap() with the string in the TRowBatch we're serializing
  // to (we don't compress directly into the TRowBatch in case the compressed data is
  // longer than the uncompressed data). Swapping avoids copying data to the TRowBatch and
  // avoids excess memory allocations: since we reuse RowBatchs and TRowBatchs, and
  // assuming all row batches are roughly the same size, all strings will eventually be
  // allocated to the right size.
  std::string compression_scratch_;
};

}

#endif
