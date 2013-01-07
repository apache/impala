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


#ifndef IMPALA_EXEC_SCAN_RANGE_CONTEXT_H
#define IMPALA_EXEC_SCAN_RANGE_CONTEXT_H

#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include "common/compiler-util.h"
#include "common/status.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/row-batch.h"

namespace impala {

class HdfsPartitionDescriptor;
class HdfsScanNode;
class MemPool;
class RowBatch;
class RuntimeState;
class StringBuffer;
class Tuple;
class TupleRow;

// This class encapsulates everything needed for hdfs scanners.  It provides two main
// abstractions:
//   - Abstracts over getting buffers from the disk io mgr.  Buffers are pushed to
//     this object from another thread and queued in this object.  The scanners 
//     call a GetBytes() API which handles blocking if bytes are not yet ready
//     and copying bytes that are split across buffers.  Scanners don't have to worry
//     about either.
//   - Abstraction of RowBatches and memory management.  Scanners should use the
//     memory/pools returned by this object and it guarantees that those buffers
//     are attached properly to the materialized row batches.  Scanners don't have
//     handle RowBatch is full conditions since that is handled by this object.
//     Resources (io buffers and mempools) get attached to the last row batch
//     that still needs them.  Row batches are consumed by the rest of the query 
//     in order and cleaned up in order.  
//
// Each scanner context maps to a single scan range.  There are three threads that
// are interacting with the context.
//   1. Disk thread that reads buffers from the disk io mgr and enqueues them to
//      the context.
//   2. Scanner thread that calls GetBytes (which can block), materializing tuples
//      from processing the bytes.  When a RowBatch is complete, this thread (via
//      this context object) enqueues the batches to the scan node.
//   3. The scan node/main thread which calls into the context to trigger cancellation
//      or other end of stream conditions.
class ScanRangeContext {
 public:
  // Create a scanner context with the parent scan_node (where materialized row batches
  // get pushed to) and the initial io buffer.  
  ScanRangeContext(RuntimeState*, HdfsScanNode*, HdfsPartitionDescriptor*,
      DiskIoMgr::BufferDescriptor* initial_buffer);

  ~ScanRangeContext();

  // Sets whether of not the resulting tuples have a compact format.  If not, the
  // io buffers must be attached to the row batch, otherwise they can be returned
  // immediately.  This by default, is inferred from the scan_node tuple descriptor
  // but can be overridden (e.g. row compressed sequence files are always compact).
  void set_compact_data(bool is_compact) { compact_data_ = is_compact; }

  // Returns if the scanner should return compact row batches.
  bool compact_data() const { return compact_data_; }

  // Sets the number of bytes to read past the scan range when necessary.  This
  // can be set by the scanner if it knows something about the file, otherwise
  // the default is used.
  // Reading past the end of the scan range is likely a remote read.  We want
  // to minimize the number of io requests as well as the data volume.
  void set_read_past_buffer_size(int size) { read_past_buffer_size_ = size; }
  
  // Returns the next *len bytes or an error.  This can block if bytes are not
  // available.  
  //  - *buffer on return is a pointer to the buffer.  The memory is owned by
  //    the ScanRangeContext and should not be modified.  If the buffer is entirely
  //    from one disk io buffer, a pointer inside that buffer is returned directly.
  //    if the requested buffer straddles io buffers, a copy is done here.
  //  - requested_len is the number of bytes requested.  This function will return
  //    those number of bytes unless end of file or an error occurred.  
  //    if requested_len is 0, the next complete buffer will be returned
  //  - *out_len is the number of bytes returned.
  //  - *eos is set to true if all the bytes in this scan range are returned
  //  - *status is set if there is an error.  
  // Returns true if the call was success (i.e. status->ok())
  // This should only be called from the scanner thread.
  bool GetBytes(uint8_t** buffer, int requested_len, int* out_len, 
      bool* eos, Status* status);

  // Gets the bytes from the first available buffer without advancing the scan
  // range location (e.g. repeated calls to this function will return the same thing).
  Status GetRawBytes(uint8_t** buffer, int* out_len, bool* eos);

  // Gets memory for outputting tuples.   
  //  *pool is the mem pool that should be used for memory allocated for those tuples.
  //  *tuple_mem should be the location to output tuples, and 
  //  *tuple_row_mem for outputting tuple rows.  
  // Returns the maximum number of tuples/tuple rows that can be output (before the
  // current row batch is complete and a new one is allocated).
  // This should only be called from the scanner thread.
  int GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem);

  // Commit num_rows to the current row batch.  If this completes the row batch, the
  // row batch is enqueued with the scan node.
  // This should only be called from the scanner thread.
  // Returns true if the scanner should finish (i.e. limit was reached).
  void CommitRows(int num_rows);

  // Enqueue a buffer for this context.  This will wake up the thread in GetBytes() if
  // it is blocked.
  // This can be called from multiple threads but the buffers must be added in order 
  // (can't have the buffers be queued in non-sequential order).
  void AddBuffer(DiskIoMgr::BufferDescriptor*);

  // Flush() and Cancel() are used together to coordinate proper cleanup.
  // Valid call orders are:
  //  - Flush(): normal case when the scanner finishes
  //  - Cancel() -> Flush(): scanner is cancelled asynchronously
  //  - Flush() -> Cancel(): cancel() is ignored, scan range is already complete.
  // Note that Flush() always called.  Neither function can be called multiple 
  // times.

  // This function must be called when the scanner is complete and no longer needs
  // any resources (e.g. tuple memory, io buffers, etc) returned from the scan range
  // context.  This should be called from the scanner thread.
  // This must be called even in the error path to clean up any pending resources.
  void Flush();

  // This function can be called to terminate the scanner thread asynchronously.
  // This can be called from any thread.
  void Cancel();

  // Release all memory in 'pool' to the current row batch.  
  void AcquirePool(MemPool* pool) {
    DCHECK(current_row_batch_ != NULL);
    current_row_batch_->tuple_data_pool()->AcquireData(pool, false);
  }

  // Return the number of bytes left in the range.
  int64_t BytesLeft() { return scan_range_->len() - total_bytes_returned_; }

  // If true, the scanner is cancelled and the scanner thread should finish up
  bool cancelled() const { return cancelled_; }

  // If true, all bytes in this scan range have been returned
  bool eosr() const { return read_eosr_ || total_bytes_returned_ >= scan_range_->len(); }

  // Returns the total number of bytes returned
  int64_t total_bytes_returned() { return total_bytes_returned_; }

  // Returns the buffer's current offset in the file.
  int64_t file_offset() { return scan_range_start_ + total_bytes_returned_; }

  const DiskIoMgr::ScanRange* scan_range() { return scan_range_; }
  const char* filename() { return scan_range_->file(); }
  HdfsPartitionDescriptor* partition_descriptor() { return partition_desc_; }

  Tuple* next_tuple(Tuple* t) const { 
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + tuple_byte_size_);
  }

  TupleRow* next_row(TupleRow* r) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(r);
    return reinterpret_cast<TupleRow*>(mem + row_byte_size());
  }

  int row_byte_size() const {
    return current_row_batch_->row_byte_size();
  }

  Tuple* template_tuple() const {
    return template_tuple_;
  }

 private:
  RuntimeState* state_;
  HdfsScanNode* scan_node_;

  int tuple_byte_size_;

  HdfsPartitionDescriptor* partition_desc_;
  const DiskIoMgr::ScanRange* scan_range_;

  // Template tuple for this scan range
  Tuple* template_tuple_;

  // Byte offset for this scan range
  int64_t scan_range_start_;

  // Byte offset into the current (first) io buffer.
  uint8_t* current_buffer_pos_;

  // Bytes left in the first buffer
  int current_buffer_bytes_left_;

  // Total number of bytes returned from GetBytes()
  int64_t total_bytes_returned_;

  // If true, tuple data in the row batches is compact and the io buffers can be
  // recycled immediately
  bool compact_data_;

  // The buffer size to use for when reading past the end of the scan range.  A
  // default value is pickd and scanners can overwrite it (i.e. the scanner knows
  // more about the file format)
  int read_past_buffer_size_;

  // Current row batch that tuples are being written to.
  RowBatch* current_row_batch_;

  // Tuple memory for current row batch.
  uint8_t* tuple_mem_;

  // Pool for allocating boundary buffers.  
  boost::scoped_ptr<MemPool> boundary_pool_;
  boost::scoped_ptr<StringBuffer> boundary_buffer_;

  // Lock to protect fields below.  
  boost::mutex lock_;

  // Condition variable (with lock_) for waking up the scanner thread in Read(). This
  // condition variable is signaled when from (AddBuffer()) and Cancel()
  boost::condition_variable read_ready_cv_;

  // If true, the scan range has been cancelled and the scanner thread should abort
  bool cancelled_;

  // Set to true when a buffer returns the end of the scan range.
  bool read_eosr_;

  // Buffers that are ready for the reader
  std::list<DiskIoMgr::BufferDescriptor*> buffers_;

  // List of buffers that are completed but still have bytes referenced by the caller.
  // On the next GetBytes() call, these buffers are released (the caller by calling
  // GetBytes() signals it is done with its previous bytes).  At this point the
  // buffers are either returned to the io mgr or attached to the current row batch.
  std::list<DiskIoMgr::BufferDescriptor*> completed_buffers_;

  // The current buffer (buffers_.front()) or NULL if there are none available.  This
  // is used for the fast GetBytes path.
  DiskIoMgr::BufferDescriptor* current_buffer_;

  // Removes the first buffer from the queue, adding it to the row batch if necessary.
  void RemoveFirstBuffer();

  // Create a new row batch and tuple buffer.
  void NewRowBatch();

  // Attach all completed io buffers to the current row batch
  void AttachCompletedResources(bool done);
  
  // GetBytes helper to handle the slow path 
  // If peek is set then return the data but do not move the current offset.
  Status GetBytesInternal(uint8_t** buffer, int requested_len,
                          bool peek, int* out_len, bool* eos);
};
  
// Handle the fast common path where all the bytes are in the first buffer.  This
// is the path used by sequence/rc/trevni file formats to read a very small number
// (i.e. single int) of bytes.
inline bool ScanRangeContext::GetBytes(uint8_t** buffer, int requested_len, 
    int* out_len, bool* eos, Status* status) {

  if (UNLIKELY(requested_len == 0)) {
    *status = GetBytesInternal(buffer, requested_len, false, out_len, eos);
    return status->ok();
  }

  // Note: the fast path does not grab any locks even though another thread might be 
  // updating current_buffer_bytes_left_, current_buffer_ and current_buffer_pos_.
  // See the implementation of AddBuffer() on why this is okay.
  if (LIKELY(requested_len < current_buffer_bytes_left_)) {
    *eos = false;
    // Memory barrier to guarantee current_buffer_pos_ is not read before the 
    // above if statement.
    __sync_synchronize();
    DCHECK(current_buffer_ != NULL);
    *buffer = current_buffer_pos_;
    *out_len = requested_len;
    current_buffer_bytes_left_ -= requested_len;
    current_buffer_pos_ += requested_len;
    total_bytes_returned_ += *out_len;
    if (UNLIKELY(current_buffer_bytes_left_ == 0)) {
      *eos = current_buffer_->eosr();
    }
    return true;
  }
  *status = GetBytesInternal(buffer, requested_len, false, out_len, eos);
  return status->ok();
}


}

#endif

