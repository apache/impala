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


#ifndef IMPALA_EXEC_SCANNER_CONTEXT_H
#define IMPALA_EXEC_SCANNER_CONTEXT_H

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
//   - Abstraction of RowBatches and memory management.  Scanners should use the
//     memory/pools returned by this object and it guarantees that those buffers
//     are attached properly to the materialized row batches.  Scanners don't have
//     handle RowBatch is full conditions since that is handled by this object.
//     Resources (io buffers and mempools) get attached to the last row batch
//     that still needs them.  Row batches are consumed by the rest of the query 
//     in order and cleaned up in order.  
//     As soon as a buffer is done, it is attached to the current row batch.  This
//     gurantees that the buffers (and therefore the cleanup of them) *trails* the
//     rows they are for.
//     RowBatches are passed up when they are full, independent of how many io buffers
//     are attached to them.  In the case where a denser representation is preferred
//     (e.g. very selective predicates or very wide rows), the scanner context will
//     compact the tuples, copying the (sparse) memory from the io buffers into a 
//     compact pool and returning the io buffers.
//     TODO: implement the compaction
//   - Abstracts over getting buffers from the disk io mgr.  Buffers are pushed to
//     this object from another thread and queued in this object.  The scanners 
//     call a GetBytes() API which handles blocking if bytes are not yet ready
//     and copying bytes that are split across buffers.  Scanners don't have to worry
//     about either.  This is done via the ScannerContext::Stream interface.
//     A stream is a contiguous byte range that is read end to end.  For row based
//     files, there is just a single stream per context.  For columnar files, there
//     will be one stream per column.
//
// Each scanner context maps to a single hdfs split.  There are three threads that
// are interacting with the context.
//   1. Disk thread that reads buffers from the disk io mgr and enqueues them to
//      the context's streams.
//   2. Scanner thread that calls GetBytes (which can block), materializing tuples
//      from processing the bytes.  When a RowBatch is complete, this thread (via
//      this context object) enqueues the batches to the scan node.
//   3. The scan node/main thread which calls into the context to trigger cancellation
//      or other end of stream conditions.
class ScannerContext {
 public:
  // Create a scanner context with the parent scan_node (where materialized row batches
  // get pushed to) and the initial io buffer.  
  ScannerContext(RuntimeState*, HdfsScanNode*, HdfsPartitionDescriptor*,
      DiskIoMgr::BufferDescriptor* initial_buffer);

  ~ScannerContext();

  // Encapsulates a stream (continuous byte range) that can be read.  A context
  // can contain one or more streams.  For non-columnar files, there is only
  // one stream; for columnar, there is one stream per column.
  class Stream {
   public:
    // Returns the next *len bytes or an error.  This can block if bytes are not
    // available.  
    //  - *buffer on return is a pointer to the buffer.  The memory is owned by
    //    the ScannerContext and should not be modified.  If the buffer is entirely
    //    from one disk io buffer, a pointer inside that buffer is returned directly.
    //    if the requested buffer straddles io buffers, a copy is done here.
    //  - requested_len is the number of bytes requested.  This function will return
    //    those number of bytes unless end of file or an error occurred.  
    //    if requested_len is 0, the next complete buffer will be returned
    //  - *out_len is the number of bytes returned.
    //  - *eos is set to true if all the bytes in this scan range are returned.
    //  - *status is set if there is an error.  
    // Returns true if the call was success (i.e. status->ok())
    // This should only be called from the scanner thread.
    // Note that this will return bytes past the end of the scan range if
    // requested (e.g., this can be called again after *eos is set to true).
    bool GetBytes(int requested_len, uint8_t** buffer, int* out_len, 
        bool* eos, Status* status);

    // Gets the bytes from the first available buffer without advancing the scan
    // range location (e.g. repeated calls to this function will return the same thing).
    // If the buffer is the last one in the scan range, *eos will be set to true.
    // If we are past the end of the scan range, *out_len will be 0 and *eos will be true.
    Status GetRawBytes(uint8_t** buffer, int* out_len, bool* eos);
  
    // Enqueue a buffer for this stream.  This will wake up the thread in GetBytes() if
    // it is blocked.
    // This can be called from multiple threads but the buffers must be added in order 
    // (can't have the buffers be queued in non-sequential order).
    // If this is called after Close(), the buffer is returned right away.
    void AddBuffer(DiskIoMgr::BufferDescriptor*);
  
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
  
    // Return the number of bytes left in the range for this stream.
    int64_t bytes_left() { return scan_range_->len() - total_bytes_returned_; }
  
    // If true, all bytes in this scan range have been returned
    bool eosr() const { return read_eosr_ || total_bytes_returned_ >= total_len_; }
  
    const char* filename() { return scan_range_->file(); }
    const DiskIoMgr::ScanRange* scan_range() { return scan_range_; }
  
    // Returns the buffer's current offset in the file.
    int64_t file_offset() { return scan_range_start_ + total_bytes_returned_; }
  
    // Returns the total number of bytes returned
    int64_t total_bytes_returned() { return total_bytes_returned_; }
  
    // Read a Boolean primitive value written using Java serialization.
    // Equivalent to java.io.DataInput.readBoolean()
    bool ReadBoolean(bool* boolean, Status*);
    
    // Read an Integer primitive value written using Java serialization.
    // Equivalent to java.io.DataInput.readInt()
    bool ReadInt(int32_t* val, Status*);
    
    // Read a variable-length Long value written using Writable serialization.
    // Ref: org.apache.hadoop.io.WritableUtils.readVLong()
    bool ReadVLong(int64_t* val, Status*);
    
    // Read a variable length Integer value written using Writable serialization.
    // Ref: org.apache.hadoop.io.WritableUtils.readVInt()
    bool ReadVInt(int32_t* val, Status*);
    
    // Read a zigzag encoded long
    bool ReadZLong(int64_t* val, Status*);
    
    // Skip over the next length bytes in the specified HDFS file.
    bool SkipBytes(int length, Status*);
    
    // Read length bytes into the supplied buffer.  The returned buffer is owned
    // by this object.
    bool ReadBytes(int length, uint8_t** buf, Status*);
    
    // Read a Writable Text value from the supplied file.
    // Ref: org.apache.hadoop.io.WritableUtils.readString()
    // The returned buffer is owned by this object.
    bool ReadText(uint8_t** buf, int* length, Status*);
    
    // Skip this text object.
    bool SkipText(Status*);

   private:
    friend class ScannerContext;
    ScannerContext* parent_;
    const DiskIoMgr::ScanRange* scan_range_;
  
    // Byte offset for this scan range
    int64_t scan_range_start_;
    
    // If true, tuple data in the row batches is compact and the io buffers can be
    // recycled immediately.  
    bool compact_data_;

    // Condition variable (with parent_->lock_) for waking up the scanner thread in 
    // Read(). This condition variable is signaled from AddBuffer() and Cancel()
    boost::condition_variable read_ready_cv_;
  
    // Fields below are protected by parent_->lock
    // TODO: we could make this more fine grain and have per stream locks but there's
    // not really a good reason to need to read from multiple streams in parallel (yet)
    
    // Total number of bytes returned from GetBytes()
    int64_t total_bytes_returned_;

    // Byte offset into the current (first) io buffer.
    uint8_t* current_buffer_pos_;

    // Bytes left in the first buffer
    int current_buffer_bytes_left_;
  
    // The buffer size to use for when reading past the end of the scan range.  A
    // default value is pickd and scanners can overwrite it (i.e. the scanner knows
    // more about the file format)
    int read_past_buffer_size_;
    
    // Total number of bytes that's expected to to be read from this stream.  The
    // actual number could be higher if we need to read bytes past the end.
    int64_t total_len_;
  
    // Set to true when a buffer returns the end of the scan range.
    bool read_eosr_;
  
    // Buffers that are ready for the reader
    std::list<DiskIoMgr::BufferDescriptor*> buffers_;

    // Pool for allocating boundary buffers.  
    boost::scoped_ptr<MemPool> boundary_pool_;
    boost::scoped_ptr<StringBuffer> boundary_buffer_;
  
    // List of buffers that are completed but still have bytes referenced by the caller.
    // On the next GetBytes() call, these buffers are released (the caller by calling
    // GetBytes() signals it is done with its previous bytes).  At this point the
    // buffers are either returned to the io mgr or attached to the current row batch.
    std::list<DiskIoMgr::BufferDescriptor*> completed_buffers_;
    
    // The current buffer (buffers_.front()) or NULL if there are none available.  This
    // is used for the fast GetBytes path.
    DiskIoMgr::BufferDescriptor* current_buffer_;
  
    Stream(ScannerContext* parent);

    // This must be called once when the rest buffer arrives.  This initializes
    // some tracking state for this stream.
    void SetInitialBuffer(DiskIoMgr::BufferDescriptor* buffer);

    // GetBytes helper to handle the slow path 
    // If peek is set then return the data but do not move the current offset.
    Status GetBytesInternal(int requested_len, uint8_t** buffer,
                            bool peek, int* out_len, bool* eos);
  
    // Removes the first buffer from the queue, adding it to the row batch if necessary.
    void RemoveFirstBuffer();
  
    // Attach all completed io buffers and any boundary mem pools to the current batch.
    // If done, this is the final call and any pending resources in the stream should
    // be passed to the row batch.
    void AttachCompletedResources(bool done);

    // Returns all buffers queued on this stream to the io mgr.
    void ReturnAllBuffers();
  };

  Stream* GetStream(int idx = 0) { 
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, streams_.size());
    return streams_[idx]; 
  }
  
  RowBatch* current_row_batch() { return  current_row_batch_; }

  // Gets memory for outputting tuples.   
  //  *pool is the mem pool that should be used for memory allocated for those tuples.
  //  *tuple_mem should be the location to output tuples, and 
  //  *tuple_row_mem for outputting tuple rows.  
  // Returns the maximum number of tuples/tuple rows that can be output (before the
  // current row batch is complete and a new one is allocated).
  // This should only be called from the scanner thread.
  // Memory returned from this call is invalidated after calling CommitRows or
  // GetBytes. Callers must call GetMemory again after calling either of these
  // functions.
  int GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem);

  // Commit num_rows to the current row batch.  If this completes the row batch, the
  // row batch is enqueued with the scan node.
  // This should only be called from the scanner thread.
  void CommitRows(int num_rows);

  // Creates streams for this context.  This can only be called once in the lifetime 
  // of this object.
  // Any existing streams are reset.
  void CreateStreams(int num_streams);

  // Close() and Cancel() are used together to coordinate proper cleanup.
  // Valid call orders are:
  //  - Close(): normal case when the scanner finishes
  //  - Cancel() -> Close(): scanner is cancelled asynchronously
  //  - Close() -> Cancel(): cancel() is ignored, scan range is already complete.
  // Note that Close() must always called exactly once for each context object.

  // This function must be called when the scanner is complete and no longer needs
  // any resources (e.g. tuple memory, io buffers, etc) returned from the scan range
  // context.  This should be called from the scanner thread.
  // AcquirePool must be called on all MemPools that contain data for this context
  // before calling flush.
  // This must be called even in the error path to clean up any pending resources.
  void Close();

  // This function can be called to terminate the scanner thread asynchronously.
  // This can be called from any thread.
  void Cancel();

  // Release all memory in 'pool' to the current row batch.  
  void AcquirePool(MemPool* pool) {
    DCHECK(current_row_batch_ != NULL);
    DCHECK(pool != NULL);
    current_row_batch_->tuple_data_pool()->AcquireData(pool, false);
  }

  // If true, the scanner is cancelled and the scanner thread should finish up
  bool cancelled() const { return cancelled_; }

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
  friend class Stream;

  RuntimeState* state_;
  HdfsScanNode* scan_node_;

  int tuple_byte_size_;

  HdfsPartitionDescriptor* partition_desc_;

  // Template tuple for this scan range
  Tuple* template_tuple_;

  // Current row batch that tuples are being written to.
  RowBatch* current_row_batch_;

  // Tuple memory for current row batch.
  uint8_t* tuple_mem_;

  // Lock to protect fields below.  
  boost::mutex lock_;

  // Vector of streams.  Non-columnar formats will always have one stream per context.
  std::vector<Stream*> streams_;

  // If true, flush has been called.
  bool done_;

  // If true, the scan range has been cancelled and the scanner thread should abort
  bool cancelled_;

  // Create a new row batch and tuple buffer.
  void NewRowBatch();

  // Attach all resources to the current row batch and send the batch to the scan node.
  void AddFinalBatch();
};
  
}

#endif

