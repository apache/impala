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

#include "common/compiler-util.h"
#include "common/status.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/row-batch.h"

namespace impala {

struct HdfsFileDesc;
class HdfsPartitionDescriptor;
class HdfsScanNode;
class MemPool;
class RowBatch;
class RuntimeState;
class StringBuffer;
class Tuple;
class TupleRow;

// This class abstracts over getting buffers from the IoMgr. Each ScannerContext is 1:1 
// a HdfsScanner. ScannerContexts contain Streams, which are 1:1 with a ScanRange.
// Columnar formats have multiple streams per context object.
// This class handles stitching data split across IO buffers and providing
// some basic parsing utilities.
// This class it *not* thread safe. It is designed to have a single scanner thread
// reading from it.
//
// Each scanner context maps to a single hdfs split.  There are three threads that
// are interacting with the context.
//   1. IoMgr threads that read io buffers from the disk and enqueue them to the
//      stream's underlying ScanRange object. This is the producer.
//   2. Scanner thread that calls GetBytes (which can block), materializing tuples
//      from processing the bytes. This is the consumer.
//   3. The scan node/main thread which calls into the context to trigger cancellation
//      or other end of stream conditions.
class ScannerContext {
 public:
  // Create a scanner context with the parent scan_node (where materialized row batches
  // get pushed to) and the scan range to process.
  // This context starts with 1 stream.
  ScannerContext(RuntimeState*, HdfsScanNode*, HdfsPartitionDescriptor*,
      DiskIoMgr::ScanRange* scan_range);

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

    // If true, the stream has reached the end of the file.
    bool eof();

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
    DiskIoMgr::ScanRange* scan_range_;
    const HdfsFileDesc* file_desc_;
  
    // Byte offset for this scan range
    int64_t scan_range_start_;
    
    // If true, tuple data in the row batches is compact and the io buffers can be
    // recycled immediately.  
    bool compact_data_;

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
  
    // Pool for allocating boundary buffers.  
    boost::scoped_ptr<MemPool> boundary_pool_;
    boost::scoped_ptr<StringBuffer> boundary_buffer_;
  
    // List of buffers that are completed but still have bytes referenced by the caller.
    // On the next GetBytes() call, these buffers are released (the caller by calling
    // GetBytes() signals it is done with its previous bytes).  At this point the
    // buffers are either returned to the io mgr or attached to the current row batch.
    std::list<DiskIoMgr::BufferDescriptor*> completed_buffers_;
    
    // The current io buffer. This starts as NULL before we've read any bytes
    // and then becomes NULL when we've finished the scan range.
    DiskIoMgr::BufferDescriptor* current_buffer_;
  
    Stream(ScannerContext* parent);

    // GetBytes helper to handle the slow path 
    // If peek is set then return the data but do not move the current offset.
    // Updates current_buffer_.
    Status GetBytesInternal(int requested_len, uint8_t** buffer,
                            bool peek, int* out_len, bool* eos);

    // Gets (and blocks) for the next io buffer.
    // Updates current_buffer_.
    Status GetNextBuffer();

    // Attach all completed io buffers and the boundary mem pool to batch.
    void AttachCompletedResources(RowBatch* batch, bool done);

    // Returns all buffers queued on this stream to the io mgr.
    void ReturnAllBuffers();
  };

  Stream* GetStream(int idx = 0) { 
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, streams_.size());
    return streams_[idx]; 
  }

  // Attach completed io buffers and boundary mem pools from all streams to 'batch'.
  // Attaching only completed resources ensures that buffers (and their cleanup) trail the
  // rows that reference them (row batches are consumed and cleaned up in order by the
  // rest of the query).
  // If 'done' is true, this is the final call and any pending resources in the stream are
  // also passed to the row batch.
  void AttachCompletedResources(RowBatch* batch, bool done);

  // Closes any existing streams, returning all their resources.
  void CloseStreams();

  // Add a stream to this ScannerContext for 'range'. Returns the added stream.
  // The stream is created in the runtime state's object pool
  Stream* AddStream(DiskIoMgr::ScanRange* range);

  // This function must be called when the scanner is complete and no longer needs
  // any resources (e.g. tuple memory, io buffers, etc) returned from the scan range
  // context.  This should be called from the scanner thread.
  // This must be called even in the error path to clean up any pending resources.
  void Close();

  // If true, the ScanNode has been cancelled and the scanner thread should finish up
  bool cancelled() const;

  HdfsPartitionDescriptor* partition_descriptor() { return partition_desc_; }

 private:
  friend class Stream;

  RuntimeState* state_;
  HdfsScanNode* scan_node_;

  HdfsPartitionDescriptor* partition_desc_;

  // Vector of streams.  Non-columnar formats will always have one stream per context.
  std::vector<Stream*> streams_;
};

}

#endif

