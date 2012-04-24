// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_BUFFERED_BYTE_STREAM_H_
#define IMPALA_EXEC_BUFFERED_BYTE_STREAM_H_

#include <string>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>

#include "util/runtime-profile.h"
#include "exec/byte-stream.h"
#include "runtime/mem-pool.h"
#include "common/status.h"

namespace impala {

// A Buffered ByteStream implementation.
// This class provides buffered reads from the underlying parent byte stream.
// TODO: This is needed because of the way SerDeUtils work, we should revisit this.
class BufferedByteStream : public ByteStream {
 public:
  BufferedByteStream(ByteStream* parent,
      int64_t buffer_size, RuntimeProfile::Counter* timer = NULL);

  virtual Status Open(const std::string& location);
  virtual Status Close();
  virtual Status Read(uint8_t* buf, int64_t req_len, int64_t* actual_len);
  virtual Status Seek(int64_t offset);
  virtual Status GetPosition(int64_t* position);
  virtual Status Eof(bool* eof);

  // Set the parent offset to our current position.
  Status SyncParent() {
    RETURN_IF_ERROR(parent_byte_stream_->Seek(byte_buffer_start_ + byte_offset_));
    return Status::OK;
  }

  // Set our posistion to where the parent is.
  Status SeekToParent() {
    int64_t position;
    RETURN_IF_ERROR(parent_byte_stream_->GetPosition(&position));
    RETURN_IF_ERROR(Seek(position));
    return Status::OK;
  }

 private:
  // Pointer to the source byte stream.
  ByteStream* parent_byte_stream_;

  // Memory pool to allocate buffers.
  boost::scoped_ptr<MemPool> mem_pool_;

  // Size of the buffer.
  int64_t byte_buffer_size_;

  // Buffer containing bytes.
  uint8_t* byte_buffer_;

  // Current offset within buffer.
  int64_t byte_offset_;

  // Posistion of start of buffer in parent byte stream.
  int64_t byte_buffer_start_;

  // Amount of data in buffer.
  int64_t byte_buffer_len_;

  RuntimeProfile::Counter* scanner_timer_;

};

}

#endif
