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


#ifndef IMPALA_EXEC_BUFFERED_BYTE_STREAM_H_
#define IMPALA_EXEC_BUFFERED_BYTE_STREAM_H_

#include <string>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>

#include "util/runtime-profile.h"
#include "exec/byte-stream.h"
#include "exec/hdfs-scan-node.h"
#include "runtime/mem-pool.h"
#include "common/status.h"

namespace impala {

// A Buffered ByteStream implementation.
// This class provides buffered reads from the underlying parent byte stream.
// TODO: This is needed because of the way SerDeUtils work, we should revisit this.
class BufferedByteStream : public ByteStream {
 public:
  // Inputs:
  //   parent: byte stream the actually reads the data.
  //   buffer_size: size of buffer to allocate.
  //   scan_node: scan node using this byte stream, used for statistics.
  BufferedByteStream(ByteStream* parent,
      int64_t buffer_size, HdfsScanNode* scan_node);

  virtual ~BufferedByteStream();
  virtual Status Open(const std::string& location);
  virtual Status Close();
  virtual Status Read(uint8_t* buf, int64_t req_len, int64_t* actual_len);
  virtual Status Seek(int64_t offset);
  virtual Status SeekRelative(int64_t offset) {
    return Seek(byte_buffer_start_ + byte_offset_ + offset);
  }
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

  // Position of start of buffer in parent byte stream.
  int64_t byte_buffer_start_;

  // Amount of data in buffer.
  int64_t byte_buffer_len_;

  // Scan node from caller so we can record counters.
  HdfsScanNode* scan_node_;
};

}

#endif
