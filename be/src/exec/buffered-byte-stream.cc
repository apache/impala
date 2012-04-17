// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/buffered-byte-stream.h"
#include "common/status.h"
#include <glog/logging.h>
#include <sstream>

using namespace impala;
using namespace std;

BufferedByteStream::BufferedByteStream(ByteStream* parent, int64_t buffer_size,
                                       HdfsScanNode* scan_node)
    : parent_byte_stream_(parent),
      mem_pool_(new MemPool()),
      byte_buffer_size_(buffer_size),
      byte_buffer_(mem_pool_->Allocate(byte_buffer_size_)),
      byte_offset_(0),
      byte_buffer_start_(0),
      byte_buffer_len_(0),
      scan_node_(scan_node) {
}

BufferedByteStream::~BufferedByteStream() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(), mem_pool_->peak_allocated_bytes());
}

Status BufferedByteStream::GetPosition(int64_t* position) {
  *position = byte_buffer_start_ + byte_offset_;
  return Status::OK;
}

Status BufferedByteStream::Open(const string& location) {
  return Status::OK;
}

Status BufferedByteStream::Read(uint8_t* buf, int64_t req_len, int64_t* actual_len) {
  DCHECK(buf != NULL);
  DCHECK_GE(req_len, 0);

  int number_bytes_read = 0;
  if (req_len <= byte_buffer_len_ - byte_offset_) {
    memcpy(buf, byte_buffer_ + byte_offset_, req_len);
    number_bytes_read = req_len;
    byte_offset_ += number_bytes_read;
  } else {
    while (number_bytes_read < req_len) {
      int copy_len = min(byte_buffer_len_ - byte_offset_, req_len - number_bytes_read);
      memcpy(buf + number_bytes_read, byte_buffer_ + byte_offset_, copy_len);
      number_bytes_read += copy_len;
      byte_offset_ += copy_len;
      if (byte_offset_ == byte_buffer_len_) {
        byte_buffer_start_ += byte_buffer_len_;
        {
          COUNTER_SCOPED_TIMER(scan_node_->scanner_timer());
          RETURN_IF_ERROR(parent_byte_stream_->Read(
              byte_buffer_, byte_buffer_size_, &byte_buffer_len_));
        }
        byte_offset_ = 0;

        if (byte_buffer_len_ == 0) break;
      }
    }
  }

  *actual_len = number_bytes_read;
  return Status::OK;
}

Status BufferedByteStream::Close() {
  return Status::OK;
}

Status BufferedByteStream::Seek(int64_t offset) {
  if (offset >= byte_buffer_start_ && offset < byte_buffer_start_ + byte_buffer_len_) {
    byte_offset_ = offset - byte_buffer_start_;
  } else {
    RETURN_IF_ERROR(parent_byte_stream_->Seek(offset));
    byte_buffer_start_ = offset;
    byte_buffer_len_ = 0;
    byte_offset_ = 0;
  }

  return Status::OK;
}

Status BufferedByteStream::Eof(bool* eof) {
  if (byte_offset_ < byte_buffer_len_) {
    *eof = false;
    return Status::OK;
  }
  RETURN_IF_ERROR(SyncParent());
  RETURN_IF_ERROR(parent_byte_stream_->Eof(eof));
  return Status::OK;
}
