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


#ifndef IMPALA_RUNTIME_DISK_STRUCTS_H
#define IMPALA_RUNTIME_DISK_STRUCTS_H

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include "buffer-pool.h"

namespace impala {

// Marks the position of data within a file.
struct FilePosition {
  const char* file;
  int64_t offset;
  int disk_id;

  FilePosition() : file(NULL), offset(0), disk_id(0) {
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << "file=" << file << " disk_id=" << disk_id << " offset=" << offset;
    return ss.str();
  }
};

// A Block represents data which is either in memory or on disk (or both).
// Blocks provide a Pin() and Unpin() interface to ensure they remain in memory
// when expected. These methods, along with ReleaseBufferIfUnpinned(), are thread-safe.
// Otherwise, Block methods are not thread-safe.
class Block {
 public:
  FilePosition file_pos;

  Block()
      : in_mem_(false), pincount_(0), persisted_(false), do_not_persist_(false),
        buf_desc_(NULL), len_(0) {
  }

  bool in_mem() const { return in_mem_; }
  bool persisted() const { return persisted_; }
  bool should_not_persist() const { return do_not_persist_; }
  BufferPool::BufferDescriptor* buf_desc() const { return buf_desc_; }
  int64_t len() const { return len_; }

  void DoNotPersist() { do_not_persist_ = true; }
  void SetBuffer(BufferPool::BufferDescriptor* buf_desc) {
    boost::lock_guard<boost::mutex> guard(mem_lock_);
    buf_desc_ = buf_desc;
    in_mem_ = true;
  }
  void IncrementLength(int64_t inc) { len_ += inc; }

  // Pins the Block, preventing a transition from in_mem -> !in_mem.
  // Note that the Block may not be in memory when pinned.
  void Pin() {
    boost::lock_guard<boost::mutex> guard(mem_lock_);
    DCHECK_GE(pincount_, 0);
    ++pincount_;
  }

  // Unpins the Block. If the pincount becomes 0, the Block's buffer can be released
  // at any point.
  void Unpin() {
    boost::lock_guard<boost::mutex> guard(mem_lock_);
    --pincount_;
    DCHECK(in_mem_);
    DCHECK_GE(pincount_, 0);
  }

  // Releases our in-memory buffer if it's unpinned. Returns true if we released it.
  bool ReleaseBufferIfUnpinned() {
    boost::lock_guard<boost::mutex> guard(mem_lock_);
    if (pincount_ > 0) return false;
    if (!in_mem_) return false;
    CHECK(buf_desc_ != NULL);
    buf_desc_->Return();
    in_mem_ = false;
    buf_desc_ = NULL;
    return true;
  }

 private:
  friend class DiskWriter;
  void SetPersisted(bool persisted) { persisted_ = persisted; }

  bool in_mem_;
  int pincount_;
  bool persisted_;
  bool do_not_persist_;
  BufferPool::BufferDescriptor* buf_desc_;

  // The length is the logical number of bytes addressed by this Block.
  int64_t len_;

  // Locks in_mem_, buf_desc_, and pincount_.
  boost::mutex mem_lock_;
};

}

#endif
