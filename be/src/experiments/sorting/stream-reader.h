// Copyright 2013 Cloudera Inc.
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


#ifndef IMPALA_SORTING_STREAM_READER_H_
#define IMPALA_SORTING_STREAM_READER_H_

#include <boost/scoped_ptr.hpp>

#include "runtime/disk-io-mgr.h"

namespace impala {

// Provides an interface to read a set of ordered, disjoint ScanRanges from disk
// via a vector of Blocks which are disk-resident.
// An assumption made by this Reader is that the size of the Block is less than
// the size of the Block read from disk.
//
// The caller initially provides a tentative set of Blocks which are known to be out of
// memory (and thus will be read eventually). These Blocks are double buffered
// in order to provide fast sequential access. The caller is also allowed to request
// Blocks other than the ones provided initially -- however, these reads would be
// unbuffered and thus cause performance to degrade.
class StreamReader {
 public:
  StreamReader(DiskIoMgr* io_mgr, DiskIoMgr::ReaderContext* reader,
      BufferPool* buffer_pool, const std::vector<Block*>& blocks,
      RuntimeProfile::Counter* blocks_read, RuntimeProfile::Counter* buffer_misses)
      : io_mgr_(io_mgr), reader_(reader), buffer_pool_(buffer_pool), blocks_(blocks),
        blocks_read_(blocks_read), buffer_misses_(buffer_misses) {
    blocks_iterator_ = blocks_.begin();
  }

  ~StreamReader() {
    // IoMgr complains if we didn't use all of our ScanRanges
    DCHECK(blocks_iterator_ == blocks_.end());
  }

  // Prepares the StreamReader by opening the ReaderContext on the IO Manager
  // and scheduling the read for the first Block.
  void Prepare() {
    if (!blocks_.empty()) cur_scan_range_.reset(AddScanRange(*blocks_iterator_));
  }

  // Reads the next Block from disk, which should be 'block'.
  // If the given Block was not part of the original set of out-of-memory blocks,
  // we will have to do a fully synchronous read.
  void ReadBlock(Block* block) {
    DiskIoMgr::ScanRange* scan_range;
    if (blocks_iterator_ != blocks_.end() && *blocks_iterator_ == block) {
      DCHECK(cur_scan_range_.get() != NULL);
      scan_range = cur_scan_range_.get();
    } else {
      COUNTER_UPDATE(buffer_misses_, 1);
      scan_range = AddScanRange(block);
    }

    DiskIoMgr::BufferDescriptor* buffer;
    scan_range->GetNext(&buffer);
    COUNTER_UPDATE(blocks_read_, 1);
    DCHECK(buffer->eosr()); // TODO: Not necessarily

    // Get a Buffer of our own, copy the data into it, and return the IO Mgr's buffer.
    DCHECK_EQ(buffer->scan_range()->meta_data(), block);
    block->SetBuffer(buffer_pool_->GetBuffer());
    DCHECK_GE(block->buf_desc()->size, buffer->len());
    DCHECK_LE(buffer->len(), block->len());
    memcpy(block->buf_desc()->buffer, buffer->buffer(), buffer->len());
    // TODO: Should be able to just take over the IoMgr::BufferDescriptor.
    buffer->Return();

    // Schedule the next scan range (if we used the currently buffered one).
    if (blocks_iterator_ != blocks_.end() && *blocks_iterator_ == block) {
      ++blocks_iterator_;
      DiskIoMgr::ScanRange* next_range = NULL;
      if (blocks_iterator_ != blocks_.end()) {
        next_range = AddScanRange(*blocks_iterator_);
      }
      cur_scan_range_.reset(next_range);
    }
  }

 private:
  // Adds a ScanRange which will read the given Block.
  // The ScanRange is scheduled immediately.
  DiskIoMgr::ScanRange* AddScanRange(Block* block) {
    DCHECK(!block->in_mem());
    DiskIoMgr::ScanRange* scan_range = new DiskIoMgr::ScanRange();
    std::vector<FilePosition*> fpos;
    fpos.push_back(&block->file_pos);
    scan_range->Reset(block->file_pos.file, block->len(),
                      block->file_pos.offset, block->file_pos.disk_id, block);
    std::vector<DiskIoMgr::ScanRange*> ranges;
    ranges.push_back(scan_range);
    io_mgr_->AddScanRanges(reader_, ranges, true);
    return scan_range;
  }

  DiskIoMgr* io_mgr_;
  DiskIoMgr::ReaderContext* reader_;
  BufferPool* buffer_pool_;
  std::vector<Block*> blocks_;
  std::vector<Block*>::iterator blocks_iterator_;

  boost::scoped_ptr<DiskIoMgr::ScanRange> cur_scan_range_;

  RuntimeProfile::Counter* blocks_read_;
  RuntimeProfile::Counter* buffer_misses_;
};

}

#endif
