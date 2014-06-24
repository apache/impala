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

#ifndef IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_H
#define IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_H

#include "common/status.h"
#include "runtime/buffered-block-mgr.h"

namespace impala {

class BufferedBlockMgr;
class RuntimeProfile;
class RuntimeState;
class RowBatch;
class RowDescriptor;
class SlotDescriptor;
class TupleRow;

// Class that provides an abstraction for a stream of tuple rows. Rows can be
// added to the stream and returned. There is no constraint on the order the rows
// are returned.
//
// The underlying memory management is done by the BufferedBlockMgr.
//
// The BufferedTupleStream is *not* thread safe from the caller's point of view. It is
// expected that all the APIs are called from a single thread. Internally, the
// object is thread safe wrt to the underlying block mgr.
//
// Buffer management: The tuple stream can do memory management differently on both
// the read and write path.
// Read:
//   1. Delete on read: Blocks are deleted as we go through the stream. The data
//   returned by the tuple stream is valid until the next read call so the caller does
//   not need to copy if it is streaming.
//   2. Pinned: Blocks are kept pinned on read (useful to keep a whole stream in memory)
// Write:
//   1. Unpin written blocks. Unpin blocks as they fill up. This means only a single
//   (i.e. the current) block needs to be in memory regardless of the input size.
//   2. Don't unpin. If we run out of blocks, the write will fail and the caller needs
//   to free memory from the underlying block mgr.
//
// Tuple row layout: Tuples are stored back to back. Each tuple starts with the fixed
// length portion, directly followed by the var len portion. (Fixed len and var len
// are interleaved).
//
// TODO: we need to be able to do read ahead in the BufferedBlockMgr. It currently
// only has Pin() which is blocking. We need a non-blocking version of this or some way
// to indicate a block will need to be pinned soon.
// TODO: see if this can be merged with Sorter::Run. The key difference is that this
// does not need to return rows in the order they were added, which allows it to be
// simpler.
// TODO: the API as is, does not allow rereading the stream. This is trivial to add
// when necessary.
// TODO: improvements:
//   - Think about how to layout for the var len data more, possibly filling in them
//     from the end of the same block. Don't interleave fixed and var len data.
//   - We will want to multithread this. Add a AddBlock() call so the synchronization
//     happens at the block level. This is a natural extension.
//   - Instead of allocating all blocks from the block_mgr, allocate some blocks that
//     are much smaller (e.g. 16K and doubling up to the block size). This way, very
//     small streams (a common case) will use very little memory. This small blocks
//     are always in memory since spilling them frees up negligible memory.
//   - Return row batches in GetNext() instead of filling one in.
class BufferedTupleStream {
 public:
  // row_desc: description of rows stored in the stream. This is the desc for rows
  // that are added and the rows being returned.
  // block_mgr: Underlying block mgr that owns the data blocks.
  // The tuple stream starts off with Read 1 & Write 2 behavior (above).
  BufferedTupleStream(RuntimeState* state, const RowDescriptor& row_desc,
      BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client);

  // Initializes the tuple stream object. Must be called once before any of the
  // other APIs.
  Status Init();

  // Adds a single row to the stream. Returns false if an error occurred.
  // BufferedTupleStream will do a deep copy of the memory in the row.
  bool AddRow(TupleRow* row);

  // Called to indicate all input is done and rewinds the stream to the beginning.
  // Must be called after the last AddRow() and before GetNext().
  // If pin is true, the entire stream will be pinned in memory. If there is not
  // enough memory *pinned will be set to false.
  Status PrepareForRead(bool pin = false, bool* pinned = NULL);

  // Unpins all blocks in this stream except the current block and switches to
  // write mode 1.
  Status Unpin();

  // Get the next batch of output rows. Memory is still owned by the BufferedTupleStream
  // and must be copied out by the caller.
  Status GetNext(RowBatch* batch, bool* eos);

  // Must be called once at the end to cleanup all resources.
  void Close();

  // Returns the status of the stream. We don't want to return a more costly Status
  // object on AddRow() which is way that API returns a bool.
  Status status() const { return status_; }

  // Number of rows in the stream.
  int64_t num_rows() const { return num_rows_; }

  // Number of rows returned via GetNext().
  int64_t rows_returned() const { return rows_returned_; }

  // Returns the byte size necessary to store the entire stream in memory.
  int64_t byte_size() const {
    int num_blocks = 0;
    num_blocks += pinned_blocks_.size() + unpinned_blocks_.size();
    if (current_block_ != NULL) ++num_blocks;
    return num_blocks * block_mgr_->block_size();
  }

  // Returns the byte size of the stream that is currently pinned in memory.
  // If ignore_current is true, that memory is not included.
  int64_t bytes_in_mem(bool ignore_current) const;

  // Returns the number of bytes that are in unpinned blocks.
  int64_t bytes_unpinned() const;

 private:
  // Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  // Description of rows stored in the stream.
  const RowDescriptor& desc_;

  // Sum of the fixed length portition of all the tuples in desc_.
  int fixed_tuple_row_size_;

  // Vector of all the strings slots grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*> > > string_slots_;

  // Block manager and client used to allocate, pin and release blocks. Not owned.
  BufferedBlockMgr* block_mgr_;
  BufferedBlockMgr::Client* block_mgr_client_;

  // Lock to protect state that is synchronized better the main (caller) thread
  // and the callback from the block_mgr_.
  SpinLock lock_;

  // The current block for read or write. NULL at eos on the read path. NULL
  // on the write path if there is no available block to write to.
  // This block is not in pinned_blocks_ or unpinned_blocks_.
  BufferedBlockMgr::Block* current_block_;

  // Current ptr offset in current_block_'s buffer.
  uint8_t* read_ptr_;

  // Remaining bytes left in read_ptr_.
  int64_t read_bytes_left_;

  // Blocks that make up the stream, split between ones that are pinned and not pinned.
  std::list<BufferedBlockMgr::Block*> pinned_blocks_;
  std::list<BufferedBlockMgr::Block*> unpinned_blocks_;

  // Number of rows returned to the caller from GetNext().
  int64_t rows_returned_;

  Status status_;

  // Number of rows stored in the stream.
  int64_t num_rows_;

  // If true, this stream has been explicitly pinned by the caller. This changes
  // the memory management of the stream. The blocks are not unpinned until the
  // caller calls Unpin().
  bool pinned_;

  // If true, this stream automatically unpins on the write path, meaning it will
  // only keep one block (current_block_) pinned in memory.
  bool auto_unpin_;

  // Copies row into 'current_block_'. Returns false if there is not enough space
  // in 'current_block_'.
  bool DeepCopy(TupleRow* row);

  // Gets a new block from the block_mgr_, updating current_block_ and
  // setting *got_block. If there are no blocks available, current_block_ is set to NULL
  // and *got_block is set to false.
  Status NewBlockForWrite(bool* got_block);

  // Reads the next block from the block_mgr_. This blocks if necessary.
  // Updates current_block_, read_ptr_ and read_bytes_left_.
  Status NextBlockForRead();
};

}

#endif
