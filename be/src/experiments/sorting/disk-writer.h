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


#ifndef IMPALA_RUNTIME_DISK_WRITER_H
#define IMPALA_RUNTIME_DISK_WRITER_H

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include <fcntl.h>
#include <vector>

#include "buffer-pool.h"
#include "disk-structs.h"
#include "util/blocking-queue.h"

namespace impala {

// Mocked up interface to write stuff to disk. Main interface is EnqueueBlock()
// to enqueue a block to be written to disk.
// TODO: Real implementation in IoMgr would stripe Blocks across disk in some
// load-friendly way (randomly, sequentially, more intelligently). It would also delete
// the created temporary files at the conclusion of a query.
class DiskWriter {
 public:
  class BufferManager;

  DiskWriter() : queued_writes_(1024), shutdown_(false) {
  }

  void Init();

  void EnqueueBlock(Block* block, BufferManager* buffer_manager) {
    queued_writes_.BlockingPut(EnqueuedBlock(block, buffer_manager));
  }

  void Cancel();

  // Manages the movement of buffers to disk.
  // Buffers are placed into the BufferManager as soon as they are no longer needed,
  // and they are written to disk as soon as possible.
  // When a Buffer is successfully written to disk, it goes onto the persisted_blocks_
  // vector. We avoid actually returning these buffer to the BufferPool until
  // the BufferPool runs out of buffers to assign.
  // This policy maximizes our disk writing throughput while keeping Blocks in memory
  // for as long as possible.
  class BufferManager {
   public:
    BufferManager(BufferPool* buffer_pool, DiskWriter* writer);

    ~BufferManager();

    // Enqueue a single Block.
    // Smaller priority -> will be sent to disk first.
    void EnqueueBlock(Block* block, int priority);

    // Flushes the next Block out of the BufferDiskManager to disk.
    // "Next" is determined by Run length -- we will flush out the last block of the
    // Run which has the most blocks still in memory.
    // This call does not block.
    void FlushNextBlock();

    // Finds a block that has been persisted to disk (and is unpinned),
    // and returns its buffer to the  BufferPool.
    bool ReturnPersistedBuffer();

    // Called by the BufferPool when a free buffer is needed.
    bool FreeBufferRequested();

    // Called right after a Block is written to disk.
    // Note that this is called even if the block was pinned -- this is so we can
    // enqueue the next block onto the disk queue.
    void BlockWritten(Block* block);

    void StopFlushingBuffers() { stop_disk_flush_ = true; }

   private:
    friend class DiskWriter;

    // A set of in-memory blocks that we are managing.
    // Provides a natural ordering based on priorities assigned to blocks.
    struct PriorityBlock {
      Block* block;
      int priority;

      PriorityBlock(Block* block, int priority)
        : block(block), priority(priority) {
      }

      // Max heap, so x.priority < y.priority -> x > y (should be at top of heap).
      bool operator< (const PriorityBlock& rhs) {
        return priority > rhs.priority;
      }
    };

    BufferPool* buffer_pool_;
    DiskWriter* writer_;

    // True if we should stop writing buffers to disk.
    bool stop_disk_flush_;

    // The max-heap that allows us to choose which Block to next flush out.
    std::vector<PriorityBlock> priority_blocks_heap_;

    // The number of blocks we are managing (i.e., have been Enqueued but not Flushed).
    int num_blocks_managed_;

    // The number of blocks that have been Flushed but not Written.
    AtomicInt<int> num_blocks_disk_queue_;

    // Number of buffers which the BufferPool has requested to be freed (but which we
    // didn't have any buffers available to free).
    // This is an upper bound on the number of threads waiting in the BufferPool
    // for freed buffers.
    int num_freed_buffers_requested_;

    // Guards priority_blocks_heap_, num_blocks_managed_, and num_blocks_disk_queue_.
    boost::mutex disk_lock_;

    std::vector<Block*> persisted_blocks_;

    // Locks persisted_blocks_ and num_freed_buffers_reuqested_.
    boost::mutex persisted_blocks_lock_;
  };

 private:
  void WriterThread(int disk_id);

  struct EnqueuedBlock {
    Block* block;
    BufferManager* buffer_manager;

    EnqueuedBlock(Block* block, BufferManager* buffer_manager)
      : block(block), buffer_manager(buffer_manager) {
    }

    EnqueuedBlock() {
    }
  };

 private:
  FILE* my_file_;
  boost::thread_group workers_;
  BlockingQueue<EnqueuedBlock> queued_writes_;
  bool shutdown_;

  boost::scoped_ptr<BufferManager> buffer_manager_;

  // Serializes calls to Cancel().
  boost::mutex cancel_lock_;
};

}

#endif
