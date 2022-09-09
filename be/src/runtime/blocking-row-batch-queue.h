// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <list>
#include <memory>

#include "runtime/row-batch.h"
#include "util/blocking-queue.h"
#include "util/spinlock.h"

namespace impala {

class RowBatch;

/// Functor that returns the bytes in MemPool chunks for a row batch.
/// Note that we don't include attached BufferPool::BufferHandle objects because this
/// queue is only used in scan nodes that don't attach buffers.
struct RowBatchBytesFn {
  int64_t operator()(const std::unique_ptr<RowBatch>& batch) {
    return batch->tuple_data_pool()->total_reserved_bytes();
  }
};

/// Provides blocking queue semantics for row batches. Row batches have a property that
/// they must be processed in the order they were produced, even in cancellation
/// paths. Preceding row batches can contain ptrs to memory in subsequent row batches
/// and we need to make sure those ptrs stay valid.
///
/// Row batches that are added after Shutdown() are queued in a separate "cleanup"
/// queue, which can be cleaned up during Close().
///
/// The queue supports limiting the capacity in terms of bytes enqueued and number of
/// batches to be enqueued.
///
/// The queue takes in two counters: 'get_batch_wait_timer' and 'add_batch_wait_timer'.
/// 'get_batch_wait_timer' tracks how long GetBatch spends blocking waiting for batches
/// to be added to the queue. 'add_batch_wait_timer' tracks how long AddBatch spends
/// blocking waiting for space to be available in the queue.
///
/// All functions are thread safe.
class BlockingRowBatchQueue {
 public:
  /// 'max_batches' is the maximum number of row batches that can be queued.
  /// 'max_bytes' is the maximum number of bytes of row batches that can be queued (-1
  /// means no limit).
  /// 'get_batch_wait_timer' tracks how long GetBatch blocks waiting for batches.
  /// 'add_batch_wait_timer' tracks how long AddBatch blocks waiting for space in the
  /// queue.
  /// When the queue is full, producers will block.
  BlockingRowBatchQueue(int max_batches, int64_t max_bytes,
      RuntimeProfile::Counter* get_batch_wait_timer,
      RuntimeProfile::Counter* add_batch_wait_timer);
  ~BlockingRowBatchQueue();

  /// Adds a batch to the queue. This is blocking if the queue is full.
  void AddBatch(std::unique_ptr<RowBatch> batch);

  /// Adds a batch to the queue waiting for the specified amount of time for space to
  /// be available in the queue. Returns true if the batch was successfully added to the
  /// queue, false otherwise. 'batch' is passed by r-value reference because this method
  /// does not transfer ownership of the 'batch'. This is necessary because this method
  /// may or may not successfully add 'batch' to the queue (depending on if the timeout
  /// was hit).
  bool AddBatchWithTimeout(std::unique_ptr<RowBatch>&& batch, int64_t timeout_micros);

  /// Gets a row batch from the queue, blocks if the queue is empty. Returns NULL if
  /// the queue has already been shutdown.
  std::unique_ptr<RowBatch> GetBatch();

  /// Returns true if the queue is full, false otherwise. Does not account of the current
  /// size of the cleanup queue. A queue is considered full if it either contains the max
  /// number of row batches specified in the constructor, or it contains the max number
  /// of bytes specified in the construtor.
  bool IsFull() const;

  /// Shutdowns the underlying BlockingQueue. Future calls to AddBatch will put the
  /// RowBatch on the cleanup queue. Future calls to GetBatch will continue to return
  /// RowBatches from the BlockingQueue.
  void Shutdown();

  /// Resets all RowBatches currently in the queue and clears the cleanup_queue_. Not
  /// valid to call AddBatch() after this is called. Finalizes all counters started for
  /// this queue.
  void Cleanup();

 private:
  /// Lock protecting cleanup_queue_
  SpinLock lock_;

  /// Queue of orphaned row batches enqueued after the RowBatchQueue has been closed.
  /// They need to exist as preceding row batches may reference buffers owned by row
  /// batches in this queue.
  std::list<std::unique_ptr<RowBatch>> cleanup_queue_;

  /// BlockingQueue that stores the RowBatches
  std::unique_ptr<BlockingQueue<std::unique_ptr<RowBatch>, RowBatchBytesFn>> batch_queue_;
};
}
