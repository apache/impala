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

#ifndef IMPALA_RUNTIME_BLOCKING_QUEUE_H
#define IMPALA_RUNTIME_BLOCKING_QUEUE_H

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

/// Extends blocking queue for row batches. Row batches have a property that
/// they must be processed in the order they were produced, even in cancellation
/// paths. Preceding row batches can contain ptrs to memory in subsequent row batches
/// and we need to make sure those ptrs stay valid.
/// Row batches that are added after Shutdown() are queued in a separate "cleanup"
/// queue, which can be cleaned up during Close().
///
/// The queue supports limiting the capacity in terms of bytes enqueued.
///
/// All functions are thread safe.
class RowBatchQueue : public BlockingQueue<std::unique_ptr<RowBatch>, RowBatchBytesFn> {
 public:
  /// 'max_batches' is the maximum number of row batches that can be queued.
  /// 'max_bytes' is the maximum number of bytes of row batches that can be queued (-1
  /// means no limit).
  /// When the queue is full, producers will block.
  RowBatchQueue(int max_batches, int64_t max_bytes);
  ~RowBatchQueue();

  /// Adds a batch to the queue. This is blocking if the queue is full.
  void AddBatch(std::unique_ptr<RowBatch> batch);

  /// Gets a row batch from the queue. Returns NULL if there are no more.
  /// This function blocks.
  /// Returns NULL after Shutdown().
  std::unique_ptr<RowBatch> GetBatch();

  /// Deletes all row batches in cleanup_queue_. Not valid to call AddBatch()
  /// after this is called.
  void Cleanup();

 private:
  /// Lock protecting cleanup_queue_
  SpinLock lock_;

  /// Queue of orphaned row batches
  std::list<std::unique_ptr<RowBatch>> cleanup_queue_;
};
}
#endif
