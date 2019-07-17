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

#include "runtime/blocking-row-batch-queue.h"
#include "runtime/row-batch.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace std;

namespace impala {

BlockingRowBatchQueue::BlockingRowBatchQueue(int max_batches, int64_t max_bytes,
    RuntimeProfile::Counter* get_batch_wait_timer,
    RuntimeProfile::Counter* add_batch_wait_timer)
  : batch_queue_(new BlockingQueue<std::unique_ptr<RowBatch>, RowBatchBytesFn>(
        max_batches, max_bytes, get_batch_wait_timer, add_batch_wait_timer)) {}

BlockingRowBatchQueue::~BlockingRowBatchQueue() {
  DCHECK(cleanup_queue_.empty());
}

void BlockingRowBatchQueue::AddBatch(unique_ptr<RowBatch> batch) {
  if (!batch_queue_->BlockingPut(move(batch))) {
    lock_guard<SpinLock> l(lock_);
    cleanup_queue_.push_back(move(batch));
  }
}

bool BlockingRowBatchQueue::AddBatchWithTimeout(
    unique_ptr<RowBatch>&& batch, int64_t timeout_micros) {
  return batch_queue_->BlockingPutWithTimeout(
      forward<unique_ptr<RowBatch>>(batch), timeout_micros);
}

unique_ptr<RowBatch> BlockingRowBatchQueue::GetBatch() {
  unique_ptr<RowBatch> result;
  if (batch_queue_->BlockingGet(&result)) return result;
  return unique_ptr<RowBatch>();
}

bool BlockingRowBatchQueue::IsFull() const {
  return batch_queue_->AtCapacity();
}

void BlockingRowBatchQueue::Shutdown() {
  batch_queue_->Shutdown();
}

void BlockingRowBatchQueue::Cleanup() {
  unique_ptr<RowBatch> batch = nullptr;
  while ((batch = GetBatch()) != nullptr) {
    batch.reset();
  }

  lock_guard<SpinLock> l(lock_);
  cleanup_queue_.clear();
}
}
