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

#include "runtime/row-batch-queue.h"

#include "runtime/row-batch.h"

#include "common/names.h"

namespace impala {

RowBatchQueue::RowBatchQueue(int max_batches, int64_t max_bytes)
  : BlockingQueue<unique_ptr<RowBatch>,RowBatchBytesFn>(max_batches, max_bytes) {}

RowBatchQueue::~RowBatchQueue() {
  DCHECK(cleanup_queue_.empty());
}

void RowBatchQueue::AddBatch(unique_ptr<RowBatch> batch) {
  if (!BlockingPut(move(batch))) {
    lock_guard<SpinLock> l(lock_);
    cleanup_queue_.push_back(move(batch));
  }
}

unique_ptr<RowBatch> RowBatchQueue::GetBatch() {
  unique_ptr<RowBatch> result;
  if (BlockingGet(&result)) return result;
  return unique_ptr<RowBatch>();
}

void RowBatchQueue::Cleanup() {
  unique_ptr<RowBatch> batch = nullptr;
  while ((batch = GetBatch()) != nullptr) {
    batch.reset();
  }

  lock_guard<SpinLock> l(lock_);
  cleanup_queue_.clear();
}
}
