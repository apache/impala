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

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "util/runtime-profile.h"

namespace impala {

class MemTracker;
class RowBatch;
class RuntimeState;

/// In-memory exchange. One producer pushes RowBatches which are consumed by multiple
/// consumers. Pull returns the original RowBatch; creating a copy is the responsibility
/// of the caller. Thread-safe. Inspired by StarRocks' multi_cast_local_exchange.
class LocalExchanger {
 public:
  LocalExchanger(int32_t num_consumers): consumer_count_(num_consumers) {
    DCHECK_GT(num_consumers, 0);
    // Use a dummy cell to populate progress so there's always a container we can easily
    // update with a next pointer after Push().
    Cell* dummy = new Cell();
    head_ = tail_ = dummy;
    progress_.resize(num_consumers, dummy);
  }

  ~LocalExchanger() {
    DCHECK(head_ == nullptr) << "Release() must be called to free resources.";
  }

  Status Init(RuntimeProfile* profile, MemTracker* tracker, const std::string& name);

  /// Called by each consumer to get its unique consumer index.
  int32_t Open() {
    std::lock_guard l(mutex_);
    DCHECK(next_consumer_index_ < consumer_count_);
    return next_consumer_index_++;
  }

  /// Store batch and make it available for consumers. Takes ownership of the batch.
  Status Push(std::unique_ptr<RowBatch> batch);

  /// Returns pointer to batch. If no batches are currently available, returns nullptr.
  /// If closed, sets eos to true.
  /// Pointer may be invalid after next call to Pull or Close for the same consumer_index.
  RowBatch* Pull(int32_t consumer_index, bool* eos);

  /// Closes exchanger for producer and signals eos. After this call, no more Push()
  /// calls are allowed.
  void CloseProducer();

  /// Closes exchanger for consumer_index. After this call, no more Pull() calls
  /// are allowed for this consumer.
  void CloseConsumer(int32_t consumer_index);

  /// Blocks until all consumers have closed or timeout_ms elapses.
  /// Returns true if all consumers are done, false if timed out.
  bool ReadFinished(int timeout_ms) const {
    std::unique_lock l(mutex_);
    return all_consumers_done_cv_.wait_for(l, std::chrono::milliseconds(timeout_ms),
        [this]() { return consumers_done_ == consumer_count_; });
  }

  void Release();

 private:
  // Release completed cells. Must be called with mutex_ held.
  void release_cells();

  /// Linked list of batches.
  struct Cell {
    std::unique_ptr<RowBatch> batch;
    Cell* next = nullptr;
    int32_t consumers_left = 0;
  };

  const int32_t consumer_count_;
  RuntimeProfile* runtime_profile_ = nullptr;
  std::unique_ptr<MemTracker> mem_tracker_;
  RuntimeProfile::HighWaterMarkCounter* num_cells_counter_ = nullptr;
  mutable std::mutex mutex_;
  mutable std::condition_variable all_consumers_done_cv_;
  std::condition_variable batch_available_cv_;
  int32_t consumers_done_ = 0;
  int32_t next_consumer_index_ = 0;
  Cell* head_;
  Cell* tail_;
  std::vector<Cell*> progress_;
  bool eos_ = false;
};

}
