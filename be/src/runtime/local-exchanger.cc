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

#include <gutil/strings/substitute.h>

#include "exec/exec-node.h"
#include "runtime/local-exchanger.h"
#include "runtime/row-batch.h"

using namespace strings;

namespace impala {

Status LocalExchanger::Init(
    RuntimeProfile* profile, MemTracker* tracker, const std::string& name) {
  runtime_profile_ = profile->CreateChild(Substitute(
      "$0$1", RuntimeProfile::PREFIX_LOCAL_EXCHANGER, name), true, true, false);
  mem_tracker_.reset(
      new MemTracker(runtime_profile_, -1, runtime_profile_->name(), tracker));
  mem_tracker_->Consume(sizeof(Cell)); // for dummy cell
  num_cells_counter_ = runtime_profile_->AddHighWaterMarkCounter("MaxCells", TUnit::UNIT);
  return Status::OK();
}

Status LocalExchanger::Push(std::unique_ptr<RowBatch> batch) {
  if (int64_t bytes = sizeof(Cell); !mem_tracker_->TryConsume(bytes)) {
    const string& msg = Substitute("Failed to allocate '$0' bytes for local exchange $1",
        bytes, runtime_profile_->name());
    return mem_tracker_->MemLimitExceeded(NULL, msg, bytes);
  }
  Cell* cell = new Cell();
  cell->batch = std::move(batch);

  std::lock_guard l(mutex_);
  cell->consumers_left = consumer_count_ - consumers_done_;
  tail_->next = cell;
  tail_ = cell;
  num_cells_counter_->Add(1);
  batch_available_cv_.notify_all();
  return Status::OK();
}

RowBatch* LocalExchanger::Pull(int32_t consumer_index, bool* eos) {
  DCHECK(consumer_index >= 0 && consumer_index < consumer_count_);
  std::unique_lock l(mutex_);
  // head_ starts as a dummy cell, and after the first RowBatch has been returned to all
  // consumers we retain it so Push always has a Cell to append to and all consumers can
  // use it before it's deleted; once all consumers have fetched the next Cell, the prior
  // can be deleted. This simplifies tracking progress for each consumer.
  Cell* cell = progress_[consumer_index]->next;
  if (cell == nullptr) {
    // No batches currently available: block until Push delivers one or CloseProducer
    // signals EOS.
    batch_available_cv_.wait(l, [&]() {
      return progress_[consumer_index]->next != nullptr || eos_;
    });
    cell = progress_[consumer_index]->next;
    if (cell == nullptr) {
      // Producer closed with no remaining batches.
      *eos = true;
      return nullptr;
    }
  }
  cell->consumers_left--;
  DCHECK_GE(cell->consumers_left, 0);
  progress_[consumer_index] = cell;
  release_cells();
  *eos = false;
  return cell->batch.get();
}

void LocalExchanger::CloseProducer() {
  std::lock_guard l(mutex_);
  eos_ = true;
  batch_available_cv_.notify_all();
}

void LocalExchanger::CloseConsumer(int32_t consumer_index) {
  DCHECK(consumer_index >= 0 && consumer_index < consumer_count_);
  std::lock_guard l(mutex_);
  while (progress_[consumer_index] != nullptr) {
    Cell* cell = progress_[consumer_index]->next;
    if (cell != nullptr) {
      cell->consumers_left--;
      DCHECK_GE(cell->consumers_left, 0);
    }
    progress_[consumer_index] = cell;
  }
  release_cells();
  ++consumers_done_;
  all_consumers_done_cv_.notify_all();
}

void LocalExchanger::Release() {
  {
    std::unique_lock l(mutex_);
    // Ensure consumers can drain by signalling EOS if CloseProducer() was not yet
    // called (e.g. if Open() never ran due to an early error).
    if (!eos_) {
      eos_ = true;
      batch_available_cv_.notify_all();
    }
    // Wait for all consumers to have called CloseConsumer(). During normal execution
    // this condition is already satisfied before Release() is reached. During query
    // cancellation the producer's Close() may race ahead of consumer fragment instances
    // that are still draining the exchanger.
    all_consumers_done_cv_.wait(l, [&]() {
      return consumers_done_ == consumer_count_;
    });
  }
  std::lock_guard l(mutex_);
  release_cells();
  DCHECK(head_->next == nullptr) << "All batches must be consumed before Release()";
  delete head_;
  mem_tracker_->Release(sizeof(Cell));
  head_ = nullptr;
  if (mem_tracker_) mem_tracker_->Close();
}

void LocalExchanger::release_cells() {
  // Advance head_ while all consumers have moved past the cell.
  while (head_ != nullptr && head_->consumers_left == 0) {
    Cell* next_cell = head_->next;
    // Do not advance head_ while any consumer is currently "on" next_cell (i.e., holds
    // a live pointer to next_cell->batch from a prior Pull()); consumers_left > 0 means
    // at least one consumer has not yet advanced past next_cell.
    if (next_cell == nullptr || next_cell->consumers_left > 0) break;
    // All consumers have moved past head_, can release it.
    delete head_;
    mem_tracker_->Release(sizeof(Cell));
    head_ = next_cell;
    num_cells_counter_->Add(-1);
  }
}

} // namespace impala
