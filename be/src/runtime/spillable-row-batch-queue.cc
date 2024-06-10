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

#include "runtime/spillable-row-batch-queue.h"

#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/pretty-printer.h"

#include "common/names.h"

namespace impala {

SpillableRowBatchQueue::SpillableRowBatchQueue(const string& name,
    int64_t max_unpinned_bytes, RuntimeState* state, MemTracker* mem_tracker,
    RuntimeProfile* profile, const RowDescriptor* row_desc,
    const TBackendResourceProfile& resource_profile, const TDebugOptions& debug_options)
  : name_(name),
    state_(state),
    mem_tracker_(mem_tracker),
    profile_(profile),
    row_desc_(row_desc),
    resource_profile_(resource_profile),
    debug_options_(debug_options),
    max_unpinned_bytes_(max_unpinned_bytes) {
  DCHECK_GT(max_unpinned_bytes_, 0);
  DCHECK_GT(resource_profile_.spillable_buffer_size, 0);
}

SpillableRowBatchQueue::~SpillableRowBatchQueue() {
  DCHECK(closed_);
}

Status SpillableRowBatchQueue::Open() {
  // Initialize the ResevationManager and then claim the initial reservation.
  reservation_manager_.Init(name_, profile_, state_->instance_buffer_reservation(),
      mem_tracker_, resource_profile_, debug_options_);
  RETURN_IF_ERROR(reservation_manager_.ClaimBufferReservation(state_));

  // Create the BufferedTupleStream, initialize it, and then create the read and write
  // buffer pages.
  batch_queue_ = make_unique<BufferedTupleStream>(state_, row_desc_,
      reservation_manager_.buffer_pool_client(), resource_profile_.spillable_buffer_size,
      resource_profile_.max_row_buffer_size);
  RETURN_IF_ERROR(batch_queue_->Init(name_, true));
  bool got_reservation = false;
  RETURN_IF_ERROR(batch_queue_->PrepareForReadWrite(true, &got_reservation));
  DCHECK(got_reservation) << "SpillableRowBatchQueue failed to get reservation using "
                          << "buffer pool client: "
                          << reservation_manager_.buffer_pool_client()->DebugString();
  return Status::OK();
}

Status SpillableRowBatchQueue::AddBatch(RowBatch* batch) {
  DCHECK(!IsFull()) << "Cannot AddBatch on a full SpillableRowBatchQueue";
  DCHECK(!closed_) << "Cannot AddBatch on a closed SpillableRowBatchQueue";
  Status status;
  FOREACH_ROW(batch, 0, batch_itr) {
    // AddRow should only return false if there was not enough unused reservation to
    // allocate a page for the given row. If a row cannot be added to the batch_queue_
    // then start spilling to disk by unpining the stream. Once the stream is unpinned,
    // adding the row to the stream should succeed unless the unpinned pages needed to
    // be spilled and either (1) there was an error (e.g. IO error) when writing to disk,
    // (2) there is no more scratch space left to write to disk, or (3) spilling to disk
    // is disabled.
    if (UNLIKELY(!batch_queue_->AddRow(batch_itr.Get(), &status))) {
      RETURN_IF_ERROR(status);
      // StartSpilling checks if spilling is disabled and returns an error if it is not.
      RETURN_IF_ERROR(state_->StartSpilling(mem_tracker_));

      // The pin should be stream at this point.
      DCHECK(batch_queue_->is_pinned());
      DCHECK_EQ(batch_queue_->bytes_unpinned(), 0);

      // Unpin the stream and then add the row.
      RETURN_IF_ERROR(
          batch_queue_->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));

      // Append "Spilled" to the "ExecOption" info string in the runtime profile.
      profile_->AppendExecOption("Spilled");

      if (!batch_queue_->AddRow(batch_itr.Get(), &status)) {
        RETURN_IF_ERROR(status);
        // If the row could not be added after the stream was unpinned, an error should
        // have been set.
        DCHECK(false) << Substitute("Row with a size of $0 should be added successfully "
                                    "in unpinned mode unless an error occurred. "
                                    "batch_queue_: $1",
            PrettyPrinter::PrintBytes(
                batch_queue_->ComputeRowSize(batch_itr.Get())),
            batch_queue_->DebugString());
      }
    }
  }
  return Status::OK();
}

Status SpillableRowBatchQueue::GetBatch(RowBatch* batch) {
  DCHECK(!IsEmpty()) << "Cannot GetBatch on an empty SpillableRowBatchQueue";
  DCHECK(!closed_) << "Cannot GetBatch on a closed SpillableRowBatchQueue";
  bool eos = false;
  RETURN_IF_ERROR(batch_queue_->GetNext(batch, &eos));
  // Validate that the value of eos is consistent with IsEmpty().
  DCHECK_EQ(eos, IsEmpty());
  return Status::OK();
}

bool SpillableRowBatchQueue::IsFull() const {
  // The queue is considered full if the number of unpinned bytes is greater than the
  // max number of unpinned bytes. The queue can only be full after the stream has been
  // unpinned. The number of unpinned bytes in the stream may exceed the imposed limit
  // because the entire stream is unpinned at once, without checking against the
  // max_unpinned_bytes_ limit.
  DCHECK(!closed_);
  return batch_queue_->bytes_unpinned() >= max_unpinned_bytes_;
}

bool SpillableRowBatchQueue::IsEmpty() const {
  // The batch_queue_ tracks how many rows have been added to the stream (regardless of
  // whether those rows have already been removed) and how many rows have been read from
  // the stream. If these values are equal, the queue is considered empty.
  DCHECK(!closed_);
  return batch_queue_->num_rows() == batch_queue_->rows_returned();
}

bool SpillableRowBatchQueue::IsOpen() const {
  return !closed_;
}

void SpillableRowBatchQueue::Close() {
  if (closed_) return;
  if (batch_queue_ != nullptr) {
    batch_queue_->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  reservation_manager_.Close(state_);
  closed_ = true;
}
} // namespace impala
