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

#include "runtime/io/disk-io-mgr-internal.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr,
    RequestContext* reader, ScanRange* scan_range, uint8_t* buffer,
    int64_t buffer_len)
  : io_mgr_(io_mgr),
    reader_(reader),
    scan_range_(scan_range),
    buffer_(buffer),
    buffer_len_(buffer_len) {
  DCHECK(io_mgr != nullptr);
  DCHECK(scan_range != nullptr);
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer_len, 0);
}

Status RequestContext::AllocBuffer(ScanRange* range, int64_t buffer_size,
    unique_ptr<BufferDescriptor>* buffer_desc) {
  DCHECK(range->external_buffer_tag_ == ScanRange::ExternalBufferTag::NO_BUFFER)
      << static_cast<int>(range->external_buffer_tag_);
  DCHECK_LE(buffer_size, parent_->max_buffer_size_);
  DCHECK_GT(buffer_size, 0);
  buffer_size = BitUtil::RoundUpToPowerOfTwo(
      max(parent_->min_buffer_size_, min(parent_->max_buffer_size_, buffer_size)));

  DCHECK(mem_tracker_ != nullptr);
  if (!mem_tracker_->TryConsume(buffer_size)) {
    return mem_tracker_->MemLimitExceeded(nullptr, "disk I/O buffer", buffer_size);
  }

  uint8_t* buffer = reinterpret_cast<uint8_t*>(malloc(buffer_size));
  if (buffer == nullptr) {
    mem_tracker_->Release(buffer_size);
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Could not malloc buffer of $0 bytes"));
  }
  buffer_desc->reset(new BufferDescriptor(parent_, this, range, buffer, buffer_size));
  return Status::OK();
}

void RequestContext::FreeBuffer(BufferDescriptor* buffer) {
  DCHECK(buffer->buffer_ != nullptr);
  if (!buffer->is_cached() && !buffer->is_client_buffer()) {
    // Only buffers that were not allocated by DiskIoMgr need to have memory freed.
    free(buffer->buffer_);
    mem_tracker_->Release(buffer->buffer_len_);
  }
  buffer->buffer_ = nullptr;
}

// Cancellation of a RequestContext requires coordination from multiple threads that may
// hold references to the context:
//  1. Disk threads that are currently processing a range for this context.
//  2. Caller threads that are waiting in GetNext().
//
// Each thread that currently has a reference to the request context must notice the
// cancel, cancel any pending operations involving the context and remove the contxt from
// tracking structures. Once no more operations are pending on the context and no more
// I/O mgr threads hold references to the context, the context can be marked inactive
// (see CancelAndMarkInactive()), after which the owner of the context object can free
// it.
//
// The steps are:
// 1. Cancel() will immediately set the context in the Cancelled state. This prevents any
// other thread from adding more ready buffers to the context (they all take a lock and
// check the state before doing so), or any write ranges to the context.
// 2. Cancel() will call Cancel() on each ScanRange that is not yet complete, unblocking
// any threads in GetNext(). If there was no prior error for a scan range, any reads from
// that scan range will return a CANCELLED Status. Cancel() also invokes callbacks for
// all WriteRanges with a CANCELLED Status.
// 3. Disk threads notice the context is cancelled either when picking the next context
// to process or when they try to enqueue a ready buffer. Upon noticing the cancelled
// state, removes the context from the disk queue. The last thread per disk then calls
// DecrementDiskRefCount(). After the last disk thread has called DecrementDiskRefCount(),
// cancellation is done and it is safe to unregister the context.
void RequestContext::Cancel() {
  // Callbacks are collected in this vector and invoked while no lock is held.
  vector<WriteRange::WriteDoneCallback> write_callbacks;
  {
    unique_lock<mutex> lock(lock_);
    DCHECK(Validate()) << endl << DebugString();

    // Already being cancelled
    if (state_ == RequestContext::Cancelled) return;

    // The reader will be put into a cancelled state until call cleanup is complete.
    state_ = RequestContext::Cancelled;

    // Cancel all scan ranges for this reader. Each range could be one one of
    // four queues.
    for (int i = 0; i < disk_states_.size(); ++i) {
      PerDiskState& state = disk_states_[i];
      RequestRange* range = nullptr;
      while ((range = state.in_flight_ranges()->Dequeue()) != nullptr) {
        if (range->request_type() == RequestType::READ) {
          static_cast<ScanRange*>(range)->Cancel(Status::CANCELLED);
        } else {
          DCHECK(range->request_type() == RequestType::WRITE);
          write_callbacks.push_back(static_cast<WriteRange*>(range)->callback_);
        }
      }

      ScanRange* scan_range;
      while ((scan_range = state.unstarted_scan_ranges()->Dequeue()) != nullptr) {
        scan_range->Cancel(Status::CANCELLED);
      }
      WriteRange* write_range;
      while ((write_range = state.unstarted_write_ranges()->Dequeue()) != nullptr) {
        write_callbacks.push_back(write_range->callback_);
      }
    }

    ScanRange* range = nullptr;
    while ((range = ready_to_start_ranges_.Dequeue()) != nullptr) {
      range->Cancel(Status::CANCELLED);
    }
    while ((range = blocked_ranges_.Dequeue()) != nullptr) {
      range->Cancel(Status::CANCELLED);
    }
    while ((range = cached_ranges_.Dequeue()) != nullptr) {
      range->Cancel(Status::CANCELLED);
    }

    // Ensure that the reader is scheduled on all disks (it may already be scheduled on
    // some). The disk threads will notice that the context is cancelled and do any
    // required cleanup for the disk state.
    for (int i = 0; i < disk_states_.size(); ++i) {
      disk_states_[i].ScheduleContext(lock, this, i);
    }
  }

  for (const WriteRange::WriteDoneCallback& write_callback: write_callbacks) {
    write_callback(Status::CANCELLED);
  }

  // Signal reader and unblock the GetNext/Read thread.  That read will fail with
  // a cancelled status.
  ready_to_start_ranges_cv_.NotifyAll();
}

void RequestContext::CancelAndMarkInactive() {
  Cancel();

  boost::unique_lock<boost::mutex> l(lock_);
  DCHECK_NE(state_, Inactive);
  DCHECK(Validate()) << endl << DebugString();

  // Wait until the ranges finish up.
  while (num_disks_with_ranges_ > 0) disks_complete_cond_var_.Wait(l);

  // Validate that no buffers were leaked from this context.
  DCHECK_EQ(num_buffers_in_reader_.Load(), 0) << endl << DebugString();
  DCHECK_EQ(num_used_buffers_.Load(), 0) << endl << DebugString();

  // Validate that no threads are active and the context is not queued.
  for (const PerDiskState& disk_state : disk_states_) {
    DCHECK_EQ(0, disk_state.in_flight_ranges()->size()) << endl << DebugString();
    DCHECK_EQ(0, disk_state.unstarted_scan_ranges()->size()) << endl << DebugString();
    DCHECK_EQ(0, disk_state.num_threads_in_op()) << endl << DebugString();
    DCHECK(!disk_state.is_on_queue()) << endl << DebugString();
  }
  DCHECK(Validate()) << endl << DebugString();
  state_ = Inactive;
}

void RequestContext::AddRequestRange(const unique_lock<mutex>& lock,
    RequestRange* range, bool schedule_immediately) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  PerDiskState& state = disk_states_[range->disk_id()];
  if (state.done()) {
    DCHECK_EQ(state.num_remaining_ranges(), 0);
    state.set_done(false);
    ++num_disks_with_ranges_;
  }

  bool schedule_context;
  if (range->request_type() == RequestType::READ) {
    ScanRange* scan_range = static_cast<ScanRange*>(range);
    if (schedule_immediately) {
      ScheduleScanRange(lock, scan_range);
    } else {
      state.unstarted_scan_ranges()->Enqueue(scan_range);
      num_unstarted_scan_ranges_.Add(1);
    }
    // If next_scan_range_to_start is NULL, schedule this RequestContext so that it will
    // be set. If it's not NULL, this context will be scheduled when GetNextRange() is
    // invoked.
    schedule_context = state.next_scan_range_to_start() == NULL;
  } else {
    DCHECK(range->request_type() == RequestType::WRITE);
    DCHECK(!schedule_immediately);
    WriteRange* write_range = static_cast<WriteRange*>(range);
    state.unstarted_write_ranges()->Enqueue(write_range);

    // ScheduleContext() has no effect if the context is already scheduled,
    // so this is safe.
    schedule_context = true;
  }

  if (schedule_context) state.ScheduleContext(lock, this, range->disk_id());
  ++state.num_remaining_ranges();
}

RequestContext::RequestContext(
    DiskIoMgr* parent, int num_disks, MemTracker* tracker)
  : parent_(parent), mem_tracker_(tracker), disk_states_(num_disks) {}

// Dumps out request context information. Lock should be taken by caller
string RequestContext::DebugString() const {
  stringstream ss;
  ss << endl << "  RequestContext: " << (void*)this << " (state=";
  if (state_ == RequestContext::Inactive) ss << "Inactive";
  if (state_ == RequestContext::Cancelled) ss << "Cancelled";
  if (state_ == RequestContext::Active) ss << "Active";
  if (state_ != RequestContext::Inactive) {
    ss << " #ready_buffers=" << num_ready_buffers_.Load()
       << " #used_buffers=" << num_used_buffers_.Load()
       << " #num_buffers_in_reader=" << num_buffers_in_reader_.Load()
       << " #finished_scan_ranges=" << num_finished_ranges_.Load()
       << " #disk_with_ranges=" << num_disks_with_ranges_
       << " #disks=" << num_disks_with_ranges_;
    for (int i = 0; i < disk_states_.size(); ++i) {
      ss << endl << "   " << i << ": "
         << "is_on_queue=" << disk_states_[i].is_on_queue()
         << " done=" << disk_states_[i].done()
         << " #num_remaining_scan_ranges=" << disk_states_[i].num_remaining_ranges()
         << " #in_flight_ranges=" << disk_states_[i].in_flight_ranges()->size()
         << " #unstarted_scan_ranges=" << disk_states_[i].unstarted_scan_ranges()->size()
         << " #unstarted_write_ranges="
         << disk_states_[i].unstarted_write_ranges()->size()
         << " #reading_threads=" << disk_states_[i].num_threads_in_op();
    }
  }
  ss << ")";
  return ss.str();
}

bool RequestContext::Validate() const {
  if (state_ == RequestContext::Inactive) {
    LOG(WARNING) << "state_ == RequestContext::Inactive";
    return false;
  }

  if (num_used_buffers_.Load() < 0) {
    LOG(WARNING) << "num_used_buffers_ < 0: #used=" << num_used_buffers_.Load();
    return false;
  }

  if (num_ready_buffers_.Load() < 0) {
    LOG(WARNING) << "num_ready_buffers_ < 0: #used=" << num_ready_buffers_.Load();
    return false;
  }

  int total_unstarted_ranges = 0;
  for (int i = 0; i < disk_states_.size(); ++i) {
    const PerDiskState& state = disk_states_[i];
    bool on_queue = state.is_on_queue();
    int num_reading_threads = state.num_threads_in_op();

    total_unstarted_ranges += state.unstarted_scan_ranges()->size();

    if (num_reading_threads < 0) {
      LOG(WARNING) << "disk_id=" << i
                   << "state.num_threads_in_read < 0: #threads="
                   << num_reading_threads;
      return false;
    }

    if (state_ != RequestContext::Cancelled) {
      if (state.unstarted_scan_ranges()->size() + state.in_flight_ranges()->size() >
          state.num_remaining_ranges()) {
        LOG(WARNING) << "disk_id=" << i
                     << " state.unstarted_ranges.size() + state.in_flight_ranges.size()"
                     << " > state.num_remaining_ranges:"
                     << " #unscheduled=" << state.unstarted_scan_ranges()->size()
                     << " #in_flight=" << state.in_flight_ranges()->size()
                     << " #remaining=" << state.num_remaining_ranges();
        return false;
      }

      // If we have an in_flight range, the reader must be on the queue or have a
      // thread actively reading for it.
      if (!state.in_flight_ranges()->empty() && !on_queue && num_reading_threads == 0) {
        LOG(WARNING) << "disk_id=" << i
                     << " reader has inflight ranges but is not on the disk queue."
                     << " #in_flight_ranges=" << state.in_flight_ranges()->size()
                     << " #reading_threads=" << num_reading_threads
                     << " on_queue=" << on_queue;
        return false;
      }

      if (state.done() && num_reading_threads > 0) {
        LOG(WARNING) << "disk_id=" << i
                     << " state set to done but there are still threads working."
                     << " #reading_threads=" << num_reading_threads;
        return false;
      }
    } else {
      // Is Cancelled
      if (!state.in_flight_ranges()->empty()) {
        LOG(WARNING) << "disk_id=" << i
                     << "Reader cancelled but has in flight ranges.";
        return false;
      }
      if (!state.unstarted_scan_ranges()->empty()) {
        LOG(WARNING) << "disk_id=" << i
                     << "Reader cancelled but has unstarted ranges.";
        return false;
      }
    }

    if (state.done() && on_queue) {
      LOG(WARNING) << "disk_id=" << i
                   << " state set to done but the reader is still on the disk queue."
                   << " state.done=true and state.is_on_queue=true";
      return false;
    }
  }

  if (state_ != RequestContext::Cancelled) {
    if (total_unstarted_ranges != num_unstarted_scan_ranges_.Load()) {
      LOG(WARNING) << "total_unstarted_ranges=" << total_unstarted_ranges
                   << " sum_in_states=" << num_unstarted_scan_ranges_.Load();
      return false;
    }
  } else {
    if (!ready_to_start_ranges_.empty()) {
      LOG(WARNING) << "Reader cancelled but has ready to start ranges.";
      return false;
    }
    if (!blocked_ranges_.empty()) {
      LOG(WARNING) << "Reader cancelled but has blocked ranges.";
      return false;
    }
  }

  return true;
}

void RequestContext::PerDiskState::ScheduleContext(const unique_lock<mutex>& context_lock,
    RequestContext* context, int disk_id) {
  DCHECK(context_lock.mutex() == &context->lock_ && context_lock.owns_lock());
  if (is_on_queue_.Load() == 0 && !done_) {
    is_on_queue_.Store(1);
    context->parent_->disk_queues_[disk_id]->EnqueueContext(context);
  }
}
