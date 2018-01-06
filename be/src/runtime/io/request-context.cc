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

#include "runtime/exec-env.h"

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

BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr, RequestContext* reader,
    ScanRange* scan_range, BufferPool::ClientHandle* bp_client,
    BufferPool::BufferHandle handle) :
  io_mgr_(io_mgr),
  reader_(reader),
  scan_range_(scan_range),
  buffer_(handle.data()),
  buffer_len_(handle.len()),
  bp_client_(bp_client),
  handle_(move(handle)) {
  DCHECK(io_mgr != nullptr);
  DCHECK(scan_range != nullptr);
  DCHECK(bp_client_->is_registered());
  DCHECK(handle_.is_open());
}

void RequestContext::FreeBuffer(BufferDescriptor* buffer) {
  DCHECK(buffer->buffer_ != nullptr);
  if (!buffer->is_cached() && !buffer->is_client_buffer()) {
    // Only buffers that were allocated by DiskIoMgr need to be freed.
    ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(
        buffer->bp_client_, &buffer->handle_);
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

    // Clear out all request ranges from queues for this reader. Cancel the scan
    // ranges and invoke the write range callbacks to propagate the cancellation.
    for (ScanRange* range : active_scan_ranges_) range->CancelInternal(Status::CANCELLED);
    active_scan_ranges_.clear();
    for (PerDiskState& disk_state : disk_states_) {
      RequestRange* range;
      while ((range = disk_state.in_flight_ranges()->Dequeue()) != nullptr) {
        if (range->request_type() == RequestType::WRITE) {
          write_callbacks.push_back(static_cast<WriteRange*>(range)->callback_);
        }
      }
      while (disk_state.unstarted_scan_ranges()->Dequeue() != nullptr);
      WriteRange* write_range;
      while ((write_range = disk_state.unstarted_write_ranges()->Dequeue()) != nullptr) {
        write_callbacks.push_back(write_range->callback_);
      }
    }
    // Clear out the lists of scan ranges.
    while (ready_to_start_ranges_.Dequeue() != nullptr);
    while (cached_ranges_.Dequeue() != nullptr);

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

  // Validate that no ranges are active.
  DCHECK_EQ(0, active_scan_ranges_.size()) << endl << DebugString();

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

void RequestContext::AddRangeToDisk(const unique_lock<mutex>& lock,
    RequestRange* range, ScheduleMode schedule_mode) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK_EQ(state_, Active) << DebugString();
  PerDiskState* disk_state = &disk_states_[range->disk_id()];
  if (disk_state->done()) {
    DCHECK_EQ(disk_state->num_remaining_ranges(), 0);
    disk_state->set_done(false);
    ++num_disks_with_ranges_;
  }
  if (range->request_type() == RequestType::READ) {
    ScanRange* scan_range = static_cast<ScanRange*>(range);
    if (schedule_mode == ScheduleMode::IMMEDIATELY) {
      ScheduleScanRange(lock, scan_range);
    } else if (schedule_mode == ScheduleMode::UPON_GETNEXT) {
      disk_state->unstarted_scan_ranges()->Enqueue(scan_range);
      num_unstarted_scan_ranges_.Add(1);
      // If there's no 'next_scan_range_to_start', schedule this RequestContext so that
      // one of the 'unstarted_scan_ranges' will become the 'next_scan_range_to_start'.
      if (disk_state->next_scan_range_to_start() == nullptr) {
        disk_state->ScheduleContext(lock, this, range->disk_id());
      }
    }
  } else {
    DCHECK(range->request_type() == RequestType::WRITE);
    DCHECK(schedule_mode == ScheduleMode::IMMEDIATELY) << static_cast<int>(schedule_mode);
    WriteRange* write_range = static_cast<WriteRange*>(range);
    disk_state->unstarted_write_ranges()->Enqueue(write_range);

    // Ensure that the context is scheduled so that the write range gets picked up.
    // ScheduleContext() has no effect if already scheduled, so this is safe to do always.
    disk_state->ScheduleContext(lock, this, range->disk_id());
  }
  ++disk_state->num_remaining_ranges();
}

void RequestContext::AddActiveScanRangeLocked(
    const unique_lock<mutex>& lock, ScanRange* range) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK(state_ == Active);
  active_scan_ranges_.insert(range);
}

void RequestContext::RemoveActiveScanRange(ScanRange* range) {
  unique_lock<mutex> lock(lock_);
  RemoveActiveScanRangeLocked(lock, range);
}

void RequestContext::RemoveActiveScanRangeLocked(
    const unique_lock<mutex>& lock, ScanRange* range) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  active_scan_ranges_.erase(range);
}

RequestContext::RequestContext(DiskIoMgr* parent, int num_disks)
  : parent_(parent), disk_states_(num_disks) {}

// Dumps out request context information. Lock should be taken by caller
string RequestContext::DebugString() const {
  stringstream ss;
  ss << endl << "  RequestContext: " << (void*)this << " (state=";
  if (state_ == RequestContext::Inactive) ss << "Inactive";
  if (state_ == RequestContext::Cancelled) ss << "Cancelled";
  if (state_ == RequestContext::Active) ss << "Active";
  if (state_ != RequestContext::Inactive) {
    ss << " #disk_with_ranges=" << num_disks_with_ranges_
       << " #disks=" << num_disks_with_ranges_
       << " #active scan ranges=" << active_scan_ranges_.size();
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
    if (!active_scan_ranges_.empty()) {
      LOG(WARNING) << "Reader cancelled but has active ranges.";
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
