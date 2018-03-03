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

void RequestContext::Cancel(const Status& status) {
  DCHECK(!status.ok());

  // Callbacks are collected in this vector and invoked while no lock is held.
  vector<WriteRange::WriteDoneCallback> write_callbacks;
  {
    lock_guard<mutex> lock(lock_);
    DCHECK(Validate()) << endl << DebugString();

    // Already being cancelled
    if (state_ == RequestContext::Cancelled) return;

    DCHECK(status_.ok());
    status_ = status;

    // The reader will be put into a cancelled state until call cleanup is complete.
    state_ = RequestContext::Cancelled;

    // Cancel all scan ranges for this reader. Each range could be one one of
    // four queues.
    for (int i = 0; i < disk_states_.size(); ++i) {
      RequestContext::PerDiskState& state = disk_states_[i];
      RequestRange* range = NULL;
      while ((range = state.in_flight_ranges()->Dequeue()) != NULL) {
        if (range->request_type() == RequestType::READ) {
          static_cast<ScanRange*>(range)->Cancel(status);
        } else {
          DCHECK(range->request_type() == RequestType::WRITE);
          write_callbacks.push_back(static_cast<WriteRange*>(range)->callback_);
        }
      }

      ScanRange* scan_range;
      while ((scan_range = state.unstarted_scan_ranges()->Dequeue()) != NULL) {
        scan_range->Cancel(status);
      }
      WriteRange* write_range;
      while ((write_range = state.unstarted_write_ranges()->Dequeue()) != NULL) {
        write_callbacks.push_back(write_range->callback_);
      }
    }

    ScanRange* range = NULL;
    while ((range = ready_to_start_ranges_.Dequeue()) != NULL) {
      range->Cancel(status);
    }
    while ((range = blocked_ranges_.Dequeue()) != NULL) {
      range->Cancel(status);
    }
    while ((range = cached_ranges_.Dequeue()) != NULL) {
      range->Cancel(status);
    }

    // Schedule reader on all disks. The disks will notice it is cancelled and do any
    // required cleanup
    for (int i = 0; i < disk_states_.size(); ++i) {
      RequestContext::PerDiskState& state = disk_states_[i];
      state.ScheduleContext(this, i);
    }
  }

  for (const WriteRange::WriteDoneCallback& write_callback: write_callbacks) {
    write_callback(status_);
  }

  // Signal reader and unblock the GetNext/Read thread.  That read will fail with
  // a cancelled status.
  ready_to_start_ranges_cv_.NotifyAll();
}

void RequestContext::CancelAndMarkInactive() {
  Cancel(Status::CANCELLED);

  boost::unique_lock<boost::mutex> l(lock_);
  DCHECK_NE(state_, Inactive);
  DCHECK(Validate()) << endl << DebugString();

  // Wait until the ranges finish up.
  while (num_disks_with_ranges_ > 0) disks_complete_cond_var_.Wait(l);

  // Validate that no buffers were leaked from this context.
  DCHECK_EQ(num_buffers_in_reader_.Load(), 0) << endl << DebugString();
  DCHECK_EQ(num_used_buffers_.Load(), 0) << endl << DebugString();
  DCHECK(Validate()) << endl << DebugString();
  state_ = Inactive;
}

void RequestContext::AddRequestRange(
    RequestRange* range, bool schedule_immediately) {
  // DCHECK(lock_.is_locked()); // TODO: boost should have this API
  RequestContext::PerDiskState& state = disk_states_[range->disk_id()];
  if (state.done()) {
    DCHECK_EQ(state.num_remaining_ranges(), 0);
    state.set_done(false);
    ++num_disks_with_ranges_;
  }

  bool schedule_context;
  if (range->request_type() == RequestType::READ) {
    ScanRange* scan_range = static_cast<ScanRange*>(range);
    if (schedule_immediately) {
      ScheduleScanRange(scan_range);
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

  if (schedule_context) state.ScheduleContext(this, range->disk_id());
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
    ss << " status_=" << (status_.ok() ? "OK" : status_.GetDetail())
       << " #ready_buffers=" << num_ready_buffers_.Load()
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

void RequestContext::PerDiskState::ScheduleContext(
    RequestContext* context, int disk_id) {
  if (!is_on_queue_ && !done_) {
    is_on_queue_ = true;
    context->parent_->disk_queues_[disk_id]->EnqueueContext(context);
  }
}
