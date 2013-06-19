// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/disk-io-mgr-internal.h"

using namespace boost;
using namespace impala;
using namespace std;

// Default ready buffer queue capacity. This constant doesn't matter too much
// since the system dynamically adjusts.
static const int DEFAULT_QUEUE_CAPACITY = 5;

void DiskIoMgr::ReaderContext::Cancel(const Status& status) {
  DCHECK(!status.ok());

  {
    unique_lock<mutex> reader_lock(lock_);
    DCHECK(Validate()) << endl << DebugString();

    // Already being cancelled
    if (state_ == ReaderContext::Cancelled) return;

    DCHECK(status_.ok());
    status_ = status;

    // The reader will be put into a cancelled state until call cleanup is complete.
    state_ = ReaderContext::Cancelled;

    // Cancel all scan ranges for this reader. Each range could be one one of
    // four queues.
    for (int i = 0; i < disk_states_.size(); ++i) {
      ReaderContext::PerDiskState& state = disk_states_[i];
      ScanRange* range = NULL;
      while ((range = state.in_flight_ranges.Dequeue()) != NULL) {
        range->Cancel();
      }
      while ((range = state.unstarted_ranges.Dequeue()) != NULL) {
        range->Cancel();
      }
    }

    ScanRange* range = NULL;
    while ((range = ready_to_start_ranges_.Dequeue()) != NULL) {
      range->Cancel();
    }
    while ((range = blocked_ranges_.Dequeue()) != NULL) {
      range->Cancel();
    }
    
    // Schedule reader on all disks. The disks will notice it is cancelled and do any
    // required cleanup
    for (int i = 0; i < disk_states_.size(); ++i) {
      ScheduleOnDisk(i);
    }
  }
  
  // Signal reader and unblock the GetNext/Read thread.  That read will fail with
  // a cancelled status.
  ready_to_start_ranges_cv_.notify_all();
}

DiskIoMgr::ReaderContext::ReaderContext(DiskIoMgr* parent, int num_disks)
  : parent_(parent),
    bytes_read_counter_(NULL),
    read_timer_(NULL),
    active_read_thread_counter_(NULL),
    disks_accessed_bitmap_(NULL),
    state_(Inactive),
    disk_states_(num_disks) {
}

// Resets this object for a new reader
void DiskIoMgr::ReaderContext::Reset(hdfsFS hdfs_connection, MemLimit* limit) {
  DCHECK_EQ(state_, Inactive);
  status_ = Status::OK;

  bytes_read_counter_ = NULL;
  read_timer_ = NULL;
  active_read_thread_counter_ = NULL;
  disks_accessed_bitmap_ = NULL;

  state_ = Active;
  hdfs_connection_ = hdfs_connection;
  mem_limit_ = limit;

  num_unstarted_ranges_ = 0;
  num_disks_with_ranges_ = 0;
  num_used_buffers_ = 0;
  num_buffers_in_reader_ = 0;
  num_ready_buffers_ = 0;
  num_finished_ranges_ = 0;
  bytes_read_local_ = 0;
  bytes_read_short_circuit_ = 0;

  DCHECK(ready_to_start_ranges_.empty());
  DCHECK(blocked_ranges_.empty());

  for (int i = 0; i < disk_states_.size(); ++i) {
    disk_states_[i].Reset();
  }
}

int DiskIoMgr::ReaderContext::initial_scan_range_queue_capacity() {
  if (num_finished_ranges_ > 0) {
    return total_range_queue_capacity_ / num_finished_ranges_;
  }
  return DEFAULT_QUEUE_CAPACITY;
}

// Dumps out reader information.  Lock should be taken by caller
string DiskIoMgr::ReaderContext::DebugString() const {
  stringstream ss;
  ss << endl << "  Reader: " << (void*)this << " (state=";
  if (state_ == ReaderContext::Inactive) ss << "Inactive";
  if (state_ == ReaderContext::Cancelled) ss << "Cancelled";
  if (state_ == ReaderContext::Active) ss << "Active";
  if (state_ != ReaderContext::Inactive) {
    ss << " status_=" << (status_.ok() ? "OK" : status_.GetErrorMsg())
       << " #ready_buffers=" << num_ready_buffers_
       << " #used_buffers=" << num_used_buffers_
       << " #num_buffers_in_reader=" << num_buffers_in_reader_
       << " #finished_scan_ranges=" << num_finished_ranges_
       << " #disk_with_ranges=" << num_disks_with_ranges_
       << " #disks=" << num_disks_with_ranges_;
    for (int i = 0; i < disk_states_.size(); ++i) {
      ss << endl << "   " << i << ": "
         << "is_on_queue=" << disk_states_[i].is_on_queue
         << " done=" << disk_states_[i].done
         << " #num_remaining_scan_ranges=" << disk_states_[i].num_remaining_ranges
         << " #in_flight_ranges=" << disk_states_[i].in_flight_ranges.size()
         << " #unstarted_ranges=" << disk_states_[i].unstarted_ranges.size()
         << " #reading_threads=" << disk_states_[i].num_threads_in_read;
    }
  }
  ss << ")";
  return ss.str();
}

bool DiskIoMgr::ReaderContext::Validate() const {
  if (state_ == ReaderContext::Inactive) {
    LOG(WARNING) << "state_ == ReaderContext::Inactive";
    return false;
  }

  int num_disks_with_ranges = 0;
  int num_reading_threads = 0;

  if (num_used_buffers_ < 0) {
    LOG(WARNING) << "num_used_buffers_ < 0: #used=" << num_used_buffers_;
    return false;
  }

  if (num_ready_buffers_ < 0) {
    LOG(WARNING) << "num_ready_buffers_ < 0: #used=" << num_ready_buffers_;
    return false;
  }

  int total_unstarted_ranges = 0;
  for (int i = 0; i < disk_states_.size(); ++i) {
    const PerDiskState& state = disk_states_[i];
    num_reading_threads += state.num_threads_in_read;
    total_unstarted_ranges += state.unstarted_ranges.size();

    if (state.num_remaining_ranges > 0) ++num_disks_with_ranges;
    if (state.num_threads_in_read < 0) {
      LOG(WARNING) << "disk_id=" << i
                   << "state.num_threads_in_read < 0: #threads="
                   << state.num_threads_in_read;
      return false;
    }

    if (state_ != ReaderContext::Cancelled) {
      if (state.unstarted_ranges.size() + state.in_flight_ranges.size() >
          state.num_remaining_ranges) {
        LOG(WARNING) << "disk_id=" << i
                     << " state.unstarted_ranges.size() + state.in_flight_ranges.size()"
                     << " > state.num_remaining_ranges:"
                     << " #unscheduled=" << state.unstarted_ranges.size()
                     << " #in_flight=" << state.in_flight_ranges.size()
                     << " #remaining=" << state.num_remaining_ranges;
        return false;
      }

      // If we have an in_flight range, the reader must be on the queue or have a
      // thread actively reading for it.
      if (!state.in_flight_ranges.empty() && !state.is_on_queue && 
          state.num_threads_in_read == 0) {
        LOG(WARNING) << "disk_id=" << i
                     << " reader has inflight ranges but is not on the disk queue."
                     << " #in_flight_ranges=" << state.in_flight_ranges.size()
                     << " #reading_threads=" << state.num_threads_in_read;
        return false;
      }

      if (state.done && state.num_threads_in_read > 0) {
        LOG(WARNING) << "disk_id=" << i
                     << " state set to done but there are still threads working."
                     << " #reading_threads=" << state.num_threads_in_read;
        return false;
      }
    }

    if (state.done && state.is_on_queue) {
      LOG(WARNING) << "disk_id=" << i
                   << " state set to done but the reader is still on the disk queue."
                   << " state.done=true and state.is_on_queue=true";
      return false;
    }
  }

  if (state_ != ReaderContext::Cancelled) {
    if (total_unstarted_ranges != num_unstarted_ranges_) {
      LOG(WARNING) << "total_unstarted_ranges=" << total_unstarted_ranges
                  << " sum_in_states=" << num_unstarted_ranges_;
      return false;
    }
  }
  
  return true;
}

