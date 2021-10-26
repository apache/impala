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
#include "common/thread-debug-info.h"

using namespace impala;
using namespace impala::io;

// Cancelled status with an error message to distinguish from user-initiated cancellation.
static const Status& CONTEXT_CANCELLED =
    Status::CancelledInternal("IoMgr RequestContext");

/// Struct containing state per disk. See comments in the disk read loop on how
/// they are used.
class RequestContext::PerDiskState {
 public:
  /// Set reference to global disk queue corresponding to this state. Must be called
  /// immediately after construction.
  void set_disk_queue(DiskQueue* queue) { disk_queue_ = queue; }

  bool done() const { return done_; }
  void set_done(bool b) { done_ = b; }

  int num_remaining_ranges() const { return num_remaining_ranges_; }
  int& num_remaining_ranges() { return num_remaining_ranges_; }

  ScanRange* next_scan_range_to_start() { return next_scan_range_to_start_; }
  void set_next_scan_range_to_start(ScanRange* range) {
    next_scan_range_to_start_ = range;
  }

  bool is_on_queue() const { return is_on_queue_.Load() != 0; }

  int num_threads_in_op() const { return num_threads_in_op_.Load(); }

  const InternalQueue<ScanRange>* unstarted_scan_ranges() const {
    return &unstarted_scan_ranges_;
  }
  const InternalQueue<WriteRange>* unstarted_write_ranges() const {
    return &unstarted_write_ranges_;
  }
  const InternalQueue<RemoteOperRange>* unstarted_remote_file_oper_ranges() const {
    return &unstarted_remote_file_op_ranges_;
  }

  const InternalQueue<RequestRange>* in_flight_ranges() const {
    return &in_flight_ranges_;
  }

  InternalQueue<ScanRange>* unstarted_scan_ranges() { return &unstarted_scan_ranges_; }
  InternalQueue<WriteRange>* unstarted_write_ranges() {
    return &unstarted_write_ranges_;
  }
  InternalQueue<RemoteOperRange>* unstarted_remote_file_oper_ranges() {
    return &unstarted_remote_file_op_ranges_;
  }

  InternalQueue<RequestRange>* in_flight_ranges() { return &in_flight_ranges_; }

  /// Schedules the request context on this disk if it's not already on the queue.
  /// context->lock_ must be held by the caller via 'context_lock'.
  void ScheduleContext(const unique_lock<mutex>& context_lock,
      RequestContext* context, int disk_id);

  /// See RequestContext::IncrementDiskThreadAfterDequeue() comment for usage.
  ///
  /// The caller does not need to hold 'lock_', so this can execute concurrently with
  /// itself and DecrementDiskThread().
  void IncrementDiskThreadAfterDequeue() {
    /// Incrementing 'num_threads_in_op_' first so that there is no window when other
    /// threads see 'is_on_queue_ == num_threads_in_op_ == 0' and think there are no
    /// references left to this context.
    num_threads_in_op_.Add(1);
    is_on_queue_.Store(0);
  }

  /// Decrement the count of disks threads with a reference to this context. Does final
  /// cleanup if the context is cancelled and this is the last thread for the disk.
  /// context->lock_ must be held by the caller via 'context_lock'.
  void DecrementDiskThread(const unique_lock<mutex>& context_lock,
      RequestContext* context) {
    DCHECK(context_lock.mutex() == &context->lock_ && context_lock.owns_lock());
    num_threads_in_op_.Add(-1);

    if (context->state_ != Cancelled) {
      DCHECK_EQ(context->state_, Active);
      return;
    }
    // The state is cancelled, check to see if we're the last thread to touch the
    // context on this disk. We need to load 'is_on_queue_' and 'num_threads_in_op_'
    // in this order to avoid a race with IncrementDiskThreadAfterDequeue().
    if (is_on_queue_.Load() == 0 && num_threads_in_op_.Load() == 0 && !done_) {
      context->DecrementDiskRefCount(context_lock);
      done_ = true;
    }
  }

 private:
  /// The IoMgr disk queue corresponding to this context. Assigned during construction
  /// of the RequestContext and not modified after.
  DiskQueue* disk_queue_ = nullptr;

  /// If true, this disk is all done for this request context, including any cleanup.
  /// If done is true, it means that this request must not be on this disk's queue
  /// *AND* there are no threads currently working on this context. To satisfy
  /// this, only the last thread (per disk) can set this to true.
  bool done_ = true;

  /// For each disk, keeps track if the context is on this disk's queue, indicating
  /// the disk must do some work for this context. 1 means that the context is on the
  /// disk queue, 0 means that it's not on the queue (either because it has on ranges
  /// active for the disk or because a disk thread dequeued the context and is
  /// currently processing a request).
  ///
  /// The disk needs to do work in 4 cases:
  ///  1) in_flight_ranges is not empty, the disk needs to read for this reader.
  ///  2) next_range_to_start is NULL, the disk needs to prepare a scan range to be
  ///     read next.
  ///  3) the reader has been cancelled and this disk needs to participate in the
  ///     cleanup.
  ///  4) A write range is added to queue.
  /// In general, we only want to put a context on the disk queue if there is something
  /// useful that can be done. If there's nothing useful, the disk queue will wake up
  /// and then remove the reader from the queue. Doing this causes thrashing of the
  /// threads.
  ///
  /// This variable is important during context cancellation because it indicates
  /// whether a queue has a reference to the context that must be released before
  /// the context is considered unregistered. Atomically set to false after
  /// incrementing 'num_threads_in_op_' when dequeueing so that there is no window
  /// when other threads see 'is_on_queue_ == num_threads_in_op_ == 0' and think there
  /// are no references left to this context.
  /// TODO: this could be combined with 'num_threads_in_op_' to be a single refcount.
  AtomicInt32 is_on_queue_{0};

  /// For each disks, the number of request ranges that have not been fully read.
  /// In the non-cancellation path, this will hit 0, and done will be set to true
  /// by the disk thread. This is undefined in the cancellation path (the various
  /// threads notice by looking at the RequestContext's state_).
  int num_remaining_ranges_ = 0;

  /// Queue of ranges that have not started being read.  This list is exclusive
  /// with in_flight_ranges.
  InternalQueue<ScanRange> unstarted_scan_ranges_;

  /// Queue of pending IO requests for this disk in the order that they will be
  /// processed. A ScanRange is added to this queue when it is returned in
  /// GetNextUnstartedRange(), or when it is added with schedule_mode == IMMEDIATELY.
  /// A WriteRange is added to this queue from unstarted_write_ranges_ for each
  /// invocation of GetNextRequestRange() in WorkLoop().
  /// The size of this queue is always less than or equal to num_remaining_ranges.
  InternalQueue<RequestRange> in_flight_ranges_;

  /// The next range to start for this reader on this disk. Each disk (for each reader)
  /// picks the next range to start. The range is set here and also added to the
  /// ready_to_start_ranges_ queue. The reader pulls from the queue in FIFO order,
  /// so the ranges from different disks are round-robined. When the range is pulled
  /// off the ready_to_start_ranges_ queue, it sets this variable to NULL, so the disk
  /// knows to populate it again and add it to ready_to_start_ranges_ i.e. it is used
  /// as a flag by DiskIoMgr::GetNextScanRange to determine if it needs to add another
  /// range to ready_to_start_ranges_.
  ScanRange* next_scan_range_to_start_ = nullptr;

  /// For each disk, the number of disk threads issuing the underlying read/write on
  /// behalf of this context. There are a few places where we release the context lock,
  /// do some work, and then grab the lock again.  Because we don't hold the lock for
  /// the entire operation, we need this ref count to keep track of which thread should
  /// do final resource cleanup during cancellation.
  /// Only the thread that sees the count at 0 should do the final cleanup.
  AtomicInt32 num_threads_in_op_{0};

  /// Queue of write ranges to process for this disk. A write range is always added
  /// to in_flight_ranges_ in GetNextRequestRange(). There is a separate
  /// unstarted_read_ranges_ and unstarted_write_ranges_ to alternate between reads
  /// and writes. (Otherwise, since next_scan_range_to_start is set
  /// in GetNextRequestRange() whenever it is null, repeated calls to
  /// GetNextRequestRange() and GetNextUnstartedRange() may result in only reads being
  /// processed)
  InternalQueue<WriteRange> unstarted_write_ranges_;

  /// A Queue for file operation ranges to process file uploading or fetching operations.
  InternalQueue<RemoteOperRange> unstarted_remote_file_op_ranges_;
};

void RequestContext::ReadDone(int disk_id, ReadOutcome outcome, ScanRange* range) {
  // TODO: IMPALA-4249: it is safe to touch 'range' until DecrementDiskThread() is
  // called because all clients of DiskIoMgr keep ScanRange objects alive until they
  // unregister their RequestContext.
  unique_lock<mutex> lock(lock_);
  RequestContext::PerDiskState* disk_state = &disk_states_[disk_id];
  DCHECK_GT(disk_state->num_threads_in_op(), 0);
  if (outcome == ReadOutcome::SUCCESS_EOSR) {
    // No more reads to do.
    --disk_state->num_remaining_ranges();
  } else if (outcome == ReadOutcome::SUCCESS_NO_EOSR) {
    // Schedule the next read.
    if (state_ != RequestContext::Cancelled) {
      ScheduleScanRange(lock, range);
    }
  } else if (outcome == ReadOutcome::BLOCKED_ON_BUFFER) {
    // Do nothing - the caller must add a buffer to the range or cancel it.
  } else {
    DCHECK(outcome == ReadOutcome::CANCELLED) << static_cast<int>(outcome);
    // No more reads - clean up the scan range.
    --disk_state->num_remaining_ranges();
    RemoveActiveScanRangeLocked(lock, range);
  }
  // Release refcount that was taken in IncrementDiskThreadAfterDequeue().
  disk_state->DecrementDiskThread(lock, this);
  DCHECK(Validate()) << endl << DebugString();
}

void RequestContext::OperDone(RequestRange* range, const Status& status) {
  DCHECK(range != nullptr);

  // Copy disk_id before running callback: the callback may modify range.
  int disk_id = range->disk_id();

  // Execute the callback before decrementing the thread count. Otherwise
  // RequestContext::Cancel() that waits for the disk ref count to be 0 will
  // return, creating a race, e.g. see IMPALA-1890.
  // The status of the operation does not affect the status of the request context.
  if (range->request_type() == RequestType::WRITE) {
    (static_cast<WriteRange*>(range))->callback()(status);
  } else {
    DCHECK(range->request_type() == RequestType::FILE_UPLOAD
        || range->request_type() == RequestType::FILE_FETCH);
    (static_cast<RemoteOperRange*>(range))->callback()(status);
  }
  {
    unique_lock<mutex> lock(lock_);
    DCHECK(Validate()) << endl << DebugString();
    RequestContext::PerDiskState& state = disk_states_[disk_id];
    state.DecrementDiskThread(lock, this);
    --state.num_remaining_ranges();
  }
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
// that scan range will return a CANCELLED_INTERNALLY Status. Cancel() also invokes
// callbacks for all WriteRanges with a CANCELLED_INTERNALLY Status.
// 3. Disk threads notice the context is cancelled either when picking the next context
// to process or when they try to enqueue a ready buffer. Upon noticing the cancelled
// state, removes the context from the disk queue. The last thread per disk then calls
// DecrementDiskRefCount(). After the last disk thread has called DecrementDiskRefCount(),
// cancellation is done and it is safe to unregister the context.
void RequestContext::Cancel() {
  // Callbacks are collected in this vector and invoked while no lock is held.
  vector<WriteRange::WriteDoneCallback> write_callbacks;
  vector<RemoteOperRange::RemoteOperDoneCallback> remote_oper_callbacks;
  {
    unique_lock<mutex> lock(lock_);
    DCHECK(Validate()) << endl << DebugString();

    // Already being cancelled
    if (state_ == RequestContext::Cancelled) return;

    // The reader will be put into a cancelled state until call cleanup is complete.
    state_ = RequestContext::Cancelled;

    // Clear out all request ranges from queues for this reader. Cancel the scan
    // ranges and invoke the write range callbacks to propagate the cancellation.
    for (ScanRange* range : active_scan_ranges_) {
      range->CancelInternal(CONTEXT_CANCELLED, false);
    }
    active_scan_ranges_.clear();
    for (PerDiskState& disk_state : disk_states_) {
      RequestRange* range;
      while ((range = disk_state.in_flight_ranges()->Dequeue()) != nullptr) {
        if (range->request_type() == RequestType::WRITE) {
          write_callbacks.push_back(static_cast<WriteRange*>(range)->callback());
        }
      }
      while (disk_state.unstarted_scan_ranges()->Dequeue() != nullptr);
      WriteRange* write_range;
      while ((write_range = disk_state.unstarted_write_ranges()->Dequeue()) != nullptr) {
        write_callbacks.push_back(write_range->callback());
      }

      RemoteOperRange* oper_range;
      while ((oper_range = disk_state.unstarted_remote_file_oper_ranges()->Dequeue())
          != nullptr) {
        remote_oper_callbacks.push_back(oper_range->callback());
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
    write_callback(CONTEXT_CANCELLED);
  }

  for (const RemoteOperRange::RemoteOperDoneCallback& oper_callback :
      remote_oper_callbacks) {
    oper_callback(CONTEXT_CANCELLED);
  }

  // Signal reader and unblock the GetNext/Read thread.  That read will fail with
  // a cancelled status.
  ready_to_start_ranges_cv_.NotifyAll();
}

void RequestContext::CancelAndMarkInactive() {
  Cancel();

  unique_lock<mutex> l(lock_);
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
    } else if (schedule_mode != ScheduleMode::BY_CALLER) {
      if (schedule_mode == ScheduleMode::UPON_GETNEXT_TAIL) {
        disk_state->unstarted_scan_ranges()->Enqueue(scan_range);
      } else {
        DCHECK_ENUM_EQ(schedule_mode, ScheduleMode::UPON_GETNEXT_HEAD);
        disk_state->unstarted_scan_ranges()->PushFront(scan_range);
      }
      num_unstarted_scan_ranges_.Add(1);
      // If there's no 'next_scan_range_to_start', schedule this RequestContext so that
      // one of the 'unstarted_scan_ranges' will become the 'next_scan_range_to_start'.
      if (disk_state->next_scan_range_to_start() == nullptr) {
        disk_state->ScheduleContext(lock, this, range->disk_id());
      }
    }
  } else if (range->request_type() == RequestType::WRITE) {
    DCHECK(schedule_mode == ScheduleMode::IMMEDIATELY) << static_cast<int>(schedule_mode);
    WriteRange* write_range = static_cast<WriteRange*>(range);
    disk_state->unstarted_write_ranges()->Enqueue(write_range);

    // Ensure that the context is scheduled so that the write range gets picked up.
    // ScheduleContext() has no effect if already scheduled, so this is safe to do always.
    disk_state->ScheduleContext(lock, this, range->disk_id());
  } else {
    DCHECK(range->request_type() == RequestType::FILE_UPLOAD
        || range->request_type() == RequestType::FILE_FETCH);
    DCHECK(schedule_mode == ScheduleMode::IMMEDIATELY) << static_cast<int>(schedule_mode);
    RemoteOperRange* oper_range = static_cast<RemoteOperRange*>(range);
    disk_state->unstarted_remote_file_oper_ranges()->Enqueue(oper_range);
    disk_state->ScheduleContext(lock, this, range->disk_id());
  }

  ++disk_state->num_remaining_ranges();
}

Status RequestContext::AddScanRanges(
    const vector<ScanRange*>& ranges, EnqueueLocation enqueue_location) {
  DCHECK_GT(ranges.size(), 0);
  // Validate and initialize all ranges
  for (int i = 0; i < ranges.size(); ++i) {
    RETURN_IF_ERROR(parent_->ValidateScanRange(ranges[i]));
    ranges[i]->InitInternal(parent_, this);
  }

  unique_lock<mutex> lock(lock_);
  DCHECK(Validate()) << endl << DebugString();

  if (state_ == RequestContext::Cancelled) return CONTEXT_CANCELLED;

  // Add each range to the queue of the disk the range is on
  for (ScanRange* range : ranges) {
    // Don't add empty ranges.
    DCHECK_NE(range->bytes_to_read(), 0);
    AddActiveScanRangeLocked(lock, range);
    if (range->UseHdfsCache()) {
      cached_ranges_.Enqueue(range);
    } else {
      AddRangeToDisk(lock, range, (enqueue_location == EnqueueLocation::HEAD) ?
              ScheduleMode::UPON_GETNEXT_HEAD :
              ScheduleMode::UPON_GETNEXT_TAIL);
    }
  }
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK();
}

// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status RequestContext::GetNextUnstartedRange(ScanRange** range, bool* needs_buffers) {
  DCHECK(range != nullptr);
  *range = nullptr;
  *needs_buffers = false;

  unique_lock<mutex> lock(lock_);
  DCHECK(Validate()) << endl << DebugString();
  while (true) {
    if (state_ == RequestContext::Cancelled) return CONTEXT_CANCELLED;

    if (num_unstarted_scan_ranges_.Load() == 0 && ready_to_start_ranges_.empty()
        && cached_ranges_.empty()) {
      // All ranges are done, just return.
      return Status::OK();
    }

    if (!cached_ranges_.empty()) {
      // We have a cached range.
      *range = cached_ranges_.Dequeue();
      DCHECK((*range)->UseHdfsCache());
      bool cached_read_succeeded;
      RETURN_IF_ERROR(TryReadFromCache(lock, *range, &cached_read_succeeded,
          needs_buffers));
      if (cached_read_succeeded) return Status::OK();

      // This range ended up not being cached. Loop again and pick up a new range.
      AddRangeToDisk(lock, *range, ScheduleMode::UPON_GETNEXT_TAIL);
      DCHECK(Validate()) << endl << DebugString();
      *range = nullptr;
      continue;
    }

    if (ready_to_start_ranges_.empty()) {
      ready_to_start_ranges_cv_.Wait(lock);
    } else {
      *range = ready_to_start_ranges_.Dequeue();
      DCHECK(*range != nullptr);
      int disk_id = (*range)->disk_id();
      DCHECK_EQ(*range, disk_states_[disk_id].next_scan_range_to_start());
      // Set this to nullptr, the next time this disk runs for this reader, it will
      // get another range ready.
      disk_states_[disk_id].set_next_scan_range_to_start(nullptr);
      if ((*range)->buffer_manager_->is_internal_buffer()) {
        // We can't schedule this range until the client gives us buffers. The context
        // must be rescheduled regardless to ensure that 'next_scan_range_to_start' is
        // refilled.
        disk_states_[disk_id].ScheduleContext(lock, this, disk_id);
        (*range)->SetBlockedOnBuffer();
        *needs_buffers = true;
      } else {
        ScheduleScanRange(lock, *range);
      }
      return Status::OK();
    }
  }
}

Status RequestContext::StartScanRange(ScanRange* range, bool* needs_buffers) {
  RETURN_IF_ERROR(parent_->ValidateScanRange(range));
  range->InitInternal(parent_, this);

  unique_lock<mutex> lock(lock_);
  DCHECK(Validate()) << endl << DebugString();
  if (state_ == RequestContext::Cancelled) return CONTEXT_CANCELLED;

  DCHECK_NE(range->bytes_to_read(), 0);
  if (range->UseHdfsCache()) {
    bool cached_read_succeeded;
    RETURN_IF_ERROR(TryReadFromCache(lock, range, &cached_read_succeeded,
        needs_buffers));
    if (cached_read_succeeded) return Status::OK();
    // Cached read failed, fall back to normal read path.
  }
  // If we don't have a buffer yet, the caller must allocate buffers for the range.
  *needs_buffers =
      range->buffer_manager_->is_internal_buffer();
  if (*needs_buffers) range->SetBlockedOnBuffer();
  AddActiveScanRangeLocked(lock, range);
  AddRangeToDisk(lock, range,
      *needs_buffers ? ScheduleMode::BY_CALLER : ScheduleMode::IMMEDIATELY);
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK();
}

Status RequestContext::TryReadFromCache(const unique_lock<mutex>& lock,
    ScanRange* range, bool* read_succeeded, bool* needs_buffers) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  RETURN_IF_ERROR(range->ReadFromCache(lock, read_succeeded));
  if (!*read_succeeded) return Status::OK();

  DCHECK(Validate()) << endl << DebugString();
  // The following cases are possible at this point:
  // * The scan range doesn't have sub-ranges:
  // ** 'range->buffer_manager_->buffer_tag' is CACHED_BUFFER and the buffer is
  //    already available to the reader.
  //    (there is nothing to do)
  //
  // * The scan range has sub-ranges, and 'range->buffer_manager_->buffer_tag' is:
  // ** INTERNAL_BUFFER: the client needs to add buffers to the scan range.
  // ** CLIENT_BUFFER: the client already provided a buffer to copy data into it.
  *needs_buffers = range->buffer_manager_->is_internal_buffer();
  if (*needs_buffers) {
    DCHECK(range->HasSubRanges());
    range->SetBlockedOnBuffer();
    // The range will be scheduled when buffers are added to it.
    AddRangeToDisk(lock, range, ScheduleMode::BY_CALLER);
  } else if (range->buffer_manager_->is_client_buffer()) {
    DCHECK(range->HasSubRanges());
    AddRangeToDisk(lock, range, ScheduleMode::IMMEDIATELY);
  }
  return Status::OK();
}

Status RequestContext::AddWriteRange(WriteRange* write_range) {
  unique_lock<mutex> lock(lock_);
  if (state_ == RequestContext::Cancelled) return CONTEXT_CANCELLED;
  write_range->SetRequestContext(this);
  AddRangeToDisk(lock, write_range, ScheduleMode::IMMEDIATELY);
  return Status::OK();
}

Status RequestContext::AddRemoteOperRange(RemoteOperRange* oper_range) {
  unique_lock<mutex> lock(lock_);
  if (state_ == RequestContext::Cancelled) return CONTEXT_CANCELLED;
  AddRangeToDisk(lock, oper_range, ScheduleMode::IMMEDIATELY);
  return Status::OK();
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

// This function gets the next RequestRange to work on for this RequestContext and disk
// combination this disk. It checks for cancellation and:
// a) Updates ready_to_start_ranges if there are no scan ranges queued for this disk.
// b) Adds an unstarted write range to in_flight_ranges_. The write range is processed
//    immediately if there are no preceding scan ranges in in_flight_ranges_
RequestRange* RequestContext::GetNextRequestRange(int disk_id) {
  PerDiskState* request_disk_state = &disk_states_[disk_id];
  // NOTE: no locks are held, so other threads could have modified the state of the reader
  // and disk state since this context was pulled off the queue. Only one disk thread can
  // be in this function for this reader, since the reader was removed from the queue and
  // has not be re-added. Other disk threads may be operating on this reader in other
  // functions though.
  unique_lock<mutex> request_lock(lock_);
  VLOG_FILE << "Disk (id=" << disk_id << ") reading for " << DebugString();

  // Check if reader has been cancelled
  if (state_ == RequestContext::Cancelled) {
    request_disk_state->DecrementDiskThread(request_lock, this);
    return nullptr;
  }
  DCHECK_EQ(state_, RequestContext::Active) << DebugString();
  if (request_disk_state->next_scan_range_to_start() == nullptr &&
      !request_disk_state->unstarted_scan_ranges()->empty()) {
    // We don't have a range queued for this disk for what the caller should
    // read next. Populate that.  We want to have one range waiting to minimize
    // wait time in GetNextUnstartedRange().
    ScanRange* new_range = request_disk_state->unstarted_scan_ranges()->Dequeue();
    num_unstarted_scan_ranges_.Add(-1);
    ready_to_start_ranges_.Enqueue(new_range);
    request_disk_state->set_next_scan_range_to_start(new_range);

    if (num_unstarted_scan_ranges_.Load() == 0) {
      // All the ranges have been started, notify everyone blocked on
      // GetNextUnstartedRange(). Only one of them will get work so make sure to return
      // nullptr to the other caller threads.
      ready_to_start_ranges_cv_.NotifyAll();
    } else {
      ready_to_start_ranges_cv_.NotifyOne();
    }
  }

  // Always enqueue a WriteRange to be processed into in_flight_ranges_.
  // This is done so in_flight_ranges_ does not exclusively contain ScanRanges.
  // For now, enqueuing a WriteRange on each invocation of GetNextRequestRange()
  // does not flood in_flight_ranges() with WriteRanges because the entire
  // WriteRange is processed and removed from the queue after GetNextRequestRange()
  // returns.
  if (!request_disk_state->unstarted_write_ranges()->empty()) {
    WriteRange* write_range = request_disk_state->unstarted_write_ranges()->Dequeue();
    request_disk_state->in_flight_ranges()->Enqueue(write_range);
  }

  // Do remote temporary files related work.
  if (!request_disk_state->unstarted_remote_file_oper_ranges()->empty()) {
    RemoteOperRange* oper_range;
    if (!request_disk_state->unstarted_remote_file_oper_ranges()->empty()) {
      oper_range = request_disk_state->unstarted_remote_file_oper_ranges()->Dequeue();
      request_disk_state->in_flight_ranges()->Enqueue(oper_range);
    }
  }

  // Get the next scan range to work on from the reader. Only in_flight_ranges
  // are eligible since the disk threads do not start new ranges on their own.
  if (request_disk_state->in_flight_ranges()->empty()) {
    // There are no inflight ranges, nothing to do.
    request_disk_state->DecrementDiskThread(request_lock, this);
    return nullptr;
  }
  DCHECK_GT(request_disk_state->num_remaining_ranges(), 0);
  RequestRange* range = request_disk_state->in_flight_ranges()->Dequeue();
  DCHECK(range != nullptr);

  // Now that we've picked a request range, put the context back on the queue so
  // another thread can pick up another request range for this context.
  request_disk_state->ScheduleContext(request_lock, this, disk_id);
  DCHECK(Validate()) << endl << DebugString();
  return range;
}

RequestContext::RequestContext(
    DiskIoMgr* parent, const std::vector<DiskQueue*>& disk_queues)
  : parent_(parent), disk_states_(disk_queues.size()) {
  // PerDiskState is not movable, so we need to initialize the vector in this awkward way.
  for (int i = 0; i < disk_queues.size(); ++i) {
    disk_states_[i].set_disk_queue(disk_queues[i]);
  }
  ThreadDebugInfo* tdi = GetThreadDebugInfo();
  if (tdi != nullptr) {
    set_query_id(tdi->GetQueryId());
    set_instance_id(tdi->GetInstanceId());
  }
}

RequestContext::~RequestContext() {
  DCHECK_EQ(state_, Inactive) << "Must be unregistered. " << DebugString();
}

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

void RequestContext::UnregisterDiskQueue(int disk_id) {
  unique_lock<mutex> lock(lock_);
  DCHECK_EQ(disk_states_[disk_id].num_threads_in_op(), 0);
  DCHECK(disk_states_[disk_id].done());
  DecrementDiskRefCount(lock);
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
    disk_queue_->EnqueueContext(context);
  }
}

void RequestContext::IncrementDiskThreadAfterDequeue(int disk_id) {
  disk_states_[disk_id].IncrementDiskThreadAfterDequeue();
}

void RequestContext::DecrementDiskRefCount(const std::unique_lock<std::mutex>& lock) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK_GT(num_disks_with_ranges_, 0);
  if (--num_disks_with_ranges_ == 0) {
    disks_complete_cond_var_.NotifyAll();
  }
  DCHECK(Validate()) << std::endl << DebugString();
}

void RequestContext::ScheduleScanRange(
    const std::unique_lock<std::mutex>& lock, ScanRange* range) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK_EQ(state_, Active);
  DCHECK(range != nullptr);
  RequestContext::PerDiskState& state = disk_states_[range->disk_id()];
  state.in_flight_ranges()->Enqueue(range);
  state.ScheduleContext(lock, this, range->disk_id());
}
