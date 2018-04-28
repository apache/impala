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

#ifndef IMPALA_RUNTIME_IO_REQUEST_CONTEXT_H
#define IMPALA_RUNTIME_IO_REQUEST_CONTEXT_H

#include <boost/unordered_set.hpp>

#include "runtime/io/disk-io-mgr.h"
#include "util/condition-variable.h"

namespace impala {
namespace io {

// Mode argument for AddRangeToDisk().
enum class ScheduleMode {
  IMMEDIATELY, UPON_GETNEXT, BY_CALLER
};
/// A request context is used to group together I/O requests belonging to a client of the
/// I/O manager for management and scheduling.
///
/// Implementation Details
/// ======================
/// This object maintains a lot of state that is carefully synchronized. The context
/// maintains state across all disks as well as per disk state.
/// The unit for an IO request is a RequestRange, which may be a ScanRange or a
/// WriteRange.
/// A scan range for the reader is on one of six states:
/// 1) PerDiskState's 'unstarted_scan_ranges_': This range has only been queued
///    and nothing has been read from it.
/// 2) RequestContext's 'ready_to_start_ranges_': This range is about to be started.
///    As soon as the reader picks it up, it will move to the 'in_flight_ranges_'
///    queue.
/// 3) PerDiskState's 'in_flight_ranges_': This range is being processed and will
///    be read from the next time a disk thread picks it up in GetNextRequestRange()
/// 4) The ScanRange is blocked waiting for buffers because it does not have any unused
///    buffers to read data into. It is unblocked when a client adds new buffers via
///    AllocateBuffersForRange() or returns existing buffers via ReturnBuffer().
///    ScanRanges in this state are identified by 'blocked_on_buffer_' == true.
/// 5) ScanRange is cached and in the 'cached_ranges_' queue.
/// 6) Inactive - either all the data for the range was returned or the range was
///    cancelled. I.e. ScanRange::eosr_ is true or ScanRange::cancel_status_ != OK.
///
/// If the scan range is read and does not get blocked waiting for buffers, the
/// transitions are: 1 -> 2 -> 3.
/// If the scan range does get blocked, the transitions are
/// 1 -> 2 -> 3 -> (4 -> 3)*
///
/// In the case of a cached scan range, the range is immediately put in 'cached_ranges_'.
/// When the caller asks for the next range to process, we first pull ranges from
/// the 'cache_ranges_' queue. If the range was cached, the range is removed and
/// done (ranges are either entirely cached or not at all). If the cached read attempt
/// fails, we put the range in state 1.
///
/// All scan ranges in states 1-5 are tracked in 'active_scan_ranges_' so that they can be
/// cancelled when the RequestContext is cancelled. Scan ranges are removed from
/// 'active_scan_ranges_' during their transition to state 6.
///
/// A write range for a context may be in one of two queues:
/// 1) 'unstarted_write_ranges_': Ranges that have been queued but not processed.
/// 2) 'in_flight_ranges_': The write range is ready to be processed by the next disk
///    thread that picks it up in GetNextRequestRange().
///
/// AddWriteRange() adds WriteRanges for a disk.
/// It is the responsibility of the client to pin the data to be written via a WriteRange
/// in memory. After a WriteRange has been written, a callback is invoked to inform the
/// client that the write has completed.
///
/// An important assumption is that write does not exceed the maximum read size and that
/// the entire range is written when the write request is handled. (In other words, writes
/// are not broken up.)
///
/// When a RequestContext is processed by a disk thread in GetNextRequestRange(),
/// a write range is always removed from the list of unstarted write ranges and appended
/// to the in_flight_ranges_ queue. This is done to alternate reads and writes - a read
/// that is scheduled (by calling GetNextUnstartedRange()) is always followed by a write
/// (if one exists). And since at most one WriteRange can be present in in_flight_ranges_
/// at any time (once a write range is returned from GetNetxRequestRange() it is completed
/// and not re-enqueued), a scan range scheduled via a call to GetNextUnstartedRange() can
/// be queued up behind at most one write range.
class RequestContext {
 public:
  ~RequestContext() {
    DCHECK_EQ(state_, Inactive) << "Must be unregistered. " << DebugString();
  }

  /// Cancel the context asynchronously. All outstanding requests are cancelled
  /// asynchronously. This does not need to be called if the context finishes normally.
  /// Calling GetNext() on any scan ranges belonging to this RequestContext will return
  /// CANCELLED (or another error, if an error was encountered for that scan range before
  /// it is cancelled).
  void Cancel();

  bool IsCancelled() {
    boost::unique_lock<boost::mutex> lock(lock_);
    return state_ == Cancelled;
  }

  int64_t bytes_read_local() const { return bytes_read_local_.Load(); }
  int64_t bytes_read_short_circuit() const { return bytes_read_short_circuit_.Load(); }
  int64_t bytes_read_dn_cache() const { return bytes_read_dn_cache_.Load(); }
  int num_remote_ranges() const { return num_remote_ranges_.Load(); }
  int64_t unexpected_remote_bytes() const { return unexpected_remote_bytes_.Load(); }

  int cached_file_handles_hit_count() const {
    return cached_file_handles_hit_count_.Load();
  }

  int cached_file_handles_miss_count() const {
    return cached_file_handles_miss_count_.Load();
  }

  void set_bytes_read_counter(RuntimeProfile::Counter* bytes_read_counter) {
    bytes_read_counter_ = bytes_read_counter;
  }

  void set_read_timer(RuntimeProfile::Counter* read_timer) { read_timer_ = read_timer; }

  void set_open_file_timer(RuntimeProfile::Counter* open_file_timer) {
    open_file_timer_ = open_file_timer;
  }

  void set_active_read_thread_counter(
      RuntimeProfile::Counter* active_read_thread_counter) {
   active_read_thread_counter_ = active_read_thread_counter;
  }

  void set_disks_accessed_bitmap(RuntimeProfile::Counter* disks_accessed_bitmap) {
    disks_accessed_bitmap_ = disks_accessed_bitmap;
  }

 protected:
  // Protected methods are accessed by other classes in io:: but not external classes.
  friend class DiskQueue;
  friend class DiskIoMgr;
  friend class ScanRange;

  enum State {
    /// Reader is initialized and maps to a client
    Active,

    /// Reader is in the process of being cancelled.  Cancellation is coordinated between
    /// different threads and when they are all complete, the reader context is moved to
    /// the inactive state.
    Cancelled,

    /// Reader context does not map to a client.  Accessing memory in this context
    /// is invalid (i.e. it is equivalent to a dangling pointer).
    Inactive,
  };

  RequestContext(DiskIoMgr* parent, int num_disks);

  /// Called when dequeueing this RequestContext from the disk queue to increment the
  /// count of disk threads with a reference to this context for 'disk_id. These threads
  /// do not hold any locks while reading from HDFS, so we need to prevent the
  /// RequestContext from being destroyed underneath them.
  void IncrementDiskThreadAfterDequeue(int disk_id);

  /// Called when the disk queue for disk 'disk_id' shuts down. Only used in backend
  /// tests - disk queues are not shut down for the singleton DiskIoMgr in a daemon.
  void UnregisterDiskQueue(int disk_id);

  /// Decrements the number of active disks for this reader.  If the disk count
  /// goes to 0, the disk complete condition variable is signaled.
  /// 'lock_' must be held via 'lock'.
  void DecrementDiskRefCount(const boost::unique_lock<boost::mutex>& lock);

  /// Reader & Disk Scheduling: Readers that currently can't do work are not on
  /// the disk's queue. These readers are ones that don't have any ranges in the
  /// in_flight_queue AND have not prepared a range by setting next_range_to_start.
  /// The rule to make sure readers are scheduled correctly is to ensure anytime a
  /// range is put on the in_flight_queue or anytime next_range_to_start is set to
  /// NULL, the reader is scheduled.

  /// Adds range to in_flight_ranges, scheduling this reader on the disk threads
  /// if necessary.
  /// 'lock_' must be held via 'lock'. Only valid to call if this context is active.
  void ScheduleScanRange(const boost::unique_lock<boost::mutex>& lock, ScanRange* range);

  // Called from the disk thread for 'disk_id' to get the next request range to process
  // for this context for the disk. Returns nullptr if there are no ranges currently
  // available. The calling disk thread should hold no locks and must hold a refcount
  // obtained from IncrementDiskThreadAfterDequeue() to ensure that the context is not
  // destroyed while executing this function.
  RequestRange* GetNextRequestRange(int disk_id);

  /// Called from a disk thread when a read completes. Decrements the disk thread count
  /// and other bookkeeping and re-schedules 'range' if there are more reads to do.
  /// Caller must not hold 'lock_'.
  void ReadDone(int disk_id, ReadOutcome outcome, ScanRange* range);

  /// Cancel the context if not already cancelled, wait for all scan ranges to finish
  /// and mark the context as inactive, after which it cannot be used.
  void CancelAndMarkInactive();

  /// Adds a request range to the appropriate disk state. 'schedule_mode' controls which
  /// queue the range is placed in. This RequestContext is scheduled on the disk state
  /// if required by 'schedule_mode'.
  ///
  /// Write ranges must always have 'schedule_mode' IMMEDIATELY and are added to the
  /// 'unstarted_write_ranges_' queue, from which they will be asynchronously moved to the
  /// 'in_flight_ranges_' queue.
  ///
  /// Scan ranges can have different 'schedule_mode' values. If IMMEDIATELY, the range is
  /// immediately added to the 'in_flight_ranges_' queue where it will be processed
  /// asynchronously by disk threads. If UPON_GETNEXT, the range is added to the
  /// 'unstarted_ranges_' queue, from which it can be returned to a client by
  /// DiskIoMgr::GetNextUnstartedRange(). If BY_CALLER, the scan range is not added to
  /// any queues. The range will be scheduled later as a separate step, e.g. when it is
  /// unblocked by adding buffers to it. Caller must hold 'lock_' via 'lock'.
  void AddRangeToDisk(const boost::unique_lock<boost::mutex>& lock, RequestRange* range,
      ScheduleMode schedule_mode);

  /// Adds an active range to 'active_scan_ranges_'
  void AddActiveScanRangeLocked(
      const boost::unique_lock<boost::mutex>& lock, ScanRange* range);

  /// Removes the range from 'active_scan_ranges_'. Called by ScanRange after eos or
  /// cancellation. If calling the Locked version, the caller must hold
  /// 'lock_'. Otherwise the function will acquire 'lock_'.
  void RemoveActiveScanRange(ScanRange* range);
  void RemoveActiveScanRangeLocked(
      const boost::unique_lock<boost::mutex>& lock, ScanRange* range);

  // Counters are updated by other classes - expose to other io:: classes for convenience.

  /// Total bytes read for this reader
  RuntimeProfile::Counter* bytes_read_counter_ = nullptr;

  /// Total time spent in hdfs reading
  RuntimeProfile::Counter* read_timer_ = nullptr;

  /// Total time spent open hdfs file handles
  RuntimeProfile::Counter* open_file_timer_ = nullptr;

  /// Number of active read threads
  RuntimeProfile::Counter* active_read_thread_counter_ = nullptr;

  /// Disk access bitmap. The counter's bit[i] is set if disk id i has been accessed.
  /// TODO: we can only support up to 64 disks with this bitmap but it lets us use a
  /// builtin atomic instruction. Probably good enough for now.
  RuntimeProfile::Counter* disks_accessed_bitmap_ = nullptr;

  /// Total number of bytes read locally, updated at end of each range scan
  AtomicInt64 bytes_read_local_{0};

  /// Total number of bytes read via short circuit read, updated at end of each range scan
  AtomicInt64 bytes_read_short_circuit_{0};

  /// Total number of bytes read from date node cache, updated at end of each range scan
  AtomicInt64 bytes_read_dn_cache_{0};

  /// Total number of bytes from remote reads that were expected to be local.
  AtomicInt64 unexpected_remote_bytes_{0};

  /// The number of scan ranges that required a remote read, updated at the end of each
  /// range scan. Only used for diagnostics.
  AtomicInt32 num_remote_ranges_{0};

  /// The total number of scan ranges that have not been started. Only used for
  /// diagnostics. This is the sum of all unstarted_scan_ranges across all disks.
  AtomicInt32 num_unstarted_scan_ranges_{0};

  /// Total number of file handle opens where the file handle was present in the cache
  AtomicInt32 cached_file_handles_hit_count_{0};

  /// Total number of file handle opens where the file handle was not in the cache
  AtomicInt32 cached_file_handles_miss_count_{0};

 private:
  DISALLOW_COPY_AND_ASSIGN(RequestContext);

  /// Validates invariants of reader. 'lock_' must be held by caller.
  bool Validate() const;

  /// Dumps out reader information. 'lock_' must be held by caller.
  std::string DebugString() const;

  /// Parent object
  DiskIoMgr* const parent_;

  /// All fields below are accessed by multiple threads and the lock needs to be
  /// taken before accessing them. Must be acquired before ScanRange::lock_ if both
  /// are held simultaneously.
  boost::mutex lock_;

  /// Current state of the reader
  State state_ = Active;

  /// Scan ranges that have been added to the IO mgr for this context. Ranges can only
  /// be added when 'state_' is Active. When this context is cancelled, Cancel() is
  /// called for all the active ranges. If a client attempts to add a range while
  /// 'state_' is Cancelled, the range is not added to this list and Status::CANCELLED
  /// is returned to the client. This ensures that all active ranges are cancelled as a
  /// result of RequestContext cancellation.
  /// Ranges can be cancelled or hit eos non-atomically with their removal from this set,
  /// so eos or cancelled ranges may be temporarily present here. Cancelling these ranges
  /// a second time or cancelling after eos is safe and has no effect.
  boost::unordered_set<ScanRange*> active_scan_ranges_;

  /// The number of disks with scan ranges remaining (always equal to the sum of
  /// disks with ranges).
  int num_disks_with_ranges_ = 0;

  /// This is the list of ranges that are expected to be cached on the DN.
  /// When the reader asks for a new range (GetNextScanRange()), we first
  /// return ranges from this list.
  InternalList<ScanRange> cached_ranges_;

  /// A list of ranges that should be returned in subsequent calls to
  /// GetNextUnstartedRange().
  /// There is a trade-off with when to populate this list.  Populating it on
  /// demand means consumers need to wait (happens in DiskIoMgr::GetNextUnstartedRange()).
  /// Populating it preemptively means we make worse scheduling decisions.
  /// We currently populate one range per disk.
  /// TODO: think about this some more.
  InternalList<ScanRange> ready_to_start_ranges_;
  ConditionVariable ready_to_start_ranges_cv_; // used with lock_

  /// Condition variable for UnregisterContext() to wait for all disks to complete
  ConditionVariable disks_complete_cond_var_;

 private:
  /// Per disk states to synchronize multiple disk threads accessing the same request
  /// context.
  class PerDiskState;
  std::vector<PerDiskState> disk_states_;
};

/// Struct containing state per disk. See comments in the disk read loop on how
/// they are used.
class RequestContext::PerDiskState {
 public:
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
  const InternalQueue<RequestRange>* in_flight_ranges() const {
    return &in_flight_ranges_;
  }

  InternalQueue<ScanRange>* unstarted_scan_ranges() { return &unstarted_scan_ranges_; }
  InternalQueue<WriteRange>* unstarted_write_ranges() {
    return &unstarted_write_ranges_;
  }
  InternalQueue<RequestRange>* in_flight_ranges() { return &in_flight_ranges_; }

  /// Schedules the request context on this disk if it's not already on the queue.
  /// context->lock_ must be held by the caller via 'context_lock'.
  void ScheduleContext(const boost::unique_lock<boost::mutex>& context_lock,
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
  void DecrementDiskThread(const boost::unique_lock<boost::mutex>& context_lock,
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
};

inline void RequestContext::IncrementDiskThreadAfterDequeue(int disk_id) {
  disk_states_[disk_id].IncrementDiskThreadAfterDequeue();
}

inline void RequestContext::DecrementDiskRefCount(
    const boost::unique_lock<boost::mutex>& lock) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK_GT(num_disks_with_ranges_, 0);
  if (--num_disks_with_ranges_ == 0) {
    disks_complete_cond_var_.NotifyAll();
  }
  DCHECK(Validate()) << std::endl << DebugString();
}

inline void RequestContext::ScheduleScanRange(
    const boost::unique_lock<boost::mutex>& lock, ScanRange* range) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK_EQ(state_, Active);
  DCHECK(range != nullptr);
  RequestContext::PerDiskState& state = disk_states_[range->disk_id()];
  state.in_flight_ranges()->Enqueue(range);
  state.ScheduleContext(lock, this, range->disk_id());
}
}
}

#endif
