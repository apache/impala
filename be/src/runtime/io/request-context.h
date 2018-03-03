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

#include "runtime/io/disk-io-mgr.h"
#include "util/condition-variable.h"

namespace impala {
namespace io {
/// A request context is used to group together I/O requests belonging to a client of the
/// I/O manager for management and scheduling. For most I/O manager clients it is an
/// opaque pointer, but some clients may need to include this header, e.g. to make the
/// unique_ptr<DiskIoRequestContext> destructor work correctly.
///
/// Implementation Details
/// ======================
/// This object maintains a lot of state that is carefully synchronized. The context
/// maintains state across all disks as well as per disk state.
/// The unit for an IO request is a RequestRange, which may be a ScanRange or a
/// WriteRange.
/// A scan range for the reader is on one of five states:
/// 1) PerDiskState's unstarted_ranges: This range has only been queued
///    and nothing has been read from it.
/// 2) RequestContext's ready_to_start_ranges_: This range is about to be started.
///    As soon as the reader picks it up, it will move to the in_flight_ranges
///    queue.
/// 3) PerDiskState's in_flight_ranges: This range is being processed and will
///    be read from the next time a disk thread picks it up in GetNextRequestRange()
/// 4) ScanRange's outgoing ready buffers is full. We can't read for this range
///    anymore. We need the caller to pull a buffer off which will put this in
///    the in_flight_ranges queue. These ranges are in the RequestContext's
///    blocked_ranges_ queue.
/// 5) ScanRange is cached and in the cached_ranges_ queue.
//
/// If the scan range is read and does not get blocked on the outgoing queue, the
/// transitions are: 1 -> 2 -> 3.
/// If the scan range does get blocked, the transitions are
/// 1 -> 2 -> 3 -> (4 -> 3)*
//
/// In the case of a cached scan range, the range is immediately put in cached_ranges_.
/// When the caller asks for the next range to process, we first pull ranges from
/// the cache_ranges_ queue. If the range was cached, the range is removed and
/// done (ranges are either entirely cached or not at all). If the cached read attempt
/// fails, we put the range in state 1.
//
/// A write range for a context may be in one of two lists:
/// 1) unstarted_write_ranges_ : Ranges that have been queued but not processed.
/// 2) in_flight_ranges_: The write range is ready to be processed by the next disk thread
///    that picks it up in GetNextRequestRange().
//
/// AddWriteRange() adds WriteRanges for a disk.
/// It is the responsibility of the client to pin the data to be written via a WriteRange
/// in memory. After a WriteRange has been written, a callback is invoked to inform the
/// client that the write has completed.
//
/// An important assumption is that write does not exceed the maximum read size and that
/// the entire range is written when the write request is handled. (In other words, writes
/// are not broken up.)
//
/// When a RequestContext is processed by a disk thread in GetNextRequestRange(),
/// a write range is always removed from the list of unstarted write ranges and appended
/// to the in_flight_ranges_ queue. This is done to alternate reads and writes - a read
/// that is scheduled (by calling GetNextRange()) is always followed by a write (if one
/// exists).  And since at most one WriteRange can be present in in_flight_ranges_ at any
/// time (once a write range is returned from GetNetxRequestRange() it is completed an
/// not re-enqueued), a scan range scheduled via a call to GetNextRange() can be queued up
/// behind at most one write range.
class RequestContext {
 public:
  ~RequestContext() { DCHECK_EQ(state_, Inactive) << "Must be unregistered."; }

 private:
  DISALLOW_COPY_AND_ASSIGN(RequestContext);
  friend class DiskIoMgr;
  friend class ScanRange;

  class PerDiskState;

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

  RequestContext(DiskIoMgr* parent, int num_disks, MemTracker* tracker);

  /// Decrements the number of active disks for this reader.  If the disk count
  /// goes to 0, the disk complete condition variable is signaled.
  /// Reader lock must be taken before this call.
  void DecrementDiskRefCount() {
    // boost doesn't let us dcheck that the reader lock is taken
    DCHECK_GT(num_disks_with_ranges_, 0);
    if (--num_disks_with_ranges_ == 0) {
      disks_complete_cond_var_.NotifyAll();
    }
    DCHECK(Validate()) << std::endl << DebugString();
  }

  /// Reader & Disk Scheduling: Readers that currently can't do work are not on
  /// the disk's queue. These readers are ones that don't have any ranges in the
  /// in_flight_queue AND have not prepared a range by setting next_range_to_start.
  /// The rule to make sure readers are scheduled correctly is to ensure anytime a
  /// range is put on the in_flight_queue or anytime next_range_to_start is set to
  /// NULL, the reader is scheduled.

  /// Adds range to in_flight_ranges, scheduling this reader on the disk threads
  /// if necessary.
  /// Reader lock must be taken before this.
  void ScheduleScanRange(ScanRange* range) {
    DCHECK_EQ(state_, Active);
    DCHECK(range != NULL);
    RequestContext::PerDiskState& state = disk_states_[range->disk_id()];
    state.in_flight_ranges()->Enqueue(range);
    state.ScheduleContext(this, range->disk_id());
  }

  /// Cancels the context with status code 'status'
  void Cancel(const Status& status);

  /// Cancel the context if not already cancelled, wait for all scan ranges to finish
  /// and mark the context as inactive, after which it cannot be used.
  void CancelAndMarkInactive();

  /// Adds request range to disk queue for this request context. Currently,
  /// schedule_immediately must be false is RequestRange is a write range.
  void AddRequestRange(RequestRange* range, bool schedule_immediately);

  /// Validates invariants of reader.  Reader lock must be taken beforehand.
  bool Validate() const;

  /// Dumps out reader information.  Lock should be taken by caller
  std::string DebugString() const;

  /// Parent object
  DiskIoMgr* const parent_;

  /// Memory used for this reader.  This is unowned by this object.
  MemTracker* const mem_tracker_;

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

  /// The number of buffers that have been returned to the reader (via GetNext) that the
  /// reader has not returned. Only included for debugging and diagnostics.
  AtomicInt32 num_buffers_in_reader_{0};

  /// The number of scan ranges that have been completed for this reader.
  AtomicInt32 num_finished_ranges_{0};

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

  /// The number of buffers that are being used for this reader. This is the sum
  /// of all buffers in ScanRange queues and buffers currently being read into (i.e. about
  /// to be queued). This includes both IOMgr-allocated buffers and client-provided
  /// buffers.
  AtomicInt32 num_used_buffers_{0};

  /// The total number of ready buffers across all ranges.  Ready buffers are buffers
  /// that have been read from disk but not retrieved by the caller.
  /// This is the sum of all queued buffers in all ranges for this reader context.
  AtomicInt32 num_ready_buffers_{0};

  /// All fields below are accessed by multiple threads and the lock needs to be
  /// taken before accessing them. Must be acquired before ScanRange::lock_ if both
  /// are held simultaneously.
  boost::mutex lock_;

  /// Current state of the reader
  State state_ = Active;

  /// Status of this reader.  Set to non-ok if cancelled.
  Status status_;

  /// The number of disks with scan ranges remaining (always equal to the sum of
  /// disks with ranges).
  int num_disks_with_ranges_ = 0;

  /// This is the list of ranges that are expected to be cached on the DN.
  /// When the reader asks for a new range (GetNextScanRange()), we first
  /// return ranges from this list.
  InternalQueue<ScanRange> cached_ranges_;

  /// A list of ranges that should be returned in subsequent calls to
  /// GetNextRange.
  /// There is a trade-off with when to populate this list.  Populating it on
  /// demand means consumers need to wait (happens in DiskIoMgr::GetNextRange()).
  /// Populating it preemptively means we make worse scheduling decisions.
  /// We currently populate one range per disk.
  /// TODO: think about this some more.
  InternalQueue<ScanRange> ready_to_start_ranges_;
  ConditionVariable ready_to_start_ranges_cv_; // used with lock_

  /// Ranges that are blocked due to back pressure on outgoing buffers.
  InternalQueue<ScanRange> blocked_ranges_;

  /// Condition variable for UnregisterContext() to wait for all disks to complete
  ConditionVariable disks_complete_cond_var_;

  /// Struct containing state per disk. See comments in the disk read loop on how
  /// they are used.
  class PerDiskState {
   public:
    bool done() const { return done_; }
    void set_done(bool b) { done_ = b; }

    int num_remaining_ranges() const { return num_remaining_ranges_; }
    int& num_remaining_ranges() { return num_remaining_ranges_; }

    ScanRange* next_scan_range_to_start() { return next_scan_range_to_start_; }
    void set_next_scan_range_to_start(ScanRange* range) {
      next_scan_range_to_start_ = range;
    }

    /// We need to have a memory barrier to prevent this load from being reordered
    /// with num_threads_in_op(), since these variables are set without the reader
    /// lock taken
    bool is_on_queue() const {
      bool b = is_on_queue_;
      __sync_synchronize();
      return b;
    }

    int num_threads_in_op() const {
      int v = num_threads_in_op_.Load();
      // TODO: determine whether this barrier is necessary for any callsites.
      AtomicUtil::MemoryBarrier();
      return v;
    }

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
    /// Context lock must be taken before this.
    void ScheduleContext(RequestContext* context, int disk_id);

    /// Increment the ref count on reader.  We need to track the number of threads per
    /// reader per disk that are in the unlocked hdfs read code section. This is updated
    /// by multiple threads without a lock so we need to use an atomic int.
    void IncrementRequestThreadAndDequeue() {
      num_threads_in_op_.Add(1);
      is_on_queue_ = false;
    }

    void DecrementRequestThread() { num_threads_in_op_.Add(-1); }

    /// Decrement request thread count and do final cleanup if this is the last
    /// thread. RequestContext lock must be taken before this.
    void DecrementRequestThreadAndCheckDone(RequestContext* context) {
      num_threads_in_op_.Add(-1); // Also acts as a barrier.
      if (!is_on_queue_ && num_threads_in_op_.Load() == 0 && !done_) {
        // This thread is the last one for this reader on this disk, do final cleanup
        context->DecrementDiskRefCount();
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
    /// the disk must do some work for this context. The disk needs to do work in 4 cases:
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
    bool is_on_queue_ = false;

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
    /// GetNextRange(), or when it is added with schedule_immediately = true.
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

    /// For each disk, the number of threads issuing the underlying read/write on behalf
    /// of this context. There are a few places where we release the context lock, do some
    /// work, and then grab the lock again.  Because we don't hold the lock for the
    /// entire operation, we need this ref count to keep track of which thread should do
    /// final resource cleanup during cancellation.
    /// Only the thread that sees the count at 0 should do the final cleanup.
    AtomicInt32 num_threads_in_op_{0};

    /// Queue of write ranges to process for this disk. A write range is always added
    /// to in_flight_ranges_ in GetNextRequestRange(). There is a separate
    /// unstarted_read_ranges_ and unstarted_write_ranges_ to alternate between reads
    /// and writes. (Otherwise, since next_scan_range_to_start is set
    /// in GetNextRequestRange() whenever it is null, repeated calls to
    /// GetNextRequestRange() and GetNextRange() may result in only reads being processed)
    InternalQueue<WriteRange> unstarted_write_ranges_;
  };

  /// Per disk states to synchronize multiple disk threads accessing the same request
  /// context.
  std::vector<PerDiskState> disk_states_;
};
}
}

#endif
