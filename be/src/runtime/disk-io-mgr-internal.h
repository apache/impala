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

#ifndef IMPALA_RUNTIME_DISK_IO_MGR_INTERNAL_H
#define IMPALA_RUNTIME_DISK_IO_MGR_INTERNAL_H

#include "disk-io-mgr.h"
#include <queue>
#include <boost/thread/locks.hpp>

#include "common/logging.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"

// This file contains internal structures to the IoMgr. Users of the IoMgr do
// not need to include this file.
namespace impala {

// Per disk state
struct DiskIoMgr::DiskQueue {
  // Disk id (0-based)
  int disk_id;

  // Lock that protects access to 'readers' and 'work_available'
  boost::mutex lock;

  // Condition variable to signal the disk threads that there is work to do or the
  // thread should shut down.  A disk thread will be woken up when there is a reader
  // added to the queue. A reader is only on the queue when it has at least one
  // scan range that is not blocked on available buffers.
  boost::condition_variable work_available;

  // list of all readers that have work queued on this disk
  std::list<ReaderContext*> readers;

  // Enqueue the reader to the disk queue.  The DiskQueue lock must not be taken.
  inline void EnqueueReader(ReaderContext* reader) {
    {
      boost::unique_lock<boost::mutex> disk_lock(lock);
      // Check that the reader is not already on the queue
      DCHECK(find(readers.begin(), readers.end(), reader) == readers.end());
      readers.push_back(reader);
    }
    work_available.notify_all();
  }

  DiskQueue(int id) : disk_id(id) { }
};

// Internal per reader state. This object maintains a lot of state that is carefully
// synchronized.  The reader maintains state across all disks as well as per disk state.
// A scan range for the reader is on one of four states:
// 1) PerDiskState's unstarted_ranges: This range has only been queued
//    and nothing has been read from it.
// 2) ReaderContext's ready_to_start_ranges_: This range is about to be started.
//    As soon as the reader picks it up, it will move to the in_flight_ranges
//    queue.
// 3) PerDiskState's in_flight_ranges: This range is being processed and will
//    be read from the next time a disk thread picks it up.
// 4) ScanRange's outgoing ready buffers is full. We can't read for this range
//    anymore. We need the caller to pull a buffer off which will put this in
//    the in_flight_ranges queue. These ranges are in the ReaderContext's
//    blocked_ranges_ queue.
//
// If the scan range is read and does not get blocked on the outgoing queue, the
// transitions are: 1 -> 2 -> 3.
// If the scan range does get blocked, the transitions are
// 1 -> 2 -> 3 -> (4 -> 3)*
class DiskIoMgr::ReaderContext {
 public:
  enum State {
    // Reader is initialized and maps to a client
    Active,

    // Reader is in the process of being cancelled.  Cancellation is coordinated between
    // different threads and when they are all complete, the reader context is moved to
    // the inactive state.
    Cancelled,

    // Reader context does not map to a client.  Accessing memory in this context
    // is invalid (i.e. it is equivalent to a dangling pointer).
    Inactive,
  };

  ReaderContext(DiskIoMgr* parent, int num_disks);

  // Resets this object for a new reader
  void Reset(hdfsFS hdfs_connection, MemTracker* tracker);

  // Decrements the number of active disks for this reader.  If the disk count
  // goes to 0, the disk complete condition variable is signaled.
  // Reader lock must be taken before this call.
  void DecrementDiskRefCount() {
    // boost doesn't let us dcheck that the reader lock is taken
    DCHECK_GT(num_disks_with_ranges_, 0);
    if (--num_disks_with_ranges_ == 0) {
      disks_complete_cond_var_.notify_one();
    }
    DCHECK(Validate()) << std::endl << DebugString();
  }

  // Reader & Disk Scheduling: Readers that currently can't do work are not on
  // the disk's queue. These readers are ones that don't have any ranges in the
  // in_flight_queue AND have not prepared a range by setting next_range_to_start.
  // The rule to make sure readers are scheduled correctly is to ensure anytime a
  // range is put on the in_flight_queue or anytime next_range_to_start is set to
  // NULL, the reader is scheduled.

  // Adds range to in_flight_ranges, scheduling this reader on the disk threads
  // if necessary.
  // Reader lock must be taken before this.
  void ScheduleScanRange(DiskIoMgr::ScanRange* range) {
    DCHECK_EQ(state_, Active);
    DCHECK(range != NULL);
    ReaderContext::PerDiskState& state = disk_states_[range->disk_id()];
    state.in_flight_ranges()->Enqueue(range);
    state.ScheduleReader(this, range->disk_id());
  }

  // Cancels the reader with status code 'status'.
  void Cancel(const Status& status);

  // Returns the default queue capacity for scan ranges. This is updated
  // as the reader processes ranges.
  int initial_scan_range_queue_capacity();

  // Validates invariants of reader.  Reader lock must be taken beforehand.
  bool Validate() const;

  // Dumps out reader information.  Lock should be taken by caller
  std::string DebugString() const;

 private:
  friend class DiskIoMgr;
  // Parent object
  DiskIoMgr* parent_;

  // hdfsFS connection handle.  This is set once and never changed for the duration
  // of the reader.  NULL if this is a local reader.
  hdfsFS hdfs_connection_;

  // Memory used for this reader.  This is unowned by this object.
  MemTracker* mem_tracker_;

  // Total bytes read for this reader
  RuntimeProfile::Counter* bytes_read_counter_;

  // Total time spent in hdfs reading
  RuntimeProfile::Counter* read_timer_;

  // Number of active read threads
  RuntimeProfile::Counter* active_read_thread_counter_;

  // Disk access bitmap. The counter's bit[i] is set if disk id i has been accessed.
  // TODO: we can only support up to 64 disks with this bitmap but it lets us use a
  // builtin atomic instruction. Probably good enough for now.
  RuntimeProfile::Counter* disks_accessed_bitmap_;

  // Total number of bytes read locally, updated at end of each range scan
  AtomicInt<int64_t> bytes_read_local_;

  // Total number of bytes read via short circuit read, updated at end of each range scan
  AtomicInt<int64_t> bytes_read_short_circuit_;

  // The number of buffers that have been returned to the reader (via GetNext) that the
  // reader has not returned. Only included for debugging and diagnostics.
  AtomicInt<int> num_buffers_in_reader_;

  // The number of scan ranges that have been completed for this reader.  Only
  // used for diagnostics.
  AtomicInt<int> num_finished_ranges_;

  // The total number of ranges that have not been started. Only used for
  // diagnostics. This is the sum of all unstarted_ranges across all disks.
  AtomicInt<int> num_unstarted_ranges_;

  // The number of buffers that are being used for this reader. This is the sum
  // of all buffers in ScanRange queues and buffers currently being read into (i.e. about
  // to be queued).
  AtomicInt<int> num_used_buffers_;

  // The total number of ready buffers across all ranges.  Ready buffers are buffers
  // that have been read from disk but not retrieved by the caller.
  // This is the sum of all queued buffers in all ranges for this reader context.
  AtomicInt<int> num_ready_buffers_;

  // The total (sum) of queue capacities for finished scan ranges. This value
  // divided by num_finished_ranges_ is the average for finished ranges and
  // used to seed the starting queue capacity for future ranges. The assumption
  // is that if previous ranges were fast, new ones will be fast too. The scan
  // range adjusts the queue capacity dynamically so a rough approximation will do.
  AtomicInt<int> total_range_queue_capacity_;

  // All fields below are accessed by multiple threads and the lock needs to be
  // taken before accessing them.
  boost::mutex lock_;

  // Current state of the reader
  State state_;

  // Status of this reader.  Set to non-ok if cancelled.
  Status status_;

  // The number of disks with scan ranges remaining (always equal to the sum of
  // disks with ranges).
  int num_disks_with_ranges_;

  // A list of ranges that should be returned in subsequent calls to
  // GetNextRange.
  // There is a trade-off with when to populate this list.  Populating it on
  // demand means consumers need to wait (happens in DiskIoMgr::GetNextRange()).
  // Populating it preemptively means we make worse scheduling decisions.
  // We currently populate one range per disk.
  // TODO: think about this some more.
  InternalQueue<ScanRange> ready_to_start_ranges_;
  boost::condition_variable ready_to_start_ranges_cv_;  // used with lock_

  // Ranges that are blocked due to back pressure on outgoing buffers.
  InternalQueue<ScanRange> blocked_ranges_;

  // Condition variable for UnregisterReader to wait for all disks to complete
  boost::condition_variable disks_complete_cond_var_;

  // Struct containing state per disk. See comments in the disk read loop on how
  // they are used.
  class PerDiskState {
   public:
    bool done() const { return done_; }
    void set_done(bool b) { done_ = b; }

    int num_remaining_ranges() const { return num_remaining_ranges_; }
    int& num_remaining_ranges() { return num_remaining_ranges_; }

    ScanRange* next_range_to_start() { return next_range_to_start_; }
    void set_next_range_to_start(ScanRange* range) { next_range_to_start_ = range; }

    // We need to have a memory barrier to prevent this load from being reordered
    // with num_threads_in_read(), since these variables are set without the reader
    // lock taken
    bool is_on_queue() const {
      bool b = is_on_queue_;
      __sync_synchronize();
      return b;
    }

    int num_threads_in_read() const {
      int v = num_threads_in_read_;
      __sync_synchronize();
      return v;
    }

    const InternalQueue<ScanRange>* unstarted_ranges() const {
      return &unstarted_ranges_;
    }
    const InternalQueue<ScanRange>* in_flight_ranges() const {
      return &in_flight_ranges_;
    }

    InternalQueue<ScanRange>* unstarted_ranges() { return &unstarted_ranges_; }
    InternalQueue<ScanRange>* in_flight_ranges() { return &in_flight_ranges_; }

    PerDiskState() {
      Reset();
    }

    // Schedules reader on this disk if it's not already on the queue.
    // Reader lock must be taken before this.
    void ScheduleReader(ReaderContext* reader, int disk_id) {
      if (!is_on_queue_ && !done_) {
        is_on_queue_ = true;
        reader->parent_->disk_queues_[disk_id]->EnqueueReader(reader);
      }
    }

    // Increment the ref count on reader.  We need to track the number of threads per
    // reader per disk that are in the unlocked hdfs read code section. This is updated
    // by multiple threads without a lock so we need to use an atomic int.
    void IncrementReadThreadAndDequeue() {
      ++num_threads_in_read_;
      is_on_queue_ = false;
    }

    void DecrementReadThread() {
      --num_threads_in_read_;
    }

    // Decrement read thread count and do final cleanup if this is the last
    // read thread. Reader lock must be taken before this.
    void DecrementReadThreadAndCheckDone(ReaderContext* reader) {
      --num_threads_in_read_;
      // We don't need to worry about reordered loads here because updating
      // num_threads_in_read_ uses an atomic, which is a barrier.
      if (!is_on_queue_ && num_threads_in_read_ == 0 && !done_) {
        // This thread is the last one for this reader on this disk, do final
        // cleanup
        reader->DecrementDiskRefCount();
        done_ = true;
      }
    }

    void Reset() {
      DCHECK(in_flight_ranges_.empty());
      DCHECK(unstarted_ranges_.empty());
      done_ = true;
      num_remaining_ranges_ = 0;
      is_on_queue_ = false;
      num_threads_in_read_ = 0;
      next_range_to_start_ = NULL;
    }

   private:
    // If true, this disk is all done for this reader, including any cleanup.
    // If done is true, it means that this reader must not be on this disk's queue
    // *AND* there are no reading threads currently working on this reader. To satisfy
    // this, only the last thread (per disk) can set this to true.
    bool done_;

    // For each disk, keeps track if the reader is on this disk's queue, indicating
    // the disk must do some work for this reader. The disk needs to do work in 3
    // cases:
    //  1) in_flight_ranges is not empty, the disk needs to read for this reader.
    //  2) next_range_to_start is NULL, the disk needs to prepare a scan range to be
    //     read next.
    //  3) the reader has been cancelled and this disk needs to participate in the
    //     cleanup.
    // In general, we only want to put a reader on the disk queue if there is something
    // useful that can be done. If there's nothing useful, the disk queue will wake up
    // and then remove the reader from the queue. Doing this causes thrashing of the
    // threads.
    bool is_on_queue_;

    // For each disks, the number of scan ranges that have not been fully read.
    // In the non-cancellation path, this will hit 0, and done will be set to true
    // by the disk thread. This is undefined in the cancellation path (the various
    // threads notice by looking at the ReaderContext's state_).
    int num_remaining_ranges_;

    // Queue of ranges that have not started being read.  This list is exclusive
    // with in_flight_ranges.
    InternalQueue<ScanRange> unstarted_ranges_;

    // Queue of scan ranges for this context on this disk that are currently in progress.
    // A scan range that is currently being read by one thread cannot be picked up by
    // another thread and is temporarily removed from the queue.  The size of this
    // queue is always less than or equal to num_remaining_ranges.
    InternalQueue<ScanRange> in_flight_ranges_;

    // The next range to start for this reader on this disk. Each disk (for each reader)
    // picks the next range to start. The range is set here and also added to the
    // ready_to_start_ranges_ queue. The reader pulls from the queue in FIFO order,
    // so the ranges from different disks are round-robined. When the range is pulled
    // off the ready_to_start_ranges_ queue, it sets this variable to NULL, so the disk
    // knows to populate it again and add it to ready_to_start_ranges_ i.e. it is used
    // as a flag by DiskIoMgr::GetNextScanRange to determine if it needs to add another
    // range to ready_to_start_ranges_.
    ScanRange* next_range_to_start_;

    // For each disk, the number of threads issuing the underlying read on behalf of
    // this context. There are a few places where we release the reader lock, do some
    // work, and then grab the lock again.  Because we don't hold the lock for the
    // entire operation, we need this ref count to keep track of which thread should do
    // final resource cleanup during cancellation.
    // Only the thread that sees the count at 0 should do the final cleanup.
    AtomicInt<int> num_threads_in_read_;
  };

  // Per disk states to synchronize multiple disk threads accessing the same reader
  // context.
  std::vector<PerDiskState> disk_states_;
};

}

#endif

