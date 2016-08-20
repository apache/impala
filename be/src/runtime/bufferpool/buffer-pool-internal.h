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

// This file includes definitions of classes used internally in the buffer pool.
//
/// +========================+
/// | IMPLEMENTATION NOTES   |
/// +========================+
///
/// Lock Ordering
/// =============
/// The lock acquisition order is:
/// 1. Client::lock_
/// 2. BufferPool::clean_pages_lock_
/// 3. Page::lock
///
/// If a reference to a Page is acquired through a page list, the Page* reference only
/// remains valid so long as list's lock is held.
///
/// Page States
/// ===========
/// Each Page object is owned by at most one InternalList<Page> at any given point.
/// Each page is either pinned or unpinned. Unpinned has a number of sub-states, which
/// is determined by which list in Client/BufferPool contains the page.
/// * Pinned: Always in this state when 'pin_count' > 0. The page is in
///     Client::pinned_pages_.
/// * Unpinned - Dirty: When no write has been started for an unpinned page. The page is
///     in Client::dirty_unpinned_pages_.
/// * Unpinned - Write in flight: When the write has been started but not completed for
///     a dirty unpinned page. The page is in Client::write_in_flight_pages_. For
///     accounting purposes this is considered a dirty page.
/// * Unpinned - Clean: When the write has completed but the page was not evicted. The
///     page is in BufferPool::clean_pages_.
/// * Unpinned - Evicted: After a clean page's buffer has been reclaimed. The page is
///     not in any list.
///
/// Page Eviction Policy
/// ====================
/// The page eviction policy is designed so that clients that run only in-memory (i.e.
/// don't unpin pages) never block on I/O. To achieve this, we must be able to
/// fulfil reservations by either allocating buffers or evicting clean pages. Assuming
/// reservations are not overcommitted (they shouldn't be), this global invariant can be
/// maintained by enforcing a local invariant for every client:
///
///   unused reservation >= dirty unpinned pages
///
/// The local invariant is maintained by writing pages to disk as the first step of any
/// operation that uses reservation. I.e. the R.H.S. of the invariant must be decreased
/// before the L.H.S. can be decreased. These operations block waiting for enough writes
/// to complete to satisfy the invariant.
/// TODO: this invariant can be broken if a client calls DecreaseReservation() on the
/// ReservationTracker. We should refactor so that DecreaseReservation() goes through
/// the client before closing IMPALA-3202.

#ifndef IMPALA_RUNTIME_BUFFER_POOL_INTERNAL_H
#define IMPALA_RUNTIME_BUFFER_POOL_INTERNAL_H

#include <memory>
#include <sstream>

#include <boost/thread/mutex.hpp>

#include "runtime/bufferpool/buffer-pool-counters.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "util/condition-variable.h"

namespace impala {

/// The internal state for the client.
class BufferPool::Client {
 public:
  Client(BufferPool* pool, TmpFileMgr::FileGroup* file_group, const string& name,
      RuntimeProfile* profile);

  ~Client() {
    DCHECK_EQ(0, num_pages_);
    DCHECK_EQ(0, pinned_pages_.size());
    DCHECK_EQ(0, dirty_unpinned_pages_.size());
    DCHECK_EQ(0, in_flight_write_pages_.size());
  }

  /// Add a new pinned page 'page' to the pinned pages list. 'page' must not be in any
  /// other lists. Neither the client's lock nor page->buffer_lock should be held by the
  /// caller.
  void AddNewPinnedPage(Page* page);

  /// Reset 'handle', clean up references to handle->page and release any resources
  /// associated with handle->page. If the page is pinned, 'out_buffer' can be passed in
  /// and the page's buffer will be returned.
  /// Neither the client's lock nor handle->page_->buffer_lock should be held by the
  /// caller.
  void DestroyPageInternal(PageHandle* handle, BufferHandle* out_buffer = NULL);

  /// Updates client state to reflect that 'page' is now a dirty unpinned page. May
  /// initiate writes for this or other dirty unpinned pages.
  /// Neither the client's lock nor page->buffer_lock should be held by the caller.
  void MoveToDirtyUnpinned(int64_t unused_reservation, Page* page);

  /// Move an unpinned page to the pinned state, moving between data structures and
  /// reading from disk if necessary. Returns once the page's buffer is allocated
  /// and contains the page's data. Neither the client's lock nor
  /// handle->page_->buffer_lock should be held by the caller.
  Status MoveToPinned(ClientHandle* client, PageHandle* handle);

  /// Must be called before allocating a buffer to ensure that the client can allocate
  /// 'allocation_len' bytes without pinned bytes plus dirty unpinned bytes exceeding the
  /// client's reservation. No page or client locks should be held by the caller.
  Status CleanPagesBeforeAllocation(
      ReservationTracker* reservation, int64_t allocation_len);

  /// Same as CleanPagesBeforeAllocation(), except 'lock_' must be held by 'client_lock'.
  /// 'client_lock' may be released temporarily while waiting for writes to complete.
  Status CleanPagesBeforeAllocationLocked(boost::unique_lock<boost::mutex>* client_lock,
      ReservationTracker* reservation, int64_t allocation_len);

  /// Initiates asynchronous writes of dirty unpinned pages to disk. Ensures that at
  /// least 'min_bytes_to_write' bytes of writes will be written asynchronously. May
  /// start writes more aggressively so that I/O and compute can be overlapped. If
  /// any errors are encountered, 'write_status_' is set. 'write_status_' must therefore
  /// be checked before reading back any pages. 'lock_' must be held by the caller.
  void WriteDirtyPagesAsync(int64_t min_bytes_to_write = 0);

  /// Wait for the in-flight write for 'page' to complete.
  /// 'lock_' must be held by the caller via 'client_lock'. page->bufffer_lock should
  /// not be held.
  void WaitForWrite(boost::unique_lock<boost::mutex>* client_lock, Page* page);

  /// Asserts that 'client_lock' is holding 'lock_'.
  void DCheckHoldsLock(const boost::unique_lock<boost::mutex>& client_lock) {
    DCHECK(client_lock.mutex() == &lock_ && client_lock.owns_lock());
  }

  const BufferPoolClientCounters& counters() const { return counters_; }
  bool spilling_enabled() const { return file_group_ != NULL; }

  std::string DebugString();

 private:
  // Check consistency of client, DCHECK if inconsistent. 'lock_' must be held.
  void DCheckConsistency() {
    DCHECK_GE(in_flight_write_bytes_, 0);
    DCHECK_LE(in_flight_write_bytes_, dirty_unpinned_bytes_);
    DCHECK_LE(pinned_pages_.size() + dirty_unpinned_pages_.size()
            + in_flight_write_pages_.size(),
        num_pages_);
    if (in_flight_write_pages_.empty()) DCHECK_EQ(0, in_flight_write_bytes_);
    if (in_flight_write_pages_.empty() && dirty_unpinned_pages_.empty()) {
      DCHECK_EQ(0, dirty_unpinned_bytes_);
    }
  }

  /// Called when a write for 'page' completes.
  void WriteCompleteCallback(Page* page, const Status& write_status);

  /// Move an evicted page to the pinned state by allocating a new buffer, reading data
  /// from disk and moving the page to 'pinned_pages_'. client->impl must be locked by
  /// the caller via 'client_lock' and handle->page must be unlocked. 'client_lock' is
  /// released then reacquired.
  Status MoveEvictedToPinned(boost::unique_lock<boost::mutex>* client_lock,
      ClientHandle* client, PageHandle* handle);

  /// The buffer pool that owns the client.
  BufferPool* const pool_;

  /// The file group that should be used for allocating scratch space. If NULL, spilling
  /// is disabled.
  TmpFileMgr::FileGroup* const file_group_;

  /// A name identifying the client.
  const std::string name_;

  /// The RuntimeProfile counters for this client, owned by the client's RuntimeProfile.
  /// All non-NULL.
  BufferPoolClientCounters counters_;

  /// Lock to protect the below member variables;
  boost::mutex lock_;

  /// Condition variable signalled when a write for this client completes.
  ConditionVariable write_complete_cv_;

  /// All non-OK statuses returned by write operations are merged into this status.
  /// All operations that depend on pages being written to disk successfully (e.g.
  /// reading pages back from disk) must check 'write_status_' before proceeding, so
  /// that write errors that occurred asynchronously are correctly propagated. The
  /// write error is global to the client so can be propagated to any Status-returning
  /// operation for the client (even for operations on different Pages or Buffers).
  /// Write errors are not recoverable so it is best to propagate them as quickly
  /// as possible, instead of waiting to propagate them in a specific way.
  Status write_status_;

  /// Total number of pages for this client. Used for debugging and enforcing that all
  /// pages are destroyed before the client.
  int64_t num_pages_;

  /// All pinned pages for this client. Only used for debugging.
  InternalList<Page> pinned_pages_;

  /// Dirty unpinned pages for this client for which writes are not in flight. Page
  /// writes are started in LIFO order, because operators typically have sequential access
  /// patterns where the most recently evicted page will be last to be read.
  InternalList<Page> dirty_unpinned_pages_;

  /// Dirty unpinned pages for this client for which writes are in flight.
  InternalList<Page> in_flight_write_pages_;

  /// Total bytes of dirty unpinned pages for this client.
  int64_t dirty_unpinned_bytes_;

  /// Total bytes of in-flight writes for dirty unpinned pages. Bytes accounted here
  /// are also accounted in 'dirty_unpinned_bytes_'.
  int64_t in_flight_write_bytes_;
};

/// The internal representation of a page, which can be pinned or unpinned. See the
/// class comment for explanation of the different page states.
///
/// Code manipulating the page is responsible for acquiring 'lock' when reading or
/// modifying the page.
struct BufferPool::Page : public InternalList<Page>::Node {
  Page(Client* client, int64_t len) : client(client), len(len), pin_count(0) {}

  std::string DebugString();

  // Helper for BufferPool::DebugString().
  static bool DebugStringCallback(std::stringstream* ss, BufferPool::Page* page);

  /// The client that the page belongs to.
  Client* const client;

  /// The length of the page in bytes.
  const int64_t len;

  /// The pin count of the page. Only accessed in contexts that are passed the associated
  /// PageHandle, so it cannot be accessed by multiple threads concurrently.
  int pin_count;

  /// Non-null if there is a write in flight, the page is clean, or the page is evicted.
  std::unique_ptr<TmpFileMgr::WriteHandle> write_handle;

  /// Condition variable signalled when a write for this page completes. Protected by
  /// client->lock_.
  ConditionVariable write_complete_cv_;

  /// This lock must be held when accessing 'buffer' if the page is unpinned and not
  /// evicted (i.e. it is safe to access 'buffer' if the page is pinned or evicted).
  SpinLock buffer_lock;

  /// Buffer with the page's contents. Closed only iff page is evicted. Open otherwise.
  BufferHandle buffer;
};
}

#endif
