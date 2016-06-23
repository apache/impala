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

#ifndef IMPALA_BUFFER_POOL_H
#define IMPALA_BUFFER_POOL_H

#include <boost/thread/locks.hpp>
#include <string>
#include <stdint.h>

#include "bufferpool/buffer-allocator.h"
#include "common/atomic.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "util/internal-queue.h"
#include "util/spinlock.h"

namespace impala {

class ReservationTracker;

/// A buffer pool that manages memory buffers for all queries in an Impala daemon.
/// The buffer pool enforces buffer reservations, limits, and implements policies
/// for moving spilled memory from in-memory buffers to disk. It also enables reuse of
/// buffers between queries, to avoid frequent allocations.
///
/// The buffer pool can be used for allocating any large buffers (above a configurable
/// minimum length), whether or not the buffers will be spilled. Smaller allocations
/// are not serviced directly by the buffer pool: clients of the buffer pool must
/// subdivide buffers if they wish to use smaller allocations.
///
/// All buffer pool operations are in the context of a registered buffer pool client.
/// A buffer pool client should be created for every allocator of buffers at the level
/// of granularity required for reporting and enforcement of reservations, e.g. an exec
/// node. The client tracks buffer reservations via its ReservationTracker and also
/// includes info that is helpful for debugging (e.g. the operator that is associated
/// with the buffer). The client is not threadsafe, i.e. concurrent buffer pool
/// operations should not be invoked for the same client.
///
/// TODO:
/// * Implement spill-to-disk.
/// * Decide on, document, and enforce upper limits on page size.
///
/// Pages, Buffers and Pinning
/// ==========================
/// * A page is a logical block of memory that can reside in memory or on disk.
/// * A buffer is a physical block of memory that can hold a page in memory.
/// * A page handle is used by buffer pool clients to identify and access a page and
///   the corresponding buffer. Clients do not interact with pages directly.
/// * A buffer handle is used by buffer pool clients to identify and access a buffer.
/// * A page is pinned if it has pin count > 0. A pinned page stays mapped to the same
///   buffer.
/// * An unpinned page can be written out to disk by the buffer pool so that the buffer
///   can be used for another purpose.
///
/// Buffer/Page Sizes
/// =================
/// The buffer pool has a minimum buffer size, which must be a power-of-two. Page and
/// buffer sizes must be an exact multiple of the minimum buffer size.
///
/// Reservations
/// ============
/// Before allocating buffers or pinning pages, a client must reserve memory through its
/// ReservationTracker. Reservation of n bytes give a client the right to allocate
/// buffers or pin pages summing up to n bytes. Reservations are both necessary and
/// sufficient for a client to allocate buffers or pin pages: the operations succeed
/// unless a "system error" such as a disk write error is encountered that prevents
/// unpinned pages from being  to disk.
///
/// More memory may be reserved than is used, e.g. if a client is not using its full
/// reservation. In such cases, the buffer pool can use the free buffers in any way,
/// e.g. for keeping unpinned pages in memory, so long as it is able to fulfill the
/// reservations when needed, e.g. by flushing unpinned pages to disk.
///
/// Page/Buffer Handles
/// ===================
/// The buffer pool exposes PageHandles and BufferHandles, which are owned by clients of
/// the buffer pool, and act as a proxy for the internal data structure representing the
/// page or buffer in the buffer pool. Handles are "open" if they are associated with a
/// page or buffer. An open PageHandle is obtained by creating a page. PageHandles are
/// closed by calling BufferPool::DestroyPage(). An open BufferHandle is obtained by
/// allocating a buffer or extracting a BufferHandle from a PageHandle. A page's buffer
/// can also be accessed through the PageHandle. The handle destructors check for
/// resource leaks, e.g. an open handle that would result in a buffer leak.
///
/// Pin Counting of Page Handles:
/// ----------------------------------
/// Page handles are scoped to a client. The invariants are as follows:
/// * A page can only be accessed through an open handle.
/// * A page is destroyed once the handle is destroyed via DestroyPage().
/// * A page's buffer can only be accessed through a pinned handle.
/// * Pin() can be called on an open handle, incrementing the handle's pin count.
/// * Unpin() can be called on a pinned handle, but not an unpinned handle.
/// * Pin() always increases usage of reservations, and Unpin() always decreases usage,
///   i.e. the handle consumes <pin count> * <page size> bytes of reservation.
///
/// Example Usage: Buffers
/// ==================================
/// The simplest use case is to allocate a memory buffer.
/// * The new buffer is created with AllocateBuffer().
/// * The client reads and writes to the buffer as it sees fit.
/// * If the client is done with the buffer's contents it can call FreeBuffer() to
///   destroy the handle and free the buffer, or use TransferBuffer() to transfer
///   the buffer to a different client.
///
/// Example Usage: Spillable Pages
/// ==============================
/// * A spilling operator creates a new page with CreatePage().
/// * The client reads and writes to the page's buffer as it sees fit.
/// * If the operator encounters memory pressure, it can decrease reservation usage by
///   calling Unpin() on the page. The page may then be written to disk and its buffer
///   repurposed internally by BufferPool.
/// * Once the operator needs the page's contents again and has sufficient unused
///   reservations, it can call Pin(), which brings the page's contents back into memory,
///   perhaps in a different buffer. Therefore the operator must fix up any pointers into
///   the previous buffer.
/// * If the operator is done with the page, it can call FreeBuffer() to destroy the
///   handle and release resources, or call ExtractBuffer() to extract the buffer.
///
/// Synchronization
/// ===============
/// The data structures in the buffer pool itself are thread-safe. Client-owned data
/// structures - Client, PageHandle and BufferHandle - are not protected from concurrent
/// access by the buffer pool: clients must ensure that they do not invoke concurrent
/// operations with the same Client, PageHandle or BufferHandle.
//
/// +========================+
/// | IMPLEMENTATION DETAILS |
/// +========================+
/// ... TODO ...
class BufferPool {
 public:
  class Client;
  class BufferHandle;
  class PageHandle;

  /// Constructs a new buffer pool.
  /// 'min_buffer_len': the minimum buffer length for the pool. Must be a power of two.
  /// 'buffer_bytes_limit': the maximum physical memory in bytes that can be used by the
  ///     buffer pool. If 'buffer_bytes_limit' is not a multiple of 'min_buffer_len', the
  ///     remainder will not be usable.
  BufferPool(int64_t min_buffer_len, int64_t buffer_bytes_limit);
  ~BufferPool();

  /// Register a client. Returns an error status and does not register the client if the
  /// arguments are invalid. 'name' is an arbitrary used to identify the client in any
  /// errors messages or logging. 'client' is the client to register. 'client' should not
  /// already be registered.
  Status RegisterClient(const std::string& name, ReservationTracker* reservations,
      Client* client);

  /// Deregister 'client' if it is registered. Idempotent.
  void DeregisterClient(Client* client);

  /// Create a new page of 'len' bytes with pin count 1. 'len' must be a page length
  /// supported by BufferPool (see BufferPool class comment). The client must have
  /// sufficient unused reservations to pin the new page (otherwise it will DCHECK).
  /// CreatePage() only fails when a system error prevents the buffer pool from fulfilling
  /// the reservation.
  /// On success, the handle is mapped to the new page.
  Status CreatePage(Client* client, int64_t len, PageHandle* handle);

  /// Increment the pin count of 'handle'. After Pin() the underlying page will
  /// be mapped to a buffer, which will be accessible through 'handle'. Uses
  /// reservation from 'client'. The caller is responsible for ensuring it has enough
  /// unused reservation before calling Pin() (otherwise it will DCHECK). Pin() only
  /// fails when a system error prevents the buffer pool from fulfilling the reservation.
  /// 'handle' must be open.
  Status Pin(Client* client, PageHandle* handle);

  /// Decrement the pin count of 'handle'. Decrease client's reservation usage. If the
  /// handle's pin count becomes zero, it is no longer valid for the underlying page's
  /// buffer to be accessed via 'handle'. If the page's total pin count across all
  /// handles that reference it goes to zero, the page's data may be written to disk and
  /// the buffer reclaimed. 'handle' must be open and have a pin count > 0.
  /// TODO: once we implement spilling, it will be an error to call Unpin() with
  /// spilling disabled. E.g. if Impala is running without scratch (we want to be
  /// able to test Unpin() before we implement actual spilling).
  void Unpin(Client* client, PageHandle* handle);

  /// Destroy the page referenced by 'handle' (if 'handle' is open). Any buffers or disk
  /// storage backing the page are freed. Idempotent. If the page is pinned, the
  /// reservation usage is decreased accordingly.
  void DestroyPage(Client* client, PageHandle* handle);

  /// Extracts buffer from a pinned page. After this returns, the page referenced by
  /// 'page_handle' will be destroyed and 'buffer_handle' will reference the buffer from
  /// 'page_handle'. This may decrease reservation usage if the page was pinned multiple
  /// times via 'page_handle'.
  void ExtractBuffer(PageHandle* page_handle, BufferHandle* buffer_handle);

  /// Allocates a new buffer of 'len' bytes. Uses reservation from 'client'. The caller
  /// is responsible for ensuring it has enough unused reservation before calling
  /// AllocateBuffer() (otherwise it will DCHECK). AllocateBuffer() only fails when
  /// a system error prevents the buffer pool from fulfilling the reservation.
  Status AllocateBuffer(Client* client, int64_t len, BufferHandle* handle);

  /// If 'handle' is open, close 'handle', free the buffer and and decrease the
  /// reservation usage from 'client'. Idempotent.
  void FreeBuffer(Client* client, BufferHandle* handle);

  /// Transfer ownership of buffer from 'src_client' to 'dst_client' and move the
  /// handle from 'src' to 'dst'. Increases reservation usage in 'dst_client' and
  /// decreases reservation usage in 'src_client'. 'src' must be open and 'dst' must
  /// be closed
  /// before calling. After a successful call, 'src' is closed and 'dst' is open.
  Status TransferBuffer(Client* src_client, BufferHandle* src, Client* dst_client,
      BufferHandle* dst);

  /// Print a debug string with the state of the buffer pool.
  std::string DebugString();

  int64_t min_buffer_len() const;
  int64_t buffer_bytes_limit() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(BufferPool);
};

/// External representation of a client of the BufferPool. Clients are used for
/// reservation accounting, and will be used in the future for tracking per-client
/// buffer pool counters. This class is the external handle for a client so
/// each Client instance is owned by the BufferPool's client, rather than the BufferPool.
/// Each Client should only be used by a single thread at a time: concurrently calling
/// Client methods or BufferPool methods with the Client as an argument is not supported.
class BufferPool::Client {
 public:
  Client() {}
  /// Client must be deregistered.
  ~Client() { DCHECK(!is_registered()); }

  bool is_registered() const;
  ReservationTracker* reservations();

  std::string DebugString() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(Client);
};


/// The handle for a page used by clients of the BufferPool. Each PageHandle should
/// only be used by a single thread at a time: concurrently calling PageHandle methods
/// or BufferPool methods with the PageHandle as an argument is not supported.
class BufferPool::PageHandle {
 public:
  PageHandle();
  ~PageHandle() { DCHECK(!is_open()); }

  // Allow move construction of page handles, to support std::move().
  PageHandle(PageHandle&& src);

  // Allow move assignment of page handles, to support STL classes like std::vector.
  // Destination must be closed.
  PageHandle& operator=(PageHandle&& src);

  bool is_open() const;
  bool is_pinned() const;
  int64_t len() const;
  /// Get a pointer to the start of the page's buffer. Only valid to call if the page
  /// is pinned via this handle.
  uint8_t* data() const;

  /// Return a pointer to the page's buffer handle. Only valid to call if the page is
  /// pinned via this handle. Only const accessors of the returned handle can be used:
  /// it is invalid to call FreeBuffer() or TransferBuffer() on it or to otherwise modify
  /// the handle.
  const BufferHandle* buffer_handle() const;

  std::string DebugString() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(PageHandle);
};

/// A handle to a buffer allocated from the buffer pool. Each BufferHandle should only
/// be used by a single thread at a time: concurrently calling BufferHandle methods or
/// BufferPool methods with the BufferHandle as an argument is not supported.
class BufferPool::BufferHandle {
 public:
  BufferHandle();
  ~BufferHandle() { DCHECK(!is_open()); }

  /// Allow move construction of handles, to support std::move().
  BufferHandle(BufferHandle&& src);

  /// Allow move assignment of handles, to support STL classes like std::vector.
  /// Destination must be uninitialized.
  BufferHandle& operator=(BufferHandle&& src);

  bool is_open() const;
  int64_t len() const;
  /// Get a pointer to the start of the buffer.
  uint8_t* data() const;

  std::string DebugString() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(BufferHandle);
};

}

#endif
