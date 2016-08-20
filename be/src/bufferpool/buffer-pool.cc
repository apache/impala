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

#include "bufferpool/buffer-pool.h"

#include <boost/bind.hpp>
#include <limits>
#include <sstream>

#include "bufferpool/reservation-tracker.h"
#include "common/names.h"
#include "gutil/strings/substitute.h"
#include "util/bit-util.h"
#include "util/uid-util.h"

using strings::Substitute;

namespace impala {

/// The internal representation of a page, which can be pinned or unpinned. If the
/// page is pinned, a buffer is associated with the page.
///
/// Code manipulating the page is responsible for acquiring 'lock' when reading or
/// modifying the page.
struct BufferPool::Page : public BufferPool::PageList::Node {
  Page(int64_t len) : len(len), pin_count(0), dirty(false) {}

  /// Increment the pin count. Caller must hold 'lock'.
  void IncrementPinCount(PageHandle* handle) {
    lock.DCheckLocked();
    ++pin_count;
    // Pinned page buffers may be modified by anyone with a pointer to the buffer, so we
    // have to assume they are dirty.
    dirty = true;
  }

  /// Decrement the pin count. Caller must hold 'lock'.
  void DecrementPinCount(PageHandle* handle) {
    lock.DCheckLocked();
    DCHECK(pin_count > 0);
    --pin_count;
  }

  string DebugString() {
    return Substitute("<BufferPool::Page> $0 len: $1 pin_count: $2 buf: $3 dirty: $4", this,
        len, pin_count, buffer.DebugString(), dirty);
  }

  // Helper for BufferPool::DebugString().
  static bool DebugStringCallback(stringstream* ss, BufferPool::Page* page) {
    lock_guard<SpinLock> pl(page->lock);
    (*ss) << page->DebugString() << "\n";
    return true;
  }

  /// The length of the page in bytes.
  const int64_t len;

  /// Lock to protect the below members of Page. The lock must be held when modifying any
  /// of the below members and when reading any of the below members of an unpinned page.
  SpinLock lock;

  /// The pin count of the page.
  int pin_count;

  /// Buffer with the page's contents, Always open if pinned. Closed if page is unpinned
  /// and was evicted from memory.
  BufferHandle buffer;

  /// True if the buffer's contents need to be saved before evicting it from memory.
  bool dirty;
};

BufferPool::BufferHandle::BufferHandle() {
  Reset();
}

BufferPool::BufferHandle::BufferHandle(BufferHandle&& src) {
  *this = std::move(src);
}

BufferPool::BufferHandle& BufferPool::BufferHandle::operator=(BufferHandle&& src) {
  DCHECK(!is_open());
  // Copy over all members then close src.
  client_ = src.client_;
  data_ = src.data_;
  len_ = src.len_;
  src.Reset();
  return *this;
}

void BufferPool::BufferHandle::Open(const Client* client, uint8_t* data, int64_t len) {
  client_ = client;
  data_ = data;
  len_ = len;
}

void BufferPool::BufferHandle::Reset() {
  client_ = NULL;
  data_ = NULL;
  len_ = -1;
}

BufferPool::PageHandle::PageHandle() {
  Reset();
}

BufferPool::PageHandle::PageHandle(PageHandle&& src) {
  *this = std::move(src);
}

BufferPool::PageHandle& BufferPool::PageHandle::operator=(PageHandle&& src) {
  DCHECK(!is_open());
  // Copy over all members then close src.
  page_ = src.page_;
  client_ = src.client_;
  src.Reset();
  return *this;
}

void BufferPool::PageHandle::Open(Page* page, Client* client) {
  DCHECK(!is_open());
  page->lock.DCheckLocked();
  page_ = page;
  client_ = client;
}

void BufferPool::PageHandle::Reset() {
  page_ = NULL;
  client_ = NULL;
}

int BufferPool::PageHandle::pin_count() const {
  DCHECK(is_open());
  // The pin count can only be modified via this PageHandle, which must not be
  // concurrently accessed by multiple threads, so it is safe to access without locking
  return page_->pin_count;
}

int64_t BufferPool::PageHandle::len() const {
  DCHECK(is_open());
  // The length of the page cannot change, so it is safe to access without locking.
  return page_->len;
}

const BufferPool::BufferHandle* BufferPool::PageHandle::buffer_handle() const {
  DCHECK(is_pinned());
  // The 'buffer' field cannot change while the page is pinned, so it is safe to access
  // without locking.
  return &page_->buffer;
}

BufferPool::BufferPool(int64_t min_buffer_len, int64_t buffer_bytes_limit)
  : allocator_(new BufferAllocator(min_buffer_len)),
    min_buffer_len_(min_buffer_len),
    buffer_bytes_limit_(buffer_bytes_limit),
    buffer_bytes_remaining_(buffer_bytes_limit) {
  DCHECK_GT(min_buffer_len, 0);
  DCHECK_EQ(min_buffer_len, BitUtil::RoundUpToPowerOfTwo(min_buffer_len));
}

BufferPool::~BufferPool() {
  DCHECK(pages_.empty());
}

Status BufferPool::RegisterClient(
    const string& name, ReservationTracker* reservation, Client* client) {
  DCHECK(!client->is_registered());
  DCHECK(reservation != NULL);
  client->reservation_ = reservation;
  client->name_ = name;
  return Status::OK();
}

void BufferPool::DeregisterClient(Client* client) {
  if (!client->is_registered()) return;
  client->reservation_->Close();
  client->name_.clear();
  client->reservation_ = NULL;
}

Status BufferPool::CreatePage(Client* client, int64_t len, PageHandle* handle) {
  DCHECK(!handle->is_open());
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_EQ(len, BitUtil::RoundUpToPowerOfTwo(len));

  BufferHandle buffer;
  // No changes have been made to state yet, so we can cleanly return on error.
  RETURN_IF_ERROR(AllocateBufferInternal(client, len, &buffer));

  Page* page = new Page(len);
  {
    lock_guard<SpinLock> pl(page->lock);
    page->buffer = std::move(buffer);
    handle->Open(page, client);
    page->IncrementPinCount(handle);
  }

  // Only add to globally-visible list after page is initialized. The page lock also
  // needs to be released before enqueueing to respect the lock ordering.
  pages_.Enqueue(page);

  client->reservation_->AllocateFrom(len);
  return Status::OK();
}

void BufferPool::DestroyPage(Client* client, PageHandle* handle) {
  if (!handle->is_open()) return; // DestroyPage() should be idempotent.

  Page* page = handle->page_;
  if (handle->is_pinned()) {
    // In the pinned case, delegate to ExtractBuffer() and FreeBuffer() to do the work
    // of cleaning up the page and freeing the buffer.
    BufferHandle buffer;
    ExtractBuffer(client, handle, &buffer);
    FreeBuffer(client, &buffer);
    return;
  }

  {
    lock_guard<SpinLock> pl(page->lock); // Lock page while we work on its state.
    // In the unpinned case, no reservation is consumed, so just free the buffer.
    // TODO: wait for in-flight writes for 'page' so we can safely free 'page'.
    if (page->buffer.is_open()) FreeBufferInternal(&page->buffer);
  }
  CleanUpPage(handle);
}

void BufferPool::CleanUpPage(PageHandle* handle) {
  // Remove the destroyed page from data structures in a way that ensures no other
  // threads have a remaining reference. Threads that access pages via the 'pages_'
  // list hold 'pages_.lock_', so Remove() will not return until those threads are done
  // and it is safe to delete page.
  pages_.Remove(handle->page_);
  delete handle->page_;
  handle->Reset();
}

Status BufferPool::Pin(Client* client, PageHandle* handle) {
  DCHECK(client->is_registered());
  DCHECK(handle->is_open());
  DCHECK_EQ(handle->client_, client);

  Page* page = handle->page_;
  {
    lock_guard<SpinLock> pl(page->lock); // Lock page while we work on its state.
    if (!page->buffer.is_open()) {
      // No changes have been made to state yet, so we can cleanly return on error.
      RETURN_IF_ERROR(AllocateBufferInternal(client, page->len, &page->buffer));
    }
    page->IncrementPinCount(handle);

    // TODO: will need to initiate/wait for read if the page is not in-memory.
  }

  client->reservation_->AllocateFrom(page->len);
  return Status::OK();
}

void BufferPool::Unpin(Client* client, PageHandle* handle) {
  DCHECK(handle->is_open());
  lock_guard<SpinLock> pl(handle->page_->lock);
  UnpinLocked(client, handle);
}

void BufferPool::UnpinLocked(Client* client, PageHandle* handle) {
  DCHECK(client->is_registered());
  DCHECK_EQ(handle->client_, client);
  // If handle is pinned, we can assume that the page itself is pinned.
  DCHECK(handle->is_pinned());
  Page* page = handle->page_;
  page->lock.DCheckLocked();

  page->DecrementPinCount(handle);
  client->reservation_->ReleaseTo(page->len);

  // TODO: can evict now. Only need to preserve contents if 'page->dirty' is true.
}

void BufferPool::ExtractBuffer(
    Client* client, PageHandle* page_handle, BufferHandle* buffer_handle) {
  DCHECK(page_handle->is_pinned());
  DCHECK_EQ(page_handle->client_, client);

  Page* page = page_handle->page_;
  {
    lock_guard<SpinLock> pl(page->lock); // Lock page while we work on its state.
    // TODO: wait for in-flight writes for 'page' so we can safely free 'page'.

    // Bring the pin count to 1 so that we're not using surplus reservations.
    while (page->pin_count > 1) UnpinLocked(client, page_handle);
    *buffer_handle = std::move(page->buffer);
  }
  CleanUpPage(page_handle);
}

Status BufferPool::AllocateBuffer(Client* client, int64_t len, BufferHandle* handle) {
  client->reservation_->AllocateFrom(len);
  return AllocateBufferInternal(client, len, handle);
}

Status BufferPool::AllocateBufferInternal(
    Client* client, int64_t len, BufferHandle* buffer) {
  DCHECK(!buffer->is_open());
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_EQ(len, BitUtil::RoundUpToPowerOfTwo(len));

  // If there is headroom in 'buffer_bytes_remaining_', we can just allocate a new buffer.
  if (TryDecreaseBufferBytesRemaining(len)) {
    uint8_t* data;
    Status status = allocator_->Allocate(len, &data);
    if (!status.ok()) {
      buffer_bytes_remaining_.Add(len);
      return status;
    }
    DCHECK(data != NULL);
    buffer->Open(client, data, len);
    return Status::OK();
  }

  // If there is no remaining capacity, we must evict another page.
  return Status(TErrorCode::NOT_IMPLEMENTED_ERROR,
      Substitute("Buffer bytes limit $0 of buffer pool is exhausted and page eviction is "
                 "not implemented yet!", buffer_bytes_limit_));
}

void BufferPool::FreeBuffer(Client* client, BufferHandle* handle) {
  if (!handle->is_open()) return; // Should be idempotent.
  DCHECK_EQ(client, handle->client_);
  client->reservation_->ReleaseTo(handle->len_);
  FreeBufferInternal(handle);
}

void BufferPool::FreeBufferInternal(BufferHandle* handle) {
  DCHECK(handle->is_open());
  allocator_->Free(handle->data(), handle->len());
  buffer_bytes_remaining_.Add(handle->len());
  handle->Reset();
}

Status BufferPool::TransferBuffer(
    Client* src_client, BufferHandle* src, Client* dst_client, BufferHandle* dst) {
  DCHECK(src->is_open());
  DCHECK(!dst->is_open());
  DCHECK_EQ(src_client, src->client_);
  DCHECK_NE(src, dst);
  DCHECK_NE(src_client, dst_client);

  dst_client->reservation_->AllocateFrom(src->len());
  src_client->reservation_->ReleaseTo(src->len());
  *dst = std::move(*src);
  dst->client_ = dst_client;
  return Status::OK();
}

bool BufferPool::TryDecreaseBufferBytesRemaining(int64_t len) {
  // TODO: we may want to change this policy so that we don't always use up to the limit
  // for buffers, since this may starve other operators using non-buffer-pool memory.
  while (true) {
    int64_t old_value = buffer_bytes_remaining_.Load();
    if (old_value < len) return false;
    int64_t new_value = old_value - len;
    if (buffer_bytes_remaining_.CompareAndSwap(old_value, new_value)) {
      return true;
    }
  }
}

string BufferPool::Client::DebugString() const {
  if (is_registered()) {
    return Substitute("<BufferPool::Client> $0 name: $1 reservation: {$2}", this, name_,
        reservation_->DebugString());
  } else {
    return Substitute("<BufferPool::Client> $0 UNREGISTERED", this);
  }
}

string BufferPool::PageHandle::DebugString() const {
  if (is_open()) {
    lock_guard<SpinLock> pl(page_->lock);
    return Substitute(
        "<BufferPool::PageHandle> $0 client: {$1} page: {$2}",
        this, client_->DebugString(), page_->DebugString());
  } else {
    return Substitute("<BufferPool::PageHandle> $0 CLOSED", this);
  }
}

string BufferPool::BufferHandle::DebugString() const {
  if (is_open()) {
    return Substitute("<BufferPool::BufferHandle> $0 client: {$1} data: $2 len: $3", this,
        client_->DebugString(), data_, len_);
  } else {
    return Substitute("<BufferPool::BufferHandle> $0 CLOSED", this);
  }
}

string BufferPool::DebugString() {
  stringstream ss;
  ss << "<BufferPool> " << this << " min_buffer_len: " << min_buffer_len_
     << " buffer_bytes_limit: " << buffer_bytes_limit_
     << " buffer_bytes_remaining: " << buffer_bytes_remaining_.Load() << "\n";
  pages_.Iterate(bind<bool>(Page::DebugStringCallback, &ss, _1));
  return ss.str();
}
}
