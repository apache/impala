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

#include "runtime/bufferpool/buffer-pool-internal.h"

#include <limits>
#include <sstream>
#include <boost/bind.hpp>

#include "common/names.h"
#include "gutil/strings/substitute.h"
#include "runtime/bufferpool/buffer-allocator.h"
#include "util/bit-util.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

DEFINE_int32(concurrent_scratch_ios_per_device, 2,
    "Set this to influence the number of concurrent write I/Os issues to write data to "
    "scratch files. This is multiplied by the number of active scratch directories to "
    "obtain the target number of scratch write I/Os per query.");

namespace impala {

void BufferPool::BufferHandle::Open(uint8_t* data, int64_t len) {
  client_ = nullptr;
  data_ = data;
  len_ = len;
}

BufferPool::PageHandle::PageHandle() {
  Reset();
}

BufferPool::PageHandle::PageHandle(PageHandle&& src) {
  Reset();
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

void BufferPool::PageHandle::Open(Page* page, ClientHandle* client) {
  DCHECK(!is_open());
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
  return page_->len; // Does not require locking.
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
  DCHECK_EQ(0, clean_pages_.size());
}

Status BufferPool::RegisterClient(const string& name, TmpFileMgr::FileGroup* file_group,
    ReservationTracker* parent_reservation, MemTracker* mem_tracker,
    int64_t reservation_limit, RuntimeProfile* profile, ClientHandle* client) {
  DCHECK(!client->is_registered());
  DCHECK(parent_reservation != NULL);
  client->impl_ = new Client(this, file_group, name, parent_reservation, mem_tracker,
      reservation_limit, profile);
  return Status::OK();
}

void BufferPool::DeregisterClient(ClientHandle* client) {
  if (!client->is_registered()) return;
  client->impl_->Close(); // Will DCHECK if any remaining buffers or pinned pages.
  delete client->impl_; // Will DCHECK if there are any remaining pages.
  client->impl_ = NULL;
}

Status BufferPool::CreatePage(ClientHandle* client, int64_t len, PageHandle* handle) {
  DCHECK(!handle->is_open());
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_EQ(len, BitUtil::RoundUpToPowerOfTwo(len));

  BufferHandle buffer;
  // No changes have been made to state yet, so we can cleanly return on error.
  RETURN_IF_ERROR(AllocateBuffer(client, len, &buffer));

  Page* page = new Page(client->impl_, len);
  page->buffer = std::move(buffer);
  handle->Open(page, client);
  page->pin_count++;
  client->impl_->AddNewPinnedPage(page);
  return Status::OK();
}

void BufferPool::DestroyPage(ClientHandle* client, PageHandle* handle) {
  if (!handle->is_open()) return; // DestroyPage() should be idempotent.

  if (handle->is_pinned()) {
    // In the pinned case, delegate to ExtractBuffer() and FreeBuffer() to do the work
    // of cleaning up the page, freeing the buffer and updating reservations correctly.
    BufferHandle buffer;
    ExtractBuffer(client, handle, &buffer);
    FreeBuffer(client, &buffer);
  } else {
    // In the unpinned case, no reservations are used so we just clean up the page.
    client->impl_->DestroyPageInternal(handle);
  }
}

Status BufferPool::Pin(ClientHandle* client, PageHandle* handle) {
  DCHECK(client->is_registered());
  DCHECK(handle->is_open());
  DCHECK_EQ(handle->client_, client);

  Page* page = handle->page_;
  if (page->pin_count == 0) {
    RETURN_IF_ERROR(client->impl_->MoveToPinned(client, handle));
    COUNTER_ADD(client->impl_->counters().peak_unpinned_bytes, -page->len);
  }
  // Update accounting last to avoid complicating the error return path above.
  ++page->pin_count;
  client->impl_->reservation()->AllocateFrom(page->len);
  return Status::OK();
}

void BufferPool::Unpin(ClientHandle* client, PageHandle* handle) {
  DCHECK(handle->is_open());
  DCHECK(client->is_registered());
  DCHECK_EQ(handle->client_, client);
  // If handle is pinned, we can assume that the page itself is pinned.
  DCHECK(handle->is_pinned());
  Page* page = handle->page_;
  ReservationTracker* reservation = client->impl_->reservation();
  reservation->ReleaseTo(page->len);

  if (--page->pin_count > 0) return;
  client->impl_->MoveToDirtyUnpinned(reservation->GetUnusedReservation(), page);
  COUNTER_ADD(client->impl_->counters().total_unpinned_bytes, handle->len());
  COUNTER_ADD(client->impl_->counters().peak_unpinned_bytes, handle->len());
}

void BufferPool::ExtractBuffer(
    ClientHandle* client, PageHandle* page_handle, BufferHandle* buffer_handle) {
  DCHECK(page_handle->is_pinned());
  DCHECK(!buffer_handle->is_open());
  DCHECK_EQ(page_handle->client_, client);

  // Bring the pin count to 1 so that we're not using surplus reservations.
  while (page_handle->pin_count() > 1) Unpin(client, page_handle);

  // Destroy the page and extract the buffer.
  client->impl_->DestroyPageInternal(page_handle, buffer_handle);
  DCHECK(buffer_handle->is_open());
}

Status BufferPool::AllocateBuffer(
    ClientHandle* client, int64_t len, BufferHandle* handle) {
  ReservationTracker* reservation = client->impl_->reservation();
  RETURN_IF_ERROR(client->impl_->CleanPagesBeforeAllocation(reservation, len));
  reservation->AllocateFrom(len);
  return AllocateBufferInternal(client, len, handle);
}

Status BufferPool::AllocateBufferInternal(
    ClientHandle* client, int64_t len, BufferHandle* buffer) {
  DCHECK(!buffer->is_open());
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_EQ(len, BitUtil::RoundUpToPowerOfTwo(len));
  SCOPED_TIMER(client->impl_->counters().get_buffer_time);

  // If there is headroom in 'buffer_bytes_remaining_', we can just allocate a new buffer.
  int64_t delta = DecreaseBufferBytesRemaining(len);
  if (delta < len) {
    // We must evict some pages to free memory before allocating.
    int64_t to_evict = len - delta;
    RETURN_IF_ERROR(EvictCleanPages(to_evict));
  }
  Status status = allocator_->Allocate(len, buffer);
  if (!status.ok()) {
    buffer_bytes_remaining_.Add(len);
    return status;
  }
  DCHECK(buffer->is_open());
  buffer->client_ = client;
  return Status::OK();
}

void BufferPool::FreeBuffer(ClientHandle* client, BufferHandle* handle) {
  if (!handle->is_open()) return; // Should be idempotent.
  DCHECK_EQ(client, handle->client_);
  client->impl_->reservation()->ReleaseTo(handle->len_);
  FreeBufferInternal(handle);
}

void BufferPool::FreeBufferInternal(BufferHandle* handle) {
  DCHECK(handle->is_open());
  int64_t buffer_len = handle->len();
  allocator_->Free(move(*handle));
  buffer_bytes_remaining_.Add(buffer_len);
  handle->Reset();
}

Status BufferPool::TransferBuffer(ClientHandle* src_client, BufferHandle* src,
    ClientHandle* dst_client, BufferHandle* dst) {
  DCHECK(src->is_open());
  DCHECK(!dst->is_open());
  DCHECK_EQ(src_client, src->client_);
  DCHECK_NE(src, dst);
  DCHECK_NE(src_client, dst_client);

  dst_client->impl_->reservation()->AllocateFrom(src->len());
  src_client->impl_->reservation()->ReleaseTo(src->len());
  *dst = std::move(*src);
  dst->client_ = dst_client;
  return Status::OK();
}

int64_t BufferPool::DecreaseBufferBytesRemaining(int64_t max_decrease) {
  // TODO: we may want to change this policy so that we don't always use up to the limit
  // for buffers, since this may starve other operators using non-buffer-pool memory.
  while (true) {
    int64_t old_value = buffer_bytes_remaining_.Load();
    int64_t decrease = min(old_value, max_decrease);
    int64_t new_value = old_value - decrease;
    if (buffer_bytes_remaining_.CompareAndSwap(old_value, new_value)) {
      return decrease;
    }
  }
}

void BufferPool::AddCleanPage(const unique_lock<mutex>& client_lock, Page* page) {
  page->client->DCheckHoldsLock(client_lock);
  lock_guard<SpinLock> cpl(clean_pages_lock_);
  clean_pages_.Enqueue(page);
}

bool BufferPool::RemoveCleanPage(const unique_lock<mutex>& client_lock, Page* page) {
  page->client->DCheckHoldsLock(client_lock);
  lock_guard<SpinLock> cpl(clean_pages_lock_);
  bool found = clean_pages_.Contains(page);
  if (found) clean_pages_.Remove(page);
  return found;
}

Status BufferPool::EvictCleanPages(int64_t bytes_to_evict) {
  DCHECK_GE(bytes_to_evict, 0);
  vector<BufferHandle> buffers;
  int64_t bytes_found = 0;
  {
    lock_guard<SpinLock> cpl(clean_pages_lock_);
    while (bytes_found < bytes_to_evict) {
      Page* page = clean_pages_.Dequeue();
      if (page == NULL) break;
      lock_guard<SpinLock> pl(page->buffer_lock);
      bytes_found += page->len;
      buffers.emplace_back(move(page->buffer));
    }
  }

  // Free buffers after releasing all the locks. Do this regardless of success to avoid
  // leaking buffers.
  for (BufferHandle& buffer : buffers) allocator_->Free(move(buffer));
  if (bytes_found < bytes_to_evict) {
    // The buffer pool should not be overcommitted so this should only happen if there
    // is an accounting error. Add any freed buffers back to 'buffer_bytes_remaining_'
    // to restore consistency.
    buffer_bytes_remaining_.Add(bytes_found);
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Tried to evict $0 bytes but only $1 bytes of clean pages:\n$2",
                      bytes_to_evict, bytes_found, DebugString()));
  }
  // Update 'buffer_bytes_remaining_' with any excess.
  if (bytes_found > bytes_to_evict) {
    buffer_bytes_remaining_.Add(bytes_found - bytes_to_evict);
  }
  return Status::OK();
}

bool BufferPool::ClientHandle::IncreaseReservation(int64_t bytes) {
  return impl_->reservation()->IncreaseReservation(bytes);
}

bool BufferPool::ClientHandle::IncreaseReservationToFit(int64_t bytes) {
  return impl_->reservation()->IncreaseReservationToFit(bytes);
}

int64_t BufferPool::ClientHandle::GetReservation() const {
  return impl_->reservation()->GetReservation();
}

int64_t BufferPool::ClientHandle::GetUsedReservation() const {
  return impl_->reservation()->GetUsedReservation();
}

int64_t BufferPool::ClientHandle::GetUnusedReservation() const {
  return impl_->reservation()->GetUnusedReservation();
}

BufferPool::Client::Client(BufferPool* pool, TmpFileMgr::FileGroup* file_group,
    const string& name, ReservationTracker* parent_reservation, MemTracker* mem_tracker,
    int64_t reservation_limit, RuntimeProfile* profile)
  : pool_(pool),
    file_group_(file_group),
    name_(name),
    num_pages_(0),
    dirty_unpinned_bytes_(0),
    in_flight_write_bytes_(0) {
  reservation_.InitChildTracker(
      profile, parent_reservation, mem_tracker, reservation_limit);
  counters_.get_buffer_time = ADD_TIMER(profile, "BufferPoolGetBufferTime");
  counters_.read_wait_time = ADD_TIMER(profile, "BufferPoolReadIoWaitTime");
  counters_.read_io_ops = ADD_COUNTER(profile, "BufferPoolReadIoOps", TUnit::UNIT);
  counters_.bytes_read = ADD_COUNTER(profile, "BufferPoolReadIoBytes", TUnit::BYTES);
  counters_.write_wait_time = ADD_TIMER(profile, "BufferPoolWriteIoWaitTime");
  counters_.write_io_ops = ADD_COUNTER(profile, "BufferPoolWriteIoOps", TUnit::UNIT);
  counters_.bytes_written = ADD_COUNTER(profile, "BufferPoolWriteIoBytes", TUnit::BYTES);
  counters_.peak_unpinned_bytes =
      profile->AddHighWaterMarkCounter("BufferPoolPeakUnpinnedBytes", TUnit::BYTES);
  counters_.total_unpinned_bytes =
      ADD_COUNTER(profile, "BufferPoolTotalUnpinnedBytes", TUnit::BYTES);
}

void BufferPool::Client::AddNewPinnedPage(Page* page) {
  DCHECK_GT(page->pin_count, 0);
  boost::lock_guard<boost::mutex> lock(lock_);
  pinned_pages_.Enqueue(page);
  ++num_pages_;
}

void BufferPool::Client::DestroyPageInternal(
    PageHandle* handle, BufferHandle* out_buffer) {
  DCHECK(handle->is_pinned() || out_buffer == NULL);
  Page* page = handle->page_;
  // Remove the page from the list that it is currently present in (if any).
  {
    unique_lock<mutex> cl(lock_);
    if (pinned_pages_.Contains(page)) {
      pinned_pages_.Remove(page);
    } else if (dirty_unpinned_pages_.Contains(page)) {
      dirty_unpinned_pages_.Remove(page);
      dirty_unpinned_bytes_ -= page->len;
    } else {
      // The page either has a write in flight, is clean, or is evicted.
      // Let the write complete, if in flight.
      WaitForWrite(&cl, page);
      // If clean, remove it from the clean pages list. If evicted, this is a no-op.
      pool_->RemoveCleanPage(cl, page);
    }
    DCHECK(!page->in_queue());
    --num_pages_;
  }

  if (page->write_handle != NULL) {
    // Discard any on-disk data.
    file_group_->DestroyWriteHandle(move(page->write_handle));
  }
  if (out_buffer != NULL) {
    DCHECK(page->buffer.is_open());
    *out_buffer = std::move(page->buffer);
  } else if (page->buffer.is_open()) {
    pool_->FreeBufferInternal(&page->buffer);
  }
  delete page;
  handle->Reset();
}

void BufferPool::Client::MoveToDirtyUnpinned(int64_t unused_reservation, Page* page) {
  // Only valid to unpin pages if spilling is enabled.
  DCHECK(spilling_enabled());
  DCHECK_EQ(0, page->pin_count);
  unique_lock<mutex> lock(lock_);
  DCHECK(pinned_pages_.Contains(page));
  pinned_pages_.Remove(page);
  dirty_unpinned_pages_.Enqueue(page);
  dirty_unpinned_bytes_ += page->len;

  // Check if we should initiate writes for this (or another) dirty page.
  WriteDirtyPagesAsync();
}

Status BufferPool::Client::MoveToPinned(ClientHandle* client, PageHandle* handle) {
  Page* page = handle->page_;
  unique_lock<mutex> cl(lock_);
  // Propagate any write errors that occurred for this client.
  RETURN_IF_ERROR(write_status_);

  // Check if the page is evicted first. This is not necessary for correctness, since
  // we re-check this later, but by doing it upfront we avoid grabbing the global
  // 'clean_pages_lock_' in the common case.
  bool evicted;
  {
    lock_guard<SpinLock> pl(page->buffer_lock);
    evicted = !page->buffer.is_open();
  }
  if (evicted) return MoveEvictedToPinned(&cl, client, handle);

  if (dirty_unpinned_pages_.Contains(page)) {
    // No writes were initiated for the page - just move it back to the pinned state.
    dirty_unpinned_pages_.Remove(page);
    pinned_pages_.Enqueue(page);
    dirty_unpinned_bytes_ -= page->len;
    return Status::OK();
  }
  if (in_flight_write_pages_.Contains(page)) {
    // A write is in flight. If so, wait for it to complete - then we only have to
    // handle the pinned and evicted cases.
    WaitForWrite(&cl, page);
    RETURN_IF_ERROR(write_status_); // The write may have set 'write_status_'.
  }
  if (pool_->RemoveCleanPage(cl, page)) {
    // The clean page still has an associated buffer. Just clean up the write, restore
    // the data, and move the page back to the pinned state.
    pinned_pages_.Enqueue(page);
    DCHECK(page->buffer.is_open());
    DCHECK(page->write_handle != NULL);
    // Don't need on-disk data.
    cl.unlock(); // Don't block progress for other threads operating on other pages.
    return file_group_->CancelWriteAndRestoreData(
        move(page->write_handle), page->buffer.mem_range());
  }
  // If the page wasn't in the global clean pages list, it must have been evicted after
  // the earlier 'evicted' check.
  return MoveEvictedToPinned(&cl, client, handle);
}

Status BufferPool::Client::MoveEvictedToPinned(
    unique_lock<mutex>* client_lock, ClientHandle* client, PageHandle* handle) {
  Page* page = handle->page_;
  DCHECK(!page->buffer.is_open());
  RETURN_IF_ERROR(CleanPagesBeforeAllocationLocked(
      client_lock, client->impl_->reservation(), page->len));

  // Don't hold any locks while allocating or reading back the data. It is safe to modify
  // the page's buffer handle without holding any locks because no concurrent operations
  // can modify evicted pages.
  client_lock->unlock();
  BufferHandle buffer;
  RETURN_IF_ERROR(pool_->AllocateBufferInternal(client, page->len, &page->buffer));
  COUNTER_ADD(counters().bytes_read, page->len);
  COUNTER_ADD(counters().read_io_ops, 1);
  {
    SCOPED_TIMER(counters().read_wait_time);
    RETURN_IF_ERROR(
        file_group_->Read(page->write_handle.get(), page->buffer.mem_range()));
  }
  file_group_->DestroyWriteHandle(move(page->write_handle));
  client_lock->lock();
  pinned_pages_.Enqueue(page);
  return Status::OK();
}

Status BufferPool::Client::CleanPagesBeforeAllocation(
    ReservationTracker* reservation, int64_t allocation_len) {
  unique_lock<mutex> lock(lock_);
  return CleanPagesBeforeAllocationLocked(&lock, reservation, allocation_len);
}

Status BufferPool::Client::CleanPagesBeforeAllocationLocked(
    unique_lock<mutex>* client_lock, ReservationTracker* reservation,
    int64_t allocation_len) {
  DCheckHoldsLock(*client_lock);
  int64_t unused_reservation = reservation->GetUnusedReservation();
  DCHECK_LE(allocation_len, unused_reservation);
  int64_t unused_reservation_after_alloc = unused_reservation - allocation_len;
  // Start enough writes to ensure that the loop condition below will eventually become
  // false (or a write error will be encountered).
  int64_t min_in_flight_bytes = dirty_unpinned_bytes_ - unused_reservation_after_alloc;
  WriteDirtyPagesAsync(max<int64_t>(0, min_in_flight_bytes - in_flight_write_bytes_));

  // One of the writes we initiated, or an earlier in-flight write may have hit an error.
  RETURN_IF_ERROR(write_status_);

  // Wait until enough writes have finished that the allocation plus dirty pages won't
  // exceed our reservation. I.e. so that other clients can immediately get the allocated
  // memory they're entitled to without waiting for this client's write to complete.
  DCHECK_GE(in_flight_write_bytes_, min_in_flight_bytes);
  while (dirty_unpinned_bytes_ > unused_reservation_after_alloc) {
    SCOPED_TIMER(counters().write_wait_time);
    write_complete_cv_.Wait(*client_lock);
    RETURN_IF_ERROR(write_status_); // Check if error occurred while waiting.
  }
  return Status::OK();
}

void BufferPool::Client::WriteDirtyPagesAsync(int64_t min_bytes_to_write) {
  DCHECK_GE(min_bytes_to_write, 0);
  DCheckConsistency();
  if (file_group_ == NULL) {
    // Spilling disabled - there should be no unpinned pages to write.
    DCHECK_EQ(0, min_bytes_to_write);
    DCHECK_EQ(0, dirty_unpinned_bytes_);
    return;
  }
  // No point in starting writes if an error occurred because future operations for the
  // client will fail regardless.
  if (!write_status_.ok()) return;

  const int64_t writeable_bytes = dirty_unpinned_bytes_ - in_flight_write_bytes_;
  DCHECK_LE(min_bytes_to_write, writeable_bytes);
  // Compute the ideal amount of writes to start. We use a simple heuristic based on the
  // total number of writes. The FileGroup's allocation should spread the writes across
  // disks somewhat, but doesn't guarantee we're fully using all available disks. In
  // future we could track the # of writes per-disk.
  const int64_t target_writes = FLAGS_concurrent_scratch_ios_per_device
      * file_group_->tmp_file_mgr()->NumActiveTmpDevices();

  int64_t bytes_written = 0;
  while (bytes_written < writeable_bytes
      && (bytes_written < min_bytes_to_write
             || in_flight_write_pages_.size() < target_writes)) {
    Page* page = dirty_unpinned_pages_.tail(); // LIFO.
    DCHECK(page != NULL) << "Should have been enough dirty unpinned pages";
    {
      lock_guard<SpinLock> pl(page->buffer_lock);
      DCHECK(file_group_ != NULL);
      DCHECK(page->buffer.is_open());
      COUNTER_ADD(counters().bytes_written, page->len);
      COUNTER_ADD(counters().write_io_ops, 1);
      Status status = file_group_->Write(page->buffer.mem_range(),
          [this, page](const Status& write_status) {
            WriteCompleteCallback(page, write_status);
          },
          &page->write_handle);
      // Exit early on error: there is no point in starting more writes because future
      /// operations for this client will fail regardless.
      if (!status.ok()) {
        write_status_.MergeStatus(status);
        return;
      }
    }
    // Now that the write is in flight, update all the state
    Page* tmp = dirty_unpinned_pages_.PopBack();
    DCHECK_EQ(tmp, page);
    in_flight_write_pages_.Enqueue(page);
    bytes_written += page->len;
    in_flight_write_bytes_ += page->len;
  }
}

void BufferPool::Client::WriteCompleteCallback(Page* page, const Status& write_status) {
  {
    unique_lock<mutex> cl(lock_);
    DCHECK(in_flight_write_pages_.Contains(page));
    // The status should always be propagated.
    // TODO: if we add cancellation support to TmpFileMgr, consider cancellation path.
    if (!write_status.ok()) write_status_.MergeStatus(write_status);
    in_flight_write_pages_.Remove(page);
    // Move to clean pages list even if an error was encountered - the buffer can be
    // repurposed by other clients and 'write_status_' must be checked by this client
    // before reading back the bad data.
    pool_->AddCleanPage(cl, page);
    dirty_unpinned_bytes_ -= page->len;
    in_flight_write_bytes_ -= page->len;
    WriteDirtyPagesAsync(); // Start another asynchronous write if needed.

    // Notify before releasing lock to avoid race with Page and Client destruction.
    page->write_complete_cv_.NotifyAll();
    write_complete_cv_.NotifyAll();
  }
}

void BufferPool::Client::WaitForWrite(unique_lock<mutex>* client_lock, Page* page) {
  DCheckHoldsLock(*client_lock);
  while (in_flight_write_pages_.Contains(page)) {
    SCOPED_TIMER(counters().write_wait_time);
    page->write_complete_cv_.Wait(*client_lock);
  }
}

string BufferPool::Client::DebugString() {
  lock_guard<mutex> lock(lock_);
  stringstream ss;
  ss << Substitute("<BufferPool::Client> $0 name: $1 write_status: $2 num_pages: $3 "
                   "dirty_unpinned_bytes: $4 in_flight_write_bytes: $5 reservation: {$6}",
      this, name_, write_status_.GetDetail(), num_pages_, dirty_unpinned_bytes_,
      in_flight_write_bytes_, reservation_.DebugString());
  ss << "\n  " << pinned_pages_.size() << " pinned pages: ";
  pinned_pages_.Iterate(bind<bool>(Page::DebugStringCallback, &ss, _1));
  ss << "\n  " << dirty_unpinned_pages_.size() << " dirty unpinned pages: ";
  dirty_unpinned_pages_.Iterate(bind<bool>(Page::DebugStringCallback, &ss, _1));
  ss << "\n  " << in_flight_write_pages_.size() << " in flight write pages: ";
  in_flight_write_pages_.Iterate(bind<bool>(Page::DebugStringCallback, &ss, _1));
  return ss.str();
}

string BufferPool::ClientHandle::DebugString() const {
  if (is_registered()) {
    return Substitute(
        "<BufferPool::Client> $0 internal state: {$1}", this, impl_->DebugString());
  } else {
    return Substitute("<BufferPool::ClientHandle> $0 UNREGISTERED", this);
  }
}

string BufferPool::PageHandle::DebugString() const {
  if (is_open()) {
    lock_guard<SpinLock> pl(page_->buffer_lock);
    return Substitute("<BufferPool::PageHandle> $0 client: $1/$2 page: {$3}", this,
        client_, client_->impl_, page_->DebugString());
  } else {
    return Substitute("<BufferPool::PageHandle> $0 CLOSED", this);
  }
}

string BufferPool::Page::DebugString() {
  return Substitute("<BufferPool::Page> $0 len: $1 pin_count: $2 buf: $3", this, len,
      pin_count, buffer.DebugString());
}

bool BufferPool::Page::DebugStringCallback(stringstream* ss, BufferPool::Page* page) {
  lock_guard<SpinLock> pl(page->buffer_lock);
  (*ss) << page->DebugString() << "\n";
  return true;
}

string BufferPool::BufferHandle::DebugString() const {
  if (is_open()) {
    return Substitute("<BufferPool::BufferHandle> $0 client: $1/$2 data: $3 len: $4",
        this, client_, client_->impl_, data_, len_);
  } else {
    return Substitute("<BufferPool::BufferHandle> $0 CLOSED", this);
  }
}

string BufferPool::DebugString() {
  stringstream ss;
  ss << "<BufferPool> " << this << " min_buffer_len: " << min_buffer_len_
     << " buffer_bytes_limit: " << buffer_bytes_limit_
     << " buffer_bytes_remaining: " << buffer_bytes_remaining_.Load() << "\n"
     << "  Clean pages: ";
  {
    lock_guard<SpinLock> cpl(clean_pages_lock_);
    clean_pages_.Iterate(bind<bool>(Page::DebugStringCallback, &ss, _1));
  }
  return ss.str();
}
}
