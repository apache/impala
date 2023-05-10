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
#include "runtime/tmp-file-mgr.h"
#include "util/bit-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/runtime-profile-counters.h"
#include "util/scope-exit-trigger.h"
#include "util/time.h"

DEFINE_int32(concurrent_scratch_ios_per_device, 2,
    "Set this to influence the number of concurrent write I/Os issues to write data to "
    "scratch files. This is multiplied by the number of active scratch directories to "
    "obtain the target number of scratch write I/Os per query.");

namespace impala {

constexpr int BufferPool::LOG_MAX_BUFFER_BYTES;
constexpr int64_t BufferPool::MAX_BUFFER_BYTES;
constexpr int BufferPool::MAX_PAGE_ITER_DEBUG;

void BufferPool::BufferHandle::Open(uint8_t* data, int64_t len, int home_core) {
  DCHECK_LE(0, home_core);
  DCHECK_LT(home_core, CpuInfo::GetMaxNumCores());
  client_ = nullptr;
  data_ = data;
  len_ = len;
  home_core_ = home_core;
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

Status BufferPool::PageHandle::GetBuffer(const BufferHandle** buffer) const {
  DCHECK(is_open());
  DCHECK(client_->is_registered());
  DCHECK(is_pinned());
  // Dirty check to see if we might need to wait for the pin to finish (this
  // avoids the lock acquisition in the common case).
  if (page_->pin_in_flight.Load()) {
    // Finish the work started in Pin().
    RETURN_IF_ERROR(client_->impl_->FinishMoveEvictedToPinned(page_));
  }
  DCHECK(!page_->pin_in_flight.Load());
  *buffer = &page_->buffer;
  DCHECK((*buffer)->is_open());
  return Status::OK();
}

BufferPool::BufferPool(MetricGroup* metrics, int64_t min_buffer_len,
    int64_t buffer_bytes_limit, int64_t clean_page_bytes_limit)
  : allocator_(new BufferAllocator(
        this, metrics, min_buffer_len, buffer_bytes_limit, clean_page_bytes_limit)),
    min_buffer_len_(min_buffer_len) {
  DCHECK_GT(min_buffer_len, 0);
  DCHECK_EQ(min_buffer_len, BitUtil::RoundUpToPowerOfTwo(min_buffer_len));
}

BufferPool::~BufferPool() {}

Status BufferPool::RegisterClient(const string& name, TmpFileGroup* file_group,
    ReservationTracker* parent_reservation, MemTracker* mem_tracker,
    int64_t reservation_limit, RuntimeProfile* profile, ClientHandle* client,
    MemLimit mem_limit_mode) {
  DCHECK(!client->is_registered());
  DCHECK(parent_reservation != NULL);
  client->impl_ = new Client(this, file_group, name, parent_reservation, mem_tracker,
      mem_limit_mode, reservation_limit, profile);
  return Status::OK();
}

void BufferPool::DeregisterClient(ClientHandle* client) {
  if (!client->is_registered()) return;
  client->impl_->Close(); // Will DCHECK if any remaining buffers or pinned pages.
  delete client->impl_; // Will DCHECK if there are any remaining pages.
  client->impl_ = NULL;
}

Status BufferPool::CreatePage(
    ClientHandle* client, int64_t len, PageHandle* handle, const BufferHandle** buffer) {
  DCHECK(!handle->is_open());
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_EQ(len, BitUtil::RoundUpToPowerOfTwo(len));

  BufferHandle new_buffer;
  // No changes have been made to state yet, so we can cleanly return on error.
  RETURN_IF_ERROR(AllocateBuffer(client, len, &new_buffer));
  Page* page = client->impl_->CreatePinnedPage(move(new_buffer));
  handle->Open(page, client);
  if (buffer != nullptr) *buffer = &page->buffer;
  return Status::OK();
}

void BufferPool::DestroyPage(ClientHandle* client, PageHandle* handle) {
  if (!handle->is_open()) return; // DestroyPage() should be idempotent.

  if (handle->is_pinned()) {
    // Cancel the read I/O - we don't need the data any more.
    if (handle->page_->pin_in_flight.Load()) {
      handle->page_->write_handle->CancelRead();
      handle->page_->pin_in_flight.Store(false);
    }
    // In the pinned case, delegate to ExtractBuffer() and FreeBuffer() to do the work
    // of cleaning up the page, freeing the buffer and updating reservations correctly.
    BufferHandle buffer;
    Status status = ExtractBuffer(client, handle, &buffer);
    DCHECK(status.ok()) << status.msg().msg();
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
    RETURN_IF_ERROR(client->impl_->StartMoveToPinned(client, page));
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
  if (page->pin_in_flight.Load()) {
    // Data is not in memory - move it back to evicted.
    client->impl_->UndoMoveEvictedToPinned(page);
  } else {
    // Data is in memory - move it to dirty unpinned.
    client->impl_->MoveToDirtyUnpinned(page);
  }
  COUNTER_ADD(client->impl_->counters().peak_unpinned_bytes, handle->len());
}

Status BufferPool::ExtractBuffer(
    ClientHandle* client, PageHandle* page_handle, BufferHandle* buffer_handle) {
  DCHECK(page_handle->is_pinned());
  DCHECK(!buffer_handle->is_open());
  DCHECK_EQ(page_handle->client_, client);

  // If an async pin is in flight, we need to wait for it.
  const BufferHandle* dummy;
  RETURN_IF_ERROR(page_handle->GetBuffer(&dummy));

  // Bring the pin count to 1 so that we're not using surplus reservations.
  while (page_handle->pin_count() > 1) Unpin(client, page_handle);

  // Destroy the page and extract the buffer.
  client->impl_->DestroyPageInternal(page_handle, buffer_handle);
  DCHECK(buffer_handle->is_open());
  return Status::OK();
}

Status BufferPool::AllocateBuffer(
    ClientHandle* client, int64_t len, BufferHandle* handle) {
  RETURN_IF_ERROR(client->impl_->PrepareToAllocateBuffer(len, true, nullptr));
  Status status = allocator_->Allocate(client, len, handle);
  // If the allocation failed, update client's accounting to reflect the failure.
  if (!status.ok()) client->impl_->FreedBuffer(len);
  return status;
}

Status BufferPool::AllocateUnreservedBuffer(
    ClientHandle* client, int64_t len, BufferHandle* handle) {
  DCHECK(!handle->is_open());
  bool success;
  RETURN_IF_ERROR(client->impl_->PrepareToAllocateBuffer(len, false, &success));
  if (!success) return Status::OK(); // Leave 'handle' closed to indicate failure.

  Status status = allocator_->Allocate(client, len, handle);
  // If the allocation failed, update client's accounting to reflect the failure.
  if (!status.ok()) client->impl_->FreedBuffer(len);
  return status;
}

void BufferPool::FreeBuffer(ClientHandle* client, BufferHandle* handle) {
  if (!handle->is_open()) return; // Should be idempotent.
  DCHECK_EQ(client, handle->client_);
  int64_t len = handle->len_;
  allocator_->Free(move(*handle));
  client->impl_->FreedBuffer(len);
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

void BufferPool::Maintenance() {
  allocator_->Maintenance();
}

void BufferPool::ReleaseMemory(int64_t bytes_to_free) {
  allocator_->ReleaseMemory(bytes_to_free);
}

int64_t BufferPool::GetSystemBytesLimit() const {
  return allocator_->system_bytes_limit();
}

int64_t BufferPool::GetSystemBytesAllocated() const {
  return allocator_->GetSystemBytesAllocated();
}

int64_t BufferPool::GetCleanPageBytesLimit() const {
  return allocator_->GetCleanPageBytesLimit();
}

int64_t BufferPool::GetNumCleanPages() const {
  return allocator_->GetNumCleanPages();
}

int64_t BufferPool::GetCleanPageBytes() const {
  return allocator_->GetCleanPageBytes();
}

int64_t BufferPool::GetNumFreeBuffers() const {
  return allocator_->GetNumFreeBuffers();
}

int64_t BufferPool::GetFreeBufferBytes() const {
  return allocator_->GetFreeBufferBytes();
}

bool BufferPool::ClientHandle::IncreaseReservation(int64_t bytes) {
  return impl_->reservation()->IncreaseReservation(bytes);
}

bool BufferPool::ClientHandle::IncreaseReservationToFit(int64_t bytes) {
  return impl_->reservation()->IncreaseReservationToFit(bytes);
}

Status BufferPool::ClientHandle::DecreaseReservationTo(
    int64_t max_decrease, int64_t target_bytes) {
  return impl_->DecreaseReservationTo(max_decrease, target_bytes);
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

bool BufferPool::ClientHandle::TransferReservationFrom(
    ReservationTracker* src, int64_t bytes) {
  DCHECK(!impl_->has_unpinned_pages());
  return src->TransferReservationTo(impl_->reservation(), bytes);
}

Status BufferPool::ClientHandle::TransferReservationTo(
    ReservationTracker* dst, int64_t bytes, bool* transferred) {
  return impl_->TransferReservationTo(dst, bytes, transferred);
}

Status BufferPool::ClientHandle::TransferReservationTo(ClientHandle* dst, int64_t bytes,
    bool* transferred) {
  DCHECK(dst->is_registered());
  return TransferReservationTo(dst->impl_->reservation(), bytes, transferred);
}

void BufferPool::ClientHandle::SaveReservation(SubReservation* dst, int64_t bytes) {
  DCHECK(!dst->is_closed());
  DCHECK_EQ(dst->tracker_->parent(), impl_->reservation());
  bool success = impl_->reservation()->TransferReservationTo(dst->tracker_.get(), bytes);
  DCHECK(success); // SubReservation should not have a limit, so this shouldn't fail.
}

void BufferPool::ClientHandle::RestoreReservation(SubReservation* src, int64_t bytes) {
  DCHECK(!src->is_closed());
  DCHECK_EQ(src->tracker_->parent(), impl_->reservation());
  bool success = src->tracker_->TransferReservationTo(impl_->reservation(), bytes);
  DCHECK(success); // Transferring reservation to parent shouldn't fail.
}

void BufferPool::ClientHandle::RestoreAllReservation(SubReservation* src) {
  RestoreReservation(src, src->GetReservation());
}

void BufferPool::ClientHandle::SetDebugDenyIncreaseReservation(double probability) {
  impl_->reservation()->SetDebugDenyIncreaseReservation(probability);
}

int64_t BufferPool::ClientHandle::min_buffer_len() const {
  return impl_->min_buffer_len();
}

bool BufferPool::ClientHandle::has_unpinned_pages() const {
  return impl_->has_unpinned_pages();
}

BufferPool::SubReservation::SubReservation() {
  DCHECK(is_closed()) << "subreservation must be closed.";
}

BufferPool::SubReservation::SubReservation(ClientHandle* client) {
  DCHECK(client->is_registered());
  Init(client);
}

void BufferPool::SubReservation::Init(ClientHandle* client) {
  DCHECK(tracker_ == nullptr);
  DCHECK(client->is_registered());
  tracker_.reset(new ReservationTracker);
  tracker_->InitChildTracker(
      nullptr, client->impl_->reservation(), nullptr, numeric_limits<int64_t>::max());
}

BufferPool::SubReservation::~SubReservation() {}

int64_t BufferPool::SubReservation::GetReservation() const {
  return tracker_->GetReservation();
}

void BufferPool::SubReservation::Close() {
  // Give any reservation back to the client.
  if (is_closed()) return;
  bool success =
      tracker_->TransferReservationTo(tracker_->parent(), tracker_->GetReservation());
  DCHECK(success); // Transferring reservation to parent shouldn't fail.
  tracker_->Close();
  tracker_.reset();
}

BufferPool::Client::Client(BufferPool* pool, TmpFileGroup* file_group,
    const string& name, ReservationTracker* parent_reservation, MemTracker* mem_tracker,
    MemLimit mem_limit_mode, int64_t reservation_limit, RuntimeProfile* profile)
  : pool_(pool),
    file_group_(file_group),
    name_(name),
    debug_write_delay_ms_(0),
    num_pages_(0),
    buffers_allocated_bytes_(0) {
  // Set up a child profile with buffer pool info.
  RuntimeProfile* child_profile =
      profile->CreateChild(RuntimeProfile::BUFFER_POOL, true, true, false);
  reservation_.InitChildTracker(
      child_profile, parent_reservation, mem_tracker, reservation_limit, mem_limit_mode);
  counters_.alloc_time = ADD_TIMER(child_profile, "AllocTime");
  counters_.sys_alloc_time = ADD_TIMER(child_profile, "SystemAllocTime");
  counters_.compression_time = ADD_TIMER(child_profile, "CompressionTime");
  counters_.encryption_time = ADD_TIMER(child_profile, "EncryptionTime");
  counters_.cumulative_allocations =
      ADD_COUNTER(child_profile, "CumulativeAllocations", TUnit::UNIT);
  counters_.cumulative_bytes_alloced =
      ADD_COUNTER(child_profile, "CumulativeAllocationBytes", TUnit::BYTES);
  counters_.read_wait_time = ADD_TIMER(child_profile, "ReadIoWaitTime");
  counters_.read_io_ops = ADD_COUNTER(child_profile, "ReadIoOps", TUnit::UNIT);
  counters_.bytes_read = ADD_COUNTER(child_profile, "ReadIoBytes", TUnit::BYTES);
  counters_.write_wait_time = ADD_TIMER(child_profile, "WriteIoWaitTime");
  counters_.write_io_ops = ADD_COUNTER(child_profile, "WriteIoOps", TUnit::UNIT);
  counters_.bytes_written = ADD_COUNTER(child_profile, "WriteIoBytes", TUnit::BYTES);
  counters_.peak_unpinned_bytes =
      child_profile->AddHighWaterMarkCounter("PeakUnpinnedBytes", TUnit::BYTES);
}

BufferPool::Page* BufferPool::Client::CreatePinnedPage(BufferHandle&& buffer) {
  Page* page = new Page(this, buffer.len());
  page->buffer = move(buffer);
  page->pin_count = 1;

  std::lock_guard<std::mutex> lock(lock_);
  // The buffer is transferred to the page so will be accounted for in
  // pinned_pages_.bytes() instead of buffers_allocated_bytes_.
  buffers_allocated_bytes_ -= page->len;
  pinned_pages_.Enqueue(page);
  ++num_pages_;
  DCHECK_CONSISTENCY();
  return page;
}

void BufferPool::Client::DestroyPageInternal(
    PageHandle* handle, BufferHandle* out_buffer) {
  DCHECK(handle->is_pinned() || out_buffer == NULL);
  Page* page = handle->page_;
  // Remove the page from the list that it is currently present in (if any).
  {
    unique_lock<mutex> cl(lock_);
    // First try to remove from the pinned or dirty unpinned lists.
    if (!pinned_pages_.Remove(page) && !dirty_unpinned_pages_.Remove(page)) {
      // The page either has a write in flight, is clean, or is evicted.
      // Let the write complete, if in flight.
      WaitForWrite(&cl, page);
      // If clean, remove it from the clean pages list. If evicted, this is a no-op.
      pool_->allocator_->RemoveCleanPage(cl, out_buffer != nullptr, page);
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
    buffers_allocated_bytes_ += out_buffer->len();
  } else if (page->buffer.is_open()) {
    pool_->allocator_->Free(move(page->buffer));
  }
  delete page;
  handle->Reset();
}

void BufferPool::Client::MoveToDirtyUnpinned(Page* page) {
  // Only valid to unpin pages if spilling is enabled.
  DCHECK(spilling_enabled());
  DCHECK_EQ(0, page->pin_count);

  unique_lock<mutex> lock(lock_);
  DCHECK_CONSISTENCY();
  DCHECK(pinned_pages_.Contains(page));
  pinned_pages_.Remove(page);
  dirty_unpinned_pages_.Enqueue(page);

  // Check if we should initiate writes for this (or another) dirty page.
  WriteDirtyPagesAsync();
}

Status BufferPool::Client::StartMoveToPinned(ClientHandle* client, Page* page) {
  unique_lock<mutex> cl(lock_);
  DCHECK_CONSISTENCY();
  // Propagate any write errors that occurred for this client.
  RETURN_IF_ERROR(write_status_);

  if (dirty_unpinned_pages_.Remove(page)) {
    // No writes were initiated for the page - just move it back to the pinned state.
    pinned_pages_.Enqueue(page);
    return Status::OK();
  }
  if (in_flight_write_pages_.Contains(page)) {
    // A write is in flight. If so, wait for it to complete - then we only have to
    // handle the pinned and evicted cases.
    WaitForWrite(&cl, page);
    RETURN_IF_ERROR(write_status_); // The write may have set 'write_status_'.
  }

  // At this point we need to either reclaim a clean page or allocate a new buffer.
  // We may need to clean some pages to do so.
  RETURN_IF_ERROR(CleanPages(&cl, page->len));
  if (pool_->allocator_->RemoveCleanPage(cl, true, page)) {
    // The clean page still has an associated buffer. Restore the data, and move the page
    // back to the pinned state.
    pinned_pages_.Enqueue(page);
    DCHECK(page->buffer.is_open());
    DCHECK(page->write_handle != NULL);
    // Don't need on-disk data.
    cl.unlock(); // Don't block progress for other threads operating on other pages.
    return file_group_->RestoreData(
        move(page->write_handle), page->buffer.mem_range(), &counters_);
  }
  // If the page wasn't in the clean pages list, it must have been evicted.
  return StartMoveEvictedToPinned(&cl, client, page);
}

Status BufferPool::Client::StartMoveEvictedToPinned(
    unique_lock<mutex>* client_lock, ClientHandle* client, Page* page) {
  DCHECK(!page->buffer.is_open());

  // Safe to modify the page's buffer handle without holding the page lock because no
  // concurrent operations can modify evicted pages.
  BufferHandle buffer;
  RETURN_IF_ERROR(pool_->allocator_->Allocate(client, page->len, &page->buffer));
  COUNTER_ADD(counters().bytes_read, page->len);
  COUNTER_ADD(counters().read_io_ops, 1);
  RETURN_IF_ERROR(
      file_group_->ReadAsync(page->write_handle.get(), page->buffer.mem_range()));
  pinned_pages_.Enqueue(page);
  page->pin_in_flight.Store(true);
  DCHECK_CONSISTENCY();
  return Status::OK();
}

void BufferPool::Client::UndoMoveEvictedToPinned(Page* page) {
  // We need to get the page back to the evicted state where:
  // * There is no in-flight read.
  // * The page's data is on disk referenced by 'write_handle'
  // * The page has no attached buffer.
  DCHECK(page->pin_in_flight.Load());
  page->write_handle->CancelRead();
  page->pin_in_flight.Store(false);

  unique_lock<mutex> lock(lock_);
  DCHECK_CONSISTENCY();
  DCHECK(pinned_pages_.Contains(page));
  pinned_pages_.Remove(page);
  // Discard the buffer - the pin was in flight so there was no way that a valid
  // reference to the buffer's contents was returned since the pin was still in flight.
  pool_->allocator_->Free(move(page->buffer));
}

Status BufferPool::Client::FinishMoveEvictedToPinned(Page* page) {
  SCOPED_TIMER(counters().read_wait_time);
  lock_guard<SpinLock> pl(page->buffer_lock);
  // Another thread may have moved it to pinned in the meantime.
  if (!page->pin_in_flight.Load()) return Status::OK();
  // Don't hold any locks while reading back the data. It is safe to modify the page's
  // buffer handle without holding any locks because no concurrent operations can modify
  // evicted pages.
  RETURN_IF_ERROR(file_group_->WaitForAsyncRead(
      page->write_handle.get(), page->buffer.mem_range(), &counters_));
  file_group_->DestroyWriteHandle(move(page->write_handle));
  page->pin_in_flight.Store(false);
  return Status::OK();
}

Status BufferPool::Client::PrepareToAllocateBuffer(
    int64_t len, bool reserved, bool* success) {
  if (success != nullptr) *success = false;
  // Don't need to hold the client's 'lock_' yet because 'reservation_' operations are
  // threadsafe.
  if (reserved) {
    // The client must have already reserved the memory.
    reservation_.AllocateFrom(len);
  } else {
    DCHECK(success != nullptr);
    // The client may not have reserved the memory.
    if (!reservation_.IncreaseReservationToFitAndAllocate(len)) return Status::OK();
  }

  {
    unique_lock<mutex> lock(lock_);
    // Clean enough pages to allow allocation to proceed without violating our eviction
    // policy.
    Status status = CleanPages(&lock, len);
    if (!status.ok()) {
      // Reverse the allocation.
      reservation_.ReleaseTo(len);
      return status;
    }
    buffers_allocated_bytes_ += len;
    DCHECK_CONSISTENCY();
  }
  if (success != nullptr) *success = true;
  return Status::OK();
}

Status BufferPool::Client::DecreaseReservationTo(
    int64_t max_decrease, int64_t target_bytes) {
  unique_lock<mutex> lock(lock_);
  // Get a snapshot of the current reservation. Reservation may be increased concurrently
  // without holding 'lock_' but cannot be decreased, so the end result may be higher
  // than 'target_bytes' if another thread concurrently increases reservation. This
  // interleaving of threads gives the same result as IncreaseReservation() running
  // after DecreaseReservationTo(). Similarly, running IncreaseReservationToFit() and
  // DecreaseReservationTo() concurrently can lead to a range of outcomes, but that
  // is unavoidable by the nature of the methods.
  int64_t current_reservation = reservation_.GetReservation();
  DCHECK_GE(current_reservation, target_bytes);
  int64_t amount_to_free = min(max_decrease, current_reservation - target_bytes);
  if (amount_to_free == 0) return Status::OK();
  // Clean enough pages to allow us to safely release reservation.
  RETURN_IF_ERROR(CleanPages(&lock, amount_to_free));
  reservation_.DecreaseReservation(amount_to_free);
  return Status::OK();
}

Status BufferPool::Client::TransferReservationTo(
    ReservationTracker* dst, int64_t bytes, bool* transferred) {
  unique_lock<mutex> lock(lock_);
  // Only flush pages if necessary, to avoid propagating write errors unnecessarily.
  RETURN_IF_ERROR(CleanPages(&lock, bytes, /*lazy_flush=*/true));
  *transferred = reservation_.TransferReservationTo(dst, bytes);
  return Status::OK();
}

Status BufferPool::Client::CleanPages(
    unique_lock<mutex>* client_lock, int64_t len, bool lazy_flush) {
  DCHECK_GE(len, 0);
  DCHECK_LE(len, reservation_.GetReservation());
  DCheckHoldsLock(*client_lock);
  // If another thread is in CleanPages() for this client (and has dropped the lock while
  // waiting on 'write_complete_cv_', wait for it to flush its pages before proceeding so
  // that we don't overcommit memory.
  while (cleaning_pages_) clean_pages_done_cv_.Wait(*client_lock);

  DCHECK_CONSISTENCY();
  // Work out what we need to get bytes of dirty unpinned + in flight pages down to
  // in order to satisfy the eviction policy.
  int64_t target_dirty_bytes = reservation_.GetReservation() - buffers_allocated_bytes_
      - pinned_pages_.bytes() - len;
  if (VLOG_IS_ON(3)) {
    VLOG(3)   << "target_dirty_bytes=" << target_dirty_bytes
              << " reservation=" << reservation_.GetReservation()
              << " buffers_allocated_bytes_=" << buffers_allocated_bytes_
              << " pinned_pages_.bytes()=" << pinned_pages_.bytes()
              << " len=" << len << "\n"
              << DebugStringLocked();
  }
  // Start enough writes to ensure that the loop condition below will eventually become
  // false (or a write error will be encountered).
  int64_t min_bytes_to_write =
      max<int64_t>(0, dirty_unpinned_pages_.bytes() - target_dirty_bytes);
  if (lazy_flush
      && dirty_unpinned_pages_.bytes() + in_flight_write_pages_.bytes()
          <= target_dirty_bytes) {
    return Status::OK();
  }
  cleaning_pages_ = true;
  auto exit_trigger = MakeScopeExitTrigger([this, client_lock]() {
    DCheckHoldsLock(*client_lock);
    cleaning_pages_ = false;
    clean_pages_done_cv_.NotifyAll();
  });
  WriteDirtyPagesAsync(min_bytes_to_write);

  // One of the writes we initiated, or an earlier in-flight write may have hit an error.
  RETURN_IF_ERROR(write_status_);

  // Wait until enough writes have finished so that we can make the allocation without
  // violating the eviction policy. I.e. so that other clients can immediately get the
  // memory they're entitled to without waiting for this client's write to complete.
  DCHECK_GE(in_flight_write_pages_.bytes(), min_bytes_to_write) << DebugStringLocked();
  while (dirty_unpinned_pages_.bytes() + in_flight_write_pages_.bytes()
      > target_dirty_bytes) {
    SCOPED_TIMER(counters().write_wait_time);
    write_complete_cv_.Wait(*client_lock);
    RETURN_IF_ERROR(write_status_); // Check if error occurred while waiting.
  }
  return Status::OK();
}

void BufferPool::Client::WriteDirtyPagesAsync(int64_t min_bytes_to_write) {
  DCHECK_GE(min_bytes_to_write, 0) << DebugStringLocked();
  DCHECK_LE(min_bytes_to_write, dirty_unpinned_pages_.bytes()) << DebugStringLocked();
  if (file_group_ == NULL) {
    // Spilling disabled - there should be no unpinned pages to write.
    DCHECK_EQ(0, min_bytes_to_write);
    DCHECK_EQ(0, dirty_unpinned_pages_.bytes());
    return;
  }
  // No point in starting writes if an error occurred because future operations for the
  // client will fail regardless.
  if (!write_status_.ok()) return;

  // Compute the ideal amount of writes to start. We use a simple heuristic based on the
  // total number of writes. The FileGroup's allocation should spread the writes across
  // disks somewhat, but doesn't guarantee we're fully using all available disks. In
  // future we could track the # of writes per-disk.
  const int64_t target_writes = FLAGS_concurrent_scratch_ios_per_device
      * file_group_->tmp_file_mgr()->NumActiveTmpDevices();

  int64_t bytes_written = 0;
  while (!dirty_unpinned_pages_.empty()
      && (bytes_written < min_bytes_to_write
             || in_flight_write_pages_.size() < target_writes)) {
    Page* page = dirty_unpinned_pages_.tail(); // LIFO.
    DCHECK(page != NULL) << "Should have been enough dirty unpinned pages";
    {
      lock_guard<SpinLock> pl(page->buffer_lock);
      DCHECK(file_group_ != NULL);
      DCHECK(page->buffer.is_open());
      Status status = file_group_->Write(page->buffer.mem_range(),
          [this, page](
              const Status& write_status) { WriteCompleteCallback(page, write_status); },
          &page->write_handle, &counters_);
      // Exit early on error: there is no point in starting more writes because future
      /// operations for this client will fail regardless.
      if (!status.ok()) {
        write_status_.MergeStatus(status);
        return;
      }
      COUNTER_ADD(counters().bytes_written, page->write_handle->on_disk_len());
      COUNTER_ADD(counters().write_io_ops, 1);
    }
    // Now that the write is in flight, update all the state
    Page* tmp = dirty_unpinned_pages_.PopBack();
    DCHECK_EQ(tmp, page);
    in_flight_write_pages_.Enqueue(page);
    bytes_written += page->len;
  }
}

void BufferPool::Client::WriteCompleteCallback(Page* page, const Status& write_status) {
#ifndef NDEBUG
  if (debug_write_delay_ms_ > 0) SleepForMs(debug_write_delay_ms_);
#endif
  {
    unique_lock<mutex> cl(lock_);
    DCHECK(in_flight_write_pages_.Contains(page)) << DebugStringLocked();
    // The status should always be propagated.
    // TODO: if we add cancellation support to TmpFileMgr, consider cancellation path.
    if (!write_status.ok()) write_status_.MergeStatus(write_status);
    in_flight_write_pages_.Remove(page);
    // Move to clean pages list even if an error was encountered - the buffer can be
    // repurposed by other clients and 'write_status_' must be checked by this client
    // before it can be re-pinned.
    pool_->allocator_->AddCleanPage(cl, page);
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

void BufferPool::Client::WaitForAllWrites() {
  unique_lock<mutex> cl(lock_);
  while (in_flight_write_pages_.size() > 0) {
    write_complete_cv_.Wait(cl);
  }
}

string BufferPool::Client::DebugString() {
  lock_guard<mutex> lock(lock_);
  return DebugStringLocked();
}

string BufferPool::Client::DebugStringLocked() {
  stringstream ss;
  ss << Substitute("<BufferPool::Client> $0 name: $1 write_status: $2 "
                   "buffers allocated $3 num_pages: $4 pinned_bytes: $5 "
                   "dirty_unpinned_bytes: $6 in_flight_write_bytes: $7 reservation: {$8}",
      this, name_, write_status_.GetDetail(), buffers_allocated_bytes_, num_pages_,
      pinned_pages_.bytes(), dirty_unpinned_pages_.bytes(),
      in_flight_write_pages_.bytes(), reservation_.DebugString());
  int page_to_print = min(pinned_pages_.size(), BufferPool::MAX_PAGE_ITER_DEBUG);
  ss << "\n  " << page_to_print << " out of " << pinned_pages_.size()
     << " pinned pages: ";
  pinned_pages_.IterateFirstN(
      bind<bool>(Page::DebugStringCallback, &ss, _1), page_to_print);

  page_to_print = min(dirty_unpinned_pages_.size(), MAX_PAGE_ITER_DEBUG);
  ss << "\n  " << page_to_print << " out of " << dirty_unpinned_pages_.size()
     << " dirty unpinned pages: ";
  dirty_unpinned_pages_.IterateFirstN(
      bind<bool>(Page::DebugStringCallback, &ss, _1), page_to_print);

  page_to_print = min(in_flight_write_pages_.size(), MAX_PAGE_ITER_DEBUG);
  ss << "\n  " << page_to_print << " out of " << in_flight_write_pages_.size()
     << " in flight write pages: ";
  in_flight_write_pages_.IterateFirstN(
      bind<bool>(Page::DebugStringCallback, &ss, _1), page_to_print);
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

BufferPool::Page::Page(Client* client, int64_t len)
    : client(client), len(len), pin_count(0), pin_in_flight(false) {}

BufferPool::Page::~Page() {}

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
  ss << "<BufferPool> " << this << " min_buffer_len: " << min_buffer_len_ << "\n"
     << allocator_->DebugString();
  return ss.str();
}
}
