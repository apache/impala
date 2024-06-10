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

#include "runtime/buffered-tuple-stream.inline.h"

#include <boost/bind.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

#ifdef NDEBUG
#define CHECK_CONSISTENCY_FAST(read_it)
#define CHECK_CONSISTENCY_FULL(read_it)
#else
#define CHECK_CONSISTENCY_FAST(read_it) CheckConsistencyFast(read_it)
#define CHECK_CONSISTENCY_FULL(read_it) CheckConsistencyFull(read_it)
#endif

using namespace impala;
using namespace strings;

using BufferHandle = BufferPool::BufferHandle;
using FlushMode = RowBatch::FlushMode;

constexpr int64_t BufferedTupleStream::MAX_PAGE_ITER_DEBUG;

BufferedTupleStream::BufferedTupleStream(RuntimeState* state,
    const RowDescriptor* row_desc, BufferPool::ClientHandle* buffer_pool_client,
    int64_t default_page_len, int64_t max_page_len, const set<SlotId>& ext_varlen_slots)
  : state_(state),
    desc_(row_desc),
    buffer_pool_(ExecEnv::GetInstance()->buffer_pool()),
    buffer_pool_client_(buffer_pool_client),
    read_page_reservation_(buffer_pool_client_),
    large_read_page_reservation_(buffer_pool_client_),
    write_page_reservation_(buffer_pool_client_),
    default_page_len_(default_page_len),
    max_page_len_(max_page_len),
    has_nullable_tuple_(row_desc->IsAnyTupleNullable()) {
  DCHECK_GE(max_page_len, default_page_len);
  DCHECK(BitUtil::IsPowerOf2(default_page_len)) << default_page_len;
  DCHECK(BitUtil::IsPowerOf2(max_page_len)) << max_page_len;
  read_it_.Reset(&pages_);
  for (int i = 0; i < desc_->tuple_descriptors().size(); ++i) {
    const TupleDescriptor* tuple_desc = desc_->tuple_descriptors()[i];
    const int tuple_byte_size = tuple_desc->byte_size();
    fixed_tuple_sizes_.push_back(tuple_byte_size);
    CollectInlinedSlots(tuple_desc, ext_varlen_slots, i);
  }
}

void BufferedTupleStream::CollectInlinedSlots(const TupleDescriptor* tuple_desc,
    const set<SlotId>& ext_varlen_slots, int tuple_idx) {
  vector<SlotDescriptor*> inlined_string_slots;
  vector<SlotDescriptor*> inlined_coll_slots;
  for (SlotDescriptor* slot : tuple_desc->string_slots()) {
    if (ext_varlen_slots.find(slot->id()) == ext_varlen_slots.end()) {
      inlined_string_slots.push_back(slot);
    }
  }
  for (SlotDescriptor* slot : tuple_desc->collection_slots()) {
    if (ext_varlen_slots.find(slot->id()) == ext_varlen_slots.end()) {
      inlined_coll_slots.push_back(slot);
    }
  }
  if (!inlined_string_slots.empty()) {
    inlined_string_slots_.push_back(make_pair(tuple_idx, inlined_string_slots));
  }
  if (!inlined_coll_slots.empty()) {
    inlined_coll_slots_.push_back(make_pair(tuple_idx, inlined_coll_slots));
  }
}

void BufferedTupleStream::CheckConsistencyFull(const ReadIterator& read_it) const {
  CheckConsistencyFast(read_it);
  // The below checks require iterating over all the pages in the stream.
  DCHECK_EQ(bytes_pinned_, CalcBytesPinned()) << DebugString();
  DCHECK_EQ(pages_.size(), num_pages_) << DebugString();
  for (const Page& page : pages_) CheckPageConsistency(&page);
}

void BufferedTupleStream::CheckConsistencyFast(const ReadIterator& read_it) const {
  // All the below checks should be O(1).
  DCHECK(has_write_iterator() || write_page_ == nullptr);
  if (write_page_ != nullptr) {
    CheckPageConsistency(write_page_);
    DCHECK(write_page_->is_pinned());
    DCHECK(write_page_->retrieved_buffer.Load());
    const BufferHandle* write_buffer;
    Status status = write_page_->GetBuffer(&write_buffer);
    DCHECK(status.ok()); // Write buffer should never have been unpinned.
    DCHECK_GE(write_ptr_, write_buffer->data());
    DCHECK_EQ(write_end_ptr_, write_buffer->data() + write_page_->len());
    DCHECK_GE(write_end_ptr_, write_ptr_);
  }
  DCHECK(read_it.is_valid() || read_it.read_page_ == pages_.end());
  if (read_it.read_page_ != pages_.end()) {
    CheckPageConsistency(&*read_it.read_page_);
    if (!read_it.read_page_->attached_to_output_batch) {
      DCHECK(read_it.read_page_->is_pinned());
      DCHECK(read_it.read_page_->retrieved_buffer.Load());
      // Can't check read buffer without affecting behaviour, because a read may be in
      // flight and this would required blocking on that write.
      DCHECK_GE(read_it.read_end_ptr_, read_it.read_ptr_);
    }
  }
  DCHECK(&read_it == &read_it_ || !read_it_.attach_on_read_)
      << "External read iterators cannot coexist with attach_on_read mode";
  if (&read_it == &read_it_ && NeedReadReservation()) {
    DCHECK_EQ(default_page_len_, read_page_reservation_.GetReservation())
        << DebugString();
  } else if (!read_page_reservation_.is_closed()) {
    DCHECK_EQ(0, read_page_reservation_.GetReservation());
  }
  if (NeedWriteReservation()) {
    DCHECK_EQ(default_page_len_, write_page_reservation_.GetReservation());
  } else if (!write_page_reservation_.is_closed()) {
    DCHECK_EQ(0, write_page_reservation_.GetReservation());
  }
}

void BufferedTupleStream::CheckPageConsistency(const Page* page) const {
  if (page->attached_to_output_batch) {
    /// Read page was just attached to output batch.
    DCHECK(is_read_page(page)) << page->DebugString();
    DCHECK(!page->handle.is_open());
    return;
  }
  DCHECK_EQ(ExpectedPinCount(pinned_, page), page->pin_count()) << DebugString();
  // Only one large row per page.
  if (page->len() > default_page_len_) DCHECK_LE(page->num_rows, 1);
  // We only create pages when we have a row to append to them.
  DCHECK_GT(page->num_rows, 0);
}

string BufferedTupleStream::DebugString() const {
  stringstream ss;
  ss << "BufferedTupleStream num_rows=" << num_rows_ << " pinned=" << pinned_
     << " closed=" << closed_ << "\n"
     << " bytes_pinned=" << bytes_pinned_ << " has_write_iterator=" << has_write_iterator_
     << " write_page=" << write_page_ << " read_iterator=" << read_it_.DebugString(pages_)
     << "\n"
     << " read_page_reservation=";
  if (read_page_reservation_.is_closed()) {
    ss << "<closed>";
  } else {
    ss << read_page_reservation_.GetReservation();
  }
  ss << " large_read_page_reservation=";
  if (large_read_page_reservation_.is_closed()) {
    ss << "<closed>";
  } else {
    ss << large_read_page_reservation_.GetReservation();
  }
  ss << " write_page_reservation=";
  if (write_page_reservation_.is_closed()) {
    ss << "<closed>";
  } else {
    ss << write_page_reservation_.GetReservation();
  }
  int64_t max_page = min(num_pages_, BufferedTupleStream::MAX_PAGE_ITER_DEBUG);
  ss << "\n " << max_page << " out of " << num_pages_ << " pages=[\n";
  for (auto page = pages_.begin(); (page != pages_.end()) && (max_page > 0); ++page) {
    ss << "{" << page->DebugString() << "}";
    max_page--;
    if (max_page > 0) ss << ",\n";
  }
  ss << "]";
  return ss.str();
}

void BufferedTupleStream::Page::AttachBufferToBatch(
    BufferedTupleStream* parent, RowBatch* batch, FlushMode flush) {
  DCHECK(is_pinned());
  DCHECK(retrieved_buffer.Load());
  parent->bytes_pinned_ -= len();
  // ExtractBuffer() cannot fail because the buffer is already in memory.
  BufferPool::BufferHandle buffer;
  Status status =
      parent->buffer_pool_->ExtractBuffer(parent->buffer_pool_client_, &handle, &buffer);
  DCHECK(status.ok());
  batch->AddBuffer(parent->buffer_pool_client_, move(buffer), flush);
  attached_to_output_batch = true;
}

string BufferedTupleStream::Page::DebugString() const {
  return Substitute("$0 num_rows=$1 retrieved_buffer=$2 attached_to_output_batch=$3",
      handle.DebugString(), num_rows, retrieved_buffer.Load(), attached_to_output_batch);
}

Status BufferedTupleStream::Init(const string& caller_label, bool pinned) {
  if (!pinned) RETURN_IF_ERROR(UnpinStream(UNPIN_ALL_EXCEPT_CURRENT));
  caller_label_ = caller_label;
  return Status::OK();
}

Status BufferedTupleStream::PrepareForWrite(bool* got_reservation) {
  // This must be the first iterator created.
  DCHECK(pages_.empty());
  DCHECK(!read_it_.attach_on_read_);
  DCHECK(!has_write_iterator());
  DCHECK(!has_read_iterator());
  CHECK_CONSISTENCY_FULL(read_it_);

  *got_reservation = buffer_pool_client_->IncreaseReservationToFit(default_page_len_);
  if (!*got_reservation) return Status::OK();
  has_write_iterator_ = true;
  // Save reservation for the write iterators.
  buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
  CHECK_CONSISTENCY_FULL(read_it_);
  return Status::OK();
}

Status BufferedTupleStream::PrepareForReadWrite(
    bool attach_on_read, bool* got_reservation) {
  // This must be the first iterator created.
  DCHECK(pages_.empty());
  DCHECK(!read_it_.attach_on_read_);
  DCHECK(!has_write_iterator());
  DCHECK(!has_read_iterator());
  CHECK_CONSISTENCY_FULL(read_it_);

  *got_reservation = buffer_pool_client_->IncreaseReservationToFit(2 * default_page_len_);
  if (!*got_reservation) return Status::OK();
  has_write_iterator_ = true;
  // Save reservation for both the read and write iterators.
  buffer_pool_client_->SaveReservation(&read_page_reservation_, default_page_len_);
  buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
  RETURN_IF_ERROR(PrepareForReadInternal(attach_on_read, &read_it_));
  return Status::OK();
}

void BufferedTupleStream::Close(RowBatch* batch, FlushMode flush) {
  for (Page& page : pages_) {
    if (page.attached_to_output_batch) continue; // Already returned.
    if (batch != nullptr && page.retrieved_buffer.Load()) {
      // Subtle: We only need to attach buffers from pages that we may have returned
      // references to.
      page.AttachBufferToBatch(this, batch, flush);
    } else {
      buffer_pool_->DestroyPage(buffer_pool_client_, &page.handle);
    }
  }
  read_page_reservation_.Close();
  large_read_page_reservation_.Close();
  write_page_reservation_.Close();
  pages_.clear();
  num_pages_ = 0;
  bytes_pinned_ = 0;
  bytes_unpinned_ = 0;
  closed_ = true;
}

int64_t BufferedTupleStream::CalcBytesPinned() const {
  int64_t result = 0;
  for (const Page& page : pages_) {
    if (!page.attached_to_output_batch) result += page.pin_count() * page.len();
  }
  return result;
}

Status BufferedTupleStream::PinPage(Page* page) {
  RETURN_IF_ERROR(buffer_pool_->Pin(buffer_pool_client_, &page->handle));
  bytes_pinned_ += page->len();
  bytes_unpinned_ -= page->len();
  DCHECK_GE(bytes_unpinned_, 0);
  return Status::OK();
}

int BufferedTupleStream::ExpectedPinCount(bool stream_pinned, const Page* page) const {
  DCHECK(!page->attached_to_output_batch);
  return (stream_pinned || is_read_page(page) || is_write_page(page)) ? 1 : 0;
}

Status BufferedTupleStream::PinPageIfNeeded(Page* page, bool stream_pinned) {
  int new_pin_count = ExpectedPinCount(stream_pinned, page);
  if (new_pin_count != page->pin_count()) {
    DCHECK_EQ(new_pin_count, page->pin_count() + 1);
    RETURN_IF_ERROR(PinPage(page));
  }
  return Status::OK();
}

void BufferedTupleStream::UnpinPageIfNeeded(Page* page, bool stream_pinned) {
  int new_pin_count = ExpectedPinCount(stream_pinned, page);
  if (new_pin_count != page->pin_count()) {
    DCHECK_EQ(new_pin_count, page->pin_count() - 1);
    buffer_pool_->Unpin(buffer_pool_client_, &page->handle);
    bytes_pinned_ -= page->len();
    DCHECK_GE(bytes_pinned_, 0);
    bytes_unpinned_ += page->len();
    if (page->pin_count() == 0) page->retrieved_buffer.Store(false);
  }
}

bool BufferedTupleStream::NeedWriteReservation() const {
  return NeedWriteReservation(pinned_);
}

bool BufferedTupleStream::NeedWriteReservation(bool stream_pinned) const {
  return NeedWriteReservation(stream_pinned, num_pages_, has_write_iterator(),
      write_page_ != nullptr, has_read_write_page());
}

bool BufferedTupleStream::NeedWriteReservation(bool stream_pinned, int64_t num_pages,
    bool has_write_iterator, bool has_write_page, bool has_read_write_page) {
  if (!has_write_iterator) return false;
  // If the stream is empty the write reservation hasn't been used yet.
  if (num_pages == 0) return true;
  if (stream_pinned) {
    // Make sure we've saved the write reservation for the next page if the only
    // page is a read/write page.
    return has_read_write_page && num_pages == 1;
  } else {
    // Make sure we've saved the write reservation if it's not being used to pin
    // a page in the stream.
    return !has_write_page || has_read_write_page;
  }
}

bool BufferedTupleStream::NeedReadReservation() const {
  return NeedReadReservation(pinned_);
}

bool BufferedTupleStream::NeedReadReservation(bool stream_pinned) const {
  return NeedReadReservation(stream_pinned, num_pages_, has_read_iterator(),
      read_it_.read_page_ != pages_.end());
}

bool BufferedTupleStream::NeedReadReservation(bool stream_pinned, int64_t num_pages,
    bool has_read_iterator, bool has_read_page) const {
  return NeedReadReservation(stream_pinned, num_pages, has_read_iterator, has_read_page,
      has_write_iterator(), write_page_ != nullptr);
}

bool BufferedTupleStream::NeedReadReservation(bool stream_pinned, int64_t num_pages,
    bool has_read_iterator, bool has_read_page, bool has_write_iterator,
    bool has_write_page) {
  if (!has_read_iterator) return false;
  if (stream_pinned) {
    // Need reservation if there are no pages currently pinned for reading but we may add
    // a page.
    return num_pages == 0 && has_write_iterator;
  } else {
    // Only need to save reservation for an unpinned stream if there is no read page
    // and we may advance to one in the future.
    return (has_write_iterator || num_pages > 0) && !has_read_page;
  }
}

Status BufferedTupleStream::NewWritePage(int64_t page_len) noexcept {
  DCHECK(!closed_);
  DCHECK(write_page_ == nullptr);

  Page new_page;
  const BufferHandle* write_buffer;
  RETURN_IF_ERROR(buffer_pool_->CreatePage(
      buffer_pool_client_, page_len, &new_page.handle, &write_buffer));
  bytes_pinned_ += page_len;
  total_byte_size_ += page_len;

  pages_.push_back(std::move(new_page));
  ++num_pages_;
  write_page_ = &pages_.back();
  DCHECK_EQ(write_page_->num_rows, 0);
  write_ptr_ = write_buffer->data();
  write_end_ptr_ = write_ptr_ + page_len;
  return Status::OK();
}

Status BufferedTupleStream::CalcPageLenForRow(int64_t row_size, int64_t* page_len) {
  if (UNLIKELY(row_size > max_page_len_)) {
    return Status(TErrorCode::MAX_ROW_SIZE,
        PrettyPrinter::Print(row_size, TUnit::BYTES), caller_label_,
        PrettyPrinter::Print(state_->query_options().max_row_size, TUnit::BYTES));
  }
  *page_len = max(default_page_len_, BitUtil::RoundUpToPowerOfTwo(row_size));
  return Status::OK();
}

Status BufferedTupleStream::AdvanceWritePage(
    int64_t row_size, bool* got_reservation) noexcept {
  DCHECK(has_write_iterator());
  CHECK_CONSISTENCY_FAST(read_it_);

  int64_t page_len;
  RETURN_IF_ERROR(CalcPageLenForRow(row_size, &page_len));

  // Reservation may have been saved for the next write page, e.g. by PrepareForWrite()
  // if the stream is empty.
  int64_t write_reservation_to_restore = 0, read_reservation_to_restore = 0;
  if (NeedWriteReservation(
          pinned_, num_pages_, true, write_page_ != nullptr, has_read_write_page())
      && !NeedWriteReservation(pinned_, num_pages_ + 1, true, true, false)) {
    write_reservation_to_restore = default_page_len_;
  }
  // If the stream is pinned, we need to keep the previous write page pinned for reading.
  // Check if we saved reservation for this case.
  if (NeedReadReservation(pinned_, num_pages_, has_read_iterator(),
          read_it_.read_page_ != pages_.end(), true, write_page_ != nullptr)
      && !NeedReadReservation(pinned_, num_pages_ + 1, has_read_iterator(),
             read_it_.read_page_ != pages_.end(), true, true)) {
    read_reservation_to_restore = default_page_len_;
  }

  // We may reclaim reservation by unpinning a page that was pinned for writing.
  int64_t write_page_reservation_to_reclaim =
      (write_page_ != nullptr && !pinned_ && !has_read_write_page()) ?
      write_page_->len() : 0;
  // Check to see if we can get the reservation before changing the state of the stream.
  if (!buffer_pool_client_->IncreaseReservationToFit(page_len
          - write_reservation_to_restore - read_reservation_to_restore
          - write_page_reservation_to_reclaim)) {
    DCHECK(pinned_ || page_len > default_page_len_)
        << "If the stream is unpinned, this should only fail for large pages";
    CHECK_CONSISTENCY_FAST(read_it_);
    *got_reservation = false;
    return Status::OK();
  }
  if (write_reservation_to_restore > 0) {
    buffer_pool_client_->RestoreReservation(
        &write_page_reservation_, write_reservation_to_restore);
  }
  if (read_reservation_to_restore > 0) {
    buffer_pool_client_->RestoreReservation(
        &read_page_reservation_, read_reservation_to_restore);
  }
  ResetWritePage();
  RETURN_IF_ERROR(NewWritePage(page_len));
  *got_reservation = true;
  return Status::OK();
}

void BufferedTupleStream::ResetWritePage() {
  if (write_page_ == nullptr) return;
  // Unpin the write page if we're reading in unpinned mode.
  Page* prev_write_page = write_page_;
  write_page_ = nullptr;
  write_ptr_ = nullptr;
  write_end_ptr_ = nullptr;

  // May need to decrement pin count now that it's not the write page, depending on
  // the stream's mode.
  UnpinPageIfNeeded(prev_write_page, pinned_);
}

void BufferedTupleStream::InvalidateWriteIterator() {
  if (!has_write_iterator()) return;
  ResetWritePage();
  has_write_iterator_ = false;
  // No more pages will be appended to stream - do not need any write reservation.
  write_page_reservation_.Close();
  // May not need a read reservation once the write iterator is invalidated.
  if (NeedReadReservation(pinned_, num_pages_, has_read_iterator(),
          read_it_.read_page_ != pages_.end(), true, write_page_ != nullptr)
      && !NeedReadReservation(pinned_, num_pages_, has_read_iterator(),
             read_it_.read_page_ != pages_.end(), false, false)) {
    buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
  }
}

void BufferedTupleStream::SaveLargeReadPageReservation() {
  DCHECK(!pinned_);
  if (large_read_page_reservation_.GetReservation() < max_page_len_ - default_page_len_) {
    // Reclaim the reservation for reading the next large page.
    // large_read_page_reservation_ may not be 0 since we might only used some portion of
    // it in reading the previous large page which is smaller than max_page_len_.
    int64_t reservation_to_reclaim = max_page_len_ - default_page_len_
        - large_read_page_reservation_.GetReservation();
    buffer_pool_client_->SaveReservation(&large_read_page_reservation_,
        reservation_to_reclaim);
  }
}

void BufferedTupleStream::RestoreLargeReadPageReservation() {
  DCHECK(!pinned_);
  buffer_pool_client_->RestoreAllReservation(&large_read_page_reservation_);
}

bool BufferedTupleStream::HasLargeReadPageReservation() {
  return large_read_page_reservation_.GetReservation() > 0;
}

Status BufferedTupleStream::NextReadPage(ReadIterator* read_iter) {
  DCHECK(read_iter->is_valid());
  DCHECK(!closed_);
  DCHECK(read_iter == &read_it_ || (pinned_ && !read_iter->attach_on_read_))
      << "External read iterators only support pinned streams with no attach on read "
      << read_iter->DebugString(pages_);
  CHECK_CONSISTENCY_FAST(*read_iter);

  if (read_iter->read_page_ == pages_.end()) {
    // No rows read yet - start reading at first page. If the stream is unpinned, we can
    // use the reservation saved in PrepareForReadWrite() to pin the first page.
    read_iter->SetReadPage(pages_.begin());
    if (NeedReadReservation(pinned_, num_pages_, true, false)
        && !NeedReadReservation(pinned_, num_pages_, true, true)) {
      buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
    }
  } else if (read_iter->attach_on_read_) {
    DCHECK(read_iter->read_page_ == pages_.begin())
        << read_iter->read_page_->DebugString() << " " << DebugString();
    DCHECK_NE(&*read_iter->read_page_, write_page_);
    DCHECK(read_iter->read_page_->attached_to_output_batch);
    pages_.pop_front();
    --num_pages_;
    read_iter->SetReadPage(pages_.begin());
  } else {
    // Unpin pages after reading them if needed.
    Page* prev_read_page = &*read_iter->read_page_;
    read_iter->AdvanceReadPage(pages_);
    UnpinPageIfNeeded(prev_read_page, pinned_);
  }

  if (read_iter->read_page_ == pages_.end()) {
    CHECK_CONSISTENCY_FULL(*read_iter);
    return Status::OK();
  }

  int64_t read_page_len = read_iter->read_page_->len();
  if (!pinned_ && read_page_len > default_page_len_) {
    // If we are iterating over an unpinned stream and encounter a page that is larger
    // than the default page length, then unpinning the previous page may not have
    // freed up enough reservation to pin the next one. Try to restore some extra saved
    // reservation for reading a large page.
    int64_t needed_reservation = read_page_len - default_page_len_;
    if (large_read_page_reservation_.GetReservation() >= needed_reservation) {
      buffer_pool_client_->RestoreReservation(&large_read_page_reservation_,
          needed_reservation);
    }
    if (buffer_pool_client_->GetUnusedReservation() < read_page_len) {
      // Still failed to get enough unused reservation. The client is responsible for
      // ensuring the reservation is available, so this indicates a bug.
      return Status(TErrorCode::INTERNAL_ERROR, Substitute("Internal error: couldn't pin "
          "large page of $0 bytes, client only had $1 bytes of unused reservation:\n$2",
          read_page_len, buffer_pool_client_->GetUnusedReservation(),
          buffer_pool_client_->DebugString()));
    }
  }
  // Ensure the next page is pinned for reading. By this point we should have enough
  // reservation to pin the page. If the stream is pinned, the page is already pinned.
  // If the stream is unpinned, we freed up enough memory for a default-sized page by
  // deleting or unpinning the previous page and ensured that, if the page was larger,
  // that the reservation is available with the above check.
  RETURN_IF_ERROR(PinPageIfNeeded(&*read_iter->read_page_, pinned_));
  RETURN_IF_ERROR(read_iter->InitReadPtrs());

  // We may need to save reservation for the write page in the case when the write page
  // became a read/write page.
  if (!NeedWriteReservation(pinned_, num_pages_, has_write_iterator(),
             write_page_ != nullptr, false)
      && NeedWriteReservation(pinned_, num_pages_, has_write_iterator(),
             write_page_ != nullptr, has_read_write_page())) {
    buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
  }
  CHECK_CONSISTENCY_FAST(*read_iter);
  return Status::OK();
}

void BufferedTupleStream::InvalidateReadIterator() {
  int64_t rows_returned = read_it_.rows_returned();
  if (read_it_.read_page_ != pages_.end()) {
    // Unpin the write page if we're reading in unpinned mode.
    Page* prev_read_page = &*read_it_.read_page_;
    read_it_.Reset(&pages_);

    // May need to decrement pin count after destroying read iterator.
    UnpinPageIfNeeded(prev_read_page, pinned_);
  } else {
    read_it_.Reset(&pages_);
  }
  if (read_page_reservation_.GetReservation() > 0) {
    buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
  }
  // It is safe to re-read an attach-on-read stream if no rows were read and no pages
  // were therefore deleted.
  DCHECK(read_it_.attach_on_read_ == false || rows_returned == 0);
  if (rows_returned == 0 && read_it_.attach_on_read_) {
    read_it_.attach_on_read_ = false;
  }
}

void BufferedTupleStream::DoneWriting() {
  CHECK_CONSISTENCY_FULL(read_it_);
  InvalidateWriteIterator();
}

Status BufferedTupleStream::PrepareForRead(bool attach_on_read, bool* got_reservation) {
  CHECK_CONSISTENCY_FULL(read_it_);
  InvalidateWriteIterator();
  InvalidateReadIterator();
  // If already pinned, no additional pin is needed (see ExpectedPinCount()).
  *got_reservation = pinned_ || pages_.empty()
      || buffer_pool_client_->IncreaseReservationToFit(default_page_len_);
  if (!*got_reservation) return Status::OK();
  return PrepareForReadInternal(attach_on_read, &read_it_);
}

Status BufferedTupleStream::PrepareForPinnedRead(ReadIterator* iter) {
  DCHECK(pinned_) << "Can only read pinned stream with external iterator";
  DCHECK(!has_write_iterator());
  return PrepareForReadInternal(false, iter);
}

Status BufferedTupleStream::PrepareForReadInternal(
    bool attach_on_read, ReadIterator* read_iter) {
  DCHECK(!closed_);
  DCHECK(!read_it_.attach_on_read_);
  DCHECK(!read_iter->is_valid());

  read_iter->Init(attach_on_read);
  if (pages_.empty()) {
    // No rows to return, or a the first read/write page has not yet been allocated.
    read_iter->SetReadPage(pages_.end());
  } else {
    // Eagerly pin the first page in the stream.
    read_iter->SetReadPage(pages_.begin());
    if (read_iter == &read_it_ && !pinned_
        && read_iter->read_page_->len() > default_page_len_) {
      int64_t extra_needed_reservation = read_iter->read_page_->len() - default_page_len_;
      if (large_read_page_reservation_.GetReservation() >= extra_needed_reservation) {
        buffer_pool_client_->RestoreReservation(&large_read_page_reservation_,
            extra_needed_reservation);
      }
    }
    // Check if we need to increment the pin count of the read page.
    RETURN_IF_ERROR(PinPageIfNeeded(&*read_iter->read_page_, pinned_));
    DCHECK(read_iter->read_page_->is_pinned());
    RETURN_IF_ERROR(read_iter->InitReadPtrs());
  }
  CHECK_CONSISTENCY_FULL(*read_iter);
  return Status::OK();
}

Status BufferedTupleStream::PinStream(bool* pinned) {
  DCHECK(!closed_);
  CHECK_CONSISTENCY_FULL(read_it_);
  if (pinned_) {
    *pinned = true;
    return Status::OK();
  }
  *pinned = false;
  // First, make sure we have the reservation to pin all the pages for reading.
  int64_t bytes_to_pin = 0;
  for (Page& page : pages_) {
    bytes_to_pin += (ExpectedPinCount(true, &page) - page.pin_count()) * page.len();
  }

  // Check if we have some reservation to restore.
  bool restore_write_reservation =
      NeedWriteReservation(false) && !NeedWriteReservation(true);
  bool restore_read_reservation =
      NeedReadReservation(false) && !NeedReadReservation(true);
  int64_t increase_needed = bytes_to_pin
      - (restore_write_reservation ? default_page_len_ : 0)
      - (restore_read_reservation ? default_page_len_ : 0);
  bool reservation_granted =
      buffer_pool_client_->IncreaseReservationToFit(increase_needed);
  if (!reservation_granted) return Status::OK();

  // If there is no current write page we should have some saved reservation to use.
  // Only continue saving it if the stream is empty and need it to pin the first page.
  if (restore_write_reservation) {
    buffer_pool_client_->RestoreReservation(&write_page_reservation_, default_page_len_);
  }
  if (restore_read_reservation) {
    buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
  }

  // At this point success is guaranteed - go through to pin the pages we need to pin.
  // If the page data was evicted from memory, the read I/O can happen in parallel
  // because we defer calling GetBuffer() until NextReadPage().
  for (Page& page : pages_) RETURN_IF_ERROR(PinPageIfNeeded(&page, true));

  pinned_ = true;
  *pinned = true;
  CHECK_CONSISTENCY_FULL(read_it_);
  return Status::OK();
}

Status BufferedTupleStream::UnpinStream(UnpinMode mode) {
  CHECK_CONSISTENCY_FULL(read_it_);
  DCHECK(!closed_);
  if (mode == UNPIN_ALL) {
    // Invalidate the iterators so they don't keep pages pinned.
    InvalidateWriteIterator();
    InvalidateReadIterator();
  }

  if (pinned_) {
    CHECK_CONSISTENCY_FULL(read_it_);
    bool defer_advancing_read_page = false;
    if (&*read_it_.read_page_ != write_page_ && read_it_.read_page_ != pages_.end()
        && read_it_.read_page_rows_returned_ == read_it_.read_page_->num_rows) {
      if (has_write_iterator_ && read_it_.attach_on_read_
          && (num_pages_ <= 2 || !read_it_.read_page_->attached_to_output_batch)) {
        // In a read-write stream + attach_on_read mode, there are cases where we should
        // NOT advance the read page even though the page has been fully exhausted:
        //
        // 1. Stream has exactly 2 pages: 1 read and 1 write.
        //    NextReadPage() will attempt to save default_page_len_ into write
        //    reservation if the stream ended up with only 1 read/write page after
        //    advancing the read page. This can potentially lead to negative unused
        //    reservation if the reader has not freed the row batch where the read page
        //    buffer is attached to (see IMPALA-10584).
        // 2. Read page buffer has not been attached yet to the output row batch.
        //    The previous GetNext() would not attach the read page buffer to the output
        //    row batch if it was a read-write page (see IMPALA-10714).
        //
        // We defer advancing the read page for these cases until the next GetNext()
        // call by the reader.
        defer_advancing_read_page = true;
      }

      if (!defer_advancing_read_page) {
        RETURN_IF_ERROR(NextReadPage(&read_it_));
      }
    }

    // If the stream was pinned, there may be some remaining pinned pages that should
    // be unpinned at this point.
    DCHECK_EQ(bytes_unpinned_, 0);
    std::list<Page>::iterator it = pages_.begin();
    if (defer_advancing_read_page) {
      // We skip advancing the read page earlier, so the first page must be a read page
      // and the reader has not done reading it. We should keep the first page pinned. The
      // next GetNext() call is the one who will be responsible to unpin the first page.
      DCHECK(read_it_.read_page_ == pages_.begin());
      ++it;
    }
    while (it != pages_.end()) {
      UnpinPageIfNeeded(&(*it), false);
      ++it;
    }

    // Check to see if we need to save some of the reservation we freed up.
    if (!NeedWriteReservation(true) && NeedWriteReservation(false)) {
      buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
    }
    if (!NeedReadReservation(true) && NeedReadReservation(false)) {
      buffer_pool_client_->SaveReservation(&read_page_reservation_, default_page_len_);
    }
    pinned_ = false;
  }
  CHECK_CONSISTENCY_FULL(read_it_);
  return Status::OK();
}

Status BufferedTupleStream::GetNext(RowBatch* batch, bool* eos) {
  return GetNextInternal<false>(&read_it_, batch, eos, nullptr);
}

Status BufferedTupleStream::GetNext(ReadIterator* read_iter, RowBatch* batch, bool* eos) {
  DCHECK(pinned_) << "Stream must remain pinned";
  DCHECK(read_iter->is_valid());
  return GetNextInternal<false>(read_iter, batch, eos, nullptr);
}

Status BufferedTupleStream::GetNext(
    RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
  return GetNextInternal<true>(&read_it_, batch, eos, flat_rows);
}

template <bool FILL_FLAT_ROWS>
Status BufferedTupleStream::GetNextInternal(ReadIterator* RESTRICT read_iter,
    RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
  if (has_nullable_tuple_) {
    return GetNextInternal<FILL_FLAT_ROWS, true>(read_iter, batch, eos, flat_rows);
  } else {
    return GetNextInternal<FILL_FLAT_ROWS, false>(read_iter, batch, eos, flat_rows);
  }
}

template <bool FILL_FLAT_ROWS, bool HAS_NULLABLE_TUPLE>
Status BufferedTupleStream::GetNextInternal(ReadIterator* RESTRICT read_iter,
    RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
  DCHECK(!closed_);
  DCHECK(batch->row_desc()->Equals(*desc_));
  DCHECK(is_pinned() || !FILL_FLAT_ROWS)
      << "FlatRowPtrs are only valid for pinned streams";
  *eos = (read_iter->rows_returned_ == num_rows_);
  if (*eos) return Status::OK();

  if (UNLIKELY(read_iter->read_page_ == pages_.end()
          || read_iter->read_page_rows_returned_ == read_iter->read_page_->num_rows)) {
    if (read_iter->read_page_ != pages_.end() && read_iter->attach_on_read_
        && !read_iter->read_page_->attached_to_output_batch) {
      DCHECK(has_write_iterator());
      // We're in a read-write stream in the case where we're at the end of the read page
      // but the buffer was not attached on the last GetNext() call because the write
      // iterator had not yet advanced.
      read_iter->read_page_->AttachBufferToBatch(this, batch, FlushMode::FLUSH_RESOURCES);
      return Status::OK();
    }
    // Get the next page in the stream (or the first page if read_page_ was not yet
    // initialized.) We need to do this at the beginning of the GetNext() call to ensure
    // the buffer management semantics. NextReadPage() may unpin or delete the buffer
    // backing the rows returned from the *previous* call to GetNext().
    RETURN_IF_ERROR(NextReadPage(read_iter));
  }

  DCHECK(read_iter->is_valid());
  DCHECK(read_iter->read_page_ != pages_.end());
  DCHECK(read_iter->read_page_->is_pinned()) << DebugString();
  DCHECK_GE(read_iter->read_page_rows_returned_, 0);

  int rows_left_in_page = read_iter->GetRowsLeftInPage();
  int rows_to_fill = std::min(batch->capacity() - batch->num_rows(), rows_left_in_page);
  DCHECK_GE(rows_to_fill, 1);
  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->GetRow(batch->num_rows()));

  // Produce tuple rows from the current page and the corresponding position on the
  // null tuple indicator.
  if (FILL_FLAT_ROWS) {
    DCHECK(flat_rows != nullptr);
    DCHECK(!read_iter->attach_on_read_);
    DCHECK_EQ(batch->num_rows(), 0);
    flat_rows->clear();
    flat_rows->reserve(rows_to_fill);
  }

  const uint64_t tuples_per_row = desc_->tuple_descriptors().size();
  // Start reading from the current position in 'read_iter->read_page_'.
  for (int i = 0; i < rows_to_fill; ++i) {
    if (FILL_FLAT_ROWS) {
      flat_rows->push_back(read_iter->read_ptr_);
      DCHECK_EQ(flat_rows->size(), i + 1);
    }
    // Copy the row into the output batch.
    TupleRow* output_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    tuple_row_mem += sizeof(Tuple*) * tuples_per_row;
    UnflattenTupleRow<HAS_NULLABLE_TUPLE>(&read_iter->read_ptr_, output_row);

    // Update string slot ptrs, skipping external strings.
    for (int j = 0; j < inlined_string_slots_.size(); ++j) {
      Tuple* tuple = output_row->GetTuple(inlined_string_slots_[j].first);
      if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
      FixUpStringsForRead(inlined_string_slots_[j].second, read_iter, tuple);
    }

    // Update collection slot ptrs, skipping external collections. We traverse the
    // collection structure in the same order as it was written to the stream, allowing
    // us to infer the data layout based on the length of collections and strings.
    for (int j = 0; j < inlined_coll_slots_.size(); ++j) {
      Tuple* tuple = output_row->GetTuple(inlined_coll_slots_[j].first);
      if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
      FixUpCollectionsForRead(inlined_coll_slots_[j].second, read_iter, tuple);
    }
  }

  batch->CommitRows(rows_to_fill);
  read_iter->IncrRowsReturned(rows_to_fill);
  *eos = (read_iter->rows_returned() == num_rows_);
  if (read_iter->GetRowsLeftInPage() == 0) {
    // No more data in this page. NextReadPage() may need to reuse the reservation
    // currently used for 'read_page_' so we may need to flush resources. When
    // 'attach_on_read_' is true, we're returning the buffer. Otherwise the buffer will
    // be unpinned later but we're returning a reference to the memory so we need to
    // signal to the caller that the resources are going away. Note that if there is a
    // read-write page it is not safe to attach the buffer yet because more rows may be
    // appended to the page.
    if (read_iter->attach_on_read_) {
      if (!has_read_write_page()) {
        // Safe to attach because we already called GetBuffer() in NextReadPage().
        // TODO: always flushing for pinned streams is overkill since we may not need
        // to reuse the reservation immediately. Changing this may require modifying
        // callers of this class.
        read_iter->read_page_->AttachBufferToBatch(
            this, batch, FlushMode::FLUSH_RESOURCES);
      }
    } else if (!pinned_) {
      // Flush resources so that we can safely unpin the page on the next GetNext() call.
      // Note that if this is a read/write page we might not actually do the advance on
      // the next call to GetNext(). In that case the flush is still safe to do.
      batch->MarkFlushResources();
    }
  }
  if (FILL_FLAT_ROWS) DCHECK_EQ(flat_rows->size(), rows_to_fill);
  DCHECK_LE(read_iter->read_ptr_, read_iter->read_end_ptr_);
  return Status::OK();
}

void BufferedTupleStream::FixUpStringsForRead(const vector<SlotDescriptor*>& string_slots,
    ReadIterator* RESTRICT read_iter, Tuple* tuple) {
  DCHECK(tuple != nullptr);
  for (const SlotDescriptor* slot_desc : string_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

    StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
    if (sv->IsSmall()) continue;
    sv->SetPtr(reinterpret_cast<char*>(read_iter->read_ptr_));
    read_iter->AdvanceReadPtr(sv->Len());
  }
}

void BufferedTupleStream::FixUpCollectionsForRead(
    const vector<SlotDescriptor*>& collection_slots, ReadIterator* RESTRICT read_iter,
    Tuple* tuple) {
  DCHECK(tuple != nullptr);
  for (const SlotDescriptor* slot_desc : collection_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

    CollectionValue* cv = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->children_tuple_descriptor();
    int coll_byte_size = cv->num_tuples * item_desc.byte_size();
    cv->ptr = reinterpret_cast<uint8_t*>(read_iter->read_ptr_);
    read_iter->AdvanceReadPtr(coll_byte_size);

    if (!item_desc.HasVarlenSlots()) continue;
    uint8_t* coll_data = cv->ptr;
    for (int i = 0; i < cv->num_tuples; ++i) {
      Tuple* item = reinterpret_cast<Tuple*>(coll_data);
      FixUpStringsForRead(item_desc.string_slots(), read_iter, item);
      FixUpCollectionsForRead(item_desc.collection_slots(), read_iter, item);
      coll_data += item_desc.byte_size();
    }
  }
}

int64_t BufferedTupleStream::ComputeRowSize(TupleRow* row)
    const noexcept {
  int64_t size = 0;
  if (has_nullable_tuple_) {
    size += NullIndicatorBytesPerRow();
    for (int i = 0; i < fixed_tuple_sizes_.size(); ++i) {
      if (row->GetTuple(i) != nullptr) size += fixed_tuple_sizes_[i];
    }
  } else {
    for (int i = 0; i < fixed_tuple_sizes_.size(); ++i) {
      size += fixed_tuple_sizes_[i];
    }
  }
  for (int i = 0; i < inlined_string_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(inlined_string_slots_[i].first);
    if (tuple == nullptr) continue;
    const vector<SlotDescriptor*>& slots = inlined_string_slots_[i].second;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
      if (tuple->IsNull((*it)->null_indicator_offset())) continue;
      StringValue* sv = tuple->GetStringSlot((*it)->tuple_offset());
      if (sv->IsSmall()) continue;
      size += sv->Len();
    }
  }

  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (tuple == nullptr) continue;
    const vector<SlotDescriptor*>& slots = inlined_coll_slots_[i].second;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
      if (tuple->IsNull((*it)->null_indicator_offset())) continue;
      CollectionValue* cv = tuple->GetCollectionSlot((*it)->tuple_offset());
      const TupleDescriptor& item_desc = *(*it)->children_tuple_descriptor();
      size += cv->num_tuples * item_desc.byte_size();

      if (!item_desc.HasVarlenSlots()) continue;
      for (int j = 0; j < cv->num_tuples; ++j) {
        Tuple* item = reinterpret_cast<Tuple*>(&cv->ptr[j * item_desc.byte_size()]);
        size += item->VarlenByteSize(item_desc);
      }
    }
  }
  return size;
}

bool BufferedTupleStream::AddRowSlow(TupleRow* row, Status* status) noexcept {
  // Use AddRowCustom*() to do the work of advancing the page.
  int64_t row_size = ComputeRowSize(row);
  uint8_t* data = AddRowCustomBeginSlow(row_size, status);
  if (data == nullptr) return false;
  bool success = DeepCopy(row, &data, data + row_size);
  DCHECK(success);
  DCHECK_EQ(data, write_ptr_);
  AddRowCustomEnd(row_size);
  return true;
}

uint8_t* BufferedTupleStream::AddRowCustomBeginSlow(
    int64_t size, Status* status) noexcept {
  bool got_reservation;
  *status = AdvanceWritePage(size, &got_reservation);
  if (!status->ok() || !got_reservation) return nullptr;

  // We have a large-enough page so now success is guaranteed.
  uint8_t* result = AddRowCustomBegin(size, status);
  DCHECK(result != nullptr);
  return result;
}

void BufferedTupleStream::AddLargeRowCustomEnd(int64_t size) noexcept {
  DCHECK_GT(size, default_page_len_);
  // Immediately unpin the large write page so that we're not using up extra reservation
  // and so we don't append another row to the page.
  ResetWritePage();
  // Save some of the reservation we freed up so we can create the next write page when
  // needed.
  if (NeedWriteReservation()) {
    buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
  }
  // The stream should be in a consistent state once the row is added.
  CHECK_CONSISTENCY_FAST(read_it_);
}

bool BufferedTupleStream::AddRow(TupleRow* row, Status* status) noexcept {
  DCHECK(!closed_);
  DCHECK(has_write_iterator());
  if (UNLIKELY(write_page_ == nullptr || !DeepCopy(row, &write_ptr_, write_end_ptr_))) {
    return AddRowSlow(row, status);
  }
  ++num_rows_;
  ++write_page_->num_rows;
  return true;
}

bool BufferedTupleStream::DeepCopy(
    TupleRow* row, uint8_t** data, const uint8_t* data_end) noexcept {
  return has_nullable_tuple_ ? DeepCopyInternal<true>(row, data, data_end) :
                               DeepCopyInternal<false>(row, data, data_end);
}

// TODO: consider codegening this.
// TODO: in case of duplicate tuples, this can redundantly serialize data.
template <bool HAS_NULLABLE_TUPLE>
bool BufferedTupleStream::DeepCopyInternal(
    TupleRow* row, uint8_t** data, const uint8_t* data_end) noexcept {
  uint8_t* pos = *data;
  const uint64_t tuples_per_row = desc_->tuple_descriptors().size();
  // Copy the not NULL fixed len tuples. For the NULL tuples just update the NULL tuple
  // indicator.
  if (HAS_NULLABLE_TUPLE) {
    int null_indicator_bytes = NullIndicatorBytesPerRow();
    if (UNLIKELY(pos + null_indicator_bytes > data_end)) return false;
    uint8_t* null_indicators = pos;
    pos += NullIndicatorBytesPerRow();
    memset(null_indicators, 0, null_indicator_bytes);
    for (int i = 0; i < tuples_per_row; ++i) {
      uint8_t* null_word = null_indicators + (i >> 3);
      const uint32_t null_pos = i & 7;
      const int tuple_size = fixed_tuple_sizes_[i];
      Tuple* t = row->GetTuple(i);
      const uint8_t mask = 1 << (7 - null_pos);
      if (t != nullptr) {
        if (UNLIKELY(pos + tuple_size > data_end)) return false;
        memcpy(pos, t, tuple_size);
        pos += tuple_size;
      } else {
        *null_word |= mask;
      }
    }
  } else {
    // If we know that there are no nullable tuples no need to set the nullability flags.
    for (int i = 0; i < tuples_per_row; ++i) {
      const int tuple_size = fixed_tuple_sizes_[i];
      if (UNLIKELY(pos + tuple_size > data_end)) return false;
      Tuple* t = row->GetTuple(i);
      // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
      // is delivered, the check below should become DCHECK(t != nullptr).
      DCHECK(t != nullptr || tuple_size == 0);
      memcpy(pos, t, tuple_size);
      pos += tuple_size;
    }
  }

  // Copy inlined string slots. Note: we do not need to convert the string ptrs to offsets
  // on the write path, only on the read. The tuple data is immediately followed
  // by the string data so only the len information is necessary.
  for (int i = 0; i < inlined_string_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_string_slots_[i].first);
    if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
    if (UNLIKELY(!CopyStrings(tuple, inlined_string_slots_[i].second, &pos, data_end)))
      return false;
  }

  // Copy inlined collection slots. We copy collection data in a well-defined order so
  // we do not need to convert pointers to offsets on the write path.
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
    if (UNLIKELY(!CopyCollections(tuple, inlined_coll_slots_[i].second, &pos, data_end)))
      return false;
  }
  *data = pos;
  return true;
}

bool BufferedTupleStream::CopyStrings(const Tuple* tuple,
    const vector<SlotDescriptor*>& string_slots, uint8_t** data, const uint8_t* data_end) {
  for (const SlotDescriptor* slot_desc : string_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
    if (LIKELY(sv->Len() > 0) && !sv->IsSmall()) {
      StringValue::SimpleString s = sv->ToSimpleString();
      if (UNLIKELY(*data + s.len > data_end)) return false;

      memcpy(*data, s.ptr, s.len);
      *data += s.len;
    }
  }
  return true;
}

bool BufferedTupleStream::CopyCollections(const Tuple* tuple,
    const vector<SlotDescriptor*>& collection_slots, uint8_t** data, const uint8_t* data_end) {
  for (const SlotDescriptor* slot_desc : collection_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const CollectionValue* cv = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->children_tuple_descriptor();
    if (LIKELY(cv->num_tuples > 0)) {
      int coll_byte_size = cv->num_tuples * item_desc.byte_size();
      if (UNLIKELY(*data + coll_byte_size > data_end)) return false;
      uint8_t* coll_data = *data;
      memcpy(coll_data, cv->ptr, coll_byte_size);
      *data += coll_byte_size;

      if (!item_desc.HasVarlenSlots()) continue;
      // Copy variable length data when present in collection items.
      for (int i = 0; i < cv->num_tuples; ++i) {
        const Tuple* item = reinterpret_cast<Tuple*>(coll_data);
        if (UNLIKELY(!CopyStrings(item, item_desc.string_slots(), data, data_end))) {
          return false;
        }
        if (UNLIKELY(
                !CopyCollections(item, item_desc.collection_slots(), data, data_end))) {
          return false;
        }
        coll_data += item_desc.byte_size();
      }
    }
  }
  return true;
}

void BufferedTupleStream::ReadIterator::Reset(std::list<Page>* pages) {
  valid_ = false;
  read_page_ = pages->end();
  rows_returned_ = 0;
  read_page_rows_returned_ = -1;
  read_ptr_ = nullptr;
  read_end_ptr_ = nullptr;
}

void BufferedTupleStream::ReadIterator::Init(bool attach_on_read) {
  valid_ = true;
  rows_returned_ = 0;
  DCHECK(!attach_on_read_) << "attach_on_read can only be set once";
  // Only set 'attach_on_read' if needed. Otherwise, if this is the builtin
  // iterator, a benign data race may be flagged by TSAN (see IMPALA-9701).
  if (attach_on_read) attach_on_read_ = attach_on_read;
}

void BufferedTupleStream::ReadIterator::SetReadPage(list<Page>::iterator read_page) {
  read_page_ = read_page;
  read_ptr_ = nullptr;
  read_end_ptr_ = nullptr;
  read_page_rows_returned_ = 0;
}

void BufferedTupleStream::ReadIterator::AdvanceReadPage(const list<Page>& pages) {
  DCHECK(read_page_ != pages.end());
  ++read_page_;
  read_ptr_ = nullptr;
  read_end_ptr_ = nullptr;
  read_page_rows_returned_ = 0;
}

Status BufferedTupleStream::ReadIterator::InitReadPtrs() {
  DCHECK(read_page_->is_pinned());
  DCHECK_EQ(read_page_rows_returned_, 0);
  const BufferHandle* read_buffer;
  RETURN_IF_ERROR(read_page_->GetBuffer(&read_buffer));
  read_ptr_ = read_buffer->data();
  read_end_ptr_ = read_ptr_ + read_buffer->len();
  return Status::OK();
}

void BufferedTupleStream::ReadIterator::IncrRowsReturned(int64_t rows) {
  rows_returned_ += rows;
  read_page_rows_returned_ += rows;
}

string BufferedTupleStream::ReadIterator::DebugString(const list<Page>& pages) const {
  stringstream ss;
  ss << "{valid=" << valid_ << " attach_on_read=" << attach_on_read_ << " read_page=";
  if (read_page_ == pages.end()) {
    ss << "<end>";
  } else {
    ss << read_page_->DebugString();
  }
  ss << " read_page_rows_returned=" << read_page_rows_returned_
     << " rows_returned=" << rows_returned_
     << " read_ptr_=" << static_cast<const void*>(read_ptr_)
     << " read_end_ptr_=" << static_cast<const void*>(read_end_ptr_) << "}";
  return ss.str();
}
