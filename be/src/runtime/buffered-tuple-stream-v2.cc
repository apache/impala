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

#include "runtime/buffered-tuple-stream-v2.inline.h"

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
#include "util/runtime-profile-counters.h"

#include "common/names.h"

#ifdef NDEBUG
#define CHECK_CONSISTENCY()
#else
#define CHECK_CONSISTENCY() CheckConsistency()
#endif

using namespace impala;
using namespace strings;

BufferedTupleStreamV2::BufferedTupleStreamV2(RuntimeState* state,
    const RowDescriptor& row_desc, BufferPool::ClientHandle* buffer_pool_client,
    int64_t page_len, const set<SlotId>& ext_varlen_slots)
  : state_(state),
    desc_(row_desc),
    buffer_pool_(state->exec_env()->buffer_pool()),
    buffer_pool_client_(buffer_pool_client),
    total_byte_size_(0),
    read_page_rows_returned_(-1),
    read_ptr_(nullptr),
    write_ptr_(nullptr),
    write_end_ptr_(nullptr),
    rows_returned_(0),
    write_page_(nullptr),
    bytes_pinned_(0),
    num_rows_(0),
    page_len_(page_len),
    has_nullable_tuple_(row_desc.IsAnyTupleNullable()),
    delete_on_read_(false),
    closed_(false),
    pinned_(true) {
  read_page_ = pages_.end();
  fixed_tuple_row_size_ = 0;
  for (int i = 0; i < desc_.tuple_descriptors().size(); ++i) {
    const TupleDescriptor* tuple_desc = desc_.tuple_descriptors()[i];
    const int tuple_byte_size = tuple_desc->byte_size();
    fixed_tuple_sizes_.push_back(tuple_byte_size);
    fixed_tuple_row_size_ += tuple_byte_size;

    vector<SlotDescriptor*> tuple_string_slots;
    vector<SlotDescriptor*> tuple_coll_slots;
    for (int j = 0; j < tuple_desc->slots().size(); ++j) {
      SlotDescriptor* slot = tuple_desc->slots()[j];
      if (!slot->type().IsVarLenType()) continue;
      if (ext_varlen_slots.find(slot->id()) == ext_varlen_slots.end()) {
        if (slot->type().IsVarLenStringType()) {
          tuple_string_slots.push_back(slot);
        } else {
          DCHECK(slot->type().IsCollectionType());
          tuple_coll_slots.push_back(slot);
        }
      }
    }
    if (!tuple_string_slots.empty()) {
      inlined_string_slots_.push_back(make_pair(i, tuple_string_slots));
    }

    if (!tuple_coll_slots.empty()) {
      inlined_coll_slots_.push_back(make_pair(i, tuple_coll_slots));
    }
  }
  if (has_nullable_tuple_) fixed_tuple_row_size_ += NullIndicatorBytesPerRow();
}

BufferedTupleStreamV2::~BufferedTupleStreamV2() {
  DCHECK(closed_);
}

void BufferedTupleStreamV2::CheckConsistency() const {
  DCHECK_EQ(bytes_pinned_, CalcBytesPinned()) << DebugString();
  for (const Page& page : pages_) {
    DCHECK_EQ(ExpectedPinCount(pinned_, &page), page.pin_count()) << DebugString();
  }
  if (has_write_iterator()) {
    DCHECK(write_page_->is_pinned());
    DCHECK_GE(write_ptr_, write_page_->data());
    DCHECK_EQ(write_end_ptr_, write_page_->data() + write_page_->len());
    DCHECK_GE(write_end_ptr_, write_ptr_);
  }
  if (has_read_iterator()) {
    DCHECK(read_page_->is_pinned());
    uint8_t* read_end_ptr = read_page_->data() + read_page_->len();
    DCHECK_GE(read_ptr_, read_page_->data());
    DCHECK_GE(read_end_ptr, read_ptr_);
  }
}

string BufferedTupleStreamV2::DebugString() const {
  stringstream ss;
  ss << "BufferedTupleStreamV2 num_rows=" << num_rows_
     << " rows_returned=" << rows_returned_ << " pinned=" << pinned_
     << " delete_on_read=" << delete_on_read_ << " closed=" << closed_
     << " bytes_pinned=" << bytes_pinned_ << " write_page=" << write_page_
     << " read_page=";
  if (!has_read_iterator()) {
    ss << "<end>";
  } else {
    ss << &*read_page_;
  }
  ss << " pages=[\n";
  for (const Page& page : pages_) {
    ss << "{" << page.DebugString() << "}";
    if (&page != &pages_.back()) ss << ",\n";
  }
  ss << "]";
  return ss.str();
}

string BufferedTupleStreamV2::Page::DebugString() const {
  return Substitute("$0 num_rows=$1", handle.DebugString(), num_rows);
}

Status BufferedTupleStreamV2::Init(int node_id, bool pinned) {
  if (!pinned) UnpinStream(UNPIN_ALL_EXCEPT_CURRENT);
  return Status::OK();
}

Status BufferedTupleStreamV2::PrepareForWrite(bool* got_reservation) {
  // This must be the first iterator created.
  DCHECK(pages_.empty());
  DCHECK(!delete_on_read_);
  DCHECK(!has_write_iterator());
  DCHECK(!has_read_iterator());
  CHECK_CONSISTENCY();

  RETURN_IF_ERROR(CheckPageSizeForRow(fixed_tuple_row_size_));
  *got_reservation = buffer_pool_client_->IncreaseReservationToFit(page_len_);
  if (!*got_reservation) return Status::OK();
  RETURN_IF_ERROR(NewWritePage());
  CHECK_CONSISTENCY();
  return Status::OK();
}

Status BufferedTupleStreamV2::PrepareForReadWrite(
    bool delete_on_read, bool* got_reservation) {
  // This must be the first iterator created.
  DCHECK(pages_.empty());
  DCHECK(!delete_on_read_);
  DCHECK(!has_write_iterator());
  DCHECK(!has_read_iterator());
  CHECK_CONSISTENCY();

  RETURN_IF_ERROR(CheckPageSizeForRow(fixed_tuple_row_size_));
  *got_reservation = buffer_pool_client_->IncreaseReservationToFit(2 * page_len_);
  if (!*got_reservation) return Status::OK();
  RETURN_IF_ERROR(NewWritePage());
  RETURN_IF_ERROR(PrepareForReadInternal(delete_on_read));
  return Status::OK();
}

void BufferedTupleStreamV2::Close(RowBatch* batch, RowBatch::FlushMode flush) {
  for (Page& page : pages_) {
    if (batch != nullptr && page.is_pinned()) {
      BufferPool::BufferHandle buffer;
      buffer_pool_->ExtractBuffer(buffer_pool_client_, &page.handle, &buffer);
      batch->AddBuffer(buffer_pool_client_, move(buffer), flush);
    } else {
      buffer_pool_->DestroyPage(buffer_pool_client_, &page.handle);
    }
  }
  pages_.clear();
  bytes_pinned_ = 0;
  closed_ = true;
}

int64_t BufferedTupleStreamV2::CalcBytesPinned() const {
  int64_t result = 0;
  for (const Page& page : pages_) result += page.pin_count() * page.len();
  return result;
}

Status BufferedTupleStreamV2::PinPage(Page* page) {
  RETURN_IF_ERROR(buffer_pool_->Pin(buffer_pool_client_, &page->handle));
  bytes_pinned_ += page->len();
  return Status::OK();
}

int BufferedTupleStreamV2::ExpectedPinCount(bool stream_pinned, const Page* page) const {
  int pin_count = 0;
  if (stream_pinned && has_write_iterator() && has_read_iterator()) {
    // The stream is pinned, so all pages have a pin for that (and this pin will be used
    // as the read iterator when the stream is unpinned)
    pin_count++;
    // The write iterator gets it's own pin so that we can unpin the stream without
    // needing additional reservation.
    if (is_write_page(page)) pin_count++;
  } else if (stream_pinned) {
    // The stream is pinned and only has one iterator. When it's unpinned, either the read
    // or write iterator can use this pin count.
    pin_count++;
  } else {
    // The stream is unpinned. Each iterator gets a pin count.
    if (is_read_page(page)) pin_count++;
    if (is_write_page(page)) pin_count++;
  }
  return pin_count;
}

Status BufferedTupleStreamV2::PinPageIfNeeded(Page* page, bool stream_pinned) {
  int new_pin_count = ExpectedPinCount(stream_pinned, page);
  if (new_pin_count != page->pin_count()) {
    DCHECK_EQ(new_pin_count, page->pin_count() + 1);
    RETURN_IF_ERROR(PinPage(page));
  }
  return Status::OK();
}

void BufferedTupleStreamV2::UnpinPageIfNeeded(Page* page, bool stream_pinned) {
  int new_pin_count = ExpectedPinCount(stream_pinned, page);
  if (new_pin_count != page->pin_count()) {
    DCHECK_EQ(new_pin_count, page->pin_count() - 1);
    buffer_pool_->Unpin(buffer_pool_client_, &page->handle);
    bytes_pinned_ -= page->len();
  }
}

Status BufferedTupleStreamV2::NewWritePage() noexcept {
  DCHECK(!closed_);
  DCHECK(!has_write_iterator());

  Page new_page;
  RETURN_IF_ERROR(
      buffer_pool_->CreatePage(buffer_pool_client_, page_len_, &new_page.handle));
  bytes_pinned_ += page_len_;
  total_byte_size_ += page_len_;

  pages_.push_back(std::move(new_page));
  write_page_ = &pages_.back();
  DCHECK_EQ(write_page_->num_rows, 0);
  write_ptr_ = write_page_->data();
  write_end_ptr_ = write_page_->data() + page_len_;
  return Status::OK();
}

Status BufferedTupleStreamV2::CheckPageSizeForRow(int64_t row_size) {
  // TODO: IMPALA-3208: need to rework this logic to support large pages - should pick
  // next power-of-two size.
  if (UNLIKELY(row_size > page_len_)) {
    // TODO: IMPALA-3208: change the message to reference the query option controlling
    // max row size.
    return Status(TErrorCode::BTS_BLOCK_OVERFLOW,
        PrettyPrinter::Print(row_size, TUnit::BYTES),
        PrettyPrinter::Print(0, TUnit::BYTES));
  }
  return Status::OK();
}

Status BufferedTupleStreamV2::AdvanceWritePage(
    int64_t row_size, bool* got_reservation) noexcept {
  CHECK_CONSISTENCY();

  // Get ready to move to the next write page by unsetting 'write_page_' and
  // potentially (depending on the mode of this stream) freeing up reservation for the
  // next write page.
  ResetWritePage();

  RETURN_IF_ERROR(CheckPageSizeForRow(row_size));
  // May need to pin the new page for both reading and writing. See ExpectedPinCount();
  bool pin_for_read = has_read_iterator() && pinned_;
  int64_t new_page_reservation = pin_for_read ? 2 * page_len_ : page_len_;
  if (!buffer_pool_client_->IncreaseReservationToFit(new_page_reservation)) {
    *got_reservation = false;
    return Status::OK();
  }
  RETURN_IF_ERROR(NewWritePage());
  // We may need to pin the page for reading also.
  if (pin_for_read) RETURN_IF_ERROR(PinPage(write_page_));

  CHECK_CONSISTENCY();
  *got_reservation = true;
  return Status::OK();
}

void BufferedTupleStreamV2::ResetWritePage() {
  if (!has_write_iterator()) return;
  // Unpin the write page if we're reading in unpinned mode.
  Page* prev_write_page = write_page_;
  write_page_ = nullptr;

  // May need to decrement pin count now that it's not the write page, depending on
  // the stream's mode.
  UnpinPageIfNeeded(prev_write_page, pinned_);
}

Status BufferedTupleStreamV2::NextReadPage() {
  DCHECK(!closed_);
  CHECK_CONSISTENCY();

  if (delete_on_read_) {
    DCHECK(read_page_ == pages_.begin()) << read_page_->DebugString() << " " << DebugString();
    DCHECK_NE(&*read_page_, write_page_);
    bytes_pinned_ -= pages_.front().len();
    buffer_pool_->DestroyPage(buffer_pool_client_, &pages_.front().handle);
    pages_.pop_front();
    read_page_ = pages_.begin();
  } else {
    // Unpin pages after reading them if needed.
    Page* prev_read_page = &*read_page_;
    ++read_page_;
    UnpinPageIfNeeded(prev_read_page, pinned_);
  }

  if (!has_read_iterator()) {
    CHECK_CONSISTENCY();
    return Status::OK();
  }

  // Ensure the next page is pinned for reading. If the stream is unpinned, we freed up
  // enough reservation by deleting or unpinning the previous page.
  // TODO: IMPALA-3208: this page may be larger than the previous, so this could
  // actually fail once we have variable-length pages.
  RETURN_IF_ERROR(PinPageIfNeeded(&*read_page_, pinned_));

  read_page_rows_returned_ = 0;
  read_ptr_ = read_page_->data();

  CHECK_CONSISTENCY();
  return Status::OK();
}

void BufferedTupleStreamV2::ResetReadPage() {
  if (!has_read_iterator()) return;
  // Unpin the write page if we're reading in unpinned mode.
  Page* prev_read_page = &*read_page_;
  read_page_ = pages_.end();

  // May need to decrement pin count after destroying read iterator.
  UnpinPageIfNeeded(prev_read_page, pinned_);
}

Status BufferedTupleStreamV2::PrepareForRead(bool delete_on_read, bool* got_reservation) {
  CHECK_CONSISTENCY();
  ResetWritePage();
  ResetReadPage();
  // If already pinned, no additional pin is needed (see ExpectedPinCount()).
  *got_reservation = pinned_ || buffer_pool_client_->IncreaseReservationToFit(page_len_);
  if (!*got_reservation) return Status::OK();
  return PrepareForReadInternal(delete_on_read);
}

Status BufferedTupleStreamV2::PrepareForReadInternal(bool delete_on_read) {
  DCHECK(!closed_);
  DCHECK(!delete_on_read_);
  DCHECK(!pages_.empty());
  DCHECK(!has_read_iterator());

  // Check if we need to increment the pin count of the read page.
  read_page_ = pages_.begin();
  RETURN_IF_ERROR(PinPageIfNeeded(&*read_page_, pinned_));

  DCHECK(has_read_iterator());
  DCHECK(read_page_->is_pinned());
  read_page_rows_returned_ = 0;
  read_ptr_ = read_page_->data();
  rows_returned_ = 0;
  delete_on_read_ = delete_on_read;
  CHECK_CONSISTENCY();
  return Status::OK();
}

Status BufferedTupleStreamV2::PinStream(bool* pinned) {
  DCHECK(!closed_);
  CHECK_CONSISTENCY();
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
  bool reservation_granted = buffer_pool_client_->IncreaseReservationToFit(bytes_to_pin);
  if (!reservation_granted) return Status::OK();

  // At this point success is guaranteed - go through to pin the pages we need to pin.
  for (Page& page : pages_) RETURN_IF_ERROR(PinPageIfNeeded(&page, true));

  pinned_ = true;
  *pinned = true;
  CHECK_CONSISTENCY();
  return Status::OK();
}

void BufferedTupleStreamV2::UnpinStream(UnpinMode mode) {
  DCHECK(!closed_);
  if (mode == UNPIN_ALL) {
    // Invalidate the iterators so they don't keep pages pinned.
    ResetWritePage();
    ResetReadPage();
  }

  if (pinned_) {
    // If the stream was pinned, there may be some remaining pinned pages that should
    // be unpinned at this point.
    for (Page& page : pages_) UnpinPageIfNeeded(&page, false);
    pinned_ = false;
  }
  CHECK_CONSISTENCY();
}

Status BufferedTupleStreamV2::GetRows(
    MemTracker* tracker, scoped_ptr<RowBatch>* batch, bool* got_rows) {
  if (num_rows() > numeric_limits<int>::max()) {
    // RowBatch::num_rows_ is a 32-bit int, avoid an overflow.
    return Status(Substitute("Trying to read $0 rows into in-memory batch failed. Limit "
                             "is $1",
        num_rows(), numeric_limits<int>::max()));
  }
  RETURN_IF_ERROR(PinStream(got_rows));
  if (!*got_rows) return Status::OK();
  bool got_reservation;
  RETURN_IF_ERROR(PrepareForRead(false, &got_reservation));
  DCHECK(got_reservation) << "Stream was pinned";
  batch->reset(new RowBatch(desc_, num_rows(), tracker));
  bool eos = false;
  // Loop until GetNext fills the entire batch. Each call can stop at page
  // boundaries. We generally want it to stop, so that pages can be freed
  // as we read. It is safe in this case because we pin the entire stream.
  while (!eos) {
    RETURN_IF_ERROR(GetNext(batch->get(), &eos));
  }
  return Status::OK();
}

Status BufferedTupleStreamV2::GetNext(RowBatch* batch, bool* eos) {
  return GetNextInternal<false>(batch, eos, nullptr);
}

Status BufferedTupleStreamV2::GetNext(
    RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
  return GetNextInternal<true>(batch, eos, flat_rows);
}

template <bool FILL_FLAT_ROWS>
Status BufferedTupleStreamV2::GetNextInternal(
    RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
  if (has_nullable_tuple_) {
    return GetNextInternal<FILL_FLAT_ROWS, true>(batch, eos, flat_rows);
  } else {
    return GetNextInternal<FILL_FLAT_ROWS, false>(batch, eos, flat_rows);
  }
}

template <bool FILL_FLAT_ROWS, bool HAS_NULLABLE_TUPLE>
Status BufferedTupleStreamV2::GetNextInternal(
    RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
  DCHECK(!closed_);
  DCHECK(batch->row_desc().Equals(desc_));
  DCHECK(is_pinned() || !FILL_FLAT_ROWS)
      << "FlatRowPtrs are only valid for pinned streams";
  *eos = (rows_returned_ == num_rows_);
  if (*eos) return Status::OK();

  if (UNLIKELY(read_page_rows_returned_ == read_page_->num_rows)) {
    // Get the next page in the stream. We need to do this at the beginning of the
    // GetNext() call to ensure the buffer management semantics. NextReadPage() may
    // unpin or delete the buffer backing the rows returned from the *previous* call
    // to GetNext().
    RETURN_IF_ERROR(NextReadPage());
  }

  DCHECK(has_read_iterator());
  DCHECK(read_page_->is_pinned()) << DebugString();
  DCHECK_GE(read_page_rows_returned_, 0);

  int rows_left_in_page = read_page_->num_rows - read_page_rows_returned_;
  int rows_to_fill = std::min(batch->capacity() - batch->num_rows(), rows_left_in_page);
  DCHECK_GE(rows_to_fill, 1);
  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->GetRow(batch->num_rows()));

  // Produce tuple rows from the current page and the corresponding position on the
  // null tuple indicator.
  if (FILL_FLAT_ROWS) {
    DCHECK(flat_rows != nullptr);
    DCHECK(!delete_on_read_);
    DCHECK_EQ(batch->num_rows(), 0);
    flat_rows->clear();
    flat_rows->reserve(rows_to_fill);
  }

  const uint64_t tuples_per_row = desc_.tuple_descriptors().size();
  // Start reading from the current position in 'read_page_'.
  for (int i = 0; i < rows_to_fill; ++i) {
    if (FILL_FLAT_ROWS) {
      flat_rows->push_back(read_ptr_);
      DCHECK_EQ(flat_rows->size(), i + 1);
    }
    // Copy the row into the output batch.
    TupleRow* output_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    tuple_row_mem += sizeof(Tuple*) * tuples_per_row;
    UnflattenTupleRow<HAS_NULLABLE_TUPLE>(&read_ptr_, output_row);

    // Update string slot ptrs, skipping external strings.
    for (int j = 0; j < inlined_string_slots_.size(); ++j) {
      Tuple* tuple = output_row->GetTuple(inlined_string_slots_[j].first);
      if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
      FixUpStringsForRead(inlined_string_slots_[j].second, tuple);
    }

    // Update collection slot ptrs, skipping external collections. We traverse the
    // collection structure in the same order as it was written to the stream, allowing
    // us to infer the data layout based on the length of collections and strings.
    for (int j = 0; j < inlined_coll_slots_.size(); ++j) {
      Tuple* tuple = output_row->GetTuple(inlined_coll_slots_[j].first);
      if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
      FixUpCollectionsForRead(inlined_coll_slots_[j].second, tuple);
    }
  }

  batch->CommitRows(rows_to_fill);
  rows_returned_ += rows_to_fill;
  read_page_rows_returned_ += rows_to_fill;
  *eos = (rows_returned_ == num_rows_);
  if (read_page_rows_returned_ == read_page_->num_rows && (!pinned_ || delete_on_read_)) {
    // No more data in this page. The batch must be immediately returned up the operator
    // tree and deep copied so that NextReadPage() can reuse the read page's buffer.
    // TODO: IMPALA-4179 - instead attach the buffer and flush the resources.
    batch->MarkNeedsDeepCopy();
  }
  if (FILL_FLAT_ROWS) DCHECK_EQ(flat_rows->size(), rows_to_fill);
  DCHECK_LE(read_ptr_, read_page_->data() + read_page_->len());
  return Status::OK();
}

void BufferedTupleStreamV2::FixUpStringsForRead(
    const vector<SlotDescriptor*>& string_slots, Tuple* tuple) {
  DCHECK(tuple != nullptr);
  for (const SlotDescriptor* slot_desc : string_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

    StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
    DCHECK_LE(read_ptr_ + sv->len, read_page_->data() + read_page_->len());
    sv->ptr = reinterpret_cast<char*>(read_ptr_);
    read_ptr_ += sv->len;
  }
}

void BufferedTupleStreamV2::FixUpCollectionsForRead(
    const vector<SlotDescriptor*>& collection_slots, Tuple* tuple) {
  DCHECK(tuple != nullptr);
  for (const SlotDescriptor* slot_desc : collection_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

    CollectionValue* cv = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    int coll_byte_size = cv->num_tuples * item_desc.byte_size();
    DCHECK_LE(read_ptr_ + coll_byte_size, read_page_->data() + read_page_->len());
    cv->ptr = reinterpret_cast<uint8_t*>(read_ptr_);
    read_ptr_ += coll_byte_size;

    if (!item_desc.HasVarlenSlots()) continue;
    uint8_t* coll_data = cv->ptr;
    for (int i = 0; i < cv->num_tuples; ++i) {
      Tuple* item = reinterpret_cast<Tuple*>(coll_data);
      FixUpStringsForRead(item_desc.string_slots(), item);
      FixUpCollectionsForRead(item_desc.collection_slots(), item);
      coll_data += item_desc.byte_size();
    }
  }
}

int64_t BufferedTupleStreamV2::ComputeRowSize(TupleRow* row) const noexcept {
  int64_t size = 0;
  if (has_nullable_tuple_) {
    size += NullIndicatorBytesPerRow();
    for (int i = 0; i < fixed_tuple_sizes_.size(); ++i) {
      if (row->GetTuple(i) != nullptr) size += fixed_tuple_sizes_[i];
    }
  } else {
    size = fixed_tuple_row_size_;
  }
  for (int i = 0; i < inlined_string_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(inlined_string_slots_[i].first);
    if (tuple == nullptr) continue;
    const vector<SlotDescriptor*>& slots = inlined_string_slots_[i].second;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
      if (tuple->IsNull((*it)->null_indicator_offset())) continue;
      size += tuple->GetStringSlot((*it)->tuple_offset())->len;
    }
  }

  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (tuple == nullptr) continue;
    const vector<SlotDescriptor*>& slots = inlined_coll_slots_[i].second;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
      if (tuple->IsNull((*it)->null_indicator_offset())) continue;
      CollectionValue* cv = tuple->GetCollectionSlot((*it)->tuple_offset());
      const TupleDescriptor& item_desc = *(*it)->collection_item_descriptor();
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

bool BufferedTupleStreamV2::AddRowSlow(TupleRow* row, Status* status) noexcept {
  bool got_reservation;
  *status = AdvanceWritePage(ComputeRowSize(row), &got_reservation);
  if (!status->ok() || !got_reservation) return false;
  return DeepCopy(row);
}

uint8_t* BufferedTupleStreamV2::AllocateRowSlow(
    int fixed_size, int varlen_size, uint8_t** varlen_data, Status* status) noexcept {
  int64_t row_size = static_cast<int64_t>(fixed_size) + varlen_size;
  bool got_reservation;
  *status = AdvanceWritePage(row_size, &got_reservation);
  if (!status->ok() || !got_reservation) return nullptr;

  // We have a large-enough page so now success is guaranteed.
  uint8_t* result = AllocateRow(fixed_size, varlen_size, varlen_data, status);
  DCHECK(result != nullptr);
  return result;
}

bool BufferedTupleStreamV2::DeepCopy(TupleRow* row) noexcept {
  if (has_nullable_tuple_) {
    return DeepCopyInternal<true>(row);
  } else {
    return DeepCopyInternal<false>(row);
  }
}

// TODO: consider codegening this.
// TODO: in case of duplicate tuples, this can redundantly serialize data.
template <bool HAS_NULLABLE_TUPLE>
bool BufferedTupleStreamV2::DeepCopyInternal(TupleRow* row) noexcept {
  if (UNLIKELY(write_page_ == nullptr)) return false;
  DCHECK(write_page_->is_pinned()) << DebugString() << std::endl
                                   << write_page_->DebugString();

  const uint64_t tuples_per_row = desc_.tuple_descriptors().size();
  uint32_t bytes_remaining = write_end_ptr_ - write_ptr_;

  // Move to the next page we may not have enough space to append the fixed-length part
  // of the row.
  if (UNLIKELY((bytes_remaining < fixed_tuple_row_size_))) return false;

  // Copy the not NULL fixed len tuples. For the NULL tuples just update the NULL tuple
  // indicator.
  if (HAS_NULLABLE_TUPLE) {
    uint8_t* null_indicators = write_ptr_;
    int null_indicator_bytes = NullIndicatorBytesPerRow();
    memset(null_indicators, 0, null_indicator_bytes);
    write_ptr_ += NullIndicatorBytesPerRow();
    for (int i = 0; i < tuples_per_row; ++i) {
      uint8_t* null_word = null_indicators + (i >> 3);
      const uint32_t null_pos = i & 7;
      const int tuple_size = fixed_tuple_sizes_[i];
      Tuple* t = row->GetTuple(i);
      const uint8_t mask = 1 << (7 - null_pos);
      if (t != nullptr) {
        memcpy(write_ptr_, t, tuple_size);
        write_ptr_ += tuple_size;
      } else {
        *null_word |= mask;
      }
    }
  } else {
    // If we know that there are no nullable tuples no need to set the nullability flags.
    for (int i = 0; i < tuples_per_row; ++i) {
      const int tuple_size = fixed_tuple_sizes_[i];
      Tuple* t = row->GetTuple(i);
      // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
      // is delivered, the check below should become DCHECK(t != nullptr).
      DCHECK(t != nullptr || tuple_size == 0);
      memcpy(write_ptr_, t, tuple_size);
      write_ptr_ += tuple_size;
    }
  }

  // Copy inlined string slots. Note: we do not need to convert the string ptrs to offsets
  // on the write path, only on the read. The tuple data is immediately followed
  // by the string data so only the len information is necessary.
  for (int i = 0; i < inlined_string_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_string_slots_[i].first);
    if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
    if (UNLIKELY(!CopyStrings(tuple, inlined_string_slots_[i].second))) return false;
  }

  // Copy inlined collection slots. We copy collection data in a well-defined order so
  // we do not need to convert pointers to offsets on the write path.
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
    if (UNLIKELY(!CopyCollections(tuple, inlined_coll_slots_[i].second))) return false;
  }

  ++num_rows_;
  ++write_page_->num_rows;
  return true;
}

bool BufferedTupleStreamV2::CopyStrings(
    const Tuple* tuple, const vector<SlotDescriptor*>& string_slots) {
  for (const SlotDescriptor* slot_desc : string_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
    if (LIKELY(sv->len > 0)) {
      if (UNLIKELY(write_ptr_ + sv->len > write_end_ptr_)) return false;

      memcpy(write_ptr_, sv->ptr, sv->len);
      write_ptr_ += sv->len;
    }
  }
  return true;
}

bool BufferedTupleStreamV2::CopyCollections(
    const Tuple* tuple, const vector<SlotDescriptor*>& collection_slots) {
  for (const SlotDescriptor* slot_desc : collection_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const CollectionValue* cv = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    if (LIKELY(cv->num_tuples > 0)) {
      int coll_byte_size = cv->num_tuples * item_desc.byte_size();
      if (UNLIKELY(write_ptr_ + coll_byte_size > write_end_ptr_)) return false;
      uint8_t* coll_data = write_ptr_;
      memcpy(coll_data, cv->ptr, coll_byte_size);
      write_ptr_ += coll_byte_size;

      if (!item_desc.HasVarlenSlots()) continue;
      // Copy variable length data when present in collection items.
      for (int i = 0; i < cv->num_tuples; ++i) {
        const Tuple* item = reinterpret_cast<Tuple*>(coll_data);
        if (UNLIKELY(!CopyStrings(item, item_desc.string_slots()))) return false;
        if (UNLIKELY(!CopyCollections(item, item_desc.collection_slots()))) return false;
        coll_data += item_desc.byte_size();
      }
    }
  }
  return true;
}

void BufferedTupleStreamV2::GetTupleRow(FlatRowPtr flat_row, TupleRow* row) const {
  DCHECK(row != nullptr);
  DCHECK(!closed_);
  DCHECK(is_pinned());
  DCHECK(!delete_on_read_);
  uint8_t* data = flat_row;
  return has_nullable_tuple_ ? UnflattenTupleRow<true>(&data, row) :
                               UnflattenTupleRow<false>(&data, row);
}

template <bool HAS_NULLABLE_TUPLE>
void BufferedTupleStreamV2::UnflattenTupleRow(uint8_t** data, TupleRow* row) const {
  const int tuples_per_row = desc_.tuple_descriptors().size();
  uint8_t* ptr = *data;
  if (has_nullable_tuple_) {
    // Stitch together the tuples from the page and the NULL ones.
    const uint8_t* null_indicators = ptr;
    ptr += NullIndicatorBytesPerRow();
    for (int i = 0; i < tuples_per_row; ++i) {
      const uint8_t* null_word = null_indicators + (i >> 3);
      const uint32_t null_pos = i & 7;
      const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
      row->SetTuple(
          i, reinterpret_cast<Tuple*>(reinterpret_cast<uint64_t>(ptr) * is_not_null));
      ptr += fixed_tuple_sizes_[i] * is_not_null;
    }
  } else {
    for (int i = 0; i < tuples_per_row; ++i) {
      row->SetTuple(i, reinterpret_cast<Tuple*>(ptr));
      ptr += fixed_tuple_sizes_[i];
    }
  }
  *data = ptr;
}
