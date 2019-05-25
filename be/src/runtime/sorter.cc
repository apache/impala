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

#include "runtime/sorter-internal.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"
#include "util/ubsan.h"

#include "common/names.h"

using boost::uniform_int;
using boost::mt19937_64;
using namespace strings;

namespace impala {

// Number of pinned pages required for a merge with fixed-length data only.
const int MIN_BUFFERS_PER_MERGE = 3;

Status Sorter::Page::Init(Sorter* sorter) {
  const BufferPool::BufferHandle* page_buffer;
  RETURN_IF_ERROR(pool()->CreatePage(sorter->buffer_pool_client_, sorter->page_len_,
      &handle_, &page_buffer));
  data_ = page_buffer->data();
  return Status::OK();
}

BufferPool::BufferHandle Sorter::Page::ExtractBuffer(BufferPool::ClientHandle* client) {
  DCHECK(data_ != nullptr) << "Page must be in memory";
  BufferPool::BufferHandle buffer;
  Status status = pool()->ExtractBuffer(client, &handle_, &buffer);
  DCHECK(status.ok()) << "Page was in memory, ExtractBuffer() shouldn't fail";
  Reset();
  return buffer;
}

uint8_t* Sorter::Page::AllocateBytes(int64_t len) {
  DCHECK_GE(len, 0);
  DCHECK_LE(len, BytesRemaining());
  DCHECK(data_ != nullptr);
  uint8_t* result = data_ + valid_data_len_;
  valid_data_len_ += len;
  return result;
}

void Sorter::Page::FreeBytes(int64_t len) {
  DCHECK_GE(len, 0);
  DCHECK_LE(len, valid_data_len_);
  DCHECK(data_ != nullptr);
  valid_data_len_ -= len;
}

Status Sorter::Page::WaitForBuffer() {
  DCHECK(handle_.is_pinned());
  if (data_ != nullptr) return Status::OK();
  const BufferPool::BufferHandle* page_buffer;
  RETURN_IF_ERROR(handle_.GetBuffer(&page_buffer));
  data_ = page_buffer->data();
  return Status::OK();
}

Status Sorter::Page::Pin(BufferPool::ClientHandle* client) {
  DCHECK(!handle_.is_pinned());
  return pool()->Pin(client, &handle_);
}

void Sorter::Page::Unpin(BufferPool::ClientHandle* client) {
  pool()->Unpin(client, &handle_);
  data_ = nullptr;
}

void Sorter::Page::Close(BufferPool::ClientHandle* client) {
  pool()->DestroyPage(client, &handle_);
  Reset();
}

void Sorter::Page::Reset() {
  DCHECK(!handle_.is_open());
  valid_data_len_ = 0;
  data_ = nullptr;
}

BufferPool* Sorter::Page::pool() {
  return ExecEnv::GetInstance()->buffer_pool();
}

// Sorter::Run methods
Sorter::Run::Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool initial_run)
  : sorter_(parent),
    sort_tuple_desc_(sort_tuple_desc),
    sort_tuple_size_(sort_tuple_desc->byte_size()),
    page_capacity_(parent->page_len_ / sort_tuple_size_),
    has_var_len_slots_(sort_tuple_desc->HasVarlenSlots()),
    initial_run_(initial_run),
    is_pinned_(initial_run),
    is_finalized_(false),
    is_sorted_(!initial_run),
    num_tuples_(0) {}

Status Sorter::Run::Init() {
  int num_to_create = 1 + has_var_len_slots_
      + (has_var_len_slots_ && initial_run_ && sorter_->enable_spilling_);
  int64_t required_mem = num_to_create * sorter_->page_len_;
  if (!sorter_->buffer_pool_client_->IncreaseReservationToFit(required_mem)) {
    return Status(Substitute(
        "Unexpected error trying to reserve $0 bytes for a sorted run: $1",
        required_mem, sorter_->buffer_pool_client_->DebugString()));
  }

  RETURN_IF_ERROR(AddPage(&fixed_len_pages_));
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(AddPage(&var_len_pages_));
    if (initial_run_ && sorter_->enable_spilling_) {
      // Need additional var len page to reorder var len data in UnpinAllPages().
      RETURN_IF_ERROR(var_len_copy_page_.Init(sorter_));
    }
  }
  if (initial_run_) {
    sorter_->initial_runs_counter_->Add(1);
  } else {
    sorter_->spilled_runs_counter_->Add(1);
  }
  return Status::OK();
}

template <bool HAS_VAR_LEN_SLOTS, bool INITIAL_RUN>
Status Sorter::Run::AddBatchInternal(
    RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(!is_finalized_);
  DCHECK(!fixed_len_pages_.empty());
  DCHECK_EQ(HAS_VAR_LEN_SLOTS, has_var_len_slots_);
  DCHECK_EQ(INITIAL_RUN, initial_run_);

  *num_processed = 0;
  Page* cur_fixed_len_page = &fixed_len_pages_.back();

  if (!INITIAL_RUN) {
    // For intermediate merges, the input row is the sort tuple.
    DCHECK_EQ(batch->row_desc()->tuple_descriptors().size(), 1);
    DCHECK_EQ(batch->row_desc()->tuple_descriptors()[0], sort_tuple_desc_);
  }

  /// Keep initial unsorted runs pinned in memory so we can sort them.
  const AddPageMode add_mode = INITIAL_RUN ? KEEP_PREV_PINNED : UNPIN_PREV;

  // Input rows are copied/materialized into tuples allocated in fixed_len_pages_.
  // The variable length column data are copied into pages stored in var_len_pages_.
  // Input row processing is split into two loops.
  // The inner loop processes as many input rows as will fit in cur_fixed_len_page.
  // The outer loop allocates a new page for fixed-len data if the input batch is
  // not exhausted.

  // cur_input_index is the index into the input 'batch' of the current input row being
  // processed.
  int cur_input_index = start_index;
  vector<StringValue*> string_values;
  string_values.reserve(sort_tuple_desc_->string_slots().size());
  while (cur_input_index < batch->num_rows()) {
    // tuples_remaining is the number of tuples to copy/materialize into
    // cur_fixed_len_page.
    int tuples_remaining = cur_fixed_len_page->BytesRemaining() / sort_tuple_size_;
    tuples_remaining = min(batch->num_rows() - cur_input_index, tuples_remaining);

    for (int i = 0; i < tuples_remaining; ++i) {
      int total_var_len = 0;
      TupleRow* input_row = batch->GetRow(cur_input_index);
      Tuple* new_tuple =
          reinterpret_cast<Tuple*>(cur_fixed_len_page->AllocateBytes(sort_tuple_size_));
      if (INITIAL_RUN) {
        new_tuple->MaterializeExprs<HAS_VAR_LEN_SLOTS, true>(input_row,
            *sort_tuple_desc_, sorter_->sort_tuple_expr_evals_, nullptr,
            &string_values, &total_var_len);
        if (total_var_len > sorter_->page_len_) {
          int64_t max_row_size = sorter_->state_->query_options().max_row_size;
          return Status(TErrorCode::MAX_ROW_SIZE,
              PrettyPrinter::Print(total_var_len, TUnit::BYTES), sorter_->node_id_,
              PrettyPrinter::Print(max_row_size, TUnit::BYTES));
        }
      } else {
        memcpy(new_tuple, input_row->GetTuple(0), sort_tuple_size_);
        if (HAS_VAR_LEN_SLOTS) {
          CollectNonNullVarSlots(new_tuple, &string_values, &total_var_len);
        }
      }

      if (HAS_VAR_LEN_SLOTS) {
        DCHECK_GT(var_len_pages_.size(), 0);
        Page* cur_var_len_page = &var_len_pages_.back();
        if (cur_var_len_page->BytesRemaining() < total_var_len) {
          bool added;
          RETURN_IF_ERROR(TryAddPage(add_mode, &var_len_pages_, &added));
          if (added) {
            cur_var_len_page = &var_len_pages_.back();
          } else {
            // There was not enough space in the last var-len page for this tuple, and
            // the run could not be extended. Return the fixed-len allocation and exit.
            cur_fixed_len_page->FreeBytes(sort_tuple_size_);
            return Status::OK();
          }
        }

        // Sorting of tuples containing array values is not implemented. The planner
        // combined with projection should guarantee that none are in each tuple.
        for (const SlotDescriptor* coll_slot: sort_tuple_desc_->collection_slots()) {
          DCHECK(new_tuple->IsNull(coll_slot->null_indicator_offset()));
        }

        uint8_t* var_data_ptr = cur_var_len_page->AllocateBytes(total_var_len);
        if (INITIAL_RUN) {
          CopyVarLenData(string_values, var_data_ptr);
        } else {
          DCHECK_EQ(&var_len_pages_.back(), cur_var_len_page);
          CopyVarLenDataConvertOffset(string_values, var_len_pages_.size() - 1,
              cur_var_len_page->data(), var_data_ptr);
        }
      }
      ++num_tuples_;
      ++*num_processed;
      ++cur_input_index;
    }

    // If there are still rows left to process, get a new page for the fixed-length
    // tuples. If the run is already too long, return.
    if (cur_input_index < batch->num_rows()) {
      bool added;
      RETURN_IF_ERROR(TryAddPage(add_mode, &fixed_len_pages_, &added));
      if (!added) return Status::OK();
      cur_fixed_len_page = &fixed_len_pages_.back();
    }
  }
  return Status::OK();
}

Status Sorter::Run::FinalizeInput() {
  DCHECK(!is_finalized_);

  RETURN_IF_ERROR(FinalizePages(&fixed_len_pages_));
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(FinalizePages(&var_len_pages_));
  }
  is_finalized_ = true;
  return Status::OK();
}

Status Sorter::Run::FinalizePages(vector<Page>* pages) {
  DCHECK_GT(pages->size(), 0);
  Page* last_page = &pages->back();
  if (last_page->valid_data_len() > 0) {
    DCHECK_EQ(initial_run_, is_pinned_);
    if (!is_pinned_) {
      // Unpin the last page of this unpinned run. We've finished writing the run so
      // all pages in the run can now be unpinned.
      last_page->Unpin(sorter_->buffer_pool_client_);
    }
  } else {
    last_page->Close(sorter_->buffer_pool_client_);
    pages->pop_back();
  }
  return Status::OK();
}

void Sorter::Run::CloseAllPages() {
  DeleteAndClearPages(&fixed_len_pages_);
  DeleteAndClearPages(&var_len_pages_);
  if (var_len_copy_page_.is_open()) {
    var_len_copy_page_.Close(sorter_->buffer_pool_client_);
  }
}

Status Sorter::Run::UnpinAllPages() {
  DCHECK(is_sorted_);
  DCHECK(initial_run_);
  DCHECK(is_pinned_);
  DCHECK(is_finalized_);
  // A list of var len pages to replace 'var_len_pages_'. Note that after we are done
  // we may have a different number of pages, because internal fragmentation may leave
  // slightly different amounts of wasted space at the end of each page.
  // We need to be careful to clean up these pages if we run into an error in this method.
  vector<Page> sorted_var_len_pages;
  sorted_var_len_pages.reserve(var_len_pages_.size());

  vector<StringValue*> string_values;
  int total_var_len;
  string_values.reserve(sort_tuple_desc_->string_slots().size());
  Page* cur_sorted_var_len_page = nullptr;
  if (HasVarLenPages()) {
    DCHECK(var_len_copy_page_.is_open());
    sorted_var_len_pages.push_back(move(var_len_copy_page_));
    cur_sorted_var_len_page = &sorted_var_len_pages.back();
  } else if (has_var_len_slots_) {
    // If we don't have any var-len pages, clean up the copy page.
    DCHECK(var_len_copy_page_.is_open());
    var_len_copy_page_.Close(sorter_->buffer_pool_client_);
  } else {
    DCHECK(!var_len_copy_page_.is_open());
  }

  Status status;
  for (auto& fixed_len_page : fixed_len_pages_) {
    Page* cur_fixed_page = &fixed_len_page;
    // Skip converting the pointers if no var-len slots, or if all the values are null
    // or zero-length. This will possibly leave zero-length pointers pointing to
    // arbitrary memory, but zero-length data cannot be dereferenced anyway.
    if (HasVarLenPages()) {
      for (int page_offset = 0; page_offset < cur_fixed_page->valid_data_len();
           page_offset += sort_tuple_size_) {
        Tuple* cur_tuple = reinterpret_cast<Tuple*>(cur_fixed_page->data() + page_offset);
        CollectNonNullVarSlots(cur_tuple, &string_values, &total_var_len);
        DCHECK(cur_sorted_var_len_page->is_open());
        if (cur_sorted_var_len_page->BytesRemaining() < total_var_len) {
          bool added;
          status = TryAddPage(UNPIN_PREV, &sorted_var_len_pages, &added);
          if (!status.ok()) goto cleanup_pages;
          DCHECK(added) << "TryAddPage() with UNPIN_PREV should not fail to add";
          cur_sorted_var_len_page = &sorted_var_len_pages.back();
        }
        uint8_t* var_data_ptr = cur_sorted_var_len_page->AllocateBytes(total_var_len);
        DCHECK_EQ(&sorted_var_len_pages.back(), cur_sorted_var_len_page);
        CopyVarLenDataConvertOffset(string_values, sorted_var_len_pages.size() - 1,
            cur_sorted_var_len_page->data(), var_data_ptr);
      }
    }
    cur_fixed_page->Unpin(sorter_->buffer_pool_client_);
  }

  if (HasVarLenPages()) {
    DCHECK_GT(sorted_var_len_pages.back().valid_data_len(), 0);
    sorted_var_len_pages.back().Unpin(sorter_->buffer_pool_client_);
  }

  // Clear var_len_pages_ and replace with it with the contents of sorted_var_len_pages
  DeleteAndClearPages(&var_len_pages_);
  sorted_var_len_pages.swap(var_len_pages_);
  is_pinned_ = false;
  sorter_->spilled_runs_counter_->Add(1);
  return Status::OK();

cleanup_pages:
  DeleteAndClearPages(&sorted_var_len_pages);
  return status;
}

Status Sorter::Run::PrepareRead() {
  DCHECK(is_finalized_);
  DCHECK(is_sorted_);

  fixed_len_pages_index_ = 0;
  fixed_len_page_offset_ = 0;
  var_len_pages_index_ = 0;
  end_of_fixed_len_page_ = end_of_var_len_page_ = fixed_len_pages_.empty();
  num_tuples_returned_ = 0;

  buffered_batch_.reset(new RowBatch(
      sorter_->output_row_desc_, sorter_->state_->batch_size(), sorter_->mem_tracker_));

  // If the run is pinned, all pages are already pinned, so we're ready to read.
  if (is_pinned_) return Status::OK();

  // Pins the first fixed len page.
  if (!fixed_len_pages_.empty()) {
    RETURN_IF_ERROR(fixed_len_pages_[0].Pin(sorter_->buffer_pool_client_));
  }

  // Pins the first var len page if there is any.
  if (HasVarLenPages()) {
    RETURN_IF_ERROR(var_len_pages_[0].Pin(sorter_->buffer_pool_client_));
  }

  return Status::OK();
}

Status Sorter::Run::GetNextBatch(RowBatch** output_batch) {
  DCHECK(buffered_batch_ != nullptr);
  buffered_batch_->Reset();
  // Fill more rows into buffered_batch_.
  bool eos;
  if (HasVarLenPages() && !is_pinned_) {
    RETURN_IF_ERROR(GetNext<true>(buffered_batch_.get(), &eos));
  } else {
    RETURN_IF_ERROR(GetNext<false>(buffered_batch_.get(), &eos));
  }

  if (eos) {
    // Setting output_batch to nullptr signals eos to the caller, so GetNext() is not
    // allowed to attach resources to the batch on eos.
    DCHECK_EQ(buffered_batch_->num_rows(), 0);
    DCHECK_EQ(buffered_batch_->num_buffers(), 0);
    *output_batch = nullptr;
    return Status::OK();
  }
  *output_batch = buffered_batch_.get();
  return Status::OK();
}

template <bool CONVERT_OFFSET_TO_PTR>
Status Sorter::Run::GetNext(RowBatch* output_batch, bool* eos) {
  // Var-len offsets are converted only when reading var-len data from unpinned runs.
  // We shouldn't convert var len offsets if there are no pages, since in that case
  // they must all be null or zero-length strings, which don't point into a valid page.
  DCHECK_EQ(CONVERT_OFFSET_TO_PTR, HasVarLenPages() && !is_pinned_);

  if (end_of_fixed_len_page_
      && fixed_len_pages_index_ >= static_cast<int>(fixed_len_pages_.size()) - 1) {
    // All pages were previously attached to output batches. GetNextBatch() assumes
    // that we don't attach resources to the batch on eos.
    DCHECK_EQ(NumOpenPages(fixed_len_pages_), 0);
    DCHECK_EQ(NumOpenPages(var_len_pages_), 0);

    CloseAllPages();
    *eos = true;
    DCHECK_EQ(num_tuples_returned_, num_tuples_);
    return Status::OK();
  }

  // Advance the fixed or var len page if we reached the end in the previous call to
  // GetNext().
  if (end_of_fixed_len_page_) {
    RETURN_IF_ERROR(PinNextReadPage(&fixed_len_pages_, fixed_len_pages_index_));
    ++fixed_len_pages_index_;
    fixed_len_page_offset_ = 0;
    end_of_fixed_len_page_ = false;
  }
  if (end_of_var_len_page_) {
    RETURN_IF_ERROR(PinNextReadPage(&var_len_pages_, var_len_pages_index_));
    ++var_len_pages_index_;
    end_of_var_len_page_ = false;
  }

  // Fills rows into the output batch until a page boundary is reached.
  Page* fixed_len_page = &fixed_len_pages_[fixed_len_pages_index_];
  DCHECK(fixed_len_page != nullptr);

  // Ensure we have a reference to the fixed-length page's buffer.
  RETURN_IF_ERROR(fixed_len_page->WaitForBuffer());

  // If we're converting offsets into unpinned var-len pages, make sure the
  // current var-len page is in memory.
  if (CONVERT_OFFSET_TO_PTR && HasVarLenPages()) {
    RETURN_IF_ERROR(var_len_pages_[var_len_pages_index_].WaitForBuffer());
  }

  while (!output_batch->AtCapacity()
      && fixed_len_page_offset_ < fixed_len_page->valid_data_len()) {
    DCHECK(fixed_len_page != nullptr);
    Tuple* input_tuple =
        reinterpret_cast<Tuple*>(fixed_len_page->data() + fixed_len_page_offset_);

    if (CONVERT_OFFSET_TO_PTR && !ConvertOffsetsToPtrs(input_tuple)) {
      DCHECK(!is_pinned_);
      // The var-len data is in the next page. We are done with the current page, so
      // return rows we've accumulated so far along with the page's buffer and advance
      // to the next page in the next GetNext() call. We need the page's reservation to
      // pin the next page, so flush resources.
      DCHECK_GE(var_len_pages_index_, 0);
      DCHECK_LT(var_len_pages_index_, var_len_pages_.size());
      BufferPool::BufferHandle buffer =
          var_len_pages_[var_len_pages_index_].ExtractBuffer(
              sorter_->buffer_pool_client_);
      output_batch->AddBuffer(sorter_->buffer_pool_client_, move(buffer),
          RowBatch::FlushMode::FLUSH_RESOURCES);
      end_of_var_len_page_ = true;
      break;
    }
    output_batch->GetRow(output_batch->AddRow())->SetTuple(0, input_tuple);
    output_batch->CommitLastRow();
    fixed_len_page_offset_ += sort_tuple_size_;
    ++num_tuples_returned_;
  }

  if (fixed_len_page_offset_ >= fixed_len_page->valid_data_len()) {
    // Reached the page boundary, need to move to the next page.
    BufferPool::ClientHandle* client = sorter_->buffer_pool_client_;
    // Attach page to batch. For unpinned pages we need to flush resource so we can pin
    // the next page on the next call to GetNext().
    RowBatch::FlushMode flush = is_pinned_ ? RowBatch::FlushMode::NO_FLUSH_RESOURCES :
                                             RowBatch::FlushMode::FLUSH_RESOURCES;
    output_batch->AddBuffer(
        client, fixed_len_pages_[fixed_len_pages_index_].ExtractBuffer(client), flush);
    // Attach remaining var-len pages at eos once no more rows will reference the pages.
    if (fixed_len_pages_index_ == fixed_len_pages_.size() - 1) {
      for (Page& var_len_page : var_len_pages_) {
        DCHECK(!is_pinned_ || var_len_page.is_open());
        if (var_len_page.is_open()) {
          output_batch->AddBuffer(client, var_len_page.ExtractBuffer(client),
              RowBatch::FlushMode::NO_FLUSH_RESOURCES);
        }
      }
      var_len_pages_.clear();
    }
    end_of_fixed_len_page_ = true;
  }
  *eos = false;
  return Status::OK();
}

Status Sorter::Run::PinNextReadPage(vector<Page>* pages, int page_index) {
  DCHECK_GE(page_index, 0);
  DCHECK_LT(page_index, pages->size() - 1);
  Page* curr_page = &(*pages)[page_index];
  Page* next_page = &(*pages)[page_index + 1];
  DCHECK_EQ(is_pinned_, next_page->is_pinned());
  // The current page was attached to a batch.
  DCHECK(!curr_page->is_open());
  // 'next_page' is already pinned if the whole stream is pinned.
  if (is_pinned_) return Status::OK();
  RETURN_IF_ERROR(next_page->Pin(sorter_->buffer_pool_client_));
  return Status::OK();
}

void Sorter::Run::CollectNonNullVarSlots(Tuple* src,
    vector<StringValue*>* string_values, int* total_var_len) {
  string_values->clear();
  *total_var_len = 0;
  for (const SlotDescriptor* string_slot: sort_tuple_desc_->string_slots()) {
    if (!src->IsNull(string_slot->null_indicator_offset())) {
      StringValue* string_val =
          reinterpret_cast<StringValue*>(src->GetSlot(string_slot->tuple_offset()));
      string_values->push_back(string_val);
      *total_var_len += string_val->len;
    }
  }
}

Status Sorter::Run::TryAddPage(
    AddPageMode mode, vector<Page>* page_sequence, bool* added) {
  DCHECK(!page_sequence->empty());
  if (mode == KEEP_PREV_PINNED) {
    if (!sorter_->buffer_pool_client_->IncreaseReservationToFit(sorter_->page_len_)) {
      *added = false;
      return Status::OK();
    }
  } else {
    DCHECK(mode == UNPIN_PREV);
    // Unpin the prev page to free up the memory required to pin the next page.
    page_sequence->back().Unpin(sorter_->buffer_pool_client_);
  }

  RETURN_IF_ERROR(AddPage(page_sequence));
  *added = true;
  return Status::OK();
}

Status Sorter::Run::AddPage(vector<Page>* page_sequence) {
  Page new_page;
  RETURN_IF_ERROR(new_page.Init(sorter_));
  page_sequence->push_back(move(new_page));
  return Status::OK();
}

void Sorter::Run::CopyVarLenData(const vector<StringValue*>& string_values,
    uint8_t* dest) {
  for (StringValue* string_val: string_values) {
    Ubsan::MemCpy(dest, string_val->ptr, string_val->len);
    string_val->ptr = reinterpret_cast<char*>(dest);
    dest += string_val->len;
  }
}

void Sorter::Run::CopyVarLenDataConvertOffset(const vector<StringValue*>& string_values,
    int page_index, const uint8_t* page_start, uint8_t* dest) {
  DCHECK_GE(page_index, 0);
  DCHECK_GE(dest - page_start, 0);

  for (StringValue* string_val : string_values) {
    memcpy(dest, string_val->ptr, string_val->len);
    DCHECK_LE(dest - page_start, sorter_->page_len_);
    DCHECK_LE(dest - page_start, numeric_limits<uint32_t>::max());
    uint32_t page_offset = dest - page_start;
    uint64_t packed_offset = (static_cast<uint64_t>(page_index) << 32) | page_offset;
    string_val->ptr = reinterpret_cast<char*>(packed_offset);
    dest += string_val->len;
  }
}

bool Sorter::Run::ConvertOffsetsToPtrs(Tuple* tuple) {
  // We need to be careful to handle the case where var_len_pages_ is empty,
  // e.g. if all strings are nullptr.
  uint8_t* page_start =
      var_len_pages_.empty() ? nullptr : var_len_pages_[var_len_pages_index_].data();

  const vector<SlotDescriptor*>& string_slots = sort_tuple_desc_->string_slots();
  int num_non_null_string_slots = 0;
  for (auto slot_desc : string_slots) {
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    ++num_non_null_string_slots;

    DCHECK(slot_desc->type().IsVarLenStringType());
    StringValue* value = reinterpret_cast<StringValue*>(
        tuple->GetSlot(slot_desc->tuple_offset()));
    // packed_offset includes the page index in the upper 32 bits and the page
    // offset in the lower 32 bits. See CopyVarLenDataConvertOffset().
    uint64_t packed_offset = reinterpret_cast<uint64_t>(value->ptr);
    uint32_t page_index = packed_offset >> 32;
    uint32_t page_offset = packed_offset & 0xFFFFFFFF;

    if (page_index > var_len_pages_index_) {
      // We've reached the page boundary for the current var-len page.
      // This tuple will be returned in the next call to GetNext().
      DCHECK_GE(page_index, 0);
      DCHECK_LE(page_index, var_len_pages_.size());
      DCHECK_EQ(page_index, var_len_pages_index_ + 1);
      DCHECK_EQ(page_offset, 0); // The data is the first thing in the next page.
      // This must be the first slot with var len data for the tuple. Var len data
      // for tuple shouldn't be split across blocks.
      DCHECK_EQ(num_non_null_string_slots, 1);
      return false;
    }

    DCHECK_EQ(page_index, var_len_pages_index_);
    if (var_len_pages_.empty()) {
      DCHECK_EQ(value->len, 0);
    } else {
      DCHECK_LE(page_offset + value->len, var_len_pages_[page_index].valid_data_len());
    }
    // Calculate the address implied by the offset and assign it. May be nullptr for
    // zero-length strings if there are no pages in the run since page_start is nullptr.
    DCHECK(page_start != nullptr || page_offset == 0);
    value->ptr = reinterpret_cast<char*>(page_start + page_offset);
  }
  return true;
}

int64_t Sorter::Run::TotalBytes() const {
  int64_t total_bytes = 0;
  for (const Page& page : fixed_len_pages_) {
    if (page.is_open()) total_bytes += page.valid_data_len();
  }

  for (const Page& page : var_len_pages_) {
    if (page.is_open()) total_bytes += page.valid_data_len();
  }
  return total_bytes;
}

Status Sorter::Run::AddInputBatch(RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(initial_run_);
  if (has_var_len_slots_) {
    return AddBatchInternal<true, true>(batch, start_index, num_processed);
  } else {
    return AddBatchInternal<false, true>(batch, start_index, num_processed);
  }
}

Status Sorter::Run::AddIntermediateBatch(
    RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(!initial_run_);
  if (has_var_len_slots_) {
    return AddBatchInternal<true, false>(batch, start_index, num_processed);
  } else {
    return AddBatchInternal<false, false>(batch, start_index, num_processed);
  }
}

void Sorter::Run::CleanupRuns(deque<Sorter::Run*>* runs) {
  for (Run* run : *runs) {
    run->CloseAllPages();
  }
  runs->clear();
}

int Sorter::Run::NumOpenPages(const vector<Sorter::Page>& pages) {
  int count = 0;
  for (const Page& page : pages) {
    if (page.is_open()) ++count;
  }
  return count;
}

void Sorter::Run::DeleteAndClearPages(vector<Sorter::Page>* pages) {
  for (Page& page : *pages) {
    if (page.is_open()) page.Close(sorter_->buffer_pool_client_);
  }
  pages->clear();
}

Sorter::Run::~Run() {
  DCHECK(fixed_len_pages_.empty());
  DCHECK(var_len_pages_.empty());
  DCHECK(!var_len_copy_page_.is_open());
}

Sorter::TupleIterator::TupleIterator(Sorter::Run* run, int64_t index)
    : index_(index), tuple_(nullptr) {
  DCHECK(run->is_finalized_);
  DCHECK_GE(index, 0);
  DCHECK_LE(index, run->num_tuples());
  // If the run is empty, only index_ and tuple_ are initialized.
  if (run->num_tuples() == 0) {
    DCHECK_EQ(index, 0);
    return;
  }

  const int tuple_size = run->sort_tuple_size_;
  uint32_t page_offset;
  if (UNLIKELY(index == run->num_tuples())) {
    // If the iterator is initialized past the end, set up buffer_start_index_,
    // 'buffer_end_index_' and 'page_index_' for the last page, then set 'tuple' to
    // 'tuple_size' bytes past the last tuple, so everything is correct when Prev() is
    // invoked.
    page_index_ = run->fixed_len_pages_.size() - 1;
    page_offset = ((index - 1) % run->page_capacity_) * tuple_size + tuple_size;
  } else {
    page_index_ = index / run->page_capacity_;
    page_offset = (index % run->page_capacity_) * tuple_size;
  }
  buffer_start_index_ = page_index_ * run->page_capacity_;
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  tuple_ = run->fixed_len_pages_[page_index_].data() + page_offset;
}

Sorter::TupleSorter::TupleSorter(Sorter* parent, const TupleRowComparator& comp,
    int tuple_size, RuntimeState* state)
  : parent_(parent),
    tuple_size_(tuple_size),
    comparator_(comp),
    num_comparisons_till_free_(state->batch_size()),
    state_(state) {
  temp_tuple_buffer_ = new uint8_t[tuple_size];
  swap_buffer_ = new uint8_t[tuple_size];
}

Sorter::TupleSorter::~TupleSorter() {
  delete[] temp_tuple_buffer_;
  delete[] swap_buffer_;
}

Status Sorter::TupleSorter::Sort(Run* run) {
  DCHECK(run->is_finalized());
  DCHECK(!run->is_sorted());
  run_ = run;
  RETURN_IF_ERROR(SortHelper(TupleIterator::Begin(run_), TupleIterator::End(run_)));
  run_->set_sorted();
  return Status::OK();
}

Sorter::Sorter(const std::vector<ScalarExpr*>& ordering_exprs,
      const std::vector<bool>& is_asc_order, const std::vector<bool>& nulls_first,
    const vector<ScalarExpr*>& sort_tuple_exprs, RowDescriptor* output_row_desc,
    MemTracker* mem_tracker, BufferPool::ClientHandle* buffer_pool_client,
    int64_t page_len, RuntimeProfile* profile, RuntimeState* state, int node_id,
    bool enable_spilling)
  : node_id_(node_id),
    state_(state),
    expr_perm_pool_(mem_tracker),
    expr_results_pool_(mem_tracker),
    compare_less_than_(ordering_exprs, is_asc_order, nulls_first),
    in_mem_tuple_sorter_(nullptr),
    buffer_pool_client_(buffer_pool_client),
    page_len_(page_len),
    has_var_len_slots_(false),
    sort_tuple_exprs_(sort_tuple_exprs),
    mem_tracker_(mem_tracker),
    output_row_desc_(output_row_desc),
    enable_spilling_(enable_spilling),
    unsorted_run_(nullptr),
    merge_output_run_(nullptr),
    profile_(profile),
    initial_runs_counter_(nullptr),
    num_merges_counter_(nullptr),
    in_mem_sort_timer_(nullptr),
    sorted_data_size_(nullptr),
    run_sizes_(nullptr) {}

Sorter::~Sorter() {
  DCHECK(sorted_runs_.empty());
  DCHECK(merging_runs_.empty());
  DCHECK(unsorted_run_ == nullptr);
  DCHECK(merge_output_run_ == nullptr);
}

Status Sorter::Prepare(ObjectPool* obj_pool) {
  DCHECK(in_mem_tuple_sorter_ == nullptr) << "Already prepared";
  // Page byte offsets are packed into uint32_t values, which limits the supported
  // page size.
  if (page_len_ > numeric_limits<uint32_t>::max()) {
    return Status(Substitute(
          "Page size $0 exceeded maximum supported in sorter ($1)",
          PrettyPrinter::PrintBytes(page_len_),
          PrettyPrinter::PrintBytes(numeric_limits<uint32_t>::max())));
  }

  TupleDescriptor* sort_tuple_desc = output_row_desc_->tuple_descriptors()[0];
  if (sort_tuple_desc->byte_size() > page_len_) {
    return Status(TErrorCode::MAX_ROW_SIZE,
        PrettyPrinter::Print(sort_tuple_desc->byte_size(), TUnit::BYTES), node_id_,
        PrettyPrinter::Print(state_->query_options().max_row_size, TUnit::BYTES));
  }
  has_var_len_slots_ = sort_tuple_desc->HasVarlenSlots();
  in_mem_tuple_sorter_.reset(
      new TupleSorter(this, compare_less_than_, sort_tuple_desc->byte_size(), state_));

  if (enable_spilling_) {
    initial_runs_counter_ = ADD_COUNTER(profile_, "InitialRunsCreated", TUnit::UNIT);
    spilled_runs_counter_ = ADD_COUNTER(profile_, "SpilledRuns", TUnit::UNIT);
    num_merges_counter_ = ADD_COUNTER(profile_, "TotalMergesPerformed", TUnit::UNIT);
  } else {
    initial_runs_counter_ = ADD_COUNTER(profile_, "RunsCreated", TUnit::UNIT);
  }
  in_mem_sort_timer_ = ADD_TIMER(profile_, "InMemorySortTime");
  sorted_data_size_ = ADD_COUNTER(profile_, "SortDataSize", TUnit::BYTES);
  run_sizes_ = ADD_SUMMARY_STATS_COUNTER(profile_, "NumRowsPerRun", TUnit::UNIT);

  RETURN_IF_ERROR(ScalarExprEvaluator::Create(sort_tuple_exprs_, state_, obj_pool,
      &expr_perm_pool_, &expr_results_pool_, &sort_tuple_expr_evals_));
  return Status::OK();
}

Status Sorter::Codegen(RuntimeState* state) {
  return compare_less_than_.Codegen(state);
}

Status Sorter::Open() {
  DCHECK(in_mem_tuple_sorter_ != nullptr) << "Not prepared";
  DCHECK(unsorted_run_ == nullptr) << "Already open";
  RETURN_IF_ERROR(compare_less_than_.Open(&obj_pool_, state_, &expr_perm_pool_,
      &expr_results_pool_));
  TupleDescriptor* sort_tuple_desc = output_row_desc_->tuple_descriptors()[0];
  unsorted_run_ = run_pool_.Add(new Run(this, sort_tuple_desc, true));
  RETURN_IF_ERROR(unsorted_run_->Init());
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(sort_tuple_expr_evals_, state_));
  return Status::OK();
}

// Must be kept in sync with SortNode.computeNodeResourceProfile() in fe.
int64_t Sorter::ComputeMinReservation() const {
  int min_buffers_required = enable_spilling_ ? MIN_BUFFERS_PER_MERGE : 1;
  // Fixed and var-length pages are separate, so we need double the pages
  // if there is var-length data.
  if (output_row_desc_->HasVarlenSlots()) min_buffers_required *= 2;
  return min_buffers_required * page_len_;
}

Status Sorter::AddBatch(RowBatch* batch) {
  DCHECK(unsorted_run_ != nullptr);
  DCHECK(batch != nullptr);
  DCHECK(enable_spilling_);
  int num_processed = 0;
  int cur_batch_index = 0;
  while (cur_batch_index < batch->num_rows()) {
    RETURN_IF_ERROR(AddBatchNoSpill(batch, cur_batch_index, &num_processed));

    cur_batch_index += num_processed;
    if (cur_batch_index < batch->num_rows()) {
      // The current run is full. Sort it, spill it and begin the next one.
      RETURN_IF_ERROR(state_->StartSpilling(mem_tracker_));
      RETURN_IF_ERROR(SortCurrentInputRun());
      RETURN_IF_ERROR(sorted_runs_.back()->UnpinAllPages());
      unsorted_run_ =
          run_pool_.Add(new Run(this, output_row_desc_->tuple_descriptors()[0], true));
      RETURN_IF_ERROR(unsorted_run_->Init());
    }
  }
  // Clear any temporary allocations made while materializing the sort tuples.
  expr_results_pool_.Clear();
  return Status::OK();
}

Status Sorter::AddBatchNoSpill(RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(batch != nullptr);
  RETURN_IF_ERROR(unsorted_run_->AddInputBatch(batch, start_index, num_processed));
  // Clear any temporary allocations made while materializing the sort tuples.
  expr_results_pool_.Clear();
  return Status::OK();
}

Status Sorter::InputDone() {
  // Sort the tuples in the last run.
  RETURN_IF_ERROR(SortCurrentInputRun());

  if (sorted_runs_.size() == 1) {
    // The entire input fit in one run. Read sorted rows in GetNext() directly from the
    // in-memory sorted run.
    DCHECK(sorted_runs_.back()->is_pinned());
    RETURN_IF_ERROR(sorted_runs_.back()->PrepareRead());
    return Status::OK();
  }
  DCHECK(enable_spilling_);

  // Unpin the final run to free up memory for the merge.
  // TODO: we could keep it in memory in some circumstances as an optimisation, once
  // we have a buffer pool with more reliable reservations (IMPALA-3200).
  RETURN_IF_ERROR(sorted_runs_.back()->UnpinAllPages());

  // Merge intermediate runs until we have a final merge set-up.
  return MergeIntermediateRuns();
}

Status Sorter::GetNext(RowBatch* output_batch, bool* eos) {
  if (sorted_runs_.size() == 1) {
    DCHECK(sorted_runs_.back()->is_pinned());
    return sorted_runs_.back()->GetNext<false>(output_batch, eos);
  } else {
    RETURN_IF_ERROR(merger_->GetNext(output_batch, eos));
    // Clear any temporary allocations made by the merger.
    expr_results_pool_.Clear();
    return Status::OK();
  }
}

void Sorter::Reset() {
  DCHECK(unsorted_run_ == nullptr) << "Cannot Reset() before calling InputDone()";
  merger_.reset();
  // Free resources from the current runs.
  CleanupAllRuns();
  compare_less_than_.Close(state_);
}

void Sorter::Close(RuntimeState* state) {
  CleanupAllRuns();
  compare_less_than_.Close(state);
  ScalarExprEvaluator::Close(sort_tuple_expr_evals_, state);
  expr_perm_pool_.FreeAll();
  expr_results_pool_.FreeAll();
  obj_pool_.Clear();
}

void Sorter::CleanupAllRuns() {
  Run::CleanupRuns(&sorted_runs_);
  Run::CleanupRuns(&merging_runs_);
  if (unsorted_run_ != nullptr) unsorted_run_->CloseAllPages();
  unsorted_run_ = nullptr;
  if (merge_output_run_ != nullptr) merge_output_run_->CloseAllPages();
  merge_output_run_ = nullptr;
  run_pool_.Clear();
}

Status Sorter::SortCurrentInputRun() {
  RETURN_IF_ERROR(unsorted_run_->FinalizeInput());

  {
    SCOPED_TIMER(in_mem_sort_timer_);
    RETURN_IF_ERROR(in_mem_tuple_sorter_->Sort(unsorted_run_));
  }
  sorted_runs_.push_back(unsorted_run_);
  sorted_data_size_->Add(unsorted_run_->TotalBytes());
  run_sizes_->UpdateCounter(unsorted_run_->num_tuples());
  unsorted_run_ = nullptr;

  RETURN_IF_CANCELLED(state_);
  return Status::OK();
}

int Sorter::MaxRunsInNextMerge() const {
  int num_available_buffers = buffer_pool_client_->GetUnusedReservation() / page_len_;
  DCHECK_GE(num_available_buffers, ComputeMinReservation() / page_len_);
  int num_runs_in_one_merge = 0;
  int num_required_buffers = 0;

  for (int i = 0; i < sorted_runs_.size(); ++i) {
    int num_buffers_for_this_run = (sorted_runs_[i]->HasVarLenPages()) ? 2 : 1;

    if (num_required_buffers + num_buffers_for_this_run <= num_available_buffers) {
      num_required_buffers += num_buffers_for_this_run;
      ++num_runs_in_one_merge;
    } else {
      // Not enough buffers to merge all the runs in one final merge. Intermediate merge
      // is required.
      // Increasing the required buffers count to include the result run of the
      // intermediate merge.
      num_required_buffers +=
          (output_row_desc_->tuple_descriptors()[0]->HasVarlenSlots()) ? 2 : 1;
      // Have to reduce the number of runs for this merge to fit in the available buffer
      // pool memory.
      for (int j = i - 1; j >= 0; --j) {
        num_required_buffers -= sorted_runs_[j]->HasVarLenPages() ? 2 : 1;
        --num_runs_in_one_merge;
        if (num_required_buffers <= num_available_buffers) break;
      }
      DCHECK_LE(num_required_buffers, num_available_buffers);
      break;
    }
  }

  DCHECK_GT(num_runs_in_one_merge, 1);
  return num_runs_in_one_merge;
}

void Sorter::TryToIncreaseMemAllocationForMerge() {
  int pages_needed_for_full_merge = 0;
  for (auto run : sorted_runs_) {
    pages_needed_for_full_merge += (run->HasVarLenPages()) ? 2 : 1;
  }
  int available_pages = buffer_pool_client_->GetUnusedReservation() / page_len_;

  // Start allocating more pages than available now. Stop once no more memory can be
  // allocated.
  for (int i = 0; i < pages_needed_for_full_merge - available_pages; ++i) {
    if (!buffer_pool_client_->IncreaseReservation(page_len_)) return;
  }
}

int Sorter::GetNumOfRunsForMerge() const {
  int max_runs_in_next_merge = MaxRunsInNextMerge();

  // Check if all the runs fit in a final merge. Won't need an extra run for the output
  // compared to an intermediate run.
  if (max_runs_in_next_merge == sorted_runs_.size()) {
    return max_runs_in_next_merge;
  }

  // If this is the last intermediate merge before the final merge then distributes the
  // runs between the intermediate and the final merge to saturate the final merge with
  // as many runs as possible reducing the number of merging on the same rows.
  if (max_runs_in_next_merge * 2 >= sorted_runs_.size()) {
    return sorted_runs_.size() - max_runs_in_next_merge;
  }
  return max_runs_in_next_merge;
}

Status Sorter::MergeIntermediateRuns() {
  DCHECK_GE(sorted_runs_.size(), 2);

  // Attempt to allocate more memory before doing intermediate merges. This may
  // be possible if other operators have relinquished memory after the sort has built
  // its runs.
  TryToIncreaseMemAllocationForMerge();

  while (true) {
    int num_of_runs_to_merge = GetNumOfRunsForMerge();

    DCHECK(merge_output_run_ == nullptr) << "Should have finished previous merge.";
    RETURN_IF_ERROR(CreateMerger(num_of_runs_to_merge));

    // If CreateMerger() consumed all the sorted runs, we have set up the final merge.
    if (sorted_runs_.empty()) return Status::OK();

    merge_output_run_ = run_pool_.Add(
        new Run(this, output_row_desc_->tuple_descriptors()[0], false));
    RETURN_IF_ERROR(merge_output_run_->Init());
    RETURN_IF_ERROR(ExecuteIntermediateMerge(merge_output_run_));
    sorted_runs_.push_back(merge_output_run_);
    merge_output_run_ = nullptr;
  }
  return Status::OK();
}

Status Sorter::CreateMerger(int num_runs) {
  DCHECK_GE(num_runs, 2);
  DCHECK_GE(sorted_runs_.size(), 2);
  // Clean up the runs from the previous merge.
  Run::CleanupRuns(&merging_runs_);

  // TODO: 'deep_copy_input' is set to true, which forces the merger to copy all rows
  // from the runs being merged. This is unnecessary overhead that is not required if we
  // correctly transfer resources.
  merger_.reset(
      new SortedRunMerger(compare_less_than_, output_row_desc_, profile_, true));

  vector<function<Status (RowBatch**)>> merge_runs;
  merge_runs.reserve(num_runs);
  for (int i = 0; i < num_runs; ++i) {
    Run* run = sorted_runs_.front();
    RETURN_IF_ERROR(run->PrepareRead());

    // Run::GetNextBatch() is used by the merger to retrieve a batch of rows to merge
    // from this run.
    merge_runs.emplace_back(bind<Status>(mem_fn(&Run::GetNextBatch), run, _1));
    sorted_runs_.pop_front();
    merging_runs_.push_back(run);
  }
  RETURN_IF_ERROR(merger_->Prepare(merge_runs));

  num_merges_counter_->Add(1);
  return Status::OK();
}

Status Sorter::ExecuteIntermediateMerge(Sorter::Run* merged_run) {
  RowBatch intermediate_merge_batch(
      output_row_desc_, state_->batch_size(), mem_tracker_);
  bool eos = false;
  while (!eos) {
    // Copy rows into the new run until done.
    int num_copied;
    RETURN_IF_CANCELLED(state_);
    // Clear any temporary allocations made by the merger.
    expr_results_pool_.Clear();
    RETURN_IF_ERROR(merger_->GetNext(&intermediate_merge_batch, &eos));
    RETURN_IF_ERROR(
        merged_run->AddIntermediateBatch(&intermediate_merge_batch, 0, &num_copied));

    DCHECK_EQ(num_copied, intermediate_merge_batch.num_rows());
    intermediate_merge_batch.Reset();
  }

  RETURN_IF_ERROR(merged_run->FinalizeInput());
  return Status::OK();
}

bool Sorter::HasSpilledRuns() const {
  // All runs in 'merging_runs_' are spilled. 'sorted_runs_' can contain at most one
  // non-spilled run.
  return !merging_runs_.empty() || sorted_runs_.size() > 1 ||
      (sorted_runs_.size() == 1 && !sorted_runs_.back()->is_pinned());
}
} // namespace impala
