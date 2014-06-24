// Copyright 2013 Cloudera Inc.
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

#include "runtime/buffered-tuple-stream.h"

#include <boost/bind.hpp>

#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"

using namespace boost;
using namespace impala;
using namespace std;

BufferedTupleStream::BufferedTupleStream(RuntimeState* state,
    const RowDescriptor& row_desc, BufferedBlockMgr* block_mgr,
    BufferedBlockMgr::Client* client)
  : state_(state),
    desc_(row_desc),
    block_mgr_(block_mgr),
    block_mgr_client_(client),
    current_block_(NULL),
    read_ptr_(NULL),
    read_bytes_left_(0),
    rows_returned_(0),
    num_rows_(0),
    pinned_(false),
    auto_unpin_(false) {
  fixed_tuple_row_size_ = 0;
  for (int i = 0; i < desc_.tuple_descriptors().size(); ++i) {
    fixed_tuple_row_size_ += desc_.tuple_descriptors()[i]->byte_size();
    const TupleDescriptor* tuple_desc = desc_.tuple_descriptors()[i];
    if (tuple_desc->string_slots().empty()) continue;
    string_slots_.push_back(make_pair(i, tuple_desc->string_slots()));
  }
}

Status BufferedTupleStream::Init() {
  bool got_block = false;;
  RETURN_IF_ERROR(NewBlockForWrite(&got_block));
  if (!got_block) return Status("Not enough memory to initialize BufferedTupleStream.");
  DCHECK(current_block_ != NULL);
  return Status::OK;
}

void BufferedTupleStream::Close() {
  ScopedSpinLock l(&lock_);
  if (current_block_ != NULL) {
    current_block_->Delete();
    current_block_ = NULL;
  }

  for (list<BufferedBlockMgr::Block*>::iterator it = pinned_blocks_.begin();
      it != pinned_blocks_.end(); ++it) {
    (*it)->Delete();
  }
  pinned_blocks_.clear();
  for (list<BufferedBlockMgr::Block*>::iterator it = unpinned_blocks_.begin();
      it != unpinned_blocks_.end(); ++it) {
    (*it)->Delete();
  }
  unpinned_blocks_.clear();
}

int64_t BufferedTupleStream::bytes_in_mem(bool ignore_current) const {
  int num_in_mem_blocks = pinned_blocks_.size();
  if (current_block_ != NULL && !ignore_current) {
    DCHECK(current_block_->is_pinned());
    ++num_in_mem_blocks;
  }
  return num_in_mem_blocks * block_mgr_->block_size();
}

int64_t BufferedTupleStream::bytes_unpinned() const {
  return unpinned_blocks_.size() * block_mgr_->block_size();
}

Status BufferedTupleStream::NewBlockForWrite(bool* got_block) {
  if (current_block_ != NULL) {
    DCHECK(current_block_->is_pinned());
    if (auto_unpin_) {
      current_block_->Unpin();
      unpinned_blocks_.push_back(current_block_);
    } else {
      pinned_blocks_.push_back(current_block_);
    }
    current_block_ = NULL;
  }

  BufferedBlockMgr::Block* new_block = NULL;
  RETURN_IF_ERROR(block_mgr_->GetNewBlock(block_mgr_client_, &new_block));
  *got_block = (new_block != NULL);

  if (!*got_block) return Status::OK;
  current_block_ = new_block;
  DCHECK(current_block_ != NULL);
  return Status::OK;
}

Status BufferedTupleStream::NextBlockForRead() {
  ScopedSpinLock l(&lock_);
  DCHECK(current_block_ != NULL);
  if (!pinned_) RETURN_IF_ERROR(current_block_->Delete());

  // Start with blocks that are already pinned.
  if (!pinned_blocks_.empty()) {
    current_block_ = pinned_blocks_.front();
    pinned_blocks_.pop_front();
  } else if (!unpinned_blocks_.empty()) {
    current_block_ = unpinned_blocks_.front();
    unpinned_blocks_.pop_front();
    bool pinned;
    RETURN_IF_ERROR(current_block_->Pin(&pinned));
    DCHECK(pinned) << "Should have reserved enough blocks";
  }
  if (current_block_ != NULL) {
    DCHECK(current_block_->is_pinned());
    read_ptr_ = current_block_->buffer();
    read_bytes_left_ = current_block_->valid_data_len();
  }
  return Status::OK;
}

Status BufferedTupleStream::PrepareForRead(bool pin, bool* pinned) {
  if (pin) {
    DCHECK(pinned != NULL);
    ScopedSpinLock l(&lock_);
    *pinned = true;
    for (list<BufferedBlockMgr::Block*>::iterator it = unpinned_blocks_.begin();
        it != unpinned_blocks_.end(); ++it) {
      RETURN_IF_ERROR((*it)->Pin(pinned));
      if (!*pinned) return Status::OK;
      pinned_blocks_.push_back(*it);
    }
    unpinned_blocks_.clear();
    *pinned = true;
    pinned_ = true;
  }

  DCHECK(read_ptr_ == NULL);
  DCHECK(current_block_ != NULL);
  read_ptr_ = current_block_->buffer();
  read_bytes_left_ = current_block_->valid_data_len();
  rows_returned_ = 0;
  return Status::OK;
}

Status BufferedTupleStream::Unpin() {
  ScopedSpinLock l(&lock_);
  for (list<BufferedBlockMgr::Block*>::iterator it = pinned_blocks_.begin();
      it != pinned_blocks_.end(); ++it) {
    RETURN_IF_ERROR((*it)->Unpin());
    unpinned_blocks_.push_back(*it);
  }
  pinned_blocks_.clear();
  pinned_ = false;
  auto_unpin_ = true;
  return Status::OK;
}

Status BufferedTupleStream::GetNext(RowBatch* batch, bool* eos) {
  DCHECK(batch->row_desc().Equals(desc_));
  DCHECK_EQ(batch->num_rows(), 0);
  *eos = (rows_returned_ == num_rows_);
  int64_t rows_left = num_rows_ - rows_returned_;

  int rows_to_fill = std::min(static_cast<int64_t>(batch->capacity()), rows_left);
  batch->AddRows(rows_to_fill);
  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->GetRow(0));

  if (UNLIKELY(read_bytes_left_ < fixed_tuple_row_size_)) {
    // Get the next block in the stream. We need to do this at the beginning of
    // the GetNext() call to ensure the buffer management semantics. NextBlockForRead()
    // will recycle the memory for the rows returned from the *previous* call to
    // GetNext().
    RETURN_IF_ERROR(NextBlockForRead());
  }

  int i = 0;
  // Produce tuple rows from the current block.
  for (; i < rows_to_fill; ++i) {
    DCHECK(current_block_ != NULL);
    DCHECK(read_ptr_ != NULL);
    // Check if current block is done.
    if (UNLIKELY(read_bytes_left_ < fixed_tuple_row_size_)) break;

    // Copy the row into the output batch.
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    for (int j = 0; j < desc_.tuple_descriptors().size(); ++j) {
      row->SetTuple(j, reinterpret_cast<Tuple*>(read_ptr_));
      read_ptr_ += desc_.tuple_descriptors()[j]->byte_size();
    }
    read_bytes_left_ -= fixed_tuple_row_size_;
    tuple_row_mem += sizeof(Tuple*) * desc_.tuple_descriptors().size();

    // Update string slot ptrs.
    for (int j = 0; j < string_slots_.size(); ++j) {
      Tuple* tuple = row->GetTuple(string_slots_[i].first);
      if (tuple == NULL) continue;
      for (int k = 0; k < string_slots_[j].second.size(); ++k) {
        const SlotDescriptor* slot_desc = string_slots_[i].second[j];
        if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

        StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
        DCHECK(sv->len <= read_bytes_left_);
        sv->ptr = reinterpret_cast<char*>(read_ptr_);
        read_ptr_ += sv->len;
      }
    }
  }

  batch->CommitRows(i);
  rows_returned_ += i;
  *eos = (rows_returned_ == num_rows_);
  return Status::OK;
}
