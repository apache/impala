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
#include "util/debug-util.h"

using namespace boost;
using namespace impala;
using namespace std;

BufferedTupleStream::BufferedTupleStream(RuntimeState* state,
    const RowDescriptor& row_desc, BufferedBlockMgr* block_mgr,
    BufferedBlockMgr::Client* client, bool delete_on_read, bool read_write)
  : delete_on_read_(delete_on_read),
    read_write_(read_write),
    state_(state),
    desc_(row_desc),
    block_mgr_(block_mgr),
    block_mgr_client_(client),
    read_ptr_(NULL),
    read_bytes_(0),
    rows_returned_(0),
    write_block_(NULL),
    num_pinned_(0),
    closed_(false),
    num_rows_(0),
    pinned_(true) {
  read_block_ = blocks_.end();
  fixed_tuple_row_size_ = 0;
  for (int i = 0; i < desc_.tuple_descriptors().size(); ++i) {
    fixed_tuple_row_size_ += desc_.tuple_descriptors()[i]->byte_size();
    const TupleDescriptor* tuple_desc = desc_.tuple_descriptors()[i];
    if (tuple_desc->string_slots().empty()) continue;
    string_slots_.push_back(make_pair(i, tuple_desc->string_slots()));
  }
}

// Returns the number of pinned blocks in the list. Only called in DCHECKs to validate
// num_pinned_.
int NumPinned(const list<BufferedBlockMgr::Block*>& blocks) {
  int num_pinned = 0;
  for (list<BufferedBlockMgr::Block*>::const_iterator it = blocks.begin();
      it != blocks.end(); ++it) {
    if ((*it)->is_pinned()) ++num_pinned;
  }
  return num_pinned;
}

string BufferedTupleStream::DebugString() const {
  stringstream ss;
  ss << "BufferedTupleStream num_rows=" << num_rows_ << " rows_returned="
     << rows_returned_ << " pinned=" << (pinned_ ? "true" : "false")
     << " num_pinned=" << num_pinned_
     << " write_block=" << write_block_ << " read_block_=";
  if (read_block_ == blocks_.end()) {
    ss << "<end>";
  } else {
    ss << *read_block_;
  }
  ss << " blocks=[\n";
  for (list<BufferedBlockMgr::Block*>::const_iterator it = blocks_.begin();
      it != blocks_.end(); ++it) {
    ss << "{" << (*it)->DebugString() << "}";
    if (*it != blocks_.back()) ss << ",\n";
  }
  ss << "]";
  return ss.str();
}

Status BufferedTupleStream::Init(bool pinned) {
  bool got_block = false;
  RETURN_IF_ERROR(NewBlockForWrite(&got_block));
  if (!got_block) return Status("Not enough memory to initialize BufferedTupleStream.");
  DCHECK(write_block_ != NULL);
  if (read_write_) RETURN_IF_ERROR(PrepareForRead());
  if (!pinned) RETURN_IF_ERROR(UnpinStream());
  return Status::OK;
}

void BufferedTupleStream::Close() {
  for (list<BufferedBlockMgr::Block*>::iterator it = blocks_.begin();
      it != blocks_.end(); ++it) {
    (*it)->Delete();
  }
  blocks_.clear();
  num_pinned_ = 0;
  DCHECK_EQ(num_pinned_, NumPinned(blocks_));
  closed_ = true;
}

int64_t BufferedTupleStream::bytes_in_mem(bool ignore_current) const {
  int num_in_mem_blocks = num_pinned_;
  if (write_block_ != NULL && ignore_current) {
    DCHECK(write_block_->is_pinned());
    --num_in_mem_blocks;
  }
  return num_in_mem_blocks * block_mgr_->block_size();
}

int64_t BufferedTupleStream::bytes_unpinned() const {
  return (blocks_.size() - num_pinned_) * block_mgr_->block_size();
}

Status BufferedTupleStream::NewBlockForWrite(bool* got_block) {
  DCHECK(!closed_);
  if (write_block_ != NULL) {
    DCHECK(write_block_->is_pinned());
    if (!pinned_ && write_block_ != *read_block_) {
      RETURN_IF_ERROR(write_block_->Unpin());
      --num_pinned_;
      DCHECK_EQ(num_pinned_, NumPinned(blocks_));
    }
    write_block_ = NULL;
  }

  BufferedBlockMgr::Block* new_block = NULL;
  RETURN_IF_ERROR(block_mgr_->GetNewBlock(block_mgr_client_, &new_block));
  *got_block = (new_block != NULL);

  if (!*got_block) return Status::OK;
  blocks_.push_back(new_block);
  write_block_ = new_block;
  DCHECK(write_block_->is_pinned());
  ++num_pinned_;
  DCHECK_EQ(num_pinned_, NumPinned(blocks_));
  return Status::OK;
}

Status BufferedTupleStream::NextBlockForRead() {
  DCHECK(!closed_);
  DCHECK(read_block_ != blocks_.end());
  if (delete_on_read_ && !pinned_) {
    DCHECK(read_block_ == blocks_.begin());
    (*read_block_)->Delete();
    --num_pinned_;
    DCHECK_EQ(num_pinned_, NumPinned(blocks_));
    blocks_.pop_front();
    read_block_ = blocks_.begin();
  } else {
    DCHECK((*read_block_)->is_pinned());
    if (!pinned_) {
      (*read_block_)->Unpin();
      --num_pinned_;
      DCHECK_EQ(num_pinned_, NumPinned(blocks_));
    }
    ++read_block_;
  }

  if (read_block_ != blocks_.end()) {
    if (!(*read_block_)->is_pinned()) {
      DCHECK(!pinned_) << DebugString(); // Should already be pinned if pinned_
      bool pinned;
      RETURN_IF_ERROR((*read_block_)->Pin(&pinned));
      DCHECK(pinned) << "Should have reserved enough blocks";
      ++num_pinned_;
      DCHECK_EQ(num_pinned_, NumPinned(blocks_));
    }
    read_ptr_ = (*read_block_)->buffer();
    read_bytes_ = 0;
  }
  return Status::OK;
}

Status BufferedTupleStream::PrepareForRead(bool* got_buffer) {
  DCHECK(!closed_);
  if (blocks_.empty()) return Status::OK;

  if (!read_write_ && write_block_ != NULL) {
    DCHECK(write_block_->is_pinned());
    if (!pinned_ && write_block_ != blocks_.front()) {
      write_block_->Unpin();
      --num_pinned_;
    }
    write_block_ = NULL;
    DCHECK_EQ(num_pinned_, NumPinned(blocks_));
  }

  read_block_ = blocks_.begin();
  if (!(*read_block_)->is_pinned()) {
    bool current_pinned;
    RETURN_IF_ERROR((*read_block_)->Pin(&current_pinned));
    if (!current_pinned) {
      if (got_buffer == NULL) {
        DCHECK(current_pinned) << "Should have reserved enough blocks";
        return Status::MEM_LIMIT_EXCEEDED;
      } else {
        *got_buffer = false;
        return Status::OK;
      }
    }
    ++num_pinned_;
    DCHECK_EQ(num_pinned_, NumPinned(blocks_));
  }

  DCHECK(read_block_ != blocks_.end());
  read_ptr_ = (*read_block_)->buffer();
  read_bytes_ = 0;
  rows_returned_ = 0;
  if (got_buffer != NULL) *got_buffer = true;
  return Status::OK;
}

Status BufferedTupleStream::PinStream(bool* pinned) {
  DCHECK(!closed_);
  DCHECK(pinned != NULL);
  *pinned = true;

  for (list<BufferedBlockMgr::Block*>::iterator it = blocks_.begin();
      it != blocks_.end(); ++it) {
    if ((*it)->is_pinned()) continue;
    RETURN_IF_ERROR((*it)->Pin(pinned));
    if (!*pinned) {
      UnpinStream(true);
      return Status::OK;
    }
    ++num_pinned_;
    DCHECK_EQ(num_pinned_, NumPinned(blocks_));
  }
  *pinned = true;
  pinned_ = true;
  return Status::OK;
}

Status BufferedTupleStream::UnpinStream(bool all) {
  DCHECK(!closed_);
  for (list<BufferedBlockMgr::Block*>::iterator it = blocks_.begin();
      it != blocks_.end(); ++it) {
    if (!(*it)->is_pinned()) continue;
    if (!all && (*it == write_block_ || (read_write_ && it == read_block_))) continue;
    RETURN_IF_ERROR((*it)->Unpin());
    --num_pinned_;
    DCHECK_EQ(num_pinned_, NumPinned(blocks_));
  }
  if (all) {
    read_block_ = blocks_.end();
    write_block_ = NULL;
  }
  pinned_ = false;
  return Status::OK;
}

Status BufferedTupleStream::GetNext(RowBatch* batch, bool* eos) {
  DCHECK(!closed_);
  DCHECK(batch->row_desc().Equals(desc_));
  DCHECK_EQ(batch->num_rows(), 0);
  *eos = (rows_returned_ == num_rows_);
  if (*eos) return Status::OK;

  int64_t rows_left = num_rows_ - rows_returned_;
  int rows_to_fill = std::min(static_cast<int64_t>(batch->capacity()), rows_left);
  batch->AddRows(rows_to_fill);
  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->GetRow(0));

  int64_t data_len = (*read_block_)->valid_data_len();
  if (UNLIKELY((data_len - read_bytes_) < fixed_tuple_row_size_)) {
    // Get the next block in the stream. We need to do this at the beginning of
    // the GetNext() call to ensure the buffer management semantics. NextBlockForRead()
    // will recycle the memory for the rows returned from the *previous* call to
    // GetNext().
    RETURN_IF_ERROR(NextBlockForRead());
    data_len = (*read_block_)->valid_data_len();
  }

  DCHECK(read_block_ != blocks_.end());
  DCHECK((*read_block_)->is_pinned());
  DCHECK(read_ptr_ != NULL);

  int i = 0;
  // Produce tuple rows from the current block.
  for (; i < rows_to_fill; ++i) {
    // Check if current block is done.
    if (UNLIKELY((data_len - read_bytes_) < fixed_tuple_row_size_)) break;

    // Copy the row into the output batch.
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    for (int j = 0; j < desc_.tuple_descriptors().size(); ++j) {
      row->SetTuple(j, reinterpret_cast<Tuple*>(read_ptr_));
      read_ptr_ += desc_.tuple_descriptors()[j]->byte_size();
    }
    read_bytes_ += fixed_tuple_row_size_;
    tuple_row_mem += sizeof(Tuple*) * desc_.tuple_descriptors().size();

    // Update string slot ptrs.
    for (int j = 0; j < string_slots_.size(); ++j) {
      Tuple* tuple = row->GetTuple(string_slots_[j].first);
      if (tuple == NULL) continue;
      for (int k = 0; k < string_slots_[j].second.size(); ++k) {
        const SlotDescriptor* slot_desc = string_slots_[j].second[k];
        if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

        StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
        DCHECK_LE(sv->len, data_len - read_bytes_) << DebugString();
        sv->ptr = reinterpret_cast<char*>(read_ptr_);
        read_ptr_ += sv->len;
        read_bytes_ += sv->len;
      }
    }
  }

  batch->CommitRows(i);
  rows_returned_ += i;
  *eos = (rows_returned_ == num_rows_);
  return Status::OK;
}
