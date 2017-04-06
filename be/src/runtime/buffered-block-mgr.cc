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

#include "runtime/buffered-block-mgr.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/tmp-file-mgr.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/impalad-metrics.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include <gutil/strings/substitute.h>

#include "common/names.h"

using namespace strings; // for Substitute

namespace impala {

BufferedBlockMgr::BlockMgrsMap BufferedBlockMgr::query_to_block_mgrs_;
SpinLock BufferedBlockMgr::static_block_mgrs_lock_;


struct BufferedBlockMgr::Client {
  Client(const string& debug_info, BufferedBlockMgr* mgr, int num_reserved_buffers,
      bool tolerates_oversubscription, MemTracker* tracker,
         RuntimeState* state)
      : debug_info_(debug_info),
        mgr_(mgr),
        state_(state),
        tracker_(tracker),
        query_tracker_(mgr_->mem_tracker_->parent()),
        num_reserved_buffers_(num_reserved_buffers),
        tolerates_oversubscription_(tolerates_oversubscription),
        num_tmp_reserved_buffers_(0),
        num_pinned_buffers_(0),
        logged_large_allocation_warning_(false) {
    DCHECK(tracker != NULL);
  }

  /// A string that will be printed to identify the client, e.g. which exec node it
  /// belongs to.
  string debug_info_;

  /// Unowned.
  BufferedBlockMgr* mgr_;

  /// Unowned.
  RuntimeState* state_;

  /// Tracker for this client. Unowned.
  /// When the client gets a buffer, we update the consumption on this tracker. However,
  /// we don't want to transfer the buffer from the block mgr to the client (i.e. release
  /// from the block mgr), since the block mgr is where the block mem usage limit is
  /// enforced. Even when we give a buffer to a client, the buffer is still owned and
  /// counts against the block mgr tracker (i.e. there is a fixed pool of buffers
  /// regardless of if they are in the block mgr or the clients).
  MemTracker* tracker_;

  /// This is the common ancestor between the block mgr tracker and the client tracker.
  /// When memory is transferred to the client, we want it to stop at this tracker.
  MemTracker* query_tracker_;

  /// Number of buffers reserved by this client.
  int num_reserved_buffers_;

  /// If false, return MEM_LIMIT_EXCEEDED when a reserved buffer cannot be allocated.
  /// If true, return Status::OK() as with a non-reserved buffer.
  bool tolerates_oversubscription_;

  /// Number of buffers temporarily reserved.
  int num_tmp_reserved_buffers_;

  /// Number of buffers pinned by this client.
  int num_pinned_buffers_;

  /// Whether a warning about a large allocation has been made for this client. Used
  /// to avoid producing excessive log messages.
  bool logged_large_allocation_warning_;

  void PinBuffer(BufferDescriptor* buffer) {
    DCHECK(buffer != NULL);
    if (buffer->len == mgr_->max_block_size()) {
      ++num_pinned_buffers_;
      tracker_->ConsumeLocal(buffer->len, query_tracker_);
    }
  }

  void UnpinBuffer(BufferDescriptor* buffer) {
    DCHECK(buffer != NULL);
    if (buffer->len == mgr_->max_block_size()) {
      DCHECK_GT(num_pinned_buffers_, 0);
      --num_pinned_buffers_;
      tracker_->ReleaseLocal(buffer->len, query_tracker_);
    }
  }

  string DebugString() const {
    stringstream ss;
    ss << "Client " << this << endl
       << " " << debug_info_ << endl
       << "  num_reserved_buffers=" << num_reserved_buffers_ << endl
       << "  num_tmp_reserved_buffers=" << num_tmp_reserved_buffers_ << endl
       << "  num_pinned_buffers=" << num_pinned_buffers_;
    return ss.str();
  }
};

// BufferedBlockMgr::Block methods.
BufferedBlockMgr::Block::Block(BufferedBlockMgr* block_mgr)
  : buffer_desc_(NULL),
    block_mgr_(block_mgr),
    client_(NULL),
    valid_data_len_(0),
    num_rows_(0) {}

Status BufferedBlockMgr::Block::Pin(bool* pinned, Block* release_block, bool unpin) {
  return block_mgr_->PinBlock(this, pinned, release_block, unpin);
}

Status BufferedBlockMgr::Block::Unpin() {
  return block_mgr_->UnpinBlock(this);
}

void BufferedBlockMgr::Block::Delete() {
  block_mgr_->DeleteBlock(this);
}

void BufferedBlockMgr::Block::Init() {
  // No locks are taken because the block is new or has previously been deleted.
  is_pinned_ = false;
  in_write_ = false;
  is_deleted_ = false;
  valid_data_len_ = 0;
  client_ = NULL;
  num_rows_ = 0;
}

bool BufferedBlockMgr::Block::Validate() const {
  if (is_deleted_ && (is_pinned_ || (!in_write_ && buffer_desc_ != NULL))) {
    LOG(ERROR) << "Deleted block in use - " << DebugString();
    return false;
  }

  if (buffer_desc_ == NULL && (is_pinned_ || in_write_)) {
    LOG(ERROR) << "Block without buffer in use - " << DebugString();
    return false;
  }

  if (buffer_desc_ == NULL && block_mgr_->unpinned_blocks_.Contains(this)) {
    LOG(ERROR) << "Unpersisted block without buffer - " << DebugString();
    return false;
  }

  if (buffer_desc_ != NULL && (buffer_desc_->block != this)) {
    LOG(ERROR) << "Block buffer inconsistency - " << DebugString();
    return false;
  }

  return true;
}

string BufferedBlockMgr::Block::TmpFilePath() const {
  if (write_handle_ == NULL) return "";
  return write_handle_->TmpFilePath();
}

string BufferedBlockMgr::Block::DebugString() const {
  stringstream ss;
  ss << "Block: " << this << endl
     << "  Buffer Desc: " << buffer_desc_ << endl
     << "  Data Len: " << valid_data_len_ << endl
     << "  Num Rows: " << num_rows_ << endl;
  if (is_pinned_) ss << "  Buffer Len: " << buffer_len() << endl;
  ss << "  Deleted: " << is_deleted_ << endl
     << "  Pinned: " << is_pinned_ << endl
     << "  Write Issued: " << in_write_ << endl
     << "  Client Local: " << client_local_ << endl;
  if (write_handle_ != NULL) {
    ss << "  Write handle: " << write_handle_->DebugString() << endl;
  }
  if (client_ != NULL) ss << "  Client: " << client_->DebugString();
  return ss.str();
}

BufferedBlockMgr::BufferedBlockMgr(RuntimeState* state, TmpFileMgr* tmp_file_mgr,
    int64_t block_size, int64_t scratch_limit)
  : max_block_size_(block_size),
    // Keep two writes in flight per scratch disk so the disks can stay busy.
    block_write_threshold_(tmp_file_mgr->NumActiveTmpDevices() * 2),
    disable_spill_(state->query_ctx().disable_spilling || block_write_threshold_ == 0
        || scratch_limit == 0),
    query_id_(state->query_id()),
    initialized_(false),
    unfullfilled_reserved_buffers_(0),
    total_pinned_buffers_(0),
    non_local_outstanding_writes_(0),
    tmp_file_group_(NULL),
    is_cancelled_(false),
    writes_issued_(0),
    debug_write_delay_ms_(0) {}

Status BufferedBlockMgr::Create(RuntimeState* state, MemTracker* parent,
    RuntimeProfile* profile, TmpFileMgr* tmp_file_mgr, int64_t mem_limit,
    int64_t block_size, shared_ptr<BufferedBlockMgr>* block_mgr) {
  DCHECK(parent != NULL);
  int64_t scratch_limit = state->query_options().scratch_limit;
  block_mgr->reset();
  {
    lock_guard<SpinLock> lock(static_block_mgrs_lock_);
    BlockMgrsMap::iterator it = query_to_block_mgrs_.find(state->query_id());
    if (it != query_to_block_mgrs_.end()) *block_mgr = it->second.lock();
    if (*block_mgr == NULL) {
      // weak_ptr::lock returns NULL if the weak_ptr is expired. This means
      // all shared_ptr references have gone to 0 and it is in the process of
      // being deleted. This can happen if the last shared reference is released
      // but before the weak ptr is removed from the map.
      block_mgr->reset(
          new BufferedBlockMgr(state, tmp_file_mgr, block_size, scratch_limit));
      query_to_block_mgrs_[state->query_id()] = *block_mgr;
    }
  }
  (*block_mgr)
      ->Init(state->io_mgr(), tmp_file_mgr, profile, parent, mem_limit, scratch_limit);
  return Status::OK();
}

int64_t BufferedBlockMgr::available_buffers(Client* client) const {
  int64_t unused_reserved = client->num_reserved_buffers_ +
      client->num_tmp_reserved_buffers_ - client->num_pinned_buffers_;
  return max<int64_t>(0, remaining_unreserved_buffers()) +
      max<int64_t>(0, unused_reserved);
}

int64_t BufferedBlockMgr::remaining_unreserved_buffers() const {
  int64_t num_buffers = free_io_buffers_.size() +
      unpinned_blocks_.size() + non_local_outstanding_writes_;
  num_buffers += mem_tracker_->SpareCapacity() / max_block_size();
  num_buffers -= unfullfilled_reserved_buffers_;
  return num_buffers;
}

Status BufferedBlockMgr::RegisterClient(const string& debug_info,
    int num_reserved_buffers, bool tolerates_oversubscription, MemTracker* tracker,
    RuntimeState* state, Client** client) {
  DCHECK_GE(num_reserved_buffers, 0);
  Client* aClient = new Client(debug_info, this, num_reserved_buffers,
      tolerates_oversubscription, tracker, state);
  lock_guard<mutex> lock(lock_);
  *client = obj_pool_.Add(aClient);
  unfullfilled_reserved_buffers_ += num_reserved_buffers;
  return Status::OK();
}

void BufferedBlockMgr::ClearReservations(Client* client) {
  lock_guard<mutex> lock(lock_);
  if (client->num_pinned_buffers_ < client->num_reserved_buffers_) {
    unfullfilled_reserved_buffers_ -=
        client->num_reserved_buffers_ - client->num_pinned_buffers_;
  }
  client->num_reserved_buffers_ = 0;

  unfullfilled_reserved_buffers_ -= client->num_tmp_reserved_buffers_;
  client->num_tmp_reserved_buffers_ = 0;
}

bool BufferedBlockMgr::TryAcquireTmpReservation(Client* client, int num_buffers) {
  lock_guard<mutex> lock(lock_);
  DCHECK_EQ(client->num_tmp_reserved_buffers_, 0);
  if (client->num_pinned_buffers_ < client->num_reserved_buffers_) {
    // If client has unused reserved buffers, we use those first.
    num_buffers -= client->num_reserved_buffers_ - client->num_pinned_buffers_;
  }
  if (num_buffers < 0) return true;
  if (available_buffers(client) < num_buffers) return false;

  client->num_tmp_reserved_buffers_ = num_buffers;
  unfullfilled_reserved_buffers_ += num_buffers;
  return true;
}

bool BufferedBlockMgr::ConsumeMemory(Client* client, int64_t size) {
  int64_t buffers_needed = BitUtil::Ceil(size, max_block_size());
  if (UNLIKELY(!BitUtil::IsNonNegative32Bit(buffers_needed))) {
    VLOG_QUERY << "Trying to consume " << size << " which is out of range.";
    return false;
  }
  DCHECK_GT(buffers_needed, 0) << "Trying to consume 0 memory";

  unique_lock<mutex> lock(lock_);
  if (size < max_block_size() && mem_tracker_->TryConsume(size)) {
    // For small allocations (less than a block size), just let the allocation through.
    client->tracker_->ConsumeLocal(size, client->query_tracker_);
    return true;
  }

  if (max<int64_t>(0, remaining_unreserved_buffers()) +
      client->num_tmp_reserved_buffers_ < buffers_needed) {
    return false;
  }

  if (mem_tracker_->TryConsume(size)) {
    // There was still unallocated memory, don't need to recycle allocated blocks.
    client->tracker_->ConsumeLocal(size, client->query_tracker_);
    return true;
  }

  // Bump up client->num_tmp_reserved_buffers_ to satisfy this request. We don't want
  // another client to grab the buffer.
  int additional_tmp_reservations = 0;
  if (client->num_tmp_reserved_buffers_ < buffers_needed) {
    additional_tmp_reservations = buffers_needed - client->num_tmp_reserved_buffers_;
    client->num_tmp_reserved_buffers_ += additional_tmp_reservations;
    unfullfilled_reserved_buffers_ += additional_tmp_reservations;
  }

  // Loop until we have freed enough memory.
  // We free all the memory at the end. We don't want another component to steal the
  // memory.
  int buffers_acquired = 0;
  do {
    BufferDescriptor* buffer_desc = NULL;
    Status s = FindBuffer(lock, &buffer_desc); // This waits on the lock.
    if (buffer_desc == NULL) break;
    DCHECK(s.ok());
    all_io_buffers_.erase(buffer_desc->all_buffers_it);
    if (buffer_desc->block != NULL) buffer_desc->block->buffer_desc_ = NULL;
    delete[] buffer_desc->buffer;
    ++buffers_acquired;
  } while (buffers_acquired != buffers_needed);

  Status status = Status::OK();
  if (buffers_acquired == buffers_needed) status = WriteUnpinnedBlocks();
  // If we either couldn't acquire enough buffers or WriteUnpinnedBlocks() failed, undo
  // the reservation.
  if (buffers_acquired != buffers_needed || !status.ok()) {
    if (!status.ok() && !status.IsCancelled()) {
      VLOG_QUERY << "Query: " << query_id_ << " write unpinned buffers failed.";
      client->state_->LogError(status.msg());
    }
    client->num_tmp_reserved_buffers_ -= additional_tmp_reservations;
    unfullfilled_reserved_buffers_ -= additional_tmp_reservations;
    mem_tracker_->Release(buffers_acquired * max_block_size());
    return false;
  }

  client->num_tmp_reserved_buffers_ -= buffers_acquired;
  unfullfilled_reserved_buffers_ -= buffers_acquired;

  DCHECK_GE(buffers_acquired * max_block_size(), size);
  mem_tracker_->Release(buffers_acquired * max_block_size());
  if (!mem_tracker_->TryConsume(size)) return false;
  client->tracker_->ConsumeLocal(size, client->query_tracker_);
  DCHECK(Validate()) << endl << DebugInternal();
  return true;
}

void BufferedBlockMgr::ReleaseMemory(Client* client, int64_t size) {
  mem_tracker_->Release(size);
  client->tracker_->ReleaseLocal(size, client->query_tracker_);
}

void BufferedBlockMgr::Cancel() {
  {
    lock_guard<mutex> lock(lock_);
    if (is_cancelled_) return;
    is_cancelled_ = true;
  }
}

bool BufferedBlockMgr::IsCancelled() {
  lock_guard<mutex> lock(lock_);
  return is_cancelled_;
}

Status BufferedBlockMgr::MemLimitTooLowError(Client* client, int node_id) {
  VLOG_QUERY << "Query: " << query_id_ << ". Node=" << node_id
             << " ran out of memory: " << endl
             << DebugInternal() << endl << client->DebugString();
  int64_t min_memory = client->num_reserved_buffers_ * max_block_size();
  string msg = Substitute(
      "The memory limit is set too low to initialize spilling operator (id=$0). The "
      "minimum required memory to spill this operator is $1.",
      node_id, PrettyPrinter::Print(min_memory, TUnit::BYTES));
  return client->tracker_->MemLimitExceeded(client->state_, msg);
}

Status BufferedBlockMgr::GetNewBlock(Client* client, Block* unpin_block, Block** block,
    int64_t len) {
  DCHECK_LE(len, max_block_size_) << "Cannot request block bigger than max_len";
  DCHECK_NE(len, 0) << "Cannot request block of zero size";
  *block = NULL;
  Block* new_block = NULL;
  Status status;

  {
    lock_guard<mutex> lock(lock_);
    if (is_cancelled_) return Status::CANCELLED;
    new_block = GetUnusedBlock(client);
    DCHECK(new_block->Validate()) << endl << new_block->DebugString();
    DCHECK_EQ(new_block->client_, client);
    DCHECK_NE(new_block, unpin_block);

    if (len > 0 && len < max_block_size_) {
      DCHECK(unpin_block == NULL);
      if (client->tracker_->TryConsume(len)) {
        uint8_t* buffer = new uint8_t[len];
        // Descriptors for non-I/O sized buffers are deleted when the block is deleted.
        new_block->buffer_desc_ = new BufferDescriptor(buffer, len);
        new_block->buffer_desc_->block = new_block;
        new_block->is_pinned_ = true;
        client->PinBuffer(new_block->buffer_desc_);
        ++total_pinned_buffers_;
        *block = new_block;
        return Status::OK();
      } else {
        status = Status::OK();
        goto no_buffer_avail;
      }
    }
  }

  bool in_mem;
  status = FindBufferForBlock(new_block, &in_mem);
  if (!status.ok()) goto no_buffer_avail;
  DCHECK(!in_mem) << "A new block cannot start in mem.";
  DCHECK(!new_block->is_pinned() || new_block->buffer_desc_ != NULL)
      << new_block->DebugString();

  if (!new_block->is_pinned()) {
    if (unpin_block == NULL) {
      // We couldn't get a new block and no unpin block was provided. Can't return
      // a block.
      status = Status::OK();
      goto no_buffer_avail;
    } else {
      // We need to transfer the buffer from unpin_block to new_block.
      status = TransferBuffer(new_block, unpin_block, true);
      if (!status.ok()) goto no_buffer_avail;
    }
  } else if (unpin_block != NULL) {
    // Got a new block without needing to transfer. Just unpin this block.
    status = unpin_block->Unpin();
    if (!status.ok()) goto no_buffer_avail;
  }

  DCHECK(new_block->is_pinned());
  *block = new_block;
  return Status::OK();

no_buffer_avail:
  DCHECK(new_block != NULL);
  DeleteBlock(new_block);
  return status;
}

Status BufferedBlockMgr::TransferBuffer(Block* dst, Block* src, bool unpin) {
  Status status = Status::OK();
  DCHECK(dst != NULL);
  DCHECK(src != NULL);
  unique_lock<mutex> lock(lock_);

  DCHECK(src->is_pinned_);
  DCHECK(!dst->is_pinned_);
  DCHECK(dst->buffer_desc_ == NULL);
  DCHECK_EQ(src->buffer_desc_->len, max_block_size_);

  // Ensure that there aren't any writes in flight for 'src'.
  WaitForWrite(lock, src);
  src->is_pinned_ = false;

  if (unpin) {
    // First write out the src block so we can grab its buffer.
    src->client_local_ = true;
    status = WriteUnpinnedBlock(src);
    if (!status.ok()) {
      // The transfer failed, return the buffer to src.
      src->is_pinned_ = true;
      return status;
    }
    // Wait for the write to complete.
    WaitForWrite(lock, src);
    if (is_cancelled_) {
      // We can't be sure the write succeeded, so return the buffer to src.
      src->is_pinned_ = true;
      return Status::CANCELLED;
    }
    DCHECK(!src->in_write_);
  }
  // Assign the buffer to the new block.
  dst->buffer_desc_ = src->buffer_desc_;
  dst->buffer_desc_->block = dst;
  src->buffer_desc_ = NULL;
  dst->is_pinned_ = true;
  if (!unpin) DeleteBlockLocked(lock, src);
  return Status::OK();
}

BufferedBlockMgr::~BufferedBlockMgr() {
  shared_ptr<BufferedBlockMgr> other_mgr_ptr;
  {
    lock_guard<SpinLock> lock(static_block_mgrs_lock_);
    BlockMgrsMap::iterator it = query_to_block_mgrs_.find(query_id_);
    // IMPALA-2286: Another fragment may have called Create() for this query_id_ and
    // saw that this BufferedBlockMgr is being destructed.  That fragement will
    // overwrite the map entry for query_id_, pointing it to a different
    // BufferedBlockMgr object.  We should let that object's destructor remove the
    // entry.  On the other hand, if the second BufferedBlockMgr is destructed before
    // this thread acquires the lock, then we'll remove the entry (because we can't
    // distinguish between the two expired pointers), and when the other
    // ~BufferedBlockMgr() call occurs, it won't find an entry for this query_id_.
    if (it != query_to_block_mgrs_.end()) {
      other_mgr_ptr = it->second.lock();
      if (other_mgr_ptr.get() == NULL) {
        // The BufferBlockMgr object referenced by this entry is being deconstructed.
        query_to_block_mgrs_.erase(it);
      } else {
        // The map references another (still valid) BufferedBlockMgr.
        DCHECK_NE(other_mgr_ptr.get(), this);
      }
    }
  }
  // IMPALA-4274: releasing the reference count can recursively call ~BufferedBlockMgr().
  // Do not do that with 'static_block_mgrs_lock_' held.
  other_mgr_ptr.reset();

  // Delete tmp files and cancel any in-flight writes.
  tmp_file_group_->Close();

  // If there are any outstanding writes and we are here it means that when the
  // WriteComplete() callback gets executed it is going to access invalid memory.
  // See IMPALA-1890.
  DCHECK_EQ(non_local_outstanding_writes_, 0) << endl << DebugInternal();

  // Validate that clients deleted all of their blocks. Since all writes have
  // completed at this point, any deleted blocks should be in unused_blocks_.
  for (auto it = all_blocks_.begin(); it != all_blocks_.end(); ++it) {
    Block* block = *it;
    DCHECK(block->Validate()) << block->DebugString();
    DCHECK(unused_blocks_.Contains(block)) << block->DebugString();
  }

  // Free memory resources.
  for (BufferDescriptor* buffer: all_io_buffers_) {
    mem_tracker_->Release(buffer->len);
    delete[] buffer->buffer;
  }
  DCHECK_EQ(mem_tracker_->consumption(), 0);
  mem_tracker_->UnregisterFromParent();
  mem_tracker_.reset();
}

int64_t BufferedBlockMgr::bytes_allocated() const {
  return mem_tracker_->consumption();
}

int BufferedBlockMgr::num_pinned_buffers(Client* client) const {
  return client->num_pinned_buffers_;
}

int BufferedBlockMgr::num_reserved_buffers_remaining(Client* client) const {
  return max<int>(client->num_reserved_buffers_ - client->num_pinned_buffers_, 0);
}

MemTracker* BufferedBlockMgr::get_tracker(Client* client) const {
  return client->tracker_;
}

int64_t BufferedBlockMgr::GetNumWritesOutstanding() {
  // Acquire lock to avoid returning mid-way through WriteComplete() when the
  // state may be inconsistent.
  lock_guard<mutex> lock(lock_);
  return profile()->GetCounter("BlockWritesOutstanding")->value();
}

Status BufferedBlockMgr::DeleteOrUnpinBlock(Block* block, bool unpin) {
  if (block == NULL) {
    return IsCancelled() ? Status::CANCELLED : Status::OK();
  }
  if (unpin) {
    return block->Unpin();
  } else {
    block->Delete();
    return IsCancelled() ? Status::CANCELLED : Status::OK();
  }
}

Status BufferedBlockMgr::PinBlock(Block* block, bool* pinned, Block* release_block,
    bool unpin) {
  DCHECK(block != NULL);
  DCHECK(!block->is_deleted_);
  Status status;
  *pinned = false;
  if (block->is_pinned_) {
    *pinned = true;
    return DeleteOrUnpinBlock(release_block, unpin);
  }

  bool in_mem = false;
  status = FindBufferForBlock(block, &in_mem);
  if (!status.ok()) goto error;
  *pinned = block->is_pinned_;

  if (in_mem) {
    // The block's buffer is still in memory with the original data.
    status = CancelWrite(block);
    if (!status.ok()) goto error;
    return DeleteOrUnpinBlock(release_block, unpin);
  }

  if (!block->is_pinned_) {
    if (release_block == NULL) return Status::OK();

    if (block->buffer_desc_ != NULL) {
      // The block's buffer is still in memory but we couldn't get an additional buffer
      // because it would eat into another client's reservation. However, we can use
      // release_block's reservation, so reclaim the buffer.
      {
        lock_guard<mutex> lock(lock_);
        if (free_io_buffers_.Contains(block->buffer_desc_)) {
          DCHECK(!block->is_pinned_ && !block->in_write_ &&
                 !unpinned_blocks_.Contains(block)) << endl << block->DebugString();
          free_io_buffers_.Remove(block->buffer_desc_);
        } else if (unpinned_blocks_.Contains(block)) {
          unpinned_blocks_.Remove(block);
        } else {
          DCHECK(block->in_write_);
        }
        block->is_pinned_ = true;
        *pinned = true;
        block->client_->PinBuffer(block->buffer_desc_);
        ++total_pinned_buffers_;
        status = WriteUnpinnedBlocks();
        if (!status.ok()) goto error;
      }
      status = CancelWrite(block);
      if (!status.ok()) goto error;
      return DeleteOrUnpinBlock(release_block, unpin);
    }
    // FindBufferForBlock() wasn't able to find a buffer so transfer the one from
    // 'release_block'.
    status = TransferBuffer(block, release_block, unpin);
    if (!status.ok()) goto error;
    DCHECK(!release_block->is_pinned_);
    release_block = NULL; // Handled by transfer.
    DCHECK(block->is_pinned_);
    *pinned = true;
  }

  DCHECK(block->write_handle_ != NULL) << block->DebugString() << endl << release_block;

  // The block is on disk - read it back into memory.
  if (block->valid_data_len() > 0) {
    status = tmp_file_group_->Read(block->write_handle_.get(), block->valid_data());
    if (!status.ok()) goto error;
  }
  tmp_file_group_->DestroyWriteHandle(move(block->write_handle_));
  return DeleteOrUnpinBlock(release_block, unpin);

error:
  DCHECK(!status.ok());
  // Make sure to delete the block if we hit an error before calling DeleteOrUnpin().
  if (release_block != NULL && !unpin) DeleteBlock(release_block);
  return status;
}

Status BufferedBlockMgr::CancelWrite(Block* block) {
  {
    unique_lock<mutex> lock(lock_);
    DCHECK(block->buffer_desc_ != NULL);
    // If there is an in-flight write, wait for it to finish. This is sub-optimal
    // compared to just cancelling the write, but reduces the number of possible
    // code paths in this legacy code.
    WaitForWrite(lock, block);
    if (is_cancelled_) return Status::CANCELLED;
  }
  if (block->write_handle_ != NULL) {
    // Make sure the write is not in-flight.
    block->write_handle_->Cancel();
    block->write_handle_->WaitForWrite();
    // Restore the in-memory data without reading from disk (e.g. decrypt it).
    RETURN_IF_ERROR(
        tmp_file_group_->RestoreData(move(block->write_handle_), block->valid_data()));
  }
  return Status::OK();
}

Status BufferedBlockMgr::UnpinBlock(Block* block) {
  DCHECK(!block->is_deleted_) << "Unpin for deleted block.";

  lock_guard<mutex> unpinned_lock(lock_);
  if (is_cancelled_) return Status::CANCELLED;
  DCHECK(block->Validate()) << endl << block->DebugString();
  if (!block->is_pinned_) return Status::OK();
  DCHECK_EQ(block->buffer_desc_->len, max_block_size_) << "Can only unpin io blocks.";
  DCHECK(Validate()) << endl << DebugInternal();
  // Add 'block' to the list of unpinned blocks and set is_pinned_ to false.
  // Cache its position in the list for later removal.
  block->is_pinned_ = false;
  DCHECK(!unpinned_blocks_.Contains(block)) << " Unpin for block in unpinned list";
  if (!block->in_write_) unpinned_blocks_.Enqueue(block);
  block->client_->UnpinBuffer(block->buffer_desc_);
  if (block->client_->num_pinned_buffers_ < block->client_->num_reserved_buffers_) {
    ++unfullfilled_reserved_buffers_;
  }
  --total_pinned_buffers_;
  RETURN_IF_ERROR(WriteUnpinnedBlocks());
  DCHECK(Validate()) << endl << DebugInternal();
  DCHECK(block->Validate()) << endl << block->DebugString();
  return Status::OK();
}

Status BufferedBlockMgr::WriteUnpinnedBlocks() {
  if (disable_spill_) return Status::OK();

  // Assumes block manager lock is already taken.
  while (non_local_outstanding_writes_ + free_io_buffers_.size() < block_write_threshold_
      && !unpinned_blocks_.empty()) {
    // Pop a block from the back of the list (LIFO).
    Block* write_block = unpinned_blocks_.PopBack();
    write_block->client_local_ = false;
    RETURN_IF_ERROR(WriteUnpinnedBlock(write_block));
    ++non_local_outstanding_writes_;
  }
  DCHECK(Validate()) << endl << DebugInternal();
  return Status::OK();
}

Status BufferedBlockMgr::WriteUnpinnedBlock(Block* block) {
  // Assumes block manager lock is already taken.
  DCHECK(!block->is_pinned_) << block->DebugString();
  DCHECK(!block->in_write_) << block->DebugString();
  DCHECK(block->write_handle_ == NULL) << block->DebugString();
  DCHECK_EQ(block->buffer_desc_->len, max_block_size_);

  // The block is on disk - read it back into memory.
  RETURN_IF_ERROR(tmp_file_group_->Write(block->valid_data(),
      [this, block](const Status& write_status) { WriteComplete(block, write_status); },
      &block->write_handle_));

  block->in_write_ = true;
  DCHECK(block->Validate()) << endl << block->DebugString();
  outstanding_writes_counter_->Add(1);
  ++writes_issued_;
  if (writes_issued_ == 1) {
    if (ImpaladMetrics::NUM_QUERIES_SPILLED != NULL) {
      ImpaladMetrics::NUM_QUERIES_SPILLED->Increment(1);
    }
  }
  return Status::OK();
}

void BufferedBlockMgr::WaitForWrite(unique_lock<mutex>& lock, Block* block) {
  DCHECK(!block->is_deleted_);
  while (block->in_write_ && !is_cancelled_) {
    block->write_complete_cv_.wait(lock);
  }
}

void BufferedBlockMgr::WriteComplete(Block* block, const Status& write_status) {
#ifndef NDEBUG
  if (debug_write_delay_ms_ > 0) {
    usleep(static_cast<int64_t>(debug_write_delay_ms_) * 1000);
  }
#endif
  Status status = Status::OK();
  lock_guard<mutex> lock(lock_);
  DCHECK(Validate()) << endl << DebugInternal();
  DCHECK(is_cancelled_ || block->in_write_) << "WriteComplete() for block not in write."
                                            << endl
                                            << block->DebugString();
  DCHECK(block->buffer_desc_ != NULL);

  outstanding_writes_counter_->Add(-1);
  if (!block->client_local_) {
    DCHECK_GT(non_local_outstanding_writes_, 0) << block->DebugString();
    --non_local_outstanding_writes_;
  }
  block->in_write_ = false;

  // ReturnUnusedBlock() will clear the block, so save required state in local vars.
  // state is not valid if the block was deleted because the state may be torn down
  // after the state's fragment has deleted all of its blocks.
  RuntimeState* state = block->is_deleted_ ? NULL : block->client_->state_;

  // If the block was re-pinned when it was in the IOMgr queue, don't free it.
  if (block->is_pinned_) {
    // The number of outstanding writes has decreased but the number of free buffers
    // hasn't.
    DCHECK(!block->is_deleted_);
    DCHECK(!block->client_local_)
        << "Client should be waiting. No one should have pinned this block.";
    if (write_status.ok() && !is_cancelled_ && !state->is_cancelled()) {
      status = WriteUnpinnedBlocks();
    }
  } else if (block->client_local_) {
    DCHECK(!block->is_deleted_)
        << "Client should be waiting. No one should have deleted this block.";
  } else {
    DCHECK_EQ(block->buffer_desc_->len, max_block_size_)
        << "Only io sized buffers should spill";
    free_io_buffers_.Enqueue(block->buffer_desc_);
  }

  if (!write_status.ok() || !status.ok() || is_cancelled_) {
    VLOG_FILE << "Query: " << query_id_ << ". Write did not complete successfully: "
                                           "write_status="
              << write_status.GetDetail() << ", status=" << status.GetDetail()
              << ". is_cancelled_=" << is_cancelled_;
    // If the instance is already cancelled, don't confuse things with these errors.
    if (!write_status.ok() && !write_status.IsCancelled()) {
      // Report but do not attempt to recover from write error.
      VLOG_QUERY << "Query: " << query_id_ << " write complete callback with error.";

      if (state != NULL) state->LogError(write_status.msg());
    }
    if (!status.ok() && !status.IsCancelled()) {
      VLOG_QUERY << "Query: " << query_id_ << " error while writing unpinned blocks.";
      if (state != NULL) state->LogError(status.msg());
    }
    // Set cancelled. Threads waiting for a write will be woken up in the normal way when
    // one of the writes they are waiting for completes.
    is_cancelled_ = true;
  }

  // Notify any threads that may have been expecting to get block's buffer based on
  // the value of 'non_local_outstanding_writes_'. Wake them all up. If we added
  // a buffer to 'free_io_buffers_', one thread will get a buffer. All the others
  // will re-evaluate whether they should continue waiting and if another write needs
  // to be initiated.
  if (!block->client_local_) buffer_available_cv_.notify_all();
  if (block->is_deleted_) {
    // Finish the DeleteBlock() work.
    tmp_file_group_->DestroyWriteHandle(move(block->write_handle_));
    block->buffer_desc_->block = NULL;
    block->buffer_desc_ = NULL;
    ReturnUnusedBlock(block);
    block = NULL;
  } else {
    // Wake up the thread waiting on this block (if any).
    block->write_complete_cv_.notify_one();
  }

  DCHECK(Validate()) << endl << DebugInternal();
}

void BufferedBlockMgr::DeleteBlock(Block* block) {
  unique_lock<mutex> lock(lock_);
  DeleteBlockLocked(lock, block);
}

void BufferedBlockMgr::DeleteBlockLocked(const unique_lock<mutex>& lock, Block* block) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  DCHECK(block->Validate()) << endl << DebugInternal();
  DCHECK(!block->is_deleted_);
  block->is_deleted_ = true;

  if (block->is_pinned_) {
    if (block->is_max_size()) --total_pinned_buffers_;
    block->is_pinned_ = false;
    block->client_->UnpinBuffer(block->buffer_desc_);
    if (block->client_->num_pinned_buffers_ < block->client_->num_reserved_buffers_) {
      ++unfullfilled_reserved_buffers_;
    }
  } else if (unpinned_blocks_.Contains(block)) {
    // Remove block from unpinned list.
    unpinned_blocks_.Remove(block);
  }

  if (block->in_write_) {
    DCHECK(block->buffer_desc_ != NULL && block->buffer_desc_->len == max_block_size_)
        << "Should never be writing a small buffer";
    // If a write is still pending, cancel it and return. Cleanup will be done in
    // WriteComplete(). Cancelling the write ensures that it won't try to log to the
    // RuntimeState (which may be torn down before the block manager).
    DCHECK(block->Validate()) << endl << block->DebugString();
    return;
  }

  if (block->buffer_desc_ != NULL) {
    if (block->buffer_desc_->len != max_block_size_) {
      // Just delete the block for now.
      delete[] block->buffer_desc_->buffer;
      block->client_->tracker_->Release(block->buffer_desc_->len);
      delete block->buffer_desc_;
      block->buffer_desc_ = NULL;
    } else {
      if (!free_io_buffers_.Contains(block->buffer_desc_)) {
        free_io_buffers_.Enqueue(block->buffer_desc_);
        // Wake up one of the waiting threads, which will grab the buffer.
        buffer_available_cv_.notify_one();
      }
      block->buffer_desc_->block = NULL;
      block->buffer_desc_ = NULL;
    }
  }

  // Discard any on-disk data. The write is finished so this won't call back into
  // BufferedBlockMgr.
  if (block->write_handle_ != NULL) {
    tmp_file_group_->DestroyWriteHandle(move(block->write_handle_));
  }
  ReturnUnusedBlock(block);
  DCHECK(block->Validate()) << endl << block->DebugString();
  DCHECK(Validate()) << endl << DebugInternal();
}

void BufferedBlockMgr::ReturnUnusedBlock(Block* block) {
  DCHECK(block->is_deleted_) << block->DebugString();
  DCHECK(!block->is_pinned_) << block->DebugString();;
  DCHECK(block->buffer_desc_ == NULL);
  block->Init();
  unused_blocks_.Enqueue(block);
}

Status BufferedBlockMgr::FindBufferForBlock(Block* block, bool* in_mem) {
  DCHECK(block != NULL);
  Client* client = block->client_;
  DCHECK(client != NULL);
  DCHECK(!block->is_pinned_ && !block->is_deleted_)
      << "Pinned or deleted block " << endl << block->DebugString();
  *in_mem = false;

  unique_lock<mutex> l(lock_);
  if (is_cancelled_) return Status::CANCELLED;

  // First check if there is enough reserved memory to satisfy this request.
  bool is_reserved_request = false;
  if (client->num_pinned_buffers_ < client->num_reserved_buffers_) {
    is_reserved_request = true;
  } else if (client->num_tmp_reserved_buffers_ > 0) {
    is_reserved_request = true;
    --client->num_tmp_reserved_buffers_;
  }

  DCHECK(Validate()) << endl << DebugInternal();
  if (is_reserved_request) --unfullfilled_reserved_buffers_;

  if (!is_reserved_request && remaining_unreserved_buffers() < 1) {
    // The client already has its quota and there are no unreserved blocks left.
    // Note that even if this passes, it is still possible for the path below to
    // see OOM because another query consumed memory from the process tracker. This
    // only happens if the buffer has not already been allocated by the block mgr.
    // This check should ensure that the memory cannot be consumed by another client
    // of the block mgr.
    return Status::OK();
  }

  if (block->buffer_desc_ != NULL) {
    // The block is in memory. It may be in 3 states:
    //  1. In the unpinned list. The buffer will not be in the free list.
    //  2. in_write_ == true. The buffer will not be in the free list.
    //  3. The buffer is free, but hasn't yet been reassigned to a different block.
    DCHECK_EQ(block->buffer_desc_->len, max_block_size())
        << "Non-I/O blocks are always pinned";
    DCHECK(unpinned_blocks_.Contains(block) ||
           block->in_write_ ||
           free_io_buffers_.Contains(block->buffer_desc_));
    if (unpinned_blocks_.Contains(block)) {
      unpinned_blocks_.Remove(block);
      DCHECK(!free_io_buffers_.Contains(block->buffer_desc_));
    } else if (block->in_write_) {
      DCHECK(block->in_write_ && !free_io_buffers_.Contains(block->buffer_desc_));
    } else {
      free_io_buffers_.Remove(block->buffer_desc_);
    }
    buffered_pin_counter_->Add(1);
    *in_mem = true;
  } else {
    BufferDescriptor* buffer_desc = NULL;
    RETURN_IF_ERROR(FindBuffer(l, &buffer_desc));

    if (buffer_desc == NULL) {
      // There are no free buffers or blocks we can evict. We need to fail this request.
      // If this is an optional request, return OK. If it is required, return OOM.
      if (!is_reserved_request || client->tolerates_oversubscription_) return Status::OK();

      if (VLOG_QUERY_IS_ON) {
        stringstream ss;
        ss << "Query id=" << query_id_ << " was unable to get minimum required buffers."
           << endl << DebugInternal() << endl << client->DebugString();
        VLOG_QUERY << ss.str();
      }
      return client->tracker_->MemLimitExceeded(client->state_,
          "Query did not have enough memory to get the minimum required buffers in the "
          "block manager.");
    }

    DCHECK(buffer_desc != NULL);
    DCHECK_EQ(buffer_desc->len, max_block_size()) << "Non-I/O buffer";
    if (buffer_desc->block != NULL) {
      // This buffer was assigned to a block but now we are reusing it. Reset the
      // previous block->buffer link.
      DCHECK(buffer_desc->block->Validate()) << endl << buffer_desc->block->DebugString();
      buffer_desc->block->buffer_desc_ = NULL;
    }
    buffer_desc->block = block;
    block->buffer_desc_ = buffer_desc;
  }
  DCHECK(block->buffer_desc_ != NULL);
  DCHECK(block->buffer_desc_->len < max_block_size() || !block->is_pinned_)
      << "Trying to pin already pinned block. "
      << block->buffer_desc_->len << " " << block->is_pinned_;
  block->is_pinned_ = true;
  client->PinBuffer(block->buffer_desc_);
  ++total_pinned_buffers_;

  DCHECK(block->Validate()) << endl << block->DebugString();
  // The number of free buffers has decreased. Write unpinned blocks if the number
  // of free buffers is less than the threshold.
  RETURN_IF_ERROR(WriteUnpinnedBlocks());
  DCHECK(Validate()) << endl << DebugInternal();
  return Status::OK();
}

// We need to find a new buffer. We prefer getting this buffer in this order:
//  1. Allocate a new block if the number of free blocks is less than the write threshold
//     or if we are running without spilling, until we run out of memory.
//  2. Pick a buffer from the free list.
//  3. Wait and evict an unpinned buffer.
Status BufferedBlockMgr::FindBuffer(unique_lock<mutex>& lock,
    BufferDescriptor** buffer_desc) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  *buffer_desc = NULL;

  // First, try to allocate a new buffer.
  DCHECK(block_write_threshold_ > 0 || disable_spill_);
  if ((free_io_buffers_.size() < block_write_threshold_ || disable_spill_) &&
      mem_tracker_->TryConsume(max_block_size_)) {
    uint8_t* new_buffer = new uint8_t[max_block_size_];
    *buffer_desc = obj_pool_.Add(new BufferDescriptor(new_buffer, max_block_size_));
    (*buffer_desc)->all_buffers_it = all_io_buffers_.insert(
        all_io_buffers_.end(), *buffer_desc);
    return Status::OK();
  }

  // Second, try to pick a buffer from the free list.
  if (free_io_buffers_.empty()) {
    // There are no free buffers. If spills are disabled or there no unpinned blocks we
    // can write, return. We can't get a buffer.
    if (disable_spill_) {
      if (block_write_threshold_ == 0) {
        return Status("Spilling has been disabled due to no usable scratch space. "
            "Please specify a usable scratch space location via the --scratch_dirs "
            "impalad flag.");
      } else {
        return Status("Spilling has been disabled for plans that do not have stats and "
            "are not hinted to prevent potentially bad plans from using too many cluster "
            "resources. Please run COMPUTE STATS on these tables, hint the plan or "
            "disable this behavior via the DISABLE_UNSAFE_SPILLS query option.");
      }
    }

    // Third, this block needs to use a buffer that was unpinned from another block.
    // Get a free buffer from the front of the queue and assign it to the block.
    do {
      if (unpinned_blocks_.empty() && non_local_outstanding_writes_ == 0) {
        return Status::OK();
      }
      SCOPED_TIMER(buffer_wait_timer_);
      // Try to evict unpinned blocks before waiting.
      RETURN_IF_ERROR(WriteUnpinnedBlocks());
      DCHECK_GT(non_local_outstanding_writes_, 0) << endl << DebugInternal();
      buffer_available_cv_.wait(lock);
      if (is_cancelled_) return Status::CANCELLED;
    } while (free_io_buffers_.empty());
  }
  *buffer_desc = free_io_buffers_.Dequeue();
  return Status::OK();
}

BufferedBlockMgr::Block* BufferedBlockMgr::GetUnusedBlock(Client* client) {
  DCHECK(client != NULL);
  Block* new_block = NULL;
  if (unused_blocks_.empty()) {
    new_block = obj_pool_.Add(new Block(this));
    all_blocks_.push_back(new_block);
    new_block->Init();
    created_block_counter_->Add(1);
  } else {
    new_block = unused_blocks_.Dequeue();
    recycled_blocks_counter_->Add(1);
  }
  DCHECK(new_block != NULL);
  new_block->client_ = client;
  return new_block;
}

bool BufferedBlockMgr::Validate() const {
  int num_free_io_buffers = 0;

  if (total_pinned_buffers_ < 0) {
    LOG(ERROR) << "total_pinned_buffers_ < 0: " << total_pinned_buffers_;
    return false;
  }

  for (BufferDescriptor* buffer: all_io_buffers_) {
    bool is_free = free_io_buffers_.Contains(buffer);
    num_free_io_buffers += is_free;

    if (*buffer->all_buffers_it != buffer) {
      LOG(ERROR) << "All buffers list is corrupt. Buffer iterator is not valid.";
      return false;
    }

    if (buffer->block == NULL && !is_free) {
      LOG(ERROR) << "Buffer with no block not in free list." << endl << DebugInternal();
      return false;
    }

    if (buffer->len != max_block_size_) {
      LOG(ERROR) << "Non-io sized buffers should not end up on free list.";
      return false;
    }

    if (buffer->block != NULL) {
      if (buffer->block->buffer_desc_ != buffer) {
        LOG(ERROR) << "buffer<->block pointers inconsistent. Buffer: " << buffer
          << endl << buffer->block->DebugString();
        return false;
      }

      if (!buffer->block->Validate()) {
        LOG(ERROR) << "buffer->block inconsistent."
          << endl << buffer->block->DebugString();
        return false;
      }

      if (is_free && (buffer->block->is_pinned_ || buffer->block->in_write_ ||
            unpinned_blocks_.Contains(buffer->block))) {
        LOG(ERROR) << "Block with buffer in free list and"
          << " is_pinned_ = " << buffer->block->is_pinned_
          << " in_write_ = " << buffer->block->in_write_
          << " Unpinned_blocks_.Contains = "
          << unpinned_blocks_.Contains(buffer->block)
          << endl << buffer->block->DebugString();
        return false;
      }
    }
  }

  if (free_io_buffers_.size() != num_free_io_buffers) {
    LOG(ERROR) << "free_buffer_list_ inconsistency."
      << " num_free_io_buffers = " << num_free_io_buffers
      << " free_io_buffers_.size() = " << free_io_buffers_.size()
      << endl << DebugInternal();
    return false;
  }

  Block* block = unpinned_blocks_.head();
  while (block != NULL) {
    if (!block->Validate()) {
      LOG(ERROR) << "Block inconsistent in unpinned list."
        << endl << block->DebugString();
      return false;
    }

    if (block->in_write_ || free_io_buffers_.Contains(block->buffer_desc_)) {
      LOG(ERROR) << "Block in unpinned list with"
        << " in_write_ = " << block->in_write_
        << " free_io_buffers_.Contains = "
        << free_io_buffers_.Contains(block->buffer_desc_)
        << endl << block->DebugString();
      return false;
    }
    block = block->Next();
  }

  // Check if we're writing blocks when the number of free buffers is less than
  // the write threshold. We don't write blocks after cancellation.
  if (!is_cancelled_ && !unpinned_blocks_.empty() && !disable_spill_ &&
      (free_io_buffers_.size() + non_local_outstanding_writes_ <
       block_write_threshold_)) {
    // TODO: this isn't correct when WriteUnpinnedBlocks() fails during the call to
    // WriteUnpinnedBlock() so just log the condition but don't return false. Figure
    // out a way to re-enable this change?
    LOG(ERROR) << "Missed writing unpinned blocks";
  }
  return true;
}

string BufferedBlockMgr::DebugString(Client* client) {
  stringstream ss;
  unique_lock<mutex> l(lock_);
  ss <<  DebugInternal();
  if (client != NULL) ss << endl << client->DebugString();
  return ss.str();
}

string BufferedBlockMgr::DebugInternal() const {
  stringstream ss;
  ss << "Buffered block mgr " << this << endl
     << "  Num writes outstanding: " << outstanding_writes_counter_->value() << endl
     << "  Num free io buffers: " << free_io_buffers_.size() << endl
     << "  Num unpinned blocks: " << unpinned_blocks_.size() << endl
     << "  Num available buffers: " << remaining_unreserved_buffers() << endl
     << "  Total pinned buffers: " << total_pinned_buffers_ << endl
     << "  Unfullfilled reserved buffers: " << unfullfilled_reserved_buffers_ << endl
     << "  Remaining memory: " << mem_tracker_->SpareCapacity()
     << " (#blocks=" << (mem_tracker_->SpareCapacity() / max_block_size_) << ")" << endl
     << "  Block write threshold: " << block_write_threshold_;
  if (tmp_file_group_ != NULL) ss << tmp_file_group_->DebugString();
  return ss.str();
}

void BufferedBlockMgr::Init(DiskIoMgr* io_mgr, TmpFileMgr* tmp_file_mgr,
    RuntimeProfile* parent_profile, MemTracker* parent_tracker, int64_t mem_limit,
    int64_t scratch_limit) {
  unique_lock<mutex> l(lock_);
  if (initialized_) return;

  profile_.reset(new RuntimeProfile(&obj_pool_, "BlockMgr"));
  parent_profile->AddChild(profile_.get());

  tmp_file_group_.reset(new TmpFileMgr::FileGroup(
      tmp_file_mgr, io_mgr, profile_.get(), query_id_, scratch_limit));

  mem_limit_counter_ = ADD_COUNTER(profile_.get(), "MemoryLimit", TUnit::BYTES);
  mem_limit_counter_->Set(mem_limit);
  block_size_counter_ = ADD_COUNTER(profile_.get(), "MaxBlockSize", TUnit::BYTES);
  block_size_counter_->Set(max_block_size_);
  created_block_counter_ = ADD_COUNTER(profile_.get(), "BlocksCreated", TUnit::UNIT);
  recycled_blocks_counter_ = ADD_COUNTER(profile_.get(), "BlocksRecycled", TUnit::UNIT);
  outstanding_writes_counter_ =
      ADD_COUNTER(profile_.get(), "BlockWritesOutstanding", TUnit::UNIT);
  buffered_pin_counter_ = ADD_COUNTER(profile_.get(), "BufferedPins", TUnit::UNIT);
  buffer_wait_timer_ = ADD_TIMER(profile_.get(), "TotalBufferWaitTime");

  // Create a new mem_tracker and allocate buffers.
  mem_tracker_.reset(
      new MemTracker(profile(), mem_limit, "Block Manager", parent_tracker));

  initialized_ = true;
}

} // namespace impala
