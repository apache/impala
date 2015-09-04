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

#ifndef IMPALA_RUNTIME_BUFFERED_BLOCK_MGR
#define IMPALA_RUNTIME_BUFFERED_BLOCK_MGR

#include "runtime/disk-io-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "util/mem-range.h"

namespace impala {

class RuntimeState;

/// The BufferedBlockMgr is used to allocate and manage blocks of data using a fixed memory
/// budget. Available memory is split into a pool of fixed-size memory buffers. When a
/// client allocates or requests a block, the block is assigned a buffer from this pool and
/// is 'pinned' in memory. Clients can also unpin a block, allowing the manager to reassign
/// its buffer to a different block.
//
/// The BufferedBlockMgr typically allocates blocks in IO buffer size to get maximal IO
/// efficiency when spilling. Clients can also request smaller buffers that cannot spill
/// (note that it would be possible to spill small buffers, but we currently do not allow
/// it). This is useful to present the same block API and mem tracking for clients (one can
/// use the block mgr API to mem track non-spillable (smaller) buffers). Clients that do
/// partitioning (e.g. PHJ and PAGG) will start with these smaller buffer sizes to reduce
/// the minimum buffering requirements and grow to max sized buffers as the input grows.
/// For simplicity, these small buffers are not recycled (there's also not really a need
/// since they are allocated all at once on query startup). These buffers are not counted
/// against the reservation.
//
/// The BufferedBlockMgr reserves one buffer per disk ('block_write_threshold_') for
/// itself. When the number of free buffers falls below 'block_write_threshold', unpinned
/// blocks are flushed in Last-In-First-Out order. (It is assumed that unpinned blocks are
/// re-read in FIFO order). The TmpFileMgr is used to obtain file handles to write to
/// within the tmp directories configured for Impala.
//
/// It is expected to have one BufferedBlockMgr per query. All allocations that can grow
/// proportional to the input size and that might need to spill to disk should allocate
/// from the same BufferedBlockMgr.
//
/// A client must pin a block in memory to read/write its contents and unpin it when it is
/// no longer in active use. The BufferedBlockMgr guarantees that:
///  a) The memory buffer assigned to a block is not removed or released while it is pinned.
///  b) The contents of an unpinned block will be available on a subsequent call to pin.
//
/// The Client supports the following operations:
///  GetNewBlock(): Returns a new pinned block.
///  Close(): Frees all memory and disk space. Called when a query is closed or cancelled.
///   Close() is idempotent.
//
/// A Block supports the following operations:
///  Pin(): Pins a block to a buffer in memory, and reads its contents from disk if
///   necessary. If there are no free buffers, waits for a buffer to become available.
///   Invoked before the contents of a block are read or written. The block
///   will be maintained in memory until Unpin() is called.
///  Unpin(): Invoked to indicate the block is not in active use. The block is added to a
///   list of unpinned blocks. Unpinned blocks are only written when the number of free
///   blocks falls below the 'block_write_threshold'.
///  Delete(): Invoked to deallocate a block. The buffer associated with the block is
///   immediately released and its on-disk location (if any) reused. All blocks must be
///   deleted before the block manager is torn down.
///
/// The block manager is thread-safe with the following caveat: A single block cannot be
/// used simultaneously by multiple clients in any capacity.
/// However, the block manager client is not thread-safe. That is, the block manager
/// allows multiple single-threaded block manager clients.
///
/// TODO: replace with BufferPool.
class BufferedBlockMgr {
 private:
  struct BufferDescriptor;

 public:
  /// A client of the BufferedBlockMgr. There is a single BufferedBlockMgr per plan
  /// fragment and all operators that need blocks from it should use a separate client.
  /// Each client has the option to reserve a number of blocks that it can claim later.
  /// The remaining memory that is not reserved by any clients is free for all and
  /// available to all clients.
  /// This is an opaque handle.
  struct Client;

  /// A fixed-size block of data that may be be persisted to disk. The state of the block
  /// is maintained by the block manager and is described by 3 bools:
  /// is_pinned_ = True if the block is pinned. The block has a non-null buffer_desc_,
  ///   buffer_desc_ cannot be in the free buffer list and the block cannot be in
  ///   unused_blocks_ or unpinned_blocks_. Newly allocated blocks are pinned.
  /// in_write_ = True if a write has been issued but not completed for this block.
  ///   The block cannot be in the unpinned_blocks_ and must have a non-null buffer_desc_
  ///   that's not in the free buffer list. It may be pinned or unpinned.
  /// is_deleted_ = True if Delete() has been called on a block. After this, no API call
  ///   is valid on the block.
  //
  /// Pin() and Unpin() can be invoked on a block any number of times before Delete().
  /// When a pinned block is unpinned for the first time, it is added to the
  /// unpinned_blocks_ list and its buffer is removed from the free list.
  /// If it is pinned or deleted at any time while it is on the unpinned list, it is
  /// simply removed from that list. When it is dequeued from that list and enqueued
  /// for writing, in_write_ is set to true. The block may be pinned, unpinned or deleted
  /// while in_write_ is true. After the write has completed, the block's buffer will be
  /// returned to the free buffer list if it is no longer pinned, and the block itself
  /// will be put on the unused blocks list if Delete() was called.
  //
  /// A block MUST have a non-null buffer_desc_ if
  ///  a) is_pinned_ is true (i.e. the client is using it), or
  ///  b) in_write_ is true, (i.e. IO mgr is using it), or
  ///  c) It is on the unpinned list (buffer has not been persisted.)
  //
  /// In addition to the block manager API, Block exposes Allocate(), ReturnAllocation()
  /// and BytesRemaining() to allocate and free memory within a block, and buffer() and
  /// valid_data_len() to read/write the contents of a block. These are not thread-safe.
  class Block : public InternalQueue<Block>::Node {
   public:
    /// Pins a block in memory--assigns a free buffer to a block and reads it from disk if
    /// necessary. If there are no free blocks and no unpinned blocks, '*pinned' is set to
    /// false and the block is not pinned. If 'release_block' is non-NULL, if there is
    /// memory pressure, this block will be pinned using the buffer from 'release_block'.
    /// If 'unpin' is true, 'release_block' will be unpinned (regardless of whether or not
    /// the buffer was used for this block). If 'unpin' is false, 'release_block' is
    /// deleted. 'release_block' must be pinned. If an error occurs and 'unpin' was false,
    /// 'release_block' is always deleted. If 'unpin' was true and an error occurs,
    /// 'release_block' may be left pinned or unpinned.
    Status Pin(bool* pinned, Block* release_block = NULL, bool unpin = true);

    /// Unpins a block by adding it to the list of unpinned blocks maintained by the block
    /// manager. An unpinned block must be flushed before its buffer is released or
    /// assigned to a different block. Is non-blocking.
    Status Unpin();

    /// Delete a block. Its buffer is released and on-disk location can be over-written.
    /// Non-blocking.
    void Delete();

    void AddRow() { ++num_rows_; }
    int num_rows() const { return num_rows_; }

    /// Allocates the specified number of bytes from this block.
    template <typename T> T* Allocate(int size) {
      DCHECK_GE(BytesRemaining(), size);
      uint8_t* current_location = buffer_desc_->buffer + valid_data_len_;
      valid_data_len_ += size;
      return reinterpret_cast<T*>(current_location);
    }

    /// Return the number of remaining bytes that can be allocated in this block.
    int BytesRemaining() const {
      DCHECK(buffer_desc_ != NULL);
      return buffer_desc_->len - valid_data_len_;
    }

    /// Return size bytes from the most recent allocation.
    void ReturnAllocation(int size) {
      DCHECK_GE(valid_data_len_, size);
      valid_data_len_ -= size;
    }

    /// Pointer to start of the block data in memory. Only guaranteed to be valid if the
    /// block is pinned.
    uint8_t* buffer() const {
      DCHECK(buffer_desc_ != NULL);
      return buffer_desc_->buffer;
    }

    /// Returns a reference to the valid data in the block's buffer. Only guaranteed to
    /// be valid if the block is pinned.
    MemRange valid_data() const {
      DCHECK(buffer_desc_ != NULL);
      return MemRange(buffer_desc_->buffer, valid_data_len_);
    }

    /// Return the number of bytes allocated in this block.
    int64_t valid_data_len() const { return valid_data_len_; }

    /// Returns the length of the underlying buffer. Only callable if the block is
    /// pinned.
    int64_t buffer_len() const {
      DCHECK(is_pinned());
      return buffer_desc_->len;
    }

    /// Returns true if this block is the max block size. Only callable if the block
    /// is pinned.
    bool is_max_size() const {
      DCHECK(is_pinned());
      return buffer_desc_->len == block_mgr_->max_block_size();
    }

    bool is_pinned() const { return is_pinned_; }

    /// Path of temporary file backing the block. Intended for use in testing.
    /// Returns empty string if no backing file allocated.
    std::string TmpFilePath() const;

    /// Debug helper method to print the state of a block.
    std::string DebugString() const;

   private:
    friend class BufferedBlockMgr;

    Block(BufferedBlockMgr* block_mgr);

    /// Initialize the state of a block and set the number of bytes allocated to 0.
    void Init();

    /// Debug helper method to validate the state of a block. block_mgr_ lock must already
    /// be taken.
    bool Validate() const;

    /// Pointer to the buffer associated with the block. NULL if the block is not in
    /// memory and cannot be changed while the block is pinned or being written.
    BufferDescriptor* buffer_desc_;

    /// Parent block manager object. Responsible for maintaining the state of the block.
    BufferedBlockMgr* block_mgr_;

    /// The client that owns this block.
    Client* client_;

    /// Non-NULL when the block data is written to scratch or is in the process of being
    /// written.
    std::unique_ptr<TmpFileMgr::WriteHandle> write_handle_;

    /// Length of valid (i.e. allocated) data within the block.
    int64_t valid_data_len_;

    /// Number of rows in this block.
    int num_rows_;

    /// Block state variables. The block's buffer can be freed only if is_pinned_ and
    /// in_write_ are both false.

    /// is_pinned_ is true while the block is pinned by a client.
    bool is_pinned_;

    /// in_write_ is set to true when the block is enqueued for writing via DiskIoMgr,
    /// and set to false when the write is complete.
    bool in_write_;

    /// True if the block is deleted by the client.
    bool is_deleted_;

    /// Condition variable to wait for the write to this block to finish. If 'in_write_'
    /// is true, notify_one() will eventually be called on this condition variable. Only
    /// on thread should wait on this cv at a time.
    boost::condition_variable write_complete_cv_;

    /// If true, this block is being written out so the underlying buffer can be
    /// transferred to another block from the same client. We don't want this buffer
    /// getting picked up by another client.
    bool client_local_;
  }; // class Block

  /// Create a block manager with the specified mem_limit. If a block mgr with the
  /// same query id has already been created, that block mgr is returned.
  /// - mem_limit: maximum memory that will be used by the block mgr.
  /// - buffer_size: maximum size of each buffer.
  static Status Create(RuntimeState* state, MemTracker* parent,
      RuntimeProfile* profile, TmpFileMgr* tmp_file_mgr, int64_t mem_limit,
      int64_t buffer_size, std::shared_ptr<BufferedBlockMgr>* block_mgr);

  ~BufferedBlockMgr();

  /// Registers a client with 'num_reserved_buffers'. The returned client is owned
  /// by the BufferedBlockMgr and has the same lifetime as it.
  /// We allow oversubscribing the reserved buffers. It is likely that the
  /// 'num_reserved_buffers' will be very pessimistic for small queries and we don't want
  /// to
  /// fail all of them with mem limit exceeded.
  /// The min reserved buffers is often independent of data size and we still want
  /// to run small queries with very small limits.
  /// Buffers used by this client are reflected in tracker.
  /// 'tolerates_oversubscription' determines how oversubscription is handled. If true,
  /// failure to allocate a reserved buffer is not an error. If false, failure to
  /// allocate a reserved buffer is a MEM_LIMIT_EXCEEDED error.
  /// 'debug_info' is a string that will be printed in debug messages and errors to
  /// identify the client.
  Status RegisterClient(const std::string& debug_info, int num_reserved_buffers,
      bool tolerates_oversubscription, MemTracker* tracker, RuntimeState* state,
      Client** client);

  /// Clears all reservations for this client.
  void ClearReservations(Client* client);

  /// Tries to acquire a one-time reservation of num_buffers. The semantics are:
  ///  - If this call fails, the next 'num_buffers' calls to Pin()/GetNewBlock() might
  ///    not have enough memory.
  ///  - If this call succeeds, the next 'num_buffers' call to Pin()/GetNewBlock() will
  ///    be guaranteed to get the block. Once these blocks have been pinned, the
  ///    reservation from this call has no more effect.
  /// Blocks coming from the tmp reservation also count towards the regular reservation.
  /// This is useful to Pin() a number of blocks and guarantee all or nothing behavior.
  bool TryAcquireTmpReservation(Client* client, int num_buffers);

  /// Return a new pinned block. If there is no memory for this block, *block will be set
  /// to NULL.
  /// If len > 0, GetNewBlock() will return a block with a buffer of size len. len
  /// must be less than max_block_size and this block cannot be unpinned.
  /// This function will try to allocate new memory for the block up to the limit.
  /// Otherwise it will (conceptually) write out an unpinned block and use that memory.
  /// The caller can pass a non-NULL 'unpin_block' to transfer memory from 'unpin_block'
  /// to the new block. If 'unpin_block' is non-NULL, the new block can never fail to
  /// get a buffer. The semantics of this are:
  ///   - If 'unpin_block' is non-NULL, it must be pinned.
  ///   - If the call succeeds, 'unpin_block' is unpinned.
  ///   - If there is no memory pressure, block will get a newly allocated buffer.
  ///   - If there is memory pressure, block will get the buffer from 'unpin_block'.
  Status GetNewBlock(Client* client, Block* unpin_block, Block** block, int64_t len = -1);

  /// Test helper to cancel the block mgr. All subsequent calls that return a Status fail
  /// with Status::CANCELLED. Idempotent.
  void Cancel();

  /// Returns true if the block manager was cancelled.
  bool IsCancelled();

  /// Dumps block mgr state. Grabs lock. If client is not NULL, also dumps its state.
  std::string DebugString(Client* client = NULL);

  /// Consumes 'size' bytes from the buffered block mgr. This is used by callers that want
  /// the memory to come from the block mgr pool (and therefore trigger spilling) but need
  /// the allocation to be more flexible than blocks. Buffer space reserved with
  /// TryAcquireTmpReservation may be used to fulfill the request if available. If the
  /// request is unsuccessful, that temporary buffer space is not consumed.
  /// Returns false if there was not enough memory.
  ///
  /// This is used only for the Buckets structure in the hash table, which cannot be
  /// segmented into blocks.
  bool ConsumeMemory(Client* client, int64_t size);

  /// All successful allocates bytes from ConsumeMemory() must have a corresponding
  /// ReleaseMemory() call.
  void ReleaseMemory(Client* client, int64_t size);

  /// Returns a MEM_LIMIT_EXCEEDED error which includes the minimum memory required by
  /// this 'client' that acts on behalf of the node with id 'node_id'. 'node_id' is used
  /// only for error reporting.
  Status MemLimitTooLowError(Client* client, int node_id);

  int num_pinned_buffers(Client* client) const;
  int num_reserved_buffers_remaining(Client* client) const;
  MemTracker* mem_tracker() const { return mem_tracker_.get(); }
  MemTracker* get_tracker(Client* client) const;
  int64_t max_block_size() const { return max_block_size_; }
  int64_t bytes_allocated() const;
  RuntimeProfile* profile() { return profile_.get(); }
  int writes_issued() const { return writes_issued_; }

  void set_debug_write_delay_ms(int val) { debug_write_delay_ms_ = val; }

 private:
  friend class BufferedBlockMgrTest;
  friend struct Client;

  /// Descriptor for a single memory buffer in the pool.
  struct BufferDescriptor : public InternalQueue<BufferDescriptor>::Node {
    /// Start of the buffer.
    uint8_t* buffer;

    /// Length of the buffer.
    int64_t len;

    /// Block that this buffer is assigned to. May be NULL.
    Block* block;

    /// Iterator into all_io_buffers_ for this buffer.
    std::list<BufferDescriptor*>::iterator all_buffers_it;

    BufferDescriptor(uint8_t* buf, int64_t len) : buffer(buf), len(len), block(NULL) {}
  };

  BufferedBlockMgr(RuntimeState* state, TmpFileMgr* tmp_file_mgr, int64_t block_size,
      int64_t scratch_limit);

  /// Initializes the block mgr. Idempotent and thread-safe.
  void Init(DiskIoMgr* io_mgr, TmpFileMgr* tmp_file_mgr, RuntimeProfile* profile,
      MemTracker* parent_tracker, int64_t mem_limit, int64_t scratch_limit);

  /// PinBlock(), UnpinBlock(), DeleteBlock() perform the actual work of Block::Pin(),
  /// Unpin() and Delete(). DeleteBlock() must be called without the lock_ taken and
  /// DeleteBlockLocked() must be called with the lock_ taken.
  Status PinBlock(Block* block, bool* pinned, Block* src, bool unpin);
  Status UnpinBlock(Block* block);
  void DeleteBlock(Block* block);
  void DeleteBlockLocked(const boost::unique_lock<boost::mutex>& lock, Block* block);

  /// If there is an in-flight write, cancel the write and restore the contents of the
  /// block's buffer. If no write has been started for 'block', does nothing. 'block'
  /// must have an associated buffer. Returns an error status if an error is encountered
  /// while cancelling the write or CANCELLED if the block mgr is cancelled.
  Status CancelWrite(Block* block);

  /// If the 'block' is NULL, checks if cancelled and returns. Otherwise, depending on
  /// 'unpin' calls either  DeleteBlock() or UnpinBlock(), which both first check for
  /// cancellation. It should be called without the lock_ acquired.
  Status DeleteOrUnpinBlock(Block* block, bool unpin);

  /// Transfers the buffer from 'src' to 'dst'. 'src' must be pinned. If a write is
  /// already in flight for 'src', this may block until that write completes.
  /// If unpin == false, 'src' is simply deleted.
  /// If unpin == true, 'src' is unpinned and it may block until the write of 'src' is
  /// completed.
  /// The caller should not hold 'lock_'.
  Status TransferBuffer(Block* dst, Block* src, bool unpin);

  /// The number of buffers available for client. That is, if all other clients were
  /// stopped, the number of buffers this client could get.
  int64_t available_buffers(Client* client) const;

  /// Returns the total number of unreserved buffers. This is the sum of unpinned,
  /// free and buffers we can still allocate minus the total number of reserved buffers
  /// that are not pinned.
  /// Note this can be negative if the buffers are oversubscribed.
  /// Must be called with lock_ taken.
  int64_t remaining_unreserved_buffers() const;

  /// Finds a buffer for a block and pins it. If the block's buffer has not been evicted,
  /// it removes the block from the unpinned list and sets *in_mem = true.
  /// If the block is not in memory, it will call FindBuffer() that may block.
  /// If we can't get a buffer (e.g. no more memory, nothing in the unpinned and free
  /// lists) this function returns with the block unpinned.
  /// Uses the lock_, the caller should not have already acquired the lock_.
  Status FindBufferForBlock(Block* block, bool* in_mem);

  /// Returns a new buffer that can be used. *buffer is set to NULL if there was no
  /// memory.
  /// Otherwise, this function gets a new buffer by:
  ///   1. Allocating a new buffer if possible
  ///   2. Using a buffer from the free list (which is populated by moving blocks from
  ///      the unpinned list by writing them out).
  /// Must be called with the lock_ already taken. This function can block.
  Status FindBuffer(boost::unique_lock<boost::mutex>& lock, BufferDescriptor** buffer);

  /// Writes unpinned blocks via DiskIoMgr until one of the following is true:
  ///   1. The number of outstanding writes >= (block_write_threshold_ - num free buffers)
  ///   2. There are no more unpinned blocks
  /// Must be called with the lock_ already taken. Is not blocking.
  Status WriteUnpinnedBlocks();

  /// Issues the write for this block to the DiskIoMgr.
  Status WriteUnpinnedBlock(Block* block);

  /// Wait until either there is no in-flight write for 'block' or the block mgr is
  /// cancelled. 'lock_' must be held with 'lock'.
  void WaitForWrite(boost::unique_lock<boost::mutex>& lock, Block* block);

  /// Callback used by DiskIoMgr to indicate a block write has completed.  write_status
  /// is the status of the write. is_cancelled_ is set to true if write_status is not
  /// Status::OK or a re-issue of the write fails. Returns the block's buffer to the
  /// free buffers list if it is no longer pinned. Returns the block itself to the free
  /// blocks list if it has been deleted.
  void WriteComplete(Block* block, const Status& write_status);

  /// Returns a deleted block to the list of free blocks. Assumes the block's buffer has
  /// already been returned to the free buffers list. Non-blocking.
  /// Thread-safe and does not need the lock_ acquired.
  void ReturnUnusedBlock(Block* block);

  /// Checks unused_blocks_ for an unused block object, else allocates a new one.
  /// Non-blocking and needs no lock_.
  Block* GetUnusedBlock(Client* client);

  // Test helper to get the number of block writes currently outstanding.
  int64_t GetNumWritesOutstanding();

  /// Used to debug the state of the block manager. Lock must already be taken.
  bool Validate() const;
  std::string DebugInternal() const;

  /// Size of the largest/default block in bytes.
  const int64_t max_block_size_;

  /// Unpinned blocks are written when the number of free buffers is below this threshold.
  /// Equal to two times the number of disks.
  const int block_write_threshold_;

  /// If true, spilling is disabled. The client calls will fail if there is not enough
  /// memory.
  const bool disable_spill_;

  const TUniqueId query_id_;

  ObjectPool obj_pool_;

  /// Track buffers allocated by the block manager.
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// This lock protects the block and buffer lists below, except for unused_blocks_.
  /// It also protects the various counters and changes to block state. Additionally, it
  /// is used for the blocking condvars: buffer_available_cv_ and
  /// block->write_complete_cv_.
  boost::mutex lock_;

  /// If true, Init() has been called.
  bool initialized_;

  /// The total number of reserved buffers across all clients that are not pinned.
  int unfullfilled_reserved_buffers_;

  /// The total number of pinned buffers across all clients.
  int total_pinned_buffers_;

  /// Number of outstanding writes (Writes issued but not completed).
  /// This does not include client-local writes.
  int non_local_outstanding_writes_;

  /// Signal availability of free buffers. Also signalled when a write completes for a
  /// pinned block, in case another thread was expecting to obtain its buffer. If
  /// 'non_local_outstanding_writes_' > 0, notify_all() will eventually be called on
  /// this condition variable. To avoid free buffers accumulating while threads wait
  /// on the cv, a woken thread must grab an available buffer (unless is_cancelled_ is
  /// true at that time).
  boost::condition_variable buffer_available_cv_;

  /// All used or unused blocks allocated by the BufferedBlockMgr.
  vector<Block*> all_blocks_;

  /// List of blocks is_pinned_ = false AND are not on DiskIoMgr's write queue.
  /// Blocks are added to and removed from the back of the list. (i.e. in LIFO order).
  /// Blocks in this list must have is_pinned_ = false, in_write_ = false,
  /// is_deleted_ = false.
  InternalQueue<Block> unpinned_blocks_;

  /// List of blocks that have been deleted and are no longer in use.
  /// Can be reused in GetNewBlock(). Blocks in this list must be in the Init'ed state,
  /// i.e. buffer_desc_ = NULL, is_pinned_ = false, in_write_ = false,
  /// is_deleted_ = false, valid_data_len = 0.
  InternalQueue<Block> unused_blocks_;

  /// List of buffers that can be assigned to a block in Pin() or GetNewBlock().
  /// These buffers either have no block associated with them or are associated with an
  /// an unpinned block that has been persisted. That is, either block = NULL or
  /// (!block->is_pinned_  && !block->in_write_  && !unpinned_blocks_.Contains(block)).
  /// All of these buffers are io sized.
  InternalQueue<BufferDescriptor> free_io_buffers_;

  /// All allocated io-sized buffers.
  std::list<BufferDescriptor*> all_io_buffers_;

  /// Group of temporary physical files, (one per tmp device) to which
  /// blocks may be written. Blocks are round-robined across these files.
  boost::scoped_ptr<TmpFileMgr::FileGroup> tmp_file_group_;

  /// If true, a disk write failed and all API calls return.
  /// Status::CANCELLED. Set to true if there was an error writing a block, or if
  /// WriteComplete() needed to reissue the write and that failed.
  bool is_cancelled_;

  /// Counters and timers to track behavior.
  boost::scoped_ptr<RuntimeProfile> profile_;

  /// These have a fixed value for the lifetime of the manager and show memory usage.
  RuntimeProfile::Counter* mem_limit_counter_;
  RuntimeProfile::Counter* block_size_counter_;

  /// Total number of blocks created.
  RuntimeProfile::Counter* created_block_counter_;

  /// Number of deleted blocks reused.
  RuntimeProfile::Counter* recycled_blocks_counter_;

  /// Number of Pin() calls that did not require a disk read.
  RuntimeProfile::Counter* buffered_pin_counter_;

  /// Time spent waiting for a free buffer.
  RuntimeProfile::Counter* buffer_wait_timer_;

  /// Number of writes outstanding (issued but not completed).
  RuntimeProfile::Counter* outstanding_writes_counter_;

  /// Number of writes issued.
  int writes_issued_;

  /// Protects query_to_block_mgrs_.
  static SpinLock static_block_mgrs_lock_;

  /// All per-query BufferedBlockMgr objects that are in use.  For memory management, this
  /// map contains only weak ptrs. BufferedBlockMgrs that are handed out are shared ptrs.
  /// When all the shared ptrs are no longer referenced, the BufferedBlockMgr
  /// d'tor will be called at which point the weak ptr will be removed from the map.
  typedef boost::unordered_map<TUniqueId, std::weak_ptr<BufferedBlockMgr>> BlockMgrsMap;
  static BlockMgrsMap query_to_block_mgrs_;

  /// Debug option to delay write completion.
  int debug_write_delay_ms_;

}; // class BufferedBlockMgr

} // namespace impala.

#endif
