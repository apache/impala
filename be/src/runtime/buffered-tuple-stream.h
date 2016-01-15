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

#ifndef IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_H
#define IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_H

#include "common/status.h"
#include "runtime/buffered-block-mgr.h"

namespace impala {

class BufferedBlockMgr;
class RuntimeProfile;
class RuntimeState;
class RowBatch;
class RowDescriptor;
class SlotDescriptor;
class TupleRow;

/// Class that provides an abstraction for a stream of tuple rows. Rows can be
/// added to the stream and returned. Rows are returned in the order they are added.
///
/// The underlying memory management is done by the BufferedBlockMgr.
///
/// The tuple stream consists of a number of small (less than IO-sized blocks) before
/// an arbitrary number of IO-sized blocks. The smaller blocks do not spill and are
/// there to lower the minimum buffering requirements. For example, an operator that
/// needs to maintain 64 streams (1 buffer per partition) would need, by default,
/// 64 * 8MB = 512MB of buffering. A query with 5 of these operators would require
/// 2.56GB just to run, regardless of how much of that is used. This is
/// problematic for small queries. Instead we will start with a fixed number of small
/// buffers (currently 2 small buffers: one 64KB and one 512KB) and only start using IO
/// sized buffers when those fill up. The small buffers never spill.
/// The stream will *not* automatically switch from using small buffers to IO-sized
/// buffers when all the small buffers for this stream have been used.
///
/// The BufferedTupleStream is *not* thread safe from the caller's point of view. It is
/// expected that all the APIs are called from a single thread. Internally, the
/// object is thread safe wrt to the underlying block mgr.
///
/// Buffer management:
/// The stream is either pinned or unpinned, set via PinStream() and UnpinStream().
/// Blocks are optionally deleted as they are read, set with the delete_on_read argument
/// to PrepareForRead().
///
/// Block layout:
/// At the header of each block, starting at position 0, there is a bitstring with null
/// indicators for all the tuples in each row in the block. Then there are the tuple rows.
/// We further optimize the codepaths when we know that no tuple is nullable, indicated
/// by 'nullable_tuple_'.
///
/// Tuple row layout:
/// Tuples are stored back to back. Each tuple starts with the fixed length portion,
/// directly followed by the var len portion. (Fixed len and var len are interleaved).
/// If any tuple in the row is nullable, then there is a bitstring of null tuple
/// indicators at the header of the block. The order of bits in the null indicators
/// bitstring corresponds to the order of tuples in the block. The NULL tuples are not
/// stored in the body of the block, only as set bits in the null indicators bitsting.
///
/// The behavior of reads and writes is as follows:
/// Read:
///   1. Delete on read (delete_on_read_): Blocks are deleted as we go through the stream.
///   The data returned by the tuple stream is valid until the next read call so the
///   caller does not need to copy if it is streaming.
///   2. Unpinned: Blocks remain in blocks_ and are unpinned after reading.
///   3. Pinned: Blocks remain in blocks_ and are left pinned after reading. If the next
///   block in the stream cannot be pinned, the read call will fail and the caller needs
///   to free memory from the underlying block mgr.
/// Write:
///   1. Unpinned: Unpin blocks as they fill up. This means only a single (i.e. the
///   current) block needs to be in memory regardless of the input size (if read_write is
///   true, then two blocks need to be in memory).
///   2. Pinned: Blocks are left pinned. If we run out of blocks, the write will fail and
///   the caller needs to free memory from the underlying block mgr.
///
/// TODO: we need to be able to do read ahead in the BufferedBlockMgr. It currently
/// only has PinAllBlocks() which is blocking. We need a non-blocking version of this or
/// some way to indicate a block will need to be pinned soon.
/// TODO: see if this can be merged with Sorter::Run. The key difference is that this
/// does not need to return rows in the order they were added, which allows it to be
/// simpler.
/// TODO: we could compact the small buffers when we need to spill but they use very
/// little memory so ths might not be very useful.
/// TODO: improvements:
///   - Think about how to layout for the var len data more, possibly filling in them
///     from the end of the same block. Don't interleave fixed and var len data.
///   - It would be good to allocate the null indicators at the end of each block and grow
///     this array as new rows are inserted in the block. If we do so, then there will be
///     fewer gaps in case of many rows with NULL tuples.
///   - We will want to multithread this. Add a AddBlock() call so the synchronization
///     happens at the block level. This is a natural extension.
///   - Instead of allocating all blocks from the block_mgr, allocate some blocks that
///     are much smaller (e.g. 16K and doubling up to the block size). This way, very
///     small streams (a common case) will use very little memory. This small blocks
///     are always in memory since spilling them frees up negligible memory.
///   - Return row batches in GetNext() instead of filling one in
///   - Should we 32-bit align the start of the tuple rows? Now it is byte-aligned.
class BufferedTupleStream {
 public:
  /// Ordinal index into the stream to retrieve a row in O(1) time. This index can
  /// only be used if the stream is pinned.
  /// To read a row from a stream we need three pieces of information that we squeeze in
  /// 64 bits:
  ///  - The index of the block. The block id is stored in 16 bits. We can have up to
  ///    64K blocks per tuple stream. With 8MB blocks that is 512GB per stream.
  ///  - The offset of the start of the row (data) within the block. Since blocks are 8MB
  ///    we use 24 bits for the offsets. (In theory we could use 23 bits.)
  ///  - The idx of the row in the block. We need this for retrieving the null indicators.
  ///    We use 24 bits for this index as well.
  struct RowIdx {
    static const uint64_t BLOCK_MASK  = 0xFFFF;
    static const uint64_t BLOCK_SHIFT = 0;
    static const uint64_t OFFSET_MASK  = 0xFFFFFF0000;
    static const uint64_t OFFSET_SHIFT = 16;
    static const uint64_t IDX_MASK  = 0xFFFFFF0000000000;
    static const uint64_t IDX_SHIFT = 40;

    uint64_t block() const {
      return (data & BLOCK_MASK);
    };

    uint64_t offset() const {
      return (data & OFFSET_MASK) >> OFFSET_SHIFT;
    };

    uint64_t idx() const {
      return (data & IDX_MASK) >> IDX_SHIFT;
    }

    uint64_t set(uint64_t block, uint64_t offset, uint64_t idx) {
      DCHECK_LE(block, BLOCK_MASK)
          << "Cannot have more than 2^16 = 64K blocks in a tuple stream.";
      DCHECK_LE(offset, OFFSET_MASK >> OFFSET_SHIFT)
          << "Cannot have blocks larger than 2^24 = 16MB";
      DCHECK_LE(idx, IDX_MASK >> IDX_SHIFT)
          << "Cannot have more than 2^24 = 16M rows in a block.";
      data = block | (offset << OFFSET_SHIFT) | (idx << IDX_SHIFT);
      return data;
    }

    std::string DebugString() const;

    uint64_t data;
  };

  /// row_desc: description of rows stored in the stream. This is the desc for rows
  /// that are added and the rows being returned.
  /// block_mgr: Underlying block mgr that owns the data blocks.
  /// use_initial_small_buffers: If true, the initial N buffers allocated for the
  /// tuple stream use smaller than IO-sized buffers.
  /// read_write: Stream allows interchanging read and write operations. Requires at
  /// least two blocks may be pinned.
  BufferedTupleStream(RuntimeState* state, const RowDescriptor& row_desc,
      BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      bool use_initial_small_buffers, bool read_write);

  ~BufferedTupleStream();

  /// Initializes the tuple stream object on behalf of node 'node_id'. Must be called
  /// once before any of the other APIs.
  /// If 'pinned' is true, the tuple stream starts of pinned, otherwise it is unpinned.
  /// If 'profile' is non-NULL, counters are created.
  /// 'node_id' is only used for error reporting.
  Status Init(int node_id, RuntimeProfile* profile, bool pinned);

  /// Must be called for streams using small buffers to switch to IO-sized buffers.
  /// If it fails to get a buffer (i.e. the switch fails) it resets the use_small_buffers_
  /// back to false.
  /// TODO: this does not seem like the best mechanism.
  Status SwitchToIoBuffers(bool* got_buffer);

  /// Adds a single row to the stream. Returns false and sets *status if an error
  /// occurred.  BufferedTupleStream will do a deep copy of the memory in the row.
  bool AddRow(TupleRow* row, Status* status);

  /// Allocates space to store a row of size 'size' and returns a pointer to the memory
  /// when successful. Returns NULL if there is not enough memory or an error occurred.
  /// When returning NULL, sets *status. The returned memory is guaranteed to fit on one
  /// block.
  uint8_t* AllocateRow(int size, Status* status);

  /// Populates 'row' with the row at 'idx'. The stream must be pinned. The row must have
  /// been allocated with the stream's row desc.
  void GetTupleRow(const RowIdx& idx, TupleRow* row) const;

  /// Prepares the stream for reading. If read_write_, this can be called at any time to
  /// begin reading. Otherwise this must be called after the last AddRow() and
  /// before GetNext().
  /// delete_on_read: Blocks are deleted after they are read.
  /// If got_buffer is NULL, this function will fail (with a bad status) if no buffer
  /// is available. If got_buffer is non-null, this function will not fail on OOM and
  /// *got_buffer is true if a buffer was pinned.
  Status PrepareForRead(bool delete_on_read, bool* got_buffer = NULL);

  /// Pins all blocks in this stream and switches to pinned mode.
  /// If there is not enough memory, *pinned is set to false and the stream is unmodified.
  /// If already_reserved is true, the caller has already made a reservation on
  /// block_mgr_client_ to pin the stream.
  Status PinStream(bool already_reserved, bool* pinned);

  /// Unpins stream. If all is true, all blocks are unpinned, otherwise all blocks
  /// except the write_block_ and read_block_ are unpinned.
  Status UnpinStream(bool all = false);

  /// Get the next batch of output rows. Memory is still owned by the BufferedTupleStream
  /// and must be copied out by the caller.
  /// If 'indices' is non-NULL, that is also populated for each returned row with the
  /// index for that row.
  Status GetNext(RowBatch* batch, bool* eos, std::vector<RowIdx>* indices = NULL);

  /// Returns all the rows in the stream in batch. This pins the entire stream
  /// in the process.
  /// *got_rows is false if the stream could not be pinned.
  Status GetRows(boost::scoped_ptr<RowBatch>* batch, bool* got_rows);

  /// Must be called once at the end to cleanup all resources. Idempotent.
  void Close();

  /// Number of rows in the stream.
  int64_t num_rows() const { return num_rows_; }

  /// Number of rows returned via GetNext().
  int64_t rows_returned() const { return rows_returned_; }

  /// Returns the byte size necessary to store the entire stream in memory.
  int64_t byte_size() const { return total_byte_size_; }

  /// Returns the byte size of the stream that is currently pinned in memory.
  /// If ignore_current is true, the write_block_ memory is not included.
  int64_t bytes_in_mem(bool ignore_current) const;

  bool is_pinned() const { return pinned_; }
  int blocks_pinned() const { return num_pinned_; }
  int blocks_unpinned() const { return blocks_.size() - num_pinned_ - num_small_blocks_; }
  bool has_read_block() const { return read_block_ != blocks_.end(); }
  bool has_write_block() const { return write_block_ != NULL; }
  bool using_small_buffers() const { return use_small_buffers_; }
  bool has_tuple_footprint() const {
    return fixed_tuple_row_size_ > 0 || !string_slots_.empty() || nullable_tuple_;
  }

  std::string DebugString() const;

 private:
  friend class ArrayTupleStreamTest_TestArrayDeepCopy_Test;

  /// If true, this stream is still using small buffers.
  bool use_small_buffers_;

  /// If true, blocks are deleted after they are read.
  bool delete_on_read_;

  /// If true, read and write operations may be interleaved. Otherwise all calls
  /// to AddRow() must occur before calling PrepareForRead() and subsequent calls to
  /// GetNext().
  const bool read_write_;

  /// Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// Description of rows stored in the stream.
  const RowDescriptor& desc_;

  /// Whether any tuple in the rows is nullable.
  const bool nullable_tuple_;

  /// Sum of the fixed length portion of all the tuples in desc_.
  int fixed_tuple_row_size_;

  /// Max size (in bytes) of null indicators bitstring in the current read and write
  /// blocks. If 0, it means that there is no need to store null indicators for this
  /// RowDesc. We calculate this value based on the block's size and the
  /// fixed_tuple_row_size_. When not 0, this value is also an upper bound for the number
  /// of (rows * tuples_per_row) in this block.
  uint32_t null_indicators_read_block_;
  uint32_t null_indicators_write_block_;

  /// Vector of all the strings slots grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*> > > string_slots_;

  /// Vector of all the collection slots grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*> > > collection_slots_;

  /// Block manager and client used to allocate, pin and release blocks. Not owned.
  BufferedBlockMgr* block_mgr_;
  BufferedBlockMgr::Client* block_mgr_client_;

  /// List of blocks in the stream.
  std::list<BufferedBlockMgr::Block*> blocks_;

  /// Total size of blocks_, including small blocks.
  int64_t total_byte_size_;

  /// Iterator pointing to the current block for read. Equal to list.end() until
  /// PrepareForRead() is called.
  std::list<BufferedBlockMgr::Block*>::iterator read_block_;

  /// For each block in the stream, the buffer of the start of the block. This is only
  /// valid when the stream is pinned, giving random access to data in the stream.
  /// This is not maintained for delete_on_read_.
  std::vector<uint8_t*> block_start_idx_;

  /// Current ptr offset in read_block_'s buffer.
  uint8_t* read_ptr_;

  /// Current idx of the tuple read from the read_block_ buffer.
  uint32_t read_tuple_idx_;

  /// Current idx of the tuple written at the write_block_ buffer.
  uint32_t write_tuple_idx_;

  /// Bytes read in read_block_.
  int64_t read_bytes_;

  /// Number of rows returned to the caller from GetNext().
  int64_t rows_returned_;

  /// The block index of the current read block.
  int read_block_idx_;

  /// The current block for writing. NULL if there is no available block to write to.
  BufferedBlockMgr::Block* write_block_;

  /// Number of pinned blocks in blocks_, stored to avoid iterating over the list
  /// to compute bytes_in_mem and bytes_unpinned.
  /// This does not include small blocks.
  int num_pinned_;

  /// The total number of small blocks in blocks_;
  int num_small_blocks_;

  bool closed_; // Used for debugging.

  /// Number of rows stored in the stream.
  int64_t num_rows_;

  /// If true, this stream has been explicitly pinned by the caller. This changes the
  /// memory management of the stream. The blocks are not unpinned until the caller calls
  /// UnpinAllBlocks(). If false, only the write_block_ and/or read_block_ are pinned
  /// (both are if read_write_ is true).
  bool pinned_;

  /// Counters added by this object to the parent runtime profile.
  RuntimeProfile::Counter* pin_timer_;
  RuntimeProfile::Counter* unpin_timer_;
  RuntimeProfile::Counter* get_new_block_timer_;

  /// Copies 'row' into write_block_. Returns false if there is not enough space in
  /// 'write_block_'.
  template <bool HasNullableTuple>
  bool DeepCopyInternal(TupleRow* row);

  // Helper function to copy strings from tuple into write_block_. Increments
  // bytes_allocated by the number of bytes allocated from write_block_.
  bool CopyStrings(const Tuple* tuple, const std::vector<SlotDescriptor*>& string_slots,
      int* bytes_allocated);

  // Helper function to deep copy collections from tuple into write_block_. Increments
  // bytes_allocated by the number of bytes allocated from write_block_.
  bool CopyCollections(const Tuple* tuple,
      const std::vector<SlotDescriptor*>& collection_slots, int* bytes_allocated);

  /// Wrapper of the templated DeepCopyInternal() function.
  bool DeepCopy(TupleRow* row);

  /// Gets a new block from the block_mgr_, updating write_block_ and write_tuple_idx_,
  /// and setting *got_block. If there are no blocks available, *got_block is set to
  /// false and write_block_ is unchanged.
  /// 'min_size' is the minimum number of bytes required for this block.
  Status NewBlockForWrite(int64_t min_size, bool* got_block);

  /// Reads the next block from the block_mgr_. This blocks if necessary.
  /// Updates read_block_, read_ptr_, read_tuple_idx_ and read_bytes_.
  Status NextBlockForRead();

  /// Returns the byte size of this row when encoded in a block.
  int64_t ComputeRowSize(TupleRow* row) const;

  /// Unpins block if it is an IO-sized block and updates tracking stats.
  Status UnpinBlock(BufferedBlockMgr::Block* block);

  /// Templated GetNext implementation.
  template <bool HasNullableTuple>
  Status GetNextInternal(RowBatch* batch, bool* eos, std::vector<RowIdx>* indices);

  /// Read strings from stream by converting pointers and updating read_ptr_ and
  /// read_bytes_.
  void ReadStrings(const vector<SlotDescriptor*>& string_slots, int data_len,
      Tuple* tuple);

  /// Read collections from stream by converting pointers and updating read_ptr_ and
  /// read_bytes_.
  void ReadCollections(const vector<SlotDescriptor*>& collection_slots, int data_len,
      Tuple* tuple);

  /// Computes the number of bytes needed for null indicators for a block of 'block_size'
  int ComputeNumNullIndicatorBytes(int block_size) const;
};

}

#endif
