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

#include <vector>
#include <set>

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
/// If the stream's tuples are nullable (i.e. has_nullable_tuple_ is true), there is a
/// bitstring at the start of each block with null indicators for all tuples in each row
/// in the block. The length of the  bitstring is a function of the block size. Row data
/// is stored after the null indicators if present, or at the start of the block
/// otherwise. Rows are stored back to back in the stream, with no interleaving of data
/// from different rows. There is no padding or alignment between rows.
///
/// Null tuples:
/// The order of bits in the null indicators bitstring corresponds to the order of
/// tuples in the block. The NULL tuples are not stored in the row iself, only as set
/// bits in the null indicators bitstring.
///
/// Tuple row layout:
/// The fixed length parts of the row's tuples are stored first, followed by var len data
/// for inlined_string_slots_ and inlined_coll_slots_. Other "external" var len slots can
/// point to var len data outside the stream. When reading the stream, the length of each
/// row's var len data in the stream must be computed to find the next row's start.
///
/// The tuple stream supports reading from the stream into RowBatches without copying
/// out any data: the RowBatches' Tuple pointers will point directly into the stream's
/// blocks. The fixed length parts follow Impala's internal tuple format, so for the
/// tuple to be valid, we only need to update pointers to point to the var len data
/// in the stream. These pointers need to be updated by the stream because a spilled
/// block may be relocated to a different location in memory. The pointers are updated
/// lazily upon reading the stream via GetNext() or GetRows().
///
/// Example layout for a row with two tuples ((1, "hello"), (2, "world")) with all var
/// len data stored in the stream:
///  <---- tuple 1 -----> <------ tuple 2 ------> <- var len -> <- next row ...
/// +--------+-----------+-----------+-----------+-------------+
/// | IntVal | StringVal | BigIntVal | StringVal |             | ...
/// +--------+-----------+-----------+-----------++------------+
/// | val: 1 | len: 5    | val: 2    | len: 5    | helloworld  | ...
/// |        | ptr: 0x.. |           | ptr: 0x.. |             | ...
/// +--------+-----------+-----------+-----------+-------------+
///  <--4b--> <---12b---> <----8b---> <---12b---> <----10b---->
//
/// Example layout for a row with a single tuple (("hello", "world")) with the second
/// string slot stored externally to the stream:
///  <------ tuple 1 ------> <- var len ->  <- next row ...
/// +-----------+-----------+-------------+
/// | StringVal | StringVal |             | ...
/// +-----------+-----------+-------------+
/// | len: 5    | len: 5    |  hello      | ...
/// | ptr: 0x.. | ptr: 0x.. |             | ...
/// +-----------+-----------+-------------+
///  <---12b---> <---12b---> <-----5b---->
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
/// Memory lifetime of rows read from stream:
/// If the stream is pinned, it is valid to access any tuples returned via
/// GetNext() or GetRows() until the stream is unpinned. If the stream is unpinned, and
/// the batch returned from GetNext() has the need_to_return flag set, any tuple memory
/// returned so far from the stream may be freed on the next call to GetNext().
///
/// Manual construction of rows with AllocateRow():
/// The BufferedTupleStream supports allocation of uninitialized rows with AllocateRow().
/// The caller of AllocateRow() is responsible for writing the row with exactly the
/// layout described above.
///
/// If a caller constructs a tuple in this way, the caller can set the pointers and they
/// will not be modified until the stream is read via GetNext() or GetRows().
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
  /// ext_varlen_slots: set of varlen slots with data stored externally to the stream
  BufferedTupleStream(RuntimeState* state, const RowDescriptor& row_desc,
      BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      bool use_initial_small_buffers, bool read_write,
      const std::set<SlotId>& ext_varlen_slots = std::set<SlotId>());

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
  /// occurred. BufferedTupleStream will do a deep copy of the memory in the row.
  /// After AddRow returns false, it should not be called again, unless
  /// using_small_buffers_ is true, in which case it is valid to call SwitchToIoBuffers()
  /// then AddRow() again.
  bool AddRow(TupleRow* row, Status* status);

  /// Allocates space to store a row of with fixed length 'fixed_size' and variable
  /// length data 'varlen_size'. If successful, returns the pointer where fixed length
  /// data should be stored and assigns 'varlen_data' to where var-len data should
  /// be stored. Returns NULL if there is not enough memory or an error occurred.
  /// Sets *status if an error occurred. The returned memory is guaranteed to all
  /// be allocated in the same block. AllocateRow does not currently support nullable
  /// tuples.
  uint8_t* AllocateRow(int fixed_size, int varlen_size, uint8_t** varlen_data,
      Status* status);

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
  Status GetNext(RowBatch* batch, bool* eos);

  /// Same as above, but also populate 'indices' with the index of each returned row.
  Status GetNext(RowBatch* batch, bool* eos, std::vector<RowIdx>* indices);

  /// Returns all the rows in the stream in batch. This pins the entire stream in the
  /// process.
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

  /// Returns true if the row consumes any memory. If false, the stream only needs to
  /// store the count of rows.
  bool RowConsumesMemory() const {
    return fixed_tuple_row_size_ > 0 || has_nullable_tuple_;
  }

  std::string DebugString() const;

 private:
  friend class MultiNullableTupleStreamTest_TestComputeRowSize_Test;
  friend class ArrayTupleStreamTest_TestArrayDeepCopy_Test;
  friend class ArrayTupleStreamTest_TestComputeRowSize_Test;

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
  const bool has_nullable_tuple_;

  /// Sum of the fixed length portion of all the tuples in desc_.
  int fixed_tuple_row_size_;

  /// The size of the fixed length portion for each tuple in the row.
  std::vector<int> fixed_tuple_sizes_;

  /// Max size (in bytes) of null indicators bitstring in the current read and write
  /// blocks. If 0, it means that there is no need to store null indicators for this
  /// RowDesc. We calculate this value based on the block's size and the
  /// fixed_tuple_row_size_. When not 0, this value is also an upper bound for the number
  /// of (rows * tuples_per_row) in this block.
  uint32_t read_block_null_indicators_size_;
  uint32_t write_block_null_indicators_size_;

  /// Vectors of all the strings slots that have their varlen data stored in stream
  /// grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*> > > inlined_string_slots_;

  /// Vectors of all the collection slots that have their varlen data stored in the
  /// stream, grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*> > > inlined_coll_slots_;

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

  /// Current idx of the tuple read from the read_block_ buffer.
  uint32_t read_tuple_idx_;

  /// Current offset in read_block_ of the end of the last data read.
  uint8_t* read_ptr_;

  /// Pointer to one byte past the end of read_block_.
  uint8_t* read_end_ptr_;

  /// Current idx of the tuple written at the write_block_ buffer.
  uint32_t write_tuple_idx_;

  /// Pointer into write_block_ of the end of the last data written.
  uint8_t*  write_ptr_;

  /// Pointer to one byte past the end of write_block_.
  uint8_t* write_end_ptr_;

  /// Number of rows returned to the caller from GetNext().
  int64_t rows_returned_;

  /// The block index of the current read block in blocks_.
  int read_block_idx_;

  /// The current block for writing. NULL if there is no available block to write to.
  /// The entire write_block_ buffer is marked as allocated, so any data written into
  /// the buffer will be spilled without having to allocate additional space.
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
  /// 'write_block_'. After returning false, write_ptr_ may be left pointing to the
  /// partially-written row, and no more data can be written to write_block_.
  template <bool HAS_NULLABLE_TUPLE>
  bool DeepCopyInternal(TupleRow* row);

  /// Helper function to copy strings in string_slots from tuple into write_block_.
  /// Updates write_ptr_ to the end of the string data added. Returns false if the data
  /// does not fit in the current write block. After returning false, write_ptr_ is left
  /// pointing to the partially-written row, and no more data can be written to
  /// write_block_.
  bool CopyStrings(const Tuple* tuple, const std::vector<SlotDescriptor*>& string_slots);

  /// Helper function to deep copy collections in collection_slots from tuple into
  /// write_block_. Updates write_ptr_ to the end of the collection data added. Returns
  /// false if the data does not fit in the current write block.. After returning false,
  /// write_ptr_ is left pointing to the partially-written row, and no more data can be
  /// written to write_block_.
  bool CopyCollections(const Tuple* tuple,
      const std::vector<SlotDescriptor*>& collection_slots);

  /// Wrapper of the templated DeepCopyInternal() function.
  bool DeepCopy(TupleRow* row);

  /// Gets a new block from the block_mgr_, updating write_block_, write_tuple_idx_,
  /// write_ptr_ and write_end_ptr_. *got_block is set to true if a block was
  /// successfully acquired. If there are no blocks available, *got_block is set to
  /// false and write_block_ is unchanged.
  /// 'min_size' is the minimum number of bytes required for this block.
  Status NewBlockForWrite(int64_t min_size, bool* got_block);

  /// Reads the next block from the block_mgr_. This blocks if necessary.
  /// Updates read_block_, read_ptr_, read_tuple_idx_ and read_end_ptr_.
  Status NextBlockForRead();

  /// Returns the total additional bytes that this row will consume in write_block_ if
  /// appended to the block. This includes the fixed length part of the row and the
  /// data for inlined_string_slots_ and inlined_coll_slots_.
  int64_t ComputeRowSize(TupleRow* row) const;

  /// Unpins block if it is an IO-sized block and updates tracking stats.
  Status UnpinBlock(BufferedBlockMgr::Block* block);

  /// Templated GetNext implementations.
  template <bool FILL_INDICES>
  Status GetNextInternal(RowBatch* batch, bool* eos, std::vector<RowIdx>* indices);
  template <bool FILL_INDICES, bool HAS_NULLABLE_TUPLE>
  Status GetNextInternal(RowBatch* batch, bool* eos, std::vector<RowIdx>* indices);

  /// Helper function for GetNextInternal(). For each string slot in string_slots,
  /// update StringValue's ptr field to point to the corresponding string data stored
  /// inline in the stream (at the current value of read_ptr_) advance read_ptr_ by the
  /// StringValue's length field.
  void FixUpStringsForRead(const vector<SlotDescriptor*>& string_slots, Tuple* tuple);

  /// Helper function for GetNextInternal(). For each collection slot in collection_slots,
  /// recursively update any pointers in the CollectionValue to point to the corresponding
  /// var len data stored inline in the stream, advancing read_ptr_ as data is read.
  /// Assumes that the collection was serialized to the stream in DeepCopy()'s format.
  void FixUpCollectionsForRead(const vector<SlotDescriptor*>& collection_slots,
      Tuple* tuple);

  /// Computes the number of bytes needed for null indicators for a block of 'block_size'
  int ComputeNumNullIndicatorBytes(int block_size) const;

  uint32_t read_block_bytes_remaining() const {
    DCHECK_GE(read_end_ptr_, read_ptr_);
    DCHECK_LE(read_end_ptr_ - read_ptr_, (*read_block_)->buffer_len());
    return read_end_ptr_ - read_ptr_;
  }

  uint32_t write_block_bytes_remaining() const {
    DCHECK_GE(write_end_ptr_, write_ptr_);
    DCHECK_LE(write_end_ptr_ - write_ptr_, write_block_->buffer_len());
    return write_end_ptr_ - write_ptr_;
  }

};

}

#endif
