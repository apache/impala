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

#include "runtime/sorter.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/sorted-run-merger.h"
#include "util/runtime-profile.h"

using namespace boost;
using namespace std;

namespace impala {

// A run is a sequence of blocks containing tuples that are or will eventually be in
// sorted order.
// A run may maintain two sequences of blocks - one containing the tuples themselves,
// (i.e. fixed-len slots and ptrs to var-len data), and the other for the var-length
// column data pointed to by those tuples.
// Tuples in a run may be sorted in place (in-memory) and merged using a merger.
class Sorter::Run {
 public:
  // materialize_slots is true for runs constructed from input rows. The input rows
  // are materialized into single sort tuples using the expressions in
  // sort_tuple_slot_exprs_. For intermediate merges, the tuples are already materialized
  // so materialize_slots is false.
  Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool materialize_slots);

  // Initialize the run for input rows by allocating new blocks.
  Status Init();

  // Add a batch of input rows to the current run. Returns the number
  // of rows actually added in num_processed. If the run is full (no more blocks can
  // be allocated), num_processed may be less than the number of rows in the batch.
  // If materialize_slots_ is true, materializes the input rows using the expressions
  // in sorter_->sort_tuple_slot_exprs_, else just copies the input rows.
  template <bool has_var_len_data>
  Status AddBatch(RowBatch* batch, int start_index, int* num_processed);

  // Unpins all the blocks in a sorted run. Var-length column data is copied into new
  // blocks in sorted order. Pointers in the original tuples are converted to offsets
  // from the beginning of the sequence of var-len data blocks.
  Status UnpinAllBlocks();

  // Interface for merger - get the next batch of rows from this run. The callee (Run)
  // still owns the returned batch. Calls GetNext(RowBatch*, bool*).
  Status GetNextBatch(RowBatch** sorted_batch);

 private:
  friend class Sorter;
  friend class TupleSorter;

  // Fill output_batch with rows from this run. If convert_offset_to_ptr is true, offsets
  // in var-length slots are converted back to pointers. Only row pointers are copied
  // into output_batch.
  // If this run was unpinned, one block (2 if there are var-len slots) is pinned while
  // rows are filled into output_batch. The block is unpinned before the next block is
  // pinned. Atmost 1 (2) block(s) will be pinned at any time.
  // If the run was pinned, the blocks are not unpinned (Sorter holds on to the memory).
  // In either case, all rows in output_batch will have their fixed and var-len data from
  // the same block.
  // TODO: If we leave the last run to be merged in memory, the fixed-len blocks can be
  // unpinned as they are consumed.
  template <bool convert_offset_to_ptr>
  Status GetNext(RowBatch* output_batch, bool* eos);

  // Check if a run can be extended by allocating additional blocks from the block
  // manager. Always true when building a sorted run in an intermediate merge, because
  // the current block(s) can be unpinned before getting the next free block (so a block
  // is always available)
  bool CanExtendRun() const;

  // Collect the non-null var-len (e.g. STRING) slots from 'src' in var_slots and return
  // the total length of all var_len slots in total_var_len.
  void CollectNonNullVarSlots(Tuple* src, vector<StringValue*>* var_len_values,
      int* total_var_len);

  // Check if the current run can be extended by a block. Add the newly allocated block
  // to block_sequence, or set added to false if the run could not be extended.
  // If the run is sorted (produced by an intermediate merge), unpin the last block in
  // block_sequence before allocating and adding a new block - the run can always be
  // extended in this case. If the run is unsorted, check max_blocks_in_unsorted_run_
  // to see if a block can be added to the run. Also updates the sort bytes counter.
  Status TryAddBlock(vector<BufferedBlockMgr::Block*>* block_sequence, bool* added);

  // Prepare to read a sorted run. Pins the first block(s) in the run if the run was
  // previously unpinned.
  Status PrepareRead();

  // Copy the StringValue data in var_values to dest in order and update the StringValue
  // ptrs to point to the copied data.
  void CopyVarLenData(char* dest, const vector<StringValue*>& var_values);

  // Copy the StringValue in var_values to dest in order. Update the StringValue ptrs to
  // contain an offset to the copied data. Parameter 'offset' is the offset for the first
  // StringValue.
  void CopyVarLenDataConvertOffset(char* dest, int64_t offset,
      const vector<StringValue*>& var_values);

  // Parent sorter object.
  const Sorter* sorter_;

  // Materialized sort tuple. Input rows are materialized into 1 tuple (with descriptor
  // sort_tuple_desc_) before sorting.
  const TupleDescriptor* sort_tuple_desc_;

  // Sizes of sort tuple and block.
  const int sort_tuple_size_;
  const int block_size_;

  const bool has_var_len_slots_;

  // Maximum number of blocks that can be used to build an unsorted run.
  // When building an unsorted run, all the blocks in the run must be pinned
  // simultaneously. Additionally, an extra block must be pinned in UnpinAllBlocks()
  // to copy over var-len data in sorted order. Therefore, this equals the maximum
  // number of buffers available from the block manager - 1.
  const int max_blocks_in_unsorted_run_;

  // True if the sort tuple must be materialized from the input batch in AddBatch().
  // materialize_slots_ is true for runs being constructed from input batches, and
  // is false for runs being constructed from intermediate merges.
  const bool materialize_slots_;

  // True if the run is sorted. Set to true after an in-memory sort, and initialized to
  // true for runs resulting from merges.
  bool is_sorted_;

  // True if all blocks in the run are pinned.
  bool is_pinned_;

  // Sequence of blocks in this run containing the fixed-length portion of the
  // sort tuples comprising this run. The data pointed to by the var-len slots are in
  // var_len_blocks_. If is_sorted_ is true, the tuples in fixed_len_blocks_ will be in
  // sorted order.
  vector<BufferedBlockMgr::Block*> fixed_len_blocks_;

  // Sequence of blocks in this run containing the var-length data corresponding to the
  // var-length column data from fixed_len_blocks_. These are reconstructed to be in
  // sorted order in UnpinAllBlocks().
  vector<BufferedBlockMgr::Block*> var_len_blocks_;

  // Number of tuples so far in this run.
  int64_t num_tuples_;

  // Number of tuples returned via GetNext(), maintained for debug purposes.
  int64_t num_tuples_returned_;

  // buffered_batch_ is used to return TupleRows to the merger when this run is being
  // merged. buffered_batch_ is returned in calls to GetNextBatch().
  scoped_ptr<RowBatch> buffered_batch_;

  // Members used when a run is read in GetNext()
  // The index into the fixed_ and var_len_blocks_ vectors of the current blocks being
  // processed in GetNext().
  int fixed_len_blocks_index_;
  int var_len_blocks_index_;

  // If true, pin the next fixed and var-len blocks and delete the previous ones
  // during in the next call to GetNext(). Set during the previous call to GetNext().
  // Not used if a run is already pinned.
  bool pin_next_fixed_len_block_;
  bool pin_next_var_len_block_;

  // Offset into the current fixed length data block being processed.
  int fixed_len_block_offset_;
}; // class Sorter::Run

// Sorts a sequence of tuples from a run in place using a provided tuple comparator.
// Quick sort is used for sequences of tuples larger that 16 elements, and
// insertion sort is used for smaller sequences.
class Sorter::TupleSorter {
 public:
  typedef function<bool (Tuple*, Tuple*)> LessThanComparator;

  TupleSorter(const LessThanComparator& less_than_comp, int64_t block_size,
      int tuple_size);

  ~TupleSorter();

  // Performs a quicksort for tuples in the range [0, num_tuples)
  // followed by an insertion sort to finish smaller blocks.
  void Sort(Run* run, int64_t num_tuples);

 private:
  static const int INSERTION_THRESHOLD = 16;

  // The run to be sorted.
  Run* run_;

  // Size of the tuples in memory.
  const int tuple_size_;

  // Number of tuples per block in a run.
  const int block_capacity_;

  // Comparator that returns true if lhs < rhs.
  LessThanComparator less_than_comp_;

  // Temporarily allocated space to copy and swap tuples (Both are used in Partition()).
  // temp_tuple_ points to temp_tuple_buffer_. Owned by this TupleSorter instance.
  Tuple* temp_tuple_;
  uint8_t* temp_tuple_buffer_;
  uint8_t* swap_buffer_;

  // Perform an insertion sort for rows in the range [first, last) in a run.
  void InsertionSort(int64_t first, int64_t last);

  // Partitions the sequence of tuples in the range [first, last) in a run into two
  // groups around the pivot tuple - i.e. tuples in first group are <= the pivot, and
  // tuples in the second group are >= pivot. Tuples are swapped in place to create the
  // groups and the index to the first element in the second group is returned.
  int64_t Partition(int64_t first, int64_t last, Tuple* pivot);

  // Performs a quicksort of rows in the range [first, last).
  // followed by insertion sort for smaller groups of elements.
  void SortHelper(int64_t first, int64_t last);

  // Swaps left and right tuples using the swap buffer.
  void Swap(Tuple* left, Tuple* right);

  // Get the tuple at the specified index from run_.
  Tuple* GetTuple(int64_t index);
}; // class TupleSorter

// Sorter::Run methods
Sorter::Run::Run(Sorter* parent, TupleDescriptor* sort_tuple_desc,
    bool materialize_slots)
  : sorter_(parent),
    is_sorted_(false),
    is_pinned_(true),
    sort_tuple_desc_(sort_tuple_desc),
    sort_tuple_size_(sort_tuple_desc->byte_size()),
    materialize_slots_(materialize_slots),
    has_var_len_slots_(sort_tuple_desc->string_slots().size() > 0),
    max_blocks_in_unsorted_run_(parent->block_mgr_->max_available_buffers() - 1),
    block_size_(parent->block_mgr_->block_size()),
    num_tuples_(0) {
}

Status Sorter::Run::Init() {
  BufferedBlockMgr::Block* new_block;
  RETURN_IF_ERROR(sorter_->block_mgr_->GetFreeBlock(&new_block));
  fixed_len_blocks_.push_back(new_block);
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(sorter_->block_mgr_->GetFreeBlock(&new_block));
    var_len_blocks_.push_back(new_block);
  }
  sorter_->num_runs_counter_->Update(1);
  return Status::OK;
}

template <bool has_var_len_data>
Status Sorter::Run::AddBatch(RowBatch* batch, int start_index, int* num_processed) {
  *num_processed = 0;
  BufferedBlockMgr::Block* cur_fixed_len_block = fixed_len_blocks_.back();
  BufferedBlockMgr::Block* new_block;

  DCHECK_EQ(materialize_slots_, !is_sorted_);
  if (!materialize_slots_) {
    // If materialize slots is false the run is being constructed for an
    // intermediate merge and the sort tuples have already been materialized.
    // The input row should have the same schema as the sort tuples.
    DCHECK_EQ(batch->row_desc().tuple_descriptors().size(), 1);
    DCHECK_EQ(batch->row_desc().tuple_descriptors()[0], sort_tuple_desc_);
  }

  // Input rows are copied/materialized into tuples allocated in fixed_len_blocks_.
  // The variable length column data are copied into blocks stored in var_len_blocks_.
  // Input row processing is split into two loops.
  // The inner loop processes as many input rows as will fit in cur_fixed_len_block.
  // The outer loop allocates a new block for fixed-len data if the input batch is
  // not exhausted.

  // cur_input_index is the index into the input 'batch' of the current
  // input row being processed.
  int cur_input_index = start_index;
  vector<StringValue*> var_values;
  var_values.reserve(sort_tuple_desc_->string_slots().size());
  while (cur_input_index < batch->num_rows()) {
    // tuples_remaining is the number of tuples to copy/materialize into
    // cur_fixed_len_block.
    int tuples_remaining = cur_fixed_len_block->BytesRemaining() / sort_tuple_size_;
    tuples_remaining = min(batch->num_rows() - cur_input_index, tuples_remaining);

    for (int i = 0; i < tuples_remaining; ++i) {
      int total_var_len = 0;
      TupleRow* input_row = batch->GetRow(cur_input_index);
      Tuple* new_tuple = cur_fixed_len_block->Allocate<Tuple>(sort_tuple_size_);
      if (materialize_slots_) {
        new_tuple->MaterializeExprs<has_var_len_data>(input_row, *sort_tuple_desc_,
            sorter_->sort_tuple_slot_exprs_, NULL, &var_values, &total_var_len);
      } else {
        memcpy(new_tuple, input_row->GetTuple(0), sort_tuple_size_);
        if (has_var_len_data) {
          CollectNonNullVarSlots(new_tuple, &var_values, &total_var_len);
        }
      }

      if (has_var_len_data) {
        BufferedBlockMgr::Block* cur_var_len_block = var_len_blocks_.back();
        if (cur_var_len_block->BytesRemaining() < total_var_len) {
          bool added;
          RETURN_IF_ERROR(TryAddBlock(&var_len_blocks_, &added));
          if (added) {
            cur_var_len_block = var_len_blocks_.back();
          } else {
            // There wasn't enough space in the last var-len block for this tuple, and
            // the run could not be extended. Return the fixed-len allocation and exit.
            cur_fixed_len_block->ReturnAllocation(sort_tuple_size_);
            return Status::OK;
          }
        }

        char* var_data_ptr = cur_var_len_block->Allocate<char>(total_var_len);
        if (materialize_slots_) {
          CopyVarLenData(var_data_ptr, var_values);
        } else {
          int64_t offset = (var_len_blocks_.size() - 1) * block_size_;
          offset += var_data_ptr - reinterpret_cast<char*>(cur_var_len_block->buffer());
          CopyVarLenDataConvertOffset(var_data_ptr, offset, var_values);
        }
      }

      ++num_tuples_;
      ++*num_processed;
      ++cur_input_index;
    }

    // If there are still rows left to process, get a new block for the fixed-length
    // tuples. If the  run is already too long, return.
    if (cur_input_index < batch->num_rows()) {
      bool added;
      RETURN_IF_ERROR(TryAddBlock(&fixed_len_blocks_, &added));
      if (added) {
        cur_fixed_len_block = fixed_len_blocks_.back();
      } else {
        return Status::OK;
      }
    }
  }

  return Status::OK;
}

Status Sorter::Run::UnpinAllBlocks() {
  vector<BufferedBlockMgr::Block*> sorted_var_len_blocks;
  // If there are var-len slots, var len data will be copied from var_len_blocks_
  // into sorted_var_len_blocks in sorted order.
  sorted_var_len_blocks.reserve(var_len_blocks_.size());
  vector<StringValue*> var_values;
  int64_t var_data_offset = 0;
  int total_var_len;
  var_values.reserve(sort_tuple_desc_->string_slots().size());
  BufferedBlockMgr::Block* cur_sorted_var_len_block;
  if (has_var_len_slots_ && var_len_blocks_.size() > 0) {
    BufferedBlockMgr::Block* new_block;
    RETURN_IF_ERROR(sorter_->block_mgr_->GetFreeBlock(&new_block));
    sorted_var_len_blocks.push_back(new_block);
    cur_sorted_var_len_block = sorted_var_len_blocks.back();
  }

  for (int i = 0; i < fixed_len_blocks_.size(); ++i) {
    BufferedBlockMgr::Block* cur_fixed_block = fixed_len_blocks_[i];
    if (has_var_len_slots_) {
      for (int block_offset = 0; block_offset < cur_fixed_block->valid_data_len();
          block_offset += sort_tuple_size_) {
        Tuple* cur_tuple =
            reinterpret_cast<Tuple*>(cur_fixed_block->buffer() + block_offset);
        CollectNonNullVarSlots(cur_tuple, &var_values, &total_var_len);
        if (cur_sorted_var_len_block->BytesRemaining() < total_var_len) {
          bool added;
          RETURN_IF_ERROR(TryAddBlock(&sorted_var_len_blocks, &added));
          DCHECK(added);
          cur_sorted_var_len_block = sorted_var_len_blocks.back();
        }
        char* var_data_ptr = cur_sorted_var_len_block->Allocate<char>(total_var_len);
        var_data_offset = block_size_ * (sorted_var_len_blocks.size() - 1) +
            (var_data_ptr - reinterpret_cast<char*>(cur_sorted_var_len_block->buffer()));
        CopyVarLenDataConvertOffset(var_data_ptr, var_data_offset, var_values);
      }
    }
    RETURN_IF_ERROR(cur_fixed_block->Unpin());
  }

  if (has_var_len_slots_ && var_len_blocks_.size() > 0) {
    DCHECK_GT(sorted_var_len_blocks.back()->valid_data_len(), 0);
    RETURN_IF_ERROR(sorted_var_len_blocks.back()->Unpin());
  }

  // Clear var_len_blocks_ and replace with it with the contents of sorted_var_len_blocks
  BOOST_FOREACH(BufferedBlockMgr::Block* var_block, var_len_blocks_) {
    RETURN_IF_ERROR(var_block->Delete());
  }
  var_len_blocks_.clear();
  var_len_blocks_.insert(var_len_blocks_.begin(), sorted_var_len_blocks.begin(),
      sorted_var_len_blocks.end());

  is_pinned_ = false;
  return Status::OK;
}

Status Sorter::Run::PrepareRead() {
  fixed_len_blocks_index_ = 0;
  fixed_len_block_offset_ = 0;
  var_len_blocks_index_ = 0;
  pin_next_fixed_len_block_ = pin_next_var_len_block_ = false;
  num_tuples_returned_ = 0;

  buffered_batch_.reset(new RowBatch(*sorter_->output_row_desc_,
      sorter_->merge_batch_size_, sorter_->mem_tracker_));

  // If the run is pinned, merge is not invoked, so buffered_batch_ is not needed
  // and the individual blocks do not need to be pinned.
  if (is_pinned_) return Status::OK;

  if (fixed_len_blocks_.size() > 0) {
    RETURN_IF_ERROR(fixed_len_blocks_[0]->Pin());
  }
  if (has_var_len_slots_ && var_len_blocks_.size() > 0) {
    RETURN_IF_ERROR(var_len_blocks_[0]->Pin());
  }

  return Status::OK;
}

Status Sorter::Run::GetNextBatch(RowBatch** output_batch) {
  if (buffered_batch_.get() != NULL) {
    buffered_batch_->Reset();
    // Fill more rows into buffered_batch_.
    bool eos;
    if (has_var_len_slots_ && !is_pinned_) {
      RETURN_IF_ERROR(GetNext<true>(buffered_batch_.get(), &eos));
      if (buffered_batch_->num_rows() == 0 && !eos) {
        // No rows were filled because GetNext() had to read the next var-len block
        // Call GetNext() again.
        RETURN_IF_ERROR(GetNext<true>(buffered_batch_.get(), &eos));
      }
    } else {
      RETURN_IF_ERROR(GetNext<false>(buffered_batch_.get(), &eos));
    }
    DCHECK(eos || buffered_batch_->num_rows() > 0);
    if (eos) {
      // No rows are filled in GetNext() on eos, so this is safe.
      DCHECK_EQ(buffered_batch_->num_rows(), 0);
      buffered_batch_.reset();
      // The merge is complete. Delete the last blocks in the run.
      RETURN_IF_ERROR(fixed_len_blocks_.back()->Delete());
      if (has_var_len_slots_) {
        RETURN_IF_ERROR(var_len_blocks_.back()->Delete());
      }
    }
  }

  // *output_batch = NULL indicates eos.
  *output_batch = buffered_batch_.get();
  return Status::OK;
}

template <bool convert_offset_to_ptr>
Status Sorter::Run::GetNext(RowBatch* output_batch, bool* eos) {
  if (fixed_len_blocks_index_ == fixed_len_blocks_.size()) {
    *eos = true;
    DCHECK_EQ(num_tuples_returned_, num_tuples_);
    return Status::OK;
  } else {
    *eos = false;
  }

  BufferedBlockMgr::Block* fixed_len_block = fixed_len_blocks_[fixed_len_blocks_index_];

  if (!is_pinned_) {
    // Pin the next block and delete the previous if set in the previous call to
    // GetNext().
    if (pin_next_fixed_len_block_) {
      RETURN_IF_ERROR(fixed_len_blocks_[fixed_len_blocks_index_ - 1]->Delete());
      RETURN_IF_ERROR(fixed_len_block->Pin());
      pin_next_fixed_len_block_ = false;
    }
    if (pin_next_var_len_block_) {
      RETURN_IF_ERROR(var_len_blocks_[var_len_blocks_index_ - 1]->Delete());
      RETURN_IF_ERROR(var_len_blocks_[var_len_blocks_index_]->Pin());
      pin_next_var_len_block_ = false;
    }
  }

  // GetNext fills rows into the output batch until a block boundary is reached.
  while (!output_batch->AtCapacity() &&
      fixed_len_block_offset_ < fixed_len_block->valid_data_len()) {
    Tuple* input_tuple = reinterpret_cast<Tuple*>(
        fixed_len_block->buffer() + fixed_len_block_offset_);

    if (convert_offset_to_ptr) {
      // Convert the offsets in the var-len slots in input_tuple back to pointers.
      const vector<SlotDescriptor*>& var_slots = sort_tuple_desc_->string_slots();
      for (int i = 0; i < var_slots.size(); ++i) {
        SlotDescriptor* slot_desc = var_slots[i];
        if (input_tuple->IsNull(slot_desc->null_indicator_offset())) continue;

        DCHECK_EQ(slot_desc->type().type, TYPE_STRING);
        StringValue* value = reinterpret_cast<StringValue*>(
            input_tuple->GetSlot(slot_desc->tuple_offset()));
        int64_t data_offset = reinterpret_cast<int64_t>(value->ptr);

        // data_offset is an offset in bytes from the beginning of the first block
        // in var_len_blocks_. Convert it into an index into var_len_blocks_ and an
        // offset within that block.
        int block_index = data_offset / block_size_;
        int block_offset = data_offset % block_size_;

        if (block_index > var_len_blocks_index_) {
          // We've reached the block boundary for the current var-len block.
          // This tuple will be returned in the next call to GetNext().
          DCHECK_EQ(block_index, var_len_blocks_index_ + 1);
          DCHECK_EQ(block_offset, 0);
          DCHECK_EQ(i, 0);
          var_len_blocks_index_ = block_index;
          pin_next_var_len_block_ = true;
          break;
        } else {
          DCHECK_EQ(block_index, var_len_blocks_index_);
          // Calculate the address implied by the offset and assign it.
          value->ptr = reinterpret_cast<char*>(
              var_len_blocks_[var_len_blocks_index_]->buffer() + block_offset);
        } // if (block_index > var_len_blocks_index_)
      } // for (int i = 0; i < var_slots.size(); ++i)

      // The var-len data is in the next block, so end this call to GetNext().
      if (pin_next_var_len_block_) break;
    } // if (convert_offset_to_ptr)

    int output_row_idx = output_batch->AddRow();
    output_batch->GetRow(output_row_idx)->SetTuple(0, input_tuple);
    output_batch->CommitLastRow();
    fixed_len_block_offset_ += sort_tuple_size_;
    ++num_tuples_returned_;
  }

  if (fixed_len_block_offset_ >= fixed_len_block->valid_data_len()) {
    pin_next_fixed_len_block_ = true;
    ++fixed_len_blocks_index_;
    fixed_len_block_offset_ = 0;
  }

  return Status::OK;
}

void Sorter::Run::CollectNonNullVarSlots(Tuple* src,
    vector<StringValue*>* var_len_values, int* total_var_len) {
  var_len_values->clear();
  *total_var_len = 0;
  BOOST_FOREACH(const SlotDescriptor* var_slot, sort_tuple_desc_->string_slots()) {
    if (!src->IsNull(var_slot->null_indicator_offset())) {
      StringValue* string_val =
          reinterpret_cast<StringValue*>(src->GetSlot(var_slot->tuple_offset()));
      var_len_values->push_back(string_val);
      *total_var_len += string_val->len;
    }
  }
}

Status Sorter::Run::TryAddBlock(vector<BufferedBlockMgr::Block*>* block_sequence,
    bool* added) {
  BufferedBlockMgr::Block* last_block = block_sequence->back();
  if (is_sorted_ ||
      fixed_len_blocks_.size() + var_len_blocks_.size() < max_blocks_in_unsorted_run_) {
    // Sorted runs can be indefinitely extended because we unpin the previous block
    // before allocating a new one.
    if (is_sorted_) {
      RETURN_IF_ERROR(last_block->Unpin());
    } else {
      sorter_->sort_bytes_counter_->Update(last_block->valid_data_len());
    }

    BufferedBlockMgr::Block* new_block;
    RETURN_IF_ERROR(sorter_->block_mgr_->GetFreeBlock(&new_block));
    block_sequence->push_back(new_block);
    *added = true;
  } else {
    *added = false;
  }

  return Status::OK;
}

void Sorter::Run::CopyVarLenData(char* dest, const vector<StringValue*>& var_values) {
  BOOST_FOREACH(StringValue* var_val, var_values) {
    memcpy(dest, var_val->ptr, var_val->len);
    var_val->ptr = dest;
    dest += var_val->len;
  }
}

void Sorter::Run::CopyVarLenDataConvertOffset(char* dest, int64_t offset,
    const vector<StringValue*>& var_values) {
  BOOST_FOREACH(StringValue* var_val, var_values) {
    memcpy(dest, var_val->ptr, var_val->len);
    var_val->ptr = reinterpret_cast<char*>(offset);
    dest += var_val->len;
    offset += var_val->len;
  }
}

// Sorter::TupleSorter methods.
Sorter::TupleSorter::TupleSorter(const LessThanComparator& comp, int64_t block_size,
    int tuple_size)
  : less_than_comp_(comp),
    tuple_size_(tuple_size),
    block_capacity_(block_size / tuple_size) {
  temp_tuple_buffer_ = new uint8_t[tuple_size];
  temp_tuple_ = reinterpret_cast<Tuple*>(temp_tuple_buffer_);
  swap_buffer_ = new uint8_t[tuple_size];
}

Sorter::TupleSorter::~TupleSorter() {
  delete[] temp_tuple_buffer_;
  delete[] swap_buffer_;
}

void Sorter::TupleSorter::Sort(Run* run, int64_t num_tuples) {
  run_ = run;
  SortHelper(0, num_tuples);
  run->is_sorted_ = true;
}

// Sort the sequence of tuples from [first, last).
// Begin with a sorted sequence of size 1 [first, first+1).
// During each pass of the outermost loop, add the next tuple (at position 'i') to
// the sorted sequence by comparing it to each element of the sorted sequence
// (reverse order) to find its correct place in the sorted sequence, copying tuples
// along the way.
void Sorter::TupleSorter::InsertionSort(int64_t first, int64_t last) {
  for (int64_t i = first + 1; i < last; ++i) {
    Tuple* copy_to = GetTuple(i);
    // Copied to temp_tuple_ since the tuple at position 'i' may be overwritten by the
    // one at position 'i-1'
    memcpy(temp_tuple_, copy_to, tuple_size_);
    Tuple* compare_to = GetTuple(i-1);
    int64_t copy_index = i;
    while (less_than_comp_(temp_tuple_, compare_to)) {
      memcpy(copy_to, compare_to, tuple_size_);
      --copy_index;
      copy_to = compare_to;
      if (copy_index <= first) break;
      compare_to = GetTuple(copy_index - 1);
    }

    memcpy(copy_to, temp_tuple_, tuple_size_);
  }
}

int64_t Sorter::TupleSorter::Partition(int64_t first, int64_t last, Tuple* pivot) {
  // Copy pivot into temp_tuple since it points to a tuple within [first, last).
  memcpy(temp_tuple_, pivot, tuple_size_);
  while (true) {
    // Search for the first and last out-of-place elements, and swap them.
    Tuple* first_tuple = GetTuple(first);
    while (less_than_comp_(first_tuple, temp_tuple_)) {
      ++first;
      first_tuple = GetTuple(first);
    }

    --last;
    Tuple* last_tuple = GetTuple(last);
    while (less_than_comp_(temp_tuple_, last_tuple)) {
      --last;
      last_tuple = GetTuple(last);
    }

    if (first >= last) break;

    // Swap first and last tuples.
    Swap(first_tuple, last_tuple);

    ++first;
  }

  return first;
}

void Sorter::TupleSorter::SortHelper(int64_t first, int64_t last) {
  // Use insertion sort for smaller sequences.
  while (last - first > INSERTION_THRESHOLD) {
    Tuple* pivot = GetTuple(first + (last - first)/2);
    // Parititon() splits the tuples in [first, last) into two groups (<= pivot
    // and >= pivot) in-place. 'cut' is the index of the first tuple in the second group.
    int64_t cut = Partition(first, last, pivot);
    SortHelper(cut, last);
    last = cut;
  }

  InsertionSort(first, last);
}

inline void Sorter::TupleSorter::Swap(Tuple* left, Tuple* right) {
  memcpy(swap_buffer_, left, tuple_size_);
  memcpy(left, right, tuple_size_);
  memcpy(right, swap_buffer_, tuple_size_);
}

inline Tuple* Sorter::TupleSorter::GetTuple(int64_t index) {
  // TODO: This is called repeatedly from the in-memory sorter. Ensure that this
  // is not a bottleneck.
  int block_index = index / block_capacity_;
  int block_offset = (index % block_capacity_) * tuple_size_;
  uint8_t* tuple_ptr = run_->fixed_len_blocks_[block_index]->buffer() + block_offset;
  return reinterpret_cast<Tuple*>(tuple_ptr);
}

// Sorter methods
Sorter::Sorter(const TupleRowComparator& compare_less_than,
    const vector<Expr*>& slot_materialize_exprs, BufferedBlockMgr* block_mgr,
    RowDescriptor* output_row_desc, MemTracker* mem_tracker, RuntimeProfile* profile,
    int merge_batch_size)
  : block_mgr_(block_mgr),
    output_row_desc_(output_row_desc),
    mem_tracker_(mem_tracker),
    profile_(profile),
    sort_tuple_slot_exprs_(slot_materialize_exprs),
    compare_less_than_(compare_less_than),
    merge_batch_size_(merge_batch_size) {
  TupleDescriptor* sort_tuple_desc = output_row_desc->tuple_descriptors()[0];
  has_var_len_slots_ = sort_tuple_desc->string_slots().size() > 0;
  in_mem_tuple_sorter_.reset(new TupleSorter(compare_less_than, block_mgr->block_size(),
      sort_tuple_desc->byte_size()));

  num_runs_counter_ = ADD_COUNTER(profile_, "NumRuns", TCounterType::UNIT);
  num_merges_counter_ = ADD_COUNTER(profile_, "NumMerges", TCounterType::UNIT);
  in_mem_sort_timer_ = ADD_TIMER(profile_, "InMemorySortTime");
  sort_bytes_counter_ = ADD_COUNTER(profile_, "NumSortBytes", TCounterType::UNIT);

  unsorted_run_ = obj_pool_.Add(new Run(this, sort_tuple_desc, true));
  unsorted_run_->Init();
}

Sorter::~Sorter() {
}

Status Sorter::AddBatch(RowBatch* batch) {
  int num_processed = 0;
  int cur_batch_index = 0;
  while (cur_batch_index < batch->num_rows()) {
    if (has_var_len_slots_) {
      unsorted_run_->AddBatch<true>(batch, cur_batch_index, &num_processed);
    } else {
      unsorted_run_->AddBatch<false>(batch, cur_batch_index, &num_processed);
    }

    cur_batch_index += num_processed;
    if (cur_batch_index < batch->num_rows()) {
      // The current run is full. Sort it and begin the next one.
      RETURN_IF_ERROR(SortRun());
      RETURN_IF_ERROR(unsorted_run_->UnpinAllBlocks());
      unsorted_run_ = obj_pool_.Add(
          new Run(this, output_row_desc_->tuple_descriptors()[0], true));
      unsorted_run_->Init();
    }
  }

  return Status::OK;
}

Status Sorter::InputDone() {
  // Sort the tuples accumulated so far in the current run.
  RETURN_IF_ERROR(SortRun());

  if (sorted_runs_.size() == 1) {
    // The entire input fit in one run. Read sorted rows in GetNext() directly
    // from the sorted run.
    unsorted_run_->PrepareRead();
  } else {
    int blocks_in_final_run =
        unsorted_run_->fixed_len_blocks_.size() + unsorted_run_->var_len_blocks_.size();

    // At least one merge is necessary.
    int blocks_per_run = has_var_len_slots_ ? 2 : 1;
    int max_buffers_for_merge = sorted_runs_.size() * blocks_per_run;
    // Check if the final run needs to be unpinned.
    bool unpinned_final = false;
    if (block_mgr_->max_available_buffers() <
        (blocks_in_final_run + max_buffers_for_merge - blocks_per_run )) {
      // Number of available buffers is less than the size of the final run and
      // the buffers needed to read the remainder of the runs in memory.
      // Unpin the final run.
      RETURN_IF_ERROR(unsorted_run_->UnpinAllBlocks());
      unpinned_final = true;
    }

    // For an intermediate merge, intermediate_merge_batch contains deep-copied rows from
    // the input runs. If (unmerged_sorted_runs_.size() > max_runs_per_final_merge),
    // one or more intermediate merges are required.
    if (max_buffers_for_merge > block_mgr_->max_available_buffers()) {
      DCHECK(unpinned_final);
      RETURN_IF_ERROR(MergeIntermediateRuns());
    }

    // Create the final merger.
    CreateMerger(sorted_runs_.size());
  }

  return Status::OK;
}

Status Sorter::GetNext(RowBatch* output_batch, bool* eos) {
  if (sorted_runs_.size() == 1) {
    DCHECK(sorted_runs_.back()->is_pinned_);
    // In this case, only TupleRows are copied into output_batch. Sorted tuples are left
    // in the pinned blocks in the single sorted run.
    RETURN_IF_ERROR(sorted_runs_.back()->GetNext<false>(output_batch, eos));
  } else {
    // In this case, rows are deep copied into output_batch.
    RETURN_IF_ERROR(merger_->GetNext(output_batch, eos));
  }

  return Status::OK;
}

Status Sorter::SortRun() {
  BufferedBlockMgr::Block* last_block = unsorted_run_->fixed_len_blocks_.back();
  if (last_block->valid_data_len() > 0) {
    sort_bytes_counter_->Update(last_block->valid_data_len());
  } else {
    RETURN_IF_ERROR(last_block->Delete());
    unsorted_run_->fixed_len_blocks_.pop_back();
  }
  if (has_var_len_slots_) {
    last_block = unsorted_run_->var_len_blocks_.back();
    if (last_block->valid_data_len() > 0) {
      sort_bytes_counter_->Update(last_block->valid_data_len());
    } else {
      RETURN_IF_ERROR(last_block->Delete());
      unsorted_run_->var_len_blocks_.pop_back();
    }
  }
  {
    SCOPED_TIMER(in_mem_sort_timer_);
    in_mem_tuple_sorter_->Sort(unsorted_run_, unsorted_run_->num_tuples_);
  }
  sorted_runs_.push_back(unsorted_run_);
  return Status::OK;
}

uint64_t Sorter::EstimateMergeMem(uint64_t available_blocks,
    RowDescriptor* row_desc, int merge_batch_size) {
  bool has_var_len_slots = row_desc->tuple_descriptors()[0]->string_slots().size() > 0;
  int blocks_per_run = has_var_len_slots ? 2 : 1;
  int max_input_runs_per_merge = (available_blocks / blocks_per_run) - 1;
  // During a merge, the batches corresponding to the input runs contain only TupleRows.
  // (The data itself is in pinned blocks held by the run)
  uint64_t input_batch_mem =
      merge_batch_size * sizeof(Tuple*) * max_input_runs_per_merge;
  // Since rows are deep copied into the output batch for the merger, use a pessimistic
  // estimate of the memory required.
  uint64_t output_batch_mem = RowBatch::AT_CAPACITY_MEM_USAGE;

  return input_batch_mem + output_batch_mem;
}

uint32_t Sorter::MinBuffersRequired(RowDescriptor* row_desc) {
  bool has_var_len_slots = row_desc->tuple_descriptors()[0]->string_slots().size() > 0;
  int blocks_per_run = has_var_len_slots ? 2 : 1;
  // An intermediate merge requires at least 2 input runs and 1 output runs to be
  // processed at a time.
  return blocks_per_run * 3;
}

Status Sorter::MergeIntermediateRuns() {
  int blocks_per_run = has_var_len_slots_ ? 2 : 1;
  int max_runs_per_final_merge = block_mgr_->max_available_buffers() / blocks_per_run;

  // During an intermediate merge, blocks from the output sorted run will have to be
  // pinned.
  int max_runs_per_intermediate_merge = max_runs_per_final_merge - 1;
  DCHECK_GT(max_runs_per_intermediate_merge, 1);
  // For an intermediate merge, intermediate_merge_batch contains deep-copied rows from
  // the input runs. If (sorted_runs_.size() > max_runs_per_final_merge),
  // one or more intermediate merges are required.
  scoped_ptr<RowBatch> intermediate_merge_batch;
  while (sorted_runs_.size() > max_runs_per_final_merge) {
    // An intermediate merge adds one merge to unmerged_sorted_runs_.
    // Merging 'runs - (max_runs_final_ - 1)' number of runs is sifficient to guarantee
    // that the final merge can be performed.
    int num_runs_to_merge = min<int>(max_runs_per_intermediate_merge,
        sorted_runs_.size() - max_runs_per_intermediate_merge);
    CreateMerger(num_runs_to_merge);
    RowBatch intermediate_merge_batch(*output_row_desc_, merge_batch_size_,
        mem_tracker_);
    // merged_run is the new sorted run that is produced by the intermediate merge.
    Run* merged_run = obj_pool_.Add(
        new Run(this, output_row_desc_->tuple_descriptors()[0], false));
    RETURN_IF_ERROR(merged_run->Init());
    merged_run->is_sorted_ = true;
    bool eos = false;
    while (!eos) {
      // Copy rows into the new run until done.
      int num_copied;
      RETURN_IF_ERROR(merger_->GetNext(&intermediate_merge_batch, &eos));
      Status ret_status;
      if (has_var_len_slots_) {
        ret_status = merged_run->AddBatch<true>(&intermediate_merge_batch,
            0, &num_copied);
      } else {
        ret_status = merged_run->AddBatch<false>(&intermediate_merge_batch,
            0, &num_copied);
      }
      if (!ret_status.ok()) return ret_status;

      DCHECK_EQ(num_copied, intermediate_merge_batch.num_rows());
      intermediate_merge_batch.Reset();
    }

    BufferedBlockMgr::Block* last_block = merged_run->fixed_len_blocks_.back();
    if (last_block->valid_data_len() > 0) {
      RETURN_IF_ERROR(last_block->Unpin());
    } else {
      RETURN_IF_ERROR(last_block->Delete());
      merged_run->fixed_len_blocks_.pop_back();
    }
    if (has_var_len_slots_) {
      last_block = merged_run->var_len_blocks_.back();
      if (last_block->valid_data_len() > 0) {
        RETURN_IF_ERROR(last_block->Unpin());
      } else {
        RETURN_IF_ERROR(last_block->Delete());
        merged_run->var_len_blocks_.pop_back();
      }
    }
    merged_run->is_pinned_ = false;
    sorted_runs_.push_back(merged_run);
  }

  return Status::OK;
}

Status Sorter::CreateMerger(int num_runs) {
  DCHECK_GT(num_runs, 1);
  merger_.reset(
      new SortedRunMerger(compare_less_than_, output_row_desc_, profile_, true));
  vector<function<Status (RowBatch**)> > merge_runs;
  merge_runs.reserve(num_runs);
  for (int i = 0; i < num_runs; ++i) {
    Run* run = sorted_runs_.front();
    run->PrepareRead();
    // Run::GetNextBatch() is used by the merger to retrieve a batch of rows to merge
    // from this run.
    merge_runs.push_back(bind<Status>(mem_fn(&Run::GetNextBatch), run, _1));
    sorted_runs_.pop_front();
  }
  RETURN_IF_ERROR(merger_->Prepare(merge_runs));

  num_merges_counter_->Update(1);
  return Status::OK;
}

} // namespace impala
