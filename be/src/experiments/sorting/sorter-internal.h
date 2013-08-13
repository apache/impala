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


#ifndef IMPALA_SORTING_SORTER_INTERNAL_H_
#define IMPALA_SORTING_SORTER_INTERNAL_H_

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "sorter.h"
#include "stream-reader.h"
#include "util/bit-util.h"

namespace impala {

class Expr;
class RowBatch;
class RowDescriptor;
class Tuple;
class TupleDescripor;
class TupleRow;

// A SortTuple just contains a normalized key and a pointer back to the original tuple.
class Sorter::SortTuple {
 public:
  // Returns a pointer to the space reserved for the sort key.
  uint8_t* SortKey() {
    return reinterpret_cast<uint8_t*>(this) + sizeof(SortTuple);
  }

  uint32_t tuple_idx_;
};

// A comparator which compares SortTuples by following the tuple index to the actual
// Tuple. Note that this comparator is only called after the normalized key comparison
// has already failed.
class Sorter::SortTupleComparator {
 public:
  SortTupleComparator(BlockedVector<Tuple>* tuples,
      const std::vector<Expr*>& key_exprs_lhs, const std::vector<Expr*>& key_exprs_rhs,
      std::vector<bool> is_asc, bool nulls_first)
    : tuples_(tuples),
      tuple_comparator_(key_exprs_lhs, key_exprs_rhs, is_asc, nulls_first) {
  }

  bool operator() (SortTuple* lhs, SortTuple* rhs) const {
    const BlockedVector<Tuple>& tuples = *tuples_;
    return tuple_comparator_(tuples[lhs->tuple_idx_], tuples[rhs->tuple_idx_]);
  }

 private:
  BlockedVector<Tuple>* tuples_;
  TupleRowComparator tuple_comparator_;
};

// A Run is a set of tuple Blocks and auxiliary data Blocks, both of which are sorted
// in the same order. The Run owns all the Blocks, and will free them upon its deletion.
// 'aux_backwards_' is true if the aux blocks are backwards (i.e., last aux block is
// associated with first tuple block).
struct Sorter::Run {
  std::vector<Block*> tuple_blocks_;
  std::vector<Block*> aux_blocks_;
  size_t tuple_size_;
  bool aux_backwards_;

  // Constructs a Run out of the two tuple pools, and takes ownership of their blocks.
  Run(const std::vector<Block*>& tuple_blocks,
      const std::vector<Block*>& aux_blocks,
      size_t tuple_size, bool aux_backwards = false)
      : tuple_blocks_(tuple_blocks), aux_blocks_(aux_blocks), tuple_size_(tuple_size),
        aux_backwards_(aux_backwards) {
  }
};

// Used to build sorted runs using a set of incoming TupleRows.
// The value of sorter_->extract_keys_ has a major impact on this builder's behavior:
//   - If extract_keys is false, our "tuple" will be the initial tuple, with
//     the normalized sort key appended to it, so the entire size will be
//     output_tuple_desc.byte_size() + sort_key_size.
//   - If extract_keys is true, our tuple will be the same as the initial tuple, and we
//     will maintain a separate BlockedVector for "sort tuples", which contain a
//     32 bit index into the tuples_ vector followed by the normalized sort key.
//
// When building a sorted run, we first sort the tuples, and then sort the auxiliary
// data in the same order. In fact, we sort the auxiliary data starting at the end,
// which allows us to put the auxiliary data Blocks onto the disk queue as soon as
// we finish them, and still maintain the first few Blocks of the Run in memory if
// possible.
class Sorter::RunBuilder {
 public:
  RunBuilder(Sorter* sorter, BufferPool* buffer_pool);

  // Adds a Row to this Run.
  void AddRow(TupleRow* row);

  // Reserves 1 buffer in the aux pool, which is the minimum we need for BuildSortedRun().
  void ReserveAuxBuffer() {
    aux_pool_->ReserveBuffers(1);
  }

  // Sorts Tuples (and auxiliary data), and places the result in a set of blocks which
  // it passes to the Run.
  Sorter::Run* BuildSortedRun();

  // The number of buffers that this Run would need to become sorted, accounting
  // for the already-owned Tuple Blocks and the need-to-be-gotten auxiliary Blocks.
  int64_t buffers_needed() const {
    return tuple_pool_->num_blocks()
        + (sorter_->extract_keys_ ? sort_tuple_pool_->num_blocks() : 0)
        + 1 /* aux buffer */;
  }

  bool empty() const { return tuples_.size() == 0; }

 private:
  // Performs a sort over tuples_ or sort_tuples_ using the given sort exprs when
  // the normalized key is insufficent.
  void DoSort(std::vector<Expr*>* incomplete_sort_exprs_lhs,
              std::vector<Expr*>* incomplete_sort_exprs_rhs,
              std::vector<bool>* is_asc);

  // Sort all Tuples.
  void Sort();

  Sorter* sorter_;

  // The pool in which we allocate new tuples.
  boost::scoped_ptr<BlockMemPool> tuple_pool_;

  // Set of actual tuples, based on the tuple_pool_.
  BlockedVector<Tuple> tuples_;

  // Pool for sort tuples, when extract_keys is true.
  boost::scoped_ptr<BlockMemPool> sort_tuple_pool_;

  // When extract_keys is true, this contains a list of our sort tuples, backed by
  // sort_tuple_pool_.
  BlockedVector<SortTuple> sort_tuples_;

  // The pool in which we place auxiliary data (after sort).
  boost::scoped_ptr<BlockMemPool> aux_pool_;

  // The pool in which we place auxiliary data as it's coming in (before sort).
  boost::scoped_ptr<MemPool> unsorted_aux_pool_;

  // Index of the first sort_expr for which at least one tuple went over the budget
  // trying to write its normalized value.
  // TODO: Make this per-Tuple, to avoid data variance giving us extra pain.
  int first_sort_expr_over_budget_;
};

// Implementation of a RowBatchSupplier based on a Run.
// This supplier must load data from disk (if it was written out), and reconstruct
// RowBatches from a set of tuples and a set of auxiliary data.
// The RunBatchSupplier owns the given Run and will deallocate it when deleted.
class Sorter::RunBatchSupplier : public RowBatchSupplier {
 public:
  RunBatchSupplier(Sorter* sorter, BufferPool* buffer_pool, Run* run);

  ~RunBatchSupplier();

  // Prepares the supplier for merge by beginning to load necessary buffer pages.
  void Prepare();

  // Returns a StreamReader which reads the set of buffers that have been written out
  // in order.
  // If backwards is true, we'll read the set of blocks backwards.
  StreamReader* MakeStreamReader(const std::vector<Block*>& blocks,
                                 bool backwards);

  // Ensures the buffer is either in memory or brought in memory via the given reader.
  void AcquireBuffer(Block* block, StreamReader* reader);

  // Releases the given buffer. This does not persist it.
  void ReleaseBuffer(Block* block);

  // Constructs a RowBatch by merging consecutive tuples and associated string data.
  Status GetNext(RowBatch* row_batch, bool* eos);

 private:
  Sorter* sorter_;
  BufferPool* buffer_pool_;

  // Run from which we supply rows.
  Run* run_;

  size_t tuple_size_;

  // StreamReader which reads Blocks of Tuples.
  boost::scoped_ptr<StreamReader> tuple_reader_;

  // StreamReader which reads Blocks of auxiliary data.
  boost::scoped_ptr<StreamReader> aux_reader_;

  // Index of the current tuple block within the run.
  Block* cur_tuple_block_;

  // Index of the current tuple within the current tuple block.
  int cur_tuple_index_;

  // Index of the current auxiliary block within the run.
  Block* cur_aux_block_;

  // Iterator of blocks in the Run, starting at the beginning.
  std::vector<Block*>::iterator tuple_blocks_iterator_;
};

}

#endif
