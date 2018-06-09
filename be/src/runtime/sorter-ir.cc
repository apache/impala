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

#include <boost/random/uniform_int.hpp>

#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

namespace  impala {

void Sorter::TupleIterator::NextPage(Sorter::Run* run) {
  // When moving after the last tuple, stay at the last page.
  if (index_ >= run->num_tuples()) return;
  ++page_index_;
  DCHECK_LT(page_index_, run->fixed_len_pages_.size());
  buffer_start_index_ = page_index_ * run->page_capacity_;
  DCHECK_EQ(index_, buffer_start_index_);
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  tuple_ = run->fixed_len_pages_[page_index_].data();
}

void Sorter::TupleIterator::PrevPage(Sorter::Run* run) {
  // When moving before the first tuple, stay at the first page.
  if (index_ < 0) return;
  --page_index_;
  DCHECK_GE(page_index_, 0);
  buffer_start_index_ = page_index_ * run->page_capacity_;
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  DCHECK_EQ(index_, buffer_end_index_ - 1);
  int last_tuple_page_offset = run->sort_tuple_size_ * (run->page_capacity_ - 1);
  tuple_ = run->fixed_len_pages_[page_index_].data() + last_tuple_page_offset;
}

void Sorter::TupleIterator::Next(Sorter::Run* run, int tuple_size) {
  DCHECK_LT(index_, run->num_tuples()) << "Can only advance one past end of run";
  tuple_ += tuple_size;
  ++index_;
  if (UNLIKELY(index_ >= buffer_end_index_)) NextPage(run);
}

void Sorter::TupleIterator::Prev(Sorter::Run* run, int tuple_size) {
  DCHECK_GE(index_, 0) << "Can only advance one before start of run";
  tuple_ -= tuple_size;
  --index_;
  if (UNLIKELY(index_ < buffer_start_index_)) PrevPage(run);
}

bool Sorter::TupleSorter::Less(const TupleRow* lhs, const TupleRow* rhs) {
  --num_comparisons_till_free_;
  DCHECK_GE(num_comparisons_till_free_, 0);
  if (UNLIKELY(num_comparisons_till_free_ == 0)) {
    parent_->expr_results_pool_.Clear();
    num_comparisons_till_free_ = state_->batch_size();
  }
  return comparator_.Less(lhs, rhs);
}

Status Sorter::TupleSorter::Partition(TupleIterator begin,
    TupleIterator end, const Tuple* pivot, TupleIterator* cut) {
  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  int tuple_size = tuple_size_;
  Tuple* temp_tuple = reinterpret_cast<Tuple*>(temp_tuple_buffer_);
  Tuple* swap_tuple = reinterpret_cast<Tuple*>(swap_buffer_);

  // Copy pivot into temp_tuple since it points to a tuple within [begin, end).
  DCHECK(temp_tuple != nullptr);
  DCHECK(pivot != nullptr);
  memcpy(temp_tuple, pivot, tuple_size);

  TupleIterator left = begin;
  TupleIterator right = end;
  right.Prev(run, tuple_size); // Set 'right' to the last tuple in range.
  while (true) {
    // Search for the first and last out-of-place elements, and swap them.
    while (Less(left.row(), reinterpret_cast<TupleRow*>(&temp_tuple))) {
      left.Next(run, tuple_size);
    }
    while (Less(reinterpret_cast<TupleRow*>(&temp_tuple), right.row())) {
      right.Prev(run, tuple_size);
    }

    if (left.index() >= right.index()) break;
    // Swap first and last tuples.
    Swap(left.tuple(), right.tuple(), swap_tuple, tuple_size);

    left.Next(run, tuple_size);
    right.Prev(run, tuple_size);
    RETURN_IF_CANCELLED(state_);
    RETURN_IF_ERROR(state_->GetQueryStatus());
  }
  *cut = left;
  return Status::OK();
}

// Sort the sequence of tuples from [begin, last).
// Begin with a sorted sequence of size 1 [begin, begin+1).
// During each pass of the outermost loop, add the next tuple (at position 'i') to
// the sorted sequence by comparing it to each element of the sorted sequence
// (reverse order) to find its correct place in the sorted sequence, copying tuples
// along the way.
Status Sorter::TupleSorter::InsertionSort(const TupleIterator& begin,
    const TupleIterator& end) {
  DCHECK_LT(begin.index(), end.index());

  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  int tuple_size = tuple_size_;
  uint8_t* temp_tuple_buffer = temp_tuple_buffer_;

  TupleIterator insert_iter = begin;
  insert_iter.Next(run, tuple_size);
  for (; insert_iter.index() < end.index(); insert_iter.Next(run, tuple_size)) {
    // insert_iter points to the tuple after the currently sorted sequence that must
    // be inserted into the sorted sequence. Copy to temp_tuple_buffer_ since it may be
    // overwritten by the one at position 'insert_iter - 1'
    memcpy(temp_tuple_buffer, insert_iter.tuple(), tuple_size);

    // 'iter' points to the tuple that temp_tuple_buffer will be compared to.
    // 'copy_to' is the where iter should be copied to if it is >= temp_tuple_buffer.
    // copy_to always to the next row after 'iter'
    TupleIterator iter = insert_iter;
    iter.Prev(run, tuple_size);
    Tuple* copy_to = insert_iter.tuple();
    while (Less(reinterpret_cast<TupleRow*>(&temp_tuple_buffer), iter.row())) {
      memcpy(copy_to, iter.tuple(), tuple_size);
      copy_to = iter.tuple();
      // Break if 'iter' has reached the first row, meaning that the temp row
      // will be inserted in position 'begin'
      if (iter.index() <= begin.index()) break;
      iter.Prev(run, tuple_size);
    }

    memcpy(copy_to, temp_tuple_buffer, tuple_size);
  }
  RETURN_IF_CANCELLED(state_);
  RETURN_IF_ERROR(state_->GetQueryStatus());
  return Status::OK();
}


Status Sorter::TupleSorter::SortHelper(TupleIterator begin, TupleIterator end) {
  // Use insertion sort for smaller sequences.
  while (end.index() - begin.index() > INSERTION_THRESHOLD) {
    // Select a pivot and call Partition() to split the tuples in [begin, end) into two
    // groups (<= pivot and >= pivot) in-place. 'cut' is the index of the first tuple in
    // the second group.
    Tuple* pivot = SelectPivot(begin, end);
    TupleIterator cut;
    RETURN_IF_ERROR(Partition(begin, end, pivot, &cut));

    // Recurse on the smaller partition. This limits stack size to log(n) stack frames.
    if (cut.index() - begin.index() < end.index() - cut.index()) {
      // Left partition is smaller.
      RETURN_IF_ERROR(SortHelper(begin, cut));
      begin = cut;
    } else {
      // Right partition is equal or smaller.
      RETURN_IF_ERROR(SortHelper(cut, end));
      end = cut;
    }
  }

  if (begin.index() < end.index()) RETURN_IF_ERROR(InsertionSort(begin, end));
  return Status::OK();
}

Tuple* Sorter::TupleSorter::SelectPivot(TupleIterator begin, TupleIterator end) {
  // Select the median of three random tuples. The random selection avoids pathological
  // behaviour associated with techniques that pick a fixed element (e.g. picking
  // first/last/middle element) and taking the median tends to help us select better
  // pivots that more evenly split the input range. This method makes selection of
  // bad pivots very infrequent.
  //
  // To illustrate, if we define a bad pivot as one in the lower or upper 10% of values,
  // then the median of three is a bad pivot only if all three randomly-selected values
  // are in the lower or upper 10%. The probability of that is 0.2 * 0.2 * 0.2 = 0.008:
  // less than 1%. Since selection is random each time, the chance of repeatedly picking
  // bad pivots decreases exponentialy and becomes negligibly small after a few
  // iterations.
  Tuple* tuples[3];
  for (auto& tuple : tuples) {
    int64_t index = boost::uniform_int<int64_t>(begin.index(), end.index() - 1)(rng_);
    TupleIterator iter(run_, index);
    DCHECK(iter.tuple() != nullptr);
    tuple = iter.tuple();
  }

  return MedianOfThree(tuples[0], tuples[1], tuples[2]);
}

Tuple* Sorter::TupleSorter::MedianOfThree(Tuple* t1, Tuple* t2, Tuple* t3) {
  TupleRow* tr1 = reinterpret_cast<TupleRow*>(&t1);
  TupleRow* tr2 = reinterpret_cast<TupleRow*>(&t2);
  TupleRow* tr3 = reinterpret_cast<TupleRow*>(&t3);

  bool t1_lt_t2 = Less(tr1, tr2);
  bool t2_lt_t3 = Less(tr2, tr3);
  bool t1_lt_t3 = Less(tr1, tr3);

  if (t1_lt_t2) {
    // t1 < t2
    if (t2_lt_t3) {
      // t1 < t2 < t3
      return t2;
    } else if (t1_lt_t3) {
      // t1 < t3 <= t2
      return t3;
    } else {
      // t3 <= t1 < t2
      return t1;
    }
  } else {
    // t2 <= t1
    if (t1_lt_t3) {
      // t2 <= t1 < t3
      return t1;
    } else if (t2_lt_t3) {
      // t2 < t3 <= t1
      return t3;
    } else {
      // t3 <= t2 <= t1
      return t2;
    }
  }
}

void Sorter::TupleSorter::Swap(Tuple* left, Tuple* right, Tuple* swap_tuple,
    int tuple_size) {
  memcpy(swap_tuple, left, tuple_size);
  memcpy(left, right, tuple_size);
  memcpy(right, swap_tuple, tuple_size);
}

}
