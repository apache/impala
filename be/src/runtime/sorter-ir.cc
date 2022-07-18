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

void IR_ALWAYS_INLINE Sorter::TupleIterator::NextPage(Sorter::Run* run) {
  // When moving after the last tuple, stay at the last page.
  if (index_ >= run->num_tuples()) return;
  ++page_index_;
  DCHECK_LT(page_index_, run->fixed_len_pages_.size());
  buffer_start_index_ = page_index_ * run->page_capacity_;
  DCHECK_EQ(index_, buffer_start_index_);
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  tuple_ = run->fixed_len_pages_[page_index_].data();
}

void IR_ALWAYS_INLINE Sorter::TupleIterator::PrevPage(Sorter::Run* run) {
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

// IMPALA-3816: Function is not inlined into Partition() without IR_ALWAYS_INLINE hint.
void IR_ALWAYS_INLINE Sorter::TupleIterator::Next(Sorter::Run* run, int tuple_size) {
  DCHECK_LT(index_, run->num_tuples()) << "Can only advance one past end of run";
  tuple_ += tuple_size;
  ++index_;
  if (UNLIKELY(index_ >= buffer_end_index_)) NextPage(run);
}

// IMPALA-3816: Function is not inlined into Partition() without IR_ALWAYS_INLINE hint.
void IR_ALWAYS_INLINE Sorter::TupleIterator::Prev(Sorter::Run* run, int tuple_size) {
  DCHECK_GE(index_, 0) << "Can only advance one before start of run";
  tuple_ -= tuple_size;
  --index_;
  if (UNLIKELY(index_ < buffer_start_index_)) PrevPage(run);
}

void IR_ALWAYS_INLINE Sorter::TupleSorter::FreeExprResultPoolIfNeeded() {
  --num_comparisons_till_free_;
  DCHECK_GE(num_comparisons_till_free_, 0);
  if (UNLIKELY(num_comparisons_till_free_ == 0)) {
    parent_->expr_results_pool_.Clear();
    num_comparisons_till_free_ = state_->batch_size();
  }
}

// IMPALA-3816: Function is not inlined into Partition() without IR_ALWAYS_INLINE hint.
bool IR_ALWAYS_INLINE Sorter::TupleSorter::Less(
    const TupleRow* lhs, const TupleRow* rhs) {
  FreeExprResultPoolIfNeeded();
  return comparator_.Less(lhs, rhs);
}

int IR_ALWAYS_INLINE Sorter::TupleSorter::Compare(
    const TupleRow* lhs, const TupleRow* rhs) {
  FreeExprResultPoolIfNeeded();
  return comparator_.Compare(lhs, rhs);
}

Status IR_ALWAYS_INLINE Sorter::TupleSorter::Partition2way(TupleIterator begin,
    TupleIterator end, const Tuple* pivot, TupleIterator* cut) {
  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  const int tuple_size = get_tuple_size();
  Tuple* pivot_tuple = reinterpret_cast<Tuple*>(temp_tuple_buffer_);
  TupleRow* pivot_tuple_row = reinterpret_cast<TupleRow*>(&pivot_tuple);
  Tuple* swap_tuple = reinterpret_cast<Tuple*>(swap_buffer_);

  // Copy pivot into temp_tuple since it points to a tuple within [begin, end).
  DCHECK(pivot_tuple != nullptr);
  DCHECK(pivot != nullptr);
  memcpy(pivot_tuple, pivot, tuple_size);

  TupleIterator left = begin;
  TupleIterator right = end;
  right.Prev(run, tuple_size); // Set 'right' to the last tuple in range.
  while (true) {
    // Search for the first and last out-of-place elements, and swap them.
    while (Less(left.row(), pivot_tuple_row)) {
      left.Next(run, tuple_size);
    }
    while (Less(pivot_tuple_row, right.row())) {
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

// Partitions the tuples into 3 groups: < pivot, = pivot and > pivot.
// Similar to 2-way quicksort, but elements that are = pivot are put to the
// sides while partitioning < pivot and > pivot elements.
// When all non-equal elements are partitioned, the = pivot elements are
// moved to their place in the middle between the two groups of < pivot and
// > pivot elements.
Status IR_ALWAYS_INLINE Sorter::TupleSorter::Partition3way(TupleIterator begin,
    TupleIterator end, const Tuple* pivot, TupleIterator* cut_left,
    TupleIterator* cut_right) {
  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  const int tuple_size = get_tuple_size();
  Tuple* pivot_tuple = reinterpret_cast<Tuple*>(temp_tuple_buffer_);
  TupleRow* pivot_tuple_row = reinterpret_cast<TupleRow*>(&pivot_tuple);
  Tuple* swap_tuple = reinterpret_cast<Tuple*>(swap_buffer_);
  // Copy pivot into temp_tuple since it points to a tuple within [begin, end).
  DCHECK(pivot_tuple != nullptr);
  DCHECK(pivot != nullptr);
  memcpy(pivot_tuple, pivot, tuple_size);
  TupleIterator left = begin;
  TupleIterator right = end;
  right.Prev(run, tuple_size); // Set 'right' to the last tuple in range.
  // First tuple that is < pivot after last tuple that is = pivot on the left
  TupleIterator equals_left = begin;
  // First tuple that is = pivot after last tuple that is > pivot on the right
  TupleIterator equals_right = end;
  while (true) {
    // Search for elements that are = pivot, and move them to the sides.
    // Stops at the first and last out-of-place elements, so they
    // will be swapped.
    while (left.index() <= right.index()) {
      int cmp = Compare(left.row(), pivot_tuple_row);
      if (cmp > 0) {
        break;
      } else if (cmp == 0) {
        Swap(equals_left.tuple(), left.tuple(), swap_tuple, tuple_size);
        equals_left.Next(run, tuple_size);
      }
      left.Next(run, tuple_size);
    }
    while (left.index() <= right.index()) {
      int cmp = Compare(pivot_tuple_row, right.row());
      if (cmp > 0) {
        break;
      } else if (cmp == 0) {
        equals_right.Prev(run, tuple_size);
        Swap(equals_right.tuple(), right.tuple(), swap_tuple, tuple_size);
      }
      right.Prev(run, tuple_size);
    }
    if (left.index() >= right.index()) break;
    // Swap first and last out-of-place tuples.
    Swap(left.tuple(), right.tuple(), swap_tuple, tuple_size);
    left.Next(run, tuple_size);
    right.Prev(run, tuple_size);
    RETURN_IF_CANCELLED(state_);
    RETURN_IF_ERROR(state_->GetQueryStatus());
  }
  if(left.index() == right.index()) {
    DCHECK(Compare(pivot_tuple_row, right.row()) == 0);
  }
  // Move equal columns from the sides to the pivot
  while(equals_left.index() > begin.index()) {
    equals_left.Prev(run, tuple_size);
    left.Prev(run, tuple_size);
    Swap(equals_left.tuple(), left.tuple(), swap_tuple, tuple_size);
  }
  while (equals_right.index() < end.index()) {
    right.Next(run, tuple_size);
    Swap(equals_right.tuple(), right.tuple(), swap_tuple, tuple_size);
    equals_right.Next(run, tuple_size);
  }
  *cut_left = left;
  right.Next(run, tuple_size);
  *cut_right = right;
  return Status::OK();
}

// Sort the sequence of tuples from [begin, last).
// Begin with a sorted sequence of size 1 [begin, begin+1).
// During each pass of the outermost loop, add the next tuple (at position 'i') to
// the sorted sequence by comparing it to each element of the sorted sequence
// (reverse order) to find its correct place in the sorted sequence, copying tuples
// along the way.
Status IR_ALWAYS_INLINE Sorter::TupleSorter::InsertionSort(const TupleIterator& begin,
    const TupleIterator& end) {
  DCHECK_LT(begin.index(), end.index());

  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  const int tuple_size = get_tuple_size();
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
    // Select a pivot. If duplicates are found during pivot selection,
    // has_equals == true,  call Partition3way() to split the tuples in
    // [begin, end) into three groups (< pivot, = pivot and > pivot) in-place.
    // 'cut_left' is the index of the first tuple in the second group,
    // 'cut_right' is the first tuple in the third group.
    // If there are lots of equal values, 3-way quicksort is much faster
    // than 2-way, otherwise it is slightly slower.
    bool has_equals = false;
    Tuple* pivot = SelectPivot(begin, end, &has_equals);
    TupleIterator cut_left;
    TupleIterator cut_right;
    if (has_equals) {
      RETURN_IF_ERROR(Partition3way(begin, end, pivot, &cut_left, &cut_right));
    } else {
      // If no duplicates are found, call Partition2way() to split the tuples
      // in [begin, end) into two groups (<= pivot and >= pivot) in-place.
      // 'cut_left' is the index of the first tuple in the second group.
      RETURN_IF_ERROR(Partition2way(begin, end, pivot, &cut_left));
      cut_right = cut_left;
    }
    // Recurse on the smaller partition. This limits stack size to log(n) stack frames.
    if (cut_left.index() - begin.index() < end.index() - cut_right.index()) {
      // Left partition is smaller.
      RETURN_IF_ERROR(SortHelper(begin, cut_left));
      begin = cut_right;
    } else {
      // Right partition is equal or smaller.
      RETURN_IF_ERROR(SortHelper(cut_right, end));
      end = cut_left;
    }
  }

  if (begin.index() < end.index()) RETURN_IF_ERROR(InsertionSort(begin, end));
  return Status::OK();
}

Tuple* IR_ALWAYS_INLINE Sorter::TupleSorter::SelectPivot(
    TupleIterator begin, TupleIterator end, bool* has_equals) {
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

  return MedianOfThree(tuples[0], tuples[1], tuples[2], has_equals);
}

Tuple* IR_ALWAYS_INLINE Sorter::TupleSorter::MedianOfThree(
    Tuple* t1, Tuple* t2, Tuple* t3, bool* has_equals) {
  TupleRow* tr1 = reinterpret_cast<TupleRow*>(&t1);
  TupleRow* tr2 = reinterpret_cast<TupleRow*>(&t2);
  TupleRow* tr3 = reinterpret_cast<TupleRow*>(&t3);

  int t1_cmp_t2 = Compare(tr1, tr2);
  int t2_cmp_t3 = Compare(tr2, tr3);
  int t1_cmp_t3 = Compare(tr1, tr3);
  Tuple* pivot;
  if (t1_cmp_t2 < 0) {
    // t1 < t2
    if (t2_cmp_t3 < 0) {
      // t1 < t2 < t3
      pivot = t2;
    } else if (t1_cmp_t3 < 1) {
      // t1 < t3 <= t2
      pivot = t3;
    } else {
      // t3 <= t1 < t2
      pivot = t1;
    }
  } else {
    // t2 <= t1
    if (t1_cmp_t3 < 0) {
      // t2 <= t1 < t3
      pivot = t1;
    } else if (t2_cmp_t3 < 1) {
      // t2 < t3 <= t1
      pivot = t3;
    } else {
      // t3 <= t2 <= t1
      pivot = t2;
    }
  }
  if (t1_cmp_t2*t1_cmp_t3*t2_cmp_t3 == 0){
    *has_equals = true;
  } else {
    *has_equals = false;
  }
  return pivot;
}

// IMPALA-3816: Function is not inlined into Partition() without IR_ALWAYS_INLINE hint.
void IR_ALWAYS_INLINE Sorter::TupleSorter::Swap(Tuple* RESTRICT left,
    Tuple* RESTRICT right, Tuple* RESTRICT swap_tuple, int tuple_size) {
  memcpy(swap_tuple, left, tuple_size);
  memcpy(left, right, tuple_size);
  memcpy(right, swap_tuple, tuple_size);
}
}
