// Copyright 2012 Cloudera Inc.
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


#ifndef IMPALA_UTIL_VECTOR_SORTER_H_
#define IMPALA_UTIL_VECTOR_SORTER_H_

#include <boost/function.hpp>

namespace impala {

// Proivdes a sorting algorithm for use on data structures whose sizes are only
// determined at runtime (e.g., Tuples). The sorting algorithm is currently
// a stripped down version of the STL sort:
// This sort simply utilizes Quicksort to sort the majority of elements,
// followed by a pass of insertion sort for runs of INSERTION_THRESHOLD elements.
// The simplicity of an insertion sort makes it better than the recursive quicksort
// for small run sizes.
//
// The insertion sort has "guarded" and "unguarded" versions.
// Given a sorted list from [first, last], assume we are inserting 'val'.
// As we insert, we continuously compare 'val' to elements starting at 'last' and going
// backwards. If val < first, though, then we must be sure 'first' is a special value
// to ensure we don't underflow the list.
// Thus, a guarded insertion is one where we know and compare against 'first' to avoid
// underflow, and must be used if the value can be less than the first element.
// An unguarded insertion avoids one comparison, but can only be used if we are certain
// 'val' comes after 'first' (which is true for almost all elements after our initial
// Quicksort).
template <class Type>
class SortUtil {
 public:
  // Comparison function such that Comparator(x, y) returns true if x is strictly less
  // than y.
  typedef boost::function<bool (Type*, Type*)> Comparator;

  // In-place sort of normalized elements between [first, last).
  // element_size is the actual size of each element.
  // We memcmp keys starting at key_offset, of length key_length.
  // If two keys are equal, we will call fallback_comp (if provided).
  //
  // Requirements for MutableIterator:
  //   - Standard "random access" iterator with *x, ++x/--x, x+int, x-y, and comparison.
  //   - In-place mutation functions:
  //     - x.Swap(y) swaps the values at *x and *y.
  //     - x.SetValue(val) sets *x = val.
  //   - MutableIterator::value_type is the type of elements (may be void).
  //
  // This sort is a modified version of the STL std::sort, with the heapsort removed.
  // Like std::sort, this sort is not stable.
  template <class MutableIterator>
  static inline void SortNormalized(const MutableIterator& first,
      const MutableIterator& last, int element_size, int key_offset, int key_length,
      Comparator* fallback_comp = NULL) {
    SortUtil::NormalizedSorter<MutableIterator> sorter(
        element_size, key_offset, key_length, fallback_comp);
    sorter.Sort(first, last);
  }

 private:
  template <class MutableIterator>
  class NormalizedSorter {
    friend class SortUtil;

    // When sorting this many or fewer elements, it is optimal to use an insertion sort.
    // This number was directly lifted from STL.
    // TODO: It's probably better to use an L1-sized block instead of "16 elements".
    static const int INSERTION_THRESHOLD = 16;

    NormalizedSorter(int element_size, int key_offset, int key_length,
        Comparator* fallback_comp)
        : element_size_(element_size), key_offset_(key_offset), key_length_(key_length),
          fallback_comp_(fallback_comp) {
      pivot_buffer_ =  reinterpret_cast<Type*>(malloc(element_size));
      insert_buffer_ = reinterpret_cast<Type*>(malloc(element_size));
      swap_buffer_ = reinterpret_cast<Type*>(malloc(element_size));
    }

    ~NormalizedSorter() {
      free(pivot_buffer_);
      free(insert_buffer_);
      free(swap_buffer_);
    }

    // The total length of each element.
    int element_size_;

    // Offset of the key in each element.
    int key_offset_;

    // Length of the key in each element.
    int key_length_;

    // Fallback comparison function used if the keys are equal.
    // If NULL, we simply skip this step, and assume the keys are actually equal.
    Comparator* fallback_comp_;

    // Buffer used to store a copy of the pivot while we operate over the vector.
    Type* pivot_buffer_;

    // Buffer used to store a copy of the insert element while we operate over the vector.
    Type* insert_buffer_;

    // Buffer used for iterator swaps.
    Type* swap_buffer_;

    inline bool Compare(Type* x, Type*  y) {
      uint8_t* x_key = reinterpret_cast<uint8_t*>(x) + key_offset_;
      uint8_t* y_key = reinterpret_cast<uint8_t*>(y) + key_offset_;
      int cmp = memcmp(x_key, y_key, key_length_);
      if (cmp == 0 && fallback_comp_ != NULL) return (*fallback_comp_)(x, y);
      return cmp < 0;
    }

    // Returns the median of 3 elements.
    inline Type* Median(Type* a, Type* b, Type* c) {
      if (Compare(a, b)) {
        if (Compare(b, c)) {
          return b;
        } else if (Compare(a, c)) {
          return c;
        } else {
          return a;
        }
      } else if (Compare(a, c)) {
        return a;
      } else if (Compare(b, c)) {
        return c;
      } else {
        return b;
      }
    }

    // Copies 'val' into 'buffer' and returns a pointer to the buffer.
    // This is used to keep a value saved while manipulating the vector.
    inline Type* CopyToBuffer(Type* val, Type* buffer) {
      memcpy(buffer, val, element_size_);
      return buffer;
    }

    // Copies [first, last), starting from last and moving backwards,
    // to result (also moving backwards).
    // Conceptually, this allows us to move a set of elements *forwards* in the vector
    // while losing only the element at *last.
    inline void CopyBackward(const MutableIterator& first, MutableIterator last,
        MutableIterator result) {
      while (last != first) {
        --last;
        --result;
        result.SetValue(*last);
      }
    }

    // Inserts val by bubbling backwards starting at 'last', until the right spot
    // to insert val is found.
    // This function is unguarded in the sense that it cannot handle the case
    // where val is the smallest element so far (because the smallest element
    // is not a special guard value).
    inline void UnguardedLinearInsert(MutableIterator last, Type* val) {
      val = CopyToBuffer(val, insert_buffer_);

      MutableIterator next = last;
      --next;
      while (Compare(val, *next)) {
        last.SetValue(*next);
        last = next;
        --next;
      }
      last.SetValue(val);
    }

    // Inserts last into a vector which is sorted between [first, last).
    // This function assumes that 'first' is the beginning of the vector.
    inline void LinearInsert(const MutableIterator& first, MutableIterator last) {
      Type* val = *last;
      if (Compare(val, *first)) {
        val = CopyToBuffer(val, insert_buffer_);
        CopyBackward(first, last, last + 1);
        first.SetValue(val);
      } else {
        UnguardedLinearInsert(last, val);
      }
    }

    // Insertion sort which sorts [first, last). Assumes first is the first element
    // of the vector.
    void InsertionSort(MutableIterator first, MutableIterator last) {
      if (first == last) return;
      for (MutableIterator i = first + 1; i != last; ++i) {
        LinearInsert(first, i);
      }
    }

    // Insertion sort which sorts a subset [first, last) of a vector with the requirement
    // that the smallest element is to the left of 'first'. (Note that first will not
    // be the first element of the entire vector.)
    void UnguardedInsertionSort(MutableIterator first, MutableIterator last) {
      for (MutableIterator i = first; i != last; ++i) {
        UnguardedLinearInsert(i, *i);
      }
    }

    // Performs a final comb of insertion sort over the entire vector.
    void FinalInsertionSort(MutableIterator first, MutableIterator last) {
      if (last - first > INSERTION_THRESHOLD) {
        InsertionSort(first, first + INSERTION_THRESHOLD);
        // We know that the first run will contain the smallest element,
        // so it's safe to do an unguarded insertion sort for the rest.
        UnguardedInsertionSort(first + INSERTION_THRESHOLD, last);
      } else {
        InsertionSort(first, last);
      }
    }

    // Partitions the iterator into 2 groups around pivot, returning an Iterator starting
    // at the first element of the second group.
    MutableIterator Partition(MutableIterator first, MutableIterator last, Type* pivot) {
      pivot = CopyToBuffer(pivot, pivot_buffer_);

      while (true) {
        // Search for the first and last out-of-place elements, and swap them.
        while (Compare(*first, pivot)) ++first;
        --last;
        while (Compare(pivot, *last))  --last;
        if (first >= last) return first;
        first.Swap(last, swap_buffer_);
        ++first;
      }
    }

    // Performs a quicksort over the given vector [first, last).
    // Avoids sorting small groups of elements where insertion sort is faster.
    void QuicksortLoop(const MutableIterator& first, MutableIterator last) {
      while (last - first > INSERTION_THRESHOLD) {
        Type* pivot = Median(*first,
                             *(first + (last - first)/2),
                             *(last - 1));
        MutableIterator cut = Partition(first, last, pivot);
        QuicksortLoop(cut, last);
        last = cut;
      }
    }

    // Performs a quicksort followed by an insertion sort to finish smaller blocks.
    void Sort(const MutableIterator& first, const MutableIterator& last) {
      if (first != last) {
        QuicksortLoop(first, last);
        FinalInsertionSort(first, last);
      }
    }
  };
};

}

#endif
