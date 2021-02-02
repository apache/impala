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

#pragma once

#include "codegen/impala-ir.h"

namespace impala {

template <typename T, typename C>
class PriorityQueue;

// A simple iterator for PriorityQueue
template <typename T, typename C>
class PriorityQueueIterator {
 public:
  PriorityQueueIterator(PriorityQueue<T, C>* queue, int index)
    : queue_(queue), index_(index) {}
  ~PriorityQueueIterator() {}

  PriorityQueue<T, C>* GetPriorityQueue() { return queue_; }
  // The operator to cast 'this' to int
  operator int() { return index_; }
  // The dereference operator
  T& operator*() { return queue_->elements_[index_]; }
  // The prefix increment operator ++itor
  PriorityQueueIterator<T, C>& operator++() {
    index_++;
    return *this;
  }
  // The Less operator
  bool operator<(PriorityQueueIterator& other) { return index_ < other.index_; }

 private:
  // The priority queue.
  PriorityQueue<T, C>* queue_;
  // The index to the element that this iterator is pointing at.
  int index_;
};

// A priority queue template based on the array implementation of a binary search tree.
// The first type T is the type of the elements of the queue while the second type C
// is the comparator to compare elements during the heapification operations.
//
// Within the template, the array elements_ (represented by std::vector<T>) holds the
// elements of the queue.
//
// This class calls C.Less(const T& x, const T& y) to compare elements and expects a
// 'true' return when x < y. Since any node in the binary search tree is larger than
// any of its two children, this class implements a max heap.
//
// The grow and shrink of the array is maintained automatically by std::vector<T> and
// the array is destroyed upon the destruction of the queue.
//
// To add to the queue, call Push(). To remove the top element, call Pop(). To look at
// the top element without removal, call Top().
//
// The priority queue is not thread safe.
//
template <typename T, typename C>
class PriorityQueue {
 public:
  PriorityQueue(const C& c) : elements_(), comparator_(c) {}

  ~PriorityQueue() {}

  // Clear all elements
  void Clear() { elements_.clear(); }

  // The top element
  T& IR_ALWAYS_INLINE Top() {
    DCHECK(!elements_.empty());
    return elements_[0];
  }

  // The ith element
  T& operator[](int i) {
    DCHECK_LT(i, elements_.size());
    return elements_[i];
  }

  void Reserve(int64_t capacity) { elements_.reserve(capacity); }

  // The size of the heap
  int64_t IR_ALWAYS_INLINE Size() const { return elements_.size(); }

  // Check if the heap is empty
  bool IR_ALWAYS_INLINE Empty() const { return elements_.empty(); }

  // Get the begin iterator.
  PriorityQueueIterator<T, C> Begin() { return PriorityQueueIterator<T, C>(this, 0); }

  // Get the end iterator.
  PriorityQueueIterator<T, C> End() { return PriorityQueueIterator<T, C>(this, Size()); }

  // Add a new element to the end of the array and then heapify. This is an
  // O(log n) operation.
  void IR_ALWAYS_INLINE Push(const T& v) {
    // Insert the new element v at the end.
    elements_.emplace_back(v);
    // Move up the new element and its ancestors if necessary (all the way to the
    // element at index 0) to maintain the heap property.
    HeapifySubtreeUp(0, Size() - 1);
  }

  // Remove the top element from the priority queue and then heapify affected
  // elements. This is an O(log n) operation. Must not be called when the queue
  // is empty.
  T IR_ALWAYS_INLINE Pop() {
    if (Size() > 0) {
      int last = Size() - 1;
      T top_element = Top();
      // Swap the top element from 0 to index 'last'.
      Swap(elements_[0], elements_[last]);
      // Heapify from index 0 and down.
      HeapifySubtreeDown(0, last);
      elements_.pop_back();
      // Return the original top element.
      return top_element;
    }
    DCHECK(false);
    return T();
  }

  // Heapify from top element at index 0 in O(log n) complexity. It is
  // assumed that the elements in [1, size_-1] are already arranged as a heap.
  void IR_ALWAYS_INLINE HeapifyFromTop() { HeapifySubtreeDown(0, Size()); }

 protected:
  // Move up elements starting at index i to maintain the heap property.
  // The operation stops when the element at index low_bound has been reached.
  // It is assumed that the elements in [low_bound, i-1] are already arranged
  // as a heap. This is an O(log n) algorithm.
  void IR_ALWAYS_INLINE HeapifySubtreeUp(int low_bound, int i) {
    while (i > low_bound) {
      // parent = (i-2)/2            when i is even, or
      //        = (i-1)/2 = (i)/2    when i is odd.
      int parent = (i & 1) ? (i >> 1) : ((i - 2) >> 1);
      if (comparator_.Less(elements_[i], elements_[parent])) {
        break;
      } else {
        Swap(elements_[parent], elements_[i]);
      }
      // Continue to heapify the affected sub-tree at root 'parent'.
      i = parent;
    }
  }

  // Heapify a sub-tree rooted at index i, all the way to the element at index
  // high_bound-1. It is assumed that the elements in [i, high_bound-1] are already
  // arranged as a heap. This is an O(log n) algorithm.
  void IR_ALWAYS_INLINE HeapifySubtreeDown(int i, int high_bound) {
    while (i < high_bound) {
      int largest = i; // Initialize largest as the root
      int left = (i << 1) + 1; // left  = 2*i + 1
      int right = left + 1; // right = 2*i + 2
      // If the left child is larger than the root
      if (left < high_bound && comparator_.Less(elements_[largest], elements_[left])) {
        largest = left;
      }
      // If the right child is larger than the largest
      if (right < high_bound && comparator_.Less(elements_[largest], elements_[right])) {
        largest = right;
      }
      // If the largest is not the root
      if (largest != i) {
        Swap(elements_[i], elements_[largest]);
        // Continue to heapify the affected sub-tree at root largest.
        i = largest;
      } else {
        break;
      }
    }
  }

  // Swap two elements of type T.
  void IR_ALWAYS_INLINE Swap(T& x, T& y) {
    T t = x;
    x = y;
    y = t;
  }

 private:
  // The array holding the elements.
  std::vector<T> elements_;
  // The comparator
  const C& comparator_;

  friend class PriorityQueueIterator<T, C>;
};

} // namespace impala

