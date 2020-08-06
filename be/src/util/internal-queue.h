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


#ifndef IMPALA_UTIL_INTERNAL_QUEUE_H
#define IMPALA_UTIL_INTERNAL_QUEUE_H

#include <mutex>

#include <boost/function.hpp>

#include "util/fake-lock.h"
#include "util/spinlock.h"

namespace impala {

/// FIFO queue implemented as a doubly-linked lists with internal pointers. This is in
/// contrast to the STL list which allocates a wrapper Node object around the data. Since
/// it's an internal queue, the list pointers are maintained in the Nodes which is memory
/// owned by the user. The nodes cannot be deallocated while the queue has elements.
/// The internal structure is a doubly-linked list.
///  nullptr <-- N1 <--> N2 <--> N3 --> nullptr
///          (head)          (tail)
///
/// InternalQueue<T> instantiates a thread-safe queue where the queue is protected by an
/// internal Spinlock. InternalList<T> instantiates a list with no thread safety.
///
/// To use these data structures, the element to be added to the queue or list must
/// subclass ::Node.
///
/// TODO: this is an ideal candidate to be made lock free.

/// T must be a subclass of InternalQueueBase::Node.
template <typename LockType, typename T>
class InternalQueueBase {
 public:
  struct Node {
   public:
    Node() : parent_queue(nullptr), next(nullptr), prev(nullptr) {}
    virtual ~Node() {}

    /// Returns true if the node is in a queue.
    bool in_queue() const { return parent_queue != nullptr; }

    /// Returns the Next/Prev node or nullptr if this is the end/front.
    T* Next() const {
      std::lock_guard<LockType> lock(parent_queue->lock_);
      return reinterpret_cast<T*>(next);
    }
    T* Prev() const {
      std::lock_guard<LockType> lock(parent_queue->lock_);
      return reinterpret_cast<T*>(prev);
    }

   private:
    friend class InternalQueueBase<LockType, T>;

    /// Pointer to the queue this Node is on. nullptr if not on any queue.
    InternalQueueBase<LockType, T>* parent_queue;
    Node* next;
    Node* prev;
  };

  InternalQueueBase() : head_(nullptr), tail_(nullptr), size_(0) {}

  /// Returns the element at the head of the list without dequeuing or nullptr
  /// if the queue is empty. This is O(1).
  T* head() const {
    std::lock_guard<LockType> lock(lock_);
    if (IsEmptyLocked()) return nullptr;
    return reinterpret_cast<T*>(head_);
  }

  /// Returns the element at the end of the list without dequeuing or nullptr
  /// if the queue is empty. This is O(1).
  T* tail() {
    std::lock_guard<LockType> lock(lock_);
    if (IsEmptyLocked()) return nullptr;
    return reinterpret_cast<T*>(tail_);
  }

  /// Enqueue node onto the queue's tail. This is O(1).
  void Enqueue(T* n) {
    Node* node = (Node*)n;
    DCHECK(node->next == nullptr);
    DCHECK(node->prev == nullptr);
    DCHECK(node->parent_queue == nullptr);
    node->parent_queue = this;
    {
      std::lock_guard<LockType> lock(lock_);
      if (tail_ != nullptr) tail_->next = node;
      node->prev = tail_;
      tail_ = node;
      if (head_ == nullptr) head_ = node;
      ++size_;
    }
  }

  /// Pushes the node onto the queue's head. This is O(1).
  void PushFront(T* n) {
    Node* node = (Node*)n;
    DCHECK(node->next == nullptr);
    DCHECK(node->prev == nullptr);
    DCHECK(node->parent_queue == nullptr);
    node->parent_queue = this;
    {
      std::lock_guard<LockType> lock(lock_);
      if (head_ != nullptr) head_->prev = node;
      node->next = head_;
      head_ = node;
      if (tail_ == nullptr) tail_ = node;
      ++size_;
    }
  }

  /// Dequeues an element from the queue's head. Returns nullptr if the queue
  /// is empty. This is O(1).
  T* Dequeue() {
    Node* result = nullptr;
    {
      std::lock_guard<LockType> lock(lock_);
      if (IsEmptyLocked()) return nullptr;
      --size_;
      result = head_;
      head_ = head_->next;
      if (head_ == nullptr) {
        tail_ = nullptr;
      } else {
        head_->prev = nullptr;
      }
    }
    DCHECK(result != nullptr);
    result->next = result->prev = nullptr;
    result->parent_queue = nullptr;
    return reinterpret_cast<T*>(result);
  }

  /// Dequeues an element from the queue's tail. Returns nullptr if the queue
  /// is empty. This is O(1).
  T* PopBack() {
    Node* result = nullptr;
    {
      std::lock_guard<LockType> lock(lock_);
      if (IsEmptyLocked()) return nullptr;
      --size_;
      result = tail_;
      tail_ = tail_->prev;
      if (tail_ == nullptr) {
        head_ = nullptr;
      } else {
        tail_->next = nullptr;
      }
    }
    DCHECK(result != nullptr);
    result->next = result->prev = nullptr;
    result->parent_queue = nullptr;
    return reinterpret_cast<T*>(result);
  }

  /// Removes 'node' from the queue. This is O(1). No-op if node is
  /// not on the list. Returns true if removed
  bool Remove(T* n) {
    Node* node = (Node*)n;
    if (node->parent_queue != this) return false;
    {
      std::lock_guard<LockType> lock(lock_);
      if (node->next == nullptr && node->prev == nullptr) {
        // Removing only node
        DCHECK(node == head_);
        DCHECK(tail_ == node);
        head_ = tail_ = nullptr;
        --size_;
        node->parent_queue = nullptr;
        return true;
      }

      if (head_ == node) {
        DCHECK(node->prev == nullptr);
        head_ = node->next;
      } else {
        DCHECK(node->prev != nullptr);
        node->prev->next = node->next;
      }

      if (node == tail_) {
        DCHECK(node->next == nullptr);
        tail_ = node->prev;
      } else if (node->next != nullptr) {
        node->next->prev = node->prev;
      }
      --size_;
    }
    node->next = node->prev = nullptr;
    node->parent_queue = nullptr;
    return true;
  }

  /// Clears all elements in the list.
  void Clear() {
    std::lock_guard<LockType> lock(lock_);
    Node* cur = head_;
    while (cur != nullptr) {
      Node* tmp = cur;
      cur = cur->next;
      tmp->prev = tmp->next = nullptr;
      tmp->parent_queue = nullptr;
    }
    size_ = 0;
    head_ = tail_ = nullptr;
  }

  int size() const {
    std::lock_guard<LockType> lock(lock_);
    return SizeLocked();
  }
  bool empty() const {
    std::lock_guard<LockType> lock(lock_);
    return IsEmptyLocked();
  }

  /// Returns if the target is on the queue. This is O(1) and does not acquire any locks.
  bool Contains(const T* target) const {
    return target->parent_queue == this;
  }

  /// Validates the internal structure of the list
  bool Validate() {
    int num_elements_found = 0;
    std::lock_guard<LockType> lock(lock_);
    if (head_ == nullptr) {
      if (tail_ != nullptr) return false;
      if (SizeLocked() != 0) return false;
      return true;
    }

    if (head_->prev != nullptr) return false;
    Node* current = head_;
    while (current != nullptr) {
      if (current->parent_queue != this) return false;
      ++num_elements_found;
      Node* next = current->next;
      if (next == nullptr) {
        if (current != tail_) return false;
      } else {
        if (next->prev != current) return false;
      }
      current = next;
    }
    if (num_elements_found != SizeLocked()) return false;
    return true;
  }

  // Iterate over elements of queue, calling 'fn' for each element. If 'fn' returns
  // false, terminate iteration. It is invalid to call other InternalQueue methods
  // from 'fn'.
  void Iterate(boost::function<bool(T*)> fn) {
    std::lock_guard<LockType> lock(lock_);
    for (Node* current = head_; current != nullptr; current = current->next) {
      if (!fn(reinterpret_cast<T*>(current))) return;
    }
  }

  // Iterate over first 'n' elements of queue, calling 'fn' for each element. If 'n' is
  // larger than the size of the queue, iteration will finish after the last element
  // reached. If 'fn' returns false, terminate iteration. It is invalid to call other
  // InternalQueue methods from 'fn'.
  void IterateFirstN(boost::function<bool(T*)> fn, int n) {
    std::lock_guard<LockType> lock(lock_);
    for (Node* current = head_; (current != nullptr) && (n > 0);
         current = current->next) {
      if (!fn(reinterpret_cast<T*>(current))) return;
      n--;
    }
  }

  /// Prints the queue ptrs to a string.
  std::string DebugString() {
    std::stringstream ss;
    ss << "(";
    {
      std::lock_guard<LockType> lock(lock_);
      Node* curr = head_;
      while (curr != nullptr) {
        ss << (void*)curr;
        curr = curr->next;
      }
    }
    ss << ")";
    return ss.str();
  }

 private:
  friend struct Node;

  inline int SizeLocked() const { return size_; }
  inline bool IsEmptyLocked() const { return head_ == nullptr; }

  mutable LockType lock_;
  Node *head_, *tail_;
  int size_;
};

// The default LockType is SpinLock.
template <typename T>
class InternalQueue : public InternalQueueBase<SpinLock, T> {};

// InternalList is a non-threadsafe implementation.
template <typename T>
class InternalList : public InternalQueueBase<FakeLock, T> {};
}
#endif
