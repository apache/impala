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

#include <boost/function.hpp>
#include <boost/thread/locks.hpp>

#include "util/fake-lock.h"
#include "util/spinlock.h"

namespace impala {

/// FIFO queue implemented as a doubly-linked lists with internal pointers. This is in
/// contrast to the STL list which allocates a wrapper Node object around the data. Since
/// it's an internal queue, the list pointers are maintained in the Nodes which is memory
/// owned by the user. The nodes cannot be deallocated while the queue has elements.
/// The internal structure is a doubly-linked list.
///  NULL <-- N1 <--> N2 <--> N3 --> NULL
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
    Node() : parent_queue(NULL), next(NULL), prev(NULL) {}
    virtual ~Node() {}

    /// Returns true if the node is in a queue.
    bool in_queue() const { return parent_queue != NULL; }

    /// Returns the Next/Prev node or NULL if this is the end/front.
    T* Next() const {
      boost::lock_guard<LockType> lock(parent_queue->lock_);
      return reinterpret_cast<T*>(next);
    }
    T* Prev() const {
      boost::lock_guard<LockType> lock(parent_queue->lock_);
      return reinterpret_cast<T*>(prev);
    }

   private:
    friend class InternalQueueBase<LockType, T>;

    /// Pointer to the queue this Node is on. NULL if not on any queue.
    InternalQueueBase<LockType, T>* parent_queue;
    Node* next;
    Node* prev;
  };

  InternalQueueBase() : head_(NULL), tail_(NULL), size_(0) {}

  /// Returns the element at the head of the list without dequeuing or NULL
  /// if the queue is empty. This is O(1).
  T* head() const {
    boost::lock_guard<LockType> lock(lock_);
    if (empty()) return NULL;
    return reinterpret_cast<T*>(head_);
  }

  /// Returns the element at the end of the list without dequeuing or NULL
  /// if the queue is empty. This is O(1).
  T* tail() {
    boost::lock_guard<LockType> lock(lock_);
    if (empty()) return NULL;
    return reinterpret_cast<T*>(tail_);
  }

  /// Enqueue node onto the queue's tail. This is O(1).
  void Enqueue(T* n) {
    Node* node = (Node*)n;
    DCHECK(node->next == NULL);
    DCHECK(node->prev == NULL);
    DCHECK(node->parent_queue == NULL);
    node->parent_queue = this;
    {
      boost::lock_guard<LockType> lock(lock_);
      if (tail_ != NULL) tail_->next = node;
      node->prev = tail_;
      tail_ = node;
      if (head_ == NULL) head_ = node;
      ++size_;
    }
  }

  /// Dequeues an element from the queue's head. Returns NULL if the queue
  /// is empty. This is O(1).
  T* Dequeue() {
    Node* result = NULL;
    {
      boost::lock_guard<LockType> lock(lock_);
      if (empty()) return NULL;
      --size_;
      result = head_;
      head_ = head_->next;
      if (head_ == NULL) {
        tail_ = NULL;
      } else {
        head_->prev = NULL;
      }
    }
    DCHECK(result != NULL);
    result->next = result->prev = NULL;
    result->parent_queue = NULL;
    return reinterpret_cast<T*>(result);
  }

  /// Dequeues an element from the queue's tail. Returns NULL if the queue
  /// is empty. This is O(1).
  T* PopBack() {
    Node* result = NULL;
    {
      boost::lock_guard<LockType> lock(lock_);
      if (empty()) return NULL;
      --size_;
      result = tail_;
      tail_ = tail_->prev;
      if (tail_ == NULL) {
        head_ = NULL;
      } else {
        tail_->next = NULL;
      }
    }
    DCHECK(result != NULL);
    result->next = result->prev = NULL;
    result->parent_queue = NULL;
    return reinterpret_cast<T*>(result);
  }

  /// Removes 'node' from the queue. This is O(1). No-op if node is
  /// not on the list. Returns true if removed
  bool Remove(T* n) {
    Node* node = (Node*)n;
    if (node->parent_queue != this) return false;
    {
      boost::lock_guard<LockType> lock(lock_);
      if (node->next == NULL && node->prev == NULL) {
        // Removing only node
        DCHECK(node == head_);
        DCHECK(tail_ == node);
        head_ = tail_ = NULL;
        --size_;
        node->parent_queue = NULL;
        return true;
      }

      if (head_ == node) {
        DCHECK(node->prev == NULL);
        head_ = node->next;
      } else {
        DCHECK(node->prev != NULL);
        node->prev->next = node->next;
      }

      if (node == tail_) {
        DCHECK(node->next == NULL);
        tail_ = node->prev;
      } else if (node->next != NULL) {
        node->next->prev = node->prev;
      }
      --size_;
    }
    node->next = node->prev = NULL;
    node->parent_queue = NULL;
    return true;
  }

  /// Clears all elements in the list.
  void Clear() {
    boost::lock_guard<LockType> lock(lock_);
    Node* cur = head_;
    while (cur != NULL) {
      Node* tmp = cur;
      cur = cur->next;
      tmp->prev = tmp->next = NULL;
      tmp->parent_queue = NULL;
    }
    size_ = 0;
    head_ = tail_ = NULL;
  }

  int size() const { return size_; }
  bool empty() const { return head_ == NULL; }

  /// Returns if the target is on the queue. This is O(1) and does not acquire any locks.
  bool Contains(const T* target) const {
    return target->parent_queue == this;
  }

  /// Validates the internal structure of the list
  bool Validate() {
    int num_elements_found = 0;
    boost::lock_guard<LockType> lock(lock_);
    if (head_ == NULL) {
      if (tail_ != NULL) return false;
      if (size() != 0) return false;
      return true;
    }

    if (head_->prev != NULL) return false;
    Node* current = head_;
    while (current != NULL) {
      if (current->parent_queue != this) return false;
      ++num_elements_found;
      Node* next = current->next;
      if (next == NULL) {
        if (current != tail_) return false;
      } else {
        if (next->prev != current) return false;
      }
      current = next;
    }
    if (num_elements_found != size()) return false;
    return true;
  }

  // Iterate over elements of queue, calling 'fn' for each element. If 'fn' returns
  // false, terminate iteration. It is invalid to call other InternalQueue methods
  // from 'fn'.
  void Iterate(boost::function<bool(T*)> fn) {
    boost::lock_guard<LockType> lock(lock_);
    for (Node* current = head_; current != NULL; current = current->next) {
      if (!fn(reinterpret_cast<T*>(current))) return;
    }
  }

  /// Prints the queue ptrs to a string.
  std::string DebugString() {
    std::stringstream ss;
    ss << "(";
    {
      boost::lock_guard<LockType> lock(lock_);
      Node* curr = head_;
      while (curr != NULL) {
        ss << (void*)curr;
        curr = curr->next;
      }
    }
    ss << ")";
    return ss.str();
  }

 private:
  friend struct Node;
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
