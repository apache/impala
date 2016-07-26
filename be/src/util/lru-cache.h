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

#ifndef IMPALA_UTIL_LRU_CACHE_H_
#define IMPALA_UTIL_LRU_CACHE_H_

#include <boost/optional.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <list>
#include <map>
#include <stack>

#include "gutil/macros.h"
#include "util/spinlock.h"

namespace impala {

/// Implementation of a FifoMultimap.
///
/// The FifoMultimap is a cache-like data structure that keeps `capacity` number of
/// key-value pairs organized and accessible by key allowing multiple values per key. If
/// capacity is reached, key-value pairs are evicted in the least recently added order.
///
/// When accessing the values that have identical keys (via Pop(k)), values are typically
/// returned in LIFO order of this key. However, this property can only be guaranteed when
/// compiled with C++11 enabled. The motivation for accessing values in this particular
/// order is to avoid keeping all values of a particular key hot, even though only a
/// subset of the values is actually used.
///
/// On destruction of this class all resources are freed using the optional deleter
/// function.
///
/// This class is thread-safe and some methods may block under contention. This class
/// cannot be copied or assigned.
///
/// Example of a cache with capacity 3:
///
///   * First `<K1, V1>` is added to the cache, then `<K2, V2>`, `<K2, V3>`.
///
/// Eviction case: If a new key value pair is added `<K1, V1>` will be evicted as it
/// is the oldest entry. (FIFO)
///
/// Element access: If `K2` is accessed, `V3` will be returned first. (LIFO)
///
/// This class is thread-safe and protects its members using a spin lock.
template<typename Key, typename Value>
class FifoMultimap {
 public:
  typedef std::pair<Key, Value> ValueType;

  /// Deleter function type used to allow cleanup of resources held by Value when it is
  /// evicted. This method should only be used if it is not possible to embed the
  /// cleanup logic in the Value class itself.
  typedef void (*DeleterFn)(Value*);

  /// Instantiates the collection with an upper bound of `capacity` key-value pairs stored
  /// in the FifoMultimap and a `deleter` function pointer that can be used to free resources
  /// when a value is evicted from the FifoMultimap.
  FifoMultimap(size_t capacity, const DeleterFn& deleter = &FifoMultimap::DummyDeleter)
      : capacity_(capacity), deleter_(deleter), size_(0) {}

  /// Walk the list of elements and call the deleter function for each element.
  ~FifoMultimap();

  /// Add a key-value pair to the collection. Uniqueness of keys is not enforced. If the
  /// capacity is exceeded the oldest element will be evicted.
  void Put(const Key& k, const Value& v);

  /// Accesses an element of the collection by key and returns the value. In the process
  /// the key-value pair is removed from the collection. If the element is not found,
  /// returns boost::none.
  ///
  /// Typically, this will return the last element that was added under key `k` in case of
  /// duplicate keys.
  bool Pop(const Key& k, Value* out);

  /// Returns the total number of entries in the collection.
  size_t size(){
    boost::lock_guard<SpinLock> g(lock_);
    return cache_.size();
  }

  /// Returns the capacity of the cache
  size_t capacity() const { return capacity_; }

 private:
  DISALLOW_COPY_AND_ASSIGN(FifoMultimap);

  /// Total capacity, cannot be changed at run-time.
  const size_t capacity_;

  const DeleterFn deleter_;

  /// Protects access to cache_ and lru_list_.
  SpinLock lock_;

  typedef std::list<ValueType> ListType;

  /// The least recently added item is stored at the beginning of the list. The length of
  /// the list is limited to `capacity` number of elements. New elements are always
  /// appended to the end of the list.
  ListType lru_list_;

  typedef std::multimap<Key, typename ListType::iterator> MapType;

  /// Underlying associative container, that maps a key to an entry in the recently used
  /// list. The iterator on the list is only invalidated if the key-value pair is removed.
  MapType cache_;

  /// Number of elements stored in the cache.
  size_t size_;

  /// Evicts the least recently added element from the cache. First, the element is
  /// removed from both internal collections and as a last step the `deleter_` function is
  /// called to free potentially acquired resources.
  void EvictValue();

  static void DummyDeleter(Value* v) {}
};

}

#include "lru-cache.inline.h"

#endif  // IMPALA_UTIL_LRU_CACHE_H_
