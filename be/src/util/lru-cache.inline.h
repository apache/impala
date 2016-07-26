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

#ifndef IMPALA_UTIL_LRU_CACHE_INLINE_H_
#define IMPALA_UTIL_LRU_CACHE_INLINE_H_

namespace impala {

template <typename Key, typename Value>
FifoMultimap<Key, Value>::~FifoMultimap() {
  for (typename ListType::iterator b = lru_list_.begin(),
      e = lru_list_.end(); b != e; ++b) {
    deleter_(&(b->second));
  }
}

template <typename Key, typename Value>
void FifoMultimap<Key, Value>::Put(const Key& k, const Value& v) {
  boost::lock_guard<SpinLock> g(lock_);
  if (capacity_ <= 0) return;
  if (size_ >= capacity_) EvictValue();
  const ValueType& kv_pair = std::make_pair(k, v);
  const typename MapType::value_type& val =
      std::make_pair(k, lru_list_.insert(lru_list_.end(), kv_pair));
  // Find an insert hint to use with the insert operation
  typename MapType::iterator it = cache_.lower_bound(k);
  if (it == cache_.end() || it->first != k) {
    cache_.insert(val);
  } else {
    // If the insert hint is sufficiently good, the element will be inserted before in the
    // sequence of elements having the same key
    cache_.insert(it, val);
  }
  ++size_;
}

template <typename Key, typename Value>
bool FifoMultimap<Key, Value>::Pop(const Key& k, Value* out) {
  boost::lock_guard<SpinLock> g(lock_);
  // Find the first value under key k.
  typename MapType::iterator it = cache_.lower_bound(k);
  if (it == cache_.end() || it->first != k) return false;
  typename ListType::iterator lit = it->second;
  *out = lit->second;
  lru_list_.erase(lit);
  cache_.erase(it);
  --size_;
  return true;
}

template <typename Key, typename Value>
void FifoMultimap<Key, Value>::EvictValue() {
  DCHECK(!lru_list_.empty());
  typename ListType::iterator to_evict = lru_list_.begin();
  // Find all elements under this key, until C++11 the order of the elements is not
  // guaranteed.
  std::pair<typename MapType::iterator, typename MapType::iterator> range =
      cache_.equal_range(to_evict->first);
  // Search until the value is found in the range of the same key.
  while (range.first != range.second) {
    if (range.first->second == to_evict) {
      cache_.erase(range.first);
      break;
    }
    ++range.first;
  }
  DCHECK(range.first != range.second); // LCOV_EXCL_LINE
  deleter_(&(to_evict->second));
  lru_list_.erase(to_evict);
  --size_;
}

}

#endif // IMPALA_UTIL_LRU_CACHE_INLINE_H_
