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

#include "util/lru-multi-cache.h"
#include "util/time.h"

namespace impala {

template <typename KeyType, typename ValueType>
template <typename... Args>
LruMultiCache<KeyType, ValueType>::ValueType_internal::ValueType_internal(
    LruMultiCache& cache, const KeyType& key, Container_internal& container,
    Args&&... args)
  : cache(cache),
    key(key),
    container(container),
    value(std::forward<Args>(args)...),
    timestamp_seconds(MonotonicSeconds()) {}

template <typename KeyType, typename ValueType>
bool LruMultiCache<KeyType, ValueType>::ValueType_internal::IsAvailable() {
  return member_hook.is_linked();
}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::Accessor(
    ValueType_internal* p_value_internal)
  : p_value_internal_(p_value_internal) {}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::Accessor(Accessor&& rhs) {
  p_value_internal_ = std::move(rhs.p_value_internal_);
  rhs.p_value_internal_ = nullptr;
}
template <typename KeyType, typename ValueType>
auto LruMultiCache<KeyType, ValueType>::Accessor::operator=(Accessor&& rhs) -> Accessor& {
  p_value_internal_ = std::move(rhs.p_value_internal_);
  rhs.p_value_internal_ = nullptr;
  return (*this);
}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::~Accessor() {
  Release();
}

template <typename KeyType, typename ValueType>
ValueType* LruMultiCache<KeyType, ValueType>::Accessor::Get() {
  if (p_value_internal_) return &(p_value_internal_->value);

  return nullptr;
}

template <typename KeyType, typename ValueType>
const KeyType* const LruMultiCache<KeyType, ValueType>::Accessor::GetKey() const {
  if (p_value_internal_) return &(p_value_internal_->key);

  return nullptr;
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Accessor::Release() {
  /// Nullptr check as it has to be dereferenced to get the cache reference
  /// No nullptr check is needed inside LruMultiCache::Release()
  if (p_value_internal_) {
    LruMultiCache& cache = p_value_internal_->cache;
    cache.Release(p_value_internal_);
    p_value_internal_ = nullptr;
  }
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Accessor::Destroy() {
  /// Nullptr check as it has to be dereferenced to get the cache reference
  /// No nullptr check is needed inside LruMultiCache::Destroy()
  if (p_value_internal_) {
    LruMultiCache& cache = p_value_internal_->cache;
    cache.Destroy(p_value_internal_);
    p_value_internal_ = nullptr;
  }
}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::LruMultiCache(size_t capacity)
  : capacity_(capacity), size_(0) {}

template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::Size() {
  std::lock_guard<SpinLock> g(lock_);
  return size_;
}

template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::NumberOfKeys() {
  std::lock_guard<SpinLock> g(lock_);
  return hash_table_.size();
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::SetCapacity(size_t new_capacity) {
  std::lock_guard<SpinLock> g(lock_);
  capacity_ = new_capacity;
}

template <typename KeyType, typename ValueType>
auto LruMultiCache<KeyType, ValueType>::Get(const KeyType& key) -> Accessor {
  std::lock_guard<SpinLock> g(lock_);
  auto hash_table_it = hash_table_.find(key);

  // No owning list found with this key, the caller will have to create a new object
  // with EmplaceAndGet()
  if (hash_table_it == hash_table_.end()) return Accessor();

  Container& container = hash_table_it->second;

  // Empty containers are deleted automatiacally
  DCHECK(!container.empty());

  // All the available elements are in the front, only need to check the first
  auto container_it = container.begin();

  // No available object found, the caller will have to create a new one with
  // EmplaceAndGet()
  if (!container_it->IsAvailable()) return Accessor();

  // Move the object to the back of the owning list as it is no longer available.
  container.splice(container.end(), container, container_it);

  // Remove the element from the LRU list as it is no longer available
  container_it->member_hook.unlink();

  return Accessor(&(*container_it));
}

template <typename KeyType, typename ValueType>
template <typename... Args>
auto LruMultiCache<KeyType, ValueType>::EmplaceAndGet(const KeyType& key, Args&&... args)
    -> Accessor {
  std::lock_guard<SpinLock> g(lock_);

  // creates default container if there isn't one
  Container& container = hash_table_[key];

  // Get the reference of the key stored in unordered_map, the parameter could be
  // temporary object but std::unordered_map has stable references
  const KeyType& stored_key = hash_table_.find(key)->first;

  // Place it as the last entry for the owning list, as it just got reserved
  auto container_it = container.emplace(
      container.end(), (*this), stored_key, container, std::forward<Args>(args)...);

  // Only can set this after emplace
  container_it->it = container_it;

  size_++;

  // Need to remove the oldest available if the cache is over the capacity
  EvictOneIfNeeded();

  return Accessor(&(*container_it));
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Release(ValueType_internal* p_value_internal) {
  std::lock_guard<SpinLock> g(lock_);

  // This only can be used by the accessor, which already checks for nullptr
  DCHECK(p_value_internal);

  // Has to be currently not available
  DCHECK(!p_value_internal->IsAvailable());

  p_value_internal->timestamp_seconds = MonotonicSeconds();

  Container& container = p_value_internal->container;

  // Move the object to the front, keep LRU relation in owning list too to
  // be able to age out unused objects
  container.splice(container.begin(), container, p_value_internal->it);

  // Add the object to LRU list too as it is now available for usage
  lru_list_.push_front(container.front());

  // In case we overshot the capacity already, the cache can evict the oldest one
  EvictOneIfNeeded();
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Destroy(ValueType_internal* p_value_internal) {
  std::lock_guard<SpinLock> g(lock_);

  // This only can be used by the accessor, which already checks for nullptr
  DCHECK(p_value_internal);

  // Has to be currently not available
  DCHECK(!p_value_internal->IsAvailable());

  Container& container = p_value_internal->container;

  if (container.size() == 1) {
    // Last element, owning list can be removed to prevent aging
    hash_table_.erase(p_value_internal->key);
  } else {
    // Remove from owning list
    container.erase(p_value_internal->it);
  }

  size_--;
}

template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::NumberOfAvailableObjects() {
  std::lock_guard<SpinLock> g(lock_);
  return lru_list_.size();
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Rehash() {
  std::lock_guard<SpinLock> g(lock_);
  hash_table_.rehash(hash_table_.bucket_count() + 1);
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::EvictOne(ValueType_internal& value_internal) {
  // SpinLock is locked by the caller evicting function
  lock_.DCheckLocked();

  // Has to be available to evict
  DCHECK(value_internal.IsAvailable());

  // Remove from LRU cache
  value_internal.member_hook.unlink();

  Container& container = value_internal.container;

  if (container.size() == 1) {
    // Last element, owning list can be removed to prevent aging
    hash_table_.erase(value_internal.key);
  } else {
    // Remove from owning list
    container.erase(value_internal.it);
  }

  size_--;
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::EvictOneIfNeeded() {
  // SpinLock is locked by the caller public function
  lock_.DCheckLocked();

  if (!lru_list_.empty() && size_ > capacity_) {
    EvictOne(lru_list_.back());
  }
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::EvictOlderThan(
    uint64_t oldest_allowed_timestamp) {
  std::lock_guard<SpinLock> g(lock_);

  // Stop eviction if
  //   - there are no more available (i.e. evictable) objects
  //   - cache size is below capacity and the oldest object is not older than the limit
  while (!lru_list_.empty()
      && (size_ > capacity_
             || lru_list_.back().timestamp_seconds < oldest_allowed_timestamp)) {
    EvictOne(lru_list_.back());
  }
}

} // namespace impala