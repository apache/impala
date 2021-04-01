/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef THETA_UPDATE_SKETCH_BASE_IMPL_HPP_
#define THETA_UPDATE_SKETCH_BASE_IMPL_HPP_

#include <iostream>
#include <sstream>
#include <algorithm>

namespace datasketches {

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>::theta_update_sketch_base(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, uint64_t theta, uint64_t seed, const A& allocator, bool is_empty):
allocator_(allocator),
is_empty_(is_empty),
lg_cur_size_(lg_cur_size),
lg_nom_size_(lg_nom_size),
rf_(rf),
num_entries_(0),
theta_(theta),
seed_(seed),
entries_(nullptr)
{
  if (lg_cur_size > 0) {
    const size_t size = 1 << lg_cur_size;
    entries_ = allocator_.allocate(size);
    for (size_t i = 0; i < size; ++i) EK()(entries_[i]) = 0;
  }
}

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>::theta_update_sketch_base(const theta_update_sketch_base& other):
allocator_(other.allocator_),
is_empty_(other.is_empty_),
lg_cur_size_(other.lg_cur_size_),
lg_nom_size_(other.lg_nom_size_),
rf_(other.rf_),
num_entries_(other.num_entries_),
theta_(other.theta_),
seed_(other.seed_),
entries_(nullptr)
{
  if (other.entries_ != nullptr) {
    const size_t size = 1 << lg_cur_size_;
    entries_ = allocator_.allocate(size);
    for (size_t i = 0; i < size; ++i) {
      if (EK()(other.entries_[i]) != 0) {
        new (&entries_[i]) EN(other.entries_[i]);
      } else {
        EK()(entries_[i]) = 0;
      }
    }
  }
}

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>::theta_update_sketch_base(theta_update_sketch_base&& other) noexcept:
allocator_(std::move(other.allocator_)),
is_empty_(other.is_empty_),
lg_cur_size_(other.lg_cur_size_),
lg_nom_size_(other.lg_nom_size_),
rf_(other.rf_),
num_entries_(other.num_entries_),
theta_(other.theta_),
seed_(other.seed_),
entries_(other.entries_)
{
  other.entries_ = nullptr;
}

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>::~theta_update_sketch_base()
{
  if (entries_ != nullptr) {
    const size_t size = 1 << lg_cur_size_;
    for (size_t i = 0; i < size; ++i) {
      if (EK()(entries_[i]) != 0) entries_[i].~EN();
    }
    allocator_.deallocate(entries_, size);
  }
}

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>& theta_update_sketch_base<EN, EK, A>::operator=(const theta_update_sketch_base& other) {
  theta_update_sketch_base<EN, EK, A> copy(other);
  std::swap(allocator_, copy.allocator_);
  std::swap(is_empty_, copy.is_empty_);
  std::swap(lg_cur_size_, copy.lg_cur_size_);
  std::swap(lg_nom_size_, copy.lg_nom_size_);
  std::swap(rf_, copy.rf_);
  std::swap(num_entries_, copy.num_entries_);
  std::swap(theta_, copy.theta_);
  std::swap(seed_, copy.seed_);
  std::swap(entries_, copy.entries_);
  return *this;
}

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>& theta_update_sketch_base<EN, EK, A>::operator=(theta_update_sketch_base&& other) {
  std::swap(allocator_, other.allocator_);
  std::swap(is_empty_, other.is_empty_);
  std::swap(lg_cur_size_, other.lg_cur_size_);
  std::swap(lg_nom_size_, other.lg_nom_size_);
  std::swap(rf_, other.rf_);
  std::swap(num_entries_, other.num_entries_);
  std::swap(theta_, other.theta_);
  std::swap(seed_, other.seed_);
  std::swap(entries_, other.entries_);
  return *this;
}

template<typename EN, typename EK, typename A>
uint64_t theta_update_sketch_base<EN, EK, A>::hash_and_screen(const void* data, size_t length) {
  is_empty_ = false;
  const uint64_t hash = compute_hash(data, length, seed_);
  if (hash >= theta_) return 0; // hash == 0 is reserved to mark empty slots in the table
  return hash;
}

template<typename EN, typename EK, typename A>
auto theta_update_sketch_base<EN, EK, A>::find(uint64_t key) const -> std::pair<iterator, bool> {
  const size_t size = 1 << lg_cur_size_;
  const size_t mask = size - 1;
  const uint32_t stride = get_stride(key, lg_cur_size_);
  uint32_t index = static_cast<uint32_t>(key) & mask;
  // search for duplicate or zero
  const uint32_t loop_index = index;
  do {
    const uint64_t probe = EK()(entries_[index]);
    if (probe == 0) {
      return std::pair<iterator, bool>(&entries_[index], false);
    } else if (probe == key) {
      return std::pair<iterator, bool>(&entries_[index], true);
    }
    index = (index + stride) & mask;
  } while (index != loop_index);
  throw std::logic_error("key not found and no empty slots!");
}

template<typename EN, typename EK, typename A>
template<typename Fwd>
void theta_update_sketch_base<EN, EK, A>::insert(iterator it, Fwd&& entry) {
  new (it) EN(std::forward<Fwd>(entry));
  ++num_entries_;
  if (num_entries_ > get_capacity(lg_cur_size_, lg_nom_size_)) {
    if (lg_cur_size_ <= lg_nom_size_) {
      resize();
    } else {
      rebuild();
    }
  }
}

template<typename EN, typename EK, typename A>
auto theta_update_sketch_base<EN, EK, A>::begin() const -> iterator {
  return entries_;
}

template<typename EN, typename EK, typename A>
auto theta_update_sketch_base<EN, EK, A>::end() const -> iterator {
  return &entries_[1 << lg_cur_size_];
}

template<typename EN, typename EK, typename A>
uint32_t theta_update_sketch_base<EN, EK, A>::get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size) {
  const double fraction = (lg_cur_size <= lg_nom_size) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;
  return std::floor(fraction * (1 << lg_cur_size));
}

template<typename EN, typename EK, typename A>
uint32_t theta_update_sketch_base<EN, EK, A>::get_stride(uint64_t key, uint8_t lg_size) {
  // odd and independent of index assuming lg_size lowest bits of the key were used for the index
  return (2 * static_cast<uint32_t>((key >> lg_size) & STRIDE_MASK)) + 1;
}

template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::resize() {
  const size_t old_size = 1 << lg_cur_size_;
  const uint8_t lg_tgt_size = lg_nom_size_ + 1;
  const uint8_t factor = std::max(1, std::min(static_cast<int>(rf_), lg_tgt_size - lg_cur_size_));
  lg_cur_size_ += factor;
  const size_t new_size = 1 << lg_cur_size_;
  EN* old_entries = entries_;
  entries_ = allocator_.allocate(new_size);
  for (size_t i = 0; i < new_size; ++i) EK()(entries_[i]) = 0;
  num_entries_ = 0;
  for (size_t i = 0; i < old_size; ++i) {
    const uint64_t key = EK()(old_entries[i]);
    if (key != 0) {
      insert(find(key).first, std::move(old_entries[i])); // consider a special insert with no comparison
      old_entries[i].~EN();
    }
  }
  allocator_.deallocate(old_entries, old_size);
}

// assumes number of entries > nominal size
template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::rebuild() {
  const size_t size = 1 << lg_cur_size_;
  const uint32_t nominal_size = 1 << lg_nom_size_;

  // empty entries have uninitialized payloads
  // TODO: avoid this for empty or trivial payloads (arithmetic types)
  consolidate_non_empty(entries_, size, num_entries_);

  std::nth_element(entries_, entries_ + nominal_size, entries_ + num_entries_, comparator());
  this->theta_ = EK()(entries_[nominal_size]);
  EN* old_entries = entries_;
  const size_t num_old_entries = num_entries_;
  entries_ = allocator_.allocate(size);
  for (size_t i = 0; i < size; ++i) EK()(entries_[i]) = 0;
  num_entries_ = 0;
  // relies on consolidating non-empty entries to the front
  for (size_t i = 0; i < nominal_size; ++i) {
    insert(find(EK()(old_entries[i])).first, std::move(old_entries[i])); // consider a special insert with no comparison
    old_entries[i].~EN();
  }
  for (size_t i = nominal_size; i < num_old_entries; ++i) old_entries[i].~EN();
  allocator_.deallocate(old_entries, size);
}

template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::trim() {
  if (num_entries_ > static_cast<uint32_t>(1 << lg_nom_size_)) rebuild();
}

template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::consolidate_non_empty(EN* entries, size_t size, size_t num) {
  // find the first empty slot
  size_t i = 0;
  while (i < size) {
    if (EK()(entries[i]) == 0) break;
    ++i;
  }
  // scan the rest and move non-empty entries to the front
  for (size_t j = i + 1; j < size; ++j) {
    if (EK()(entries[j]) != 0) {
      new (&entries[i]) EN(std::move(entries[j]));
      entries[j].~EN();
      EK()(entries[j]) = 0;
      ++i;
      if (i == num) break;
    }
  }
}

// builder

template<typename Derived, typename Allocator>
theta_base_builder<Derived, Allocator>::theta_base_builder(const Allocator& allocator):
allocator_(allocator), lg_k_(DEFAULT_LG_K), rf_(DEFAULT_RESIZE_FACTOR), p_(1), seed_(DEFAULT_SEED) {}

template<typename Derived, typename Allocator>
Derived& theta_base_builder<Derived, Allocator>::set_lg_k(uint8_t lg_k) {
  if (lg_k < MIN_LG_K) {
    throw std::invalid_argument("lg_k must not be less than " + std::to_string(MIN_LG_K) + ": " + std::to_string(lg_k));
  }
  if (lg_k > MAX_LG_K) {
    throw std::invalid_argument("lg_k must not be greater than " + std::to_string(MAX_LG_K) + ": " + std::to_string(lg_k));
  }
  lg_k_ = lg_k;
  return static_cast<Derived&>(*this);
}

template<typename Derived, typename Allocator>
Derived& theta_base_builder<Derived, Allocator>::set_resize_factor(resize_factor rf) {
  rf_ = rf;
  return static_cast<Derived&>(*this);
}

template<typename Derived, typename Allocator>
Derived& theta_base_builder<Derived, Allocator>::set_p(float p) {
  if (p <= 0 || p > 1) throw std::invalid_argument("sampling probability must be between 0 and 1");
  p_ = p;
  return static_cast<Derived&>(*this);
}

template<typename Derived, typename Allocator>
Derived& theta_base_builder<Derived, Allocator>::set_seed(uint64_t seed) {
  seed_ = seed;
  return static_cast<Derived&>(*this);
}

template<typename Derived, typename Allocator>
uint64_t theta_base_builder<Derived, Allocator>::starting_theta() const {
  if (p_ < 1) return theta_constants::MAX_THETA * p_;
  return theta_constants::MAX_THETA;
}

template<typename Derived, typename Allocator>
uint8_t theta_base_builder<Derived, Allocator>::starting_lg_size() const {
  return starting_sub_multiple(lg_k_ + 1, MIN_LG_K, static_cast<uint8_t>(rf_));
}

template<typename Derived, typename Allocator>
uint8_t theta_base_builder<Derived, Allocator>::starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf) {
  return (lg_tgt <= lg_min) ? lg_min : (lg_rf == 0) ? lg_tgt : ((lg_tgt - lg_min) % lg_rf) + lg_min;
}

// iterator

template<typename Entry, typename ExtractKey>
theta_iterator<Entry, ExtractKey>::theta_iterator(Entry* entries, uint32_t size, uint32_t index):
entries_(entries), size_(size), index_(index) {
  while (index_ < size_ && ExtractKey()(entries_[index_]) == 0) ++index_;
}

template<typename Entry, typename ExtractKey>
auto theta_iterator<Entry, ExtractKey>::operator++() -> theta_iterator& {
  ++index_;
  while (index_ < size_ && ExtractKey()(entries_[index_]) == 0) ++index_;
  return *this;
}

template<typename Entry, typename ExtractKey>
auto theta_iterator<Entry, ExtractKey>::operator++(int) -> theta_iterator {
  theta_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename Entry, typename ExtractKey>
bool theta_iterator<Entry, ExtractKey>::operator!=(const theta_iterator& other) const {
  return index_ != other.index_;
}

template<typename Entry, typename ExtractKey>
bool theta_iterator<Entry, ExtractKey>::operator==(const theta_iterator& other) const {
  return index_ == other.index_;
}

template<typename Entry, typename ExtractKey>
auto theta_iterator<Entry, ExtractKey>::operator*() const -> Entry& {
  return entries_[index_];
}

// const iterator

template<typename Entry, typename ExtractKey>
theta_const_iterator<Entry, ExtractKey>::theta_const_iterator(const Entry* entries, uint32_t size, uint32_t index):
entries_(entries), size_(size), index_(index) {
  while (index_ < size_ && ExtractKey()(entries_[index_]) == 0) ++index_;
}

template<typename Entry, typename ExtractKey>
auto theta_const_iterator<Entry, ExtractKey>::operator++() -> theta_const_iterator& {
  ++index_;
  while (index_ < size_ && ExtractKey()(entries_[index_]) == 0) ++index_;
  return *this;
}

template<typename Entry, typename ExtractKey>
auto theta_const_iterator<Entry, ExtractKey>::operator++(int) -> theta_const_iterator {
  theta_const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename Entry, typename ExtractKey>
bool theta_const_iterator<Entry, ExtractKey>::operator!=(const theta_const_iterator& other) const {
  return index_ != other.index_;
}

template<typename Entry, typename ExtractKey>
bool theta_const_iterator<Entry, ExtractKey>::operator==(const theta_const_iterator& other) const {
  return index_ == other.index_;
}

template<typename Entry, typename ExtractKey>
auto theta_const_iterator<Entry, ExtractKey>::operator*() const -> const Entry& {
  return entries_[index_];
}

} /* namespace datasketches */

#endif
