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

#ifndef THETA_INTERSECTION_IMPL_HPP_
#define THETA_INTERSECTION_IMPL_HPP_

#include <algorithm>

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
theta_intersection_alloc<A>::theta_intersection_alloc(uint64_t seed):
is_valid_(false),
is_empty_(false),
theta_(theta_sketch_alloc<A>::MAX_THETA),
lg_size_(0),
keys_(),
num_keys_(0),
seed_hash_(theta_sketch_alloc<A>::get_seed_hash(seed))
{}

template<typename A>
void theta_intersection_alloc<A>::update(const theta_sketch_alloc<A>& sketch) {
  if (is_empty_) return;
  if (!sketch.is_empty() && sketch.get_seed_hash() != seed_hash_) throw std::invalid_argument("seed hash mismatch");
  is_empty_ |= sketch.is_empty();
  theta_ = std::min(theta_, sketch.get_theta64());
  if (is_valid_ && num_keys_ == 0) return;
  if (sketch.get_num_retained() == 0) {
    is_valid_ = true;
    if (keys_.size() > 0) {
      keys_.resize(0);
      lg_size_ = 0;
      num_keys_ = 0;
    }
    return;
  }
  if (!is_valid_) { // first update, clone incoming sketch
    is_valid_ = true;
    lg_size_ = lg_size_from_count(sketch.get_num_retained(), update_theta_sketch_alloc<A>::REBUILD_THRESHOLD);
    keys_.resize(1 << lg_size_, 0);
    for (auto key: sketch) {
      if (!update_theta_sketch_alloc<A>::hash_search_or_insert(key, keys_.data(), lg_size_)) {
        throw std::invalid_argument("duplicate key, possibly corrupted input sketch");
      }
      ++num_keys_;
    }
    if (num_keys_ != sketch.get_num_retained()) throw std::invalid_argument("num keys mismatch, possibly corrupted input sketch");
  } else { // intersection
    const uint32_t max_matches = std::min(num_keys_, sketch.get_num_retained());
    vector_u64<A> matched_keys(max_matches);
    uint32_t match_count = 0;
    uint32_t count = 0;
    for (auto key: sketch) {
      if (key < theta_) {
        if (update_theta_sketch_alloc<A>::hash_search(key, keys_.data(), lg_size_)) {
          if (match_count == max_matches) throw std::invalid_argument("max matches exceeded, possibly corrupted input sketch");
          matched_keys[match_count++] = key;
        }
      } else if (sketch.is_ordered()) {
        break; // early stop
      }
      ++count;
    }
    if (count > sketch.get_num_retained()) {
      throw std::invalid_argument(" more keys then expected, possibly corrupted input sketch");
    } else if (!sketch.is_ordered() && count < sketch.get_num_retained()) {
      throw std::invalid_argument(" fewer keys then expected, possibly corrupted input sketch");
    }
    if (match_count == 0) {
      keys_.resize(0);
      lg_size_ = 0;
      num_keys_ = 0;
      if (theta_ == theta_sketch_alloc<A>::MAX_THETA) is_empty_ = true;
    } else {
      const uint8_t lg_size = lg_size_from_count(match_count, update_theta_sketch_alloc<A>::REBUILD_THRESHOLD);
      if (lg_size != lg_size_) {
        lg_size_ = lg_size;
        keys_.resize(1 << lg_size_);
      }
      std::fill(keys_.begin(), keys_.end(), 0);
      for (uint32_t i = 0; i < match_count; i++) {
        update_theta_sketch_alloc<A>::hash_search_or_insert(matched_keys[i], keys_.data(), lg_size_);
      }
      num_keys_ = match_count;
    }
  }
}

template<typename A>
compact_theta_sketch_alloc<A> theta_intersection_alloc<A>::get_result(bool ordered) const {
  if (!is_valid_) throw std::invalid_argument("calling get_result() before calling update() is undefined");
  vector_u64<A> keys(num_keys_);
  if (num_keys_ > 0) {
    std::copy_if(keys_.begin(), keys_.end(), keys.begin(), [](uint64_t key) { return key != 0; });
    if (ordered) std::sort(keys.begin(), keys.end());
  }
  return compact_theta_sketch_alloc<A>(is_empty_, theta_, std::move(keys), seed_hash_, ordered);
}

template<typename A>
bool theta_intersection_alloc<A>::has_result() const {
  return is_valid_;
}

} /* namespace datasketches */

# endif
