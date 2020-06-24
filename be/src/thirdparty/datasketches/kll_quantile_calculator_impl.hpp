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

#ifndef KLL_QUANTILE_CALCULATOR_IMPL_HPP_
#define KLL_QUANTILE_CALCULATOR_IMPL_HPP_

#include <memory>
#include <cmath>

#include "kll_helper.hpp"

namespace datasketches {

template <typename T, typename C, typename A>
kll_quantile_calculator<T, C, A>::kll_quantile_calculator(const T* items, const uint32_t* levels, uint8_t num_levels, uint64_t n) {
  n_ = n;
  const uint32_t num_items = levels[num_levels] - levels[0];
  items_ = A().allocate(num_items);
  weights_ = AllocU64().allocate(num_items + 1); // one more is intentional
  levels_size_ = num_levels + 1;
  levels_ = AllocU32().allocate(levels_size_);
  populate_from_sketch(items, num_items, levels, num_levels);
  blocky_tandem_merge_sort(items_, weights_, num_items, levels_, num_levels_);
  convert_to_preceding_cummulative(weights_, num_items + 1);
}

template <typename T, typename C, typename A>
kll_quantile_calculator<T, C, A>::~kll_quantile_calculator() {
  const uint32_t num_items = levels_[num_levels_] - levels_[0];
  for (uint32_t i = 0; i < num_items; i++) items_[i].~T();
  A().deallocate(items_, num_items);
  AllocU64().deallocate(weights_, num_items + 1);
  AllocU32().deallocate(levels_, levels_size_);
}

template <typename T, typename C, typename A>
T kll_quantile_calculator<T, C, A>::get_quantile(double fraction) const {
  return approximately_answer_positional_query(pos_of_phi(fraction, n_));
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::populate_from_sketch(const T* items, uint32_t num_items, const uint32_t* levels, uint8_t num_levels) {
  kll_helper::copy_construct<T>(items, levels[0], levels[num_levels], items_, 0);
  uint8_t src_level = 0;
  uint8_t dst_level = 0;
  uint64_t weight = 1;
  uint32_t offset = levels[0];
  while (src_level < num_levels) {
    const uint32_t from_index(levels[src_level] - offset);
    const uint32_t to_index(levels[src_level + 1] - offset); // exclusive
    if (from_index < to_index) { // skip empty levels
      std::fill(&weights_[from_index], &weights_[to_index], weight);
      levels_[dst_level] = from_index;
      levels_[dst_level + 1] = to_index;
      dst_level++;
    }
    src_level++;
    weight *= 2;
  }
  weights_[num_items] = 0;
  num_levels_ = dst_level;
}

template <typename T, typename C, typename A>
T kll_quantile_calculator<T, C, A>::approximately_answer_positional_query(uint64_t pos) const {
  if (pos >= n_) throw std::logic_error("position out of range");
  const uint32_t weights_size(levels_[num_levels_] + 1);
  const uint32_t index = chunk_containing_pos(weights_, weights_size, pos);
  return items_[index];
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::convert_to_preceding_cummulative(uint64_t* weights, uint32_t weights_size) {
  uint64_t subtotal(0);
  for (uint32_t i = 0; i < weights_size; i++) {
    const uint32_t new_subtotal = subtotal + weights[i];
    weights[i] = subtotal;
    subtotal = new_subtotal;
  }
}

template <typename T, typename C, typename A>
uint64_t kll_quantile_calculator<T, C, A>::pos_of_phi(double phi, uint64_t n) {
  const uint64_t pos = std::floor(phi * n);
  return (pos == n) ? n - 1 : pos;
}

template <typename T, typename C, typename A>
uint32_t kll_quantile_calculator<T, C, A>::chunk_containing_pos(uint64_t* weights, uint32_t weights_size, uint64_t pos) {
  if (weights_size <= 1) throw std::logic_error("weights array too short"); // weights_ contains an "extra" position
  const uint32_t nominal_length(weights_size - 1);
  if (pos < weights[0]) throw std::logic_error("position too small");
  if (pos >= weights[nominal_length]) throw std::logic_error("position too large");
  return search_for_chunk_containing_pos(weights, pos, 0, nominal_length);
}

template <typename T, typename C, typename A>
uint32_t kll_quantile_calculator<T, C, A>::search_for_chunk_containing_pos(const uint64_t* arr, uint64_t pos, uint32_t l, uint32_t r) {
  if (l + 1 == r) {
    return l;
  }
  const uint32_t m(l + (r - l) / 2);
  if (arr[m] <= pos) {
    return search_for_chunk_containing_pos(arr, pos, m, r);
  }
  return search_for_chunk_containing_pos(arr, pos, l, m);
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::blocky_tandem_merge_sort(T* items, uint64_t* weights, uint32_t num_items, const uint32_t* levels, uint8_t num_levels) {
  if (num_levels == 1) return;

  // move the input in preparation for the "ping-pong" reduction strategy
  auto tmp_items_deleter = [num_items](T* ptr) {
    for (uint32_t i = 0; i < num_items; i++) ptr[i].~T();
    A().deallocate(ptr, num_items);
  };
  std::unique_ptr<T, decltype(tmp_items_deleter)> tmp_items(A().allocate(num_items), tmp_items_deleter);
  kll_helper::move_construct<T>(items, 0, num_items, tmp_items.get(), 0, false); // do not destroy since the items will be moved back
  auto tmp_weights_deleter = [num_items](uint64_t* ptr) { AllocU64().deallocate(ptr, num_items); };
  std::unique_ptr<uint64_t[], decltype(tmp_weights_deleter)> tmp_weights(AllocU64().allocate(num_items), tmp_weights_deleter); // don't need the extra one here
  std::copy(weights, &weights[num_items], tmp_weights.get());
  blocky_tandem_merge_sort_recursion(tmp_items.get(), tmp_weights.get(), items, weights, levels, 0, num_levels);
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::blocky_tandem_merge_sort_recursion(T* items_src, uint64_t* weights_src, T* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level, uint8_t num_levels) {
  if (num_levels == 1) return;
  const uint8_t num_levels_1 = num_levels / 2;
  const uint8_t num_levels_2 = num_levels - num_levels_1;
  if (num_levels_1 < 1) throw std::logic_error("level above 0 expected");
  if (num_levels_2 < num_levels_1) throw std::logic_error("wrong order of levels");
  const uint8_t starting_level_1 = starting_level;
  const uint8_t starting_level_2 = starting_level + num_levels_1;
  // swap roles of src and dst
  blocky_tandem_merge_sort_recursion(items_dst, weights_dst, items_src, weights_src, levels, starting_level_1, num_levels_1);
  blocky_tandem_merge_sort_recursion(items_dst, weights_dst, items_src, weights_src, levels, starting_level_2, num_levels_2);
  tandem_merge(items_src, weights_src, items_dst, weights_dst, levels, starting_level_1, num_levels_1, starting_level_2, num_levels_2);
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::tandem_merge(const T* items_src, const uint64_t* weights_src, T* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level_1, uint8_t num_levels_1, uint8_t starting_level_2, uint8_t num_levels_2) {
  const auto from_index_1 = levels[starting_level_1];
  const auto to_index_1 = levels[starting_level_1 + num_levels_1]; // exclusive
  const auto from_index_2 = levels[starting_level_2];
  const auto to_index_2 = levels[starting_level_2 + num_levels_2]; // exclusive
  auto i_src_1 = from_index_1;
  auto i_src_2 = from_index_2;
  auto i_dst = from_index_1;

  while ((i_src_1 < to_index_1) && (i_src_2 < to_index_2)) {
    if (C()(items_src[i_src_1], items_src[i_src_2])) {
      items_dst[i_dst] = std::move(items_src[i_src_1]);
      weights_dst[i_dst] = weights_src[i_src_1];
      i_src_1++;
    } else {
      items_dst[i_dst] = std::move(items_src[i_src_2]);
      weights_dst[i_dst] = weights_src[i_src_2];
      i_src_2++;
    }
    i_dst++;
  }
  if (i_src_1 < to_index_1) {
    std::move(&items_src[i_src_1], &items_src[to_index_1], &items_dst[i_dst]);
    std::copy(&weights_src[i_src_1], &weights_src[to_index_1], &weights_dst[i_dst]);
  } else if (i_src_2 < to_index_2) {
    std::move(&items_src[i_src_2], &items_src[to_index_2], &items_dst[i_dst]);
    std::copy(&weights_src[i_src_2], &weights_src[to_index_2], &weights_dst[i_dst]);
  }
}

} /* namespace datasketches */

#endif // KLL_QUANTILE_CALCULATOR_IMPL_HPP_
