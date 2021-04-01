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
#include <algorithm>

#include "kll_helper.hpp"

namespace datasketches {

template <typename T, typename C, typename A>
kll_quantile_calculator<T, C, A>::kll_quantile_calculator(const T* items, const uint32_t* levels, uint8_t num_levels, uint64_t n, const A& allocator):
n_(n), levels_(num_levels + 1, 0, allocator), entries_(allocator)
{
  const uint32_t num_items = levels[num_levels] - levels[0];
  entries_.reserve(num_items);
  populate_from_sketch(items, levels, num_levels);
  merge_sorted_blocks(entries_, levels_.data(), levels_.size() - 1, num_items);
  if (!is_sorted(entries_.begin(), entries_.end(), compare_pair_by_first<C>())) throw std::logic_error("entries must be sorted");
  convert_to_preceding_cummulative();
}

template <typename T, typename C, typename A>
T kll_quantile_calculator<T, C, A>::get_quantile(double fraction) const {
  return approximately_answer_positional_query(pos_of_phi(fraction, n_));
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::populate_from_sketch(const T* items, const uint32_t* levels, uint8_t num_levels) {
  size_t src_level = 0;
  size_t dst_level = 0;
  uint64_t weight = 1;
  uint32_t offset = levels[0];
  while (src_level < num_levels) {
    const uint32_t from_index(levels[src_level] - offset);
    const uint32_t to_index(levels[src_level + 1] - offset); // exclusive
    if (from_index < to_index) { // skip empty levels
      for (uint32_t i = from_index; i < to_index; ++i) {
        entries_.push_back(Entry(items[i + offset], weight));
      }
      levels_[dst_level] = from_index;
      levels_[dst_level + 1] = to_index;
      dst_level++;
    }
    src_level++;
    weight *= 2;
  }
  if (levels_.size() > static_cast<size_t>(dst_level + 1)) levels_.resize(dst_level + 1);
}

template <typename T, typename C, typename A>
T kll_quantile_calculator<T, C, A>::approximately_answer_positional_query(uint64_t pos) const {
  if (pos >= n_) throw std::logic_error("position out of range");
  const uint32_t num_items = levels_[levels_.size() - 1];
  if (pos > entries_[num_items - 1].second) return entries_[num_items - 1].first;
  const uint32_t index = chunk_containing_pos(pos);
  return entries_[index].first;
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::convert_to_preceding_cummulative() {
  uint64_t subtotal = 0;
  for (auto& entry: entries_) {
    const uint64_t new_subtotal = subtotal + entry.second;
    entry.second = subtotal;
    subtotal = new_subtotal;
  }
}

template <typename T, typename C, typename A>
uint64_t kll_quantile_calculator<T, C, A>::pos_of_phi(double phi, uint64_t n) {
  const uint64_t pos = std::floor(phi * n);
  return (pos == n) ? n - 1 : pos;
}

template <typename T, typename C, typename A>
uint32_t kll_quantile_calculator<T, C, A>::chunk_containing_pos(uint64_t pos) const {
  if (entries_.size() < 1) throw std::logic_error("array too short");
  if (pos < entries_[0].second) throw std::logic_error("position too small");
  if (pos > entries_[entries_.size() - 1].second) throw std::logic_error("position too large");
  return search_for_chunk_containing_pos(pos, 0, entries_.size());
}

template <typename T, typename C, typename A>
uint32_t kll_quantile_calculator<T, C, A>::search_for_chunk_containing_pos(uint64_t pos, uint32_t l, uint32_t r) const {
  if (l + 1 == r) {
    return l;
  }
  const uint32_t m(l + (r - l) / 2);
  if (entries_[m].second <= pos) {
    return search_for_chunk_containing_pos(pos, m, r);
  }
  return search_for_chunk_containing_pos(pos, l, m);
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::merge_sorted_blocks(Container& entries, const uint32_t* levels, uint8_t num_levels, uint32_t num_items) {
  if (num_levels == 1) return;
  Container temporary(entries.get_allocator());
  temporary.reserve(num_items);
  merge_sorted_blocks_direct(entries, temporary, levels, 0, num_levels);
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::merge_sorted_blocks_direct(Container& orig, Container& temp, const uint32_t* levels,
    uint8_t starting_level, uint8_t num_levels) {
  if (num_levels == 1) return;
  const uint8_t num_levels_1 = num_levels / 2;
  const uint8_t num_levels_2 = num_levels - num_levels_1;
  const uint8_t starting_level_1 = starting_level;
  const uint8_t starting_level_2 = starting_level + num_levels_1;
  const auto chunk_begin = temp.begin() + temp.size();
  merge_sorted_blocks_reversed(orig, temp, levels, starting_level_1, num_levels_1);
  merge_sorted_blocks_reversed(orig, temp, levels, starting_level_2, num_levels_2);
  const uint32_t num_items_1 = levels[starting_level_1 + num_levels_1] - levels[starting_level_1];
  std::merge(
    std::make_move_iterator(chunk_begin), std::make_move_iterator(chunk_begin + num_items_1),
    std::make_move_iterator(chunk_begin + num_items_1), std::make_move_iterator(temp.end()),
    orig.begin() + levels[starting_level], compare_pair_by_first<C>()
  );
  temp.erase(chunk_begin, temp.end());
}

template <typename T, typename C, typename A>
void kll_quantile_calculator<T, C, A>::merge_sorted_blocks_reversed(Container& orig, Container& temp, const uint32_t* levels,
    uint8_t starting_level, uint8_t num_levels) {
  if (num_levels == 1) {
    std::move(orig.begin() + levels[starting_level], orig.begin() + levels[starting_level + 1], std::back_inserter(temp));
    return;
  }
  const uint8_t num_levels_1 = num_levels / 2;
  const uint8_t num_levels_2 = num_levels - num_levels_1;
  const uint8_t starting_level_1 = starting_level;
  const uint8_t starting_level_2 = starting_level + num_levels_1;
  merge_sorted_blocks_direct(orig, temp, levels, starting_level_1, num_levels_1);
  merge_sorted_blocks_direct(orig, temp, levels, starting_level_2, num_levels_2);
  std::merge(
    std::make_move_iterator(orig.begin() + levels[starting_level_1]),
    std::make_move_iterator(orig.begin() + levels[starting_level_1 + num_levels_1]),
    std::make_move_iterator(orig.begin() + levels[starting_level_2]),
    std::make_move_iterator(orig.begin() + levels[starting_level_2 + num_levels_2]),
    std::back_inserter(temp),
    compare_pair_by_first<C>()
  );
}

} /* namespace datasketches */

#endif // KLL_QUANTILE_CALCULATOR_IMPL_HPP_
