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

#ifndef KLL_QUANTILE_CALCULATOR_HPP_
#define KLL_QUANTILE_CALCULATOR_HPP_

#include <memory>

namespace datasketches {

template <typename T, typename C, typename A>
class kll_quantile_calculator {
  public:
    // assumes that all levels are sorted including level 0
    kll_quantile_calculator(const T* items, const uint32_t* levels, uint8_t num_levels, uint64_t n, const A& allocator);
    T get_quantile(double fraction) const;

  private:
    using AllocU32 = typename std::allocator_traits<A>::template rebind_alloc<uint32_t>;
    using vector_u32 = std::vector<uint32_t, AllocU32>;
    using Entry = std::pair<T, uint64_t>;
    using AllocEntry = typename std::allocator_traits<A>::template rebind_alloc<Entry>;
    using Container = std::vector<Entry, AllocEntry>;
    uint64_t n_;
    vector_u32 levels_;
    Container entries_;

    void populate_from_sketch(const T* items, const uint32_t* levels, uint8_t num_levels);
    T approximately_answer_positional_query(uint64_t pos) const;
    void convert_to_preceding_cummulative();
    uint32_t chunk_containing_pos(uint64_t pos) const;
    uint32_t search_for_chunk_containing_pos(uint64_t pos, uint32_t l, uint32_t r) const;
    static void merge_sorted_blocks(Container& entries, const uint32_t* levels, uint8_t num_levels, uint32_t num_items);
    static void merge_sorted_blocks_direct(Container& orig, Container& temp, const uint32_t* levels, uint8_t starting_level, uint8_t num_levels);
    static void merge_sorted_blocks_reversed(Container& orig, Container& temp, const uint32_t* levels, uint8_t starting_level, uint8_t num_levels);
    static uint64_t pos_of_phi(double phi, uint64_t n);

    template<typename Comparator>
    struct compare_pair_by_first {
      template<typename Entry1, typename Entry2>
      bool operator()(Entry1&& a, Entry2&& b) const {
        return Comparator()(std::forward<Entry1>(a).first, std::forward<Entry2>(b).first);
      }
    };
};

} /* namespace datasketches */

#include "kll_quantile_calculator_impl.hpp"

#endif // KLL_QUANTILE_CALCULATOR_HPP_
