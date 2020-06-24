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
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint32_t> AllocU32;
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  public:
    // assumes that all levels are sorted including level 0
    kll_quantile_calculator(const T* items, const uint32_t* levels, uint8_t num_levels, uint64_t n);
    ~kll_quantile_calculator();
    T get_quantile(double fraction) const;

  private:
    uint64_t n_;
    T* items_;
    uint64_t* weights_;
    uint32_t* levels_;
    uint8_t levels_size_;
    uint8_t num_levels_;

    void populate_from_sketch(const T* items, uint32_t num_items, const uint32_t* levels, uint8_t num_levels);
    T approximately_answer_positional_query(uint64_t pos) const;
    static void convert_to_preceding_cummulative(uint64_t* weights, uint32_t weights_size);
    static uint64_t pos_of_phi(double phi, uint64_t n);
    static uint32_t chunk_containing_pos(uint64_t* weights, uint32_t weights_size, uint64_t pos);
    static uint32_t search_for_chunk_containing_pos(const uint64_t* arr, uint64_t pos, uint32_t l, uint32_t r);
    static void blocky_tandem_merge_sort(T* items, uint64_t* weights, uint32_t num_items, const uint32_t* levels, uint8_t num_levels);
    static void blocky_tandem_merge_sort_recursion(T* items_src, uint64_t* weights_src, T* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level, uint8_t num_levels);
    static void tandem_merge(const T* items_src, const uint64_t* weights_src, T* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level_1, uint8_t num_levels_1, uint8_t starting_level_2, uint8_t num_levels_2);
};

} /* namespace datasketches */

#include "kll_quantile_calculator_impl.hpp"

#endif // KLL_QUANTILE_CALCULATOR_HPP_
