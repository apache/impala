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

#ifndef THETA_INTERSECTION_HPP_
#define THETA_INTERSECTION_HPP_

#include <memory>
#include <functional>
#include <climits>

#include "theta_sketch.hpp"
#include "common_defs.hpp"

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
class theta_intersection_alloc {
public:
  /**
   * Creates an instance of the intersection with a given hash seed.
   * @param seed hash seed
   */
  explicit theta_intersection_alloc(uint64_t seed = DEFAULT_SEED);

  /**
   * Updates the intersection with a given sketch.
   * The intersection can be viewed as starting from the "universe" set, and every update
   * can reduce the current set to leave the overlapping subset only.
   * @param sketch represents input set for the intersection
   */
  void update(const theta_sketch_alloc<A>& sketch);

  /**
   * Produces a copy of the current state of the intersection.
   * If update() was not called, the state is the infinite "universe",
   * which is considered an undefined state, and throws an exception.
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return the result of the intersection
   */
  compact_theta_sketch_alloc<A> get_result(bool ordered = true) const;

  /**
   * Returns true if the state of the intersection is defined (not infinite "universe").
   * @return true if the state is valid
   */
  bool has_result() const;

private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  bool is_valid_;
  bool is_empty_;
  uint64_t theta_;
  uint8_t lg_size_;
  vector_u64<A> keys_;
  uint32_t num_keys_;
  uint16_t seed_hash_;
};

// alias with default allocator for convenience
typedef theta_intersection_alloc<std::allocator<void>> theta_intersection;

} /* namespace datasketches */

#include "theta_intersection_impl.hpp"

# endif
