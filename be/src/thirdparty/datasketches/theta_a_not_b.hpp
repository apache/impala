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

#ifndef THETA_A_NOT_B_HPP_
#define THETA_A_NOT_B_HPP_

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
class theta_a_not_b_alloc {
public:
  /**
   * Creates an instance of the a-not-b operation (set difference) with a given has seed.
   * @param seed hash seed
   */
  explicit theta_a_not_b_alloc(uint64_t seed = DEFAULT_SEED);

  /**
   * Computes the a-not-b set operation given two sketches.
   * @return the result of a-not-b
   */
  compact_theta_sketch_alloc<A> compute(const theta_sketch_alloc<A>& a, const theta_sketch_alloc<A>& b, bool ordered = true) const;

private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  uint16_t seed_hash_;

  class less_than {
  public:
    explicit less_than(uint64_t value): value(value) {}
    bool operator()(uint64_t value) const { return value < this->value; }
  private:
    uint64_t value;
  };
};

// alias with default allocator for convenience
typedef theta_a_not_b_alloc<std::allocator<void>> theta_a_not_b;

} /* namespace datasketches */

#include "theta_a_not_b_impl.hpp"

# endif
