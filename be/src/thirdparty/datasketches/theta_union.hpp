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

#ifndef THETA_UNION_HPP_
#define THETA_UNION_HPP_

#include <memory>
#include <functional>
#include <climits>

#include "theta_sketch.hpp"

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
class theta_union_alloc {
public:
  class builder;

  // No constructor here. Use builder instead.

  /**
   * This method is to update the union with a given sketch
   * @param sketch to update the union with
   */
  void update(const theta_sketch_alloc<A>& sketch);

  /**
   * This method produces a copy of the current state of the union as a compact sketch.
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return the result of the union
   */
  compact_theta_sketch_alloc<A> get_result(bool ordered = true) const;

private:
  bool is_empty_;
  uint64_t theta_;
  update_theta_sketch_alloc<A> state_;

  // for builder
  theta_union_alloc(uint64_t theta, update_theta_sketch_alloc<A>&& state);
};

// builder

template<typename A>
class theta_union_alloc<A>::builder {
public:
  typedef typename update_theta_sketch_alloc<A>::resize_factor resize_factor;

  /**
   * Set log2(k), where k is a nominal number of entries in the sketch
   * @param lg_k base 2 logarithm of nominal number of entries
   * @return this builder
   */
  builder& set_lg_k(uint8_t lg_k);

  /**
   * Set resize factor for the internal hash table (defaults to 8)
   * @param rf resize factor
   * @return this builder
   */
  builder& set_resize_factor(resize_factor rf);

  /**
   * Set sampling probability (initial theta). The default is 1, so the sketch retains
   * all entries until it reaches the limit, at which point it goes into the estimation mode
   * and reduces the effective sampling probability (theta) as necessary.
   * @param p sampling probability
   * @return this builder
   */
  builder& set_p(float p);

  /**
   * Set the seed for the hash function. Should be used carefully if needed.
   * Sketches produced with different seed are not compatible
   * and cannot be mixed in set operations.
   * @param seed hash seed
   * @return this builder
   */
  builder& set_seed(uint64_t seed);

  /**
   * This is to create an instance of the union with predefined parameters.
   * @return and instance of the union
   */
  theta_union_alloc<A> build() const;

private:
  typename update_theta_sketch_alloc<A>::builder sketch_builder;
};

// alias with default allocator for convenience
typedef theta_union_alloc<std::allocator<void>> theta_union;

} /* namespace datasketches */

#include "theta_union_impl.hpp"

# endif
