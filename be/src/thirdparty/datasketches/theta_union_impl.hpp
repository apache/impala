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

#ifndef THETA_UNION_IMPL_HPP_
#define THETA_UNION_IMPL_HPP_

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
theta_union_alloc<A>::theta_union_alloc(uint64_t theta, update_theta_sketch_alloc<A>&& state):
is_empty_(true), theta_(theta), state_(std::move(state)) {}

template<typename A>
void theta_union_alloc<A>::update(const theta_sketch_alloc<A>& sketch) {
  if (sketch.is_empty()) return;
  if (sketch.get_seed_hash() != state_.get_seed_hash()) throw std::invalid_argument("seed hash mismatch");
  is_empty_ = false;
  if (sketch.get_theta64() < theta_) theta_ = sketch.get_theta64();
  if (sketch.is_ordered()) {
    for (auto hash: sketch) {
      if (hash >= theta_) break; // early stop
      state_.internal_update(hash);
    }
  } else {
    for (auto hash: sketch) if (hash < theta_) state_.internal_update(hash);
  }
  if (state_.get_theta64() < theta_) theta_ = state_.get_theta64();
}

template<typename A>
compact_theta_sketch_alloc<A> theta_union_alloc<A>::get_result(bool ordered) const {
  if (is_empty_) return state_.compact(ordered);
  const uint32_t nom_num_keys = 1 << state_.lg_nom_size_;
  if (theta_ >= state_.theta_ && state_.get_num_retained() <= nom_num_keys) return state_.compact(ordered);
  uint64_t theta = std::min(theta_, state_.get_theta64());
  vector_u64<A> keys(state_.get_num_retained());
  uint32_t num_keys = 0;
  for (auto key: state_) {
    if (key < theta) keys[num_keys++] = key;
  }
  if (num_keys > nom_num_keys) {
    std::nth_element(keys.begin(), keys.begin() + nom_num_keys, keys.begin() + num_keys);
    theta = keys[nom_num_keys];
    num_keys = nom_num_keys;
  }
  if (num_keys != state_.get_num_retained()) {
    keys.resize(num_keys);
  }
  if (ordered) std::sort(keys.begin(), keys.end());
  return compact_theta_sketch_alloc<A>(false, theta, std::move(keys), state_.get_seed_hash(), ordered);
}

// builder

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_lg_k(uint8_t lg_k) {
  sketch_builder.set_lg_k(lg_k);
  return *this;
}

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_resize_factor(resize_factor rf) {
  sketch_builder.set_resize_factor(rf);
  return *this;
}

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_p(float p) {
  sketch_builder.set_p(p);
  return *this;
}

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_seed(uint64_t seed) {
  sketch_builder.set_seed(seed);
  return *this;
}

template<typename A>
theta_union_alloc<A> theta_union_alloc<A>::builder::build() const {
  update_theta_sketch_alloc<A> sketch = sketch_builder.build();
  return theta_union_alloc(sketch.get_theta64(), std::move(sketch));
}

} /* namespace datasketches */

# endif
