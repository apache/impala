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

#ifndef THETA_SKETCH_IMPL_HPP_
#define THETA_SKETCH_IMPL_HPP_

#include <sstream>
#include <vector>

#include "serde.hpp"
#include "binomial_bounds.hpp"
#include "theta_helpers.hpp"

namespace datasketches {

template<typename A>
bool theta_sketch_alloc<A>::is_estimation_mode() const {
  return get_theta64() < theta_constants::MAX_THETA && !is_empty();
}

template<typename A>
double theta_sketch_alloc<A>::get_theta() const {
  return static_cast<double>(get_theta64()) / theta_constants::MAX_THETA;
}

template<typename A>
double theta_sketch_alloc<A>::get_estimate() const {
  return get_num_retained() / get_theta();
}

template<typename A>
double theta_sketch_alloc<A>::get_lower_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_lower_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename A>
double theta_sketch_alloc<A>::get_upper_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_upper_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename A>
string<A> theta_sketch_alloc<A>::to_string(bool detail) const {
  ostrstream os;
  os << "### Theta sketch summary:" << std::endl;
  os << "   num retained entries : " << get_num_retained() << std::endl;
  os << "   seed hash            : " << get_seed_hash() << std::endl;
  os << "   empty?               : " << (is_empty() ? "true" : "false") << std::endl;
  os << "   ordered?             : " << (is_ordered() ? "true" : "false") << std::endl;
  os << "   estimation mode?     : " << (is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << get_theta64() << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  print_specifics(os);
  os << "### End sketch summary" << std::endl;
  if (detail) {
    os << "### Retained entries" << std::endl;
    for (const auto& hash: *this) {
      os << hash << std::endl;
    }
    os << "### End retained entries" << std::endl;
  }
  return os.str();
}

// update sketch

template<typename A>
update_theta_sketch_alloc<A>::update_theta_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf,
    uint64_t theta, uint64_t seed, const A& allocator):
table_(lg_cur_size, lg_nom_size, rf, theta, seed, allocator)
{}

template<typename A>
A update_theta_sketch_alloc<A>::get_allocator() const {
  return table_.allocator_;
}

template<typename A>
bool update_theta_sketch_alloc<A>::is_empty() const {
  return table_.is_empty_;
}

template<typename A>
bool update_theta_sketch_alloc<A>::is_ordered() const {
  return false;
}

template<typename A>
uint64_t update_theta_sketch_alloc<A>::get_theta64() const {
  return table_.theta_;
}

template<typename A>
uint32_t update_theta_sketch_alloc<A>::get_num_retained() const {
  return table_.num_entries_;
}

template<typename A>
uint16_t update_theta_sketch_alloc<A>::get_seed_hash() const {
  return compute_seed_hash(table_.seed_);
}

template<typename A>
uint8_t update_theta_sketch_alloc<A>::get_lg_k() const {
  return table_.lg_nom_size_;
}

template<typename A>
auto update_theta_sketch_alloc<A>::get_rf() const -> resize_factor {
  return table_.rf_;
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint64_t value) {
  update(&value, sizeof(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int64_t value) {
  update(&value, sizeof(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint32_t value) {
  update(static_cast<int32_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int32_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint16_t value) {
  update(static_cast<int16_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int16_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint8_t value) {
  update(static_cast<int8_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int8_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(double value) {
  update(canonical_double(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(float value) {
  update(static_cast<double>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(const std::string& value) {
  if (value.empty()) return;
  update(value.c_str(), value.length());
}

template<typename A>
void update_theta_sketch_alloc<A>::update(const void* data, size_t length) {
  const uint64_t hash = table_.hash_and_screen(data, length);
  if (hash == 0) return;
  auto result = table_.find(hash);
  if (!result.second) {
    table_.insert(result.first, hash);
  }
}

template<typename A>
void update_theta_sketch_alloc<A>::trim() {
  table_.trim();
}

template<typename A>
auto update_theta_sketch_alloc<A>::begin() -> iterator {
  return iterator(table_.entries_, 1 << table_.lg_cur_size_, 0);
}

template<typename A>
auto update_theta_sketch_alloc<A>::end() -> iterator {
  return iterator(nullptr, 0, 1 << table_.lg_cur_size_);
}

template<typename A>
auto update_theta_sketch_alloc<A>::begin() const -> const_iterator {
  return const_iterator(table_.entries_, 1 << table_.lg_cur_size_, 0);
}

template<typename A>
auto update_theta_sketch_alloc<A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, 1 << table_.lg_cur_size_);
}

template<typename A>
compact_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::compact(bool ordered) const {
  return compact_theta_sketch_alloc<A>(*this, ordered);
}

template<typename A>
void update_theta_sketch_alloc<A>::print_specifics(ostrstream& os) const {
  os << "   lg nominal size      : " << static_cast<int>(table_.lg_nom_size_) << std::endl;
  os << "   lg current size      : " << static_cast<int>(table_.lg_cur_size_) << std::endl;
  os << "   resize factor        : " << (1 << table_.rf_) << std::endl;
}

// builder

template<typename A>
update_theta_sketch_alloc<A>::builder::builder(const A& allocator): theta_base_builder<builder, A>(allocator) {}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::builder::build() const {
  return update_theta_sketch_alloc(this->starting_lg_size(), this->lg_k_, this->rf_, this->starting_theta(), this->seed_, this->allocator_);
}

// compact sketch

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(const Base& other, bool ordered):
is_empty_(other.is_empty()),
is_ordered_(other.is_ordered() || ordered),
seed_hash_(other.get_seed_hash()),
theta_(other.get_theta64()),
entries_(other.get_allocator())
{
  entries_.reserve(other.get_num_retained());
  std::copy(other.begin(), other.end(), std::back_inserter(entries_));
  if (ordered && !other.is_ordered()) std::sort(entries_.begin(), entries_.end());
}

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta,
    std::vector<uint64_t, A>&& entries):
is_empty_(is_empty),
is_ordered_(is_ordered),
seed_hash_(seed_hash),
theta_(theta),
entries_(std::move(entries))
{}

template<typename A>
A compact_theta_sketch_alloc<A>::get_allocator() const {
  return entries_.get_allocator();
}

template<typename A>
bool compact_theta_sketch_alloc<A>::is_empty() const {
  return is_empty_;
}

template<typename A>
bool compact_theta_sketch_alloc<A>::is_ordered() const {
  return is_ordered_;
}

template<typename A>
uint64_t compact_theta_sketch_alloc<A>::get_theta64() const {
  return theta_;
}

template<typename A>
uint32_t compact_theta_sketch_alloc<A>::get_num_retained() const {
  return entries_.size();
}

template<typename A>
uint16_t compact_theta_sketch_alloc<A>::get_seed_hash() const {
  return seed_hash_;
}

template<typename A>
auto compact_theta_sketch_alloc<A>::begin() -> iterator {
  return iterator(entries_.data(), entries_.size(), 0);
}

template<typename A>
auto compact_theta_sketch_alloc<A>::end() -> iterator {
  return iterator(nullptr, 0, entries_.size());
}

template<typename A>
auto compact_theta_sketch_alloc<A>::begin() const -> const_iterator {
  return const_iterator(entries_.data(), entries_.size(), 0);
}

template<typename A>
auto compact_theta_sketch_alloc<A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, entries_.size());
}

template<typename A>
void compact_theta_sketch_alloc<A>::print_specifics(ostrstream&) const {}

template<typename A>
void compact_theta_sketch_alloc<A>::serialize(std::ostream& os) const {
  const bool is_single_item = entries_.size() == 1 && !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() || is_single_item ? 1 : this->is_estimation_mode() ? 3 : 2;
  os.write(reinterpret_cast<const char*>(&preamble_longs), sizeof(preamble_longs));
  const uint8_t serial_version = SERIAL_VERSION;
  os.write(reinterpret_cast<const char*>(&serial_version), sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  os.write(reinterpret_cast<const char*>(&type), sizeof(type));
  const uint16_t unused16 = 0;
  os.write(reinterpret_cast<const char*>(&unused16), sizeof(unused16));
  const uint8_t flags_byte(
    (1 << flags::IS_COMPACT) |
    (1 << flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << flags::IS_ORDERED : 0)
  );
  os.write(reinterpret_cast<const char*>(&flags_byte), sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  os.write(reinterpret_cast<const char*>(&seed_hash), sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      const uint32_t num_entries = entries_.size();
      os.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
      const uint32_t unused32 = 0;
      os.write(reinterpret_cast<const char*>(&unused32), sizeof(unused32));
      if (this->is_estimation_mode()) {
        os.write(reinterpret_cast<const char*>(&(this->theta_)), sizeof(uint64_t));
      }
    }
    os.write(reinterpret_cast<const char*>(entries_.data()), entries_.size() * sizeof(uint64_t));
  }
}

template<typename A>
auto compact_theta_sketch_alloc<A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const bool is_single_item = entries_.size() == 1 && !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() || is_single_item ? 1 : this->is_estimation_mode() ? 3 : 2;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs
      + sizeof(uint64_t) * entries_.size();
  vector_bytes bytes(size, 0, entries_.get_allocator());
  uint8_t* ptr = bytes.data() + header_size_bytes;

  ptr += copy_to_mem(&preamble_longs, ptr, sizeof(preamble_longs));
  const uint8_t serial_version = SERIAL_VERSION;
  ptr += copy_to_mem(&serial_version, ptr, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  ptr += copy_to_mem(&type, ptr, sizeof(type));
  const uint16_t unused16 = 0;
  ptr += copy_to_mem(&unused16, ptr, sizeof(unused16));
  const uint8_t flags_byte(
    (1 << flags::IS_COMPACT) |
    (1 << flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << flags::IS_ORDERED : 0)
  );
  ptr += copy_to_mem(&flags_byte, ptr, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  ptr += copy_to_mem(&seed_hash, ptr, sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      const uint32_t num_entries = entries_.size();
      ptr += copy_to_mem(&num_entries, ptr, sizeof(num_entries));
      const uint32_t unused32 = 0;
      ptr += copy_to_mem(&unused32, ptr, sizeof(unused32));
      if (this->is_estimation_mode()) {
        ptr += copy_to_mem(&theta_, ptr, sizeof(uint64_t));
      }
    }
    ptr += copy_to_mem(entries_.data(), ptr, entries_.size() * sizeof(uint64_t));
  }
  return bytes;
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize(std::istream& is, uint64_t seed, const A& allocator) {
  uint8_t preamble_longs;
  is.read(reinterpret_cast<char*>(&preamble_longs), sizeof(preamble_longs));
  uint8_t serial_version;
  is.read(reinterpret_cast<char*>(&serial_version), sizeof(serial_version));
  uint8_t type;
  is.read(reinterpret_cast<char*>(&type), sizeof(type));
  uint16_t unused16;
  is.read(reinterpret_cast<char*>(&unused16), sizeof(unused16));
  uint8_t flags_byte;
  is.read(reinterpret_cast<char*>(&flags_byte), sizeof(flags_byte));
  uint16_t seed_hash;
  is.read(reinterpret_cast<char*>(&seed_hash), sizeof(seed_hash));
  checker<true>::check_sketch_type(type, SKETCH_TYPE);
  checker<true>::check_serial_version(serial_version, SERIAL_VERSION);
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  if (!is_empty) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));

  uint64_t theta = theta_constants::MAX_THETA;
  uint32_t num_entries = 0;
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_entries = 1;
    } else {
      is.read(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));
      uint32_t unused32;
      is.read(reinterpret_cast<char*>(&unused32), sizeof(unused32));
      if (preamble_longs > 2) {
        is.read(reinterpret_cast<char*>(&theta), sizeof(theta));
      }
    }
  }
  std::vector<uint64_t, A> entries(num_entries, 0, allocator);
  if (!is_empty) is.read(reinterpret_cast<char*>(entries.data()), sizeof(uint64_t) * entries.size());

  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return compact_theta_sketch_alloc(is_empty, is_ordered, seed_hash, theta, std::move(entries));
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize(const void* bytes, size_t size, uint64_t seed, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* base = ptr;
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, &preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  ptr += copy_from_mem(ptr, &type, sizeof(type));
  uint16_t unused16;
  ptr += copy_from_mem(ptr, &unused16, sizeof(unused16));
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  ptr += copy_from_mem(ptr, &seed_hash, sizeof(seed_hash));
  checker<true>::check_sketch_type(type, SKETCH_TYPE);
  checker<true>::check_serial_version(serial_version, SERIAL_VERSION);
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  if (!is_empty) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));

  uint64_t theta = theta_constants::MAX_THETA;
  uint32_t num_entries = 0;
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_entries = 1;
    } else {
      ensure_minimum_memory(size, 8); // read the first prelong before this method
      ptr += copy_from_mem(ptr, &num_entries, sizeof(num_entries));
      uint32_t unused32;
      ptr += copy_from_mem(ptr, &unused32, sizeof(unused32));
      if (preamble_longs > 2) {
        ensure_minimum_memory(size, (preamble_longs - 1) << 3);
        ptr += copy_from_mem(ptr, &theta, sizeof(theta));
      }
    }
  }
  const size_t entries_size_bytes = sizeof(uint64_t) * num_entries;
  check_memory_size(ptr - base + entries_size_bytes, size);
  std::vector<uint64_t, A> entries(num_entries, 0, allocator);
  if (!is_empty) ptr += copy_from_mem(ptr, entries.data(), entries_size_bytes);

  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  return compact_theta_sketch_alloc(is_empty, is_ordered, seed_hash, theta, std::move(entries));
}

} /* namespace datasketches */

#endif

