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

#ifndef THETA_SKETCH_HPP_
#define THETA_SKETCH_HPP_

#include <memory>
#include <functional>
#include <climits>
#include <vector>

#include "common_defs.hpp"

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

// forward-declarations
template<typename A> class theta_sketch_alloc;
template<typename A> class update_theta_sketch_alloc;
template<typename A> class compact_theta_sketch_alloc;
template<typename A> class theta_union_alloc;
template<typename A> class theta_intersection_alloc;
template<typename A> class theta_a_not_b_alloc;

// for serialization as raw bytes
template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template<typename A> using vector_u8 = std::vector<uint8_t, AllocU8<A>>;

template<typename A>
class theta_sketch_alloc {
public:
  static const uint64_t MAX_THETA = LLONG_MAX; // signed max for compatibility with Java
  static const uint8_t SERIAL_VERSION = 3;

  virtual ~theta_sketch_alloc() = default;

  /**
   * @return true if this sketch represents an empty set (not the same as no retained entries!)
   */
  bool is_empty() const;

  /**
   * @return estimate of the distinct count of the input stream
   */
  double get_estimate() const;

  /**
   * Returns the approximate lower error bound given a number of standard deviations.
   * This parameter is similar to the number of standard deviations of the normal distribution
   * and corresponds to approximately 67%, 95% and 99% confidence intervals.
   * @param num_std_devs number of Standard Deviations (1, 2 or 3)
   * @return the lower bound
   */
  double get_lower_bound(uint8_t num_std_devs) const;

  /**
   * Returns the approximate upper error bound given a number of standard deviations.
   * This parameter is similar to the number of standard deviations of the normal distribution
   * and corresponds to approximately 67%, 95% and 99% confidence intervals.
   * @param num_std_devs number of Standard Deviations (1, 2 or 3)
   * @return the upper bound
   */
  double get_upper_bound(uint8_t num_std_devs) const;

  /**
   * @return true if the sketch is in estimation mode (as opposed to exact mode)
   */
  bool is_estimation_mode() const;

  /**
   * @return theta as a fraction from 0 to 1 (effective sampling rate)
   */
  double get_theta() const;

  /**
   * @return theta as a positive integer between 0 and LLONG_MAX
   */
  uint64_t get_theta64() const;

  /**
   * @return the number of retained entries in the sketch
   */
  virtual uint32_t get_num_retained() const = 0;

  virtual uint16_t get_seed_hash() const = 0;

  /**
   * @return true if retained entries are ordered
   */
  virtual bool is_ordered() const = 0;

  /**
   * Writes a human-readable summary of this sketch to a given stream
   * @param print_items if true include the list of items retained by the sketch
   */
  virtual string<A> to_string(bool print_items = false) const = 0;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  virtual void serialize(std::ostream& os) const = 0;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  typedef vector_u8<A> vector_bytes;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is an uninitialized space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   */
  virtual vector_bytes serialize(unsigned header_size_bytes = 0) const = 0;

  // This is a convenience alias for users
  // The type returned by the following deserialize methods
  // It is not possible to return instances of an abstract type, so this has to be a pointer
  typedef std::unique_ptr<theta_sketch_alloc<A>, std::function<void(theta_sketch_alloc<A>*)>> unique_ptr;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of a sketch as a unique_ptr
   */
  static unique_ptr deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED);

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of the sketch
   */
  static unique_ptr deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED);

  class const_iterator;

  /**
   * Iterator over hash values in this sketch.
   * @return begin iterator
   */
  virtual const_iterator begin() const = 0;

  /**
   * Iterator pointing past the valid range.
   * Not to be incremented or dereferenced.
   * @return end iterator
   */
  virtual const_iterator end() const = 0;

protected:
  enum flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED };

  bool is_empty_;
  uint64_t theta_;

  theta_sketch_alloc(bool is_empty, uint64_t theta);

  static uint16_t get_seed_hash(uint64_t seed);

  static void check_sketch_type(uint8_t actual, uint8_t expected);
  static void check_serial_version(uint8_t actual, uint8_t expected);
  static void check_seed_hash(uint16_t actual, uint16_t expected);

  friend theta_intersection_alloc<A>;
  friend theta_a_not_b_alloc<A>;
};

// update sketch

template<typename A> using AllocU64 = typename std::allocator_traits<A>::template rebind_alloc<uint64_t>;
template<typename A> using vector_u64 = std::vector<uint64_t, AllocU64<A>>;

template<typename A>
class update_theta_sketch_alloc: public theta_sketch_alloc<A> {
public:
  class builder;
  enum resize_factor { X1, X2, X4, X8 };
  static const uint8_t SKETCH_TYPE = 2;

  // No constructor here. Use builder instead.

  virtual ~update_theta_sketch_alloc() = default;

  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;
  virtual bool is_ordered() const;
  virtual string<A> to_string(bool print_items = false) const;
  virtual void serialize(std::ostream& os) const;
  typedef vector_u8<A> vector_bytes; // alias for users
  // header space is reserved, but not initialized
  virtual vector_bytes serialize(unsigned header_size_bytes = 0) const;

  /**
   * Update this sketch with a given string.
   * @param value string to update the sketch with
   */
  void update(const std::string& value);

  /**
   * Update this sketch with a given unsigned 64-bit integer.
   * @param value uint64_t to update the sketch with
   */
  void update(uint64_t value);

  /**
   * Update this sketch with a given signed 64-bit integer.
   * @param value int64_t to update the sketch with
   */
  void update(int64_t value);

  /**
   * Update this sketch with a given unsigned 32-bit integer.
   * For compatibility with Java implementation.
   * @param value uint32_t to update the sketch with
   */
  void update(uint32_t value);

  /**
   * Update this sketch with a given signed 32-bit integer.
   * For compatibility with Java implementation.
   * @param value int32_t to update the sketch with
   */
  void update(int32_t value);

  /**
   * Update this sketch with a given unsigned 16-bit integer.
   * For compatibility with Java implementation.
   * @param value uint16_t to update the sketch with
   */
  void update(uint16_t value);

  /**
   * Update this sketch with a given signed 16-bit integer.
   * For compatibility with Java implementation.
   * @param value int16_t to update the sketch with
   */
  void update(int16_t value);

  /**
   * Update this sketch with a given unsigned 8-bit integer.
   * For compatibility with Java implementation.
   * @param value uint8_t to update the sketch with
   */
  void update(uint8_t value);

  /**
   * Update this sketch with a given signed 8-bit integer.
   * For compatibility with Java implementation.
   * @param value int8_t to update the sketch with
   */
  void update(int8_t value);

  /**
   * Update this sketch with a given double-precision floating point value.
   * For compatibility with Java implementation.
   * @param value double to update the sketch with
   */
  void update(double value);

  /**
   * Update this sketch with a given floating point value.
   * For compatibility with Java implementation.
   * @param value float to update the sketch with
   */
  void update(float value);

  /**
   * Update this sketch with given data of any type.
   * This is a "universal" update that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing sketches between C++ environment and Java environment.
   * Otherwise two sketches that should represent overlapping sets will be disjoint
   * For instance, for signed 32-bit values call update(int32_t) method above,
   * which does widening conversion to int64_t, if compatibility with Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   */
  void update(const void* data, unsigned length);

  /**
   * Remove retained entries in excess of the nominal size k (if any)
   */
  void trim();

  /**
   * Converts this sketch to a compact sketch (ordered or unordered).
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return compact sketch
   */
  compact_theta_sketch_alloc<A> compact(bool ordered = true) const;

  virtual typename theta_sketch_alloc<A>::const_iterator begin() const;
  virtual typename theta_sketch_alloc<A>::const_iterator end() const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of a sketch
   */
  static update_theta_sketch_alloc<A> deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED);

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of the sketch
   */
  static update_theta_sketch_alloc<A> deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED);

private:
  // resize threshold = 0.5 tuned for speed
  static constexpr double RESIZE_THRESHOLD = 0.5;
  // hash table rebuild threshold = 15/16
  static constexpr double REBUILD_THRESHOLD = 15.0 / 16.0;

  static constexpr uint8_t STRIDE_HASH_BITS = 7;
  static constexpr uint32_t STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  uint8_t lg_cur_size_;
  uint8_t lg_nom_size_;
  vector_u64<A> keys_;
  uint32_t num_keys_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;
  uint32_t capacity_;

  // for builder
  update_theta_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed);

  // for deserialize
  update_theta_sketch_alloc(bool is_empty, uint64_t theta, uint8_t lg_cur_size, uint8_t lg_nom_size, vector_u64<A>&& keys, uint32_t num_keys, resize_factor rf, float p, uint64_t seed);

  void resize();
  void rebuild();

  friend theta_union_alloc<A>;
  void internal_update(uint64_t hash);

  friend theta_intersection_alloc<A>;
  friend theta_a_not_b_alloc<A>;
  static inline uint32_t get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size);
  static inline uint32_t get_stride(uint64_t hash, uint8_t lg_size);
  static bool hash_search_or_insert(uint64_t hash, uint64_t* table, uint8_t lg_size);
  static bool hash_search(uint64_t hash, const uint64_t* table, uint8_t lg_size);

  friend theta_sketch_alloc<A>;
  static update_theta_sketch_alloc<A> internal_deserialize(std::istream& is, resize_factor rf, uint8_t lg_cur_size, uint8_t lg_nom_size, uint8_t flags_byte, uint64_t seed);
  static update_theta_sketch_alloc<A> internal_deserialize(const void* bytes, size_t size, resize_factor rf, uint8_t lg_cur_size, uint8_t lg_nom_size, uint8_t flags_byte, uint64_t seed);
};

// compact sketch

template<typename A>
class compact_theta_sketch_alloc: public theta_sketch_alloc<A> {
public:
  static const uint8_t SKETCH_TYPE = 3;

  // No constructor here.
  // Instances of this type can be obtained:
  // - by compacting an update_theta_sketch
  // - as a result of a set operation
  // - by deserializing a previously serialized compact sketch

  compact_theta_sketch_alloc(const theta_sketch_alloc<A>& other, bool ordered);
  virtual ~compact_theta_sketch_alloc() = default;

  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;
  virtual bool is_ordered() const;
  virtual string<A> to_string(bool print_items = false) const;
  virtual void serialize(std::ostream& os) const;
  typedef vector_u8<A> vector_bytes; // alias for users
  // header space is reserved, but not initialized
  virtual vector_bytes serialize(unsigned header_size_bytes = 0) const;

  virtual typename theta_sketch_alloc<A>::const_iterator begin() const;
  virtual typename theta_sketch_alloc<A>::const_iterator end() const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of a sketch
   */
  static compact_theta_sketch_alloc<A> deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED);

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of the sketch
   */
  static compact_theta_sketch_alloc<A> deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED);

private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;

  vector_u64<A> keys_;
  uint16_t seed_hash_;
  bool is_ordered_;

  friend theta_sketch_alloc<A>;
  friend update_theta_sketch_alloc<A>;
  friend theta_union_alloc<A>;
  friend theta_intersection_alloc<A>;
  friend theta_a_not_b_alloc<A>;
  compact_theta_sketch_alloc(bool is_empty, uint64_t theta, vector_u64<A>&& keys, uint16_t seed_hash, bool is_ordered);
  static compact_theta_sketch_alloc<A> internal_deserialize(std::istream& is, uint8_t preamble_longs, uint8_t flags_byte, uint16_t seed_hash);
  static compact_theta_sketch_alloc<A> internal_deserialize(const void* bytes, size_t size, uint8_t preamble_longs, uint8_t flags_byte, uint16_t seed_hash);
};

// builder

template<typename A>
class update_theta_sketch_alloc<A>::builder {
public:
  static const uint8_t MIN_LG_K = 5;
  static const uint8_t DEFAULT_LG_K = 12;
  static const resize_factor DEFAULT_RESIZE_FACTOR = X8;

  /**
   * Creates and instance of the builder with default parameters.
   */
  builder();

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
   * This is to create an instance of the sketch with predefined parameters.
   * @return and instance of the sketch
   */
  update_theta_sketch_alloc<A> build() const;

private:
  uint8_t lg_k_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;

  static uint8_t starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf);
};

// iterator
template<typename A>
class theta_sketch_alloc<A>::const_iterator: public std::iterator<std::input_iterator_tag, uint64_t> {
public:
  const_iterator& operator++();
  const_iterator operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  uint64_t operator*() const;

private:
  const uint64_t* keys_;
  uint32_t size_;
  uint32_t index_;
  const_iterator(const uint64_t* keys, uint32_t size, uint32_t index);
  friend class update_theta_sketch_alloc<A>;
  friend class compact_theta_sketch_alloc<A>;
};


// aliases with default allocator for convenience
typedef theta_sketch_alloc<std::allocator<void>> theta_sketch;
typedef update_theta_sketch_alloc<std::allocator<void>> update_theta_sketch;
typedef compact_theta_sketch_alloc<std::allocator<void>> compact_theta_sketch;

} /* namespace datasketches */

#include "theta_sketch_impl.hpp"

#endif
