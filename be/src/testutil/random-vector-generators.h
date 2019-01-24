// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <limits.h>
#include <random>
#include <type_traits>

namespace impala {

/// Generates a vector of numbers with the given length, consisting of random elements.
/// `NUM_T` must either be an integral or a floating point type.
/// The distribution is uniform across the range [min, max] (both inclusive).
template <typename NUM_T, typename Generator = std::mt19937>
std::vector<NUM_T> RandomNumberVecMinMax(Generator& gen, const int length,
    const NUM_T min, const NUM_T max) {
  static_assert(std::is_integral<NUM_T>::value || std::is_floating_point<NUM_T>::value,
      "An integral or floating point type is needed.");

  using Dist = std::conditional_t<std::is_integral<NUM_T>::value,
    std::uniform_int_distribution<NUM_T>,
    std::uniform_real_distribution<NUM_T>>;

  Dist dist(min, max);

  std::vector<NUM_T> vec(length);
  std::generate(vec.begin(), vec.end(), [&gen, &dist] () {
      return dist(gen);
      });

  return vec;
}

/// Generates a vector of numbers with the given length, consisting of random elements.
/// `NUM_T` must either be an integral or a floating point type.
/// The distribution is uniform across all values of the NUM_T.
template <typename NUM_T, typename Generator = std::mt19937>
std::vector<NUM_T> RandomNumberVec(Generator& gen, const int length) {
  return RandomNumberVecMinMax<NUM_T, Generator>(gen, length,
      std::numeric_limits<NUM_T>::min(), std::numeric_limits<NUM_T>::max());
}

/// Generates a vector of strings with the given length. The elements are randomly
/// generated strings. The length of the strings is a random number between
/// `min_str_length` and `max_str_length` (both inclusive).
template <typename Generator = std::mt19937>
std::vector<std::string> RandomStrVec(Generator& gen, const int length,
    const int max_str_length, const int min_str_length = 0) {
  std::uniform_int_distribution<int> length_dist(0, max_str_length);
  std::uniform_int_distribution<char> letter_dist('a', 'z');

  std::vector<std::string> vec(length);
  std::generate(vec.begin(), vec.end(), [&] () {
      int str_length = length_dist(gen);
      std::string s;

      for (int i = 0; i < str_length; i++) {
        s.push_back(letter_dist(gen));
      }

      return s;
      });

  return vec;
}

/// Generates a vector of `TimestampValue`s with the given length. The elements are random
/// timestamps with uniform distribution on the valid range.
template <typename Generator = std::mt19937>
std::vector<TimestampValue> RandomTimestampVec(Generator& gen, const int length) {
  const TimestampValue min_date =
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00");
  int64_t min_millis;
  bool min_success = min_date.FloorUtcToUnixTimeMillis(&min_millis);
  DCHECK(min_success);

  const TimestampValue max_date =
      TimestampValue::ParseSimpleDateFormat("9999-12-31 23:59:59");
  int64_t max_millis;
  bool max_success = max_date.FloorUtcToUnixTimeMillis(&max_millis);
  DCHECK(max_success);

  const std::vector<int64_t> unix_time_millis = RandomNumberVecMinMax<int64_t, Generator>(
      gen, length, min_millis, max_millis);
  std::vector<TimestampValue> timestamps(length);
  std::transform(unix_time_millis.begin(), unix_time_millis.end(), timestamps.begin(),
     TimestampValue::UtcFromUnixTimeMillis);

  return timestamps;
}

} // namespace impala
