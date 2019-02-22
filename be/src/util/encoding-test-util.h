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

#include <random>

namespace impala {

/// Generates a sequence that contains repeated and literal runs with random lengths.
/// Total length of the sequence is limited by 'max_run_length'. It only generates values
/// that can be represented on 'bit_width' size bits.
template<typename RandomEngine>
std::vector<int> MakeRandomSequence(RandomEngine& random_eng, int total_length,
    int max_run_length, int bit_width) {
  auto NextRunLength = [&]() {
    std::uniform_int_distribution<int> uni_dist(1, max_run_length);
    return uni_dist(random_eng);
  };
  auto IsNextRunRepeated = [&random_eng]() {
    std::uniform_int_distribution<int> uni_dist(0, 1);
    return uni_dist(random_eng) == 0;
  };
  auto NextVal = [bit_width](int val) {
    if (bit_width == CHAR_BIT * sizeof(int)) return val + 1;
    return (val + 1) % (1 << bit_width);
  };

  std::vector<int> ret;
  int run_length = 0;
  int val = 0;
  int is_repeated = false;
  while (ret.size() < total_length) {
    if (run_length == 0) {
      run_length = NextRunLength();
      is_repeated = IsNextRunRepeated();
      val = NextVal(val);
    }
    ret.push_back(val);
    if (!is_repeated) val = NextVal(val);
    --run_length;
  }
  return ret;
}

}
