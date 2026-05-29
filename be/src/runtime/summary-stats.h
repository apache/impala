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

#include <cstdint>
#include <limits>

namespace impala {

/// Similar to SummaryStatsCounter without thread-safe support so don't need
/// to acquire locks.
struct SummaryStats {
  /// The total number of values seen so far.
  int32_t total_num_values_ = 0;

  /// Summary statistics of values seen so far.
  int64_t min_ = std::numeric_limits<int64_t>::max();
  int64_t max_ = std::numeric_limits<int64_t>::min();
  int64_t sum_ = 0;

  void UpdateCounter(int64_t new_value) {
    ++total_num_values_;
    sum_ += new_value;
    if (new_value < min_) min_ = new_value;
    if (new_value > max_) max_ = new_value;
  }
};

}
