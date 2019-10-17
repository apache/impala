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

#include "runtime/runtime-filter.inline.h"

#include "util/time.h"

#include "common/names.h"

using namespace impala;

const char* RuntimeFilter::LLVM_CLASS_NAME = "class.impala::RuntimeFilter";

void RuntimeFilter::SetFilter(BloomFilter* bloom_filter, MinMaxFilter* min_max_filter) {
  DCHECK(!HasFilter()) << "SetFilter() should not be called multiple times.";
  DCHECK(bloom_filter_.Load() == nullptr && min_max_filter_.Load() == nullptr);
  if (arrival_time_.Load() != 0) return; // The filter may already have been cancelled.
  if (is_bloom_filter()) {
    bloom_filter_.Store(bloom_filter);
  } else {
    DCHECK(is_min_max_filter());
    min_max_filter_.Store(min_max_filter);
  }
  arrival_time_.Store(MonotonicMillis());
  has_filter_.Store(true);
  arrival_cv_.NotifyAll();
}

void RuntimeFilter::Cancel() {
  if (arrival_time_.Load() != 0) return;
  arrival_time_.Store(MonotonicMillis());
  arrival_cv_.NotifyAll();
}

bool RuntimeFilter::WaitForArrival(int32_t timeout_ms) const {
  unique_lock<mutex> l(arrival_mutex_);
  while (arrival_time_.Load() == 0) {
    int64_t ms_since_registration = MonotonicMillis() - registration_time_;
    int64_t ms_remaining = timeout_ms - ms_since_registration;
    if (ms_remaining <= 0) break;
    arrival_cv_.WaitFor(l, ms_remaining * MICROS_PER_MILLI);
  }
  return arrival_time_.Load() != 0;
}
