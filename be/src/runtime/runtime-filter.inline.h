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


#ifndef IMPALA_RUNTIME_RUNTIME_FILTER_INLINE_H
#define IMPALA_RUNTIME_RUNTIME_FILTER_INLINE_H

#include "runtime/runtime-filter.h"

#include <boost/thread.hpp>

#include "runtime/raw-value.inline.h"
#include "util/bloom-filter.h"
#include "util/min-max-filter.h"
#include "util/time.h"

namespace impala {

inline const RuntimeFilter* RuntimeFilterBank::GetRuntimeFilter(int32_t filter_id) {
  boost::lock_guard<boost::mutex> l(runtime_filter_lock_);
  RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
  if (it == consumed_filters_.end()) return NULL;
  return it->second;
}

inline void RuntimeFilter::SetFilter(
    BloomFilter* bloom_filter, MinMaxFilter* min_max_filter) {
  DCHECK(bloom_filter_.Load() == nullptr && min_max_filter_.Load() == nullptr);
  if (is_bloom_filter()) {
    bloom_filter_.Store(bloom_filter);
  } else {
    DCHECK(is_min_max_filter());
    min_max_filter_.Store(min_max_filter);
  }
  arrival_time_.Store(MonotonicMillis());
}

inline bool RuntimeFilter::AlwaysTrue() const {
  if (is_bloom_filter()) {
    return HasFilter() && bloom_filter_.Load() == BloomFilter::ALWAYS_TRUE_FILTER;
  } else {
    DCHECK(is_min_max_filter());
    return HasFilter() && min_max_filter_.Load()->AlwaysTrue();
  }
}

inline bool RuntimeFilter::AlwaysFalse() const {
  if (is_bloom_filter()) {
    return bloom_filter_.Load() != BloomFilter::ALWAYS_TRUE_FILTER
        && bloom_filter_.Load()->AlwaysFalse();
  } else {
    DCHECK(is_min_max_filter());
    return min_max_filter_.Load() != nullptr && min_max_filter_.Load()->AlwaysFalse();
  }
}

}

#endif
