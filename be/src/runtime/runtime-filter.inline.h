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
#include "util/time.h"

namespace impala {

inline const RuntimeFilter* RuntimeFilterBank::GetRuntimeFilter(int32_t filter_id) {
  boost::lock_guard<boost::mutex> l(runtime_filter_lock_);
  RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
  if (it == consumed_filters_.end()) return NULL;
  return it->second;
}

inline void RuntimeFilter::SetBloomFilter(BloomFilter* bloom_filter) {
  DCHECK(bloom_filter_ == NULL);
  // TODO: Barrier required here to ensure compiler does not both inline and re-order
  // this assignment. Not an issue for correctness (as assignment is atomic), but
  // potentially confusing.
  bloom_filter_ = bloom_filter;
  arrival_time_ = MonotonicMillis();
}

inline bool RuntimeFilter::AlwaysTrue() const  {
  return HasBloomFilter() && bloom_filter_ == BloomFilter::ALWAYS_TRUE_FILTER;
}

inline bool RuntimeFilter::AlwaysFalse() const {
  return bloom_filter_ != BloomFilter::ALWAYS_TRUE_FILTER && bloom_filter_->AlwaysFalse();
}

}

#endif
