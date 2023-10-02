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
#include "util/network-util.h"
#include "util/time.h"

#include "common/names.h"

using namespace impala;

const char* RuntimeFilter::LLVM_CLASS_NAME = "class.impala::RuntimeFilter";

void RuntimeFilter::SetFilter(BloomFilter* bloom_filter, MinMaxFilter* min_max_filter,
    InListFilter* in_list_filter) {
  {
    unique_lock<mutex> l(arrival_mutex_);
    DCHECK(!HasFilter()) << "SetFilter() should not be called multiple times.";
    DCHECK(bloom_filter_.Load() == nullptr);
    DCHECK(min_max_filter_.Load() == nullptr);
    DCHECK(in_list_filter_.Load() == nullptr);
    if (arrival_time_.Load() != 0) return; // The filter may already have been cancelled.
    switch (filter_desc_.type) {
      case TRuntimeFilterType::BLOOM: bloom_filter_.Store(bloom_filter); break;
      case TRuntimeFilterType::MIN_MAX: min_max_filter_.Store(min_max_filter); break;
      case TRuntimeFilterType::IN_LIST: in_list_filter_.Store(in_list_filter); break;
      default: DCHECK(false);
    }
    arrival_time_.Store(MonotonicMillis());
    has_filter_.Store(true);
  }
  arrival_cv_.NotifyAll();
}

void RuntimeFilter::SetFilter(RuntimeFilter* other) {
  DCHECK_EQ(id(), other->id());
  SetFilter(is_bloom_filter() ? other->bloom_filter_.Load() : nullptr,
      is_min_max_filter() ? other->min_max_filter_.Load() : nullptr,
      is_in_list_filter() ? other->in_list_filter_.Load() : nullptr);
}

void RuntimeFilter::Or(RuntimeFilter* other) {
  // Or() is a no-op for AlwaysTrue() destination filter.
  if (AlwaysTrue()) return;
  if (is_bloom_filter()) {
    DCHECK(bloom_filter_.Load() != nullptr);
    BloomFilter* bloom_filter = other->bloom_filter_.Load();
    if (bloom_filter == BloomFilter::ALWAYS_TRUE_FILTER) {
      bloom_filter_.Store(BloomFilter::ALWAYS_TRUE_FILTER);
    } else {
      bloom_filter_.Load()->Or(*bloom_filter);
    }
  } else {
    DCHECK(is_min_max_filter());
    min_max_filter_.Load()->Or(*other->get_min_max());
  }
}

void RuntimeFilter::Cancel() {
  {
    unique_lock<mutex> l(arrival_mutex_);
    if (arrival_time_.Load() != 0) return;
    arrival_time_.Store(MonotonicMillis());
  }
  arrival_cv_.NotifyAll();
}

bool RuntimeFilter::WaitForArrival(int32_t timeout_ms) const {
  unique_lock<mutex> l(arrival_mutex_);
  while (arrival_time_.Load() == 0) {
    int64_t ms_since_registration = MonotonicMillis() - registration_time_;
    int64_t ms_remaining = timeout_ms - ms_since_registration;
    if (ms_remaining <= 0) break;
    if (injection_delay_ > 0) SleepForMs(injection_delay_);
    arrival_cv_.WaitFor(l, ms_remaining * MICROS_PER_MILLI);
  }
  return arrival_time_.Load() != 0;
}

void RuntimeFilter::SetIntermediateAggregation(bool is_intermediate_aggregator,
    std::string intermediate_krpc_hostname, NetworkAddressPB intermediate_krpc_backend) {
  DCHECK(!intermediate_krpc_hostname.empty());
  DCHECK(IsResolvedAddress(intermediate_krpc_backend));
  is_intermediate_aggregator_ = is_intermediate_aggregator;
  intermediate_krpc_hostname_ = intermediate_krpc_hostname;
  intermediate_krpc_backend_ = intermediate_krpc_backend;
}