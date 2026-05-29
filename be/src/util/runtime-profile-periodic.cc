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

#include "util/runtime-profile-counters.h"

#include <boost/bind.hpp>

#include "common/object-pool.h"
#include "util/periodic-counter-updater.h"

#include "common/names.h"

DECLARE_int32(status_report_interval_ms);
DECLARE_int32(periodic_counter_update_period_ms);
DECLARE_int32(periodic_system_counter_update_period_ms);

namespace impala {

static const string ROOT_COUNTER = "";

void RuntimeProfile::StopPeriodicCounters() {
  lock_guard<SpinLock> l(counter_map_lock_);
  if (!has_active_periodic_counters_) return;
  for (Counter* sampling_counter : sampling_counters_) {
    PeriodicCounterUpdater::StopSamplingCounter(sampling_counter);
  }
  for (Counter* rate_counter : rate_counters_) {
    PeriodicCounterUpdater::StopRateCounter(rate_counter);
  }
  for (vector<Counter*>* counters : bucketing_counters_) {
    PeriodicCounterUpdater::StopBucketingCounters(counters);
  }
  for (auto& time_series_counter_entry : time_series_counter_map_) {
    PeriodicCounterUpdater::StopTimeSeriesCounter(time_series_counter_entry.second);
  }
  has_active_periodic_counters_ = false;
}

RuntimeProfileBase::Counter* RuntimeProfile::AddRateCounter(
    const string& name, Counter* src_counter) {
  TUnit::type dst_unit;
  switch (src_counter->unit()) {
    case TUnit::BYTES:
      dst_unit = TUnit::BYTES_PER_SECOND;
      break;
    case TUnit::UNIT:
      dst_unit = TUnit::UNIT_PER_SECOND;
      break;
    default:
      DCHECK(false) << "Unsupported src counter unit: " << src_counter->unit();
      return NULL;
  }
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    bool created;
    Counter* dst_counter = AddCounterLocked(name, dst_unit, ROOT_COUNTER, &created);
    if (!created) return dst_counter;
    rate_counters_.push_back(dst_counter);
    PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
        PeriodicCounterUpdater::RATE_COUNTER);
    has_active_periodic_counters_ = true;
    return dst_counter;
  }
}

RuntimeProfileBase::Counter* RuntimeProfile::AddRateCounter(
    const string& name, SampleFunction fn, TUnit::type dst_unit) {
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter = AddCounterLocked(name, dst_unit, ROOT_COUNTER, &created);
  if (!created) return dst_counter;
  rate_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(
      NULL, std::move(fn), dst_counter, PeriodicCounterUpdater::RATE_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

RuntimeProfileBase::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter->unit() == TUnit::UNIT);
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter =
      AddCounterLocked(name, TUnit::DOUBLE_VALUE, ROOT_COUNTER, &created);
  if (!created) return dst_counter;
  sampling_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

RuntimeProfileBase::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, SampleFunction sample_fn) {
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter =
      AddCounterLocked(name, TUnit::DOUBLE_VALUE, ROOT_COUNTER, &created);
  if (!created) return dst_counter;
  sampling_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(
      NULL, std::move(sample_fn), dst_counter, PeriodicCounterUpdater::SAMPLING_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

vector<RuntimeProfileBase::Counter*>* RuntimeProfile::AddBucketingCounters(
    Counter* src_counter, int num_buckets) {
  lock_guard<SpinLock> l(counter_map_lock_);
  vector<Counter*>* buckets = pool_->Add(new vector<Counter*>);
  for (int i = 0; i < num_buckets; ++i) {
    buckets->push_back(pool_->Add(new Counter(TUnit::DOUBLE_VALUE, 0)));
  }
  bucketing_counters_.insert(buckets);
  has_active_periodic_counters_ = true;
  PeriodicCounterUpdater::RegisterBucketingCounters(src_counter, buckets);
  return buckets;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddSamplingTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn, bool is_system) {
  DCHECK(fn != nullptr);
  lock_guard<SpinLock> l(counter_map_lock_);
  TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(name);
  if (it != time_series_counter_map_.end()) return it->second;
  int32_t update_interval = is_system ?
      FLAGS_periodic_system_counter_update_period_ms :
      FLAGS_periodic_counter_update_period_ms;
  TimeSeriesCounter* counter = pool_->Add(
      new SamplingTimeSeriesCounter(name, unit, std::move(fn), update_interval));
  if (is_system) {
    counter->SetIsSystem();
  }
  time_series_counter_map_[name] = counter;
  PeriodicCounterUpdater::RegisterTimeSeriesCounter(counter);
  has_active_periodic_counters_ = true;
  return counter;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddSamplingTimeSeriesCounter(
    const string& name, Counter* src_counter, bool is_system) {
  DCHECK(src_counter != NULL);
  return AddSamplingTimeSeriesCounter(name, src_counter->unit(),
      bind(&Counter::value, src_counter), is_system);
}

RuntimeProfile::ChunkedTimeSeriesCounter::ChunkedTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn)
  : TimeSeriesCounter(name, unit, std::move(fn)),
    period_(FLAGS_periodic_counter_update_period_ms),
    max_size_(10 * FLAGS_status_report_interval_ms / period_) {}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddChunkedTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn) {
  DCHECK(fn != nullptr);
  lock_guard<SpinLock> l(counter_map_lock_);
  TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(name);
  if (it != time_series_counter_map_.end()) return it->second;
  TimeSeriesCounter* counter =
      pool_->Add(new ChunkedTimeSeriesCounter(name, unit, std::move(fn)));
  time_series_counter_map_[name] = counter;
  PeriodicCounterUpdater::RegisterTimeSeriesCounter(counter);
  has_active_periodic_counters_ = true;
  return counter;
}

}
