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

#include "util/periodic-counter-updater.h"

#include "util/runtime-profile-counters.h"
#include "util/time.h"

#include "common/names.h"

namespace posix_time = boost::posix_time;
using boost::get_system_time;
using boost::system_time;

namespace impala {

// Period to update rate counters and sampling counters in ms.
DEFINE_int32(periodic_counter_update_period_ms, 500, "Period to update rate counters and"
    " sampling counters in ms");

PeriodicCounterUpdater PeriodicCounterUpdater::state_;

PeriodicCounterUpdater::PeriodicCounterUpdater() {
  DCHECK_EQ(this, &state_);
  state_.update_thread_.reset(
      new thread(&PeriodicCounterUpdater::UpdateLoop, this));
}

void PeriodicCounterUpdater::RegisterPeriodicCounter(
    RuntimeProfile::Counter* src_counter,
    RuntimeProfile::DerivedCounterFunction sample_fn,
    RuntimeProfile::Counter* dst_counter, PeriodicCounterType type) {
  DCHECK(src_counter == NULL || sample_fn == NULL);

  switch (type) {
    case RATE_COUNTER: {
      RateCounterInfo counter;
      counter.src_counter = src_counter;
      counter.sample_fn = sample_fn;
      counter.elapsed_ms = 0;
      lock_guard<SpinLock> ratelock(state_.rate_lock_);
      state_.rate_counters_[dst_counter] = counter;
      break;
    }
    case SAMPLING_COUNTER: {
      SamplingCounterInfo counter;
      counter.src_counter = src_counter;
      counter.sample_fn = sample_fn;
      counter.num_sampled = 0;
      counter.total_sampled_value = 0;
      lock_guard<SpinLock> samplinglock(state_.sampling_lock_);
      state_.sampling_counters_[dst_counter] = counter;
      break;
    }
    default:
      DCHECK(false) << "Unsupported PeriodicCounterType:" << type;
  }
}

void PeriodicCounterUpdater::StopRateCounter(RuntimeProfile::Counter* counter) {
  lock_guard<SpinLock> ratelock(state_.rate_lock_);
  state_.rate_counters_.erase(counter);
}

void PeriodicCounterUpdater::StopSamplingCounter(RuntimeProfile::Counter* counter) {
  lock_guard<SpinLock> samplinglock(state_.sampling_lock_);
  state_.sampling_counters_.erase(counter);
}

void PeriodicCounterUpdater::RegisterBucketingCounters(
    RuntimeProfile::Counter* src_counter, vector<RuntimeProfile::Counter*>* buckets) {
  BucketCountersInfo info;
  info.src_counter = src_counter;
  info.num_sampled = 0;
  lock_guard<SpinLock> bucketinglock(state_.bucketing_lock_);
  state_.bucketing_counters_[buckets] = info;
}

void PeriodicCounterUpdater::StopBucketingCounters(
    vector<RuntimeProfile::Counter*>* buckets, bool convert) {
  int64_t num_sampled = 0;
  {
    lock_guard<SpinLock> bucketinglock(state_.bucketing_lock_);
    BucketCountersMap::iterator itr =
        state_.bucketing_counters_.find(buckets);
    if (itr != state_.bucketing_counters_.end()) {
      num_sampled = itr->second.num_sampled;
      state_.bucketing_counters_.erase(itr);
    }
  }

  if (convert && num_sampled > 0) {
    for (int i = 0; i < buckets->size(); ++i) {
      RuntimeProfile::Counter* counter = (*buckets)[i];
      double perc = 100 * counter->value() / (double)num_sampled;
      counter->Set(perc);
    }
  }
}

void PeriodicCounterUpdater::RegisterTimeSeriesCounter(
    RuntimeProfile::TimeSeriesCounter* counter) {
  lock_guard<SpinLock> timeserieslock(state_.time_series_lock_);
  state_.time_series_counters_.insert(counter);
}

void PeriodicCounterUpdater::StopTimeSeriesCounter(
    RuntimeProfile::TimeSeriesCounter* counter) {
  lock_guard<SpinLock> timeserieslock(state_.time_series_lock_);
  state_.time_series_counters_.erase(counter);
}

void PeriodicCounterUpdater::UpdateLoop() {
  while (true) {
    system_time before_time = get_system_time();
    SleepForMs(FLAGS_periodic_counter_update_period_ms);
    posix_time::time_duration elapsed = get_system_time() - before_time;
    int elapsed_ms = elapsed.total_milliseconds();

    {
      lock_guard<SpinLock> ratelock(state_.rate_lock_);
      for (RateCounterMap::iterator it = rate_counters_.begin();
           it != rate_counters_.end(); ++it) {
        it->second.elapsed_ms += elapsed_ms;
        int64_t value;
        if (it->second.src_counter != NULL) {
          value = it->second.src_counter->value();
        } else {
          DCHECK(it->second.sample_fn != NULL);
          value = it->second.sample_fn();
        }
        int64_t rate = value * 1000 / (it->second.elapsed_ms);
        it->first->Set(rate);
      }
    }

    {
      lock_guard<SpinLock> samplinglock(state_.sampling_lock_);
      for (SamplingCounterMap::iterator it = sampling_counters_.begin();
           it != sampling_counters_.end(); ++it) {
        ++it->second.num_sampled;
        int64_t value;
        if (it->second.src_counter != NULL) {
          value = it->second.src_counter->value();
        } else {
          DCHECK(it->second.sample_fn != NULL);
          value = it->second.sample_fn();
        }
        it->second.total_sampled_value += value;
        double average = static_cast<double>(it->second.total_sampled_value) /
            it->second.num_sampled;
        it->first->Set(average);
      }
    }

    {
      lock_guard<SpinLock> bucketinglock(state_.bucketing_lock_);
      for (BucketCountersMap::iterator it = bucketing_counters_.begin();
           it != bucketing_counters_.end(); ++it) {
        int64_t val = it->second.src_counter->value();
        if (val >= it->first->size()) val = it->first->size() - 1;
        it->first->at(val)->Add(1);
        ++it->second.num_sampled;
      }
    }

    {
      lock_guard<SpinLock> timeserieslock(state_.time_series_lock_);
      for (TimeSeriesCounters::iterator it = time_series_counters_.begin();
           it != time_series_counters_.end(); ++it) {
        (*it)->AddSample(elapsed_ms);
      }
    }
  }
}




}
