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

#include <mutex>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "util/runtime-profile.h"

namespace impala {

/// Singleton utility class that updates counter values. This is used to sample some
/// metric (e.g. memory used) at regular intervals. The samples can be summarized in
/// a few ways (e.g. averaged, stored as histogram, kept as a time series data, etc).
/// This class has one thread that will wake up at a regular period and update all
/// the registered counters. Optionally, users can register functions to be called before
/// counters get updated, for example to update global metrics that the counters then
/// pull from.
/// Typically, the counter updates should be stopped as early as possible to prevent
/// future stale samples from polluting the useful values.
class PeriodicCounterUpdater {
 public:

  PeriodicCounterUpdater(const int32_t update_period)
    : update_period_(update_period) {
  }

  enum PeriodicCounterType {
    RATE_COUNTER = 0,
    SAMPLING_COUNTER,
  };

  /// Sets up data structures and starts the counter update thread. Should only be called
  /// once during process startup and must be called before other methods.
  static void Init();

  typedef std::function<void()> UpdateFn;
  /// Registers an update function that will be called before individual counters will be
  /// updated. This can be used to update some global metric once before reading it
  /// through individual counters.
  static void RegisterUpdateFunction(UpdateFn update_fn, bool is_system);

  /// Registers a periodic counter to be updated by the update thread.
  /// Either sample_fn or dst_counter must be non-NULL.  When the periodic counter
  /// is updated, it either gets the value from the dst_counter or calls the sample
  /// function to get the value.
  /// dst_counter/sample fn is assumed to be compatible types with src_counter.
  static void RegisterPeriodicCounter(RuntimeProfile::Counter* src_counter,
      RuntimeProfile::SampleFunction sample_fn,
      RuntimeProfile::Counter* dst_counter, PeriodicCounterType type);

  /// Adds a bucketing counter to be updated at regular intervals.
  static void RegisterBucketingCounters(RuntimeProfile::Counter* src_counter,
      std::vector<RuntimeProfile::Counter*>* buckets);

  /// Adds counter to be sampled and updated at regular intervals.
  static void RegisterTimeSeriesCounter(RuntimeProfile::TimeSeriesCounter* counter);

  /// Stops updating the value of 'counter'.
  static void StopRateCounter(RuntimeProfile::Counter* counter);

  /// Stops updating the value of 'counter'.
  static void StopSamplingCounter(RuntimeProfile::Counter* counter);

  /// If the bucketing counters 'buckets' are registered, stops updating the counters and
  /// convert the buckets from count to percentage. If not registered, has no effect.
  /// Perioidic counters are updated periodically so should be removed as soon as the
  /// underlying counter is no longer going to change.
  static void StopBucketingCounters(std::vector<RuntimeProfile::Counter*>* buckets,
      bool is_system = false);

  /// Stops 'counter' from receiving any more samples.
  static void StopTimeSeriesCounter(RuntimeProfile::TimeSeriesCounter* counter);

 private:
  struct RateCounterInfo {
    RuntimeProfile::Counter* src_counter;
    RuntimeProfile::SampleFunction sample_fn;
    int64_t elapsed_ms;
  };

  struct SamplingCounterInfo {
    RuntimeProfile::Counter* src_counter; // the counter to be sampled
    RuntimeProfile::SampleFunction sample_fn;
    int64_t total_sampled_value; // sum of all sampled values;
    int64_t num_sampled; // number of samples taken
  };

  struct BucketCountersInfo {
    RuntimeProfile::Counter* src_counter; // the counter to be sampled
    int64_t num_sampled; // number of samples taken
    /// TODO: customize bucketing
  };

  /// Loop for periodic counter update thread.  This thread wakes up once in a while
  /// and updates all the added rate counters and sampling counters.
  [[noreturn]] void UpdateLoop(PeriodicCounterUpdater* instance);

  /// Thread performing asynchronous updates.
  boost::scoped_ptr<boost::thread> update_thread_;

  /// List of functions that will be called before individual counters will be sampled.
  std::vector<UpdateFn> update_fns_;

  /// Spinlock that protects the list of update functions (and their execution).
  SpinLock update_fns_lock_;

  /// Spinlock that protects the map of rate counters
  SpinLock rate_lock_;

  /// A map of the dst (rate) counter to the src counter and elapsed time.
  typedef boost::unordered_map<RuntimeProfile::Counter*, RateCounterInfo> RateCounterMap;
  RateCounterMap rate_counters_;

  /// Spinlock that protects the map of averages over samples of counters
  SpinLock sampling_lock_;

  /// A map of the dst (averages over samples) counter to the src counter (to be sampled)
  /// and number of samples taken.
  typedef boost::unordered_map<RuntimeProfile::Counter*, SamplingCounterInfo>
      SamplingCounterMap;
  SamplingCounterMap sampling_counters_;

  /// Spinlock that protects the map of buckets of counters
  SpinLock bucketing_lock_;

  /// Map from a bucket of counters to the src counter
  typedef boost::unordered_map<std::vector<RuntimeProfile::Counter*>*, BucketCountersInfo>
      BucketCountersMap;
  BucketCountersMap bucketing_counters_;

  /// Spinlock that protects the map of time series counters
  SpinLock time_series_lock_;

  /// Set of time series counters that need to be updated
  typedef boost::unordered_set<RuntimeProfile::TimeSeriesCounter*> TimeSeriesCounters;
  TimeSeriesCounters time_series_counters_;

  /// Singleton object that keeps track of all profile rate counters and the thread
  /// for updating them. Interval set by flag periodic_counter_update_period_ms.
  static PeriodicCounterUpdater* instance_;

  /// Singleton object that keeps track of all system rate counters and the thread
  /// for updating them. Interval set by flag periodic_system_counter_update_period_ms.
  static PeriodicCounterUpdater* system_instance_;

  int32_t update_period_;
};
}
