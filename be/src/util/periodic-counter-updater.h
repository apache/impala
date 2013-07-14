// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_UTIL_PERIODIC_COUNTER_UPDATER_H
#define IMPALA_UTIL_PERIODIC_COUNTER_UPDATER_H

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "util/runtime-profile.h"

namespace impala {

// Singleton utility class that updates counter values. This is used to sample some
// metric (e.g. memory used) at regular intervals. The samples can be summarized in
// a few ways (e.g. averaged, stored as histogram, kept as a time series data, etc).
// This class has one thread that will wake up at a regular period and update all
// the registered counters.
// Typically, the counter updates should be stopped as early as possible to prevent
// future stale samples from polluting the useful values.
class PeriodicCounterUpdater {
 public:
  enum PeriodicCounterType {
    RATE_COUNTER = 0,
    SAMPLING_COUNTER,
  };

  // Tears down the update thread.
  ~PeriodicCounterUpdater();

  // Registers a periodic counter to be updated by the update thread.
  // Either sample_fn or dst_counter must be non-NULL.  When the periodic counter
  // is updated, it either gets the value from the dst_counter or calls the sample
  // function to get the value.
  // dst_counter/sample fn is assumed to be compatible types with src_counter.
  static void RegisterPeriodicCounter(RuntimeProfile::Counter* src_counter,
      RuntimeProfile::DerivedCounterFunction sample_fn,
      RuntimeProfile::Counter* dst_counter, PeriodicCounterType type);

  // Adds a bucketing counter to be updated at regular intervals.
  static void RegisterBucketingCounters(RuntimeProfile::Counter* src_counter,
      std::vector<RuntimeProfile::Counter*>* buckets);

  // Adds counter to be sampled and updated at regular intervals.
  static void RegisterTimeSeriesCounter(RuntimeProfile::TimeSeriesCounter* counter);

  // Stops updating the value of 'counter'.
  static void StopRateCounter(RuntimeProfile::Counter* counter);

  // Stops updating the value of 'counter'.
  static void StopSamplingCounter(RuntimeProfile::Counter* counter);

  // Stops updating the bucket counter.
  // If convert is true, convert the buckets from count to percentage.
  // Sampling counters are updated periodically so should be removed as soon as the
  // underlying counter is no longer going to change.
  static void StopBucketingCounters(std::vector<RuntimeProfile::Counter*>* buckets,
      bool convert);

  // Stops 'counter' from receiving any more samples.
  static void StopTimeSeriesCounter(RuntimeProfile::TimeSeriesCounter* counter);

 private:
  struct RateCounterInfo {
    RuntimeProfile::Counter* src_counter;
    RuntimeProfile::DerivedCounterFunction sample_fn;
    int64_t elapsed_ms;
  };

  struct SamplingCounterInfo {
    RuntimeProfile::Counter* src_counter; // the counter to be sampled
    RuntimeProfile::DerivedCounterFunction sample_fn;
    int64_t total_sampled_value; // sum of all sampled values;
    int64_t num_sampled; // number of samples taken
  };

  struct BucketCountersInfo {
    RuntimeProfile::Counter* src_counter; // the counter to be sampled
    int64_t num_sampled; // number of samples taken
    // TODO: customize bucketing
  };

  PeriodicCounterUpdater();

  // Loop for periodic counter update thread.  This thread wakes up once in a while
  // and updates all the added rate counters and sampling counters.
  void UpdateLoop();

  // Lock protecting state below
  boost::mutex lock_;

  // If true, tear down the update thread.
  volatile bool done_;

  // Thread performing asynchronous updates.
  boost::scoped_ptr<boost::thread> update_thread_;

  // A map of the dst (rate) counter to the src counter and elapsed time.
  typedef boost::unordered_map<RuntimeProfile::Counter*, RateCounterInfo> RateCounterMap;
  RateCounterMap rate_counters_;

  // A map of the dst (averages over samples) counter to the src counter (to be sampled)
  // and number of samples taken.
  typedef boost::unordered_map<RuntimeProfile::Counter*, SamplingCounterInfo>
      SamplingCounterMap;
  SamplingCounterMap sampling_counters_;

  // Map from a bucket of counters to the src counter
  typedef boost::unordered_map<std::vector<RuntimeProfile::Counter*>*, BucketCountersInfo>
      BucketCountersMap;
  BucketCountersMap bucketing_counters_;

  // Set of time series counters that need to be updated
  typedef boost::unordered_set<RuntimeProfile::TimeSeriesCounter*> TimeSeriesCounters;
  TimeSeriesCounters time_series_counters_;

  // Singleton object that keeps track of all rate counters and the thread
  // for updating them.
  static PeriodicCounterUpdater state_;
};

}

#endif
