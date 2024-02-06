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

#ifndef IMPALA_UTIL_RUNTIME_PROFILE_COUNTERS_H
#define IMPALA_UTIL_RUNTIME_PROFILE_COUNTERS_H

#include <algorithm>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <sys/resource.h>
#include <sys/time.h>

#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/atomic.h"
#include "common/logging.h"
#include "gutil/singleton.h"
#include "util/arithmetic-util.h"
#include "util/stat-util.h"
#include "util/runtime-profile.h"
#include "util/stopwatch.h"
#include "util/streaming-sampler.h"

namespace impala {

/// This file contains the declarations of various counters that can be used in runtime
/// profiles. See the class-level comment for RuntimeProfile (runtime-profile.h) for an
/// overview of what there is. When making changes, please also update that comment.

/// Define macros for updating counters.  The macros make it very easy to disable
/// all counters at compile time.  Set this to 0 to remove counters.  This is useful
/// to do to make sure the counters aren't affecting the system.
#define ENABLE_COUNTERS 1

/// Some macro magic to generate unique ids using __COUNTER__
#define CONCAT_IMPL(x, y) x##y
#define MACRO_CONCAT(x, y) CONCAT_IMPL(x, y)

#if ENABLE_COUNTERS
  #define ADD_COUNTER(profile, name, unit) (profile)->AddCounter(name, unit)
  #define ADD_TIME_SERIES_COUNTER(profile, name, src_counter) \
      (profile)->AddSamplingTimeSeriesCounter(name, src_counter)
  #define ADD_SYSTEM_TIME_SERIES_COUNTER(profile, name, src_counter) \
      (profile)->AddSamplingTimeSeriesCounter(name, src_counter, true)
  #define ADD_TIMER(profile, name) (profile)->AddCounter(name, TUnit::TIME_NS)
  #define ADD_SUMMARY_STATS_TIMER(profile, name) \
      (profile)->AddSummaryStatsCounter(name, TUnit::TIME_NS)
  #define ADD_SUMMARY_STATS_COUNTER(profile, name, unit) \
      (profile)->AddSummaryStatsCounter(name, unit)
  #define ADD_CHILD_TIMER(profile, name, parent) \
      (profile)->AddCounter(name, TUnit::TIME_NS, parent)
  #define SCOPED_TIMER(c) \
      ScopedTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_TIMER_COUNTER, __COUNTER__)(c)
  #define SCOPED_TIMER2(c1, c2) \
      ScopedTimer<MonotonicStopWatch> \
      MACRO_CONCAT(SCOPED_TIMER_COUNTER, __COUNTER__)(c1, c2)
  #define CANCEL_SAFE_SCOPED_TIMER(c, is_cancelled) \
      CANCEL_SAFE_SCOPED_TIMER3(c1, nullptr, nullptr, is_cancelled);
  #define CANCEL_SAFE_SCOPED_TIMER3(c1, c2, c3, is_cancelled) \
      ScopedTimer<MonotonicStopWatch> \
      MACRO_CONCAT(SCOPED_TIMER_COUNTER, __COUNTER__)(c1, c2, c3, is_cancelled)
  #define COUNTER_ADD(c, v) (c)->Add(v)
  #define COUNTER_SET(c, v) (c)->Set(v)
  #define ADD_THREAD_COUNTERS(profile, prefix) (profile)->AddThreadCounters(prefix)
  #define SCOPED_THREAD_COUNTER_MEASUREMENT(c) \
    ThreadCounterMeasurement \
      MACRO_CONCAT(SCOPED_THREAD_COUNTER_MEASUREMENT, __COUNTER__)(c)
  #define SCOPED_CONCURRENT_COUNTER(c)                                    \
    ScopedStopWatch<RuntimeProfile::ConcurrentTimerCounter> MACRO_CONCAT( \
      SCOPED_CONCURRENT_COUNTER, __COUNTER__)(c)
#else
  #define ADD_COUNTER(profile, name, unit) nullptr
  #define ADD_TIME_SERIES_COUNTER(profile, name, src_counter) nullptr
  #define ADD_TIMER(profile, name) nullptr
  #define ADD_SUMMARY_STATS_TIMER(profile, name) nullptr
  #define ADD_CHILD_TIMER(profile, name, parent) nullptr
  #define SCOPED_TIMER(c)
  #define SCOPED_TIMER2(c1, c2)
  #define CANCEL_SAFE_SCOPED_TIMER(c)
  #define CANCEL_SAFE_SCOPED_TIMER3(c1, c2, c3)
  #define COUNTER_ADD(c, v)
  #define COUNTER_SET(c, v)
  #define ADD_THREAD_COUNTERS(profile, prefix) nullptr
  #define SCOPED_THREAD_COUNTER_MEASUREMENT(c)
  #define SCOPED_CONCURRENT_COUNTER(c)
#endif


#define PROFILE_DEFINE_COUNTER(name, significance, unit, desc) \
  ::impala::CounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc, unit)

#define PROFILE_DEFINE_RATE_COUNTER(name, significance, unit, desc) \
  ::impala::RateCounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc, unit)

#define PROFILE_DEFINE_SUMMARY_STATS_COUNTER(name, significance, unit, desc) \
  ::impala::SummaryStatsCounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc, unit)

#define PROFILE_DEFINE_DERIVED_COUNTER(name, significance, unit, desc) \
  ::impala::DerivedCounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc, unit)

#define PROFILE_DEFINE_SAMPLING_COUNTER(name, significance, desc) \
  ::impala::SamplingCounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc)

#define PROFILE_DEFINE_HIGH_WATER_MARK_COUNTER(name, significance, unit, \
  desc)::impala::HighWaterMarkCounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc, unit)

#define PROFILE_DEFINE_TIME_SERIES_COUNTER(name, significance, unit, desc) \
  ::impala::TimeSeriesCounterPrototype PROFILE_##name( \
      #name, ::impala::ProfileEntryPrototype::Significance::significance, desc, unit)

#define PROFILE_DEFINE_TIMER(name, significance, desc) \
  ::impala::CounterPrototype PROFILE_##name(#name, \
      ::impala::ProfileEntryPrototype::Significance::significance, desc, TUnit::TIME_NS)

#define PROFILE_DEFINE_SUMMARY_STATS_TIMER(name, significance, desc) \
  ::impala::SummaryStatsCounterPrototype PROFILE_##name(#name, \
  ::impala::ProfileEntryPrototype::Significance::significance, desc, TUnit::TIME_NS)

#define PROFILE_DECLARE_COUNTER(name) extern ::impala::CounterPrototype PROFILE_##name

/// Prototype of a profile entry. All prototypes must be defined at compile time and must
/// have a unique name. Subclasses then must provide a way to create new profile entries
/// from a prototype, for example by providing an Instantiate() method.
class ProfileEntryPrototype {
 public:
  enum class Significance {
    // High level and stable counters - always useful for measuring query performance and
    // status. Counters that everyone is interested. should rarely change and if it does
    // we will make some effort to notify users.
    STABLE_HIGH,
    // Low level and stable counters - interesting counters to monitor and analyze by
    // machine. It will probably be interesting under some circumstances for users.
    // Lots of developers are interested.
    STABLE_LOW,
    // Unstable but useful - useful to understand query performance, but subject to
    // change, particularly if the implementation changes. E.g. RowBatchQueuePutWaitTime,
    // MaterializeTupleTimer
    UNSTABLE,
    // Debugging counters - generally not useful to users of Impala, the main use case is
    // low-level debugging. Can be hidden to reduce noise for most consumers of profiles.
    DEBUG
  };

  // Creates a new prototype and registers it with the singleton
  // ProfileEntryPrototypeRegistry instance.
  ProfileEntryPrototype(
      const char* name, Significance significance, const char* desc, TUnit::type unit);

  const char* name() const { return name_; }
  const char* desc() const { return desc_; }
  const char* significance() const { return SignificanceString(significance_); }
  TUnit::type unit() const { return unit_; }

  // Returns a string representation of 'significance'.
  static const char* SignificanceString(Significance significance);

  constexpr const static Significance ALLSIGNIFICANCE[4] = {Significance::STABLE_HIGH,
      Significance::STABLE_LOW, Significance::UNSTABLE, Significance::DEBUG};

  // Return the description of this significance. Should be similar with the
  // comments above but it is a more user interface descriptions
  static const char* SignificanceDescription(Significance significance);

 protected:
  // Name of this prototype, needs to have process lifetime.
  const char* name_;

  // Significance of this prototype. See enum Significance above for descriptions of
  // possible values.
  Significance significance_;

  // Description of this prototype, needs to have process lifetime. This is used to
  // generate documentation automatically.
  const char* desc_;
  TUnit::type unit_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ProfileEntryPrototype);
};

/// Registry to collect all profile entry prototypes. During process startup, all
/// prototypes will register with the singleton instance of this class. Then, this class
/// can be used to retrieve all registered prototypes, for example to display
/// documentation to users. This class is thread-safe.
class ProfileEntryPrototypeRegistry {
 public:
  // Returns the singleton instance.
  static ProfileEntryPrototypeRegistry* get() {
    return Singleton<ProfileEntryPrototypeRegistry>::get();
  }

  // Adds a prototype to the registry. Prototypes must have unique names.
  void AddPrototype(const ProfileEntryPrototype* prototype);

  // Copies all prototype pointers to 'out'.
  void GetPrototypes(std::vector<const ProfileEntryPrototype*>* out);

 private:
  mutable SpinLock lock_;
  std::map<const char*, const ProfileEntryPrototype*> prototypes_;
};

/// TODO: As an alternative to this approach we could just have a single class
/// ProfileEntryPrototype and pass its objects into the profile->Add.*Counter() methods.
class CounterPrototype : public ProfileEntryPrototype {
 public:
  CounterPrototype(
      const char* name, Significance significance, const char* desc, TUnit::type unit)
    : ProfileEntryPrototype(name, significance, desc, unit) {}

  RuntimeProfileBase::Counter* Instantiate(
      RuntimeProfile* profile, const std::string& parent_counter_name = "") {
    return profile->AddCounter(name(), unit(), parent_counter_name);
  }
};

class DerivedCounterPrototype : public ProfileEntryPrototype {
 public:
  DerivedCounterPrototype(const char* name, Significance significance, const char* desc,
      TUnit::type unit): ProfileEntryPrototype(name, significance, desc, unit) {}

  RuntimeProfile::DerivedCounter* Instantiate(RuntimeProfile* profile,
      const RuntimeProfile::SampleFunction& counter_fn,
      const std::string& parent_counter_name = "") {
    return profile->AddDerivedCounter(name(), unit(), counter_fn, parent_counter_name);
  }
};

class SamplingCounterPrototype : public ProfileEntryPrototype {
 public:
  SamplingCounterPrototype(const char* name, Significance significance, const char* desc)
    : ProfileEntryPrototype(name, significance, desc, TUnit::DOUBLE_VALUE) {}

  RuntimeProfileBase::Counter* Instantiate(
      RuntimeProfile* profile, RuntimeProfileBase::Counter* src_counter) {
    return profile->AddSamplingCounter(name(), src_counter);
  }

  RuntimeProfileBase::Counter* Instantiate(
      RuntimeProfile* profile, boost::function<int64_t()> sample_fn) {
    return profile->AddSamplingCounter(name(), sample_fn);
  }
};

class HighWaterMarkCounterPrototype : public ProfileEntryPrototype {
 public:
  HighWaterMarkCounterPrototype(
      const char* name, Significance significance, const char* desc,
      TUnit::type unit): ProfileEntryPrototype(name, significance, desc, unit) {}

  RuntimeProfile::HighWaterMarkCounter* Instantiate(RuntimeProfile* profile,
        const std::string& parent_counter_name = "") {
    return profile->AddHighWaterMarkCounter(name(), unit(), parent_counter_name);
  }
};

class TimeSeriesCounterPrototype : public ProfileEntryPrototype {
 public:
  TimeSeriesCounterPrototype(
      const char* name, Significance significance, const char* desc, TUnit::type unit)
    : ProfileEntryPrototype(name, significance, desc, unit) {}

  RuntimeProfile::TimeSeriesCounter* operator()(
      RuntimeProfile* profile, RuntimeProfileBase::Counter* src_counter) {
    DCHECK(src_counter->unit() == unit());
    return profile->AddSamplingTimeSeriesCounter(name(), src_counter);
  }

  RuntimeProfile::TimeSeriesCounter* Instantiate(
      RuntimeProfile* profile, RuntimeProfileBase::Counter* src_counter) {
    return (*this)(profile, src_counter);
  }
};

class RateCounterPrototype : public ProfileEntryPrototype {
 public:
  RateCounterPrototype(
      const char* name, Significance significance, const char* desc, TUnit::type unit)
    : ProfileEntryPrototype(name, significance, desc, unit) {}

  RuntimeProfileBase::Counter* operator()(
      RuntimeProfile* profile, RuntimeProfileBase::Counter* src_counter) {
    RuntimeProfileBase::Counter* new_counter =
        profile->AddRateCounter(name(), src_counter);
    DCHECK_EQ(unit(), new_counter->unit());
    return new_counter;
  }
  RuntimeProfileBase::Counter* Instantiate(
      RuntimeProfile* profile, RuntimeProfileBase::Counter* src_counter) {
    RuntimeProfileBase::Counter* new_counter =
        profile->AddRateCounter(name(), src_counter);
    DCHECK_EQ(unit(), new_counter->unit());
    return new_counter;
  }
};

class SummaryStatsCounterPrototype : public ProfileEntryPrototype {
 public:
  SummaryStatsCounterPrototype(const char* name, Significance significance,
      const char* desc, TUnit::type unit):
      ProfileEntryPrototype(name, significance, desc, unit) {}

  RuntimeProfile::SummaryStatsCounter* Instantiate(RuntimeProfile* profile,
      const std::string& parent_counter_name = "") {
    return profile->AddSummaryStatsCounter(name(), unit(), parent_counter_name);
  }
};

/// A counter that keeps track of the highest value seen (reporting that
/// as value()) and the current value.
class RuntimeProfile::HighWaterMarkCounter : public RuntimeProfileBase::Counter {
 public:
  HighWaterMarkCounter(TUnit::type unit) : Counter(unit) {}

  void Add(int64_t delta) override {
    int64_t new_val = current_value_.Add(delta);
    UpdateMax(new_val);
  }

  /// Tries to increase the current value by delta. If current_value() + delta
  /// exceeds max, return false and current_value is not changed.
  bool TryAdd(int64_t delta, int64_t max) {
    while (true) {
      int64_t old_val = current_value_.Load();
      int64_t new_val = old_val + delta;
      if (UNLIKELY(new_val > max)) return false;
      if (LIKELY(current_value_.CompareAndSwap(old_val, new_val))) {
        UpdateMax(new_val);
        return true;
      }
    }
  }

  void Set(int64_t v) override {
    current_value_.Store(v);
    UpdateMax(v);
  }

  int64_t current_value() const { return current_value_.Load(); }

 private:
  /// Set 'value_' to 'v' if 'v' is larger than 'value_'. The entire operation is
  /// atomic.
  void UpdateMax(int64_t v) {
    while (true) {
      int64_t old_max = value_.Load();
      int64_t new_max = std::max(old_max, v);
      if (new_max == old_max) break; // Avoid atomic update.
      if (LIKELY(value_.CompareAndSwap(old_max, new_max))) break;
    }
  }

  /// The current value of the counter. value_ in the super class represents
  /// the high water mark.
  AtomicInt64 current_value_;
};

/// A DerivedCounter also has a name and unit, but the value is computed.
/// Do not call Set() and Add().
class RuntimeProfile::DerivedCounter : public RuntimeProfileBase::Counter {
 public:
  DerivedCounter(TUnit::type unit, const SampleFunction& counter_fn)
    : Counter(unit), counter_fn_(counter_fn) {}

  int64_t value() const override {
    return counter_fn_();
  }

 private:
  SampleFunction counter_fn_;
};

/// An AveragedCounter maintains a set of counters and its value is the
/// average of the values in that set. The average is updated through calls
/// to UpdateCounter(), which may add a new counter or update an existing counter.
/// Set() and Add() should not be called.
/// TODO: IMPALA-9846: rename counter. CounterVector? AggregatedCounter?
class RuntimeProfileBase::AveragedCounter : public RuntimeProfileBase::Counter {
 public:
  /// Construct an empty counter with no values added.
  AveragedCounter(TUnit::type unit, int num_samples);

  /// Construct a counter from existing samples.
  AveragedCounter(TUnit::type unit, const std::vector<bool>& has_value,
      const std::vector<int64_t>& values);

  /// Update the counter with a new value for the input instance at 'idx'.
  /// No locks are obtained within this method because UpdateCounter() is called from
  /// the AggregatedRuntimeProfile::Update*() functions, which obtain locks on the entire
  /// counter map in a profile.
  /// Note that it is not thread-safe to call this from two threads at the same time.
  /// It is safe for it to be read at the same time as it is updated.
  void UpdateCounter(Counter* new_counter, int idx);

  /// Update the values of this counter, setting any provided values that were not
  /// previously set. 'have_value' and 'value' are applied to values of this counter
  /// starting at 'start_idx'.
  /// The same thread safety rules as UpdateCounter() apply.
  void Update(int start_idx, const std::vector<bool>& have_value,
      const std::vector<int64_t>& values);

  /// Answer the question whether there exists skew among all valid raw values
  /// backing this average counter.
  ///
  ///  Input argument threshold: the threshold used to evaluate skews.
  ///
  /// Return true if skew is detected and 'details' is populated with a list of
  /// all valid raw values and the population stddev in the form of:
  /// ([raw_value, raw_value ...], <skew-detection-formula>).
  ///  <skew-detection-formula> ::=
  ///      CoV=coefficient_of_variation_value, mean=mean_value
  ///
  /// Return false if no skew is detected and 'details' argument is not altered.
  ///
  bool HasSkew(double threshold, std::string* details);

  /// The value for this counter should be updated through UpdateCounter().
  /// Set() and Add() should not be used.
  void Set(double value) override { DCHECK(false); }
  void Set(int64_t value) override { DCHECK(false); }
  void Add(int64_t delta) override { DCHECK(false); }

  void PrettyPrint(const std::string& prefix, const std::string& name,
      Verbosity verbosity, std::ostream* s) const override;

  void ToThrift(const std::string& name, TAggCounter* tcounter) const;

  void ToJson(Verbosity verbosity, rapidjson::Document& document,
      rapidjson::Value* val) const override;

  int64_t value() const override;

 private:
  FRIEND_TEST(CountersTest, AveragedCounterStats);

  /// Number of values in the below arrays.
  const int num_values_;

  /// Whether we have a valid value for each input counter. Always initialized to have
  /// 'num_samples' entries.
  std::unique_ptr<AtomicBool[]> has_value_;

  /// The value of each input counter. Always initialized to have 'num_samples' entries.
  std::unique_ptr<AtomicInt64[]> values_;

  // Stats computed in GetStats().
  template <typename T>
  struct Stats {
    T min = 0;
    T mean = 0;
    T max = 0;
    // Values at different percentiles.
    T p50 = 0;
    T p75 = 0;
    T p90 = 0;
    T p95 = 0;
    int num_vals = 0;
  };

  /// Implementation of PrettyPrint parameterized by the type that 'values_' is
  /// interpreted as - either int64_t or double, depending on the T parameter.
  template <typename T>
  void PrettyPrintImpl(const std::string& prefix, const std::string& name,
      Verbosity verbosity, std::ostream* s) const;

  /// Implementation of ToJson parameterized by the type that 'values_' is
  /// interpreted as - either int64_t or double, depending on the T parameter.
  template <typename T>
  void ToJsonImpl(
      Verbosity verbosity, rapidjson::Document& document, rapidjson::Value* val) const;

  /// Compute all of the stats in Stats, interpreting the values in 'vals' as type T,
  /// which must be double if 'unit_' is DOUBLE_VALUE or int64_t otherwise.
  template <typename T>
  Stats<T> GetStats() const;

  /// Helper for value() that compute the mean value, interpreting the values in 'vals'
  /// as type T, which must be double if 'unit_' is DOUBLE_VALUE or int64_t otherwise.
  /// Returns the mean value, or the double bit pattern stored in an int64_t.
  template <typename T>
  int64_t ComputeMean() const;

  /// Decide whether skew exists among all valid raw values V in this counter with the
  /// following formula:
  ///
  ///   CoV(V) > threshold && mean(V) > 5000
  ///
  /// where CoV(V) is the coefficient of variation, defined as
  /// stddev_population(V) / mean(V). CoV measures the variability in relation to the
  /// mean. When values in V are identical (no skew), CoV(V) = 0. When values in V
  /// differ, CoV(V) > 0. The above formula excludes small skew casess when the average
  /// raw value is no greater than 5000.
  ///
  /// Returns true if the above formula is evaluated to true, false otherwise.
  ///
  static const int ROW_AVERAGE_LIMIT = 5000;
  template <typename T>
  bool EvaluateSkewWithCoV(double threshold, std::stringstream* details);

  /// Compute the population stddev from all valid values (of type T) in
  /// values_[] and collect these values in a comma separated list through 'details'.
  template <typename T>
  void ComputeStddevPForValidValues(std::string* details, double* stddev);
};

/// This counter records multiple values and keeps a track of the minimum, maximum and
/// average value of all the values seen so far.
/// Unlike the AveragedCounter, this only keeps track of statistics of raw values
/// whereas the AveragedCounter maintains an average of counters.
/// value() stores the average.
class RuntimeProfileBase::SummaryStatsCounter : public RuntimeProfileBase::Counter {
 public:
  SummaryStatsCounter(TUnit::type unit, int32_t total_num_values, int64_t min_value,
      int64_t max_value, int64_t sum)
    : Counter(unit),
      total_num_values_(total_num_values),
      min_(min_value),
      max_(max_value),
      sum_(sum) {
    value_.Store(total_num_values == 0 ? 0 : sum / total_num_values);
  }

  SummaryStatsCounter(TUnit::type unit)
   : Counter(unit),
     total_num_values_(0),
     min_(numeric_limits<int64_t>::max()),
     max_(numeric_limits<int64_t>::min()),
     sum_(0) {
  }

  int64_t MinValue();
  int64_t MaxValue();
  int32_t TotalNumValues();

  /// Update sum_ with the new value and also update the min and the max values
  /// seen so far.
  void UpdateCounter(int64_t new_value);

  /// The value for this counter should be updated through UpdateCounter() or SetStats().
  /// Set() and Add() should not be used.
  void Set(double value) override { DCHECK(false); }
  void Set(int64_t value) override { DCHECK(false); }
  void Add(int64_t delta) override { DCHECK(false); }

  /// Overwrites the existing counter with 'counter'
  void SetStats(const TSummaryStatsCounter& counter);

  /// Overwrites the existing counter with 'counter'. Acquires lock on both counters.
  void SetStats(const SummaryStatsCounter& other);

  /// Merge 'other' into this counter. Acquires lock on both counters.
  void Merge(const SummaryStatsCounter& other);

  void ToThrift(TSummaryStatsCounter* counter, const std::string& name);

  /// Convert a vector of summary stats counters to an aggregate representation.
  static void ToThrift(const std::string& name, TUnit::type unit,
      const std::vector<SummaryStatsCounter*>& counters,
      TAggSummaryStatsCounter* tcounter);

  void ToJson(Verbosity verbosity, rapidjson::Document& document,
      rapidjson::Value* val) const override {
    Counter::ToJson(verbosity, document, val);
    if (total_num_values_ > 0) {
      val->AddMember("min", min_, document.GetAllocator());
      val->AddMember("max", max_, document.GetAllocator());
      val->AddMember("avg", value(), document.GetAllocator());
    }
    val->AddMember("num_of_samples", total_num_values_, document.GetAllocator());
  }

  void PrettyPrint(const std::string& prefix, const std::string& name,
      Verbosity verbosity, std::ostream* s) const override;

 private:
  /// The total number of values seen so far.
  int32_t total_num_values_;

  /// Summary statistics of values seen so far.
  int64_t min_;
  int64_t max_;
  int64_t sum_;

  // Protects min_, max_, sum_, total_num_values_ and value_.
  // When acquiring locks on two counters, e.g. in Merge(), the source is acquired
  // before the destination.
  mutable SpinLock lock_;
};

/// A set of counters that measure thread info, such as total time, user time, sys time.
class RuntimeProfile::ThreadCounters {
 private:
  friend class ThreadCounterMeasurement;
  friend class RuntimeProfile;

  Counter* total_time_; // total wall clock time
  Counter* user_time_;  // user CPU time
  Counter* sys_time_;   // system CPU time

  /// The number of times a context switch resulted due to a process voluntarily giving
  /// up the processor before its time slice was completed.
  Counter* voluntary_context_switches_;

  /// The number of times a context switch resulted due to a higher priority process
  /// becoming runnable or because the current process exceeded its time slice.
  Counter* involuntary_context_switches_;
};

/// An EventSequence captures a sequence of events (each added by calling MarkEvent()).
/// Each event has a text label and a time (measured relative to the moment Start() was
/// called as t=0, or to the parameter 'when' passed to Start(int64_t when)). It is useful
/// for tracking the evolution of some serial process, such as the query lifecycle.
class RuntimeProfile::EventSequence {
 public:
  EventSequence() { }

  /// Helper constructor for building from Thrift
  EventSequence(const std::vector<int64_t>& timestamps,
                const std::vector<std::string>& labels) {
    DCHECK(timestamps.size() == labels.size());
    for (int i = 0; i < timestamps.size(); ++i) {
      events_.push_back(make_pair(labels[i], timestamps[i]));
    }
  }

  /// Starts the timer without resetting it.
  void Start() { sw_.Start(); }

  /// Starts the timer. All events will be recorded as if the timer had been started at
  /// 'start_time_ns', which must have been obtained by calling MonotonicStopWatch::Now().
  void Start(int64_t start_time_ns) {
    offset_ = MonotonicStopWatch::Now() - start_time_ns;
    // TODO: IMPALA-4631: Occasionally we see MonotonicStopWatch::Now() return
    // (start_time_ns - e), where e is 1, 2 or 3 even though 'start_time_ns' was
    // obtained using MonotonicStopWatch::Now().
    DCHECK_GE(offset_, -3);
    sw_.Start();
  }

  /// Stores an event in sequence with the given label and the current time
  /// (relative to the first time Start() was called) as the timestamp.
  int64_t MarkEvent(std::string label) {
    Event event = make_pair(move(label), sw_.ElapsedTime() + offset_);
    std::lock_guard<SpinLock> event_lock(lock_);
    events_.emplace_back(move(event));
    return event.second;
  }

  int64_t ElapsedTime() { return sw_.ElapsedTime(); }

  /// An Event is a <label, timestamp> pair.
  typedef std::pair<std::string, int64_t> Event;

  /// An EventList is a sequence of Events.
  typedef std::vector<Event> EventList;

  /// Returns a copy of 'events_' in the supplied vector 'events', sorted by their
  /// timestamps. The supplied vector 'events' is cleared before this.
  void GetEvents(std::vector<Event>* events) {
    events->clear();
    std::lock_guard<SpinLock> event_lock(lock_);
    /// It's possible that MarkEvent() logs concurrent events out of sequence so we sort
    /// the events each time we are here.
    SortEvents();
    events->insert(events->end(), events_.begin(), events_.end());
  }

  /// Adds all events from the input parameters that are newer than the last member of
  /// 'events_'. The caller must make sure that 'timestamps' is sorted. Does not adjust
  /// added timestamps by 'offset_'.
  void AddNewerEvents(
      const std::vector<int64_t>& timestamps, const std::vector<std::string>& labels) {
    DCHECK_EQ(timestamps.size(), labels.size());
    DCHECK(std::is_sorted(timestamps.begin(), timestamps.end()));
    std::lock_guard<SpinLock> event_lock(lock_);
    int64_t last_timestamp = events_.empty() ? 0 : events_.back().second;
    for (int64_t i = 0; i < timestamps.size(); ++i) {
      if (timestamps[i] <= last_timestamp) continue;
      events_.emplace_back(labels[i], timestamps[i]);
    }
  }

  void ToThrift(TEventSequence* seq);

  /// Builds a new Value into 'value', using (if required) the allocator from
  /// 'document'. Should set the following fields where appropriate:
  /// {
  ///   “offset” : xxx,
  ///   “events”: [{
  ///       “label”: xxx,
  ///       “timestamp”: xxx
  ///   },{...}]
  /// }
  void ToJson(
      Verbosity verbosity, rapidjson::Document& document, rapidjson::Value* value);

 private:
  /// Sorts events by their timestamp. Caller must hold lock_.
  void SortEvents() {
    std::sort(events_.begin(), events_.end(),
        [](Event const &event1, Event const &event2) {
        return event1.second < event2.second;
      });
  }

  /// Protect access to events_.
  SpinLock lock_;

  /// Sequence of events. Due to a race in MarkEvent() these are not necessarily ordered.
  EventList events_;

  /// Timer which allows events to be timestamped when they are recorded.
  MonotonicStopWatch sw_;

  /// Constant offset that gets added to each event's timestamp. This allows to
  /// synchronize events captured in multiple threads to a common starting point.
  int64_t offset_ = 0;
};

/// Abstract base for counters to capture a time series of values. Users can add samples
/// to counters in periodic intervals, and the RuntimeProfile class will retrieve them by
/// accessing the private interface. Methods are thread-safe where explicitly stated.
class RuntimeProfile::TimeSeriesCounter {
 public:
  virtual ~TimeSeriesCounter() {}

  // Adds a sample. Thread-safe.
  void AddSample(int ms_elapsed);

  // Returns a pointer do the sample data together with the number of samples and the
  // sampling period. This method is not thread-safe and must only be used in tests.
  const int64_t* GetSamplesTest(int* num_samples, int* period) {
    return GetSamplesLockedForSend(num_samples, period);
  }

  void ToThrift(TTimeSeriesCounter* counter);

  TUnit::type unit() const { return unit_; }

  void SetIsSystem() { is_system_ = true; }
  bool GetIsSystem() const { return  is_system_; }

 private:
  friend class RuntimeProfile;

  /// Builds a new Value into 'value', using (if required) the allocator from
  /// 'document'. Should set the following fields where appropriate:
  /// {
  ///   “counter_name” : xxx,
  ///   “unit” : xxx,
  ///   “num” : xxx,
  ///   “period” : xxx,
  ///   “data”: “x,x,x,x”
  /// }
  virtual void ToJson(
      Verbosity verbosity, rapidjson::Document& document, rapidjson::Value* val);

  /// Adds a sample to the counter. Caller must hold lock_.
  virtual void AddSampleLocked(int64_t value, int ms_elapsed) = 0;

  /// Returns a pointer to memory containing all samples of the counter. The caller must
  /// hold lock_. The returned pointer is only valid while the caller holds lock_.
  virtual const int64_t* GetSamplesLocked(int* num_samples, int* period) const = 0;

  /// Returns a pointer to memory containing all samples of the counter and marks the
  /// samples as retrieved, so that a subsequent call to Clear() can remove them. The
  /// caller must hold lock_. The returned pointer is only valid while the caller holds
  /// lock_.
  virtual const int64_t* GetSamplesLockedForSend(int* num_samples, int* period);

  /// Sets all internal samples. Thread-safe. Not implemented by all child classes. The
  /// caller must make sure that this is only called on supported classes.
  virtual void SetSamples(
      int period, const std::vector<int64_t>& samples, int64_t start_idx);

  /// Implemented by some child classes to clear internal sample buffers. No-op on other
  /// child classes.
  virtual void Clear() {}

 protected:
  TimeSeriesCounter(const std::string& name, TUnit::type unit,
      SampleFunction fn = SampleFunction())
    : name_(name), unit_(unit), sample_fn_(fn), is_system_(false) {}

  std::string name_;
  TUnit::type unit_;
  SampleFunction sample_fn_;
  /// The number of samples that have been retrieved and cleared from this counter.
  int64_t previous_sample_count_ = 0;
  mutable SpinLock lock_;
  bool is_system_;
};

typedef StreamingSampler<int64_t, 64> StreamingCounterSampler;
class RuntimeProfile::SamplingTimeSeriesCounter
    : public RuntimeProfile::TimeSeriesCounter {
 private:
  friend class RuntimeProfile;
  friend class ToJson_TimeSeriesCounterToJsonTest_Test;

  SamplingTimeSeriesCounter(
      const std::string& name, TUnit::type unit, SampleFunction fn, int initial_period)
    : TimeSeriesCounter(name, unit, fn), samples_(initial_period) {}

  virtual void AddSampleLocked(int64_t sample, int ms_elapsed) override;
  virtual const int64_t* GetSamplesLocked( int* num_samples, int* period) const override;

  /// Reset the underlying StreamingCounterSampler to the initial state. Used in tests.
  void Reset();

  StreamingCounterSampler samples_;
};

/// Time series counter that supports piece-wise transmission of its samples.
///
/// This time series counter will capture samples into an internal unbounded buffer.
/// The buffer can be reset to clear out values that have already been transmitted
/// elsewhere.
class RuntimeProfile::ChunkedTimeSeriesCounter
    : public RuntimeProfile::TimeSeriesCounter {
 public:
  /// Clears the internal sample buffer and updates the number of samples that the counter
  /// has seen in total so far.
  virtual void Clear() override;

 private:
  friend class RuntimeProfile;

  /// Constructs a time series counter that uses 'fn' to generate new samples. It's size
  /// is bounded by the expected number of samples per status update times a constant
  /// factor.
  ChunkedTimeSeriesCounter(
      const std::string& name, TUnit::type unit, SampleFunction fn);

  /// Constructs a time series object from existing sample data. This counter is then
  /// read-only (i.e. there is no sample function). This counter has no maximum size.
  ChunkedTimeSeriesCounter(const std::string& name, TUnit::type unit, int period,
      const std::vector<int64_t>& values)
    : TimeSeriesCounter(name, unit), period_(period), values_(values), max_size_(0) {}

  virtual void AddSampleLocked(int64_t value, int ms_elapsed) override;
  virtual const int64_t* GetSamplesLocked(int* num_samples, int* period) const override;
  virtual const int64_t* GetSamplesLockedForSend(int* num_samples, int* period) override;

  virtual void SetSamples(
      int period, const std::vector<int64_t>& samples, int64_t start_idx) override;

  int period_ = 0;
  std::vector<int64_t> values_;
  // The number of values returned through the last call to GetSamplesLockedForSend().
  int64_t last_get_count_ = 0;
  // The maximum number of samples that can be stored in this counter. We drop samples at
  // the front before appending new ones if we would exceed this count.
  int64_t max_size_;
};

/// Counter whose value comes from an internal ConcurrentStopWatch to track concurrent
/// running time for multiple threads.
class RuntimeProfile::ConcurrentTimerCounter : public Counter {
 public:
  ConcurrentTimerCounter(TUnit::type unit) : Counter(unit) {}

  int64_t value() const override { return csw_.TotalRunningTime(); }

  void Start() { csw_.Start(); }

  void Stop() { csw_.Stop(); }

  /// Returns lap time for caller who wants delta update of concurrent running time.
  uint64_t LapTime() { return csw_.LapTime(); }

  /// The value for this counter should come from internal ConcurrentStopWatch.
  /// Set() and Add() should not be used.
  void Set(double value) override {
    DCHECK(false);
  }

  void Set(int64_t value) override {
    DCHECK(false);
  }

  void Set(int value) override {
    DCHECK(false);
  }

  void Add(int64_t delta) override {
    DCHECK(false);
  }

 private:
  ConcurrentStopWatch csw_;
};

/// Utility class to mark an event when the object is destroyed.
class ScopedEvent {
 public:
  ScopedEvent(RuntimeProfile::EventSequence* event_sequence, const std::string& label)
    : label_(label),
      event_sequence_(event_sequence) {
  }

  /// Mark the event when the object is destroyed
  ~ScopedEvent() {
    event_sequence_->MarkEvent(label_);
  }

 private:
  /// Disable copy constructor and assignment
  ScopedEvent(const ScopedEvent& event);
  ScopedEvent& operator=(const ScopedEvent& event);

  const std::string label_;
  RuntimeProfile::EventSequence* event_sequence_;
};

/// Utility class to update time elapsed when the object goes out of scope.
/// Supports updating 1-3 counters to avoid the overhead of redundant timer calls.
///
/// 'T' must implement the StopWatch "interface" (Start,Stop,ElapsedTime) but
/// we use templates not to pay for virtual function overhead. In some cases
/// the runtime profile may be deleted while the counter is still active. In this
/// case the is_cancelled argument can be provided so that ScopedTimer will not
/// update the counter when the query is cancelled. The destructor for ScopedTimer
/// can access both is_cancelled and the counter, so the caller must ensure that it
/// is safe to access both at the end of the scope in which the timer is used.
template <class T>
class ScopedTimer {
 public:
  ScopedTimer(RuntimeProfileBase::Counter* c1 = nullptr,
      RuntimeProfileBase::Counter* c2 = nullptr,
      RuntimeProfileBase::Counter* c3 = nullptr, const bool* is_cancelled = nullptr)
    : counter1_(c1), counter2_(c2), counter3_(c3), is_cancelled_(is_cancelled) {
    DCHECK(c1 == nullptr || c1->unit() == TUnit::TIME_NS);
    DCHECK(c2 == nullptr || c2->unit() == TUnit::TIME_NS);
    DCHECK(c3 == nullptr || c3->unit() == TUnit::TIME_NS);
    sw_.Start();
  }

  void Stop() { sw_.Stop(); }
  void Start() { sw_.Start(); }

  void UpdateCounter() {
    if (IsCancelled()) return;
    int64_t elapsed = sw_.ElapsedTime();
    if (counter1_ != nullptr) counter1_->Add(elapsed);
    if (counter2_ != nullptr) counter2_->Add(elapsed);
    if (counter3_ != nullptr) counter3_->Add(elapsed);
  }

  /// Updates the underlying counters for the final time and clears the pointer to them.
  void ReleaseCounter() {
    UpdateCounter();
    counter1_ = nullptr;
    counter2_ = nullptr;
    counter3_ = nullptr;
  }

  bool IsCancelled() { return is_cancelled_ != nullptr && *is_cancelled_; }

  /// Return the total elapsed time accumulated by this timer so far.
  int64_t ElapsedTime() { return sw_.ElapsedTime(); }

  /// Update counter when object is destroyed
  ~ScopedTimer() {
    sw_.Stop();
    UpdateCounter();
  }

 private:
  /// Disable copy constructor and assignment
  ScopedTimer(const ScopedTimer& timer);
  ScopedTimer& operator=(const ScopedTimer& timer);

  T sw_;
  RuntimeProfileBase::Counter* counter1_;
  RuntimeProfileBase::Counter* counter2_;
  RuntimeProfileBase::Counter* counter3_;
  const bool* is_cancelled_;
};

#ifdef __APPLE__
// On OS X rusage via thread is not supported. In addition, the majority of the fields of
// the usage structs will be zeroed out. Since Apple is not going to be a major plaform
// initially it will most likely be enough to capture only time.
// C.f. http://blog.kuriositaet.de/?p=257
#define RUSAGE_THREAD RUSAGE_SELF
#endif

/// Utility class to update ThreadCounter when the object goes out of scope or when Stop is
/// called. Threads measurements will then be taken using getrusage.
/// This is ~5x slower than ScopedTimer due to calling getrusage.
class ThreadCounterMeasurement {
 public:
  ThreadCounterMeasurement(RuntimeProfile::ThreadCounters* counters) :
    stop_(false), counters_(counters) {
    DCHECK(counters != nullptr);
    sw_.Start();
    int ret = getrusage(RUSAGE_THREAD, &usage_base_);
    DCHECK_EQ(ret, 0);
  }

  /// Stop and update the counter
  void Stop() {
    if (stop_) return;
    stop_ = true;
    sw_.Stop();
    rusage usage;
    int ret = getrusage(RUSAGE_THREAD, &usage);
    DCHECK_EQ(ret, 0);
    int64_t utime_diff =
        (usage.ru_utime.tv_sec - usage_base_.ru_utime.tv_sec) * 1000L * 1000L * 1000L +
        (usage.ru_utime.tv_usec - usage_base_.ru_utime.tv_usec) * 1000L;
    int64_t stime_diff =
        (usage.ru_stime.tv_sec - usage_base_.ru_stime.tv_sec) * 1000L * 1000L * 1000L +
        (usage.ru_stime.tv_usec - usage_base_.ru_stime.tv_usec) * 1000L;
    counters_->total_time_->Add(sw_.ElapsedTime());
    counters_->user_time_->Add(utime_diff);
    counters_->sys_time_->Add(stime_diff);
    counters_->voluntary_context_switches_->Add(usage.ru_nvcsw - usage_base_.ru_nvcsw);
    counters_->involuntary_context_switches_->Add(
        usage.ru_nivcsw - usage_base_.ru_nivcsw);
  }

  /// Update counter when object is destroyed
  ~ThreadCounterMeasurement() {
    Stop();
  }

 private:
  /// Disable copy constructor and assignment
  ThreadCounterMeasurement(const ThreadCounterMeasurement& timer);
  ThreadCounterMeasurement& operator=(const ThreadCounterMeasurement& timer);

  bool stop_;
  rusage usage_base_;
  MonotonicStopWatch sw_;
  RuntimeProfile::ThreadCounters* counters_;
};
}

#endif
