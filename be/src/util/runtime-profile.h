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


#ifndef IMPALA_UTIL_RUNTIME_PROFILE_H
#define IMPALA_UTIL_RUNTIME_PROFILE_H

#include <boost/function.hpp>
#include <boost/thread/lock_guard.hpp>
#include <iostream>

#include "common/atomic.h"
#include "common/status.h"
#include "util/spinlock.h"

#include "gen-cpp/RuntimeProfile_types.h"

namespace impala {

class ObjectPool;

/// Runtime profile is a group of profiling counters.  It supports adding named counters
/// and being able to serialize and deserialize them.
/// The profiles support a tree structure to form a hierarchy of counters.
/// Runtime profiles supports measuring wall clock rate based counters.  There is a
/// single thread per process that will convert an amount (i.e. bytes) counter to a
/// corresponding rate based counter.  This thread wakes up at fixed intervals and updates
/// all of the rate counters.
/// Thread-safe.
class RuntimeProfile { // NOLINT: This struct is not packed, but there are not so many
                       // of them that it makes a performance difference
 public:
  class Counter {
   public:
    Counter(TUnit::type unit, int64_t value = 0) :
      value_(value),
      unit_(unit) {
    }
    virtual ~Counter(){}

    virtual void Add(int64_t delta) {
      value_.Add(delta);
    }

    /// Use this to update if the counter is a bitmap
    void BitOr(int64_t delta) {
      int64_t old;
      do {
        old = value_.Load();
        if (LIKELY((old | delta) == old)) return; // Bits already set, avoid atomic.
      } while (UNLIKELY(!value_.CompareAndSwap(old, old | delta)));
    }

    virtual void Set(int64_t value) { value_.Store(value); }

    virtual void Set(int value) { value_.Store(value); }

    virtual void Set(double value) {
      DCHECK_EQ(sizeof(value), sizeof(int64_t));
      value_.Store(*reinterpret_cast<int64_t*>(&value));
    }

    virtual int64_t value() const { return value_.Load(); }

    virtual double double_value() const {
      int64_t v = value_.Load();
      return *reinterpret_cast<const double*>(&v);
    }

    TUnit::type unit() const { return unit_; }

   protected:
    friend class RuntimeProfile;

    AtomicInt64 value_;
    TUnit::type unit_;
  };

  class AveragedCounter;
  class ConcurrentTimerCounter;
  class DerivedCounter;
  class EventSequence;
  class HighWaterMarkCounter;
  class SummaryStatsCounter;
  class ThreadCounters;
  class TimeSeriesCounter;

  typedef boost::function<int64_t ()> DerivedCounterFunction;

  /// Create a runtime profile object with 'name'. The profile, counters and any other
  /// structures owned by the profile are allocated from 'pool'.
  /// If 'is_averaged_profile' is true, the counters in this profile will be derived
  /// averages (of unit AveragedCounter) from other profiles, so the counter map will
  /// be left empty Otherwise, the counter map is initialized with a single entry for
  /// TotalTime.
  static RuntimeProfile* Create(ObjectPool* pool, const std::string& name,
      bool is_averaged_profile = false);

  ~RuntimeProfile();

  /// Deserialize from thrift.  Runtime profiles are allocated from the pool.
  static RuntimeProfile* CreateFromThrift(ObjectPool* pool,
      const TRuntimeProfileTree& profiles);

  /// Adds a child profile.  This is thread safe.
  /// Checks if 'child' is already added by searching for its name in the
  /// child map, and only adds it if the name doesn't exist.
  /// 'indent' indicates whether the child will be printed w/ extra indentation
  /// relative to the parent.
  /// If location is non-null, child will be inserted after location.  Location must
  /// already be added to the profile.
  void AddChild(RuntimeProfile* child,
      bool indent = true, RuntimeProfile* location = NULL);

  /// Adds a child profile, similarly to AddChild(). The child profile is put before any
  /// existing profiles.
  void PrependChild(RuntimeProfile* child, bool indent = true);

  /// Creates a new child profile with the given 'name'. A child profile with that name
  /// must not already exist. If 'prepend' is true, prepended before other child profiles,
  /// otherwise appended after other child profiles.
  RuntimeProfile* CreateChild(
      const std::string& name, bool indent = true, bool prepend = false);

  /// Sorts all children according to a custom comparator. Does not
  /// invalidate pointers to profiles.
  template <class Compare>
  void SortChildren(const Compare& cmp) {
    boost::lock_guard<SpinLock> l(children_lock_);
    std::sort(children_.begin(), children_.end(), cmp);
  }

  /// Updates the AveragedCounter counters in this profile with the counters from the
  /// 'src' profile. If a counter is present in 'src' but missing in this profile, a new
  /// AveragedCounter is created with the same name. This method should not be invoked
  /// if is_average_profile_ is false. Obtains locks on the counter maps and child counter
  /// maps in both this and 'src' profiles.
  void UpdateAverage(RuntimeProfile* src);

  /// Updates this profile w/ the thrift profile.
  /// Counters and child profiles in thrift_profile that already exist in this profile
  /// are updated. Counters that do not already exist are created.
  /// Info strings matched up by key and are updated or added, depending on whether
  /// the key has already been registered.
  /// TODO: Event sequences are ignored
  void Update(const TRuntimeProfileTree& thrift_profile);

  /// Add a counter with 'name'/'unit'.  Returns a counter object that the caller can
  /// update.  The counter is owned by the RuntimeProfile object.
  /// If parent_counter_name is a non-empty string, the counter is added as a child of
  /// parent_counter_name.
  /// If the counter already exists, the existing counter object is returned.
  Counter* AddCounter(const std::string& name, TUnit::type unit,
      const std::string& parent_counter_name = "");

  /// Adds a counter that tracks the min, max and average values to the runtime profile.
  /// Otherwise, same behavior as AddCounter().
  SummaryStatsCounter* AddSummaryStatsCounter(const std::string& name, TUnit::type unit,
      const std::string& parent_counter_name = "");

  /// Adds a high water mark counter to the runtime profile. Otherwise, same behavior
  /// as AddCounter().
  HighWaterMarkCounter* AddHighWaterMarkCounter(const std::string& name,
      TUnit::type unit, const std::string& parent_counter_name = "");

  ConcurrentTimerCounter* AddConcurrentTimerCounter(const std::string& name,
      TUnit::type unit, const std::string& parent_counter_name = "");

  /// Add a derived counter with 'name'/'unit'. The counter is owned by the
  /// RuntimeProfile object.
  /// If parent_counter_name is a non-empty string, the counter is added as a child of
  /// parent_counter_name.
  /// Returns NULL if the counter already exists.
  DerivedCounter* AddDerivedCounter(const std::string& name, TUnit::type unit,
      const DerivedCounterFunction& counter_fn,
      const std::string& parent_counter_name = "");

  /// Add a set of thread counters prefixed with 'prefix'. Returns a ThreadCounters object
  /// that the caller can update.  The counter is owned by the RuntimeProfile object.
  ThreadCounters* AddThreadCounters(const std::string& prefix);

  // Add a derived counter to capture the local time. This function can be called at most
  // once.
  void AddLocalTimeCounter(const DerivedCounterFunction& counter_fn);

  /// Gets the counter object with 'name'.  Returns NULL if there is no counter with
  /// that name.
  Counter* GetCounter(const std::string& name);

  /// Adds all counters with 'name' that are registered either in this or
  /// in any of the child profiles to 'counters'.
  void GetCounters(const std::string& name, std::vector<Counter*>* counters);

  /// Adds a string to the runtime profile.  If a value already exists for 'key',
  /// the value will be updated.
  void AddInfoString(const std::string& key, const std::string& value);

  /// Adds a string to the runtime profile.  If a value already exists for 'key',
  /// 'value' will be appended to the previous value, with ", " separating them.
  void AppendInfoString(const std::string& key, const std::string& value);

  /// Helper to append to the "ExecOption" info string.
  void AppendExecOption(const std::string& option) {
    AppendInfoString("ExecOption", option);
  }

  /// Helper to append "Codegen Enabled" or "Codegen Disabled" exec options. If
  /// specified, 'extra_info' is appended to the exec option, and 'extra_label'
  /// is prepended to the exec option.
  void AddCodegenMsg(bool codegen_enabled, const std::string& extra_info = "",
      const std::string& extra_label = "");

  /// Helper wrapper for AddCodegenMsg() that takes a status instead of a string
  /// describing why codegen was disabled. 'codegen_status' can be OK whether or
  /// not 'codegen_enabled' is true (e.g. if codegen is disabled by a query option,
  /// then no error occurred).
  void AddCodegenMsg(bool codegen_enabled, const Status& codegen_status,
      const std::string& extra_label = "") {
    AddCodegenMsg(codegen_enabled, codegen_status.GetDetail(), extra_label);
  }

  /// Creates and returns a new EventSequence (owned by the runtime
  /// profile) - unless a timer with the same 'key' already exists, in
  /// which case it is returned.
  /// TODO: EventSequences are not merged by Merge() or Update()
  EventSequence* AddEventSequence(const std::string& key);
  EventSequence* AddEventSequence(const std::string& key, const TEventSequence& from);

  /// Returns event sequence with the provided name if it exists, otherwise NULL.
  EventSequence* GetEventSequence(const std::string& name) const;

  /// Returns a pointer to the info string value for 'key'.  Returns NULL if
  /// the key does not exist.
  const std::string* GetInfoString(const std::string& key) const;

  /// Stops updating all counters in this profile that are periodically updated by a
  /// background thread (i.e. sampling, rate, bucketing and time series counters).
  /// Must be called before the profile is destroyed if any such counters are active.
  /// Does not stop counters on descendant profiles.
  void StopPeriodicCounters();

  /// Returns the counter for the total elapsed time.
  Counter* total_time_counter() { return counter_map_[TOTAL_TIME_COUNTER_NAME]; }
  Counter* inactive_timer() { return counter_map_[INACTIVE_TIME_COUNTER_NAME]; }
  int64_t local_time() { return local_time_ns_; }

  /// Prints the counters in a name: value format.
  /// Does not hold locks when it makes any function calls.
  void PrettyPrint(std::ostream* s, const std::string& prefix="") const;

  /// Serializes profile to thrift.
  /// Does not hold locks when it makes any function calls.
  void ToThrift(TRuntimeProfileTree* tree) const;
  void ToThrift(std::vector<TRuntimeProfileNode>* nodes) const;

  /// Serializes the runtime profile to a string.  This first serializes the
  /// object using thrift compact binary format, then gzip compresses it and
  /// finally encodes it as base64.  This is not a lightweight operation and
  /// should not be in the hot path.
  Status SerializeToArchiveString(std::string* out) const WARN_UNUSED_RESULT;
  Status SerializeToArchiveString(std::stringstream* out) const WARN_UNUSED_RESULT;

  /// Divides all counters by n
  void Divide(int n);

  void GetChildren(std::vector<RuntimeProfile*>* children);

  /// Gets all profiles in tree, including this one.
  void GetAllChildren(std::vector<RuntimeProfile*>* children);

  /// Returns the number of counters in this profile
  int num_counters() const { return counter_map_.size(); }

  /// Returns name of this profile
  const std::string& name() const { return name_; }

  /// *only call this on top-level profiles*
  /// (because it doesn't re-file child profiles)
  void set_name(const std::string& name) { name_ = name; }

  int64_t metadata() const { return metadata_; }
  void set_metadata(int64_t md) { metadata_ = md; }

  /// Derived counter function: return measured throughput as input_value/second.
  static int64_t UnitsPerSecond(
      const Counter* total_counter, const Counter* timer);

  /// Derived counter function: return aggregated value
  static int64_t CounterSum(const std::vector<Counter*>* counters);

  /// Add a rate counter to the current profile based on src_counter with name.
  /// The rate counter is updated periodically based on the src counter.
  /// The rate counter has units in src_counter unit per second.
  /// StopPeriodicCounters() must be called to stop the periodic updating before this
  /// profile is destroyed. The periodic updating can be stopped earlier by calling
  /// PeriodicCounterUpdater::StopRateCounter() if 'src_counter' stops changing.
  Counter* AddRateCounter(const std::string& name, Counter* src_counter);

  /// Same as 'AddRateCounter' above except values are taken by calling fn.
  /// The resulting counter will be of 'unit'.
  Counter* AddRateCounter(const std::string& name, DerivedCounterFunction fn,
      TUnit::type unit);

  /// Add a sampling counter to the current profile based on src_counter with name.
  /// The sampling counter is updated periodically based on the src counter by averaging
  /// the samples taken from the src counter.
  /// The sampling counter has the same unit as src_counter unit.
  /// StopPeriodicCounters() must be called to stop the periodic updating before this
  /// profile is destroyed. The periodic updating can be stopped earlier by calling
  /// PeriodicCounterUpdater::StopSamplingCounter() if 'src_counter' stops changing.
  Counter* AddSamplingCounter(const std::string& name, Counter* src_counter);

  /// Same as 'AddSamplingCounter' above except the samples are taken by calling fn.
  Counter* AddSamplingCounter(const std::string& name, DerivedCounterFunction fn);

  /// Create a set of counters, one per bucket, to store the sampled value of src_counter.
  /// The 'src_counter' is sampled periodically to obtain the index of the bucket to
  /// increment. E.g. if the value of 'src_counter' is 3, the bucket at index 3 is
  /// updated. If the index exceeds the index of the last bucket, the last bucket is
  /// updated.
  ///
  /// The created counters do not appear in the profile when serialized or
  /// pretty-printed. The caller must do its own processing of the counter value
  /// (e.g. converting it to an info string).
  /// TODO: make this interface more consistent and sane.
  ///
  /// StopPeriodicCounters() must be called to stop the periodic updating before this
  /// profile is destroyed. The periodic updating can be stopped earlier by calling
  /// PeriodicCounterUpdater::StopBucketingCounters() if 'buckets' stops changing.
  std::vector<Counter*>* AddBucketingCounters(Counter* src_counter, int num_buckets);

  /// Create a time series counter. This begins sampling immediately. This counter
  /// contains a number of samples that are collected periodically by calling sample_fn().
  /// StopPeriodicCounters() must be called to stop the periodic updating before this
  /// profile is destroyed. The periodic updating can be stopped earlier by calling
  /// PeriodicCounterUpdater::StopTimeSeriesCounter() if the input stops changing.
  /// Note: these counters don't get merged (to make average profiles)
  TimeSeriesCounter* AddTimeSeriesCounter(const std::string& name,
      TUnit::type unit, DerivedCounterFunction sample_fn);

  /// Same as above except the samples are collected from 'src_counter'.
  TimeSeriesCounter* AddTimeSeriesCounter(const std::string& name, Counter* src_counter);

  /// Recursively compute the fraction of the 'total_time' spent in this profile and
  /// its children.
  /// This function updates local_time_percent_ for each profile.
  void ComputeTimeInProfile();

 private:
  /// Pool for allocated counters. Usually owned by the creator of this
  /// object, but occasionally allocated in the constructor.
  ObjectPool* pool_;

  /// Name for this runtime profile.
  std::string name_;

  /// user-supplied, uninterpreted metadata.
  int64_t metadata_;

  /// True if this profile is an average derived from other profiles.
  /// All counters in this profile must be of unit AveragedCounter.
  bool is_averaged_profile_;

  /// Map from counter names to counters.  The profile owns the memory for the
  /// counters.
  typedef std::map<std::string, Counter*> CounterMap;
  CounterMap counter_map_;

  /// Map from parent counter name to a set of child counter name.
  /// All top level counters are the child of "" (root).
  typedef std::map<std::string, std::set<std::string>> ChildCounterMap;
  ChildCounterMap child_counter_map_;

  /// A set of bucket counters registered in this runtime profile.
  std::set<std::vector<Counter*>*> bucketing_counters_;

  /// Rate counters, which also appear in 'counter_map_'. Tracked separately to enable
  /// stopping the counters.
  std::vector<Counter*> rate_counters_;

  /// Sampling counters, which also appear in 'counter_map_'. Tracked separately to enable
  /// stopping the counters.
  std::vector<Counter*> sampling_counters_;

  /// Time series counters. These do not appear in 'counter_map_'. Tracked separately
  /// because they are displayed separately in the profile and need to be stopped.
  typedef std::map<std::string, TimeSeriesCounter*> TimeSeriesCounterMap;
  TimeSeriesCounterMap time_series_counter_map_;

  /// True if this profile has active periodic counters, including bucketing, rate,
  /// sampling and time series counters.
  bool has_active_periodic_counters_ = false;

  /// Protects counter_map_, child_counter_map_, bucketing_counters_, rate_counters_,
  /// sampling_counters_, time_series_counter_map_, and has_active_periodic_counters_.
  mutable SpinLock counter_map_lock_;

  /// Child profiles.  Does not own memory.
  /// We record children in both a map (to facilitate updates) and a vector
  /// (to print things in the order they were registered)
  typedef std::map<std::string, RuntimeProfile*> ChildMap;
  ChildMap child_map_;

  /// Vector of (profile, indentation flag).
  typedef std::vector<std::pair<RuntimeProfile*, bool>> ChildVector;
  ChildVector children_;

  /// Protects child_map_ and children_.
  mutable SpinLock children_lock_;

  typedef std::map<std::string, std::string> InfoStrings;
  InfoStrings info_strings_;

  /// Keeps track of the order in which InfoStrings are displayed when printed.
  typedef std::vector<std::string> InfoStringsDisplayOrder;
  InfoStringsDisplayOrder info_strings_display_order_;

  /// Protects info_strings_ and info_strings_display_order_.
  mutable SpinLock info_strings_lock_;

  typedef std::map<std::string, EventSequence*> EventSequenceMap;
  EventSequenceMap event_sequence_map_;

  /// Protects event_sequence_map_.
  mutable SpinLock event_sequence_lock_;

  typedef std::map<std::string, SummaryStatsCounter*> SummaryStatsCounterMap;
  SummaryStatsCounterMap summary_stats_map_;

  /// Protects summary_stats_map_.
  mutable SpinLock summary_stats_map_lock_;

  Counter counter_total_time_;

  /// Total time spent waiting (on non-children) that should not be counted when
  /// computing local_time_percent_. This is updated for example in the exchange
  /// node when waiting on the sender from another fragment.
  Counter inactive_timer_;

  /// Time spent in just in this profile (i.e. not the children) as a fraction
  /// of the total time in the entire profile tree.
  double local_time_percent_;

  /// Time spent in this node (not including the children). Computed in
  /// ComputeTimeInProfile()
  int64_t local_time_ns_;

  /// Constructor used by Create().
  RuntimeProfile(ObjectPool* pool, const std::string& name, bool is_averaged_profile);

  /// Update a subtree of profiles from nodes, rooted at *idx.
  /// On return, *idx points to the node immediately following this subtree.
  void Update(const std::vector<TRuntimeProfileNode>& nodes, int* idx);

  /// Helper function to compute compute the fraction of the total time spent in
  /// this profile and its children.
  /// Called recusively.
  void ComputeTimeInProfile(int64_t total_time);

  /// Implementation of AddInfoString() and AppendInfoString(). If 'append' is false,
  /// implements AddInfoString(), otherwise implements AppendInfoString().
  void AddInfoStringInternal(
      const std::string& key, const std::string& value, bool append);

  /// Name of the counter maintaining the total time.
  static const std::string TOTAL_TIME_COUNTER_NAME;
  static const std::string LOCAL_TIME_COUNTER_NAME;
  static const std::string INACTIVE_TIME_COUNTER_NAME;

  /// Create a subtree of runtime profiles from nodes, starting at *node_idx.
  /// On return, *node_idx is the index one past the end of this subtree
  static RuntimeProfile* CreateFromThrift(
      ObjectPool* pool, const std::vector<TRuntimeProfileNode>& nodes, int* node_idx);

  /// Internal implementations of the Add*Counter() functions for use when the caller
  /// holds counter_map_lock_. Also returns 'created', which is true if a new counter was
  /// created and false if a counter with the given name already existed.
  Counter* AddCounterLocked(const std::string& name, TUnit::type unit,
      const std::string& parent_counter_name, bool* created);
  HighWaterMarkCounter* AddHighWaterMarkCounterLocked(const std::string& name,
      TUnit::type unit, const std::string& parent_counter_name, bool* created);
  ConcurrentTimerCounter* AddConcurrentTimerCounterLocked(const std::string& name,
      TUnit::type unit, const std::string& parent_counter_name, bool* created);

  ///  Inserts 'child' before the iterator 'insert_pos' in 'children_'.
  /// 'children_lock_' must be held by the caller.
  void AddChildLocked(
      RuntimeProfile* child, bool indent, ChildVector::iterator insert_pos);

  /// Print the child counters of the given counter name
  static void PrintChildCounters(const std::string& prefix,
      const std::string& counter_name, const CounterMap& counter_map,
      const ChildCounterMap& child_counter_map, std::ostream* s);
};

}

#endif
