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

#include <iosfwd>
#include <mutex>

#include <boost/function.hpp>
#include <rapidjson/document.h>
#include "common/atomic.h"
#include "common/status.h"
#include "testutil/gtest-util.h"
#include "util/spinlock.h"

#include "gen-cpp/RuntimeProfile_types.h"

namespace impala {

class ObjectPool;
class RuntimeProfile;

/// RuntimeProfileBase is the common subclass of all runtime profiles.
///
/// Runtime profile is a group of profiling counters.  It supports adding named counters
/// and being able to serialize and deserialize them.
/// The profiles support a tree structure to form a hierarchy of counters.
/// Runtime profiles supports measuring wall clock rate based counters.  There is a
/// single thread per process that will convert an amount (i.e. bytes) counter to a
/// corresponding rate based counter.  This thread wakes up at fixed intervals and updates
/// all of the rate counters.
///
/// Runtime profile counters can be of several types. See their definition in
/// runtime-profile-counters.h for more details.
///
/// - Counter: Tracks a single value or bitmap. Also serves as the base class for several
///   |   other counters.
///   |
///   - AveragedCounter: Maintains a set of child counters. Its current value is the
///   |     average of the current values of its children.
///   |
///   - ConcurrentTimerCounter: Wraps a ConcurrentStopWatch to track concurrent running
///   |     time for multiple threads.
///   |
///   - DerivedCounter: Computes its current value by calling a function passed during
///   |     construction.
///   |
///   - HighWaterMarkCounter: Keeps track of the highest value seen so far.
///   |
///   - SummaryStatsCounter: Keeps track of minimum, maximum, and average value of all
///         values seen so far.
///
/// - EventSequence: Captures a sequence of events, each added by calling MarkEvent().
///       Events have a text label and a time, relative to when the sequence was started.
///
/// - ThreadCounters: Tracks thread runtime information, such as total time, user time,
///       sys time.
///
/// - TimeSeriesCounter (abstract): Keeps track of a value over time. Has two
///   |   implementations.
///   |
///   - SamplingTimeSeriesCounter: Maintains a fixed array of 64 values and resamples if
///   |     more value than that are added.
///   |
///   - ChunkedTimeSeriesCounter: Maintains an unbounded vector of values. Supports
///         clearing its values after they have been retrieved, and will track the number
///         of previously retrieved values.
///
/// All methods are thread-safe unless otherwise mentioned.
class RuntimeProfileBase {
 public:
  // Amount of information to include when emitting a (non-thrift) profile.
  enum class Verbosity {
    // Limit output to the most essential information.
    MINIMAL = 0,
    // Output level should be approximately the same as Impala 3.x output.
    LEGACY = 1,
    // Default output level for new profile - include some additional descriptive stats.
    DEFAULT = 2,
    // Includes more detailed descriptive statistics.
    EXTENDED = 3,
    // Dump as much information as is available.
    FULL = 4,
  };

  class Counter {
   public:
    Counter(TUnit::type unit, int64_t value = 0)
      : value_(value), unit_(unit), is_system_(false) {}
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

    double double_value() const {
      int64_t v = value();
      return *reinterpret_cast<const double*>(&v);
    }

    /// Prints the contents of the counter in a name: value format, prefixed on
    /// each line by 'prefix' and terminated with a newline.
    virtual void PrettyPrint(const std::string& prefix, const std::string& name,
        Verbosity verbosity, std::ostream* s) const;

    /// Builds a new Value into 'val', using (if required) the allocator from
    /// 'document'. Should set the following fields where appropriate:
    /// counter_name, value, kind, unit
    virtual void ToJson(
        Verbosity verbosity, rapidjson::Document& document, rapidjson::Value* val) const;

    TUnit::type unit() const { return unit_; }

    void SetIsSystem() { is_system_ = true; }
    bool GetIsSystem() const { return  is_system_; }

   protected:
    friend class RuntimeProfile;

    AtomicInt64 value_;
    TUnit::type unit_;
    bool is_system_;
  };

  class AveragedCounter;
  class SummaryStatsCounter;

  typedef boost::function<int64_t()> SampleFunction;

  virtual ~RuntimeProfileBase();

  /// Gets the counter object with 'name'.  Returns NULL if there is no counter with
  /// that name.
  Counter* GetCounter(const std::string& name) const;

  /// Adds all counters with 'name' that are registered either in this or
  /// in any of the child profiles to 'counters'.
  void GetCounters(const std::string& name, std::vector<Counter*>* counters);

  /// Recursively compute the fraction of the 'total_time' spent in this profile and
  /// its children. This function updates local_time_frac_ for each profile.
  void ComputeTimeInProfile();

  /// Prints the contents of the profile in a name: value format.
  /// Does not hold locks when it makes any function calls.
  /// This first overload prints at the configured default verbosity for this process.
  /// The second overload allows the caller to specify the verbosity.
  void PrettyPrint(std::ostream* s, const std::string& prefix = "") const;
  void PrettyPrint(
      Verbosity verbosity, std::ostream* s, const std::string& prefix = "") const;

  void GetChildren(std::vector<RuntimeProfileBase*>* children) const;

  /// Gets all profiles in tree, including this one.
  void GetAllChildren(std::vector<RuntimeProfileBase*>* children);

  /// Adds a string to the runtime profile.  If a value already exists for 'key',
  /// the value will be updated.
  /// TODO: IMPALA-9846: this can be moved to RuntimeProfile once we remove callsites for
  /// this function on AggregatedRuntimeProfile.
  void AddInfoString(const std::string& key, const std::string& value);

  /// Serializes an entire runtime profile tree to thrift. Can be overridden by subclasses
  /// to add additional info. The thrift representation is the public-facing
  /// representation if this is a RuntimeProfile that is the root of the whole query's
  /// tree. Otherwise this generates a serialized profile for internal use.
  /// Does not hold locks when it makes any function calls.
  virtual void ToThrift(TRuntimeProfileTree* tree) const;

  /// Returns name of this profile
  const std::string& name() const { return name_; }
  const TRuntimeProfileNodeMetadata& metadata() const { return metadata_; }

  /// Returns the counter for the total elapsed time.
  Counter* total_time_counter() const { return total_time_counter_; }
  /// Returns the counter for the inactive time.
  Counter* inactive_timer() const { return inactive_timer_; }
  int64_t local_time() const { return local_time_ns_.Load(); }
  int64_t total_time() const { return total_time_ns_.Load(); }

  /// Returns the number of counters in this profile. Used for unit tests.
  int num_counters() const;

  /// Return the number of input instances that contributed to this profile.
  /// Always 1 for non-aggregated profiles.
  virtual int GetNumInputProfiles() const = 0;

  /// Identify skews in certain counters for certain operators from children_ profiles
  //  recursively. Add the skew info (if identified) as follows.
  ///  1. a list of profile names to the 'root' profile with key SKEW_SUMMARY;
  ///  2. a list of counter names plus the details in those child profiles with key
  ///     SKEW_DETAILS.
  static const std::string SKEW_SUMMARY;
  static const std::string SKEW_DETAILS;
  /// Argument 'threshold' provides the threshold used in the formula to detect skew.
  void AddSkewInfo(RuntimeProfileBase* root, double threshold);

  // Parse verbosity from string (either enum name or integral value).
  // Return true if successful, false if it's not a valid verbosity value.
  static bool ParseVerbosity(const std::string& str, Verbosity* out);

 protected:
  FRIEND_TEST(CountersTest, AggregateEventSequences);

  /// Name of the counter maintaining the total time.
  static const std::string TOTAL_TIME_COUNTER_NAME;
  static const std::string LOCAL_TIME_COUNTER_NAME;
  static const std::string INACTIVE_TIME_COUNTER_NAME;

  /// Pool for allocated counters. Usually owned by the creator of this
  /// object, but occasionally allocated in the constructor.
  ObjectPool* pool_;

  /// Name for this runtime profile.
  std::string name_;

  /// Detailed metadata that identifies the plan node, sink, etc.
  TRuntimeProfileNodeMetadata metadata_;

  /// Map from counter names to counters.  The profile owns the memory for the
  /// counters.
  typedef std::map<std::string, Counter*> CounterMap;
  CounterMap counter_map_;

  /// Map from parent counter name to a set of child counter name.
  /// All top level counters are the child of "" (root).
  typedef std::map<std::string, std::set<std::string>> ChildCounterMap;
  ChildCounterMap child_counter_map_;

  /// Protects counter_map_, child_counter_map_, RuntimeProfile::bucketing_counters_,
  /// RuntimeProfile::rate_counters_, RuntimeProfile::sampling_counters_,
  /// RuntimeProfile::time_series_counter_map_, and
  /// RuntimeProfile::has_active_periodic_counters_.
  mutable SpinLock counter_map_lock_;

  /// Reference to counter_map_[TOTAL_TIME_COUNTER_NAME] for thread-safe access without
  /// acquiring 'counter_map_lock_'.
  Counter* const total_time_counter_;

  /// Reference to counter_map_[INACTIVE_TIME_COUNTER_NAME] for thread-safe access without
  /// acquiring 'counter_map_lock_'.
  Counter* const inactive_timer_;

  /// TODO: IMPALA-9846: info strings can be moved to RuntimeProfile once we remove
  /// callsites for this function on AggregatedRuntimeProfile.
  typedef std::map<std::string, std::string> InfoStrings;
  InfoStrings info_strings_;

  /// Keeps track of the order in which InfoStrings are displayed when printed.
  typedef std::vector<std::string> InfoStringsDisplayOrder;
  InfoStringsDisplayOrder info_strings_display_order_;

  /// Protects info_strings_ and info_strings_display_order_.
  mutable SpinLock info_strings_lock_;

  /// Child profiles. Does not own memory.
  /// We record children in both a map (to facilitate updates) and a vector
  /// (to print things in the order they were registered)
  typedef std::map<std::string, RuntimeProfileBase*> ChildMap;
  ChildMap child_map_;

  /// Vector of (profile, indentation flag).
  typedef std::vector<std::pair<RuntimeProfileBase*, bool>> ChildVector;
  ChildVector children_;

  /// Protects child_map_ and children_.
  mutable SpinLock children_lock_;

  /// Time spent in just in this profile (i.e. not the children) as a fraction
  /// of the total time in the entire profile tree. This is a double's bit pattern
  /// stored in an integer. Computed in ComputeTimeInProfile().
  /// Atomic so that it can be read concurrently with the value being calculated.
  AtomicInt64 local_time_frac_{0};

  /// Time spent in this node (not including the children). Computed in
  /// ComputeTimeInProfile(). Atomic b/c it can be read concurrently with
  /// ComputeTimeInProfile() executing.
  AtomicInt64 local_time_ns_{0};

  /// Total time spent in this node. Computed in ComputeTimeInProfile() and is
  /// the maximum of the total time spent in children and the value of
  /// counter_total_time_. Atomic b/c it can be read concurrently with
  /// ComputeTimeInProfile() executing.
  AtomicInt64 total_time_ns_{0};

  RuntimeProfileBase(ObjectPool* pool, const std::string& name,
      Counter* total_time_counter, Counter* inactive_timer,
      bool add_default_counters = true);

  ///  Inserts 'child' before the iterator 'insert_pos' in 'children_'.
  /// 'children_lock_' must be held by the caller.
  void AddChildLocked(
      RuntimeProfileBase* child, bool indent, ChildVector::iterator insert_pos);

  /// Helper for the various Update*() methods that finds an existing child with 'name'
  /// or creates it by calling 'create_fn', which is a zero-argument function that returns
  /// a pointer to a subclass of RuntimeProfileBase.
  ///
  /// If the child is created, it is inserted at 'insert_pos'. In either case 'insert_pos'
  /// is advanced to after the returned child. When callers add children in sequence, this
  /// is sufficient to preserve the order of children  if children are added after the
  /// first Update*() call (IMPALA-6694).  E.g. if the first update sends [B, D]
  /// and the second update sends [A, B, C, D], then this code makes sure that children_
  /// is [A, B, C, D] afterwards.
  ///
  /// 'children_lock_' must be held by the caller.
  template <typename CreateFn>
  RuntimeProfileBase* AddOrCreateChild(const std::string& name,
      ChildVector::iterator* insert_pos, CreateFn create_fn, bool indent);

  /// Update 'child_counters_map_' to add the entries in 'src'.
  /// Call must hold 'counter_map_lock_' via 'lock'.
  void UpdateChildCountersLocked(const std::unique_lock<SpinLock>& lock,
      const ChildCounterMap& src);

  /// Clear all chunked time series counters in this profile and all children.
  virtual void ClearChunkedTimeSeriesCounters();

  struct CollectedNode {
    CollectedNode(const RuntimeProfileBase* node, bool indent, int num_children)
      : node(node), indent(indent), num_children(num_children) {}
    const RuntimeProfileBase* const node;
    const bool indent;
    const int num_children;
  };

  /// Collect this node and descendants into 'nodes'. The order is a pre-order traversal
  /// 'indent' is true if this node should be indented.
  void CollectNodes(bool indent, std::vector<CollectedNode>* nodes) const;

  /// Helpers to serialize the individual plan nodes to thrift.
  void ToThriftHelper(std::vector<TRuntimeProfileNode>* nodes) const;
  void ToThriftHelper(TRuntimeProfileNode* out_node) const;

  /// Adds subclass-specific state to 'out_node'.
  virtual void ToThriftSubclass(
      std::vector<std::pair<const std::string&, const Counter*>>& counter_map_entries,
      TRuntimeProfileNode* out_node) const = 0;

  /// Create a subtree of runtime profiles from nodes, starting at *node_idx.
  /// On return, *node_idx is the index one past the end of this subtree.
  static RuntimeProfileBase* CreateFromThriftHelper(
      ObjectPool* pool, const std::vector<TRuntimeProfileNode>& nodes, int* node_idx);

  /// Init subclass-specific state from 'node'.
  virtual void InitFromThrift(const TRuntimeProfileNode& node, ObjectPool* pool) = 0;

  /// Helper for ToJson().
  void ToJsonHelper(
      Verbosity verbosity, rapidjson::Value* parent, rapidjson::Document* d) const;

  /// Adds subclass-specific state to 'parent'.
  virtual void ToJsonSubclass(
      Verbosity verbosity, rapidjson::Value* parent, rapidjson::Document* d) const = 0;

  /// Print the child counters of the given counter name.
  static void PrintChildCounters(const std::string& prefix, Verbosity verbosity,
      const std::string& counter_name, const CounterMap& counter_map,
      const ChildCounterMap& child_counter_map, std::ostream* s);

  /// Print info strings. Implemented by subclass which may store them
  /// in different ways
  virtual void PrettyPrintInfoStrings(
      std::ostream* s, const std::string& prefix, Verbosity verbosity) const = 0;

  /// Print any additional counters from the base class.
  virtual void PrettyPrintSubclassCounters(
      std::ostream* s, const std::string& prefix, Verbosity verbosity) const = 0;

  /// Add all the counters of this instance into the given parent node in JSON format
  /// Args:
  ///   parent: the root node to add all the counters
  ///   d: document of this json, could be used to get Allocator
  ///   counter_name: this will be used to find its child counters in child_counter_map
  ///   counter_map: A map of counters name to counter
  ///   child_counter_map: A map of counter to its child counters
  void ToJsonCounters(Verbosity verbosity, rapidjson::Value* parent,
      rapidjson::Document* d, const string& counter_name, const CounterMap& counter_map,
      const ChildCounterMap& child_counter_map) const;

  /// Implementation of AddInfoString() and AppendInfoString(). If 'append' is false,
  /// implements AddInfoString(), otherwise implements AppendInfoString().
  /// Redaction rules are applied on the info string if 'redact' is true.
  /// Trailing whitespace is removed.
  /// TODO: IMPALA-9846: this can be moved to RuntimeProfile once we remove callsites for
  /// this function on AggregatedRuntimeProfile.
  void AddInfoStringInternal(
      const std::string& key, std::string value, bool append, bool redact = false);
};

/// A standard runtime profile that can be mutated and updated.
class RuntimeProfile : public RuntimeProfileBase {
 public:
  // Import the nested class names from the base class so we can still refer
  // to them as RuntimeProfile::Counter, etc in the rest of the codebase.
  using RuntimeProfileBase::Counter;
  using RuntimeProfileBase::SummaryStatsCounter;
  class ConcurrentTimerCounter;
  class DerivedCounter;
  class HighWaterMarkCounter;
  class EventSequence;
  class ThreadCounters;
  class TimeSeriesCounter;
  class SamplingTimeSeriesCounter;
  class ChunkedTimeSeriesCounter;

  /// String constants for instruction count names.
  static const string FRAGMENT_INSTANCE_LIFECYCLE_TIMINGS;
  static const string BUFFER_POOL;
  static const string DEQUEUE;
  static const string ENQUEUE;
  static const string HASH_TABLE;
  static const string PREFIX_FILTER;
  static const string PREFIX_GROUPING_AGGREGATOR;

  /// Create a runtime profile object with 'name'. The profile, counters and any other
  /// structures owned by the profile are allocated from 'pool'.
  /// If add_default_counters is false, TotalTime and InactiveTotalTime will be
  /// hidden in the newly created RuntimeProfile node.
  static RuntimeProfile* Create(
      ObjectPool* pool, const std::string& name, bool add_default_counters = true);

  ~RuntimeProfile();

  /// Deserialize a runtime profile tree from thrift. Allocated objects are stored in
  /// 'pool'.
  static RuntimeProfile* CreateFromThrift(ObjectPool* pool,
      const TRuntimeProfileTree& profiles);

  /// Adds a child profile.
  /// Checks if 'child' is already added by searching for its name in the
  /// child map, and only adds it if the name doesn't exist.
  /// 'indent' indicates whether the child will be printed w/ extra indentation
  /// relative to the parent.
  /// If location is non-null, child will be inserted after location.  Location must
  /// already be added to the profile.
  void AddChild(
      RuntimeProfileBase* child, bool indent = true, RuntimeProfile* location = NULL);

  /// Adds a child profile, similarly to AddChild(). The child profile is put before any
  /// existing profiles.
  void PrependChild(RuntimeProfileBase* child, bool indent = true);

  /// Creates a new child profile with the given 'name'. A child profile with that name
  /// must not already exist. If 'prepend' is true, prepended before other child profiles,
  /// otherwise appended after other child profiles.
  /// If 'add_default_counters' is false, TotalTime and InactiveTotalTime will be
  /// hidden in any newly created RuntimeProfile node.
  RuntimeProfile* CreateChild(const std::string& name, bool indent = true,
      bool prepend = false, bool add_default_counters = false);

  /// Sorts all children according to descending total time. Does not
  /// invalidate pointers to profiles.
  void SortChildrenByTotalTime();

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
  /// If add_default_counters is false, TotalTime and InactiveTotalTime will be
  /// hidden in any newly created RuntimeProfile node.
  void Update(
      const TRuntimeProfileTree& thrift_profile, bool add_default_counters = true);

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
      const SampleFunction& counter_fn,
      const std::string& parent_counter_name = "");

  /// Add a set of thread counters prefixed with 'prefix'. Returns a ThreadCounters object
  /// that the caller can update.  The counter is owned by the RuntimeProfile object.
  ThreadCounters* AddThreadCounters(const std::string& prefix);

  // Add a derived counter to capture the local time. This function can be called at most
  // once.
  void AddLocalTimeCounter(const SampleFunction& counter_fn);

  /// Same as AddInfoString(), except that this method applies the redaction
  /// rules on 'value' before adding it to the runtime profile.
  void AddInfoStringRedacted(const std::string& key, const std::string& value);

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
    const string& err_msg = codegen_status.ok() ? "" : codegen_status.msg().msg();
    AddCodegenMsg(codegen_enabled, err_msg, extra_label);
  }

  /// Creates and returns a new EventSequence (owned by the runtime
  /// profile) - unless a timer with the same 'key' already exists, in
  /// which case it is returned.
  /// TODO: EventSequences are not merged by Update()
  /// TODO: is this comment out of date?
  EventSequence* AddEventSequence(const std::string& key);
  EventSequence* AddEventSequence(const std::string& key, const TEventSequence& from);
  EventSequence* GetEventSequence(const std::string& name) const;

  /// Updates 'value' of info string with 'key'. No-op if the key doesn't exist.
  void UpdateInfoString(const std::string& key, std::string value);

  /// Returns a pointer to the info string value for 'key'.  Returns NULL if
  /// the key does not exist.
  const std::string* GetInfoString(const std::string& key) const;

  /// Gets the summary stats counter with 'name'. Returns NULL if there is no summary
  /// stats counter with that name.
  SummaryStatsCounter* GetSummaryStatsCounter(const std::string& name);

  /// Stops updating all counters in this profile that are periodically updated by a
  /// background thread (i.e. sampling, rate, bucketing and time series counters).
  /// Must be called before the profile is destroyed if any such counters are active.
  /// Does not stop counters on descendant profiles.
  void StopPeriodicCounters();

  /// *only call this on top-level profiles*
  /// (because it doesn't re-file child profiles)
  void set_name(const std::string& name) { name_ = name; }

  /// Called if this corresponds to a plan node. Sets metadata so that later code that
  /// analyzes the profile can identify this as the plan node's profile.
  void SetPlanNodeId(int node_id);

  /// Called if this corresponds to a data sink. Sets metadata so that later code that
  /// analyzes the profile can identify this as the data sink's profile.
  void SetDataSinkId(int sink_id);

  /// Derived counter function: return measured throughput as input_value/second.
  static int64_t UnitsPerSecond(const Counter* total_counter, const Counter* timer);

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
  Counter* AddRateCounter(const std::string& name, SampleFunction fn,
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
  Counter* AddSamplingCounter(const std::string& name, SampleFunction fn);

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

  /// Creates a sampling time series counter. This begins sampling immediately. This
  /// counter contains a number of samples that are collected periodically by calling
  /// sample_fn(). StopPeriodicCounters() must be called to stop the periodic updating
  /// before this profile is destroyed. The periodic updating can be stopped earlier by
  /// calling PeriodicCounterUpdater::StopTimeSeriesCounter() if the input stops changing.
  /// Note: these counters don't get merged (to make average profiles)
  TimeSeriesCounter* AddSamplingTimeSeriesCounter(const std::string& name,
      TUnit::type unit, SampleFunction sample_fn, bool is_system = false);

  /// Same as above except the samples are collected from 'src_counter'.
  TimeSeriesCounter* AddSamplingTimeSeriesCounter(const std::string& name, Counter*
      src_counter, bool is_system = false);

  /// Adds a chunked time series counter to the profile. This begins sampling immediately.
  /// This counter will collect new samples periodically by calling 'sample_fn()'. Samples
  /// are not re-sampled into larger intervals, instead owners of this profile can call
  /// ClearChunkedTimeSeriesCounters() to reset the sample buffers of all chunked time
  /// series counters, e.g. after their current values have been transmitted to a remote
  /// node for profile aggregation.
  TimeSeriesCounter* AddChunkedTimeSeriesCounter(
      const std::string& name, TUnit::type unit, SampleFunction sample_fn);

  /// Clear all chunked time series counters in this profile and all children.
  void ClearChunkedTimeSeriesCounters() override;

  void ToThrift(TRuntimeProfileTree* tree) const override;

  /// Store an entire runtime profile tree into JSON document 'd'.
  /// This first overload prints at the configured default verbosity for this process.
  /// The second overload allows the caller to specify the verbosity.
  void ToJson(rapidjson::Document* d) const;
  void ToJson(Verbosity verbosity, rapidjson::Document* d) const;

  /// Converts a JSON Document representation of a profile. If 'pretty' is true,
  /// generates a pretty-printed string representation. If 'pretty' is false, generates
  /// a dense single-line representation.
  static void JsonProfileToString(
      const rapidjson::Document& json_profile, bool pretty, std::ostream* string_output);

  /// Serializes the runtime profile to a buffer.  This first serializes the
  /// object using thrift compact binary format and then gzip compresses it.
  /// This is not a lightweight operation and should not be in the hot path.
  Status Compress(std::vector<uint8_t>* out) const;

  /// Deserializes a compressed profile into a TRuntimeProfileTree. 'compressed_profile'
  /// is expected to have been serialized by Compress().
  static Status DecompressToThrift(
      const std::vector<uint8_t>& compressed_profile, TRuntimeProfileTree* out);

  /// Deserializes a compressed profile into a RuntimeProfile tree owned by 'pool'.
  /// 'compressed_profile' is expected to have been serialized by Compress().
  static Status DecompressToProfile(const std::vector<uint8_t>& compressed_profile,
      ObjectPool* pool, RuntimeProfile** out);

  /// Serializes the runtime profile to a string.  This first serializes the
  /// object using thrift compact binary format, then gzip compresses it and
  /// finally encodes it as base64.  This is not a lightweight operation and
  /// should not be in the hot path.
  Status SerializeToArchiveString(std::string* out) const;
  Status SerializeToArchiveString(std::stringstream* out) const;

  /// Deserializes a string into a TRuntimeProfileTree. 'archive_str' is expected to have
  /// been serialized by SerializeToArchiveString().
  static Status DeserializeFromArchiveString(
      const std::string& archive_str, TRuntimeProfileTree* out);

  /// Creates a profile from an 'archive_str' created by SerializeToArchiveString().
  static Status CreateFromArchiveString(const std::string& archive_str, ObjectPool* pool,
      RuntimeProfile** out, int32_t* profile_version_out);

  /// Set ExecSummary
  void SetTExecSummary(const TExecSummary& summary);

  /// Get a copy of exec_summary to t_exec_summary
  void GetExecSummary(TExecSummary* t_exec_summary) const;

  /// Print all the event timers as timeline into output.
  void GetTimeline(std::string* output) const;

 protected:
  virtual int GetNumInputProfiles() const override { return 1; }

  /// Adds subclass-specific state to 'out_node'.
  void ToThriftSubclass(
      std::vector<std::pair<const std::string&, const Counter*>>& counter_map_entries,
      TRuntimeProfileNode* out_node) const override;

  /// Init subclass-specific state from 'node'.
  void InitFromThrift(const TRuntimeProfileNode& node, ObjectPool* pool) override;

  /// Adds subclass-specific state to 'parent'.
  void ToJsonSubclass(Verbosity verbosity, rapidjson::Value* parent,
      rapidjson::Document* d) const override;

  /// Print info strings from this subclass.
  void PrettyPrintInfoStrings(
      std::ostream* s, const std::string& prefix, Verbosity verbosity) const override;

  void PrettyPrintTimeline(std::ostream* s, const string& prefix) const;

  /// Print any additional counters from this subclass.
  void PrettyPrintSubclassCounters(
      std::ostream* s, const std::string& prefix, Verbosity verbosity) const override;

 private:
  friend class AggregatedRuntimeProfile;

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

  typedef std::map<std::string, EventSequence*> EventSequenceMap;
  EventSequenceMap event_sequence_map_;

  /// Protects event_sequence_map_.
  mutable SpinLock event_sequence_lock_;

  typedef std::map<std::string, SummaryStatsCounter*> SummaryStatsCounterMap;
  SummaryStatsCounterMap summary_stats_map_;

  /// Protects summary_stats_map_.
  mutable SpinLock summary_stats_map_lock_;

  Counter builtin_counter_total_time_{TUnit::TIME_NS};

  /// Total time spent waiting (on non-children) that should not be counted when
  /// computing local_time_frac_. This is updated for example in the exchange
  /// node when waiting on the sender from another fragment.
  Counter builtin_inactive_timer_{TUnit::TIME_NS};

  /// The Exec Summary
  TExecSummary t_exec_summary_;

  /// Protects exec_summary.
  mutable SpinLock t_exec_summary_lock_;

  /// Constructor used by Create().
  RuntimeProfile(
      ObjectPool* pool, const std::string& name, bool add_default_counters = true);

  /// Update a subtree of profiles from nodes, rooted at *idx.
  /// On return, *idx points to the node immediately following this subtree.
  /// If add_default_counters is false, TotalTime and InactiveTotalTime will be
  /// hidden in any newly created RuntimeProfile node.
  void Update(
      const std::vector<TRuntimeProfileNode>& nodes, int* idx, bool add_default_counters);

  /// Send exec_summary to thrift
  void ExecSummaryToThrift(TRuntimeProfileTree* tree) const;

  /// Internal implementations of the Add*Counter() functions for use when the caller
  /// holds counter_map_lock_. Also returns 'created', which is true if a new counter was
  /// created and false if a counter with the given name already existed.
  Counter* AddCounterLocked(const std::string& name, TUnit::type unit,
      const std::string& parent_counter_name, bool* created);
  HighWaterMarkCounter* AddHighWaterMarkCounterLocked(const std::string& name,
      TUnit::type unit, const std::string& parent_counter_name, bool* created);
  ConcurrentTimerCounter* AddConcurrentTimerCounterLocked(const std::string& name,
      TUnit::type unit, const std::string& parent_counter_name, bool* created);
};

/// An aggregated profile that results from combining one or more RuntimeProfiles.
/// Contains averaged and otherwise aggregated versions of the counters from the
/// input profiles.
///
/// An AggregatedRuntimeProfile can only have other AggregatedRuntimeProfiles as
/// children.
class AggregatedRuntimeProfile : public RuntimeProfileBase {
 public:
  /// Create an aggregated runtime profile with 'name'. The profile, counters and any
  /// other structures owned by the profile are allocated from 'pool'.
  /// The counters in this profile will be aggregated AveragedCounters merged
  /// from other profiles.
  ///
  /// 'is_root' must be true if this is the root of the averaged profile tree which
  /// will have UpdateAggregated*() called on it. The descendants of the root are
  /// created and updated by that call.
  static AggregatedRuntimeProfile* Create(ObjectPool* pool, const std::string& name,
      int num_input_profiles, bool is_root = true, bool add_default_counters = true);

  /// Updates the AveragedCounter counters in this profile with the counters from the
  /// 'src' profile, which is the input instance that was assigned index 'idx'. If a
  /// counter is present in 'src' but missing in this profile, a new AveragedCounter
  /// is created with the same name. Obtains locks on the counter maps and child
  /// counter maps in both this and 'src' profiles.
  ///
  /// Note that 'src' must be all instances of RuntimeProfile - no
  /// AggregatedRuntimeProfiles can be part of the input.
  void UpdateAggregatedFromInstance(
      RuntimeProfile* src, int idx, bool add_default_counters = true);

  /// Updates the AveragedCounter counters in this profile with the counters from the
  /// 'src' profile, which must be a serialized AggregatedProfile. The instances in
  /// 'src' must correspond to instances [start_idx, start_idx + # src instances) in
  /// this profile. If a counter is present in 'src' but missing in this profile, a
  /// new AveragedCounter is created with the same name. Obtains locks on the counter
  /// maps and child counter maps in this profile.
  void UpdateAggregatedFromInstances(
      const TRuntimeProfileTree& src, int start_idx, bool add_default_counters = true);

 protected:
  virtual int GetNumInputProfiles() const override { return num_input_profiles_; }

  /// Adds subclass-specific state to 'out_node'. 'counter_map_entries' is a snapshot
  /// of 'counter_map_'.
  void ToThriftSubclass(
      std::vector<std::pair<const std::string&, const Counter*>>& counter_map_entries,
      TRuntimeProfileNode* out_node) const override;

  /// Init subclass-specific state from 'node'.
  void InitFromThrift(const TRuntimeProfileNode& node, ObjectPool* pool) override;

  /// Adds subclass-specific state to 'parent'.
  void ToJsonSubclass(Verbosity verbosity, rapidjson::Value* parent,
      rapidjson::Document* d) const override;

  /// Print info strings from this subclass.
  void PrettyPrintInfoStrings(
      std::ostream* s, const std::string& prefix, Verbosity verbosity) const override;

  /// Print any additional counters from this subclass.
  void PrettyPrintSubclassCounters(
      std::ostream* s, const std::string& prefix, Verbosity verbosity) const override;

 private:
  /// Number of profiles that will contribute to this aggregated profile.
  const int num_input_profiles_;

  /// Names of the input profiles. Only use on averaged profiles that are the root
  /// of an averaged profile tree.
  /// The size is 'num_input_profiles_'. Protected by 'input_profile_name_lock_'
  std::vector<std::string> input_profile_names_;
  mutable SpinLock input_profile_name_lock_;

  /// Aggregated info strings from the input profile. The key is the info string name.
  /// The value vector contains an entry for every input profile.
  std::map<std::string, std::vector<std::string>> agg_info_strings_;

  /// Protects 'agg_info_strings_'.
  mutable SpinLock agg_info_strings_lock_;

  /// Per-instance summary stats. Value is the unit and all of the instance of the
  /// counter. Some of the counters may be null if the input profile did not have
  /// that counter present.
  /// Protected by 'summary_stats_map_lock_'.
  typedef std::map<std::string, std::pair<TUnit::type, std::vector<SummaryStatsCounter*>>>
      AggSummaryStatsCounterMap;
  AggSummaryStatsCounterMap summary_stats_map_;

  /// Protects summary_stats_map_.
  mutable SpinLock summary_stats_map_lock_;

  struct AggEventSequence {
    // Unique labels in the event sequence. Values are [0, labels.size() - 1]
    std::map<std::string, int32_t> labels;

    // One entry per instance. Each entry contains the label indices for that instance's
    // event sequence.
    std::vector<std::vector<int32_t>> label_idxs;

    // One entry per instance. Each entry contains the timestamps for that instance's
    // event sequence.
    std::vector<std::vector<int64_t>> timestamps;
  };

  std::map<std::string, AggEventSequence> event_sequence_map_;

  /// Protects event_sequence_map_ and event_sequence_labels_.
  mutable SpinLock event_sequence_lock_;

  /// Time series counters. Protected by 'counter_map_lock_'.
  std::map<std::string, TAggTimeSeriesCounter> time_series_counter_map_;

  AggregatedRuntimeProfile(ObjectPool* pool, const std::string& name,
      int num_input_profiles, bool is_root, bool add_default_counters = true);

  /// Group the values in 'info_string_values' by value, with a vector of indices where
  /// that value appears. 'info_string_values' must be a value from 'info_strings_'.
  std::map<std::string, std::vector<int32_t>> GroupDistinctInfoStrings(
      const std::vector<std::string>& info_string_values) const;

  /// Helper for UpdateAggregatedFromInstance() that are invoked recursively on 'src'.
  void UpdateAggregatedFromInstanceRecursive(
      RuntimeProfile* src, int idx, bool add_default_counter = true);

  /// Helpers for UpdateAggregatedFromInstanceRecursive() that update particular parts
  /// of this profile node from 'src'.
  void UpdateCountersFromInstance(RuntimeProfile* src, int idx);
  void UpdateInfoStringsFromInstance(RuntimeProfile* src, int idx);
  void UpdateSummaryStatsFromInstance(RuntimeProfile* src, int idx);
  void UpdateEventSequencesFromInstance(RuntimeProfile* src, int idx);

  /// Helper for UpdateAggregatedFromInstances() that are invoked recursively on 'src'.
  void UpdateAggregatedFromInstancesRecursive(const TRuntimeProfileTree& src,
      int* node_idx, int start_idx, bool add_default_counter = true);

  /// Helpers for UpdateAggregatedFromInstancesRecursive() that update particular parts
  /// of this profile node from 'src'. 'src' must have an aggregated profile set.
  void UpdateFromInstances(
      const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool);
  void UpdateCountersFromInstances(
      const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool);
  void UpdateInfoStringsFromInstances(
      const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool);
  void UpdateSummaryStatsFromInstances(
      const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool);
  void UpdateEventSequencesFromInstances(
      const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool);

  /// Aggregate summary stats into a single counter. Entries in
  /// 'counter' may be NULL.
  static void AggregateSummaryStats(
      const std::vector<SummaryStatsCounter*> counters, SummaryStatsCounter* result);
};
}
