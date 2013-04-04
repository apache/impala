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


#ifndef IMPALA_UTIL_COUNTERS_H
#define IMPALA_UTIL_COUNTERS_H

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>
#include <sys/time.h>
#include <sys/resource.h>

#include "common/logging.h"
#include "common/object-pool.h"
#include "util/stopwatch.h"
#include "gen-cpp/RuntimeProfile_types.h"

namespace impala {

// Define macros for updating counters.  The macros make it very easy to disable
// all counters at compile time.  Set this to 0 to remove counters.  This is useful
// to do to make sure the counters aren't affecting the system.
#define ENABLE_COUNTERS 1

// Some macro magic to generate unique ids using __COUNTER__
#define CONCAT_IMPL(x, y) x##y
#define MACRO_CONCAT(x, y) CONCAT_IMPL(x, y)

#if ENABLE_COUNTERS
  #define ADD_COUNTER(profile, name, type) (profile)->AddCounter(name, type)
  #define ADD_TIMER(profile, name) (profile)->AddCounter(name, TCounterType::TIME_NS)
  #define SCOPED_TIMER(c) \
      ScopedTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c)
  #define COUNTER_UPDATE(c, v) (c)->Update(v)
  #define COUNTER_SET(c, v) (c)->Set(v)
  #define ADD_THREAD_COUNTERS(profile, prefix) (profile)->AddThreadCounters(prefix)
  #define SCOPED_THREAD_COUNTER_MEASUREMENT(c) \
    ThreadCounterMeasurement \
      MACRO_CONCAT(SCOPED_THREAD_COUNTER_MEASUREMENT, __COUNTER__)(c)
#else
  #define ADD_COUNTER(profile, name, type) NULL
  #define ADD_TIMER(profile, name) NULL
  #define SCOPED_TIMER(c)
  #define COUNTER_UPDATE(c, v)
  #define COUNTER_SET(c, v)
  #define ADD_THREADCOUNTERS(profile, prefix) NULL
  #define SCOPED_THREAD_COUNTER_MEASUREMENT(c)
#endif

class ObjectPool;

// Runtime profile is a group of profiling counters.  It supports adding named counters
// and being able to serialize and deserialize them.
// The profiles support a tree structure to form a hierarchy of counters.
// Runtime profiles supports measuring wall clock rate based counters.  There is a
// single thread per process that will convert an amount (i.e. bytes) counter to a
// corresponding rate based counter.  This thread wakes up at fixed intervals and updates
// all of the rate counters.
// Thread-safe.
class RuntimeProfile {
 public:
  class Counter {
   public:
    Counter(TCounterType::type type, int64_t value = 0) :
      value_(value),
      type_(type) {
    }
    virtual ~Counter(){}

    void Update(int64_t delta) {
      __sync_fetch_and_add(&value_, delta);
    }

    void Set(int64_t value) { value_ = value; }

    void SetRate(double value) {
      value_ = *reinterpret_cast<int64_t*>(&value);
    }

    virtual int64_t value() const { return value_; }

    TCounterType::type type() const { return type_; }

   private:
    friend class RuntimeProfile;

    int64_t value_;
    TCounterType::type type_;
  };

  typedef boost::function<int64_t ()> DerivedCounterFunction;

  // A DerivedCounter also has a name and type, but the value is computed.
  // Do not call Set() and Update().
  class DerivedCounter : public Counter {
   public:
    DerivedCounter(TCounterType::type type, const DerivedCounterFunction& counter_fn)
      : Counter(type),
        counter_fn_(counter_fn) {}

    virtual int64_t value() const {
      return counter_fn_();
    }

   private:
    DerivedCounterFunction counter_fn_;
  };

  // A set of counters that measure thread info, such as total time, user time, sys time.
  class ThreadCounters {
   private:
    friend class ThreadCounterMeasurement;
    friend class RuntimeProfile;

    Counter* total_time_; // total wall clock time
    Counter* user_time_;  // user CPU time
    Counter* sys_time_;   // system CPU time

    // The number of times a context switch resulted due to a process voluntarily giving
    // up the processor before its time slice was completed.
    Counter* voluntary_context_switches_;

    // The number of times a context switch resulted due to a higher priority process
    // becoming runnable or because the current process exceeded its time slice.
    Counter* involuntary_context_switches_;
  };

  // Create a runtime profile object with 'name'.  Counters and merged profile are
  // allocated from pool.
  RuntimeProfile(ObjectPool* pool, const std::string& name);

  ~RuntimeProfile();

  // Deserialize from thrift.  Runtime profiles are allocated from the pool.
  static RuntimeProfile* CreateFromThrift(ObjectPool* pool,
      const TRuntimeProfileTree& profiles);

  // Adds a child profile.  This is thread safe.
  // 'indent' indicates whether the child will be printed w/ extra indentation
  // relative to the parent.
  // If location is non-null, child will be inserted after location.  Location must
  // already be added to the profile.
  void AddChild(RuntimeProfile* child,
      bool indent = true, RuntimeProfile* location = NULL);

  // Merges the src profile into this one, combining counters that have an identical
  // path. Info strings from profiles are not merged. 'src' would be a const if it
  // weren't for locking.
  // Calling this concurrently on two RuntimeProfiles in reverse order results in
  // undefined behavior.
  void Merge(RuntimeProfile* src);

  // Updates this profile w/ the thrift profile: behaves like Merge(), except
  // that existing counters are updated rather than added up.
  // Info strings matched up by key and are updated or added, depending on whether
  // the key has already been registered.
  void Update(const TRuntimeProfileTree& thrift_profile);

  // Add a counter with 'name'/'type'.  Returns a counter object that the caller can
  // update.  The counter is owned by the RuntimeProfile object.
  // If the counter already exists, the existing counter object is returned.
  Counter* AddCounter(const std::string& name, TCounterType::type type);

  // Add a derived counter with 'name'/'type'. The counter is owned by the
  // RuntimeProfile object.
  // Returns NULL if the counter already exists.
  DerivedCounter* AddDerivedCounter(const std::string& name, TCounterType::type type,
      const DerivedCounterFunction& counter_fn);

  // Add a set of thread counters prefixed with 'prefix'. Returns a ThreadCounters object
  // that the caller can update.  The counter is owned by the RuntimeProfile object.
  ThreadCounters* AddThreadCounters(const std::string& prefix);

  // Gets the counter object with 'name'.  Returns NULL if there is no counter with
  // that name.
  Counter* GetCounter(const std::string& name);

  // Adds all counters with 'name' that are registered either in this or
  // in any of the child profiles to 'counters'.
  void GetCounters(const std::string& name, std::vector<Counter*>* counters);

  // Adds a string to the runtime profile.  If a value already exists for 'key',
  // the value will be updated.
  void AddInfoString(const std::string& key, const std::string& value);

  // Returns a pointer to the info string value for 'key'.  Returns NULL if
  // the key does not exist.
  const std::string* GetInfoString(const std::string& key);

  // Returns the counter for the total elapsed time.
  Counter* total_time_counter() { return &counter_total_time_; }

  // Prints the counters in a name: value format.
  // Does not hold locks when it makes any function calls.
  void PrettyPrint(std::ostream* s, const std::string& prefix="") const;

  // Serializes profile to thrift.
  // Does not hold locks when it makes any function calls.
  void ToThrift(TRuntimeProfileTree* tree);
  void ToThrift(std::vector<TRuntimeProfileNode>* nodes);

  // Serializes the runtime profile to a string.  This first serializes
  // the object using thrift compact binary format and then encodes
  // it as base64.  This is not a lightweight operation and should not
  // be in the hot path.
  std::string SerializeToBase64String() const;
  void SerializeToBase64String(std::stringstream* out) const;

  // Divides all counters by n
  void Divide(int n);

  void GetChildren(std::vector<RuntimeProfile*>* children);

  // Gets all profiles in tree, including this one.
  void GetAllChildren(std::vector<RuntimeProfile*>* children);

  // Returns the number of counters in this profile
  int num_counters() const { return counter_map_.size(); }

  // Returns name of this profile
  const std::string& name() const { return name_; }

  // *only call this on top-level profiles*
  // (because it doesn't re-file child profiles)
  void set_name(const std::string& name) { name_ = name; }

  int64_t metadata() const { return metadata_; }
  void set_metadata(int64_t md) { metadata_ = md; }

  // Derived counter function: return measured throughput as input_value/second.
  static int64_t UnitsPerSecond(
      const Counter* total_counter, const Counter* timer);

  // Derived counter function: return aggregated value
  static int64_t CounterSum(const std::vector<Counter*>* counters);

  // Add a rate counter to the current profile based on src_counter with name.
  // The rate counter is updated periodically based on the src counter.
  // The rate counter has units in src_counter unit per second.
  Counter* AddRateCounter(const std::string& name, Counter* src_counter);

  // Stops updating the value of 'rate_counter'. Rate counters are updated
  // periodically so should be removed as soon as the underlying amount counter is
  // no longer going to change.
  void StopRateCounterUpdates(Counter* rate_counter);

  // Recursively compute the fraction of the 'total_time' spent in this profile and
  // its children.
  // This function updates local_time_percent_ for each profile.
  void ComputeTimeInProfile();

 private:
  // Pool for allocated counters. Usually owned by the creator of this
  // object, but occasionally allocated in the constructor.
  ObjectPool* pool_;

  // True if we have to delete the pool_ on destruction.
  bool own_pool_;

  // Name for this runtime profile.
  std::string name_;

  // user-supplied, uninterpreted metadata.
  int64_t metadata_;

  // Map from counter names to counters.  The profile owns the memory for the
  // counters.
  typedef std::map<std::string, Counter*> CounterMap;
  CounterMap counter_map_;
  mutable boost::mutex counter_map_lock_;  // protects counter_map_

  // Child profiles.  Does not own memory.
  // We record children in both a map (to facilitate updates) and a vector
  // (to print things in the order they were registered)
  typedef std::map<std::string, RuntimeProfile*> ChildMap;
  ChildMap child_map_;
  // vector of (profile, indentation flag)
  typedef std::vector<std::pair<RuntimeProfile*, bool> > ChildVector;
  ChildVector children_;
  mutable boost::mutex children_lock_;  // protects child_map_ and children_

  typedef std::map<std::string, std::string> InfoStrings;
  InfoStrings info_strings_;
  mutable boost::mutex info_strings_lock_;

  Counter counter_total_time_;
  // Time spent in just in this profile (i.e. not the children) as a fraction
  // of the total time in the entire profile tree.
  double local_time_percent_;

  struct RateCounterInfo {
    Counter* src_counter;
    int64_t elapsed_ms;
  };

  // This is a static singleton object that is used to update all rate counters.
  struct RateCounterUpdateState {
    RateCounterUpdateState();

    // Tears down the update thread.
    ~RateCounterUpdateState();

    // Lock protecting state below
    boost::mutex lock;

    // If true, tear down the update thread.
    volatile bool done_;

    // Thread performing asynchronous updates.
    boost::scoped_ptr<boost::thread> update_thread;

    // A map of the dst (rate) counter to the src counter and elapsed time.
    typedef std::map<Counter*, RateCounterInfo> RateCounterMap;
    RateCounterMap counters;
  };

  // Singleton object that keeps track of all rate counters and the thread
  // for updating them.
  static RateCounterUpdateState rate_counters_state_;

  // Create a subtree of runtime profiles from nodes, starting at *node_idx.
  // On return, *node_idx is the index one past the end of this subtree
  static RuntimeProfile* CreateFromThrift(ObjectPool* pool,
      const std::vector<TRuntimeProfileNode>& nodes, int* node_idx);

  // Update a subtree of profiles from nodes, rooted at *idx.
  // On return, *idx points to the node immediately following this subtree.
  void Update(const std::vector<TRuntimeProfileNode>& nodes, int* idx);

  // Helper function to compute compute the fraction of the total time spent in
  // this profile and its children.
  // Called recusively.
  void ComputeTimeInProfile(int64_t total_time);

  // Registers a rate counter to be updated by the update thread.
  // dst/src is assumed to be of compatible types.
  static void RegisterRateCounter(Counter* src_counter, Counter* dst_counter);

  // Loop for rate counter update thread.  This thread wakes up once in a while
  // and updates all the added rate counters.
  static void RateCounterUpdateLoop();
};

// Utility class to update time elapsed when the object goes out of scope.
// 'T' must implement the StopWatch "interface" (Start,Stop,ElapsedTime) but
// we use templates not to not for virtual function overhead.
template<class T>
class ScopedTimer {
 public:
  ScopedTimer(RuntimeProfile::Counter* counter) :
    counter_(counter) {
    if (counter == NULL) return;
    DCHECK(counter->type() == TCounterType::TIME_NS);
    sw_.Start();
  }

  void Stop() {
    sw_.Stop();
  }

  void Start() {
    sw_.Start();
  }

  // Update counter when object is destroyed
  ~ScopedTimer() {
    if (counter_ != NULL) {
      sw_.Stop();
      counter_->Update(sw_.ElapsedTime());
    }
  }

 private:
  // Disable copy constructor and assignment
  ScopedTimer(const ScopedTimer& timer);
  ScopedTimer& operator=(const ScopedTimer& timer);

  T sw_;
  RuntimeProfile::Counter* counter_;
};

// Utility class to update ThreadCounter when the object goes out of scope or when Stop is
// called. Threads measurements will then be taken using getrusage.
// This is ~5x slower than ScopedTimer due to calling getrusage.
class ThreadCounterMeasurement {
 public:
  ThreadCounterMeasurement(RuntimeProfile::ThreadCounters* counters) :
    stop_(false), counters_(counters) {
    DCHECK(counters != NULL);
    sw_.Start();
    int ret = getrusage(RUSAGE_THREAD, &usage_base_);
    DCHECK_EQ(ret, 0);
  }

  // Stop and update the counter
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
    counters_->total_time_->Update(sw_.ElapsedTime());
    counters_->user_time_->Update(utime_diff);
    counters_->sys_time_->Update(stime_diff);
    counters_->voluntary_context_switches_->Update(usage.ru_nvcsw - usage_base_.ru_nvcsw);
    counters_->involuntary_context_switches_->Update(
        usage.ru_nivcsw - usage_base_.ru_nivcsw);
  }

  // Update counter when object is destroyed
  ~ThreadCounterMeasurement() {
    Stop();
  }

 private:
  // Disable copy constructor and assignment
  ThreadCounterMeasurement(const ThreadCounterMeasurement& timer);
  ThreadCounterMeasurement& operator=(const ThreadCounterMeasurement& timer);

  bool stop_;
  rusage usage_base_;
  MonotonicStopWatch sw_;
  RuntimeProfile::ThreadCounters* counters_;
};

}

#endif
