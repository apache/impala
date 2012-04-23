// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_COUNTERS_H
#define IMPALA_UTIL_COUNTERS_H

#include "common/object-pool.h"
#include "util/stopwatch.h"

#include "gen-cpp/RuntimeProfile_types.h"
#include "common/logging.h"

#include <iostream>

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
  #define COUNTER_SCOPED_TIMER(c) ScopedTimer MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c)
  #define COUNTER_UPDATE(c, v) (c)->Update(v)
  #define COUNTER_SET(c, v) (c)->Set(v)
#else
  #define ADD_COUNTER(profile, name, type) NULL
  #define COUNTER_SCOPED_TIMER(c)
  #define COUNTER_UPDATE(c, v)
  #define COUNTER_SET(c, v)
#endif

class ObjectPool;

// Runtime profile is a group of profiling counters.  It supports adding named counters
// and being able to serialize and deserialize them.
// The profiles support a tree structure to form a hierarchy of counters.
// This class is not thread safe.
class RuntimeProfile {
 public:
  class Counter {
   public:
    void Update(int64_t delta) { value_ += delta; }

    void Set(int64_t value) { value_ = value; }

    int64_t value() const { return value_; }

    TCounterType::type type() const { return type_; }

   private:
    friend class RuntimeProfile;

    Counter(TCounterType::type type, int64_t value = 0) :
      value_(value),
      type_(type) {
    }

    int64_t value_;
    TCounterType::type type_;
  };

  // Create a runtime profile object with 'name'.  Counters and merged profile are
  // allocated from pool.
  RuntimeProfile(ObjectPool* pool, const std::string& name);

  // Deserialize from thrift.  Runtime profiles are allocated from the pool.
  static RuntimeProfile* CreateFromThrift(ObjectPool* pool,
      const TRuntimeProfileTree& profiles);

  // Adds a child profile
  void AddChild(RuntimeProfile* child);

  // Merges the src profile into this one, combining counters that have an identical
  // path.
  void Merge(const RuntimeProfile& src);

  // Add a counter with 'name'/'type'.  Returns a counter object that the caller can
  // update.  The counter is owned by the RuntimeProfile object.
  Counter* AddCounter(const std::string& name, TCounterType::type type);

  // Gets the counter object with 'name'.  Returns NULL if there is no counter with
  // that name.
  Counter* GetCounter(const std::string& name);

  // Add a counter with 'name'/'type' if one does not already exist. Returns the counter
  // object, which is owned by the RuntimeProfile.
  Counter* AddCounterIfAbsent(const std::string& name, TCounterType::type type);

  // Returns the counter for the total elapsed time.
  Counter* total_time_counter() { return &counter_total_time_; }

  // Prints the counters in a name: value format
  void PrettyPrint(std::ostream* s, const std::string& prefix="") const;

  // Serializes the counters to thrift
  void ToThrift(std::vector<TRuntimeProfileNode>* nodes) const;

  // Divides all counters by n
  void Divide(int n);

  // Returns the number of counters in this profile
  int num_counters() const { return counter_map_.size(); }

  // Returns name of this profile
  const std::string& name() const { return name_; }

  // Renames the profile
  void Rename(const std::string& name) { name_ = name; }

  // Returns the children
  const std::vector<RuntimeProfile*> children() const { return children_; }

 private:
  // Pool for allocated counters
  ObjectPool* pool_;

  // Name for this runtime profile.
  std::string name_;

  // Map from counter names to counters.  The profile owns the memory for the
  // counters.
  std::map<std::string, Counter*> counter_map_;

  // Child profiles.  Does not own memory
  std::vector<RuntimeProfile*> children_;

  Counter counter_total_time_;

  // Create a subtree of runtime profiles from nodes, starting at *node_idx.
  // On return, *node_idx is the index one past the end of this subtree
  static RuntimeProfile* CreateFromThrift(ObjectPool* pool,
      const std::vector<TRuntimeProfileNode>& nodes, int* node_idx);
};

// Utility class to update time elapsed when the object goes
// out of scope.
class ScopedTimer {
 public:
  ScopedTimer(RuntimeProfile::Counter* counter) :
    counter_(counter) {
    DCHECK_EQ(counter->type(), TCounterType::CPU_TICKS);
    sw_.Start();
  }

  // Update counter when object is destroyed
  ~ScopedTimer() {
    sw_.Stop();
    counter_->Update(sw_.Ticks());
  }

 private:
  // Disable copy constructor and assignment
  ScopedTimer(const ScopedTimer& timer);
  ScopedTimer& operator=(const ScopedTimer& timer);

  StopWatch sw_;
  RuntimeProfile::Counter* counter_;
};

}

#endif
