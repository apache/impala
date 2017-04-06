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


#ifndef IMPALA_RUNTIME_MEM_TRACKER_H
#define IMPALA_RUNTIME_MEM_TRACKER_H

#include <stdint.h>
#include <map>
#include <memory>
#include <vector>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "common/logging.h"
#include "common/atomic.h"
#include "util/debug-util.h"
#include "util/internal-queue.h"
#include "util/metrics.h"
#include "util/runtime-profile-counters.h"
#include "util/spinlock.h"

#include "gen-cpp/Types_types.h" // for TUniqueId

namespace impala {

class ObjectPool;
class MemTracker;
class ReservationTrackerCounters;
class TQueryOptions;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
///
/// We use a five-level hierarchy of mem trackers: process, pool, query, fragment
/// instance. Specific parts of the fragment (exec nodes, sinks, etc) will add a
/// fifth level when they are initialized. This function also initializes a user
/// function mem tracker (in the fifth level).
///
/// By default, memory consumption is tracked via calls to Consume()/Release(), either to
/// the tracker itself or to one of its descendents. Alternatively, a consumption metric
/// can specified, and then the metric's value is used as the consumption rather than the
/// tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
//
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If LimitExceeded() is called and the limit is exceeded, it will first call
/// the GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
/// GcFunctions are called with a global lock held, so should be non-blocking and not
/// call back into MemTrackers, except to release memory.
//
/// This class is thread-safe.
class MemTracker {
 public:
  /// 'byte_limit' < 0 means no limit
  /// 'label' is the label used in the usage string (LogUsage())
  /// If 'log_usage_if_zero' is false, this tracker (and its children) will not be included
  /// in LogUsage() output if consumption is 0.
  MemTracker(int64_t byte_limit = -1, const std::string& label = std::string(),
      MemTracker* parent = NULL, bool log_usage_if_zero = true);

  /// C'tor for tracker for which consumption counter is created as part of a profile.
  /// The counter is created with name COUNTER_NAME.
  MemTracker(RuntimeProfile* profile, int64_t byte_limit,
      const std::string& label = std::string(), MemTracker* parent = NULL);

  /// C'tor for tracker that uses consumption_metric as the consumption value.
  /// Consume()/Release() can still be called. This is used for the process tracker.
  MemTracker(UIntGauge* consumption_metric, int64_t byte_limit = -1,
      const std::string& label = std::string());

  ~MemTracker();

  /// Removes this tracker from parent_->child_trackers_.
  void UnregisterFromParent();

  /// Include counters from a ReservationTracker in logs and other diagnostics.
  /// The counters should be owned by the fragment's RuntimeProfile.
  void EnableReservationReporting(const ReservationTrackerCounters& counters);

  /// Construct a MemTracker object for query 'id'. The query limits are determined based
  /// on 'query_options'. The MemTracker is a child of the request pool MemTracker for
  /// 'pool_name', which is created if needed. The returned MemTracker is owned by
  /// 'obj_pool'.
  static MemTracker* CreateQueryMemTracker(const TUniqueId& id,
      const TQueryOptions& query_options, const std::string& pool_name,
      ObjectPool* obj_pool);

  /// Increases consumption of this tracker and its ancestors by 'bytes'.
  void Consume(int64_t bytes) {
    if (bytes <= 0) {
      if (bytes < 0) Release(-bytes);
      return;
    }

    if (consumption_metric_ != NULL) {
      RefreshConsumptionFromMetric();
      return;
    }
    for (std::vector<MemTracker*>::iterator tracker = all_trackers_.begin();
         tracker != all_trackers_.end(); ++tracker) {
      (*tracker)->consumption_->Add(bytes);
      if ((*tracker)->consumption_metric_ == NULL) {
        DCHECK_GE((*tracker)->consumption_->current_value(), 0);
      }
    }
  }

  /// Increases/Decreases the consumption of this tracker and the ancestors up to (but
  /// not including) end_tracker. This is useful if we want to move tracking between
  /// trackers that share a common (i.e. end_tracker) ancestor. This happens when we want
  /// to update tracking on a particular mem tracker but the consumption against
  /// the limit recorded in one of its ancestors already happened.
  void ConsumeLocal(int64_t bytes, MemTracker* end_tracker) {
    DCHECK(consumption_metric_ == NULL) << "Should not be called on root.";
    for (int i = 0; i < all_trackers_.size(); ++i) {
      if (all_trackers_[i] == end_tracker) return;
      DCHECK(!all_trackers_[i]->has_limit());
      all_trackers_[i]->consumption_->Add(bytes);
    }
    DCHECK(false) << "end_tracker is not an ancestor";
  }

  void ReleaseLocal(int64_t bytes, MemTracker* end_tracker) {
    ConsumeLocal(-bytes, end_tracker);
  }

  /// Increases consumption of this tracker and its ancestors by 'bytes' only if
  /// they can all consume 'bytes'. If this brings any of them over, none of them
  /// are updated.
  /// Returns true if the try succeeded.
  WARN_UNUSED_RESULT
  bool TryConsume(int64_t bytes) {
    if (consumption_metric_ != NULL) RefreshConsumptionFromMetric();
    if (UNLIKELY(bytes <= 0)) return true;
    int i;
    // Walk the tracker tree top-down.
    for (i = all_trackers_.size() - 1; i >= 0; --i) {
      MemTracker* tracker = all_trackers_[i];
      const int64_t limit = tracker->limit();
      if (limit < 0) {
        tracker->consumption_->Add(bytes); // No limit at this tracker.
      } else {
        // If TryConsume fails, we can try to GC, but we may need to try several times if
        // there are concurrent consumers because we don't take a lock before trying to
        // update consumption_.
        while (true) {
          if (LIKELY(tracker->consumption_->TryAdd(bytes, limit))) break;

          VLOG_RPC << "TryConsume failed, bytes=" << bytes
                   << " consumption=" << tracker->consumption_->current_value()
                   << " limit=" << limit << " attempting to GC";
          if (UNLIKELY(tracker->GcMemory(limit - bytes))) {
            DCHECK_GE(i, 0);
            // Failed for this mem tracker. Roll back the ones that succeeded.
            for (int j = all_trackers_.size() - 1; j > i; --j) {
              all_trackers_[j]->consumption_->Add(-bytes);
            }
            return false;
          }
          VLOG_RPC << "GC succeeded, TryConsume bytes=" << bytes
                   << " consumption=" << tracker->consumption_->current_value()
                   << " limit=" << limit;
        }
      }
    }
    // Everyone succeeded, return.
    DCHECK_EQ(i, -1);
    return true;
  }

  /// Decreases consumption of this tracker and its ancestors by 'bytes'.
  void Release(int64_t bytes) {
    if (bytes <= 0) {
      if (bytes < 0) Consume(-bytes);
      return;
    }

    if (UNLIKELY(released_memory_since_gc_.Add(bytes) > GC_RELEASE_SIZE)) {
      GcTcmalloc();
    }

    if (consumption_metric_ != NULL) {
      DCHECK(parent_ == NULL);
      consumption_->Set(consumption_metric_->value());
      return;
    }
    for (std::vector<MemTracker*>::iterator tracker = all_trackers_.begin();
         tracker != all_trackers_.end(); ++tracker) {
      (*tracker)->consumption_->Add(-bytes);
      /// If a UDF calls FunctionContext::TrackAllocation() but allocates less than the
      /// reported amount, the subsequent call to FunctionContext::Free() may cause the
      /// process mem tracker to go negative until it is synced back to the tcmalloc
      /// metric. Don't blow up in this case. (Note that this doesn't affect non-process
      /// trackers since we can enforce that the reported memory usage is internally
      /// consistent.)
      if ((*tracker)->consumption_metric_ == NULL) {
        DCHECK_GE((*tracker)->consumption_->current_value(), 0)
          << std::endl << (*tracker)->LogUsage();
      }
    }

    /// TODO: Release brokered memory?
  }

  /// Returns true if a valid limit of this tracker or one of its ancestors is
  /// exceeded.
  bool AnyLimitExceeded() {
    for (std::vector<MemTracker*>::iterator tracker = limit_trackers_.begin();
         tracker != limit_trackers_.end(); ++tracker) {
      if ((*tracker)->LimitExceeded()) return true;
    }
    return false;
  }

  /// If this tracker has a limit, checks the limit and attempts to free up some memory if
  /// the limit is exceeded by calling any added GC functions. Returns true if the limit is
  /// exceeded after calling the GC functions. Returns false if there is no limit.
  bool LimitExceeded() {
    if (UNLIKELY(CheckLimitExceeded())) {
      if (bytes_over_limit_metric_ != NULL) {
        bytes_over_limit_metric_->set_value(consumption() - limit_);
      }
      return GcMemory(limit_);
    }
    return false;
  }

  /// Returns the maximum consumption that can be made without exceeding the limit on
  /// this tracker or any of its parents. Returns int64_t::max() if there are no
  /// limits and a negative value if any limit is already exceeded.
  int64_t SpareCapacity() const {
    int64_t result = std::numeric_limits<int64_t>::max();
    for (std::vector<MemTracker*>::const_iterator tracker = limit_trackers_.begin();
         tracker != limit_trackers_.end(); ++tracker) {
      int64_t mem_left = (*tracker)->limit() - (*tracker)->consumption();
      result = std::min(result, mem_left);
    }
    return result;
  }

  /// Refresh the value of consumption_. Only valid to call if consumption_metric_ is not
  /// null.
  void RefreshConsumptionFromMetric();

  int64_t limit() const { return limit_; }
  bool has_limit() const { return limit_ >= 0; }
  const std::string& label() const { return label_; }

  /// Returns the lowest limit for this tracker and its ancestors. Returns
  /// -1 if there is no limit.
  int64_t lowest_limit() const {
    if (limit_trackers_.empty()) return -1;
    int64_t v = std::numeric_limits<int64_t>::max();
    for (int i = 0; i < limit_trackers_.size(); ++i) {
      DCHECK(limit_trackers_[i]->has_limit());
      v = std::min(v, limit_trackers_[i]->limit());
    }
    return v;
  }

  /// Returns the memory 'reserved' by this resource pool mem tracker, which is the sum
  /// of the memory reserved by the queries in it (i.e. its child trackers). The mem
  /// reserved for a query is its limit_, if set (which should be the common case with
  /// admission control). Otherwise the current consumption is used.
  int64_t GetPoolMemReserved() const;

  /// Returns the memory consumed in bytes.
  int64_t consumption() const { return consumption_->current_value(); }

  /// Note that if consumption_ is based on consumption_metric_, this will the max value
  /// we've recorded in consumption(), not necessarily the highest value
  /// consumption_metric_ has ever reached.
  int64_t peak_consumption() const { return consumption_->value(); }

  MemTracker* parent() const { return parent_; }

  /// Signature for function that can be called to free some memory after limit is
  /// reached. The function should try to free at least 'bytes_to_free' bytes of
  /// memory. See the class header for further details on the expected behaviour of
  /// these functions.
  typedef std::function<void(int64_t bytes_to_free)> GcFunction;

  /// Add a function 'f' to be called if the limit is reached, if none of the other
  /// previously-added GC functions were successful at freeing up enough memory.
  /// 'f' does not need to be thread-safe as long as it is added to only one MemTracker.
  /// Note that 'f' must be valid for the lifetime of this MemTracker.
  void AddGcFunction(GcFunction f);

  /// Register this MemTracker's metrics. Each key will be of the form
  /// "<prefix>.<metric name>".
  void RegisterMetrics(MetricGroup* metrics, const std::string& prefix);

  /// Logs the usage of this tracker and all of its children (recursively).
  /// TODO: once all memory is accounted in ReservationTracker hierarchy, move
  /// reporting there.
  std::string LogUsage(const std::string& prefix = "") const;

  /// Log the memory usage when memory limit is exceeded and return a status object with
  /// details of the allocation which caused the limit to be exceeded.
  /// If 'failed_allocation_size' is greater than zero, logs the allocation size. If
  /// 'failed_allocation_size' is zero, nothing about the allocation size is logged.
  Status MemLimitExceeded(RuntimeState* state, const std::string& details,
      int64_t failed_allocation = 0) WARN_UNUSED_RESULT;

  static const std::string COUNTER_NAME;

 private:
  friend class PoolMemTrackerRegistry;

  bool CheckLimitExceeded() const { return limit_ >= 0 && limit_ < consumption(); }

  /// If consumption is higher than max_consumption, attempts to free memory by calling
  /// any added GC functions.  Returns true if max_consumption is still exceeded. Takes
  /// gc_lock. Updates metrics if initialized.
  bool GcMemory(int64_t max_consumption);

  /// Called when the total release memory is larger than GC_RELEASE_SIZE.
  /// TcMalloc holds onto released memory and very slowly (if ever) releases it back to
  /// the OS. This is problematic since it is memory we are not constantly tracking which
  /// can cause us to go way over mem limits.
  void GcTcmalloc();

  /// Walks the MemTracker hierarchy and populates all_trackers_ and
  /// limit_trackers_
  void Init();

  /// Adds tracker to child_trackers_
  void AddChildTracker(MemTracker* tracker);

  static std::string LogUsage(const std::string& prefix,
      const std::list<MemTracker*>& trackers);

  /// Size, in bytes, that is considered a large value for Release() (or Consume() with
  /// a negative value). If tcmalloc is used, this can trigger it to GC.
  /// A higher value will make us call into tcmalloc less often (and therefore more
  /// efficient). A lower value will mean our memory overhead is lower.
  /// TODO: this is a stopgap.
  static const int64_t GC_RELEASE_SIZE = 128 * 1024L * 1024L;

  /// Total amount of memory from calls to Release() since the last GC. If this
  /// is greater than GC_RELEASE_SIZE, this will trigger a tcmalloc gc.
  static AtomicInt64 released_memory_since_gc_;

  /// Lock to protect GcMemory(). This prevents many GCs from occurring at once.
  boost::mutex gc_lock_;

  /// Only valid for MemTrackers returned from CreateQueryMemTracker()
  TUniqueId query_id_;

  /// Only valid for MemTrackers returned from GetRequestPoolMemTracker()
  std::string pool_name_;

  /// Hard limit on memory consumption, in bytes. May not be exceeded. If limit_ == -1,
  /// there is no consumption limit.
  int64_t limit_;

  std::string label_;
  MemTracker* parent_;

  /// in bytes; not owned
  RuntimeProfile::HighWaterMarkCounter* consumption_;

  /// holds consumption_ counter if not tied to a profile
  RuntimeProfile::HighWaterMarkCounter local_counter_;

  /// If non-NULL, used to measure consumption (in bytes) rather than the values provided
  /// to Consume()/Release(). Only used for the process tracker, thus parent_ should be
  /// NULL if consumption_metric_ is set.
  UIntGauge* consumption_metric_;

  /// If non-NULL, counters from a corresponding ReservationTracker that should be
  /// reported in logs and other diagnostics. Owned by this MemTracker. The counters
  /// are owned by the fragment's RuntimeProfile.
  AtomicPtr<ReservationTrackerCounters> reservation_counters_;

  std::vector<MemTracker*> all_trackers_;  // this tracker plus all of its ancestors
  std::vector<MemTracker*> limit_trackers_;  // all_trackers_ with valid limits

  /// All the child trackers of this tracker. Used only for computing resource pool mem
  /// reserved and error reporting, i.e., updating a parent tracker does not update its
  /// children.
  mutable SpinLock child_trackers_lock_;
  std::list<MemTracker*> child_trackers_;

  /// Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
  /// remove.
  std::list<MemTracker*>::iterator child_tracker_it_;

  /// Functions to call after the limit is reached to free memory.
  std::vector<GcFunction> gc_functions_;

  /// If false, this tracker (and its children) will not be included in LogUsage() output
  /// if consumption is 0.
  bool log_usage_if_zero_;

  /// The number of times the GcFunctions were called.
  IntCounter* num_gcs_metric_;

  /// The number of bytes freed by the last round of calling the GcFunctions (-1 before any
  /// GCs are performed).
  IntGauge* bytes_freed_by_last_gc_metric_;

  /// The number of bytes over the limit we were the last time LimitExceeded() was called
  /// and the limit was exceeded pre-GC. -1 if there is no limit or the limit was never
  /// exceeded.
  IntGauge* bytes_over_limit_metric_;

  /// Metric for limit_.
  IntGauge* limit_metric_;
};

/// Global registry for query and pool MemTrackers. Owned by ExecEnv.
class PoolMemTrackerRegistry {
 public:
  /// Returns a MemTracker object for request pool 'pool_name'. Calling this with the same
  /// 'pool_name' will return the same MemTracker object. This is used to track the local
  /// memory usage of all requests executing in this pool. If 'create_if_not_present' is
  /// true, the first time this is called for a pool, a new MemTracker object is created
  /// with the process tracker as its parent. There is no explicit per-pool byte_limit
  /// set at any particular impalad, so newly created trackers will always have a limit
  /// of -1.
  MemTracker* GetRequestPoolMemTracker(
      const std::string& pool_name, bool create_if_not_present);

 private:
  /// All per-request pool MemTracker objects. It is assumed that request pools will live
  /// for the entire duration of the process lifetime so MemTrackers are never removed
  /// from this map. Protected by 'pool_to_mem_trackers_lock_'
  typedef boost::unordered_map<std::string, MemTracker*> PoolTrackersMap;
  PoolTrackersMap pool_to_mem_trackers_;
  /// IMPALA-3068: Use SpinLock instead of boost::mutex so that the lock won't
  /// automatically destroy itself as part of process teardown, which could cause races.
  SpinLock pool_to_mem_trackers_lock_;
};
}

#endif
