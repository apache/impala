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
#include <queue>
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
/// can be specified, and then the metric's value is used as the consumption rather than
/// the tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
/// Other consumption metrics are used in trackers below the process level to account
/// for memory (such as free buffer pool buffers) that is not tracked by Consume() and
/// Release().
///
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
  /// Consume()/Release() can still be called. This is used for the root process tracker
  /// (if 'parent' is NULL). It is also to report on other categories of memory under the
  /// process tracker, e.g. buffer pool free buffers (if 'parent - non-NULL).
  MemTracker(IntGauge* consumption_metric, int64_t byte_limit = -1,
      const std::string& label = std::string(), MemTracker* parent = NULL);

  ~MemTracker();

  /// Closes this MemTracker. After closing it is invalid to consume memory on this
  /// tracker and the tracker's consumption counter (which may be owned by a
  /// RuntimeProfile, not this MemTracker) can be safely destroyed. MemTrackers without
  /// consumption metrics in the context of a daemon must always be closed.
  /// Idempotent: calling multiple times has no effect.
  void Close();

  /// Closes the MemTracker and deregisters it from its parent. Can be called before
  /// destruction to prevent other threads from getting a reference to the MemTracker
  /// via its parent. Only used to deregister the query-level MemTracker from the
  /// global hierarchy.
  /// TODO: IMPALA-3200: this is also used by BufferedBlockMgr, which will be deleted.
  void CloseAndUnregisterFromParent();

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
    DCHECK(!closed_) << label_;
    if (bytes <= 0) {
      if (bytes < 0) Release(-bytes);
      return;
    }

    if (consumption_metric_ != NULL) {
      RefreshConsumptionFromMetric();
      return;
    }
    for (MemTracker* tracker : all_trackers_) {
      tracker->consumption_->Add(bytes);
      if (tracker->consumption_metric_ == NULL) {
        DCHECK_GE(tracker->consumption_->current_value(), 0);
      }
    }
  }

  /// Increases/Decreases the consumption of this tracker and the ancestors up to (but
  /// not including) end_tracker. This is useful if we want to move tracking between
  /// trackers that share a common (i.e. end_tracker) ancestor. This happens when we want
  /// to update tracking on a particular mem tracker but the consumption against
  /// the limit recorded in one of its ancestors already happened.
  void ConsumeLocal(int64_t bytes, MemTracker* end_tracker) {
    DCHECK(!closed_) << label_;
    DCHECK(consumption_metric_ == NULL) << "Should not be called on root.";
    for (MemTracker* tracker : all_trackers_) {
      if (tracker == end_tracker) return;
      DCHECK(!tracker->has_limit());
      DCHECK(!tracker->closed_) << tracker->label_;
      tracker->consumption_->Add(bytes);
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
    DCHECK(!closed_) << label_;
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
    DCHECK(!closed_) << label_;
    if (bytes <= 0) {
      if (bytes < 0) Consume(-bytes);
      return;
    }

    if (consumption_metric_ != NULL) {
      RefreshConsumptionFromMetric();
      return;
    }
    for (MemTracker* tracker : all_trackers_) {
      tracker->consumption_->Add(-bytes);
      /// If a UDF calls FunctionContext::TrackAllocation() but allocates less than the
      /// reported amount, the subsequent call to FunctionContext::Free() may cause the
      /// process mem tracker to go negative until it is synced back to the tcmalloc
      /// metric. Don't blow up in this case. (Note that this doesn't affect non-process
      /// trackers since we can enforce that the reported memory usage is internally
      /// consistent.)
      if (tracker->consumption_metric_ == NULL) {
        DCHECK_GE(tracker->consumption_->current_value(), 0)
          << std::endl << tracker->LogUsage(UNLIMITED_DEPTH);
      }
    }
  }

  /// Returns true if a valid limit of this tracker or one of its ancestors is
  /// exceeded.
  bool AnyLimitExceeded() {
    for (MemTracker* tracker : limit_trackers_) {
      if (tracker->LimitExceeded()) return true;
    }
    return false;
  }

  /// If this tracker has a limit, checks the limit and attempts to free up some memory if
  /// the limit is exceeded by calling any added GC functions. Returns true if the limit is
  /// exceeded after calling the GC functions. Returns false if there is no limit.
  bool LimitExceeded() {
    if (UNLIKELY(CheckLimitExceeded())) {
      if (bytes_over_limit_metric_ != NULL) {
        bytes_over_limit_metric_->SetValue(consumption() - limit_);
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

  /// Refresh the memory consumption value from the consumption metric. Only valid to
  /// call if this tracker has a consumption metric.
  void RefreshConsumptionFromMetric() {
    DCHECK(consumption_metric_ != nullptr);
    consumption_->Set(consumption_metric_->GetValue());
  }

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
  /// reserved for a query that is currently executing is its limit_, if set (which
  /// should be the common case with admission control). Otherwise, if the query has
  /// no limit or the query is finished executing, the current consumption is used.
  int64_t GetPoolMemReserved();

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

  /// Logs the usage of this tracker and optionally its children (recursively).
  /// If 'logged_consumption' is non-NULL, sets the consumption value logged.
  /// 'max_recursive_depth' specifies the maximum number of levels of children
  /// to include in the dump. If it is zero, then no children are dumped.
  /// Limiting the recursive depth reduces the cost of dumping, particularly
  /// for the process MemTracker.
  /// TODO: once all memory is accounted in ReservationTracker hierarchy, move
  /// reporting there.
  std::string LogUsage(int max_recursive_depth,
      const std::string& prefix = "", int64_t* logged_consumption = nullptr);
  /// Dumping the process MemTracker is expensive. Limiting the recursive depth
  /// to two levels limits the level of detail to a one-line summary for each query
  /// MemTracker, avoiding all MemTrackers below that level. This provides a summary
  /// of process usage with substantially lower cost than the full dump.
  static const int PROCESS_MEMTRACKER_LIMITED_DEPTH = 2;
  /// Unlimited dumping is useful for query memtrackers or error conditions that
  /// are not performance sensitive
  static const int UNLIMITED_DEPTH = INT_MAX;

  /// Logs the usage of 'limit' number of queries based on maximum total memory
  /// consumption.
  std::string LogTopNQueries(int limit);

  /// Log the memory usage when memory limit is exceeded and return a status object with
  /// details of the allocation which caused the limit to be exceeded.
  /// If 'failed_allocation_size' is greater than zero, logs the allocation size. If
  /// 'failed_allocation_size' is zero, nothing about the allocation size is logged.
  /// If 'state' is non-NULL, logs the error to 'state'.
  Status MemLimitExceeded(RuntimeState* state, const std::string& details,
      int64_t failed_allocation = 0) WARN_UNUSED_RESULT;

  void set_query_exec_finished() {
    DCHECK(is_query_mem_tracker_);
    query_exec_finished_.Store(1);
  }

  static const std::string COUNTER_NAME;

 private:
  friend class PoolMemTrackerRegistry;

  /// Returns true if the current memory tracker's limit is exceeded.
  bool CheckLimitExceeded() const { return limit_ >= 0 && limit_ < consumption(); }

  /// If consumption is higher than max_consumption, attempts to free memory by calling
  /// any added GC functions.  Returns true if max_consumption is still exceeded. Takes
  /// gc_lock. Updates metrics if initialized.
  bool GcMemory(int64_t max_consumption);

  /// Walks the MemTracker hierarchy and populates all_trackers_ and
  /// limit_trackers_
  void Init();

  /// Adds tracker to child_trackers_
  void AddChildTracker(MemTracker* tracker);

  /// Log consumption of all the trackers provided. Returns the sum of consumption in
  /// 'logged_consumption'. 'max_recursive_depth' specifies the maximum number of levels
  /// of children to include in the dump. If it is zero, then no children are dumped.
  static std::string LogUsage(int max_recursive_depth, const std::string& prefix,
      const std::list<MemTracker*>& trackers, int64_t* logged_consumption);

  /// Helper function for LogTopNQueries that iterates through the MemTracker hierarchy
  /// and populates 'min_pq' with 'limit' number of elements (that contain state related
  /// to query MemTrackers) based on maximum total memory consumption.
  void GetTopNQueries(
      std::priority_queue<pair<int64_t, string>,
          vector<pair<int64_t, string>>, std::greater<pair<int64_t, string>>>& min_pq,
      int limit);

  /// If an ancestor of this tracker is a query MemTracker, return that tracker.
  /// Otherwise return NULL.
  MemTracker* GetQueryMemTracker();

  /// Lock to protect GcMemory(). This prevents many GCs from occurring at once.
  boost::mutex gc_lock_;

  /// True if this is a Query MemTracker returned from CreateQueryMemTracker().
  bool is_query_mem_tracker_ = false;

  /// Only used if 'is_query_mem_tracker_' is true.
  /// 0 if the query is still executing or 1 if it has finished executing. Before
  /// it has finished executing, the tracker limit is treated as "reserved memory"
  /// for the purpose of admission control - see GetPoolMemReserved().
  AtomicInt32 query_exec_finished_{0};

  /// Only valid for MemTrackers returned from CreateQueryMemTracker()
  TUniqueId query_id_;

  /// Only valid for MemTrackers returned from GetRequestPoolMemTracker()
  std::string pool_name_;

  /// Hard limit on memory consumption, in bytes. May not be exceeded. If limit_ == -1,
  /// there is no consumption limit.
  const int64_t limit_;

  std::string label_;

  /// The parent of this tracker. The pointer is never modified, even after this tracker
  /// is unregistered.
  MemTracker* const parent_;

  /// in bytes; not owned
  RuntimeProfile::HighWaterMarkCounter* consumption_;

  /// holds consumption_ counter if not tied to a profile
  RuntimeProfile::HighWaterMarkCounter local_counter_;

  /// If non-NULL, used to measure consumption (in bytes) rather than the values provided
  /// to Consume()/Release(). Only used for the process tracker, thus parent_ should be
  /// NULL if consumption_metric_ is set.
  IntGauge* consumption_metric_;

  /// If non-NULL, counters from a corresponding ReservationTracker that should be
  /// reported in logs and other diagnostics. Owned by this MemTracker. The counters
  /// are owned by the fragment's RuntimeProfile.
  AtomicPtr<ReservationTrackerCounters> reservation_counters_;

  std::vector<MemTracker*> all_trackers_;  // this tracker plus all of its ancestors
  std::vector<MemTracker*> limit_trackers_;  // all_trackers_ with valid limits

  /// All the child trackers of this tracker. Used only for computing resource pool mem
  /// reserved and error reporting, i.e., updating a parent tracker does not update its
  /// children.
  SpinLock child_trackers_lock_;
  std::list<MemTracker*> child_trackers_;

  /// Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
  /// remove.
  std::list<MemTracker*>::iterator child_tracker_it_;

  /// Functions to call after the limit is reached to free memory.
  std::vector<GcFunction> gc_functions_;

  /// If false, this tracker (and its children) will not be included in LogUsage() output
  /// if consumption is 0.
  bool log_usage_if_zero_;

  bool closed_ = false;

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
  typedef boost::unordered_map<std::string, std::unique_ptr<MemTracker>> PoolTrackersMap;
  PoolTrackersMap pool_to_mem_trackers_;
  /// IMPALA-3068: Use SpinLock instead of boost::mutex so that the lock won't
  /// automatically destroy itself as part of process teardown, which could cause races.
  SpinLock pool_to_mem_trackers_lock_;
};
}

#endif
