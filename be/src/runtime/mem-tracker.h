// Copyright 2013 Cloudera Inc.
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


#ifndef IMPALA_RUNTIME_MEM_TRACKER_H
#define IMPALA_RUNTIME_MEM_TRACKER_H

#include <stdint.h>
#include <map>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "common/logging.h"
#include "common/atomic.h"
#include "util/debug-util.h"
#include "util/internal-queue.h"
#include "util/metrics.h"
#include "util/runtime-profile.h"
#include "util/spinlock.h"

#include "gen-cpp/Types_types.h" // for TUniqueId

namespace impala {

class MemTracker;
class QueryResourceMgr;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
//
/// By default, memory consumption is tracked via calls to Consume()/Release(), either to
/// the tracker itself or to one of its descendents. Alternatively, a consumption metric
/// can specified, and then the metric's value is used as the consumption rather than the
/// tally maintained by Consume() and Release(). A tcmalloc metric is used to track process
/// memory consumption, since the process memory usage may be higher than the computed
/// total memory (tcmalloc does not release deallocated memory immediately).
//
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If LimitExceeded() is called and the limit is exceeded, it will first call the
/// GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
//
/// This class is thread-safe.
class MemTracker {
 public:
  /// 'byte_limit' < 0 means no limit
  /// 'label' is the label used in the usage string (LogUsage())
  /// If 'log_usage_if_zero' is false, this tracker (and its children) will not be included
  /// in LogUsage() output if consumption is 0.
  MemTracker(int64_t byte_limit = -1, int64_t rm_reserved_limit = -1,
      const std::string& label = std::string(), MemTracker* parent = NULL,
      bool log_usage_if_zero = true);

  /// C'tor for tracker for which consumption counter is created as part of a profile.
  /// The counter is created with name COUNTER_NAME.
  MemTracker(RuntimeProfile* profile, int64_t byte_limit, int64_t rm_reserved_limit = -1,
      const std::string& label = std::string(), MemTracker* parent = NULL);

  /// C'tor for tracker that uses consumption_metric as the consumption value.
  /// Consume()/Release() can still be called. This is used for the process tracker.
  MemTracker(UIntGauge* consumption_metric,
      int64_t byte_limit = -1, int64_t rm_reserved_limit = -1,
      const std::string& label = std::string());

  ~MemTracker();

  /// Removes this tracker from parent_->child_trackers_.
  void UnregisterFromParent();

  /// Returns a MemTracker object for query 'id'.  Calling this with the same id will
  /// return the same MemTracker object.  An example of how this is used is to pass it
  /// the same query id for all fragments of that query running on this machine.  This
  /// way, we have per-query limits rather than per-fragment.
  /// The first time this is called for an id, a new MemTracker object is created with
  /// 'parent' as the parent tracker.
  /// byte_limit and parent must be the same for all GetMemTracker() calls with the
  /// same id.
  static boost::shared_ptr<MemTracker> GetQueryMemTracker(const TUniqueId& id,
      int64_t byte_limit, int64_t rm_reserved_limit, MemTracker* parent,
      QueryResourceMgr* res_mgr);

  /// Returns a MemTracker object for request pool 'pool_name'. Calling this with the same
  /// 'pool_name' will return the same MemTracker object. This is used to track the local
  /// memory usage of all requests executing in this pool. The first time this is called
  /// for a pool, a new MemTracker object is created with the parent tracker if it is not
  /// NULL. If the parent is NULL, no new tracker will be created and NULL is returned.
  /// There is no explicit per-pool byte_limit set at any particular impalad, so newly
  /// created trackers will always have a limit of -1.
  static MemTracker* GetRequestPoolMemTracker(const std::string& pool_name,
      MemTracker* parent);

  /// Returns the minimum of limit and rm_reserved_limit
  int64_t effective_limit() const {
    // TODO: maybe no limit should be MAX_LONG?
    DCHECK(rm_reserved_limit_ <= limit_ || limit_ == -1);
    if (rm_reserved_limit_ == -1) return limit_;
    return rm_reserved_limit_;
  }

  /// Increases consumption of this tracker and its ancestors by 'bytes'.
  void Consume(int64_t bytes) {
    if (bytes < 0) {
      Release(-bytes);
      return;
    }

    if (consumption_metric_ != NULL) {
      DCHECK(parent_ == NULL);
      consumption_->Set(consumption_metric_->value());
      return;
    }
    if (bytes == 0) return;
    if (UNLIKELY(enable_logging_)) LogUpdate(true, bytes);
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
    if (UNLIKELY(enable_logging_)) LogUpdate(bytes > 0, bytes);
    for (int i = 0; i < all_trackers_.size(); ++i) {
      if (all_trackers_[i] == end_tracker) return;
      DCHECK(!all_trackers_[i]->has_limit());
      all_trackers_[i]->consumption_->Add(bytes);
    }
  }

  void ReleaseLocal(int64_t bytes, MemTracker* end_tracker) {
    ConsumeLocal(-bytes, end_tracker);
  }

  /// Increases consumption of this tracker and its ancestors by 'bytes' only if
  /// they can all consume 'bytes'. If this brings any of them over, none of them
  /// are updated.
  /// Returns true if the try succeeded.
  bool TryConsume(int64_t bytes) {
    if (consumption_metric_ != NULL) consumption_->Set(consumption_metric_->value());
    if (bytes <= 0) return true;
    if (UNLIKELY(enable_logging_)) LogUpdate(true, bytes);
    int i = 0;
    // Walk the tracker tree top-down, to avoid expanding a limit on a child whose parent
    // won't accommodate the change.
    for (i = all_trackers_.size() - 1; i >= 0; --i) {
      MemTracker* tracker = all_trackers_[i];
      int64_t limit = tracker->effective_limit();
      if (limit < 0) {
        tracker->consumption_->Add(bytes); // No limit at this tracker.
      } else {
        // If TryConsume fails, we can try to GC or expand the RM reservation, but we may
        // need to try several times if there are concurrent consumers because we don't
        // take a lock before trying to update consumption_.
        bool fail_consume = false;
        while (!tracker->consumption_->TryAdd(bytes, limit)) {
          VLOG_RPC << "TryConsume failed, bytes=" << bytes
                   << " consumption=" << tracker->consumption_->current_value()
                   << " limit=" << limit << " attempting to GC and expand reservation";
          // TODO: This may not be right if more than one tracker can actually change its
          // RM reservation limit.
          if (tracker->GcMemory(limit - bytes) && !tracker->ExpandRmReservation(bytes)) {
            fail_consume = true;
            break;
          }
          VLOG_RPC << "GC or expansion succeeded, TryConsume bytes=" << bytes
                   << " consumption=" << tracker->consumption_->current_value()
                   << " new limit=" << tracker->effective_limit() << " prev=" << limit;
          // Need to update the limit if the RM reservation was expanded.
          limit = tracker->effective_limit();
        }
        if (fail_consume) break;
      }
    }
    // Everyone succeeded, return.
    if (i == -1) return true;

    // Someone failed, roll back the ones that succeeded.
    // TODO: this doesn't roll it back completely since the max values for
    // the updated trackers aren't decremented. The max values are only used
    // for error reporting so this is probably okay. Rolling those back is
    // pretty hard; we'd need something like 2PC.
    //
    // TODO: This might leave us with an allocated resource that we can't use. Do we need
    // to adjust the consumption of the query tracker to stop the resource from never
    // getting used by a subsequent TryConsume()?
    for (int j = all_trackers_.size() - 1; j > i; --j) {
      all_trackers_[j]->consumption_->Add(-bytes);
    }
    return false;
  }

  /// Decreases consumption of this tracker and its ancestors by 'bytes'.
  void Release(int64_t bytes) {
    if (bytes < 0) {
      Consume(-bytes);
      return;
    }

    if (UNLIKELY(released_memory_since_gc_.UpdateAndFetch(bytes)) > GC_RELEASE_SIZE) {
      GcTcmalloc();
    }

    if (consumption_metric_ != NULL) {
      DCHECK(parent_ == NULL);
      consumption_->Set(consumption_metric_->value());
      return;
    }
    if (bytes == 0) return;
    if (UNLIKELY(enable_logging_)) LogUpdate(false, bytes);
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

  /// Returns the memory consumed in bytes.
  int64_t consumption() const { return consumption_->current_value(); }

  /// Note that if consumption_ is based on consumption_metric_, this will the max value
  /// we've recorded in consumption(), not necessarily the highest value
  /// consumption_metric_ has ever reached.
  int64_t peak_consumption() const { return consumption_->value(); }

  MemTracker* parent() const { return parent_; }

  /// Signature for function that can be called to free some memory after limit is reached.
  typedef boost::function<void ()> GcFunction;

  /// Add a function 'f' to be called if the limit is reached.
  /// 'f' does not need to be thread-safe as long as it is added to only one MemTracker.
  /// Note that 'f' must be valid for the lifetime of this MemTracker.
  void AddGcFunction(GcFunction f) { gc_functions_.push_back(f); }

  /// Register this MemTracker's metrics. Each key will be of the form
  /// "<prefix>.<metric name>".
  void RegisterMetrics(MetricGroup* metrics, const std::string& prefix);

  /// Logs the usage of this tracker and all of its children (recursively).
  std::string LogUsage(const std::string& prefix = "") const;

  void EnableLogging(bool enable, bool log_stack) {
    enable_logging_ = enable;
    log_stack_ = log_stack;
  }

  static const std::string COUNTER_NAME;

 private:
  bool CheckLimitExceeded() const { return limit_ >= 0 && limit_ < consumption(); }

  /// If consumption is higher than max_consumption, attempts to free memory by calling any
  /// added GC functions.  Returns true if max_consumption is still exceeded. Takes
  /// gc_lock. Updates metrics if initialized.
  bool GcMemory(int64_t max_consumption);

  /// Called when the total release memory is larger than GC_RELEASE_SIZE.
  /// TcMalloc holds onto released memory and very slowly (if ever) releases it back to
  /// the OS. This is problematic since it is memory we are not constantly tracking which
  /// can cause us to go way over mem limits.
  void GcTcmalloc();

  /// Set the resource mgr to allow expansion of limits (if NULL, no expansion is possible)
  void SetQueryResourceMgr(QueryResourceMgr* context) {
    query_resource_mgr_ = context;
  }

  /// Walks the MemTracker hierarchy and populates all_trackers_ and
  /// limit_trackers_
  void Init();

  /// Adds tracker to child_trackers_
  void AddChildTracker(MemTracker* tracker);

  /// Logs the stack of the current consume/release. Used for debugging only.
  void LogUpdate(bool is_consume, int64_t bytes) const;

  static std::string LogUsage(const std::string& prefix,
      const std::list<MemTracker*>& trackers);

  /// Try to expand the limit (by asking the resource broker for more memory) by at least
  /// 'bytes'. Returns false if not possible, true if the request succeeded. May allocate
  /// more memory than was requested.
  bool ExpandRmReservation(int64_t bytes);

  /// Size, in bytes, that is considered a large value for Release() (or Consume() with
  /// a negative value). If tcmalloc is used, this can trigger it to GC.
  /// A higher value will make us call into tcmalloc less often (and therefore more
  /// efficient). A lower value will mean our memory overhead is lower.
  /// TODO: this is a stopgap.
  static const int64_t GC_RELEASE_SIZE = 128 * 1024L * 1024L;

  /// Total amount of memory from calls to Release() since the last GC. If this
  /// is greater than GC_RELEASE_SIZE, this will trigger a tcmalloc gc.
  static AtomicInt<int64_t> released_memory_since_gc_;

  /// Lock to protect GcMemory(). This prevents many GCs from occurring at once.
  SpinLock gc_lock_;

  /// Protects request_to_mem_trackers_ and pool_to_mem_trackers_
  static boost::mutex static_mem_trackers_lock_;

  /// All per-request MemTracker objects that are in use.  For memory management, this map
  /// contains only weak ptrs.  MemTrackers that are handed out via GetQueryMemTracker()
  /// are shared ptrs.  When all the shared ptrs are no longer referenced, the MemTracker
  /// d'tor will be called at which point the weak ptr will be removed from the map.
  typedef boost::unordered_map<TUniqueId, boost::weak_ptr<MemTracker> >
  RequestTrackersMap;
  static RequestTrackersMap request_to_mem_trackers_;

  /// All per-request pool MemTracker objects. It is assumed that request pools will live
  /// for the entire duration of the process lifetime.
  typedef boost::unordered_map<std::string, MemTracker*> PoolTrackersMap;
  static PoolTrackersMap pool_to_mem_trackers_;

  /// Only valid for MemTrackers returned from GetQueryMemTracker()
  TUniqueId query_id_;

  /// Only valid for MemTrackers returned from GetRequestPoolMemTracker()
  std::string pool_name_;

  /// Hard limit on memory consumption, in bytes. May not be exceeded. If limit_ == -1,
  /// there is no consumption limit.
  int64_t limit_;

  /// If > -1, when RM is enabled this is the limit after which this memtracker needs to
  /// acquire more memory from Llama.
  /// This limit is always less than or equal to the hard limit.
  int64_t rm_reserved_limit_;

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

  std::vector<MemTracker*> all_trackers_;  // this tracker plus all of its ancestors
  std::vector<MemTracker*> limit_trackers_;  // all_trackers_ with valid limits

  /// All the child trackers of this tracker. Used for error reporting only.
  /// i.e., Updating a parent tracker does not update the children.
  mutable boost::mutex child_trackers_lock_;
  std::list<MemTracker*> child_trackers_;

  /// Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
  /// remove.
  std::list<MemTracker*>::iterator child_tracker_it_;

  /// Functions to call after the limit is reached to free memory.
  std::vector<GcFunction> gc_functions_;

  /// If true, calls UnregisterFromParent() in the dtor. This is only used for
  /// the query wide trackers to remove it from the process mem tracker. The
  /// process tracker never gets deleted so it is safe to reference it in the dtor.
  /// The query tracker has lifetime shared by multiple plan fragments so it's hard
  /// to do cleanup another way.
  bool auto_unregister_;

  /// If true, logs to INFO every consume/release called. Used for debugging.
  bool enable_logging_;
  /// If true, log the stack as well.
  bool log_stack_;

  /// If false, this tracker (and its children) will not be included in LogUsage() output
  /// if consumption is 0.
  bool log_usage_if_zero_;

  /// Lock is taken during ExpandRmReservation() to prevent concurrent acquisition of new
  /// resources.
  boost::mutex resource_acquisition_lock_;

  /// If non-NULL, contains all the information required to expand resource reservations if
  /// required.
  QueryResourceMgr* query_resource_mgr_;

  /// The number of times the GcFunctions were called.
  IntCounter* num_gcs_metric_;

  /// The number of bytes freed by the last round of calling the GcFunctions (-1 before any
  /// GCs are performed).
  IntGauge* bytes_freed_by_last_gc_metric_;

  /// The number of bytes over the limit we were the last time LimitExceeded() was called
  /// and the limit was exceeded pre-GC. -1 if there is no limit or the limit was never
  /// exceeded.
  IntGauge* bytes_over_limit_metric_;
};

}

#endif
