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
#include "util/runtime-profile.h"

#include "gen-cpp/Types_types.h" // for TUniqueId

namespace impala {

class MemTracker;

// A MemTracker tracks memory consumption; it contains an optional limit
// and can be arranged into a tree structure such that the consumption tracked
// by a MemTracker is also tracked by its ancestors.
// Thread-safe.
class MemTracker {
 public:
  // byte_limit < 0 means no limit
  // 'label' is the label used in the usage string (LogUsage())
  MemTracker(int64_t byte_limit = -1, const std::string& label = std::string(),
             MemTracker* parent = NULL);

  // C'tor for tracker for which consumption counter is created as part of a profile.
  // The counter is created with name COUNTER_NAME.
  MemTracker(RuntimeProfile* profile, int64_t byte_limit,
             const std::string& label = std::string(), MemTracker* parent = NULL);

  ~MemTracker();

  // Removes this tracker from parent_->child_trackers_.
  void UnregisterFromParent();

  // Returns a MemTracker object for query 'id'.  Calling this with the same id will
  // return the same MemTracker object.  An example of how this is used is to pass it
  // the same query id for all fragments of that query running on this machine.  This
  // way, we have per-query limits rather than per-fragment.
  // The first time this is called for an id, a new MemTracker object is created with
  // 'parent' as the parent tracker.
  // byte_limit and parent must be the same for all GetMemTracker() calls with the
  // same id.
  static boost::shared_ptr<MemTracker> GetQueryMemTracker(
      const TUniqueId& id, int64_t byte_limit, MemTracker* parent);

  // Increases consumption of this tracker and its ancestors by 'bytes'.
  void Consume(int64_t bytes) {
    if (bytes == 0) return;
    if (UNLIKELY(enable_logging_)) LogUpdate(true, bytes);
    for (std::vector<MemTracker*>::iterator tracker = all_trackers_.begin();
         tracker != all_trackers_.end(); ++tracker) {
      (*tracker)->consumption_->Update(bytes);
      DCHECK_GE((*tracker)->consumption_->current_value(), 0);
    }
  }

  // Increases consumption of this tracker and its ancestors by 'bytes' only if
  // they can all consume 'bytes'. If this brings any of them over, none of them
  // are updated.
  // Returns true if the try succeeded.
  bool TryConsume(int64_t bytes) {
    if (bytes == 0) return true;
    if (UNLIKELY(enable_logging_)) LogUpdate(true, bytes);
    int i = 0;
    for (; i < all_trackers_.size(); ++i) {
      if (all_trackers_[i]->limit_ < 0) {
        all_trackers_[i]->consumption_->Update(bytes);
      } else {
        if (!all_trackers_[i]->consumption_->TryUpdate(bytes, all_trackers_[i]->limit_)) {
          // One of the trackers failed
          break;
        }
      }
    }
    // Everyone succeeded, return.
    if (i == all_trackers_.size()) return true;

    // Someone failed, roll back the ones that succeeded.
    // TODO: this doesn't roll it back completely since the max values for
    // the updated trackers aren't decremented. The max values are only used
    // for error reporting so this is probably okay. Rolling those back is
    // pretty hard; we'd need something like 2PC.
    for (int j = 0; j < i; ++j) {
      all_trackers_[j]->Release(bytes);
    }
    return false;
  }

  // Decreases consumption of this tracker and its ancestors by 'bytes'.
  void Release(int64_t bytes) {
    if (bytes == 0) return;
    if (UNLIKELY(enable_logging_)) LogUpdate(false, bytes);
    for (std::vector<MemTracker*>::iterator tracker = all_trackers_.begin();
         tracker != all_trackers_.end(); ++tracker) {
      (*tracker)->consumption_->Update(-bytes);
      DCHECK_GE((*tracker)->consumption_->current_value(), 0);
    }
  }

  // Returns true if a valid limit of this tracker or one of its ancestors is
  // exceeded.
  bool AnyLimitExceeded() {
    for (std::vector<MemTracker*>::iterator tracker = limit_trackers_.begin();
         tracker != limit_trackers_.end(); ++tracker) {
      if ((*tracker)->LimitExceeded()) return true;
    }
    return false;
  }

  bool LimitExceeded() const {
    return limit_ >= 0 && limit_ < consumption_->current_value();
  }

  int64_t limit() const { return limit_; }
  bool has_limit() const { return limit_ >= 0; }
  int64_t consumption() const { return consumption_->current_value(); }
  int64_t peak_consumption() const { return consumption_->value(); }
  MemTracker* parent() const { return parent_; }

  // Logs the usage of this tracker and all of its children (recursively).
  std::string LogUsage(const std::string& prefix = "") const;

  void EnableLogging(bool enable, bool log_stack) {
    enable_logging_ = enable;
    log_stack_ = log_stack;
  }

  static const std::string COUNTER_NAME;

 private:
  // All MemTracker objects that are in use and lock protecting it.
  // For memory management, this map contains only weak ptrs.  MemTrackers that are
  // handed out via GetQueryMemTracker() are shared ptrs.  When all the shared ptrs are
  // no longer referenced, the MemTracker d'tor will be called at which point the
  // weak ptr will be removed from the map.
  typedef boost::unordered_map<TUniqueId, boost::weak_ptr<MemTracker> > MemTrackersMap;
  static MemTrackersMap uid_to_mem_trackers_;
  static boost::mutex uid_to_mem_trackers_lock_;

  // Only valid for MemTrackers returned from GetMemTracker()
  TUniqueId query_id_;

  int64_t limit_;  // in bytes; < 0: no limit
  std::string label_;
  MemTracker* parent_;

  // in bytes; not owned
  RuntimeProfile::HighWaterMarkCounter* consumption_;

  // holds consumption_ counter if not tied to a profile
  RuntimeProfile::HighWaterMarkCounter local_counter_;

  std::vector<MemTracker*> all_trackers_;  // this tracker plus all of its ancestors
  std::vector<MemTracker*> limit_trackers_;  // all_trackers_ with valid limits

  // All the child trackers of this tracker. Used for error reporting only.
  // i.e., Updating a parent tracker does not update the children.
  mutable boost::mutex child_trackers_lock_;
  std::list<MemTracker*> child_trackers_;

  // Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
  // remove.
  std::list<MemTracker*>::iterator child_tracker_it_;

  // If true, calls UnregisterFromParent() in the dtor. This is only used for
  // the query wide trackers to remove it from the process mem tracker. The
  // process tracker never gets deleted so it is safe to reference it in the dtor.
  // The query tracker has lifetime shared by multiple plan fragments so it's hard
  // to do cleanup another way.
  bool auto_unregister_;

  // If true, logs to INFO every consume/release called. Used for debugging.
  bool enable_logging_;
  // If true, log the stack as well.
  bool log_stack_;

  // Walks the MemTracker hierarchy and populates all_trackers_ and
  // limit_trackers_
  void Init();

  // Adds tracker to child_trackers_
  void AddChildTracker(MemTracker* tracker);

  // Logs the stack of the current consume/release. Used for debugging only.
  void LogUpdate(bool is_consume, int64_t bytes);

  static std::string LogUsage(const std::string& prefix,
      const std::list<MemTracker*>& trackers);
};

}

#endif

