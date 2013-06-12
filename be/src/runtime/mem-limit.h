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


#ifndef IMPALA_RUNTIME_MEM_LIMIT_H
#define IMPALA_RUNTIME_MEM_LIMIT_H

#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "common/logging.h"
#include "common/atomic.h"
#include "util/debug-util.h"

#include "gen-cpp/Types_types.h" // for TUniqueId

namespace impala {

// A MemLimit tracks memory consumption against a particular limit.
// Thread-safe.
class MemLimit {
 public:
  MemLimit(int64_t byte_limit)
    : limit_(byte_limit),
      consumption_(0),
      peak_consumption_(0) {
  }

  ~MemLimit();

  // Returns a MemLimit object for 'id'.  Calling this with the same id will return
  // the same MemLimit object.  An example of how this is used is to pass it the
  // same query_id for all fragments of that query running on this machine.  This
  // way, we have per-query limits rather than per fragment.
  // The first time this is called for an id, a new MemLimit object is created.
  // byte_limit is expected to be the same for all GetMemLimit() calls with the
  // same id.
  static boost::shared_ptr<MemLimit> GetMemLimit(
      const TUniqueId& id, int64_t byte_limit);

  void Consume(int64_t bytes) {
    int64_t newval = __sync_add_and_fetch(&consumption_, bytes);
    DCHECK_GE(consumption_, 0);
    peak_consumption_.UpdateMax(newval);
  }

  void Release(int64_t bytes) {
    __sync_fetch_and_add(&consumption_, -1 * bytes);
    DCHECK_GE(consumption_, 0);
  }

  bool LimitExceeded() {
    return limit_ < consumption_;
  }

  int64_t limit() const { return limit_; }
  int64_t consumption() const { return consumption_; }
  int64_t peak_consumption() const { return peak_consumption_; }

  static void UpdateLimits(int64_t bytes, std::vector<MemLimit*>* limits) {
    for (std::vector<MemLimit*>::iterator i = limits->begin(); i != limits->end(); ++i) {
      (*i)->Consume(bytes);
    }
  }

  static bool LimitExceeded(const std::vector<MemLimit*>& limits) {
    for (std::vector<MemLimit*>::const_iterator i = limits.begin(); i != limits.end();
         ++i) {
      if ((*i)->LimitExceeded()) {
        // TODO: move this to per query log
        LOG(INFO) << "Query: " << PrintId((*i)->id_) << "Exceeded limit: limit="
                  << (*i)->limit() << " consumption=" << (*i)->consumption();
        return true;
      }
    }
    return false;
  }

 private:
  // All mem limit objects that are in use and lock protecting it.
  // For memory management, this map contains only weak ptrs.  MemLimits that are
  // handed out via GetMemLimit are shared ptrs.  When all the shared ptrs are no
  // longer referenced, the MemLimit d'tor will be called at which point the
  // weak ptr will be removed from the map.
  typedef boost::unordered_map<TUniqueId, boost::weak_ptr<MemLimit> > Limits;
  static Limits limits_;
  static boost::mutex limits_lock_;

  TUniqueId id_;
  int64_t limit_;  // in bytes
  int64_t consumption_;  // in bytes
  AtomicInt<int64_t> peak_consumption_; // in bytes
};

}

#endif

