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

#include "runtime/mem-limit.h"
#include "util/uid-util.h"

using namespace boost;
using namespace std;

namespace impala {

MemLimit::Limits MemLimit::limits_;
mutex MemLimit::limits_lock_;

shared_ptr<MemLimit> MemLimit::GetMemLimit(const TUniqueId& id, int64_t byte_limit) {
  lock_guard<mutex> l(limits_lock_);
  Limits::iterator it = limits_.find(id);
  if (it != limits_.end()) {
    // Return the existing MemLimit object for this id, converting the weak ptr
    // to a shared ptr.
    shared_ptr<MemLimit> limit = it->second.lock();
    DCHECK_EQ(limit->limit_, byte_limit);
    DCHECK(id == limit->id_);
    return limit;
  } else {
    // First time this id registered, make a new object.  Give a shared ptr to
    // the caller and put a weak ptr in the map.
    shared_ptr<MemLimit> limit(new MemLimit(byte_limit));
    limit->id_ = id;
    limits_[id] = limit;
    return limit;
  }
}

MemLimit::~MemLimit() {
  lock_guard<mutex> l(limits_lock_);
  // Erase the weak ptr reference from the map.
  limits_.erase(id_);
}

}
