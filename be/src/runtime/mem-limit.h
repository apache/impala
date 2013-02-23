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

namespace impala {

// A MemLimit tracks memory consumption against a particular limit.
// Thread-safe.
// TODO: track peak allocation?
class MemLimit {
 public:
  MemLimit(int64_t byte_limit): limit_(byte_limit), consumption_(0) {}

  void Consume(int64_t bytes) {
    __sync_fetch_and_add(&consumption_, bytes);
    DCHECK_GE(consumption_, 0);
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

  static void UpdateLimits(int64_t bytes, std::vector<MemLimit*>* limits) {
    for (std::vector<MemLimit*>::iterator i = limits->begin(); i != limits->end(); ++i) {
      (*i)->Consume(bytes);
    }
  }

  static bool LimitExceeded(const std::vector<MemLimit*>& limits) {
    for (std::vector<MemLimit*>::const_iterator i = limits.begin(); i != limits.end();
         ++i) {
      if ((*i)->LimitExceeded()) {
        // TODO: remove logging
        LOG(INFO) << "exceeded limit: limit=" << (*i)->limit() << " consumption="
                  << (*i)->consumption();
        return true;
      }
    }
    return false;
  }

 private:
  int64_t limit_;  // in bytes
  int64_t consumption_;  // in bytes
};

}

#endif

