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

#ifndef IMPALA_UTIL_PROMISE_H
#define IMPALA_UTIL_PROMISE_H

#include <boost/thread.hpp>

namespace impala {

// A stripped-down replacement for boost::promise which, to the best of our knowledge,
// actually works. A single producer provides a single value by calling Set(..), which one
// or more consumers retrieve through calling Get(..).
template <typename T>
class Promise {
 public:
  Promise() : val_is_set_(false) { }

  // Copies val into this promise, and notifies any consumers blocked in Get(). It is an
  // error to call Set more than once on the same Promise object.
  void Set(const T& val) {
    boost::unique_lock<boost::mutex> l(val_lock_);
    DCHECK(!val_is_set_) << "Called Set(..) twice on the same Promise";
    val_ = val;
    val_is_set_ = true;
    val_set_cond_.notify_all();
  }

  // Blocks until a value is set, and then returns a reference to that value. Once Get()
  // returns, the returned value will not change, since Set(..) may not be called twice.
  const T& Get() {
    boost::unique_lock<boost::mutex> l(val_lock_);
    while (!val_is_set_) {
      val_set_cond_.wait(l);
    }

    return val_;
  }

 private:
  // These variables deal with coordination between consumer and producer, and protect
  // access to val_;
  boost::condition_variable val_set_cond_;
  bool val_is_set_;
  boost::mutex val_lock_;

  // The actual value transferred from producer to consumer
  T val_;
};

}

#endif
