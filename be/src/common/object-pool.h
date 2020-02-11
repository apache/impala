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

#pragma once

#include <mutex>
#include <vector>

#include "gutil/macros.h"
#include "util/spinlock.h"

namespace impala {

/// An ObjectPool maintains a list of C++ objects which are deallocated by destroying or
/// clearing the pool.
/// Thread-safe.
class ObjectPool {
 public:
  ObjectPool() {}
  ~ObjectPool() { Clear(); }

  template <class T>
  T* Add(T* t) {
    // TODO: Consider using a lock-free structure.
    std::lock_guard<SpinLock> l(lock_);
    objects_.emplace_back(
        Element{t, [](void* obj) { delete reinterpret_cast<T*>(obj); }});
    return t;
  }

  void Clear() {
    std::lock_guard<SpinLock> l(lock_);
    for (Element& elem : objects_) elem.delete_fn(elem.obj);
    objects_.clear();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ObjectPool);

  /// A generic deletion function pointer. Deletes its first argument.
  typedef void (*DeleteFn)(void*);

  /// For each object, a pointer to the object and a function that deletes it.
  struct Element {
    void* obj;
    DeleteFn delete_fn;
  };
  std::vector<Element> objects_;
  SpinLock lock_;
};

}
