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

#ifndef IMPALA_UTIL_DEBUG_RUNTIME_PROFILE_H
#define IMPALA_UTIL_DEBUG_RUNTIME_PROFILE_H

#include "common/object-pool.h"
#include "util/runtime-profile.h"

namespace impala {

/// Utility class to create a self-contained profile that does not report anything.
/// This is useful if there is sometimes a RuntimeProfile associated with an object
/// but not always so that the object can still allocate counters in the same way.
class DummyProfile {
 public:
  DummyProfile() : pool_(), profile_(RuntimeProfile::Create(&pool_, "dummy")) {}
  RuntimeProfile* profile() { return profile_; }

 private:
  ObjectPool pool_;
  RuntimeProfile* const profile_;
};
}
#endif
