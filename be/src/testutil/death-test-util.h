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

#ifndef IMPALA_TESTUTIL_DEATH_TEST_UTIL_H_
#define IMPALA_TESTUTIL_DEATH_TEST_UTIL_H_

#include <sys/resource.h>

#include "util/minidump.h"

// Wrapper around gtest's ASSERT_DEBUG_DEATH that prevents coredumps and minidumps
// being generated as the result of the death test.
#define IMPALA_ASSERT_DEBUG_DEATH(fn, msg)    \
  do {                                        \
    ScopedCoredumpDisabler disable_coredumps; \
    ASSERT_DEBUG_DEATH(fn, msg);              \
  } while (false);

namespace impala {

/// Utility class that disables coredumps and minidumps within a given scope.
struct ScopedCoredumpDisabler {
 public:
  ScopedCoredumpDisabler() {
    getrlimit(RLIMIT_CORE, &limit_before_);
    rlimit limit;
    limit.rlim_cur = limit.rlim_max = 0;
    setrlimit(RLIMIT_CORE, &limit);

    minidumps_enabled_before_ = EnableMinidumpsForTest(false);
  }

  ~ScopedCoredumpDisabler() {
    setrlimit(RLIMIT_CORE, &limit_before_);

    EnableMinidumpsForTest(minidumps_enabled_before_);
  }

 private:
  rlimit limit_before_;
  bool minidumps_enabled_before_;
};
}

#endif
