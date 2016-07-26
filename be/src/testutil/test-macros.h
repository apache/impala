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

#ifndef IMPALA_TESTUTIL_TEST_MACROS_H
#define IMPALA_TESTUTIL_TEST_MACROS_H

#include <string>

// Helper macros for tests.
// Follows conventions on gtest.h.

// On a non-OK status, adds a failure but doesn't fail the test immediately.
#define EXPECT_OK(status) do { \
    Status _s = status; \
    if (_s.ok()) { \
      SUCCEED(); \
    } else { \
      ADD_FAILURE() << "Bad status: " << _s.GetDetail();  \
    } \
  } while (0);

// On a non-OK status, fails the test immediately. On an OK status records
// success.
#define ASSERT_OK(status) do { \
    Status _s = status; \
    if (_s.ok()) { \
      SUCCEED(); \
    } else { \
      FAIL() << "Bad status: " << _s.GetDetail();  \
    } \
  } while (0);

// Like the above, but doesn't record successful tests.
#define ASSERT_OK_FAST(status) do {      \
    Status _s = status; \
    if (!_s.ok()) { \
      FAIL() << "Bad status: " << _s.GetDetail();  \
    } \
  } while (0);

#endif
