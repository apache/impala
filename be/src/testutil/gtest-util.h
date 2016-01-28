// Copyright 2016 Cloudera Inc.
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

#ifndef IMPALA_TESTUTIL_GTEST_UTIL_H_
#define IMPALA_TESTUTIL_GTEST_UTIL_H_

#include <gtest/gtest.h>
#include "common/status.h"

namespace impala {

// Macros for backend tests to be used when we expect the status to be OK.
#define EXPECT_OK(status) EXPECT_TRUE(status.ok()) << "Error: " << status.GetDetail();
#define ASSERT_OK(status) ASSERT_TRUE(status.ok()) << "Error: " << status.GetDetail();

}
#endif // IMPALA_TESTUTIL_GTEST_UTIL_H_
