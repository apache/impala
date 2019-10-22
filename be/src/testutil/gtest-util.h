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

#ifndef IMPALA_TESTUTIL_GTEST_UTIL_H_
#define IMPALA_TESTUTIL_GTEST_UTIL_H_

#include <gtest/gtest.h>
#include "common/init.h"
#include "common/status.h"

namespace impala {

// Macros for backend tests to be used when we expect the status to be OK.
#define EXPECT_OK(status)                                          \
  do {                                                             \
    const Status& status_ = (status);                              \
    EXPECT_TRUE(status_.ok()) << "Error: " << status_.GetDetail(); \
  } while (0)

#define ASSERT_OK(status)                                          \
  do {                                                             \
    const Status& status_ = (status);                              \
    ASSERT_TRUE(status_.ok()) << "Error: " << status_.GetDetail(); \
  } while (0)

// Substring matches.
#define EXPECT_STR_CONTAINS(str, substr) \
  EXPECT_PRED_FORMAT2(testing::IsSubstring, substr, str)

// Error code matches.
#define EXPECT_ERROR(status, err)                                       \
  do {                                                                  \
    const Status& status_ = (status);                                   \
    EXPECT_EQ(status_.code(), err) << "Error: " << status_.GetDetail(); \
  } while (0)

// Text of error message matches.
#define ASSERT_ERROR_MSG(status, err)                                        \
  do {                                                                       \
    const Status& status_ = (status);                                        \
    ASSERT_FALSE(status_.ok());                                              \
    ASSERT_EQ(status_.msg().msg(), err) << "Error: " << status_.msg().msg(); \
  } while (0)

// Basic main() function to be used in gtest unit tests. Does not start a JVM and does
// not initialize the FE.
#define IMPALA_TEST_MAIN() \
  int main(int argc, char** argv) { \
    ::testing::InitGoogleTest(&argc, argv); \
    impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST); \
    return RUN_ALL_TESTS(); \
  } \

}
#endif // IMPALA_TESTUTIL_GTEST_UTIL_H_
