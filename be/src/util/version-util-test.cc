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

#include "util/version-util.h"

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/version_util.h"

using namespace kudu;

// Assert the overloaded equality operators '==' and '!='.
TEST(VersionUtilTest, EqualityOperators) {
  Version lhs;
  Version rhs;

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_TRUE(lhs == rhs);
  ASSERT_FALSE(lhs != rhs);

  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &lhs));
  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &rhs));
  ASSERT_TRUE(lhs == rhs);
  ASSERT_FALSE(lhs != rhs);

  ASSERT_OK(ParseVersion("1.1.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_FALSE(lhs == rhs);
  ASSERT_TRUE(lhs != rhs);

  ASSERT_OK(ParseVersion("1.0.1", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_FALSE(lhs == rhs);
  ASSERT_TRUE(lhs != rhs);

  ASSERT_OK(ParseVersion("1.0.1-SNAPSHOT", &lhs));
  ASSERT_OK(ParseVersion("1.0.0-RELEASE", &rhs));
  ASSERT_FALSE(lhs == rhs);
  ASSERT_TRUE(lhs != rhs);

  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_FALSE(lhs == rhs);
  ASSERT_TRUE(lhs != rhs);
}

// Assert the overloaded less than,and less than or equal to, and greater than operators.
TEST(VersionUtilTest, OperatorLessThan) {
  Version lhs;
  Version rhs;

  ASSERT_OK(ParseVersion("2.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_FALSE(lhs < rhs);
  ASSERT_FALSE(lhs <= rhs);
  ASSERT_TRUE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_FALSE(lhs < rhs);
  ASSERT_TRUE(lhs <= rhs);
  ASSERT_FALSE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &lhs));
  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &rhs));
  ASSERT_FALSE(lhs < rhs);
  ASSERT_TRUE(lhs <= rhs);
  ASSERT_FALSE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("2.0.0", &rhs));
  ASSERT_TRUE(lhs < rhs);
  ASSERT_TRUE(lhs <= rhs);
  ASSERT_FALSE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.1.0", &rhs));
  ASSERT_TRUE(lhs < rhs);
  ASSERT_TRUE(lhs <= rhs);
  ASSERT_FALSE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.1", &rhs));
  ASSERT_TRUE(lhs < rhs);
  ASSERT_TRUE(lhs <= rhs);
  ASSERT_FALSE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0-RELEASE", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_TRUE(lhs < rhs);
  ASSERT_TRUE(lhs <= rhs);
  ASSERT_FALSE(lhs > rhs);

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.0-RELEASE", &rhs));
  ASSERT_FALSE(lhs < rhs);
  ASSERT_FALSE(lhs <= rhs);
  ASSERT_TRUE(lhs > rhs);

  // Asserts the example provided in the comments in version-util.h.
  // 1.0.0-RELEASE, 1.0.0-SNAPSHOT, 1.0.0, 1.0.1-SNAPSHOT
  ASSERT_OK(ParseVersion("1.0.0-RELEASE", &lhs));
  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &rhs));
  ASSERT_TRUE(lhs < rhs);

  ASSERT_OK(ParseVersion("1.0.0-SNAPSHOT", &lhs));
  ASSERT_OK(ParseVersion("1.0.0", &rhs));
  ASSERT_TRUE(lhs < rhs);

  ASSERT_OK(ParseVersion("1.0.0", &lhs));
  ASSERT_OK(ParseVersion("1.0.1-SNAPSHOT", &rhs));
  ASSERT_TRUE(lhs < rhs);
}
