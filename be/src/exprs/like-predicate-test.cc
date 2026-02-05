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

#include "exprs/like-predicate.h"

#include <string.h>

#include "testutil/gtest-util.h"

namespace impala {

class LikePredicateTests {
public:
  static void TestMatchOptimization();

private:
  static LikePredicate::LikePredicateFunction GetMatchFunction(const string& pattern,
      bool case_sensitive);
};

void LikePredicateTests::TestMatchOptimization() {
  // Check that constant strings (with leading/trailing wildcard) use optimized
  // check instead of regex match.
  EXPECT_EQ(GetMatchFunction("test", true), &LikePredicate::ConstantEqualsFn);
  EXPECT_EQ(GetMatchFunction("%test", true), &LikePredicate::ConstantEndsWithFn);
  EXPECT_EQ(GetMatchFunction("test%", true), &LikePredicate::ConstantStartsWithFn);
  EXPECT_EQ(GetMatchFunction("%test%", true), &LikePredicate::ConstantSubstringFn);

  // Multiple % wilcards works the same as single ones.
  EXPECT_EQ(GetMatchFunction("%%test", true), &LikePredicate::ConstantEndsWithFn);
  EXPECT_EQ(GetMatchFunction("test%%", true), &LikePredicate::ConstantStartsWithFn);
  EXPECT_EQ(GetMatchFunction("%%test%%", true), &LikePredicate::ConstantSubstringFn);

  // Optimizations are not enabled for escaped % wildcards.
  // See IMPALA-10849 for more info.
  EXPECT_EQ(GetMatchFunction("\\%test", true), &LikePredicate::LikeFn);
  EXPECT_EQ(GetMatchFunction("test\\%", true), &LikePredicate::LikeFn);
  EXPECT_EQ(GetMatchFunction("\\%test\\%", true), &LikePredicate::LikeFn);

  // IMPALA-12374: Check that more complex patterns with trailing/leading % wildcard
  // use partial regex match, instead of full regex match.
  EXPECT_EQ(GetMatchFunction("%te%st", true), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("te%st%", true), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("%te%st%", true), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("%te%st", false), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("te%st%", false), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("%te%st%", false), &LikePredicate::LikeFnPartial);

  // Constant string match optimization is not enabled for case-insensitive,
  // but partial match is.
  EXPECT_EQ(GetMatchFunction("test", false), &LikePredicate::LikeFn);
  EXPECT_EQ(GetMatchFunction("%test", false), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("test%", false), &LikePredicate::LikeFnPartial);
  EXPECT_EQ(GetMatchFunction("%test%", false), &LikePredicate::LikeFnPartial);

  // Check case that cannot be optimized uses full regex match.
  EXPECT_EQ(GetMatchFunction("te%st", true), &LikePredicate::LikeFn);
}

LikePredicate::LikePredicateFunction LikePredicateTests::GetMatchFunction(
    const string& pattern, bool case_sensitive) {
  LikePredicate::LikePredicateState test_state;
  test_state.case_sensitive_ = case_sensitive;
  test_state.function_ = &LikePredicate::LikeFn;
  LikePredicate::OptimizeConstantPatternMatch(pattern, &test_state);
  return test_state.function_;
}

TEST(TestLikePredicate, RegexMatchOptimization) {
    LikePredicateTests::TestMatchOptimization();
}

} // namespace impala