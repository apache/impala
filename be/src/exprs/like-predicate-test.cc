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
  static bool MatchConstant(const string& pattern, const string& value,
      bool case_sensitive);

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

// Runs a constant pattern through OptimizeConstantPatternMatch() and matches it
// against the given value the same way RegexMatch() does for constant patterns.
bool LikePredicateTests::MatchConstant(const string& pattern, const string& value,
    bool case_sensitive) {
  LikePredicate::LikePredicateState test_state;
  test_state.case_sensitive_ = case_sensitive;
  test_state.function_ = &LikePredicate::LikeFn;
  LikePredicate::OptimizeConstantPatternMatch(pattern, &test_state);
  re2::StringPiece operand(value.data(), value.size());
  if (test_state.function_ == &LikePredicate::LikeFn) {
    return RE2::FullMatch(operand, *test_state.regex_.get());
  }
  return RE2::PartialMatch(operand, *test_state.regex_.get());
}

TEST(TestLikePredicate, RegexMatchOptimization) {
    LikePredicateTests::TestMatchOptimization();
}

// IMPALA-15547 regression: constant LIKE patterns with embedded NUL bytes must not
// be truncated at the first NUL when converting the LIKE pattern to a regex. The
// truncation was introduced by the IMPALA-12374 constant pattern optimization.
TEST(TestLikePredicate, RegexMatchWithEmbeddedNul) {
  // Untrimmed pattern: the trailing % must not make the suffix after the NUL
  // disappear. Before the fix the pattern was truncated to "a%" and "ax" matched.
  EXPECT_FALSE(LikePredicateTests::MatchConstant(
      std::string("a%\0b", 4), "ax", true));
  EXPECT_TRUE(LikePredicateTests::MatchConstant(
      std::string("a%\0b", 4), std::string("a\0b", 3), true));

  // Pattern with leading and trailing %: the trimmed pattern must keep the bytes
  // after the NUL. Before the fix it was truncated to "a", so any value containing
  // "a" (e.g. "xa\0") matched.
  EXPECT_TRUE(LikePredicateTests::MatchConstant(
      std::string("%a\0_%", 5), std::string("xa\0c", 4), true));
  EXPECT_TRUE(LikePredicateTests::MatchConstant(
      std::string("%a\0_%", 5), std::string("xa\0cc", 5), true));
  EXPECT_FALSE(LikePredicateTests::MatchConstant(
      std::string("%a\0_%", 5), std::string("xa\0", 3), true));

  // Case-insensitive (ILIKE) takes the same regex conversion path.
  EXPECT_TRUE(LikePredicateTests::MatchConstant(
      std::string("%a\0_%", 5), std::string("xA\0C", 4), false));
  EXPECT_FALSE(LikePredicateTests::MatchConstant(
      std::string("%a\0_%", 5), std::string("xa\0", 3), false));
}

} // namespace impala