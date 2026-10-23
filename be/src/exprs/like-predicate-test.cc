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
#include <string>
#include <vector>

#include "testutil/gtest-util.h"

namespace impala {

class LikePredicateTests {
public:
  static void TestMatchOptimization();
  static bool MatchConstant(const string& pattern, const string& value,
      bool case_sensitive);
  static bool MatchOptimized(const string& pattern, const string& value);
  static bool MatchNonConstant(const string& pattern, const string& value);

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

  // Escaped wildcards are part of the constant string (IMPALA-10849, IMPALA-15641).
  EXPECT_EQ(GetMatchFunction("\\%test", true), &LikePredicate::ConstantEqualsFn);
  EXPECT_EQ(GetMatchFunction("test\\%", true), &LikePredicate::ConstantEqualsFn);
  EXPECT_EQ(GetMatchFunction("\\%test\\%", true), &LikePredicate::ConstantEqualsFn);
  EXPECT_EQ(GetMatchFunction("test\\%%", true), &LikePredicate::ConstantStartsWithFn);
  EXPECT_EQ(GetMatchFunction("%\\%test", true), &LikePredicate::ConstantEndsWithFn);
  EXPECT_EQ(GetMatchFunction("%te\\_st%", true), &LikePredicate::ConstantSubstringFn);
  // An unescaped wildcard in the middle still needs a regex.
  EXPECT_EQ(GetMatchFunction("test\\%_%", true), &LikePredicate::LikeFnPartial);
  // A trailing escape character is a literal.
  EXPECT_EQ(GetMatchFunction("test\\", true), &LikePredicate::ConstantEqualsFn);
  EXPECT_EQ(GetMatchFunction("%test\\", true), &LikePredicate::ConstantEndsWithFn);
  EXPECT_EQ(GetMatchFunction("test%\\", true), &LikePredicate::LikeFn);

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

// Matches 'value' case-sensitively the same way as the function that
// OptimizeConstantPatternMatch() chooses for the constant 'pattern'.
bool LikePredicateTests::MatchOptimized(const string& pattern, const string& value) {
  LikePredicate::LikePredicateState test_state;
  test_state.case_sensitive_ = true;
  test_state.function_ = &LikePredicate::LikeFn;
  LikePredicate::OptimizeConstantPatternMatch(pattern, &test_state);
  const string& search = test_state.search_string_;
  LikePredicate::LikePredicateFunction fn = test_state.function_;
  if (fn == &LikePredicate::ConstantEqualsFn) return value == search;
  if (fn == &LikePredicate::ConstantStartsWithFn) {
    return value.compare(0, search.size(), search) == 0;
  }
  if (fn == &LikePredicate::ConstantEndsWithFn) {
    return value.size() >= search.size() &&
        value.compare(value.size() - search.size(), search.size(), search) == 0;
  }
  if (fn == &LikePredicate::ConstantSubstringFn) {
    return value.find(search) != string::npos;
  }
  return MatchConstant(pattern, value, true);
}

// Matches 'value' case-sensitively the same way as RegexMatch() does for a pattern that
// is not constant.
bool LikePredicateTests::MatchNonConstant(const string& pattern, const string& value) {
  LikePredicate::LikePredicateState test_state;
  StringVal pattern_val(
      reinterpret_cast<uint8_t*>(const_cast<char*>(pattern.data())), pattern.size());
  string re_pattern;
  LikePredicate::ConvertLikePattern(&test_state, pattern_val, &re_pattern);
  RE2::Options opts;
  opts.set_never_nl(false);
  opts.set_dot_nl(true);
  RE2 re(re_pattern, opts);
  return RE2::FullMatch(re2::StringPiece(value.data(), value.size()), re);
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

// IMPALA-15641: The fast paths for constant patterns must match the same values as the
// regex that is used for patterns that are not constant.
TEST(TestLikePredicate, ConstantMatchesNonConstant) {
  const std::vector<string> tokens = {"%", "_", "\\%", "\\_", "\\\\", "\\", "\\x", "x"};
  std::vector<string> patterns;
  for (const string& t : tokens) {
    for (const string& p : {t + "ab", "a" + t + "b", "ab" + t, t + "ab%", "%ab" + t,
             "%" + t + "%", "a" + t + "b%", "%a" + t + "b", "%a" + t + "b%"}) {
      patterns.push_back(p);
    }
  }
  const std::vector<string> values = {"", "ab", "xab", "axb", "abx", "%ab", "a%b",
      "ab%", "_ab", "a_b", "ab_", "\\ab", "a\\b", "ab\\", "\\xab", "a\\xb", "ab\\x",
      "a\\\\b", "x", "%", "_", "\\", "\\x", "ya\\bz", "ya%bz", "yaxbz"};
  for (const string& p : patterns) {
    for (const string& v : values) {
      EXPECT_EQ(LikePredicateTests::MatchOptimized(p, v),
          LikePredicateTests::MatchNonConstant(p, v))
          << "pattern: " << p << ", value: " << v;
    }
  }

  // The escape character only escapes the wildcards and itself.
  for (auto match : {&LikePredicateTests::MatchOptimized,
           &LikePredicateTests::MatchNonConstant}) {
    EXPECT_TRUE(match("C:\\dir%", "C:\\dir1"));
    EXPECT_FALSE(match("C:\\dir%", "C:dir1"));
    EXPECT_TRUE(match("a\\\\b%", "a\\bc"));
    EXPECT_FALSE(match("a\\\\b%", "a\\\\bc"));
    EXPECT_TRUE(match("a\\%%", "a%b"));
    EXPECT_FALSE(match("a\\%%", "a\\b"));
    EXPECT_TRUE(match("a%\\", "ab\\"));
    EXPECT_FALSE(match("a%\\", "ab"));
  }
}

} // namespace impala
