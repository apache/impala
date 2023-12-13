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


#ifndef IMPALA_EXPRS_LIKE_PREDICATE_H_
#define IMPALA_EXPRS_LIKE_PREDICATE_H_

#include <boost/scoped_ptr.hpp>
#include <re2/re2.h>
#include <string>

#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"
#include "runtime/string-search.h"
#include "udf/udf.h"

namespace re2 {
class RE2;
}

namespace impala {

/// This class handles the Like, Regexp, and Rlike predicates and uses the udf interface.
class LikePredicate: public Predicate {
 public:
  ~LikePredicate() { }

 protected:
  friend class ScalarExprEvaluator;

  LikePredicate(const TExprNode& node)
      : Predicate(node) { }

 private:
  typedef impala_udf::BooleanVal (*LikePredicateFunction) (impala_udf::FunctionContext*,
      const impala_udf::StringVal&, const impala_udf::StringVal&);

  struct LikePredicateState {
    char escape_char_;

    /// This is the function, set in the prepare function, that will be used to determine
    /// the value of the predicate. It will be set depending on whether the expression is
    /// a LIKE, RLIKE or REGEXP predicate, whether the pattern is a constant argument
    /// and whether the pattern has any constant substrings. If the pattern is not a
    /// constant argument, none of the following fields can be set because we cannot know
    /// the format of the pattern in the prepare function and must deal with each pattern
    /// seperately.
    LikePredicateFunction function_;

    /// Holds the string the StringValue points to and is set any time StringValue is
    /// used.
    std::string search_string_;

    /// Used for LIKE predicates if the pattern is a constant argument, and is either a
    /// constant string or has a constant string at the beginning or end of the pattern.
    /// This will be set in order to check for that pattern in the corresponding part of
    /// the string.
    StringValue search_string_sv_;

    /// Used for LIKE predicates if the pattern is a constant argument and has a constant
    /// string in the middle of it. This will be use in order to check for the substring
    /// in the value.
    StringSearch substring_pattern_;

    /// Used for RLIKE and REGEXP predicates if the pattern is a constant argument.
    boost::scoped_ptr<re2::RE2> regex_;

    /// Used for ILIKE and IREGEXP predicates if the pattern is not a constant argument.
    bool case_sensitive_;

    LikePredicateState() : escape_char_('\\') {
    }

    void SetSearchString(const std::string& search_string) {
      search_string_ = search_string;
      search_string_sv_ = StringValue(search_string_);
      substring_pattern_ = StringSearch(&search_string_sv_);
    }
  };

  friend class OpcodeRegistry;

  static void LikePrepare(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void ILikePrepare(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void LikePrepareInternal(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope, bool case_sensitive);

  static impala_udf::BooleanVal Like(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  static void LikeClose(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void RegexPrepare(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void IRegexPrepare(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void RegexPrepareInternal(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope, bool case_sensitive);

  static impala_udf::BooleanVal Regex(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  /// Prepare function for regexp_like() when a third optional parameter is used
  static void RegexpLikePrepare(impala_udf::FunctionContext* context,
      impala_udf::FunctionContext::FunctionStateScope scope);

  /// The cross-compiled wrapper to call RegexpLikeInternal() which is not cross-compiled.
  static impala_udf::BooleanVal RegexpLike(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern,
      const impala_udf::StringVal& match_parameter);

  /// Handles regexp_like() when 3 parameters are passed to it. This is intentionally
  /// not cross-compiled as there is no performance benefit in doing so and it will
  /// consume extra codegen time.
  static impala_udf::BooleanVal RegexpLikeInternal(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern,
      const impala_udf::StringVal& match_parameter);

  static void RegexClose(impala_udf::FunctionContext*,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal RegexFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  static impala_udf::BooleanVal LikeFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  /// Handling of like predicates that map to strstr
  static impala_udf::BooleanVal ConstantSubstringFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  /// Handling of like predicates that can be implemented using strncmp
  static impala_udf::BooleanVal ConstantStartsWithFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  /// Handling of like predicates that can be implemented using strncmp
  static impala_udf::BooleanVal ConstantEndsWithFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  /// Handling of like predicates that can be implemented using strcmp
  static impala_udf::BooleanVal ConstantEqualsFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  static impala_udf::BooleanVal ConstantRegexFnPartial(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      const impala_udf::StringVal& pattern);

  static impala_udf::BooleanVal ConstantRegexFn(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern);

  static impala_udf::BooleanVal RegexMatch(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& val, const impala_udf::StringVal& pattern,
      bool is_like_pattern);

  /// Convert a LIKE pattern (with embedded % and _) into the corresponding
  /// regular expression pattern. Escaped chars are copied verbatim.
  static void ConvertLikePattern(impala_udf::FunctionContext* context,
      const impala_udf::StringVal& pattern, std::string* re_pattern);
};

}  // namespace impala

#endif  // IMPALA_EXPRS_LIKE_PREDICATE_H_
