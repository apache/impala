// Copyright 2012 Cloudera Inc.
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

#include "exprs/like-predicate.h"

#include <string.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <sstream>

#include "gutil/strings/substitute.h"
#include "runtime/string-value.inline.h"

using namespace boost;
using namespace std;
using namespace boost;
using namespace std;
using namespace impala_udf;
using namespace re2;

namespace impala {
// A regex to match any regex pattern is equivalent to a substring search.
static const RE2 SUBSTRING_RE(
    "(?:\\.\\*)*([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)(?:\\.\\*)*");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 ENDS_WITH_RE(
    "(?:\\.\\*)*([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 STARTS_WITH_RE(
    "\\^([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)(?:\\.\\*)*");

// A regex to match any regex pattern which is equivalent to a constant string match.
static const RE2 EQUALS_RE("\\^([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

LikePredicate::LikePredicate(const TExprNode& node)
  : Predicate(node) {
}

LikePredicate::~LikePredicate() {
}

void LikePredicate::LikePrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  LikePredicateState* state = new LikePredicateState();
  state->function_ = LikeFn;
  context->SetFunctionState(scope, state);
  if (context->IsArgConstant(1)) {
    StringVal pattern_val = *reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    if (pattern_val.is_null) return;
    StringValue pattern = StringValue::FromStringVal(pattern_val);
    re2::RE2 substring_re("(?:%+)([^%_]*)(?:%+)");
    re2::RE2 ends_with_re("(?:%+)([^%_]*)");
    re2::RE2 starts_with_re("([^%_]*)(?:%+)");
    re2::RE2 equals_re("([^%_]*)");
    string pattern_str(pattern.ptr, pattern.len);
    string search_string;
    if (RE2::FullMatch(pattern_str, substring_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantSubstringFn;
    } else if (RE2::FullMatch(pattern_str, starts_with_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantStartsWithFn;
    } else if (RE2::FullMatch(pattern_str, ends_with_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEndsWithFn;
    } else if (RE2::FullMatch(pattern_str, equals_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEqualsFn;
    } else {
      string re_pattern;
      ConvertLikePattern(context,
          *reinterpret_cast<StringVal*>(context->GetConstantArg(1)), &re_pattern);
      state->regex_.reset(new RE2(re_pattern));
      if (!state->regex_->ok()) {
        context->SetError(
            strings::Substitute("Invalid regex: $0", pattern_val.ptr).c_str());
      }
    }
  }
}

BooleanVal LikePredicate::Like(FunctionContext* context, const StringVal& val,
    const StringVal& pattern) {
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return (state->function_)(context, val, pattern);
}

void LikePredicate::LikeClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    delete state;
  }
}

void LikePredicate::RegexPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  LikePredicateState* state = new LikePredicateState();
  context->SetFunctionState(scope, state);
  state->function_ = RegexFn;
  if (context->IsArgConstant(1)) {
    StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    if (pattern->is_null) {
      return;
    }
    string pattern_str(reinterpret_cast<const char*>(pattern->ptr), pattern->len);
    string search_string;
    // The following four conditionals check if the pattern is a constant string,
    // starts with a constant string and is followed by any number of wildcard characters,
    // ends with a constant string and is preceded by any number of wildcard characters or
    // has a constant substring surrounded on both sides by any number of wildcard
    // characters. In any of these conditions, we can search for the pattern more
    // efficiently by using our own string match functions rather than regex matching.
    if (RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEqualsFn;
    } else if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantStartsWithFn;
    } else if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEndsWithFn;
    } else if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantSubstringFn;
    } else {
      state->regex_.reset(new RE2(pattern_str));
      stringstream error;
      if (!state->regex_->ok()) {
        stringstream error;
        error << "Invalid regex expression" << pattern->ptr;
        context->SetError(error.str().c_str());
      }
      state->function_ = ConstantRegexFnPartial;
    }
  }
}

BooleanVal LikePredicate::Regex(FunctionContext* context, const StringVal& val,
    const StringVal& pattern) {
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return (state->function_)(context, val, pattern);
}

void LikePredicate::RegexClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
        context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    delete state;
  }
}

BooleanVal LikePredicate::RegexFn(FunctionContext* context, const StringVal& val,
    const StringVal& pattern) {
  return RegexMatch(context, val, pattern, false);
}

BooleanVal LikePredicate::LikeFn(FunctionContext* context, const StringVal& val,
    const StringVal& pattern) {
  return RegexMatch(context, val, pattern, true);
}

BooleanVal LikePredicate::ConstantSubstringFn(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (state->search_string_sv_.len == 0) return BooleanVal(true);
  StringValue pattern_value = StringValue::FromStringVal(val);
  return BooleanVal(state->substring_pattern_.Search(&pattern_value) != -1);
}

BooleanVal LikePredicate::ConstantStartsWithFn(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (val.len < state->search_string_sv_.len) {
    return BooleanVal(false);
  } else {
    StringValue v =
        StringValue(reinterpret_cast<char*>(val.ptr), state->search_string_sv_.len);
    return BooleanVal(state->search_string_sv_.Eq((v)));
  }
}

BooleanVal LikePredicate::ConstantEndsWithFn(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (val.len < state->search_string_sv_.len) {
    return BooleanVal(false);
  } else {
    char* ptr =
        reinterpret_cast<char*>(val.ptr) + val.len - state->search_string_sv_.len;
    int len = state->search_string_sv_.len;
    StringValue v = StringValue(ptr, len);
    return BooleanVal(state->search_string_sv_.Eq(v));
  }
}

BooleanVal LikePredicate::ConstantEqualsFn(FunctionContext* context, const StringVal& val,
    const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return BooleanVal(state->search_string_sv_.Eq(StringValue::FromStringVal(val)));
}

BooleanVal LikePredicate::ConstantRegexFnPartial(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  re2::StringPiece operand_sp(reinterpret_cast<const char*>(val.ptr), val.len);
  return RE2::PartialMatch(operand_sp, *state->regex_);
}

BooleanVal LikePredicate::ConstantRegexFn(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  re2::StringPiece operand_sp(reinterpret_cast<const char*>(val.ptr), val.len);
  return RE2::FullMatch(operand_sp, *state->regex_);
}

BooleanVal LikePredicate::RegexMatch(FunctionContext* context,
    const StringVal& operand_value, const StringVal& pattern_value,
    bool is_like_pattern) {
  if (operand_value.is_null || pattern_value.is_null) return BooleanVal::null();
  if (context->IsArgConstant(1)) {
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
        context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    if (is_like_pattern) {
      return RE2::FullMatch(re2::StringPiece(reinterpret_cast<const char*>(
          operand_value.ptr), operand_value.len), *state->regex_.get());
    } else {
      return RE2::PartialMatch(re2::StringPiece(reinterpret_cast<const char*>(
          operand_value.ptr), operand_value.len), *state->regex_.get());
    }
  } else {
    string re_pattern;
    if (is_like_pattern) {
      ConvertLikePattern(context, pattern_value, &re_pattern);
    } else {
      re_pattern =
        string(reinterpret_cast<const char*>(pattern_value.ptr), pattern_value.len);
    }
    re2::RE2 re(re_pattern);
    if (re.ok()) {
      if (is_like_pattern) {
        return RE2::FullMatch(re2::StringPiece(
            reinterpret_cast<const char*>(operand_value.ptr), operand_value.len), re);
      } else {
        return RE2::PartialMatch(re2::StringPiece(
            reinterpret_cast<const char*>(operand_value.ptr), operand_value.len), re);
      }
    } else {
      context->SetError(
          strings::Substitute("Invalid regex: $0", pattern_value.ptr).c_str());
      return BooleanVal(false);
    }
  }
}

void LikePredicate::ConvertLikePattern(FunctionContext* context, const StringVal& pattern,
    string* re_pattern) {
  re_pattern->clear();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  bool is_escaped = false;
  for (int i = 0; i < pattern.len; ++i) {
    if (!is_escaped && pattern.ptr[i] == '%') {
      re_pattern->append(".*");
    } else if (!is_escaped && pattern.ptr[i] == '_') {
      re_pattern->append(".");
    // check for escape char before checking for regex special chars, they might overlap
    } else if (!is_escaped && pattern.ptr[i] == state->escape_char_) {
      is_escaped = true;
    } else if (
        pattern.ptr[i] == '.'
        || pattern.ptr[i] == '['
        || pattern.ptr[i] == ']'
        || pattern.ptr[i] == '{'
        || pattern.ptr[i] == '}'
        || pattern.ptr[i] == '('
        || pattern.ptr[i] == ')'
        || pattern.ptr[i] == '\\'
        || pattern.ptr[i] == '*'
        || pattern.ptr[i] == '+'
        || pattern.ptr[i] == '?'
        || pattern.ptr[i] == '|'
        || pattern.ptr[i] == '^'
        || pattern.ptr[i] == '$'
        ) {
      // escape all regex special characters; see list at
      // http://www.boost.org/doc/libs/1_47_0/libs/regex/doc/html/boost_regex/syntax/basic_extended.html
      re_pattern->append("\\");
      re_pattern->append(1, pattern.ptr[i]);
      is_escaped = false;
    } else {
      // regular character or escaped special character
      re_pattern->append(1, pattern.ptr[i]);
      is_escaped = false;
    }
  }
}

}  // namespace impala
