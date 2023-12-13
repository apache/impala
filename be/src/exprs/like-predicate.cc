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
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <sstream>

#include "gutil/strings/substitute.h"
#include "runtime/string-value.inline.h"
#include "string-functions.h"
#include "common/names.h"

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

void LikePredicate::LikePrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  LikePrepareInternal(context, scope, true);
}

void LikePredicate::ILikePrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  LikePrepareInternal(context, scope, false);
}

// TODO: make class StringValue and StringSearch accept a case-sensitive flag and
// switch back to using the cheaper Constant<>() functions.
void LikePredicate::LikePrepareInternal(FunctionContext* context,
    FunctionContext::FunctionStateScope scope, bool case_sensitive) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  LikePredicateState* state = new LikePredicateState();
  context->SetFunctionState(scope, state);
  state->function_ = LikeFn;
  state->case_sensitive_ = case_sensitive;
  if (context->IsArgConstant(1)) {
    StringVal pattern_val = *reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    if (pattern_val.is_null) return;
    StringValue pattern = StringValue::FromStringVal(pattern_val);
    re2::RE2 substring_re("(?:%+)([^%_]*)(?:%+)");
    re2::RE2 ends_with_re("(?:%+)([^%_]*)");
    re2::RE2 starts_with_re("([^%_]*)(?:%+)");
    re2::RE2 equals_re("([^%_]*)");
    re2::RE2 ends_with_escaped_wildcard(".*\\\\%$");
    string pattern_str(pattern.Ptr(), pattern.Len());
    string search_string;
    if (case_sensitive && RE2::FullMatch(pattern_str, substring_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantSubstringFn;
    } else if (case_sensitive &&
        RE2::FullMatch(pattern_str, starts_with_re, &search_string) &&
        !RE2::FullMatch(pattern_str, ends_with_escaped_wildcard)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantStartsWithFn;
    } else if (case_sensitive &&
        RE2::FullMatch(pattern_str, ends_with_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEndsWithFn;
    } else if (case_sensitive &&
        RE2::FullMatch(pattern_str, equals_re, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEqualsFn;
    } else {
      string re_pattern;
      ConvertLikePattern(context,
          *reinterpret_cast<StringVal*>(context->GetConstantArg(1)), &re_pattern);
      RE2::Options opts;
      opts.set_never_nl(false);
      opts.set_dot_nl(true);
      opts.set_case_sensitive(case_sensitive);
      StringFunctions::SetRE2MemOpt(&opts);
      state->regex_.reset(new RE2(re_pattern, opts));
      if (!state->regex_->ok()) {
        context->SetError(Substitute("Invalid regex: $0", pattern_str).c_str());
      }
    }
  }
}

void LikePredicate::LikeClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
        context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    delete state;
    context->SetFunctionState(FunctionContext::THREAD_LOCAL, nullptr);
  }
}

void LikePredicate::RegexPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  RegexPrepareInternal(context, scope, true);
}

void LikePredicate::IRegexPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  RegexPrepareInternal(context, scope, false);
}

void LikePredicate::RegexPrepareInternal(FunctionContext* context,
    FunctionContext::FunctionStateScope scope, bool case_sensitive) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  LikePredicateState* state = new LikePredicateState();
  context->SetFunctionState(scope, state);
  state->function_ = RegexFn;
  state->case_sensitive_ = case_sensitive;
  if (context->IsArgConstant(1)) {
    StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    if (pattern->is_null) return;
    string pattern_str(reinterpret_cast<const char*>(pattern->ptr), pattern->len);
    string search_string;
    // The following four conditionals check if the pattern is a constant string,
    // starts with a constant string and is followed by any number of wildcard characters,
    // ends with a constant string and is preceded by any number of wildcard characters or
    // has a constant substring surrounded on both sides by any number of wildcard
    // characters. In any of these conditions, we can search for the pattern more
    // efficiently by using our own string match functions rather than regex matching.
    if (case_sensitive && RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEqualsFn;
    } else if (case_sensitive &&
        RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantStartsWithFn;
    } else if (case_sensitive &&
        RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantEndsWithFn;
    } else if (case_sensitive &&
        RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
      state->SetSearchString(search_string);
      state->function_ = ConstantSubstringFn;
    } else {
      RE2::Options opts;
      opts.set_case_sensitive(case_sensitive);
      StringFunctions::SetRE2MemOpt(&opts);
      state->regex_.reset(new RE2(pattern_str, opts));
      if (!state->regex_->ok()) {
        context->SetError(
            Substitute("Invalid regex expression: '$0'", pattern_str).c_str());
      }
      state->function_ = ConstantRegexFnPartial;
    }
  }
}

// This prepare function is used only when 3 parameters are passed to the regexp_like()
// function. For the 2 parameter version, the RegexPrepare() function is used to prepare.
void LikePredicate::RegexpLikePrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  LikePredicateState* state = new LikePredicateState();
  context->SetFunctionState(scope, state);
  // If both the pattern and the match parameter are constant, we pre-compile the
  // regular expression once here. Otherwise, the RE is compiled per row in RegexpLike()
  if (context->IsArgConstant(1) && context->IsArgConstant(2)) {
    StringVal* pattern;
    pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    if (pattern->is_null) return;
    StringVal* match_parameter = reinterpret_cast<StringVal*>(context->GetConstantArg(2));
    stringstream error;
    if (match_parameter->is_null) {
      error << "NULL match parameter";
      context->SetError(error.str().c_str());
      return;
    }
    RE2::Options opts;
    string error_str;
    StringFunctions::SetRE2MemOpt(&opts);
    if (!StringFunctions::SetRE2Options(*match_parameter, &error_str, &opts)) {
      context->SetError(error_str.c_str());
      return;
    }
    string pattern_str(reinterpret_cast<const char*>(pattern->ptr), pattern->len);
    state->regex_.reset(new RE2(pattern_str, opts));
    if (!state->regex_->ok()) {
      context->SetError(
          Substitute("Invalid regex expression: '$0'", pattern_str).c_str());
    }
  }
}

// This is used only for the 3 parameter version of regexp_like(). The 2 parameter
// version calls Regex() directly.
BooleanVal LikePredicate::RegexpLikeInternal(FunctionContext* context,
    const StringVal& val, const StringVal& pattern, const StringVal& match_parameter) {
  if (val.is_null || pattern.is_null) return BooleanVal::null();
  // If either the pattern or the third optional match parameter are not constant, we
  // have to recompile the RE for every row.
  if (!context->IsArgConstant(2) || !context->IsArgConstant(1)) {
    if (match_parameter.is_null) return BooleanVal::null();
    RE2::Options opts;
    string error_str;
    StringFunctions::SetRE2MemOpt(&opts);
    if (!StringFunctions::SetRE2Options(match_parameter, &error_str, &opts)) {
      context->SetError(error_str.c_str());
      return BooleanVal(false);
    }
    string re_pattern(reinterpret_cast<const char*>(pattern.ptr), pattern.len);
    re2::RE2 re(re_pattern, opts);
    if (re.ok()) {
      return RE2::PartialMatch(
          re2::StringPiece(reinterpret_cast<const char*>(val.ptr), val.len), re);
    } else {
      context->SetError(Substitute("Invalid regex: $0", re_pattern).c_str());
      return BooleanVal(false);
    }
  }
  return ConstantRegexFnPartial(context, val, pattern);
}

void LikePredicate::RegexClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
        context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    delete state;
    context->SetFunctionState(FunctionContext::THREAD_LOCAL, nullptr);
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
  if (state->search_string_sv_.Len() == 0) return BooleanVal(true);
  StringValue pattern_value = StringValue::FromStringVal(val);
  return BooleanVal(state->substring_pattern_.Search(&pattern_value) != -1);
}

BooleanVal LikePredicate::ConstantStartsWithFn(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (val.len < state->search_string_sv_.Len()) {
    return BooleanVal(false);
  } else {
    StringValue v =
        StringValue(reinterpret_cast<char*>(val.ptr), state->search_string_sv_.Len());
    return BooleanVal(state->search_string_sv_.Eq((v)));
  }
}

BooleanVal LikePredicate::ConstantEndsWithFn(FunctionContext* context,
    const StringVal& val, const StringVal& pattern) {
  if (val.is_null) return BooleanVal::null();
  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  int len = state->search_string_sv_.Len();
  if (val.len < len) {
    return BooleanVal(false);
  } else {
    char* ptr = reinterpret_cast<char*>(val.ptr) + val.len - len;
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

  LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (context->IsArgConstant(1)) {
    if (is_like_pattern) {
      return RE2::FullMatch(re2::StringPiece(reinterpret_cast<const char*>(
          operand_value.ptr), operand_value.len), *state->regex_.get());
    } else {
      return RE2::PartialMatch(re2::StringPiece(reinterpret_cast<const char*>(
          operand_value.ptr), operand_value.len), *state->regex_.get());
    }
  } else {
    string re_pattern;
    RE2::Options opts;
    opts.set_case_sensitive(state->case_sensitive_);
    StringFunctions::SetRE2MemOpt(&opts);
    if (is_like_pattern) {
      ConvertLikePattern(context, pattern_value, &re_pattern);
      opts.set_never_nl(false);
      opts.set_dot_nl(true);
    } else {
      re_pattern =
        string(reinterpret_cast<const char*>(pattern_value.ptr), pattern_value.len);
    }
    re2::RE2 re(re_pattern, opts);
    if (re.ok()) {
      if (is_like_pattern) {
        return RE2::FullMatch(re2::StringPiece(
            reinterpret_cast<const char*>(operand_value.ptr), operand_value.len), re);
      } else {
        return RE2::PartialMatch(
            re2::StringPiece(
                reinterpret_cast<const char*>(operand_value.ptr), operand_value.len),
            re);
      }
    } else {
      string pattern_str(
          reinterpret_cast<const char*>(pattern_value.ptr), pattern_value.len);
      context->SetError(Substitute("Invalid regex: $0", pattern_str).c_str());
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
