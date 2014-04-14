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

#include <sstream>
#include <string.h>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include "runtime/string-value.inline.h"

using namespace boost;
using namespace std;

namespace impala {

LikePredicate::LikePredicate(const TExprNode& node)
  : Predicate(node),
    escape_char_(node.like_pred.escape_char[0]) {
  DCHECK_EQ(node.like_pred.escape_char.size(), 1);
}

LikePredicate::~LikePredicate() {
}

void* LikePredicate::ConstantSubstringFn(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (val == NULL) return NULL;
  p->result_.bool_val = p->substring_pattern_.Search(val) != -1;
  return &p->result_.bool_val;
}

void* LikePredicate::ConstantStartsWithFn(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (val == NULL) return NULL;
  if (val->len < p->search_string_sv_.len) {
    p->result_.bool_val = false;
  } else {
    StringValue v = *val;
    v.len = p->search_string_sv_.len;
    p->result_.bool_val = p->search_string_sv_.Eq(v);
  }
  return &p->result_.bool_val;
}

void* LikePredicate::ConstantEndsWithFn(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (val == NULL) return NULL;
  if (val->len < p->search_string_sv_.len) {
    p->result_.bool_val = false;
  } else {
    StringValue v = *val;
    v.ptr = v.ptr + (v.len - p->search_string_sv_.len);
    v.len = p->search_string_sv_.len;
    p->result_.bool_val = p->search_string_sv_.Eq(v);
  }
  return &p->result_.bool_val;
}

void* LikePredicate::ConstantEqualsFn(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (val == NULL) return NULL;
  p->result_.bool_val = p->search_string_sv_.Eq(*val);
  return &p->result_.bool_val;
}

void* LikePredicate::ConstantRegexFnPartial(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* operand_val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (operand_val == NULL) return NULL;

  re2::StringPiece operand_sp(operand_val->ptr, operand_val->len);
  p->result_.bool_val = RE2::PartialMatch(operand_sp, *p->regex_);
  return &p->result_.bool_val;
}

void* LikePredicate::ConstantRegexFn(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* operand_val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (operand_val == NULL) return NULL;

  re2::StringPiece operand_sp(operand_val->ptr, operand_val->len);
  p->result_.bool_val = RE2::FullMatch(operand_sp, *p->regex_);
  return &p->result_.bool_val;
}

void* LikePredicate::RegexMatch(Expr* e, TupleRow* row, bool is_like_pattern) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  StringValue* operand_value = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (operand_value == NULL) return NULL;
  StringValue* pattern_value = static_cast<StringValue*>(e->GetChild(1)->GetValue(row));
  if (pattern_value == NULL) return NULL;
  string re_pattern;
  if (is_like_pattern) {
    p->ConvertLikePattern(pattern_value, &re_pattern);
  } else {
    re_pattern = string(pattern_value->ptr, pattern_value->len);
  }
  re2::RE2 re(re_pattern);
  if (re.ok()) {
    if (is_like_pattern) {
      p->result_.bool_val =
          RE2::FullMatch(re2::StringPiece(operand_value->ptr, operand_value->len), re);
    } else {
      p->result_.bool_val =
          RE2::PartialMatch(re2::StringPiece(operand_value->ptr, operand_value->len), re);
    }
    return &p->result_.bool_val;
  } else {
    // TODO: log error in runtime state
    p->result_.bool_val = false;
    return &p->result_.bool_val;
  }
}

void* LikePredicate::LikeFn(Expr* e, TupleRow* row) {
  return RegexMatch(e, row, true);
}

void* LikePredicate::RegexFn(Expr* e, TupleRow* row) {
  return RegexMatch(e, row, false);
}

// There is a difference in the semantics of LIKE and REGEXP
// LIKE only requires explicit use of '%' to preform partial matches
// REGEXP does partial matching by default
Status LikePredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  DCHECK_EQ(children_.size(), 2);
  bool is_like = fn_.name.function_name == "like";
  if (is_like) {
    compute_fn_ = LikeFn;
  } else if (fn_.name.function_name == "regexp" || fn_.name.function_name == "rlike") {
    compute_fn_ = RegexFn;
  } else {
    stringstream error;
    error << "Invalid LIKE operator: " << fn_.name.function_name;
    return Status(error.str());
  }

  if (GetChild(1)->IsConstant()) {
    // determine pattern and decide on eval fn
    StringValue* pattern = static_cast<StringValue*>(GetChild(1)->GetValue(NULL));
    if (pattern == NULL) return Status::OK;
    string pattern_str(pattern->ptr, pattern->len);
    // Generate a regex search to look for simple patterns:
    // - "%anything%": This maps to a fast substring search implementation.
    // - anything%: This maps to a strncmp implementation
    // - %anything: This maps to a strncmp implementation
    // - anything: This maps to a strncmp implementation
    // Regex marks the "anything" so it can be extracted for the pattern.
    re2::RE2 substring_re("(%+)([^%_]*)(%+)");
    re2::RE2 ends_with_re("(%+)([^%_]*)");
    re2::RE2 starts_with_re("([^%_]*)(%+)");
    re2::RE2 equals_re("([^%_]*)");

    DCHECK(substring_re.ok());
    DCHECK(ends_with_re.ok());
    DCHECK(starts_with_re.ok());
    DCHECK(equals_re.ok());
    void* no_arg = NULL;

    if (is_like) {
      if (RE2::FullMatch(pattern_str, substring_re, no_arg, &search_string_, no_arg)) {
        search_string_sv_ = StringValue(search_string_);
        substring_pattern_ = StringSearch(&search_string_sv_);
        compute_fn_ = ConstantSubstringFn;
        return Status::OK;
      } else if (RE2::FullMatch(pattern_str, starts_with_re, &search_string_, no_arg)) {
        search_string_sv_ = StringValue(search_string_);
        compute_fn_ = ConstantStartsWithFn;
        return Status::OK;
      } else if (RE2::FullMatch(pattern_str, ends_with_re, no_arg, &search_string_)) {
        search_string_sv_ = StringValue(search_string_);
        compute_fn_ = ConstantEndsWithFn;
        return Status::OK;
      } else if (RE2::FullMatch(pattern_str, equals_re, &search_string_)) {
        search_string_sv_ = StringValue(search_string_);
        compute_fn_ = ConstantEqualsFn;
        return Status::OK;
      }
    }
    string re_pattern;
    if (is_like) {
      ConvertLikePattern(pattern, &re_pattern);
    } else {
      re_pattern = pattern_str;
    }
    regex_.reset(new RE2(re_pattern));
    if (!regex_->ok()) return Status("Invalid regular expression: " + pattern_str);
    if (fn_.name.function_name == "regexp" || fn_.name.function_name == "rlike") {
      compute_fn_ = ConstantRegexFnPartial;
    } else {
      compute_fn_ = ConstantRegexFn;
    }

  }
  return Status::OK;
}

void LikePredicate::ConvertLikePattern(
    const StringValue* pattern, string* re_pattern) const {
  re_pattern->clear();
  int i = 0;
  bool is_escaped = false;
  while (i < pattern->len) {
    if (!is_escaped && pattern->ptr[i] == '%') {
      re_pattern->append(".*");
    } else if (!is_escaped && pattern->ptr[i] == '_') {
      re_pattern->append(".");
    // check for escape char before checking for regex special chars, they might overlap
    } else if (!is_escaped && pattern->ptr[i] == escape_char_) {
      is_escaped = true;
    } else if (
        pattern->ptr[i] == '.'
        || pattern->ptr[i] == '['
        || pattern->ptr[i] == ']'
        || pattern->ptr[i] == '{'
        || pattern->ptr[i] == '}'
        || pattern->ptr[i] == '('
        || pattern->ptr[i] == ')'
        || pattern->ptr[i] == '\\'
        || pattern->ptr[i] == '*'
        || pattern->ptr[i] == '+'
        || pattern->ptr[i] == '?'
        || pattern->ptr[i] == '|'
        || pattern->ptr[i] == '^'
        || pattern->ptr[i] == '$'
        ) {
      // escape all regex special characters; see list at
      // http://www.boost.org/doc/libs/1_47_0/libs/regex/doc/html/boost_regex/syntax/basic_extended.html
      re_pattern->append("\\");
      re_pattern->append(1, pattern->ptr[i]);
      is_escaped = false;
    } else {
      // regular character or escaped special character
      re_pattern->append(1, pattern->ptr[i]);
      is_escaped = false;
    }
    ++i;
  }
}

}
