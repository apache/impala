// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exprs/like-predicate.h"

#include <sstream>
#include <boost/regex.hpp>
#include <string.h>

#include "runtime/string-value.inline.h"

using namespace boost;
using namespace std;

namespace impala {

LikePredicate::LikePredicate(const TExprNode& node)
  : Predicate(node),
    escape_char_(node.like_pred.escape_char[0]) {
  DCHECK_EQ(node.like_pred.escape_char.size(), 1);
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

void* LikePredicate::ConstantRegexFn(Expr* e, TupleRow* row) {
  LikePredicate* p = static_cast<LikePredicate*>(e);
  DCHECK_EQ(p->GetNumChildren(), 2);
  StringValue* operand_val = static_cast<StringValue*>(e->GetChild(0)->GetValue(row));
  if (operand_val == NULL) return NULL;
  p->result_.bool_val = regex_match(operand_val->ptr,
      operand_val->ptr + operand_val->len, *p->regex_);
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
  try {
    regex re(re_pattern, regex_constants::extended);
    p->result_.bool_val = regex_match(operand_value->ptr,
        operand_value->ptr + operand_value->len, re);
    return &p->result_.bool_val;
  } catch (bad_expression& e) {
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

Status LikePredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  DCHECK_EQ(children_.size(), 2);
  if (GetChild(1)->IsConstant()) {
    // determine pattern and decide on eval fn
    StringValue* pattern = static_cast<StringValue*>(GetChild(1)->GetValue(NULL));
    string pattern_str(pattern->ptr, pattern->len);
    // Generate a regex search to look for simple patterns: 
    // - "%anything%": This maps to a fast substring search implementation. 
    // - anything%: This maps to a strncmp implementation
    // - %anything: This maps to a strncmp implementation
    // Regex marks the "anything" so it can be extracted for the pattern.
    regex substring_re("(%+)([^%_]*)(%+)", regex::extended);
    regex ends_with_re("(%+)([^%_]*)", regex::extended);
    regex starts_with_re("([^%_]*)(%+)", regex::extended);
    smatch match_res;
    if (opcode_ == TExprOpcode::LIKE) {
      if (regex_match(pattern_str, match_res, substring_re)) {
        // match_res.str(0) is the whole string, match_res.str(1) the first group, etc.
        search_string_ = match_res.str(2);
        search_string_sv_ =
            StringValue(const_cast<char*>(search_string_.c_str()), search_string_.size());
        substring_pattern_ = StringSearch(&search_string_sv_);
        compute_fn_ = ConstantSubstringFn;
        return Status::OK;
      } else if (regex_match(pattern_str, match_res, starts_with_re)) {
        search_string_ = match_res.str(1);
        search_string_sv_ =
            StringValue(const_cast<char*>(search_string_.c_str()), search_string_.size());
        compute_fn_ = ConstantStartsWithFn;
        return Status::OK;
      } else if (regex_match(pattern_str, match_res, ends_with_re)) {
        search_string_ = match_res.str(2);
        search_string_sv_ =
            StringValue(const_cast<char*>(search_string_.c_str()), search_string_.size());
        compute_fn_ = ConstantEndsWithFn;
        return Status::OK;
      } 
    } 
    string re_pattern;
    if (opcode_ == TExprOpcode::LIKE) {
      ConvertLikePattern(pattern, &re_pattern);
    } else {
      re_pattern = pattern_str;
    }
    try {
      regex_.reset(new regex(re_pattern, regex_constants::extended));
    } catch (bad_expression& e) {
      return Status("Invalid regular expression: " + pattern_str);
    }
    compute_fn_ = ConstantRegexFn;
  } else {
    switch (opcode_) {
      case TExprOpcode::LIKE:
        compute_fn_ = LikeFn;
        break;
      case TExprOpcode::REGEX:
        compute_fn_ = RegexFn;
        break;
      default:
        stringstream error;
        error << "Invalid LIKE operator: " << opcode_;
        return Status(error.str());
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
