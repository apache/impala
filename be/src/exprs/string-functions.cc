// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/string-functions.h"

#include <boost/regex.hpp>

#include "exprs/expr.h"
#include "exprs/function-call.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/url-parser.h"

using namespace std;
using namespace boost;

namespace impala {

// Implementation of Substr.  The signature is
//    string substr(string input, int pos, int len)
// This behaves identically to the mysql implementation, namely:
//  - 1-indexed positions
//  - supported negative positions (count from the end of the string)
//  - [optional] len.  No len indicates longest substr possible
void* StringFunctions::Substring(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  Expr* op3 = NULL;
  if (e->GetNumChildren() == 3) op3 = e->children()[2];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  int* pos = reinterpret_cast<int*>(op2->GetValue(row));
  int* len = op3 != NULL ? reinterpret_cast<int*>(op3->GetValue(row)) : NULL;
  if (str == NULL || pos == NULL || (op3 != NULL && len == NULL)) return NULL;
  string tmp(str->ptr, str->len);
  int fixed_pos = *pos;
  int fixed_len = (len == NULL ? str->len : *len);
  string result;
  if (fixed_pos < 0) fixed_pos = str->len + fixed_pos + 1;
  if (fixed_pos > 0 && fixed_pos <= str->len && fixed_len > 0) {
    result = tmp.substr(fixed_pos - 1, fixed_len);
  }
  e->result_.SetStringVal(result);
  return &e->result_.string_val;
}

// Implementation of Left.  The signature is
//    string left(string input, int len)
// This behaves identically to the mysql implementation.
void* StringFunctions::Left(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  int* len = reinterpret_cast<int*>(op2->GetValue(row));
  if (str == NULL || len == NULL) return NULL;
  e->result_.string_val.ptr = str->ptr;
  e->result_.string_val.len = str->len <= *len ? str->len : *len;
  return &e->result_.string_val;
}

// Implementation of Right.  The signature is
//    string right(string input, int len)
// This behaves identically to the mysql implementation.
void* StringFunctions::Right(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  int* len = reinterpret_cast<int*>(op2->GetValue(row));
  if (str == NULL || len == NULL) return NULL;
  e->result_.string_val.len = str->len <= *len ? str->len : *len;
  e->result_.string_val.ptr = str->ptr;
  if (str->len > *len) e->result_.string_val.ptr += str->len - *len;
  return &e->result_.string_val;
}

// Implementation of LENGTH
//   int length(string input)
// Returns the length in bytes of input. If input == NULL, returns
// NULL per MySQL
void* StringFunctions::Length(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.int_val = str->len;
  return &e->result_.int_val;
}

// Implementation of LOWER
//   string lower(string input)
// Returns a string identical to the input, but with all characters
// mapped to their lower-case equivalents. If input == NULL, returns
// NULL per MySQL.
// Both this function, and Upper, have opportunities for SIMD-based
// optimization (since the current implementation operates on one
// character at a time).
void* StringFunctions::Lower(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  // Writing to string_val.ptr rather than string_data.begin() ensures
  // that we bypass any range checks on the output iterator (since we
  // know for sure that we're writing str->len bytes)
  std::transform(str->ptr, str->ptr + str->len,
                 e->result_.string_val.ptr, ::tolower);

  return &e->result_.string_val;
}

// Implementation of UPPER
//   string upper(string input)
// Returns a string identical to the input, but with all characters
// mapped to their upper-case equivalents. If input == NULL, returns
// NULL per MySQL
void* StringFunctions::Upper(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  // Writing to string_val.ptr rather than string_data.begin() ensures
  // that we bypass any range checks on the output iterator (since we
  // know for sure that we're writing str->len bytes)
  std::transform(str->ptr, str->ptr + str->len,
                 e->result_.string_val.ptr, ::toupper);

  return &e->result_.string_val;
}

// Implementation of REVERSE
//   string upper(string input)
// Returns a string with caracters in reverse order to the input.
// If input == NULL, returns NULL per MySQL
void* StringFunctions::Reverse(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  std::reverse_copy(str->ptr, str->ptr + str->len,
                 e->result_.string_val.ptr);

  return (&e->result_.string_val);
}

void* StringFunctions::Trim(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str->len && str->ptr[begin] == ' ') {
    ++begin;
  }
  // Find new ending position.
  int32_t end = str->len - 1;
  while (end > begin && str->ptr[end] == ' ') {
    --end;
  }
  e->result_.string_val.ptr = str->ptr + begin;
  e->result_.string_val.len = end - begin + 1;
  return &e->result_.string_val;
}

void* StringFunctions::Ltrim(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str->len && str->ptr[begin] == ' ') {
    ++begin;
  }
  e->result_.string_val.ptr = str->ptr + begin;
  e->result_.string_val.len = str->len - begin;
  return &e->result_.string_val;
}

void* StringFunctions::Rtrim(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Find new ending position.
  int32_t end = str->len - 1;
  while (end > 0 && str->ptr[end] == ' ') {
    --end;
  }
  e->result_.string_val.ptr = str->ptr;
  e->result_.string_val.len = (str->ptr[end] == ' ') ? end : end + 1;
  return &e->result_.string_val;
}

void* StringFunctions::Space(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int32_t* num = reinterpret_cast<int32_t*>(e->children()[0]->GetValue(row));
  if (num == NULL) return NULL;
  if (*num <= 0) {
    e->result_.string_val.ptr = NULL;
    e->result_.string_val.len = 0;
    return &e->result_.string_val;
  }
  e->result_.string_data = string(*num, ' ');
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Repeat(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int32_t* num = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  if (num == NULL || str == NULL) return NULL;
  if (str->len == 0 || *num <= 0) {
    e->result_.string_val.ptr = NULL;
    e->result_.string_val.len = 0;
    return &e->result_.string_val;
  }
  e->result_.string_data.reserve(str->len * (*num));
  for (int32_t i = 0; i < *num; ++i) {
    e->result_.string_data.append(str->ptr, str->len);
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Ascii(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Hive returns 0 when given an empty string.
  e->result_.int_val = (str->len == 0) ? 0 : static_cast<int32_t>(str->ptr[0]);
  return &e->result_.int_val;
}

void* StringFunctions::Lpad(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int32_t* len = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  StringValue* pad = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || len == NULL || pad == NULL) return NULL;
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (*len <= str->len || pad->len == 0) {
    e->result_.string_val.ptr = str->ptr;
    e->result_.string_val.len = *len;
    return &e->result_.string_val;
  }
  e->result_.string_data.reserve(*len);
  // Prepend chars of pad.
  int padded_prefix_len = *len - str->len;
  int pad_index = 0;
  while (e->result_.string_data.length() < padded_prefix_len) {
    e->result_.string_data.append(1, pad->ptr[pad_index]);
    ++pad_index;
    pad_index = pad_index % pad->len;
  }
  // Append given string.
  e->result_.string_data.append(str->ptr, str->len);
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Rpad(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int32_t* len = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  StringValue* pad = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || len == NULL || pad == NULL) return NULL;
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (*len <= str->len || pad->len == 0) {
    e->result_.string_val.ptr = str->ptr;
    e->result_.string_val.len = *len;
    return &e->result_.string_val;
  }
  e->result_.string_data.reserve(*len);
  e->result_.string_data.append(str->ptr, str->len);
  // Append chars of pad until desired length.
  int pad_index = 0;
  while (e->result_.string_data.length() < *len) {
    e->result_.string_data.append(1, pad->ptr[pad_index]);
    ++pad_index;
    pad_index = pad_index % pad->len;
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Instr(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (str == NULL || substr == NULL) return NULL;
  StringSearch search(substr);
  // Hive returns positions starting from 1.
  e->result_.int_val = search.Search(str) + 1;
  return &e->result_.int_val;
}

void* StringFunctions::Locate(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (str == NULL || substr == NULL) return NULL;
  StringSearch search(substr);
  // Hive returns positions starting from 1.
  e->result_.int_val = search.Search(str) + 1;
  return &e->result_.int_val;
}

void* StringFunctions::LocatePos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  int32_t* start_pos = reinterpret_cast<int32_t*>(e->children()[2]->GetValue(row));
  if (str == NULL || substr == NULL || start_pos == NULL) return NULL;
  // Hive returns 0 for *start_pos <= 0,
  // but throws an exception for *start_pos > str->len.
  // Since returning 0 seems to be Hive's error condition, return 0.
  if (*start_pos <= 0 || *start_pos > str->len) {
    e->result_.int_val = 0;
    return &e->result_.int_val;
  }
  StringSearch search(substr);
  // Input start_pos starts from 1.
  StringValue adjusted_str(str->ptr + *start_pos - 1, str->len - *start_pos + 1);
  int32_t match_pos = search.Search(&adjusted_str);
  if (match_pos >= 0) {
    // Hive returns the position in the original string starting from 1.
    e->result_.int_val = *start_pos + match_pos;
  } else {
    e->result_.int_val = 0;
  }
  return &e->result_.int_val;
}

void* StringFunctions::RegexpExtract(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* pattern = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  int32_t* index = reinterpret_cast<int32_t*>(e->children()[2]->GetValue(row));
  if (str == NULL || pattern == NULL || index == NULL) return NULL;
  FunctionCall* func_expr = static_cast<FunctionCall*>(e);
  // Compile the regex if pattern is not constant,
  // or if pattern is constant and this is the first function invocation.
  if ((!e->children()[1]->IsConstant()) ||
      (e->children()[1]->IsConstant() && func_expr->GetRegex() == NULL)) {
    string pattern_str(pattern->ptr, pattern->len);
    bool valid_pattern = func_expr->SetRegex(pattern_str);
    // Hive throws an exception for invalid patterns.
    if (!valid_pattern) {
      return NULL;
    }
  }
  DCHECK(func_expr->GetRegex() != NULL);
  cmatch matches;
  // cast's are necessary to make boost understand which function we want.
  bool success = regex_search(const_cast<const char*>(str->ptr),
      const_cast<const char*>(str->ptr) + str->len,
      matches, *func_expr->GetRegex(), regex_constants::match_any);
  if (!success) {
    e->result_.SetStringVal("");
    return &e->result_.string_val;
  }
  // match[0] is the whole string, match_res.str(1) the first group, etc.
  e->result_.SetStringVal(matches[*index]);
  return &e->result_.string_val;
}

void* StringFunctions::RegexpReplace(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* pattern = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  StringValue* replace = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || pattern == NULL || replace == NULL) return NULL;
  FunctionCall* func_expr = static_cast<FunctionCall*>(e);
  // Compile the regex if pattern is not constant,
  // or if pattern is constant and this is the first function invocation.
  if ((!e->children()[1]->IsConstant()) ||
      (e->children()[1]->IsConstant() && func_expr->GetRegex() == NULL)) {
    string pattern_str(pattern->ptr, pattern->len);
    bool valid_pattern = func_expr->SetRegex(pattern_str);
    // Hive throws an exception for invalid patterns.
    if (!valid_pattern) {
      return NULL;
    }
  }
  DCHECK(func_expr->GetRegex() != NULL);
  // Only copy replace if it is not constant, or if it constant
  // and this is the first invocation.
  if ((!e->children()[2]->IsConstant()) ||
      (e->children()[2]->IsConstant() && func_expr->GetReplaceStr() == NULL)) {
    func_expr->SetReplaceStr(replace);
  }
  DCHECK(func_expr->GetReplaceStr() != NULL);
  e->result_.string_data.clear();
  // cast's are necessary to make boost understand which function we want.
  re_detail::string_out_iterator<basic_string<char> >
      out_iter(e->result_.string_data);
  regex_replace(out_iter, const_cast<const char*>(str->ptr),
      const_cast<const char*>(str->ptr) + str->len, *func_expr->GetRegex(),
      *func_expr->GetReplaceStr());
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Concat(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  e->result_.string_data.clear();
  int32_t total_size = 0;
  int32_t num_children = e->GetNumChildren();
  // Loop once to compute the final size and reserve space.
  for (int32_t i = 0; i < num_children; ++i) {
    StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (str == NULL) return NULL;
    total_size += str->len;
  }
  e->result_.string_data.reserve(total_size);
  // Loop again to append the data.
  for (int32_t i = 0; i < e->GetNumChildren(); ++i) {
    StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (str == NULL) return NULL;
    e->result_.string_data.append(str->ptr, str->len);
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::ConcatWs(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 2);
  StringValue* sep = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (sep == NULL) return NULL;
  StringValue* first = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (first == NULL) return NULL;
  e->result_.string_data.clear();
  int32_t total_size = first->len;
  int32_t num_children = e->GetNumChildren();
  // Loop once to compute the final size and reserve space.
  for (int32_t i = 2; i < num_children; ++i) {
      StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
      if (str == NULL) return NULL;
      total_size += sep->len + str->len;
    }
  e->result_.string_data.reserve(total_size);
  // Loop again to append the data.
  e->result_.string_data.append(first->ptr, first->len);
  for (int32_t i = 2; i < num_children; ++i) {
    StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (str == NULL) return NULL;
    e->result_.string_data.append(sep->ptr, sep->len);
    e->result_.string_data.append(str->ptr, str->len);
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::FindInSet(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str_set = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (str == NULL || str_set == NULL) return NULL;
  // Check str for commas.
  for (int i = 0; i < str->len; ++i) {
    if (str->ptr[i] == ',') {
      e->result_.int_val = 0;
      return &e->result_.int_val;
    }
  }
  // The result index starts from 1 since 0 is an error condition.
  int32_t token_index = 1;
  int32_t start = 0;
  int32_t end;
  do {
    end = start;
    // Position end.
    while(str_set->ptr[end] != ',' && end < str_set->len) ++end;
    StringValue token(str_set->ptr + start, end - start);
    if (str->Eq(token)) {
      e->result_.int_val = token_index;
      return &e->result_.int_val;
    }
    // Re-position start and end past ','
    start = end + 1;
    ++token_index;
  } while (start < str_set->len);
  e->result_.int_val = 0;
  return &e->result_.int_val;
}

void* StringFunctions::ParseUrl(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* url = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* part = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (url == NULL || part == NULL) return NULL;
  // We use e->result_.bool_val to indicate whether this is the first invocation.
  // Use e->result_.int_val to hold the URL part enum.
  if (!e->children()[1]->IsConstant() ||
      (e->children()[1]->IsConstant() && !e->result_.bool_val)) {
    e->result_.int_val = UrlParser::GetUrlPart(part);
    e->result_.bool_val = true;
  }
  UrlParser::UrlPart url_part = static_cast<UrlParser::UrlPart>(e->result_.int_val);
  if (!UrlParser::ParseUrl(url, url_part, &e->result_.string_val)) {
    // url is malformed, or url_part is invalid.
    return NULL;
  }
  return &e->result_.string_val;
}

void* StringFunctions::ParseUrlKey(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* url = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* part = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  StringValue* key = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (url == NULL || part == NULL || key == NULL) return NULL;
  // We use e->result_.bool_val to indicate whether this is the first invocation.
  // Use e->result_.int_val to hold the URL part.
  if (!e->children()[1]->IsConstant() ||
      (e->children()[1]->IsConstant() && !e->result_.bool_val)) {
    e->result_.int_val = UrlParser::GetUrlPart(part);
    e->result_.bool_val = true;
  }
  UrlParser::UrlPart url_part = static_cast<UrlParser::UrlPart>(e->result_.int_val);
  if (!UrlParser::ParseUrlKey(url, url_part, key, &e->result_.string_val)) {
    // url is malformed, or url_part is not QUERY.
    return NULL;
  }
  return &e->result_.string_val;
}

}
