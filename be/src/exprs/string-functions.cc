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

#include "exprs/string-functions.h"

#include <cctype>
#include <boost/regex.hpp>

#include "exprs/expr.h"
#include "exprs/function-call.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/url-parser.h"

using namespace std;
using namespace boost;

// NOTE: be careful not to use string::append.  It is not performant.
namespace impala {

// Implementation of Substr.  The signature is
//    string substr(string input, int pos, int len)
// This behaves identically to the mysql implementation, namely:
//  - 1-indexed positions
//  - supported negative positions (count from the end of the string)
//  - [optional] len.  No len indicates longest substr possible
template <class T>
void* StringFunctions::Substring(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  Expr* op3 = NULL;
  if (e->GetNumChildren() == 3) op3 = e->children()[2];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  T* pos = reinterpret_cast<T*>(op2->GetValue(row));
  T* len = op3 != NULL ? reinterpret_cast<T*>(op3->GetValue(row)) : NULL;
  if (str == NULL || pos == NULL || (op3 != NULL && len == NULL)) return NULL;
  T fixed_pos = *pos;
  if (fixed_pos < 0) fixed_pos = str->len + fixed_pos + 1;
  T max_len = str->len - fixed_pos + 1;
  T fixed_len = (len == NULL ? max_len : ::min(*len, max_len));
  if (fixed_pos != 0 && fixed_pos <= str->len && fixed_len > 0) {
    e->result_.string_val = str->Substring(fixed_pos - 1, fixed_len);
  } else {
    e->result_.string_val = StringValue();
  }
  return &e->result_.string_val;
}

// Implementation of Left.  The signature is
//    string left(string input, int len)
// This behaves identically to the mysql implementation.
template <class T>
void* StringFunctions::Left(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  T* len = reinterpret_cast<T*>(op2->GetValue(row));
  if (str == NULL || len == NULL) return NULL;
  e->result_.string_val.ptr = str->ptr;
  e->result_.string_val.len = str->len <= *len ? str->len : *len;
  return &e->result_.string_val;
}

// Implementation of Right.  The signature is
//    string right(string input, int len)
// This behaves identically to the mysql implementation.
template <class T>
void* StringFunctions::Right(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  T* len = reinterpret_cast<T*>(op2->GetValue(row));
  if (str == NULL || len == NULL) return NULL;
  e->result_.string_val.len = str->len <= *len ? str->len : *len;
  e->result_.string_val.ptr = str->ptr;
  if (str->len > *len) e->result_.string_val.ptr += str->len - *len;
  return &e->result_.string_val;
}

template <class T>
void* StringFunctions::Space(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  T* num = reinterpret_cast<T*>(e->children()[0]->GetValue(row));
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

template <class T>
void* StringFunctions::Repeat(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  T* num = reinterpret_cast<T*>(e->children()[1]->GetValue(row));
  if (num == NULL || str == NULL) return NULL;
  if (str->len == 0 || *num <= 0) {
    e->result_.string_val.ptr = NULL;
    e->result_.string_val.len = 0;
    return &e->result_.string_val;
  }
  e->result_.string_data.resize(str->len * (*num));
  e->result_.SyncStringVal();

  char* ptr = e->result_.string_val.ptr;
  for (T i = 0; i < *num; ++i) {
    memcpy(ptr, str->ptr, str->len);
    ptr += str->len;
  }
  return &e->result_.string_val;
}

template <class T>
void* StringFunctions::Lpad(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  T* len = reinterpret_cast<T*>(e->children()[1]->GetValue(row));
  StringValue* pad = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || len == NULL || pad == NULL || *len < 0) return NULL;
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (*len <= str->len || pad->len == 0) {
    e->result_.string_val.ptr = str->ptr;
    e->result_.string_val.len = *len;
    return &e->result_.string_val;
  }
  e->result_.string_data.resize(*len);
  e->result_.SyncStringVal();

  // Prepend chars of pad.
  T padded_prefix_len = *len - str->len;
  int pad_index = 0;
  int result_index = 0;
  char* ptr = e->result_.string_val.ptr;

  while (result_index < padded_prefix_len) {
    ptr[result_index++] = pad->ptr[pad_index++];
    pad_index = pad_index % pad->len;
  }
  // Append given string.
  memcpy(ptr + result_index, str->ptr, str->len);
  return &e->result_.string_val;
}

template <class T>
void* StringFunctions::Rpad(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  T* len = reinterpret_cast<T*>(e->children()[1]->GetValue(row));
  StringValue* pad = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || len == NULL || pad == NULL || *len < 0) return NULL;
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (*len <= str->len || pad->len == 0) {
    e->result_.string_val.ptr = str->ptr;
    e->result_.string_val.len = *len;
    return &e->result_.string_val;
  }
  e->result_.string_data.resize(*len);
  e->result_.SyncStringVal();
  char* ptr = e->result_.string_val.ptr;

  memcpy(ptr, str->ptr, str->len);
  // Append chars of pad until desired length.
  int pad_index = 0;
  int result_len = str->len;
  while (result_len < *len) {
    ptr[result_len++] = pad->ptr[pad_index++];
    pad_index = pad_index % pad->len;
  }
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

// Implementation of InitCap
//   string initcap(string input)
// Returns a string identical to the input, but with the first character
// of each word mapped to its upper-case equivalent. All other characters
// will be mapped to their lower-case equivalents. If input == NULL it
// will return NULL
void* StringFunctions::InitCap(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  char* result_ptr = e->result_.string_val.ptr;
  bool word_start = true;
  for (int32_t i = 0; i < str->len; ++i) {
    if (isspace(str->ptr[i])) {
      result_ptr[i] = str->ptr[i];
      word_start = true;
    } else {
      result_ptr[i] = (word_start ? toupper(str->ptr[i]) : tolower(str->ptr[i]));
      word_start = false;
    }
  }
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

void* StringFunctions::Translate(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  StringValue* src = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (src == NULL) return NULL;
  StringValue* dst = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (dst == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  // TODO: if we know src and dst are constant, we can prebuild a conversion
  // table to remove the inner loop.
  int result_len = 0;
  for (int i = 0; i < str->len; ++i) {
    bool matched_src = false;
    for (int j = 0; j < src->len; ++j) {
      if (str->ptr[i] == src->ptr[j]) {
        if (j < dst->len) {
          e->result_.string_val.ptr[result_len++] = dst->ptr[j];
        } else {
          // src[j] doesn't map to any char in dst, the char is dropped.
        }
        matched_src = true;
        break;
      }
    }
    if (!matched_src) e->result_.string_val.ptr[result_len++] = str->ptr[i];
  }

  e->result_.string_val.len = result_len;
  return &e->result_.string_val;
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
  if (str->len == 0) return str;

  // Find new ending position.
  int32_t end = str->len - 1;
  while (end > 0 && str->ptr[end] == ' ') {
    --end;
  }
  DCHECK_GE(end, 0);
  e->result_.string_val.ptr = str->ptr;
  e->result_.string_val.len = (str->ptr[end] == ' ') ? end : end + 1;
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

template <class T>
void* StringFunctions::LocatePos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  T* start_pos = reinterpret_cast<T*>(e->children()[2]->GetValue(row));
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

template <class T>
void* StringFunctions::RegexpExtract(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* pattern = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  T* index = reinterpret_cast<T*>(e->children()[2]->GetValue(row));
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
  // use match_posix to return the leftmost maximal match (and not the first match)
  bool success = regex_search(const_cast<const char*>(str->ptr),
      const_cast<const char*>(str->ptr) + str->len,
      matches, *func_expr->GetRegex(), regex_constants::match_posix);
  if (!success) {
    e->result_.SetStringVal(std::string());
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

  // Pass through if there's only one argument
  if (num_children == 1) return e->children()[0]->GetValue(row);

  // Loop once to compute the final size and reserve space.
  StringValue* strs[num_children];
  for (int32_t i = 0; i < num_children; ++i) {
    strs[i] = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (strs[i] == NULL) return NULL;
    total_size += strs[i]->len;
  }
  e->result_.string_data.resize(total_size);
  e->result_.SyncStringVal();
  char* ptr = e->result_.string_val.ptr;

  // Loop again to append the data.
  for (int32_t i = 0; i < num_children; ++i) {
    memcpy(ptr, strs[i]->ptr, strs[i]->len);
    ptr += strs[i]->len;
  }
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

  // Pass through if there's only one argument
  if (num_children == 1) return e->children()[0]->GetValue(row);

  // Loop once to compute the final size and reserve space.
  StringValue* strs[num_children];
  for (int32_t i = 2; i < num_children; ++i) {
      strs[i] = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
      if (strs[i] == NULL) return NULL;
      total_size += sep->len + strs[i]->len;
    }
  e->result_.string_data.resize(total_size);
  e->result_.SyncStringVal();
  char* ptr = e->result_.string_val.ptr;

  // Loop again to append the data.
  memcpy(ptr, first->ptr, first->len);
  ptr += first->len;
  for (int32_t i = 2; i < num_children; ++i) {
    memcpy(ptr, sep->ptr, sep->len);
    ptr += sep->len;
    memcpy(ptr, strs[i]->ptr, strs[i]->len);
    ptr += strs[i]->len;
  }
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

// Explicit template instantiation is required for proper linking. These functions
// are only indirectly called via a function pointer provided by the opcode registry
// which does not trigger implicit template instantiation.
// Must be kept in sync with common/function-registry/impala_functions.py.
template void* StringFunctions::Substring<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Substring<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Left<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Left<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Right<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Right<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Space<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Space<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Repeat<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Repeat<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Lpad<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Lpad<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Rpad<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::Rpad<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::LocatePos<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::LocatePos<int64_t>(Expr* e, TupleRow* row);
template void* StringFunctions::RegexpExtract<int32_t>(Expr* e, TupleRow* row);
template void* StringFunctions::RegexpExtract<int64_t>(Expr* e, TupleRow* row);

}
