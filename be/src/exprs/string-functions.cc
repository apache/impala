// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/string-functions.h"
#include "exprs/expr.h"
#include "runtime/tuple-row.h"

using namespace boost;
using namespace std;

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
  return static_cast<void*>(&e->result_.int_val);
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

  return static_cast<void*>(&e->result_.string_val);
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

  return static_cast<void*>(&e->result_.string_val);
}

}
