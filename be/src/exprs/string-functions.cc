// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/string-functions.h"
#include "exprs/expr.h"
#include "runtime/tuple-row.h"

using namespace boost;
using namespace std;

namespace impala { 

// Implementation of Substr.  The signature is
//    string substr(string input, int pos, int len)
// This behaves identically to the mysql implemenation, namely:
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

}

