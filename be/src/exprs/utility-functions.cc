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

#include "exprs/utility-functions.h"

#include "exprs/function-call.h"
#include "exprs/expr.h"
#include "util/debug-util.h"
#include "util/time.h"
#include "runtime/tuple-row.h"

using namespace std;

namespace impala {

void* UtilityFunctions::FnvHashString(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* input_val =
      reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (input_val == NULL) return NULL;
  e->result_.bigint_val = HashUtil::FnvHash64(input_val->ptr, input_val->len,
      HashUtil::FNV_SEED);
  return &e->result_.bigint_val;
}

template<int BYTE_SIZE>
void* UtilityFunctions::FnvHash(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  void* input_val = e->children()[0]->GetValue(row);
  if (input_val == NULL) return NULL;
  e->result_.bigint_val = HashUtil::FnvHash64(input_val, BYTE_SIZE, HashUtil::FNV_SEED);
  return &e->result_.bigint_val;
}

// Note that this only hashes the unscaled value and not the scale or precision, so this
// function is only valid when using over a single decimal type.
void* UtilityFunctions::FnvHashDecimal(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  void* input_val = e->children()[0]->GetValue(row);
  if (input_val == NULL) return NULL;
  int byte_size = e->children()[0]->type().GetByteSize();
  e->result_.bigint_val = HashUtil::FnvHash64(input_val, byte_size, HashUtil::FNV_SEED);
  return &e->result_.bigint_val;
}

template void* UtilityFunctions::FnvHash<1>(Expr* e, TupleRow* row);
template void* UtilityFunctions::FnvHash<2>(Expr* e, TupleRow* row);
template void* UtilityFunctions::FnvHash<4>(Expr* e, TupleRow* row);
template void* UtilityFunctions::FnvHash<8>(Expr* e, TupleRow* row);
template void* UtilityFunctions::FnvHash<12>(Expr* e, TupleRow* row);

void* UtilityFunctions::User(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 0);
  // An empty string indicates the user wasn't set in the session
  // or in the query request.
  return (e->result_.string_val.len > 0) ? &e->result_.string_val : NULL;
}

void* UtilityFunctions::Version(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 0);
  e->result_.SetStringVal(GetVersionString());
  return &e->result_.string_val;
}

void* UtilityFunctions::Pid(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 0);
  // Set in FunctionCall::Prepare(), will be -1 if the PID could not be determined
  if (e->result_.int_val == -1) return NULL;
  // Otherwise the PID should be greater than 0
  DCHECK(e->result_.int_val > 0);
  return &e->result_.int_val;
}

void* UtilityFunctions::Sleep(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int* milliseconds = reinterpret_cast<int*>(e->children()[0]->GetValue(row));
  if (milliseconds == NULL) return NULL;
  SleepForMs(*milliseconds);
  e->result_.bool_val = true;
  return &e->result_.bool_val;
}

void* UtilityFunctions::CurrentDatabase(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 0);
  // An empty string indicates the current database wasn't set.
  return (e->result_.string_val.len > 0) ? &e->result_.string_val : NULL;
}


}
