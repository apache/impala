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

void* UtilityFunctions::Sleep(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int* milliseconds = reinterpret_cast<int*>(e->children()[0]->GetValue(row));
  if (milliseconds == NULL) return NULL;
  SleepForMs(*milliseconds);
  e->result_.bool_val = true;
  return &e->result_.bool_val;
}

}
