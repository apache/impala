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

#include "exprs/conditional-functions.h"
#include "exprs/expr.h"
#include "exprs/expr-inline.h"
#include "exprs/case-expr.h"
#include "runtime/tuple-row.h"

using namespace std;

namespace impala {

void* ConditionalFunctions::IsNull(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  void* val = e->children()[0]->GetValue(row);
  if (val != NULL) return val;
  return e->children()[1]->GetValue(row);
}

template <typename T> void* ConditionalFunctions::IfFn(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    T* else_val = reinterpret_cast<T*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    return e->result_.Set(*else_val);
  } else {
    T* then_val = reinterpret_cast<T*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    return e->result_.Set(*then_val);
  }
}

template <typename T> void* ConditionalFunctions::Coalesce(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    T* child = reinterpret_cast<T*>(e->children()[i]->GetValue(row));
    if (child != NULL) return e->result_.Set(*child);
  }
  // No non-null children.
  return NULL;
}

void* ConditionalFunctions::NoCaseComputeFn(Expr* e, TupleRow* row) {
  CaseExpr* expr = static_cast<CaseExpr*>(e);
  // Make sure we set the right compute function.
  DCHECK_EQ(expr->has_case_expr(), false);
  int num_children = e->GetNumChildren();
  int loop_end = (expr->has_else_expr()) ? num_children - 1 : num_children;
  // Need at least when and then expr, and optionally an else.
  DCHECK_GE(num_children, (expr->has_else_expr()) ? 3 : 2);
  for (int i = 0; i < loop_end; i += 2) {
    bool* when_val = reinterpret_cast<bool*>(e->children()[i]->GetValue(row));
    if (when_val == NULL) continue;
    if (*when_val == true) {
      // Return then value.
      return e->children()[i + 1]->GetValue(row);
    }
  }
  if (expr->has_else_expr()) {
    // Return else value.
    return e->children()[num_children - 1]->GetValue(row);
  }
  return NULL;
}

template void* ConditionalFunctions::IfFn<bool>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<int8_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<int16_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<int32_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<int64_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<float>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<double>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<TimestampValue>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::IfFn<StringValue>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<bool>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<int8_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<int16_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<int32_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<int64_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<float>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<double>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<TimestampValue>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::Coalesce<StringValue>(Expr* e, TupleRow* row);

}
