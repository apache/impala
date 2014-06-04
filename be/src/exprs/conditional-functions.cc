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

template <typename T> void* ConditionalFunctions::NullIf(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  T* lhs_val = reinterpret_cast<T*>(e->children()[0]->GetValue(row));

  // Short-circuit in case lhs_val is NULL. Can never be equal to RHS.
  if (lhs_val == NULL) return NULL;

  // Get rhs and return NULL if lhs == rhs, lhs otherwise
  T* rhs_val = reinterpret_cast<T*>(e->children()[1]->GetValue(row));
  if ((rhs_val != NULL) && (*lhs_val == *rhs_val)) {
    return NULL;
  } else {
    return lhs_val;
  }
}

void* ConditionalFunctions::NullIfDecimal(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  void* lhs_val = e->children()[0]->GetValue(row);

  // Short-circuit in case lhs_val is NULL. Can never be equal to RHS.
  if (lhs_val == NULL) return NULL;

  // Get rhs and return NULL if lhs == rhs, lhs otherwise
  void* rhs_val = e->children()[1]->GetValue(row);
  if (rhs_val == NULL) return lhs_val;
  switch (e->type().GetByteSize()) {
    case 4:
      if (*reinterpret_cast<Decimal4Value*>(lhs_val) ==
          *reinterpret_cast<Decimal4Value*>(rhs_val)) {
        return NULL;
      }
      return lhs_val;
    case 8:
      if (*reinterpret_cast<Decimal8Value*>(lhs_val) ==
          *reinterpret_cast<Decimal8Value*>(rhs_val)) {
        return NULL;
      }
      return lhs_val;
    case 16:
      if (*reinterpret_cast<Decimal16Value*>(lhs_val) ==
          *reinterpret_cast<Decimal16Value*>(rhs_val)) {
        return NULL;
      }
      return lhs_val;
    default:
      DCHECK(false);
      return NULL;
  }
}

template <typename T> void* ConditionalFunctions::NullIfZero(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  T* argument = reinterpret_cast<T*>(e->children()[0]->GetValue(row));

  if ((argument == NULL) || (*argument == static_cast<T>(0))) {
    return NULL;
  } else {
    return e->result_.Set(*argument);
  }
}

void* ConditionalFunctions::NullIfZeroDecimal(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  void* argument = e->children()[0]->GetValue(row);

  if (argument == NULL) return NULL;
  switch (e->type().GetByteSize()) {
    case 4:
      if (reinterpret_cast<Decimal4Value*>(argument)->value() == 0) return NULL;
      return argument;
    case 8:
      if (reinterpret_cast<Decimal8Value*>(argument)->value() == 0) return NULL;
      return argument;
    case 16:
      if (reinterpret_cast<Decimal16Value*>(argument)->value() == 0) return NULL;
      return argument;
    default:
      DCHECK(false);
      return NULL;
  }
}

template <typename T> void* ConditionalFunctions::ZeroIfNull(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  T* argument = reinterpret_cast<T*>(e->children()[0]->GetValue(row));

  if (argument == NULL) {
    return e->result_.Set(static_cast<T>(0));
  } else {
    return e->result_.Set(*argument);
  }
}

void* ConditionalFunctions::ZeroIfNullDecimal(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  void* argument = e->children()[0]->GetValue(row);

  if (argument != NULL) return argument;
  switch (e->type().GetByteSize()) {
    case 4: return e->result_.Set(Decimal4Value(0));
    case 8: return e->result_.Set(Decimal8Value(0));
    case 16: return e->result_.Set(Decimal16Value(0));
    default:
      DCHECK(false);
      return NULL;
  }
}

void* ConditionalFunctions::IfFn(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    void* else_val = e->children()[2]->GetValue(row);
    if (else_val == NULL) return NULL;
    return else_val;
  } else {
    void* then_val = e->children()[1]->GetValue(row);
    if (then_val == NULL) return NULL;
    return then_val;
  }
}

void* ConditionalFunctions::Coalesce(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    void* child = e->children()[i]->GetValue(row);
    if (child != NULL) return child;
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

template void* ConditionalFunctions::NullIf<bool>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<int8_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<int16_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<int32_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<int64_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<float>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<double>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<TimestampValue>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<StringValue>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<Decimal4Value>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<Decimal8Value>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIf<Decimal16Value>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIfZero<int8_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIfZero<int16_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIfZero<int32_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIfZero<int64_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIfZero<float>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::NullIfZero<double>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::ZeroIfNull<int8_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::ZeroIfNull<int16_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::ZeroIfNull<int32_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::ZeroIfNull<int64_t>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::ZeroIfNull<float>(Expr* e, TupleRow* row);
template void* ConditionalFunctions::ZeroIfNull<double>(Expr* e, TupleRow* row);

}
