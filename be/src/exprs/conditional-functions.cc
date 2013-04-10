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
#include "exprs/case-expr.h"
#include "runtime/tuple-row.h"

using namespace std;

namespace impala {

void* ConditionalFunctions::IfBool(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    bool* else_val = reinterpret_cast<bool*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.bool_val = *else_val;
  } else {
    bool* then_val = reinterpret_cast<bool*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.bool_val = *then_val;
  }
  return &e->result_.bool_val;
}

void* ConditionalFunctions::IfTinyint(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    int8_t* else_val = reinterpret_cast<int8_t*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.tinyint_val = *else_val;
  } else {
    int8_t* then_val = reinterpret_cast<int8_t*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.tinyint_val = *then_val;
  }
  return &e->result_.tinyint_val;
}

void* ConditionalFunctions::IfSmallint(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    int16_t* else_val = reinterpret_cast<int16_t*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.smallint_val = *else_val;
  } else {
    int16_t* then_val = reinterpret_cast<int16_t*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.smallint_val = *then_val;
  }
  return &e->result_.smallint_val;
}

void* ConditionalFunctions::IfInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    int32_t* else_val = reinterpret_cast<int32_t*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.int_val = *else_val;
  } else {
    int32_t* then_val = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.int_val = *then_val;
  }
  return &e->result_.int_val;
}

void* ConditionalFunctions::IfBigint(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    int64_t* else_val = reinterpret_cast<int64_t*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.bigint_val = *else_val;
  } else {
    int64_t* then_val = reinterpret_cast<int64_t*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.bigint_val = *then_val;
  }
  return &e->result_.bigint_val;
}

void* ConditionalFunctions::IfFloat(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    float* else_val = reinterpret_cast<float*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.float_val = *else_val;
  } else {
    float* then_val = reinterpret_cast<float*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.float_val = *then_val;
  }
  return &e->result_.float_val;
}

void* ConditionalFunctions::IfDouble(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    double* else_val = reinterpret_cast<double*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.double_val = *else_val;
  } else {
    double* then_val = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.double_val = *then_val;
  }
  return &e->result_.double_val;
}

void* ConditionalFunctions::IfString(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    StringValue* else_val =
        reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.string_val = *else_val;
  } else {
    StringValue* then_val =
        reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.string_val = *then_val;
  }
  return &e->result_.string_val;
}

void* ConditionalFunctions::IfTimestamp(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  bool* cond = reinterpret_cast<bool*>(e->children()[0]->GetValue(row));
  if (cond == NULL || !*cond) {
    TimestampValue* else_val =
        reinterpret_cast<TimestampValue*>(e->children()[2]->GetValue(row));
    if (else_val == NULL) return NULL;
    e->result_.timestamp_val = *else_val;
  } else {
    TimestampValue* then_val =
        reinterpret_cast<TimestampValue*>(e->children()[1]->GetValue(row));
    if (then_val == NULL) return NULL;
    e->result_.timestamp_val = *then_val;
  }
  return &e->result_.timestamp_val;
}

void* ConditionalFunctions::CoalesceBool(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    bool* child = reinterpret_cast<bool*>(e->children()[i]->GetValue(row));
    if (child != NULL) {
      e->result_.bool_val = *child;
      return &e->result_.bool_val;
    }
  }
  // No non-null children.
  return NULL;
}

void* ConditionalFunctions::CoalesceInt(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    int64_t* child = reinterpret_cast<int64_t*>(e->children()[i]->GetValue(row));
    if (child != NULL) {
      e->result_.bigint_val = *child;
      return &e->result_.bigint_val;
    }
  }
  // No non-null children.
  return NULL;
}

void* ConditionalFunctions::CoalesceFloat(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    double* child = reinterpret_cast<double*>(e->children()[i]->GetValue(row));
    if (child != NULL) {
      e->result_.double_val = *child;
      return &e->result_.double_val;
    }
  }
  // No non-null children.
  return NULL;
}

void* ConditionalFunctions::CoalesceString(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    StringValue* child = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (child != NULL) {
      e->result_.string_val = *child;
      return &e->result_.string_val;
    }
  }
  // No non-null children.
  return NULL;
}

void* ConditionalFunctions::CoalesceTimestamp(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  int num_children = e->children().size();
  for (int i = 0; i < num_children; ++i) {
    TimestampValue* child =
        reinterpret_cast<TimestampValue*>(e->children()[i]->GetValue(row));
    if (child != NULL) {
      e->result_.timestamp_val = *child;
      return &e->result_.timestamp_val;
    }
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

}
