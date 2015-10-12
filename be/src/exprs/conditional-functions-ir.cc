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

#include "exprs/anyval-util.h"
#include "exprs/conditional-functions.h"
#include "udf/udf.h"

using namespace impala;
using namespace impala_udf;

#define IS_NULL_COMPUTE_FUNCTION(type) \
  type IsNullExpr::Get##type(ExprContext* context, TupleRow* row) { \
    DCHECK_EQ(children_.size(), 2); \
    type val = children_[0]->Get##type(context, row); \
    if (!val.is_null) return val; /* short-circuit */ \
    return children_[1]->Get##type(context, row); \
  }

IS_NULL_COMPUTE_FUNCTION(BooleanVal);
IS_NULL_COMPUTE_FUNCTION(TinyIntVal);
IS_NULL_COMPUTE_FUNCTION(SmallIntVal);
IS_NULL_COMPUTE_FUNCTION(IntVal);
IS_NULL_COMPUTE_FUNCTION(BigIntVal);
IS_NULL_COMPUTE_FUNCTION(FloatVal);
IS_NULL_COMPUTE_FUNCTION(DoubleVal);
IS_NULL_COMPUTE_FUNCTION(StringVal);
IS_NULL_COMPUTE_FUNCTION(TimestampVal);
IS_NULL_COMPUTE_FUNCTION(DecimalVal);

#define NULL_IF_COMPUTE_FUNCTION(AnyValType) \
  AnyValType NullIfExpr::Get##AnyValType(ExprContext* ctx, TupleRow* row) { \
    DCHECK_EQ(children_.size(), 2); \
    AnyValType lhs_val = children_[0]->Get##AnyValType(ctx, row); \
    /* Short-circuit in case lhs_val is NULL. Can never be equal to RHS. */ \
    if (lhs_val.is_null) return AnyValType::null(); \
    /* Get rhs and return NULL if lhs == rhs, lhs otherwise */ \
    AnyValType rhs_val = children_[1]->Get##AnyValType(ctx, row); \
    if (!rhs_val.is_null && \
        AnyValUtil::Equals(children_[0]->type(), lhs_val, rhs_val)) { \
       return AnyValType::null(); \
    } \
    return lhs_val; \
  }

NULL_IF_COMPUTE_FUNCTION(BooleanVal);
NULL_IF_COMPUTE_FUNCTION(TinyIntVal);
NULL_IF_COMPUTE_FUNCTION(SmallIntVal);
NULL_IF_COMPUTE_FUNCTION(IntVal);
NULL_IF_COMPUTE_FUNCTION(BigIntVal);
NULL_IF_COMPUTE_FUNCTION(FloatVal);
NULL_IF_COMPUTE_FUNCTION(DoubleVal);
NULL_IF_COMPUTE_FUNCTION(StringVal);
NULL_IF_COMPUTE_FUNCTION(TimestampVal);
NULL_IF_COMPUTE_FUNCTION(DecimalVal);

#define NULL_IF_ZERO_COMPUTE_FUNCTION(type) \
  type ConditionalFunctions::NullIfZero(FunctionContext* context, const type& val) { \
    if (val.is_null || val.val == 0) return type::null(); \
    return val; \
  }

DecimalVal ConditionalFunctions::NullIfZero(
    FunctionContext* context, const DecimalVal& val) {
  if (val.is_null) return DecimalVal::null();
  int type_byte_size = Expr::GetConstant<int>(*context, Expr::RETURN_TYPE_SIZE);
  switch (type_byte_size) {
    case 4:
      if (val.val4 == 0) return DecimalVal::null();
      break;
    case 8:
      if (val.val8 == 0) return DecimalVal::null();
      break;
    case 16:
      if (val.val16 == 0) return DecimalVal::null();
      break;
    default:
      DCHECK(false);
  }
  return val;
}

NULL_IF_ZERO_COMPUTE_FUNCTION(TinyIntVal);
NULL_IF_ZERO_COMPUTE_FUNCTION(SmallIntVal);
NULL_IF_ZERO_COMPUTE_FUNCTION(IntVal);
NULL_IF_ZERO_COMPUTE_FUNCTION(BigIntVal);
NULL_IF_ZERO_COMPUTE_FUNCTION(FloatVal);
NULL_IF_ZERO_COMPUTE_FUNCTION(DoubleVal);

#define ZERO_IF_NULL_COMPUTE_FUNCTION(type) \
  type ConditionalFunctions::ZeroIfNull(FunctionContext* context, const type& val) { \
    if (val.is_null) return type(0); \
    return val; \
  }

ZERO_IF_NULL_COMPUTE_FUNCTION(TinyIntVal);
ZERO_IF_NULL_COMPUTE_FUNCTION(SmallIntVal);
ZERO_IF_NULL_COMPUTE_FUNCTION(IntVal);
ZERO_IF_NULL_COMPUTE_FUNCTION(BigIntVal);
ZERO_IF_NULL_COMPUTE_FUNCTION(FloatVal);
ZERO_IF_NULL_COMPUTE_FUNCTION(DoubleVal);
ZERO_IF_NULL_COMPUTE_FUNCTION(DecimalVal);

#define IF_COMPUTE_FUNCTION(type) \
  type IfExpr::Get##type(ExprContext* context, TupleRow* row) { \
    DCHECK_EQ(children_.size(), 3); \
    BooleanVal cond = children_[0]->GetBooleanVal(context, row); \
    if (cond.is_null || !cond.val) { \
      return children_[2]->Get##type(context, row); \
    } \
    return children_[1]->Get##type(context, row); \
  }

IF_COMPUTE_FUNCTION(BooleanVal);
IF_COMPUTE_FUNCTION(TinyIntVal);
IF_COMPUTE_FUNCTION(SmallIntVal);
IF_COMPUTE_FUNCTION(IntVal);
IF_COMPUTE_FUNCTION(BigIntVal);
IF_COMPUTE_FUNCTION(FloatVal);
IF_COMPUTE_FUNCTION(DoubleVal);
IF_COMPUTE_FUNCTION(StringVal);
IF_COMPUTE_FUNCTION(TimestampVal);
IF_COMPUTE_FUNCTION(DecimalVal);

#define COALESCE_COMPUTE_FUNCTION(type) \
  type CoalesceExpr::Get##type(ExprContext* context, TupleRow* row) { \
    DCHECK_GE(children_.size(), 1); \
    for (int i = 0; i < children_.size(); ++i) {                  \
      type val = children_[i]->Get##type(context, row); \
      if (!val.is_null) return val; \
    } \
    return type::null(); \
  }

COALESCE_COMPUTE_FUNCTION(BooleanVal);
COALESCE_COMPUTE_FUNCTION(TinyIntVal);
COALESCE_COMPUTE_FUNCTION(SmallIntVal);
COALESCE_COMPUTE_FUNCTION(IntVal);
COALESCE_COMPUTE_FUNCTION(BigIntVal);
COALESCE_COMPUTE_FUNCTION(FloatVal);
COALESCE_COMPUTE_FUNCTION(DoubleVal);
COALESCE_COMPUTE_FUNCTION(StringVal);
COALESCE_COMPUTE_FUNCTION(TimestampVal);
COALESCE_COMPUTE_FUNCTION(DecimalVal);
