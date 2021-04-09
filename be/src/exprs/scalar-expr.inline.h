// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "exprs/scalar-expr.h"

namespace impala {

/// Macro to generate implementations for the below functions. 'val_type' is
/// a UDF type name, e.g. IntVal and 'type_validation' is a DCHECK expression
/// referencing 'type_' to assert that the function is only called on expressions
/// of the appropriate type.
/// * ScalarExpr::GetBooleanVal()
/// * ScalarExpr::GetTinyIntVal()
/// * ScalarExpr::GetSmallIntVal()
/// * ScalarExpr::GetIntVal()
/// * ScalarExpr::GetBigIntVal()
/// * ScalarExpr::GetFloatVal()
/// * ScalarExpr::GetDoubleVal()
/// * ScalarExpr::GetTimestampVal()
/// * ScalarExpr::GetDecimalVal()
/// * ScalarExpr::GetStringVal()
/// * ScalarExpr::GetDateVal()
/// * ScalarExpr::GetCollectionVal()
/// * ScalarExpr::GetStructVal()
#pragma push_macro("SCALAR_EXPR_GET_VAL")
#define SCALAR_EXPR_GET_VAL(val_type, type_validation)                                 \
  typedef val_type (*val_type##Wrapper)(ScalarExprEvaluator*, const TupleRow*);        \
  inline val_type ScalarExpr::Get##val_type(                                           \
      ScalarExprEvaluator* eval, const TupleRow* row) const {                          \
    DCHECK(type_validation) << type_.DebugString();                                    \
    DCHECK(eval != nullptr);                                                           \
    val_type##Wrapper fn                                                               \
      = reinterpret_cast<val_type##Wrapper>(codegend_compute_fn_.load());              \
    if (fn == nullptr) return Get##val_type##Interpreted(eval, row);                   \
    return fn(eval, row);                                                              \
  }

SCALAR_EXPR_GET_VAL(BooleanVal, type_.type == PrimitiveType::TYPE_BOOLEAN);
SCALAR_EXPR_GET_VAL(TinyIntVal, type_.type == PrimitiveType::TYPE_TINYINT);
SCALAR_EXPR_GET_VAL(SmallIntVal, type_.type == PrimitiveType::TYPE_SMALLINT);
SCALAR_EXPR_GET_VAL(IntVal, type_.type == PrimitiveType::TYPE_INT);
SCALAR_EXPR_GET_VAL(BigIntVal, type_.type == PrimitiveType::TYPE_BIGINT);
SCALAR_EXPR_GET_VAL(FloatVal, type_.type == PrimitiveType::TYPE_FLOAT);
SCALAR_EXPR_GET_VAL(DoubleVal, type_.type == PrimitiveType::TYPE_DOUBLE);
SCALAR_EXPR_GET_VAL(TimestampVal, type_.type == PrimitiveType::TYPE_TIMESTAMP);
SCALAR_EXPR_GET_VAL(DecimalVal, type_.type == PrimitiveType::TYPE_DECIMAL);
SCALAR_EXPR_GET_VAL(StringVal, type_.IsStringType()
    || type_.type == PrimitiveType::TYPE_FIXED_UDA_INTERMEDIATE);
SCALAR_EXPR_GET_VAL(DateVal, type_.type == PrimitiveType::TYPE_DATE);
SCALAR_EXPR_GET_VAL(CollectionVal, type_.IsCollectionType());
SCALAR_EXPR_GET_VAL(StructVal, type_.IsStructType());
#pragma pop_macro("SCALAR_EXPR_GET_VAL")

}
