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

#include "exprs/scalar-expr-evaluator.h"

namespace impala {

IR_ALWAYS_INLINE FunctionContext* ScalarExprEvaluator::GetFunctionContext(
    ScalarExprEvaluator* eval, int fn_ctx_idx) {
  return eval->fn_context(fn_ctx_idx);
}

IR_ALWAYS_INLINE ScalarExprEvaluator* ScalarExprEvaluator::GetChildEvaluator(
    ScalarExprEvaluator* eval, int idx) {
  DCHECK_LT(idx, eval->childEvaluators_.size());
  return eval->childEvaluators_[idx];
}

IR_ALWAYS_INLINE void* ScalarExprEvaluator::StoreResult(const AnyVal& val,
    const ColumnType& type) {
  using namespace impala_udf;

  if (val.is_null) return nullptr;

  switch (type.type) {
    case TYPE_BOOLEAN: {
      const BooleanVal& v = reinterpret_cast<const BooleanVal&>(val);
      result_.bool_val = v.val;
      return &result_.bool_val;
    }
    case TYPE_TINYINT: {
      const TinyIntVal& v = reinterpret_cast<const TinyIntVal&>(val);
      result_.tinyint_val = v.val;
      return &result_.tinyint_val;
    }
    case TYPE_SMALLINT: {
      const SmallIntVal& v = reinterpret_cast<const SmallIntVal&>(val);
      result_.smallint_val = v.val;
      return &result_.smallint_val;
    }
    case TYPE_INT: {
      const IntVal& v = reinterpret_cast<const IntVal&>(val);
      result_.int_val = v.val;
      return &result_.int_val;
    }
    case TYPE_BIGINT: {
      const BigIntVal& v = reinterpret_cast<const BigIntVal&>(val);
      result_.bigint_val = v.val;
      return &result_.bigint_val;
    }
    case TYPE_FLOAT: {
      const FloatVal& v = reinterpret_cast<const FloatVal&>(val);
      result_.float_val = v.val;
      return &result_.float_val;
    }
    case TYPE_DOUBLE: {
      const DoubleVal& v = reinterpret_cast<const DoubleVal&>(val);
      result_.double_val = v.val;
      return &result_.double_val;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringVal& v = reinterpret_cast<const StringVal&>(val);
      result_.string_val.Assign(reinterpret_cast<char*>(v.ptr), v.len);
      return &result_.string_val;
    }
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      const StringVal& v = reinterpret_cast<const StringVal&>(val);
      result_.string_val.Assign(reinterpret_cast<char*>(v.ptr), v.len);
      return result_.string_val.Ptr();
    }
    case TYPE_TIMESTAMP: {
      const TimestampVal& v = reinterpret_cast<const TimestampVal&>(val);
      result_.timestamp_val = TimestampValue::FromTimestampVal(v);
      return &result_.timestamp_val;
    }
    case TYPE_DECIMAL: {
      const DecimalVal& v = reinterpret_cast<const DecimalVal&>(val);
      int byte_size = type.GetByteSize();
      switch (byte_size) {
        case 4:
          result_.decimal4_val = v.val4;
          return &result_.decimal4_val;
        case 8:
          result_.decimal8_val = v.val8;
          return &result_.decimal8_val;
        case 16:
          result_.decimal16_val = v.val16;
          return &result_.decimal16_val;
        default:
          DCHECK(false) << byte_size;
          return nullptr;
      }
      DCHECK(false);
      return nullptr;
    }
    case TYPE_DATE: {
      const DateVal& v = reinterpret_cast<const DateVal&>(val);
      const DateValue dv = DateValue::FromDateVal(v);
      if (UNLIKELY(!dv.IsValid())) return nullptr;
      result_.date_val = dv;
      return &result_.date_val;
    }
    case TYPE_ARRAY:
    case TYPE_MAP: {
      const CollectionVal& v = reinterpret_cast<const CollectionVal&>(val);
      result_.collection_val.ptr = v.ptr;
      result_.collection_val.num_tuples = v.num_tuples;
      return &result_.collection_val;
    }
    case TYPE_STRUCT: {
      const StructVal& v = reinterpret_cast<const StructVal&>(val);
      result_.struct_val = v;
      return &result_.struct_val;
    }
    default:
      DCHECK(false) << "Type not implemented: " << type;
      return nullptr;
  }
}


} // namespace impala
