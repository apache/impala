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

#include "exprs/conditional-functions.h"

using namespace impala;

#define NULL_IF_ZERO_COMPUTE_FUNCTION(type) \
  type ConditionalFunctions::NullIfZero(FunctionContext* ctx, const type& val) { \
    if (val.is_null || val.val == 0) return type::null(); \
    return val; \
  }

DecimalVal ConditionalFunctions::NullIfZero(FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return DecimalVal::null();
  int type_byte_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SIZE);
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
  type ConditionalFunctions::ZeroIfNull(FunctionContext* ctx, const type& val) { \
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

BooleanVal ConditionalFunctions::IsFalse(FunctionContext* ctx, const BooleanVal& val) {
  if (val.is_null) return BooleanVal(false);
  return BooleanVal(!val.val);
}

BooleanVal ConditionalFunctions::IsNotFalse(FunctionContext* ctx, const BooleanVal& val) {
  if (val.is_null) return BooleanVal(true);
  return BooleanVal(val.val);
}

BooleanVal ConditionalFunctions::IsTrue(FunctionContext* ctx, const BooleanVal& val) {
  if (val.is_null) return BooleanVal(false);
  return BooleanVal(val.val);
}

BooleanVal ConditionalFunctions::IsNotTrue(FunctionContext* ctx, const BooleanVal& val) {
  if (val.is_null) return BooleanVal(true);
  return BooleanVal(!val.val);
}
