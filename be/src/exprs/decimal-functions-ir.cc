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

#include "exprs/decimal-functions.h"

#include "codegen/impala-ir.h"
#include "exprs/anyval-util.h"
#include "runtime/multi-precision.h"

#include <cctype>
#include <cmath>

#include "common/names.h"

using std::abs;

namespace impala {

IntVal DecimalFunctions::Precision(FunctionContext* ctx, const DecimalVal& val) {
  return IntVal(ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0));
}

IntVal DecimalFunctions::Scale(FunctionContext* ctx, const DecimalVal& val) {
  return IntVal(ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0));
}

DecimalVal DecimalFunctions::Abs(FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return DecimalVal::null();
  int type_byte_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  switch (type_byte_size) {
    case 4:
      return DecimalVal(abs(val.val4));
    case 8:
      return DecimalVal(abs(val.val8));
    case 16:
      return DecimalVal(abs(val.val16));
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

DecimalVal DecimalFunctions::Ceil(FunctionContext* ctx, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(ctx, val, DecimalOperators::CEIL);
}

DecimalVal DecimalFunctions::Floor(FunctionContext* ctx, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(ctx, val, DecimalOperators::FLOOR);
}

DecimalVal DecimalFunctions::Round(FunctionContext* ctx, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(ctx, val, DecimalOperators::ROUND);
}

/// Always inline in IR module so that constants can be replaced.
IR_ALWAYS_INLINE DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* ctx, const DecimalVal& val, int scale,
    DecimalOperators::DecimalRoundOp op) {
  int val_precision =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  int val_scale =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0);
  int return_precision =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION);
  int return_scale =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE);
  if (scale < 0) {
    return DecimalOperators::RoundDecimalNegativeScale(ctx,
        val, val_precision, val_scale, return_precision, return_scale, op, -scale);
  } else {
    return DecimalOperators::RoundDecimal(ctx,
        val, val_precision, val_scale, return_precision, return_scale, op);
  }
}

DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* ctx, const DecimalVal& val, const TinyIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::ROUND);
}
DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* ctx, const DecimalVal& val, const SmallIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::ROUND);
}
DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* ctx, const DecimalVal& val, const IntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::ROUND);
}
DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* ctx, const DecimalVal& val, const BigIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::ROUND);
}

DecimalVal DecimalFunctions::Truncate(FunctionContext* ctx, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(ctx, val, DecimalOperators::TRUNCATE);
}

DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* ctx, const DecimalVal& val, const TinyIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::TRUNCATE);
}
DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* ctx, const DecimalVal& val, const SmallIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::TRUNCATE);
}
DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* ctx, const DecimalVal& val, const IntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::TRUNCATE);
}
DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* ctx, const DecimalVal& val, const BigIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(ctx, val, scale.val, DecimalOperators::TRUNCATE);
}

}
