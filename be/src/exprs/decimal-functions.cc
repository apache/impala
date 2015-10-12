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

#include "exprs/decimal-functions.h"

#include "exprs/anyval-util.h"
#include "exprs/expr.h"

#include <ctype.h>
#include <math.h>

#include "common/names.h"

namespace impala {

IntVal DecimalFunctions::Precision(FunctionContext* context, const DecimalVal& val) {
  return IntVal(context->GetArgType(0)->precision);
}

IntVal DecimalFunctions::Scale(FunctionContext* context, const DecimalVal& val) {
  return IntVal(context->GetArgType(0)->scale);
}

DecimalVal DecimalFunctions::Abs(FunctionContext* context, const DecimalVal& val) {
  if (val.is_null) return DecimalVal::null();
  int type_byte_size = Expr::GetConstant<int>(*context, Expr::ARG_TYPE_SIZE, 0);
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

DecimalVal DecimalFunctions::Ceil(FunctionContext* context, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(context, val, DecimalOperators::CEIL);
}

DecimalVal DecimalFunctions::Floor(FunctionContext* context, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(context, val, DecimalOperators::FLOOR);
}

DecimalVal DecimalFunctions::Round(FunctionContext* context, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(context, val, DecimalOperators::ROUND);
}

inline DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* context, const DecimalVal& val, int scale,
    DecimalOperators::DecimalRoundOp op) {
  const FunctionContext::TypeDesc& val_type = *context->GetArgType(0);
  const FunctionContext::TypeDesc& return_type = context->GetReturnType();
  if (scale < 0) {
    return DecimalOperators::RoundDecimalNegativeScale(
        context, val, val_type, return_type, op, -scale);
  } else {
    return DecimalOperators::RoundDecimal(
        context, val, val_type, return_type, op);
  }
}

DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* context, const DecimalVal& val, const TinyIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::ROUND);
}
DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* context, const DecimalVal& val, const SmallIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::ROUND);
}
DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* context, const DecimalVal& val, const IntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::ROUND);
}
DecimalVal DecimalFunctions::RoundTo(
    FunctionContext* context, const DecimalVal& val, const BigIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::ROUND);
}

DecimalVal DecimalFunctions::Truncate(FunctionContext* context, const DecimalVal& val) {
  return DecimalOperators::RoundDecimal(context, val, DecimalOperators::TRUNCATE);
}

DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* context, const DecimalVal& val, const TinyIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::TRUNCATE);
}
DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* context, const DecimalVal& val, const SmallIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::TRUNCATE);
}
DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* context, const DecimalVal& val, const IntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::TRUNCATE);
}
DecimalVal DecimalFunctions::TruncateTo(
    FunctionContext* context, const DecimalVal& val, const BigIntVal& scale) {
  DCHECK(!scale.is_null);
  return RoundTo(context, val, scale.val, DecimalOperators::TRUNCATE);
}

}
