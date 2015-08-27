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

#include "exprs/decimal-operators.h"

#include <iomanip>
#include <sstream>
#include <math.h>

#include "exprs/anyval-util.h"
#include "exprs/case-expr.h"
#include "exprs/expr.h"
#include "runtime/tuple-row.h"
#include "util/decimal-util.h"
#include "util/string-parser.h"

#include "common/names.h"

namespace impala {

#define RETURN_IF_OVERFLOW(context, overflow) \
  do {\
    if (UNLIKELY(overflow)) {\
      context->AddWarning("Expression overflowed, returning NULL");\
      return DecimalVal::null();\
    }\
  } while (false)

inline DecimalVal DecimalOperators::IntToDecimalVal(
    FunctionContext* context, const FunctionContext::TypeDesc& type, int64_t val) {
  DCHECK_EQ(type.type, FunctionContext::TYPE_DECIMAL);
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4: {
      Decimal4Value dv =
          Decimal4Value::FromInt(type.precision, type.scale, val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(dv.value());
    }
    case 8: {
      Decimal8Value dv =
          Decimal8Value::FromInt(type.precision, type.scale, val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(dv.value());
    }
    case 16: {
      Decimal16Value dv =
          Decimal16Value::FromInt(type.precision, type.scale, val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(dv.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

inline DecimalVal DecimalOperators::FloatToDecimalVal(
    FunctionContext* context, const FunctionContext::TypeDesc& type, double val) {
  DCHECK_EQ(type.type, FunctionContext::TYPE_DECIMAL);
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4: {
      Decimal4Value dv =
          Decimal4Value::FromDouble(type.precision, type.scale, val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(dv.value());
    }
    case 8: {
      Decimal8Value dv =
          Decimal8Value::FromDouble(type.precision, type.scale, val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(dv.value());
    }
    case 16: {
      Decimal16Value dv =
          Decimal16Value::FromDouble(type.precision, type.scale, val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(dv.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

// Converting from one decimal type to another requires two steps.
// - Converting between the decimal types (e.g. decimal8 -> decimal16)
// - Adjusting the scale.
// When going from a larger type to a smaller type, we need to adjust the scales first
// (since it can reduce the magnitude of the value) to minimize cases where we overflow.
// When going from a smaller type to a larger type, we convert and then scale.

inline DecimalVal DecimalOperators::ScaleDecimalValue(FunctionContext* context,
    const Decimal4Value& val, const FunctionContext::TypeDesc& val_type,
    const FunctionContext::TypeDesc& output_type) {
  DCHECK_EQ(val_type.type, FunctionContext::TYPE_DECIMAL);
  DCHECK_EQ(output_type.type, FunctionContext::TYPE_DECIMAL);
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(output_type.precision)) {
    case 4: {
      Decimal4Value scaled_val = val.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(scaled_val.value());
    }
    case 8: {
      Decimal8Value val8 = ToDecimal8(val, &overflow);
      Decimal8Value scaled_val = val8.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(scaled_val.value());
    }
    case 16: {
      Decimal16Value val16 = ToDecimal16(val, &overflow);
      Decimal16Value scaled_val = val16.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(scaled_val.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

inline DecimalVal DecimalOperators::ScaleDecimalValue(FunctionContext* context,
    const Decimal8Value& val, const FunctionContext::TypeDesc& val_type,
    const FunctionContext::TypeDesc& output_type) {
  DCHECK_EQ(val_type.type, FunctionContext::TYPE_DECIMAL);
  DCHECK_EQ(output_type.type, FunctionContext::TYPE_DECIMAL);
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(output_type.precision)) {
    case 4: {
      Decimal8Value scaled_val = val.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      Decimal4Value val4 = ToDecimal4(scaled_val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(val4.value());
    }
    case 8: {
      Decimal8Value scaled_val = val.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(scaled_val.value());
    }
    case 16: {
      Decimal16Value val16 = ToDecimal16(val, &overflow);
      Decimal16Value scaled_val = val16.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(scaled_val.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

inline DecimalVal DecimalOperators::ScaleDecimalValue(FunctionContext* context,
    const Decimal16Value& val, const FunctionContext::TypeDesc& val_type,
    const FunctionContext::TypeDesc& output_type) {
  DCHECK_EQ(val_type.type, FunctionContext::TYPE_DECIMAL);
  DCHECK_EQ(output_type.type, FunctionContext::TYPE_DECIMAL);
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(output_type.precision)) {
    case 4: {
      Decimal16Value scaled_val = val.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      Decimal4Value val4 = ToDecimal4(scaled_val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(val4.value());
    }
    case 8: {
      Decimal16Value scaled_val = val.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      Decimal8Value val8 = ToDecimal8(scaled_val, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(val8.value());
    }
    case 16: {
      Decimal16Value scaled_val = val.ScaleTo(
          val_type.scale, output_type.scale, output_type.precision, &overflow);
      RETURN_IF_OVERFLOW(context, overflow);
      return DecimalVal(scaled_val.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

static inline Decimal4Value GetDecimal4Value(
    const DecimalVal& val, const FunctionContext::TypeDesc& type, bool* overflow) {
  DCHECK_EQ(type.type, FunctionContext::TYPE_DECIMAL);
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4: return ToDecimal4(Decimal4Value(val.val4), overflow);
    case 8: return ToDecimal4(Decimal8Value(val.val8), overflow);
    case 16: return ToDecimal4(Decimal16Value(val.val16), overflow);
    default:
      DCHECK(false);
      return Decimal4Value();
  }
}

static inline Decimal8Value GetDecimal8Value(
    const DecimalVal& val, const FunctionContext::TypeDesc& type, bool* overflow) {
  DCHECK_EQ(type.type, FunctionContext::TYPE_DECIMAL);
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4: return ToDecimal8(Decimal4Value(val.val4), overflow);
    case 8: return ToDecimal8(Decimal8Value(val.val8), overflow);
    case 16: return ToDecimal8(Decimal16Value(val.val16), overflow);
    default:
      DCHECK(false);
      return Decimal8Value();
  }
}

static inline Decimal16Value GetDecimal16Value(
    const DecimalVal& val, const FunctionContext::TypeDesc& type, bool* overflow) {
  DCHECK_EQ(type.type, FunctionContext::TYPE_DECIMAL);
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4: return ToDecimal16(Decimal4Value(val.val4), overflow);
    case 8: return ToDecimal16(Decimal8Value(val.val8), overflow);
    case 16: return ToDecimal16(Decimal16Value(val.val16), overflow);
    default:
      DCHECK(false);
      return Decimal16Value();
  }
}

#define CAST_INT_TO_DECIMAL(from_type) \
  DecimalVal DecimalOperators::CastToDecimalVal( \
      FunctionContext* context, const from_type& val) { \
    if (val.is_null) return DecimalVal::null(); \
    const FunctionContext::TypeDesc& type = context->GetReturnType(); \
    return IntToDecimalVal(context, type, val.val); \
  }

#define CAST_FLOAT_TO_DECIMAL(from_type) \
  DecimalVal DecimalOperators::CastToDecimalVal( \
      FunctionContext* context, const from_type& val) { \
    if (val.is_null) return DecimalVal::null(); \
    const FunctionContext::TypeDesc& type = context->GetReturnType(); \
    return FloatToDecimalVal(context, type, val.val); \
  }

#define CAST_DECIMAL_TO_INT(to_type) \
  to_type DecimalOperators::CastTo##to_type( \
      FunctionContext* context, const DecimalVal& val) { \
    if (val.is_null) return to_type::null(); \
    const FunctionContext::TypeDesc& type = *context->GetArgType(0); \
    switch (ColumnType::GetDecimalByteSize(type.precision)) { \
      case 4: { \
        Decimal4Value dv(val.val4); \
        return to_type(dv.whole_part(type.scale)); \
      } \
      case 8: { \
        Decimal8Value dv(val.val8); \
        return to_type(dv.whole_part(type.scale)); \
      } \
      case 16: { \
        Decimal16Value dv(val.val16); \
        return to_type(dv.whole_part(type.scale)); \
      } \
      default:\
        DCHECK(false); \
        return to_type::null(); \
    } \
  }

#define CAST_DECIMAL_TO_FLOAT(to_type) \
  to_type DecimalOperators::CastTo##to_type( \
      FunctionContext* context, const DecimalVal& val) { \
    if (val.is_null) return to_type::null(); \
    const FunctionContext::TypeDesc& type = *context->GetArgType(0); \
    switch (ColumnType::GetDecimalByteSize(type.precision)) { \
      case 4: { \
        Decimal4Value dv(val.val4); \
        return to_type(dv.ToDouble(type.scale)); \
      } \
      case 8: { \
        Decimal8Value dv(val.val8); \
        return to_type(dv.ToDouble(type.scale)); \
      } \
      case 16: { \
        Decimal16Value dv(val.val16); \
        return to_type(dv.ToDouble(type.scale)); \
      } \
      default:\
        DCHECK(false); \
        return to_type::null(); \
    } \
  }

CAST_INT_TO_DECIMAL(TinyIntVal)
CAST_INT_TO_DECIMAL(SmallIntVal)
CAST_INT_TO_DECIMAL(IntVal)
CAST_INT_TO_DECIMAL(BigIntVal)
CAST_FLOAT_TO_DECIMAL(FloatVal)
CAST_FLOAT_TO_DECIMAL(DoubleVal)

CAST_DECIMAL_TO_INT(TinyIntVal)
CAST_DECIMAL_TO_INT(SmallIntVal)
CAST_DECIMAL_TO_INT(IntVal)
CAST_DECIMAL_TO_INT(BigIntVal)
CAST_DECIMAL_TO_FLOAT(FloatVal)
CAST_DECIMAL_TO_FLOAT(DoubleVal)

DecimalVal DecimalOperators::RoundDecimalNegativeScale(FunctionContext* context,
    const DecimalVal& val, const FunctionContext::TypeDesc& val_type,
    const FunctionContext::TypeDesc& output_type, const DecimalRoundOp& op,
    int64_t rounding_scale) {
  DCHECK_GT(rounding_scale, 0);
  if (val.is_null) return DecimalVal::null();

  // 'result' holds the value prior to rounding.
  DecimalVal result;
  switch (ColumnType::GetDecimalByteSize(val_type.precision)) {
    case 4: {
      Decimal4Value val4(val.val4);
      result = ScaleDecimalValue(context, val4, val_type, output_type);
      break;
    }
    case 8: {
      Decimal8Value val8(val.val8);
      result = ScaleDecimalValue(context, val8, val_type, output_type);
      break;
    }
    case 16: {
      Decimal16Value val16(val.val16);
      result = ScaleDecimalValue(context, val16, val_type, output_type);
      break;
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }

  // This can return NULL if the value overflowed.
  if (result.is_null) return result;

  // We've done the cast portion of the computation. Now round it.
  switch (ColumnType::GetDecimalByteSize(output_type.precision)) {
    case 4: {
      Decimal4Value val4(result.val4);
      int32_t d = RoundDelta(val4, 0, -rounding_scale, op);
      int32_t base = DecimalUtil::GetScaleMultiplier<int32_t>(rounding_scale);
      result.val4 -= result.val4 % base;
      result.val4 += d * base;
      break;
    }
    case 8: {
      Decimal8Value val8(result.val8);
      int64_t d = RoundDelta(val8, 0, -rounding_scale, op);
      int64_t base = DecimalUtil::GetScaleMultiplier<int64_t>(rounding_scale);
      result.val8 -= result.val8 % base;
      result.val8 += d * base;
      break;
    }
    case 16: {
      Decimal16Value val16(result.val16);
      int128_t d = RoundDelta(val16, 0, -rounding_scale, op);
      int128_t base = DecimalUtil::GetScaleMultiplier<int128_t>(rounding_scale);
      int128_t delta = d * base - (val16.value() % base);
      // Need to check for overflow. This can't happen in the other cases since the
      // FE should have picked a high enough precision.
      if (DecimalUtil::MAX_UNSCALED_DECIMAL - abs(delta) < abs(val16.value())) {
        context->AddWarning("Expression overflowed, returning NULL");
        return DecimalVal::null();
      }
      result.val16 += delta;
      break;
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
  return result;
}

DecimalVal DecimalOperators::RoundDecimal(FunctionContext* context, const DecimalVal& val,
    const FunctionContext::TypeDesc& val_type,
    const FunctionContext::TypeDesc& output_type, const DecimalRoundOp& op) {
  DCHECK(output_type.type == FunctionContext::TYPE_DECIMAL) << output_type.type;
  if (val.is_null) return DecimalVal::null();
  // Switch on the child type.
  DecimalVal result = DecimalVal::null();
  int delta = 0;
  switch (ColumnType::GetDecimalByteSize(val_type.precision)) {
    case 4: {
      Decimal4Value val4(val.val4);
      result = ScaleDecimalValue(context, val4, val_type, output_type);
      delta = RoundDelta(val4, val_type.scale, output_type.scale, op);
      break;
    }
    case 8: {
      Decimal8Value val8(val.val8);
      result = ScaleDecimalValue(context, val8, val_type, output_type);
      delta = RoundDelta(val8, val_type.scale, output_type.scale, op);
      break;
    }
    case 16: {
      Decimal16Value val16(val.val16);
      result = ScaleDecimalValue(context, val16, val_type, output_type);
      delta = RoundDelta(val16, val_type.scale, output_type.scale, op);
      break;
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }

  // This can return NULL if the value overflowed.
  if (result.is_null) return result;

  // At this point result is the first part of the round operation. It has just
  // done the cast.
  if (delta == 0) return result;


  // The value in 'result' is before the rounding has occurred.
  // This can't overflow. Rounding to a non-negative scale means at least one digit is
  // dropped if rounding occurred and the round can add at most one digit before the
  // decimal.
  result.val16 += delta;
  return result;
}

DecimalVal DecimalOperators::RoundDecimal(
    FunctionContext* context, const DecimalVal& val, const DecimalRoundOp& op) {
  const FunctionContext::TypeDesc& val_type = *context->GetArgType(0);
  const FunctionContext::TypeDesc& return_type = context->GetReturnType();
  return RoundDecimal(context, val, val_type, return_type, op);
}

// Cast is just RoundDecimal(TRUNCATE).
// TODO: how we handle cast to a smaller scale is an implementation detail in the spec.
// We could also choose to cast by doing ROUND.
DecimalVal DecimalOperators::CastToDecimalVal(
    FunctionContext* context, const DecimalVal& val) {
  return RoundDecimal(context, val, TRUNCATE);
}

DecimalVal DecimalOperators::CastToDecimalVal(
    FunctionContext* context, const StringVal& val) {
  if (val.is_null) return DecimalVal::null();
  StringParser::ParseResult result;
  DecimalVal dv;
  const FunctionContext::TypeDesc& type = context->GetReturnType();
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4: {
      Decimal4Value dv4 = StringParser::StringToDecimal<int32_t>(
          reinterpret_cast<char*>(val.ptr), val.len, type.precision, type.scale, &result);
      dv = DecimalVal(dv4.value());
      break;
    }
    case 8: {
      Decimal8Value dv8 = StringParser::StringToDecimal<int64_t>(
          reinterpret_cast<char*>(val.ptr), val.len, type.precision, type.scale, &result);
      dv = DecimalVal(dv8.value());
      break;
    }
    case 16: {
      Decimal16Value dv16 = StringParser::StringToDecimal<int128_t>(
          reinterpret_cast<char*>(val.ptr), val.len, type.precision, type.scale, &result);
      dv = DecimalVal(dv16.value());
      break;
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
  // Like all the cast functions, we return the truncated value on underflow and NULL
  // on overflow.
  // TODO: log warning on underflow.
  if (result == StringParser::PARSE_SUCCESS || result == StringParser::PARSE_UNDERFLOW) {
    return dv;
  }
  return DecimalVal::null();
}

StringVal DecimalOperators::CastToStringVal(
    FunctionContext* context, const DecimalVal& val) {
  if (val.is_null) return StringVal::null();
  const FunctionContext::TypeDesc& type = *context->GetArgType(0);
  string s;
  switch (ColumnType::GetDecimalByteSize(type.precision)) {
    case 4:
      s = Decimal4Value(val.val4).ToString(type.precision, type.scale);
      break;
    case 8:
      s = Decimal8Value(val.val8).ToString(type.precision, type.scale);
      break;
    case 16:
      s = Decimal16Value(val.val16).ToString(type.precision, type.scale);
      break;
    default:
      DCHECK(false);
      return StringVal::null();
  }
  StringVal result(context, s.size());
  memcpy(result.ptr, s.c_str(), s.size());
  return result;
}

TimestampVal DecimalOperators::CastToTimestampVal(
    FunctionContext* context, const DecimalVal& val) {
  if (val.is_null) return TimestampVal::null();
  const FunctionContext::TypeDesc& val_type = *context->GetArgType(0);
  DCHECK_EQ(val_type.type, FunctionContext::TYPE_DECIMAL);
  TimestampVal result;
  switch (ColumnType::GetDecimalByteSize(val_type.precision)) {
    case 4: {
      Decimal4Value dv(val.val4);
      TimestampValue tv(dv.ToDouble(val_type.scale));
      tv.ToTimestampVal(&result);
      break;
    }
    case 8: {
      Decimal8Value dv(val.val8);
      TimestampValue tv(dv.ToDouble(val_type.scale));
      tv.ToTimestampVal(&result);
      break;
    }
    case 16: {
      Decimal16Value dv(val.val16);
      TimestampValue tv(dv.ToDouble(val_type.scale));
      tv.ToTimestampVal(&result);
      break;
    }
    default:
      DCHECK(false);
      return TimestampVal::null();
  }
  return result;
}

BooleanVal DecimalOperators::CastToBooleanVal(
    FunctionContext* context, const DecimalVal& val) {
  if (val.is_null) return BooleanVal::null();
  const FunctionContext::TypeDesc& val_type = *context->GetArgType(0);
  DCHECK_EQ(val_type.type, FunctionContext::TYPE_DECIMAL);
  switch (ColumnType::GetDecimalByteSize(val_type.precision)) {
    case 4:
      return BooleanVal(val.val4 != 0);
    case 8:
      return BooleanVal(val.val8 != 0);
    case 16:
      return BooleanVal(val.val16 != 0);
    default:
      DCHECK(false);
      return BooleanVal::null();
  }
}

#define DECIMAL_ARITHMETIC_OP(FN_NAME, OP_FN) \
  DecimalVal DecimalOperators::FN_NAME( \
      FunctionContext* context, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null || y.is_null) return DecimalVal::null(); \
    bool overflow = false; \
    const FunctionContext::TypeDesc& x_type = *context->GetArgType(0); \
    const FunctionContext::TypeDesc& y_type = *context->GetArgType(1); \
    const FunctionContext::TypeDesc& return_type = context->GetReturnType(); \
    switch (ColumnType::GetDecimalByteSize(return_type.precision)) { \
      case 4: { \
        Decimal4Value x_val = GetDecimal4Value(x, x_type, &overflow); \
        Decimal4Value y_val = GetDecimal4Value(y, y_type, &overflow); \
        Decimal4Value result = x_val.OP_FN<int32_t>( \
            x_type.scale, y_val, y_type.scale, return_type.scale, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        return DecimalVal(result.value()); \
      } \
      case 8: { \
        Decimal8Value x_val = GetDecimal8Value(x, x_type, &overflow); \
        Decimal8Value y_val = GetDecimal8Value(y, y_type, &overflow); \
        Decimal8Value result = x_val.OP_FN<int64_t>( \
            x_type.scale, y_val, y_type.scale, return_type.scale, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        return DecimalVal(result.value()); \
      } \
      case 16: { \
        Decimal16Value x_val = GetDecimal16Value(x, x_type, &overflow); \
        Decimal16Value y_val = GetDecimal16Value(y, y_type, &overflow); \
        Decimal16Value result = x_val.OP_FN<int128_t>( \
            x_type.scale, y_val, y_type.scale, return_type.scale, &overflow); \
        RETURN_IF_OVERFLOW(context, overflow); \
        return DecimalVal(result.value()); \
      } \
      default: \
        break; \
    } \
    return DecimalVal::null(); \
  }

#define DECIMAL_ARITHMETIC_OP_CHECK_NAN(FN_NAME, OP_FN) \
  DecimalVal DecimalOperators::FN_NAME( \
      FunctionContext* context, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null || y.is_null) return DecimalVal::null(); \
    bool overflow = false; \
    bool is_nan = false; \
    const FunctionContext::TypeDesc& x_type = *context->GetArgType(0); \
    const FunctionContext::TypeDesc& y_type = *context->GetArgType(1); \
    const FunctionContext::TypeDesc& return_type = context->GetReturnType(); \
    switch (ColumnType::GetDecimalByteSize(return_type.precision)) { \
      case 4: { \
        Decimal4Value x_val = GetDecimal4Value(x, x_type, &overflow); \
        Decimal4Value y_val = GetDecimal4Value(y, y_type, &overflow); \
        Decimal4Value result = x_val.OP_FN<int32_t>( \
            x_type.scale, y_val, y_type.scale, return_type.scale, &is_nan, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        if (is_nan) return DecimalVal::null(); \
        return DecimalVal(result.value()); \
      } \
      case 8: { \
        Decimal8Value x_val = GetDecimal8Value(x, x_type, &overflow); \
        Decimal8Value y_val = GetDecimal8Value(y, y_type, &overflow); \
        Decimal8Value result = x_val.OP_FN<int64_t>( \
            x_type.scale, y_val, y_type.scale, return_type.scale, &is_nan, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        if (is_nan) return DecimalVal::null(); \
        return DecimalVal(result.value()); \
      } \
      case 16: { \
        Decimal16Value x_val = GetDecimal16Value(x, x_type, &overflow); \
        Decimal16Value y_val = GetDecimal16Value(y, y_type, &overflow); \
        Decimal16Value result = x_val.OP_FN<int128_t>( \
            x_type.scale, y_val, y_type.scale, return_type.scale, &is_nan, &overflow); \
        RETURN_IF_OVERFLOW(context, overflow); \
        if (is_nan) return DecimalVal::null(); \
        return DecimalVal(result.value()); \
      } \
      default: \
        break; \
    } \
    return DecimalVal::null(); \
  }

#define DECIMAL_BINARY_OP_NONNULL(OP_FN, X, Y) \
  bool dummy = false; \
  const FunctionContext::TypeDesc& x_type = *context->GetArgType(0); \
  const FunctionContext::TypeDesc& y_type = *context->GetArgType(1); \
  int byte_size = ::max(ColumnType::GetDecimalByteSize(x_type.precision), \
                        ColumnType::GetDecimalByteSize(y_type.precision)); \
  switch (byte_size) { \
    case 4: { \
      Decimal4Value x_val = GetDecimal4Value(X, x_type, &dummy); \
      Decimal4Value y_val = GetDecimal4Value(Y, y_type, &dummy); \
      bool result = x_val.OP_FN(x_type.scale, y_val, y_type.scale); \
      return BooleanVal(result); \
    } \
    case 8: { \
      Decimal8Value x_val = GetDecimal8Value(X, x_type, &dummy); \
      Decimal8Value y_val = GetDecimal8Value(Y, y_type, &dummy); \
      bool result = x_val.OP_FN(x_type.scale, y_val, y_type.scale); \
      return BooleanVal(result); \
    } \
    case 16: { \
      Decimal16Value x_val = GetDecimal16Value(X, x_type, &dummy); \
      Decimal16Value y_val = GetDecimal16Value(Y, y_type, &dummy); \
      bool result = x_val.OP_FN(x_type.scale, y_val, y_type.scale); \
      return BooleanVal(result); \
    } \
    default: \
      DCHECK(false); \
      break; \
  } \
  return BooleanVal::null();

#define DECIMAL_BINARY_OP(FN_NAME, OP_FN) \
  BooleanVal DecimalOperators::FN_NAME( \
      FunctionContext* context, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null || y.is_null) return BooleanVal::null(); \
    DECIMAL_BINARY_OP_NONNULL(OP_FN, x, y) \
  }

#define NULLSAFE_DECIMAL_BINARY_OP(FN_NAME, OP_FN, IS_EQUAL) \
  BooleanVal DecimalOperators::FN_NAME( \
      FunctionContext* context, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null) return BooleanVal(IS_EQUAL ? y.is_null : !y.is_null); \
    if (y.is_null) return BooleanVal(!IS_EQUAL); \
    DECIMAL_BINARY_OP_NONNULL(OP_FN, x, y) \
  }


DECIMAL_ARITHMETIC_OP(Add_DecimalVal_DecimalVal, Add)
DECIMAL_ARITHMETIC_OP(Subtract_DecimalVal_DecimalVal, Subtract)
DECIMAL_ARITHMETIC_OP(Multiply_DecimalVal_DecimalVal, Multiply)
DECIMAL_ARITHMETIC_OP_CHECK_NAN(Divide_DecimalVal_DecimalVal, Divide)
DECIMAL_ARITHMETIC_OP_CHECK_NAN(Mod_DecimalVal_DecimalVal, Mod)

DECIMAL_BINARY_OP(Eq_DecimalVal_DecimalVal, Eq)
DECIMAL_BINARY_OP(Ne_DecimalVal_DecimalVal, Ne)
DECIMAL_BINARY_OP(Ge_DecimalVal_DecimalVal, Ge)
DECIMAL_BINARY_OP(Gt_DecimalVal_DecimalVal, Gt)
DECIMAL_BINARY_OP(Le_DecimalVal_DecimalVal, Le)
DECIMAL_BINARY_OP(Lt_DecimalVal_DecimalVal, Lt)
NULLSAFE_DECIMAL_BINARY_OP(DistinctFrom_DecimalVal_DecimalVal, Ne, false)
NULLSAFE_DECIMAL_BINARY_OP(NotDistinct_DecimalVal_DecimalVal, Eq, true)
}
