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

#include "exprs/decimal-operators.h"

#include <iomanip>
#include <sstream>
#include <math.h>

#include "codegen/impala-ir.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr.h"
#include "runtime/decimal-value.inline.h"
#include "util/decimal-util.h"
#include "util/string-parser.h"

#include "common/names.h"

namespace impala {

#define RETURN_IF_OVERFLOW(ctx, overflow, return_type) \
  do { \
    if (UNLIKELY(overflow)) { \
      if (ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2)) { \
        ctx->SetError("Decimal expression overflowed"); \
      } else { \
        ctx->AddWarning("Decimal expression overflowed, returning NULL"); \
      } \
      return return_type::null(); \
    } \
  } while (false)

// Inline in IR module so branches can be optimised out.
IR_ALWAYS_INLINE DecimalVal DecimalOperators::IntToDecimalVal(
    FunctionContext* ctx, int precision, int scale, int64_t val) {
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(precision)) {
    case 4: {
      Decimal4Value dv = Decimal4Value::FromInt(precision, scale, val, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(dv.value());
    }
    case 8: {
      Decimal8Value dv = Decimal8Value::FromInt(precision, scale, val, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(dv.value());
    }
    case 16: {
      Decimal16Value dv = Decimal16Value::FromInt(precision, scale, val, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(dv.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

// Inline in IR module so branches can be optimised out.
IR_ALWAYS_INLINE DecimalVal DecimalOperators::FloatToDecimalVal(
    FunctionContext* ctx, int precision, int scale, double val) {
  bool overflow = false;
  const bool round = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  switch (ColumnType::GetDecimalByteSize(precision)) {
    case 4: {
      Decimal4Value dv =
          Decimal4Value::FromDouble(precision, scale, val, round, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(dv.value());
    }
    case 8: {
      Decimal8Value dv =
          Decimal8Value::FromDouble(precision, scale, val, round, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(dv.value());
    }
    case 16: {
      Decimal16Value dv =
          Decimal16Value::FromDouble(precision, scale, val, round, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
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
// Inline these functions in IR module so branches can be optimised out.

IR_ALWAYS_INLINE DecimalVal DecimalOperators::ScaleDecimalValue(FunctionContext* ctx,
    const Decimal4Value& val, int val_scale, int output_precision, int output_scale) {
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(output_precision)) {
    case 4: {
      Decimal4Value scaled_val = val.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(scaled_val.value());
    }
    case 8: {
      Decimal8Value val8 = ToDecimal8(val, &overflow);
      Decimal8Value scaled_val = val8.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(scaled_val.value());
    }
    case 16: {
      Decimal16Value val16 = ToDecimal16(val, &overflow);
      Decimal16Value scaled_val = val16.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(scaled_val.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

IR_ALWAYS_INLINE DecimalVal DecimalOperators::ScaleDecimalValue(FunctionContext* ctx,
    const Decimal8Value& val, int val_scale, int output_precision, int output_scale) {
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(output_precision)) {
    case 4: {
      Decimal8Value scaled_val = val.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      Decimal4Value val4 = ToDecimal4(scaled_val, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(val4.value());
    }
    case 8: {
      Decimal8Value scaled_val = val.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(scaled_val.value());
    }
    case 16: {
      Decimal16Value val16 = ToDecimal16(val, &overflow);
      Decimal16Value scaled_val = val16.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(scaled_val.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

IR_ALWAYS_INLINE DecimalVal DecimalOperators::ScaleDecimalValue(FunctionContext* ctx,
    const Decimal16Value& val, int val_scale, int output_precision, int output_scale) {
  bool overflow = false;
  switch (ColumnType::GetDecimalByteSize(output_precision)) {
    case 4: {
      Decimal16Value scaled_val = val.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      Decimal4Value val4 = ToDecimal4(scaled_val, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(val4.value());
    }
    case 8: {
      Decimal16Value scaled_val = val.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      Decimal8Value val8 = ToDecimal8(scaled_val, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(val8.value());
    }
    case 16: {
      Decimal16Value scaled_val = val.ScaleTo(
          val_scale, output_scale, output_precision, &overflow);
      RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
      return DecimalVal(scaled_val.value());
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

// Inline in IR module so branches can be optimised out.
template <typename T>
IR_ALWAYS_INLINE T DecimalOperators::RoundDelta(const DecimalValue<T>& v, int src_scale,
    int target_scale, const DecimalRoundOp& op) {
  if (op == TRUNCATE) return 0;

  // Adding more digits, rounding does not apply. New digits are just 0.
  if (src_scale <= target_scale) return 0;

  // No need to round for floor() and the value is positive or ceil() and the value
  // is negative.
  if (v.value() > 0 && op == FLOOR) return 0;
  if (v.value() < 0 && op == CEIL) return 0;

  // We are removing the decimal places. Extract the value of the digits we are
  // dropping. For example, going from scale 5->2, means we want the last 3 digits.
  int delta_scale = src_scale - target_scale;
  DCHECK_GT(delta_scale, 0);

  // 10^delta_scale
  T trailing_base = DecimalUtil::GetScaleMultiplier<T>(delta_scale);
  T trailing_digits = v.value() % trailing_base;

  // If the trailing digits are zero, never round.
  if (trailing_digits == 0) return 0;

  // Trailing digits are non-zero.
  if (op == CEIL) return 1;
  if (op == FLOOR) return -1;

  DCHECK_EQ(op, ROUND);
  // TODO: > or greater than or equal. i.e. should .500 round up?
  if (abs(trailing_digits) < trailing_base / 2) return 0;
  return v.value() < 0 ? -1 : 1;
}

static inline Decimal4Value GetDecimal4Value(
    const DecimalVal& val, int val_byte_size, bool* overflow) {
  switch (val_byte_size) {
    case 4: return ToDecimal4(Decimal4Value(val.val4), overflow);
    case 8: return ToDecimal4(Decimal8Value(val.val8), overflow);
    case 16: return ToDecimal4(Decimal16Value(val.val16), overflow);
    default:
      DCHECK(false);
      return Decimal4Value();
  }
}

static inline Decimal8Value GetDecimal8Value(
    const DecimalVal& val, int val_byte_size, bool* overflow) {
  switch (val_byte_size) {
    case 4: return ToDecimal8(Decimal4Value(val.val4), overflow);
    case 8: return ToDecimal8(Decimal8Value(val.val8), overflow);
    case 16: return ToDecimal8(Decimal16Value(val.val16), overflow);
    default:
      DCHECK(false);
      return Decimal8Value();
  }
}

static inline Decimal16Value GetDecimal16Value(
    const DecimalVal& val, int val_byte_size, bool* overflow) {
  switch (val_byte_size) {
    case 4: return ToDecimal16(Decimal4Value(val.val4), overflow);
    case 8: return ToDecimal16(Decimal8Value(val.val8), overflow);
    case 16: return ToDecimal16(Decimal16Value(val.val16), overflow);
    default:
      DCHECK(false);
      return Decimal16Value();
  }
}

#define CAST_INT_TO_DECIMAL(from_type) \
  IR_ALWAYS_INLINE DecimalVal DecimalOperators::CastToDecimalVal( \
      FunctionContext* ctx, const from_type& val) { \
    if (val.is_null) return DecimalVal::null(); \
    int precision = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION); \
    int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE); \
    return IntToDecimalVal(ctx, precision, scale, val.val); \
  }

#define CAST_FLOAT_TO_DECIMAL(from_type) \
  IR_ALWAYS_INLINE DecimalVal DecimalOperators::CastToDecimalVal( \
      FunctionContext* ctx, const from_type& val) { \
    if (val.is_null) return DecimalVal::null(); \
    int precision = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION); \
    int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE); \
    return FloatToDecimalVal(ctx, precision, scale, val.val); \
  }

#define CAST_DECIMAL_TO_INT(to_type) \
  IR_ALWAYS_INLINE to_type DecimalOperators::CastTo##to_type( \
      FunctionContext* ctx, const DecimalVal& val) { \
    if (val.is_null) return to_type::null(); \
    int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0); \
    bool overflow = false; \
    /* TODO: IMPALA-4929: remove DECIMAL V1 code */ \
    const bool round = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2); \
    switch (ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0)) { \
      case 4: { \
        Decimal4Value dv(val.val4); \
        if (round) { \
          auto val = dv.ToInt<to_type>(scale, &overflow); \
          RETURN_IF_OVERFLOW(ctx, overflow, to_type); \
          return to_type(val); \
        } else { \
          return to_type(dv.whole_part(scale)); \
        } \
      } \
      case 8: { \
        Decimal8Value dv(val.val8); \
        if (round) { \
          auto val = dv.ToInt<to_type>(scale, &overflow); \
          RETURN_IF_OVERFLOW(ctx, overflow, to_type); \
          return to_type(val); \
        } else { \
          return to_type(dv.whole_part(scale)); \
        } \
      } \
      case 16: { \
        Decimal16Value dv(val.val16); \
        if (round) { \
          auto val = dv.ToInt<to_type>(scale, &overflow); \
          RETURN_IF_OVERFLOW(ctx, overflow, to_type); \
          return to_type(val); \
        } else { \
          return to_type(dv.whole_part(scale)); \
        } \
      } \
      default:\
        DCHECK(false); \
        return to_type::null(); \
    } \
  }

#define CAST_DECIMAL_TO_FLOAT(to_type) \
  IR_ALWAYS_INLINE to_type DecimalOperators::CastTo##to_type( \
      FunctionContext* ctx, const DecimalVal& val) { \
    if (val.is_null) return to_type::null(); \
    int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0); \
    switch (ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0)) { \
      case 4: { \
        Decimal4Value dv(val.val4); \
        return to_type(dv.ToDouble(scale)); \
      } \
      case 8: { \
        Decimal8Value dv(val.val8); \
        return to_type(dv.ToDouble(scale)); \
      } \
      case 16: { \
        Decimal16Value dv(val.val16); \
        return to_type(dv.ToDouble(scale)); \
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

// Inline in IR module so branches can be optimised out.
IR_ALWAYS_INLINE DecimalVal DecimalOperators::RoundDecimalNegativeScale(
    FunctionContext* ctx, const DecimalVal& val, int val_precision, int val_scale,
    int output_precision, int output_scale, const DecimalRoundOp& op,
    int64_t rounding_scale) {
  DCHECK_GT(rounding_scale, 0);
  if (val.is_null) return DecimalVal::null();

  // 'result' holds the value prior to rounding.
  DecimalVal result;
  switch (ColumnType::GetDecimalByteSize(val_precision)) {
    case 4: {
      Decimal4Value val4(val.val4);
      result = ScaleDecimalValue(ctx, val4, val_scale, output_precision,
          output_scale);
      break;
    }
    case 8: {
      Decimal8Value val8(val.val8);
      result = ScaleDecimalValue(ctx, val8, val_scale, output_precision,
          output_scale);
      break;
    }
    case 16: {
      Decimal16Value val16(val.val16);
      result = ScaleDecimalValue(ctx, val16, val_scale, output_precision,
          output_scale);
      break;
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }

  // This can return NULL if the value overflowed.
  if (result.is_null) return result;

  // We've done the cast portion of the computation. Now round it.
  switch (ColumnType::GetDecimalByteSize(output_precision)) {
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
      if (DecimalUtil::MAX_UNSCALED_DECIMAL16 - abs(delta) < abs(val16.value())) {
        ctx->AddWarning("Expression overflowed, returning NULL");
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

// Inline in IR module so branches can be optimised out.
IR_ALWAYS_INLINE DecimalVal DecimalOperators::RoundDecimal(FunctionContext* ctx,
    const DecimalVal& val, int val_precision, int val_scale, int output_precision,
    int output_scale, const DecimalRoundOp& op) {
  if (val.is_null) return DecimalVal::null();
  // Switch on the child type.
  DecimalVal result = DecimalVal::null();
  int delta = 0;
  switch (ColumnType::GetDecimalByteSize(val_precision)) {
    case 4: {
      Decimal4Value val4(val.val4);
      result = ScaleDecimalValue(ctx, val4, val_scale, output_precision,
          output_scale);
      delta = RoundDelta(val4, val_scale, output_scale, op);
      break;
    }
    case 8: {
      Decimal8Value val8(val.val8);
      result = ScaleDecimalValue(ctx, val8, val_scale, output_precision,
          output_scale);
      delta = RoundDelta(val8, val_scale, output_scale, op);
      break;
    }
    case 16: {
      Decimal16Value val16(val.val16);
      result = ScaleDecimalValue(ctx, val16, val_scale, output_precision,
          output_scale);
      delta = RoundDelta(val16, val_scale, output_scale, op);
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

  // The value in 'result' is before any rounding has occurred. If there is any rounding,
  // the ouput's scale must be less than the input's scale.
  DCHECK_GT(val_scale, output_scale);
  result.val16 += delta;

  // Rounding to a non-negative scale means at least one digit is dropped if rounding
  // occurred and the round can add at most one digit before the decimal. This cannot
  // overflow if output_precision >= val_precision. Otherwise, result can overflow.
  bool overflow = output_precision < val_precision &&
      abs(result.val16) >= DecimalUtil::GetScaleMultiplier<int128_t>(output_precision);
  RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal);
  return result;
}

// Inline in IR module so branches can be optimised out.
IR_ALWAYS_INLINE DecimalVal DecimalOperators::RoundDecimal(
    FunctionContext* ctx, const DecimalVal& val, const DecimalRoundOp& op) {
  int val_precision =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  int val_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0);
  int return_precision =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION);
  int return_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE);
  return RoundDecimal(ctx, val, val_precision, val_scale, return_precision,
      return_scale, op);
}

// If query option decimal_v2 is true, cast is RoundDecimal(ROUND).
// Otherwise, it's RoundDecimal(TRUNCATE).
IR_ALWAYS_INLINE DecimalVal DecimalOperators::CastToDecimalVal(
    FunctionContext* ctx, const DecimalVal& val) {
  int is_decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  DCHECK(is_decimal_v2 == 0 || is_decimal_v2 == 1);
  return RoundDecimal(ctx, val, is_decimal_v2 != 0 ? ROUND : TRUNCATE);
}

IR_ALWAYS_INLINE DecimalVal DecimalOperators::CastToDecimalVal(
    FunctionContext* ctx, const StringVal& val) {
  if (val.is_null) return DecimalVal::null();
  StringParser::ParseResult result;
  DecimalVal dv;
  int precision = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION);
  int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE);
  bool is_decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  switch (ColumnType::GetDecimalByteSize(precision)) {
    case 4: {
      Decimal4Value dv4 = StringParser::StringToDecimal<int32_t>(
          reinterpret_cast<char*>(val.ptr), val.len, precision, scale,
          is_decimal_v2, &result);
      dv = DecimalVal(dv4.value());
      break;
    }
    case 8: {
      Decimal8Value dv8 = StringParser::StringToDecimal<int64_t>(
          reinterpret_cast<char*>(val.ptr), val.len, precision, scale,
          is_decimal_v2, &result);
      dv = DecimalVal(dv8.value());
      break;
    }
    case 16: {
      Decimal16Value dv16 = StringParser::StringToDecimal<int128_t>(
          reinterpret_cast<char*>(val.ptr), val.len, precision, scale,
          is_decimal_v2, &result);
      dv = DecimalVal(dv16.value());
      break;
    }
    default:
      DCHECK(false);
      return DecimalVal::null();
  }

  if (UNLIKELY(result == StringParser::PARSE_FAILURE)) {
    if (is_decimal_v2) {
      ctx->SetError("String to Decimal parse failed");
    } else {
      ctx->AddWarning("String to Decimal parse failed");
    }
    return DecimalVal::null();
  }

  if (UNLIKELY(result == StringParser::PARSE_OVERFLOW)) {
    if (is_decimal_v2) {
      ctx->SetError("String to Decimal cast overflowed");
    } else {
      ctx->AddWarning("String to Decimal cast overflowed");
    }
    return DecimalVal::null();
  }

  DCHECK(result == StringParser::PARSE_SUCCESS || StringParser::PARSE_UNDERFLOW);
  return dv;
}

StringVal DecimalOperators::CastToStringVal(
    FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return StringVal::null();
  int precision = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0);
  string s;
  switch (ColumnType::GetDecimalByteSize(precision)) {
    case 4:
      s = Decimal4Value(val.val4).ToString(precision, scale);
      break;
    case 8:
      s = Decimal8Value(val.val8).ToString(precision, scale);
      break;
    case 16:
      s = Decimal16Value(val.val16).ToString(precision, scale);
      break;
    default:
      DCHECK(false);
      return StringVal::null();
  }
  StringVal result(ctx, s.size());
  memcpy(result.ptr, s.c_str(), s.size());
  return result;
}

template <typename T>
IR_ALWAYS_INLINE int32_t DecimalOperators::ConvertToNanoseconds(
    T val, int scale, bool round) {
  // Nanosecond scale means there should be 9 decimal digits, which is representable
  // with int32_t.
  const int NANOSECOND_SCALE = 9;
  T nanoseconds;
  if (LIKELY(scale <= NANOSECOND_SCALE)) {
    nanoseconds = val * DecimalUtil::GetScaleMultiplier<T>(
        NANOSECOND_SCALE - scale);
  } else {
    nanoseconds = DecimalUtil::ScaleDownAndRound<T>(
        val, scale - NANOSECOND_SCALE, round);
    DCHECK(nanoseconds <= 1000000000);
    DCHECK(nanoseconds != 1000000000 || round);
  }
  DCHECK(nanoseconds >= numeric_limits<int32_t>::min()
      && nanoseconds <= numeric_limits<int32_t>::max());

  return nanoseconds;
}

template <typename T>
TimestampVal DecimalOperators::ConvertToTimestampVal(
    const T& decimal_value, int scale, bool round) {
  typename T::StorageType seconds = decimal_value.whole_part(scale);
  if (seconds < numeric_limits<int64_t>::min() ||
      seconds > numeric_limits<int64_t>::max()) {
    // TimeStampVal() takes int64_t.
    return TimestampVal::null();
  }
  int32_t nanoseconds =
      ConvertToNanoseconds(decimal_value.fractional_part(scale), scale, round);
  if (decimal_value.is_negative()) nanoseconds *= -1;
  TimestampVal result;
  TimestampValue::FromUnixTimeNanos(seconds, nanoseconds).ToTimestampVal(&result);
  return result;
}


TimestampVal DecimalOperators::CastToTimestampVal(
    FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return TimestampVal::null();
  int precision = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  int scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0);
  bool is_decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  TimestampVal result;
  switch (ColumnType::GetDecimalByteSize(precision)) {
    case 4:
      return ConvertToTimestampVal(Decimal4Value(val.val4), scale, is_decimal_v2);
    case 8:
      return ConvertToTimestampVal(Decimal8Value(val.val8), scale, is_decimal_v2);
    case 16:
      return ConvertToTimestampVal(Decimal16Value(val.val16), scale, is_decimal_v2);
    default:
      DCHECK(false);
      return TimestampVal::null();
  }
}

BooleanVal DecimalOperators::CastToBooleanVal(
    FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return BooleanVal::null();
  switch (ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0)) {
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
      FunctionContext* ctx, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null || y.is_null) return DecimalVal::null(); \
    bool overflow = false; \
    int x_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0); \
    int x_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0); \
    int y_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 1); \
    int y_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 1); \
    int return_precision = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION); \
    int return_scale = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE); \
    bool round = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2); \
    switch (ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SIZE)) { \
      case 4: { \
        DCHECK_LE(x_size, 4); \
        DCHECK_LE(y_size, 4); \
        Decimal4Value x_val = GetDecimal4Value(x, x_size, &overflow); \
        Decimal4Value y_val = GetDecimal4Value(y, y_size, &overflow); \
        Decimal4Value result = x_val.OP_FN<int32_t>(x_scale, y_val, y_scale, \
            return_precision, return_scale, round, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        return DecimalVal(result.value()); \
      } \
      case 8: { \
        DCHECK_LE(x_size, 8); \
        DCHECK_LE(y_size, 8); \
        Decimal8Value x_val = GetDecimal8Value(x, x_size, &overflow); \
        Decimal8Value y_val = GetDecimal8Value(y, y_size, &overflow); \
        Decimal8Value result = x_val.OP_FN<int64_t>(x_scale, y_val, y_scale, \
            return_precision, return_scale, round, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        return DecimalVal(result.value()); \
      } \
      case 16: { \
        Decimal16Value x_val = GetDecimal16Value(x, x_size, &overflow); \
        Decimal16Value y_val = GetDecimal16Value(y, y_size, &overflow); \
        Decimal16Value result = x_val.OP_FN<int128_t>(x_scale, y_val, y_scale, \
            return_precision, return_scale, round, &overflow); \
        RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal); \
        return DecimalVal(result.value()); \
      } \
      default: \
        break; \
    } \
    return DecimalVal::null(); \
  }

#define DECIMAL_ARITHMETIC_OP_CHECK_NAN(FN_NAME, OP_FN) \
  DecimalVal DecimalOperators::FN_NAME( \
      FunctionContext* ctx, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null || y.is_null) return DecimalVal::null(); \
    bool overflow = false; \
    bool is_nan = false; \
    int x_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0); \
    int x_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0); \
    int y_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 1); \
    int y_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 1); \
    int return_size = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SIZE); \
    int return_precision = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION); \
    int return_scale = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE); \
    const bool decimal_v2 = \
        ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2); \
    /* We need a type that is big enough for the result and the operands as well */ \
    int max_size = ::max(::max(x_size, y_size), return_size); \
    switch (max_size) { \
      case 4: { \
        Decimal4Value x_val = GetDecimal4Value(x, x_size, &overflow); \
        Decimal4Value y_val = GetDecimal4Value(y, y_size, &overflow); \
        Decimal4Value result = x_val.OP_FN<int32_t>(x_scale, y_val, y_scale, \
            return_precision, return_scale, decimal_v2, &is_nan, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        if (is_nan) { \
          if (decimal_v2) ctx->SetError("Cannot divide decimal by zero"); \
          return DecimalVal::null(); \
        } \
        return DecimalVal(result.value()); \
      } \
      case 8: { \
        Decimal8Value x_val = GetDecimal8Value(x, x_size, &overflow); \
        Decimal8Value y_val = GetDecimal8Value(y, y_size, &overflow); \
        Decimal8Value result = x_val.OP_FN<int64_t>(x_scale, y_val, y_scale, \
            return_precision, return_scale, decimal_v2, &is_nan, &overflow); \
        DCHECK(!overflow) << "Cannot overflow except with Decimal16Value"; \
        if (is_nan) { \
          if (decimal_v2) ctx->SetError("Cannot divide decimal by zero"); \
          return DecimalVal::null(); \
        } \
        return DecimalVal(result.value()); \
      } \
      case 16: { \
        Decimal16Value x_val = GetDecimal16Value(x, x_size, &overflow); \
        Decimal16Value y_val = GetDecimal16Value(y, y_size, &overflow); \
        Decimal16Value result = x_val.OP_FN<int128_t>(x_scale, y_val, y_scale, \
            return_precision, return_scale, decimal_v2, &is_nan, &overflow); \
        RETURN_IF_OVERFLOW(ctx, overflow, DecimalVal); \
        if (is_nan) { \
          if (decimal_v2) ctx->SetError("Cannot divide decimal by zero"); \
          return DecimalVal::null(); \
        } \
        return DecimalVal(result.value()); \
      } \
      default: \
        break; \
    } \
    return DecimalVal::null(); \
  }

#define DECIMAL_BINARY_OP_NONNULL(OP_FN, X, Y) \
  bool dummy = false; \
  int x_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0); \
  int x_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0); \
  int y_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 1); \
  int y_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 1); \
  int byte_size = ::max(x_size, y_size); \
  switch (byte_size) { \
    case 4: { \
      Decimal4Value x_val = GetDecimal4Value(X, x_size, &dummy); \
      Decimal4Value y_val = GetDecimal4Value(Y, y_size, &dummy); \
      bool result = x_val.OP_FN(x_scale, y_val, y_scale); \
      return BooleanVal(result); \
    } \
    case 8: { \
      Decimal8Value x_val = GetDecimal8Value(X, x_size, &dummy); \
      Decimal8Value y_val = GetDecimal8Value(Y, y_size, &dummy); \
      bool result = x_val.OP_FN(x_scale, y_val, y_scale); \
      return BooleanVal(result); \
    } \
    case 16: { \
      Decimal16Value x_val = GetDecimal16Value(X, x_size, &dummy); \
      Decimal16Value y_val = GetDecimal16Value(Y, y_size, &dummy); \
      bool result = x_val.OP_FN(x_scale, y_val, y_scale); \
      return BooleanVal(result); \
    } \
    default: \
      DCHECK(false); \
      break; \
  } \
  return BooleanVal::null();

#define DECIMAL_BINARY_OP(FN_NAME, OP_FN) \
  BooleanVal DecimalOperators::FN_NAME( \
      FunctionContext* ctx, const DecimalVal& x, const DecimalVal& y) { \
    if (x.is_null || y.is_null) return BooleanVal::null(); \
    DECIMAL_BINARY_OP_NONNULL(OP_FN, x, y) \
  }

#define NULLSAFE_DECIMAL_BINARY_OP(FN_NAME, OP_FN, IS_EQUAL) \
  BooleanVal DecimalOperators::FN_NAME( \
      FunctionContext* ctx, const DecimalVal& x, const DecimalVal& y) { \
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
