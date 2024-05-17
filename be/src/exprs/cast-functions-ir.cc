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

#include "exprs/cast-functions.h"

#include <cmath>
#include <limits>
#include <sstream>
#include <string>
#include <type_traits>

#include <gutil/strings/numbers.h>
#include <gutil/strings/substitute.h>

#include "common/names.h"
#include "exprs/anyval-util.h"
#include "exprs/cast-format-expr.h"
#include "exprs/decimal-functions.h"
#include "gutil/strings/numbers.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "util/string-parser.h"

using namespace impala;
using namespace impala_udf;
using namespace datetime_parse_util;

// The maximum number of characters need to represent a floating-point number (float or
// double) as a string. 24 = 17 (maximum significant digits) + 1 (decimal point) + 1 ('E')
// + 3 (exponent digits) + 2 (negative signs) (see http://stackoverflow.com/a/1701085)
const int MAX_FLOAT_CHARS = 24;
// bigint max length is 20, give 22 as gutil required
const int MAX_EXACT_NUMERIC_CHARS = 22;
// -9.2e18 or unsigned 2^64=1.8e19
const int MAX_BIGINT_CHARS = 20;
// -2^31=-2.1e9
const int MAX_INT_CHARS = 11;
// -2^15=-32768
const int MAX_SMALLINT_CHARS = 6;
// -128~127 or unsigned 0~255
const int MAX_TINYINT_CHARS = 4;
// 1 0
const int MAX_BOOLEAN_CHARS = 1;

namespace {

template <class T>
constexpr const char* TypeToName() {
  if constexpr (std::is_same_v<T, int8_t>) {
    return "TINYINT";
  } else if constexpr (std::is_same_v<T, int16_t>) {
    return "SMALLINT";
  } else if constexpr (std::is_same_v<T, int32_t>) {
    return "INT";
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return "BIGINT";
  } else if constexpr (std::is_same_v<T, float>) {
    return "FLOAT";
  } else if constexpr (std::is_same_v<T, double>) {
    return "DOUBLE";
  } else {
    // This function doesn't support other types.
    static_assert(!std::is_same_v<T, T>);
    return nullptr;
  }
}

// This struct is used as a helper in the validation of casts. For a cast from 'FROM_TYPE'
// to 'TO_TYPE' it provides compile time constants about what type of conversion it is.
// These constants are used in the template specifications of the 'Validate()' function to
// group together the relevant cases.
template <class FROM_TYPE, class TO_TYPE>
struct ConversionType {
  // std::is_integral includes 'bool' but 'bool' behaves differently from integers in
  // conversions.
  template <class T>
  static constexpr bool is_non_bool_integer =
      std::is_integral_v<T> &&
      !std::is_same_v<T, bool>;

  ///////////////////////////////////
  // Conversions that cannot fail. //
  ///////////////////////////////////
  static constexpr bool same_type_conversion = std::is_same_v<FROM_TYPE, TO_TYPE>;

  static constexpr bool integer_to_integer_conversion =
      is_non_bool_integer<FROM_TYPE> &&
      is_non_bool_integer<TO_TYPE>;

  static constexpr bool boolean_conversion =
    std::is_same_v<FROM_TYPE, bool> ||
    std::is_same_v<TO_TYPE, bool>;

  // Integer to floating point cannot fail because a 32-bit float can represent the min
  // and max value of a 64-bit integer.
  static constexpr bool integer_to_floatingpoint_conversion =
      is_non_bool_integer<FROM_TYPE> &&
      std::is_floating_point_v<TO_TYPE>;

  static constexpr bool float_to_double_conversion =
      std::is_same_v<FROM_TYPE, float> &&
      std::is_same_v<TO_TYPE, double>;

  static constexpr bool conversion_always_succeeds =
      same_type_conversion ||
      integer_to_integer_conversion ||
      boolean_conversion ||
      integer_to_floatingpoint_conversion ||
      float_to_double_conversion;

  ////////////////////////////////
  // Conversions that can fail. //
  ////////////////////////////////
  static constexpr bool floatingpoint_to_integer_conversion =
      std::is_floating_point_v<FROM_TYPE> &&
      is_non_bool_integer<TO_TYPE>;

  static constexpr bool double_to_float_conversion =
      std::is_same_v<FROM_TYPE, double> &&
      std::is_same_v<TO_TYPE, float>;
};

template <class FROM_TYPE, class TO_TYPE, typename std::enable_if_t<
    ConversionType<FROM_TYPE, TO_TYPE>::conversion_always_succeeds
    >* = nullptr>
bool Validate(FROM_TYPE from, FunctionContext* ctx) {
  return true;
}

template <class FROM_TYPE, class TO_TYPE, typename std::enable_if_t<
    ConversionType<FROM_TYPE, TO_TYPE>::floatingpoint_to_integer_conversion
    >* = nullptr>
bool Validate(FROM_TYPE from, FunctionContext* ctx) {
  // Rules for conversion from floating point types to integral types:
  //  1. the fractional part is discarded
  //  2. if the destination type can hold the integer part, that is used as the result,
  //     otherwise the behaviour is undefined.
  // See https://en.cppreference.com/w/cpp/language/implicit_conversion
  //
  // We need to check whether the floating point number 'from' fits within the range of
  // the integer type 'TO_TYPE'. We convert the max and min value of 'TO_TYPE' to
  // 'FROM_TYPE'. This conversion may not be exact but in this case either the closest
  // higher or the closest lower value is selected. This guarentees that if 'from' is
  // within these values, it can be converted to 'TO_TYPE'.
  //
  // Note that we cannot allow equality in the general case because the maximal value of
  // 'TO_TYPE' converted to floating point may be larger than the maximal value of
  // 'TO_TYPE' because of inexact conversion (and similarly for the minimal value).
  //
  // A 'double' can exactly represent the maximal and minimal values of 32-bit and smaller
  // integers, but not of 64-bit integers. For a 'float' 32-bit integers are too big but
  // 16-bit and smaller integers are ok.
  constexpr FROM_TYPE upper_limit = static_cast<FROM_TYPE>(
      std::numeric_limits<TO_TYPE>::max());
  constexpr FROM_TYPE lower_limit = static_cast<FROM_TYPE>(
      std::numeric_limits<TO_TYPE>::lowest());

  constexpr int INT_SIZE_LIMIT = std::is_same_v<FROM_TYPE, double> ? 4 : 2;
  bool is_ok = false;
  if (sizeof(TO_TYPE) <= INT_SIZE_LIMIT) {
    is_ok = lower_limit <= from && from <= upper_limit;
  } else {
    is_ok = lower_limit < from && from < upper_limit;
  }

  if (UNLIKELY(!is_ok)) {
    constexpr const char* FROM_TYPE_NAME = TypeToName<FROM_TYPE>();
    constexpr const char* TO_TYPE_NAME = TypeToName<TO_TYPE>();
    string err;
    if (std::isnan(from)) {
      err = Substitute("NaN value of type $0 cannot be converted to $1.",
          FROM_TYPE_NAME, TO_TYPE_NAME);
    } else if (!std::isfinite(from)) {
      err = Substitute("Non-finite value of type $0 cannot be converted to $1.",
          FROM_TYPE_NAME, TO_TYPE_NAME);
    } else {
      err = Substitute("Converting value $0 of type $1 to $2 failed, "
          "value out of range for destination type.", from, FROM_TYPE_NAME, TO_TYPE_NAME);
    }
    ctx->SetError(err.c_str());
  }

  return is_ok;
}

template <class FROM_TYPE, class TO_TYPE, typename std::enable_if_t<
    ConversionType<FROM_TYPE, TO_TYPE>::double_to_float_conversion
    >* = nullptr>
bool Validate(FROM_TYPE from, FunctionContext* ctx) {
  // Rules for conversion from floating point types to integral types:
  //  1. If the source value can be represented exactly in the destination type, it does
  //     not change.
  //  2. If the source value is between two representable values of the destination type,
  //     the result is one of those two values (it is implementation-defined which one).
  //  3. Otherwise, the behavior is undefined.
  // See https://en.cppreference.com/w/cpp/language/implicit_conversion
  //
  // The algorithm is similar to the case of floating point to integer conversion. We
  // check whether the double value 'from' is within the range of float. The min and max
  // values of float can be exactly represented as double, so we allow equality.
  constexpr FROM_TYPE upper_limit = std::numeric_limits<TO_TYPE>::max();
  constexpr FROM_TYPE lower_limit = std::numeric_limits<TO_TYPE>::lowest();
  const bool in_range = lower_limit <= from && from <= upper_limit;

  // In-range values, NaNs and infinite values can be converted to float.
  const bool is_ok = in_range || std::isnan(from) || !std::isfinite(from);

  if (UNLIKELY(!is_ok)) {
    const string err = Substitute("Converting value $0 of type $1 to $2 failed, "
        "value out of range for destination type.", from, TypeToName<FROM_TYPE>(),
        TypeToName<TO_TYPE>());
    ctx->SetError(err.c_str());
  }

  return is_ok;
}

} // anonymous namespace

#define CAST_FUNCTION(from_type, to_type) \
  to_type CastFunctions::CastTo##to_type(FunctionContext* ctx, const from_type& val) { \
    if (val.is_null) return to_type::null(); \
    using from_underlying_type = decltype(from_type::val); \
    using to_underlying_type = decltype(to_type::val); \
    const bool valid = Validate<from_underlying_type, to_underlying_type>(val.val, ctx); \
    /* The query will be aborted because 'Validate()' sets an error but we have to return
     * something so we return NULL. */\
    if (UNLIKELY(!valid)) return to_type::null(); \
    return to_type(val.val); \
  }

CAST_FUNCTION(TinyIntVal, BooleanVal)
CAST_FUNCTION(SmallIntVal, BooleanVal)
CAST_FUNCTION(IntVal, BooleanVal)
CAST_FUNCTION(BigIntVal, BooleanVal)
CAST_FUNCTION(FloatVal, BooleanVal)
CAST_FUNCTION(DoubleVal, BooleanVal)

CAST_FUNCTION(BooleanVal, TinyIntVal)
CAST_FUNCTION(SmallIntVal, TinyIntVal)
CAST_FUNCTION(IntVal, TinyIntVal)
CAST_FUNCTION(BigIntVal, TinyIntVal)
CAST_FUNCTION(FloatVal, TinyIntVal)
CAST_FUNCTION(DoubleVal, TinyIntVal)

CAST_FUNCTION(BooleanVal, SmallIntVal)
CAST_FUNCTION(TinyIntVal, SmallIntVal)
CAST_FUNCTION(IntVal, SmallIntVal)
CAST_FUNCTION(BigIntVal, SmallIntVal)
CAST_FUNCTION(FloatVal, SmallIntVal)
CAST_FUNCTION(DoubleVal, SmallIntVal)

CAST_FUNCTION(BooleanVal, IntVal)
CAST_FUNCTION(TinyIntVal, IntVal)
CAST_FUNCTION(SmallIntVal, IntVal)
CAST_FUNCTION(BigIntVal, IntVal)
CAST_FUNCTION(FloatVal, IntVal)
CAST_FUNCTION(DoubleVal, IntVal)

CAST_FUNCTION(BooleanVal, BigIntVal)
CAST_FUNCTION(TinyIntVal, BigIntVal)
CAST_FUNCTION(SmallIntVal, BigIntVal)
CAST_FUNCTION(IntVal, BigIntVal)
CAST_FUNCTION(FloatVal, BigIntVal)
CAST_FUNCTION(DoubleVal, BigIntVal)

CAST_FUNCTION(BooleanVal, FloatVal)
CAST_FUNCTION(TinyIntVal, FloatVal)
CAST_FUNCTION(SmallIntVal, FloatVal)
CAST_FUNCTION(IntVal, FloatVal)
CAST_FUNCTION(BigIntVal, FloatVal)
CAST_FUNCTION(DoubleVal, FloatVal)

CAST_FUNCTION(BooleanVal, DoubleVal)
CAST_FUNCTION(TinyIntVal, DoubleVal)
CAST_FUNCTION(SmallIntVal, DoubleVal)
CAST_FUNCTION(IntVal, DoubleVal)
CAST_FUNCTION(BigIntVal, DoubleVal)
CAST_FUNCTION(FloatVal, DoubleVal)

#define CAST_FROM_STRING(num_type, native_type, string_parser_fn) \
  num_type CastFunctions::CastTo##num_type(FunctionContext* ctx, const StringVal& val) { \
    if (val.is_null) return num_type::null(); \
    StringParser::ParseResult result; \
    num_type ret; \
    ret.val = StringParser::string_parser_fn<native_type>( \
        reinterpret_cast<char*>(val.ptr), val.len, &result); \
    if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) return num_type::null(); \
    return ret; \
  }

CAST_FROM_STRING(TinyIntVal, int8_t, StringToInt)
CAST_FROM_STRING(SmallIntVal, int16_t, StringToInt)
CAST_FROM_STRING(IntVal, int32_t, StringToInt)
CAST_FROM_STRING(BigIntVal, int64_t, StringToInt)
CAST_FROM_STRING(FloatVal, float, StringToFloat)
CAST_FROM_STRING(DoubleVal, double, StringToFloat)


#define CAST_EXACT_NUMERIC_TO_STRING(num_type, max_char_size, to_string_method) \
  StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const num_type& val) { \
    if (val.is_null) return StringVal::null(); \
    DCHECK_LE(max_char_size, MAX_EXACT_NUMERIC_CHARS); \
    char char_buffer[MAX_EXACT_NUMERIC_CHARS];\
    char* end_position = to_string_method(val.val, char_buffer); \
    size_t len = end_position - char_buffer; \
    DCHECK_GT(len, 0); \
    DCHECK_LE(len, max_char_size); \
    StringVal sv = StringVal::CopyFrom(ctx,\
       reinterpret_cast<const uint8_t *> (char_buffer), len); \
    if (UNLIKELY(sv.is_null)) { \
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok()); \
      return sv; \
    } \
    AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv); \
    return sv; \
  }

// boolean, tinyint, smallint will be casted to int here via one of the gutil methods.
CAST_EXACT_NUMERIC_TO_STRING(BooleanVal, MAX_BOOLEAN_CHARS, FastInt32ToBufferLeft);
CAST_EXACT_NUMERIC_TO_STRING(TinyIntVal, MAX_TINYINT_CHARS, FastInt32ToBufferLeft);
CAST_EXACT_NUMERIC_TO_STRING(SmallIntVal, MAX_SMALLINT_CHARS, FastInt32ToBufferLeft);
CAST_EXACT_NUMERIC_TO_STRING(IntVal, MAX_INT_CHARS, FastInt32ToBufferLeft);
CAST_EXACT_NUMERIC_TO_STRING(BigIntVal, MAX_BIGINT_CHARS, FastInt64ToBufferLeft);

#define CAST_FLOAT_TO_STRING(float_type, convert_method, buffer_size)          \
  StringVal CastFunctions::CastToStringVal(                                    \
      FunctionContext* ctx, const float_type& val) {                           \
    if (val.is_null) return StringVal::null();                                 \
    /* val.val could be -nan, return "nan" instead */                          \
    if (std::isnan(val.val)) return StringVal("nan");                          \
    StringVal sv(ctx, buffer_size);                                            \
    if (UNLIKELY(sv.is_null)) {                                                \
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());                    \
      return sv;                                                               \
    }                                                                          \
    sv.len = strlen(convert_method(val.val, reinterpret_cast<char*>(sv.ptr))); \
    DCHECK_GT(sv.len, 0);                                                      \
    DCHECK_LE(sv.len, MAX_FLOAT_CHARS);                                        \
    AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);                \
    return sv;                                                                 \
  }

// Convert a double or float to a string and produce the exact same original precision.
// See gutil/strings/numbers.h and gutil/strings/numbers.cc for more details.
CAST_FLOAT_TO_STRING(FloatVal, FloatToBuffer, kFloatToBufferSize);
CAST_FLOAT_TO_STRING(DoubleVal, DoubleToBuffer, kDoubleToBufferSize);

StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const TimestampVal& val) {
  DCHECK(ctx != nullptr);
  if (val.is_null) return StringVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(val);
  const DateTimeFormatContext* format_ctx =
      reinterpret_cast<const DateTimeFormatContext*>(
          ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  StringVal sv =
      (format_ctx == nullptr) ? tv.ToStringVal(ctx) : tv.ToStringVal(ctx, *format_ctx);
  AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);
  return sv;
}

StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const DateVal& val) {
  DCHECK(ctx != nullptr);
  if (val.is_null) return StringVal::null();
  DateValue dv = DateValue::FromDateVal(val);
  if (UNLIKELY(!dv.IsValid())) return StringVal::null();
  const DateTimeFormatContext* format_ctx =
      reinterpret_cast<const DateTimeFormatContext*>(
          ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  StringVal sv;
  if (format_ctx == nullptr) {
    sv = AnyValUtil::FromString(ctx, dv.ToString());
  } else {
    string formatted_date = dv.Format(*format_ctx);
    if (formatted_date.empty()) return StringVal::null();
    sv = AnyValUtil::FromString(ctx, formatted_date);
  }
  AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);
  return sv;
}

// This function handles the following casts:
// STRING / CHAR(N) / VARCHAR(N) -> VARCHAR(N)
StringVal CastFunctions::CastToVarchar(FunctionContext* ctx, const StringVal& val) {
  if (val.is_null) return StringVal::null();
  StringVal sv;
  sv.ptr = val.ptr;
  sv.len = val.len;
  AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);
  return sv;
}

// This function handles the following casts:
// BINARY / CHAR(N) / VARCHAR(N) -> STRING
// STRING -> BINARY
// These casts are esentially NOOP, but it is useful to have a function in the
// expression tree to be able to get the exact return type.
StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const StringVal& val) {
  return val;
}

StringVal CastFunctions::CastToChar(FunctionContext* ctx, const StringVal& val) {
  if (val.is_null) return StringVal::null();

  const FunctionContext::TypeDesc& type = ctx->GetReturnType();
  DCHECK(type.type == FunctionContext::TYPE_FIXED_BUFFER);
  DCHECK_GE(type.len, 1);
  char* cptr;
  if (type.len > val.len) {
    cptr = reinterpret_cast<char*>(ctx->impl()->AllocateForResults(type.len));
    if (UNLIKELY(cptr == NULL)) {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
      return StringVal::null();
    }
    memcpy(cptr, val.ptr, min(type.len, val.len));
    StringValue::PadWithSpaces(cptr, type.len, val.len);
  } else {
    cptr = reinterpret_cast<char*>(val.ptr);
  }
  StringVal sv;
  sv.ptr = reinterpret_cast<uint8_t*>(cptr);
  sv.len = type.len;
  return sv;
}

#define CAST_FROM_TIMESTAMP(to_type) \
  to_type CastFunctions::CastTo##to_type( \
      FunctionContext* ctx, const TimestampVal& val) { \
    if (val.is_null) return to_type::null(); \
    TimestampValue tv = TimestampValue::FromTimestampVal(val); \
    time_t result; \
    const Timezone* tz = ctx->impl()->state()->time_zone_for_unix_time_conversions(); \
    if (!tv.ToUnixTime(tz, &result)) { \
      return to_type::null(); \
    } \
    return to_type(result); \
  }

CAST_FROM_TIMESTAMP(BooleanVal);
CAST_FROM_TIMESTAMP(TinyIntVal);
CAST_FROM_TIMESTAMP(SmallIntVal);
CAST_FROM_TIMESTAMP(IntVal);
CAST_FROM_TIMESTAMP(BigIntVal);

#define CAST_FROM_SUBSECOND_TIMESTAMP(to_type) \
  to_type CastFunctions::CastTo##to_type( \
      FunctionContext* ctx, const TimestampVal& val) { \
    if (val.is_null) return to_type::null(); \
    TimestampValue tv = TimestampValue::FromTimestampVal(val); \
    double result; \
    const Timezone* tz = ctx->impl()->state()->time_zone_for_unix_time_conversions(); \
    if (!tv.ToSubsecondUnixTime(tz, &result)) { \
      return to_type::null(); \
    } \
    return to_type(result);\
  }

CAST_FROM_SUBSECOND_TIMESTAMP(FloatVal);
CAST_FROM_SUBSECOND_TIMESTAMP(DoubleVal);

#define CAST_TO_SUBSECOND_TIMESTAMP(from_type) \
  TimestampVal CastFunctions::CastToTimestampVal(FunctionContext* ctx, \
                                                 const from_type& val) { \
    if (val.is_null) return TimestampVal::null(); \
    const Timezone* tz = ctx->impl()->state()->time_zone_for_unix_time_conversions(); \
    TimestampValue timestamp_value = TimestampValue::FromSubsecondUnixTime(val.val, \
        tz); \
    TimestampVal result; \
    timestamp_value.ToTimestampVal(&result); \
    return result; \
  }

CAST_TO_SUBSECOND_TIMESTAMP(FloatVal);
CAST_TO_SUBSECOND_TIMESTAMP(DoubleVal);

#define CAST_TO_TIMESTAMP(from_type) \
  TimestampVal CastFunctions::CastToTimestampVal(FunctionContext* ctx, \
                                                 const from_type& val) { \
    if (val.is_null) return TimestampVal::null(); \
    const Timezone* tz = ctx->impl()->state()->time_zone_for_unix_time_conversions(); \
    TimestampValue timestamp_value = TimestampValue::FromUnixTime(val.val, tz); \
    TimestampVal result; \
    timestamp_value.ToTimestampVal(&result); \
    return result; \
  }

CAST_TO_TIMESTAMP(BooleanVal);
CAST_TO_TIMESTAMP(TinyIntVal);
CAST_TO_TIMESTAMP(SmallIntVal);
CAST_TO_TIMESTAMP(IntVal);
CAST_TO_TIMESTAMP(BigIntVal);

TimestampVal CastFunctions::CastToTimestampVal(FunctionContext* ctx,
    const StringVal& val) {
  DCHECK(ctx != nullptr);
  if (val.is_null) return TimestampVal::null();
  const DateTimeFormatContext* format_ctx =
      reinterpret_cast<const DateTimeFormatContext*>(
          ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  TimestampValue tv;
  if (format_ctx != nullptr) {
    tv = TimestampValue::ParseIsoSqlFormat(reinterpret_cast<const char*>(val.ptr),
        val.len, *format_ctx);
  } else {
    tv = TimestampValue::ParseSimpleDateFormat(reinterpret_cast<const char*>(val.ptr),
        val.len);
  }
  TimestampVal result;
  tv.ToTimestampVal(&result);
  return result;
}

TimestampVal CastFunctions::CastToTimestampVal(FunctionContext* ctx, const DateVal& val) {
  if (val.is_null) return TimestampVal::null();
  const DateValue dv = DateValue::FromDateVal(val);

  int32_t days = 0;
  if (!dv.ToDaysSinceEpoch(&days)) return TimestampVal::null();

  TimestampValue tv = TimestampValue::FromDaysSinceUnixEpoch(days);
  if (UNLIKELY(!tv.HasDate())) {
    ctx->SetError("Date to Timestamp conversion failed. "
        "The valid date range for the Timestamp type is 1400-01-01..9999-12-31.");
    return TimestampVal::null();
  }
  TimestampVal result;
  tv.ToTimestampVal(&result);
  return result;
}

DateVal CastFunctions::CastToDateVal(FunctionContext* ctx, const StringVal& val) {
  DCHECK(ctx != nullptr);
  if (val.is_null) return DateVal::null();
  const char* string_val = reinterpret_cast<const char*>(val.ptr);
  const DateTimeFormatContext* format_ctx =
      reinterpret_cast<const DateTimeFormatContext*>(
          ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  DateValue dv;
  if (format_ctx != nullptr) {
    dv = DateValue::ParseIsoSqlFormat(string_val, val.len, *format_ctx);
  } else {
    dv = DateValue::ParseSimpleDateFormat(string_val, val.len, true);
  }
  if (UNLIKELY(!dv.IsValid())) {
    string invalid_val = string(string_val, val.len);
    string error_msg;
    if (format_ctx == nullptr) {
      error_msg = Substitute("String to Date parse failed. Invalid string val: '$0'",
        invalid_val);
    } else {
      error_msg = Substitute("String to Date parse failed. Input '$0' doesn't match "
        "with format '$1'", invalid_val, format_ctx->fmt);
    }
    ctx->SetError(error_msg.c_str());
    return DateVal::null();
  }
  return dv.ToDateVal();
}

DateVal CastFunctions::CastToDateVal(FunctionContext* ctx, const TimestampVal& val) {
  if (val.is_null) return DateVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(val);
  DateValue dv(tv.DaysSinceUnixEpoch());
  return dv.ToDateVal();
}
