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
#include <sstream>
#include <string>

#include <gutil/strings/numbers.h>
#include <gutil/strings/substitute.h>

#include "exprs/anyval-util.h"
#include "exprs/cast-format-expr.h"
#include "exprs/decimal-functions.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "util/string-parser.h"

#include "common/names.h"

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

#define CAST_FUNCTION(from_type, to_type) \
  to_type CastFunctions::CastTo##to_type(FunctionContext* ctx, const from_type& val) { \
    if (val.is_null) return to_type::null(); \
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


#define CAST_FLOAT_TO_STRING(float_type, format) \
  StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const float_type& val) { \
    if (val.is_null) return StringVal::null(); \
    /* val.val could be -nan, return "nan" instead */ \
    if (std::isnan(val.val)) return StringVal("nan"); \
    /* Add 1 to MAX_FLOAT_CHARS since snprintf adds a trailing '\0' */ \
    StringVal sv(ctx, MAX_FLOAT_CHARS + 1); \
    if (UNLIKELY(sv.is_null)) { \
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok()); \
      return sv; \
    } \
    sv.len = snprintf(reinterpret_cast<char*>(sv.ptr), sv.len, format, val.val); \
    DCHECK_GT(sv.len, 0); \
    DCHECK_LE(sv.len, MAX_FLOAT_CHARS); \
    AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv); \
    return sv; \
  }

// Floats have up to 9 significant digits, doubles up to 17
// (see http://en.wikipedia.org/wiki/Single-precision_floating-point_format
// and http://en.wikipedia.org/wiki/Double-precision_floating-point_format)
CAST_FLOAT_TO_STRING(FloatVal, "%.9g");
CAST_FLOAT_TO_STRING(DoubleVal, "%.17g");


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

StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const StringVal& val) {
  if (val.is_null) return StringVal::null();
  StringVal sv;
  sv.ptr = val.ptr;
  sv.len = val.len;
  AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);
  return sv;
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
