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

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/anyval-util.h"
#include "exprs/decimal-functions.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "util/string-parser.h"
#include "string-functions.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

// The maximum number of characters need to represent a floating-point number (float or
// double) as a string. 24 = 17 (maximum significant digits) + 1 (decimal point) + 1 ('E')
// + 3 (exponent digits) + 2 (negative signs) (see http://stackoverflow.com/a/1701085)
const int MAX_FLOAT_CHARS = 24;

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

#define CAST_TO_STRING(num_type) \
  StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const num_type& val) { \
    if (val.is_null) return StringVal::null(); \
    StringVal sv = AnyValUtil::FromString(ctx, lexical_cast<string>(val.val)); \
    AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv); \
    return sv; \
  }

CAST_TO_STRING(BooleanVal);
CAST_TO_STRING(SmallIntVal);
CAST_TO_STRING(IntVal);
CAST_TO_STRING(BigIntVal);

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

// Special-case tinyint because boost thinks it's a char and handles it differently.
// e.g. '0' is written as an empty string.
StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const TinyIntVal& val) {
  if (val.is_null) return StringVal::null();
  int64_t tmp_val = val.val;
  StringVal sv = AnyValUtil::FromString(ctx, lexical_cast<string>(tmp_val));
  AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);
  return sv;
}

StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const TimestampVal& val) {
  if (val.is_null) return StringVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(val);
  StringVal sv = AnyValUtil::FromString(ctx, lexical_cast<string>(tv));
  AnyValUtil::TruncateIfNecessary(ctx->GetReturnType(), &sv);
  return sv;
}

StringVal CastFunctions::CastToStringVal(FunctionContext* ctx, const DateVal& val) {
  if (val.is_null) return StringVal::null();
  DateValue dv = DateValue::FromDateVal(val);
  if (UNLIKELY(!dv.IsValid())) return StringVal::null();
  StringVal sv = AnyValUtil::FromString(ctx, dv.ToString());
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
    if (!tv.ToUnixTime(ctx->impl()->state()->local_time_zone(), &result)) { \
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
    if (!tv.ToSubsecondUnixTime(ctx->impl()->state()->local_time_zone(), &result)) { \
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
    TimestampValue timestamp_value = TimestampValue::FromSubsecondUnixTime(val.val, \
        ctx->impl()->state()->local_time_zone()); \
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
    TimestampValue timestamp_value = TimestampValue::FromUnixTime(val.val, \
        ctx->impl()->state()->local_time_zone()); \
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
  if (val.is_null) return TimestampVal::null();
  TimestampValue tv = TimestampValue::Parse(reinterpret_cast<char*>(val.ptr), val.len);
  // Return null if 'val' did not parse
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
  if (val.is_null) return DateVal::null();
  const char* stringVal = reinterpret_cast<const char*>(val.ptr);
  DateValue dv = DateValue::Parse(stringVal, val.len, true);
  if (UNLIKELY(!dv.IsValid())) {
    string invalidVal = string(stringVal, val.len);
    ctx->SetError(Substitute("String to Date parse failed. Invalid string val: \"$0\"",
        invalidVal).c_str());
    return DateVal::null();
  }
  return dv.ToDateVal();
}

DateVal CastFunctions::CastToDateVal(FunctionContext* ctx, const TimestampVal& val) {
  if (val.is_null) return DateVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(val);
  if (UNLIKELY(!tv.HasDate())) {
    ctx->SetError("Timestamp to Date conversion failed. "
        "Timestamp has no date component.");
    return DateVal::null();
  }

  DateValue dv(tv.DaysSinceUnixEpoch());
  return dv.ToDateVal();
}
