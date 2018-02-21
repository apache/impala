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

#include "exprs/udf-builtins.h"

#include <ctype.h>
#include <gutil/strings/substitute.h>
#include <iostream>
#include <math.h>
#include <sstream>
#include <string>

#include "gen-cpp/Exprs_types.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "udf/udf-internal.h"
#include "util/bit-util.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using namespace impala;
using namespace strings;

DoubleVal UdfBuiltins::Abs(FunctionContext* context, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(fabs(v.val));
}

DoubleVal UdfBuiltins::Pi(FunctionContext* context) {
  return DoubleVal(M_PI);
}

StringVal UdfBuiltins::Lower(FunctionContext* context, const StringVal& v) {
  if (v.is_null) return v;
  StringVal result(context, v.len);
  if (UNLIKELY(result.is_null)) {
    DCHECK(!context->impl()->state()->GetQueryStatus().ok());
    return result;
  }
  for (int i = 0; i < v.len; ++i) {
    result.ptr[i] = tolower(v.ptr[i]);
  }
  return result;
}

IntVal UdfBuiltins::MaxInt(FunctionContext* context) {
  return IntVal(numeric_limits<int32_t>::max());
}

TinyIntVal UdfBuiltins::MaxTinyInt(FunctionContext* context) {
  return TinyIntVal(numeric_limits<int8_t>::max());
}

SmallIntVal UdfBuiltins::MaxSmallInt(FunctionContext* context) {
  return SmallIntVal(numeric_limits<int16_t>::max());
}

BigIntVal UdfBuiltins::MaxBigInt(FunctionContext* context) {
  return BigIntVal(numeric_limits<int64_t>::max());
}

IntVal UdfBuiltins::MinInt(FunctionContext* context) {
  return IntVal(numeric_limits<int32_t>::min());
}

TinyIntVal UdfBuiltins::MinTinyInt(FunctionContext* context) {
  return TinyIntVal(numeric_limits<int8_t>::min());
}

SmallIntVal UdfBuiltins::MinSmallInt(FunctionContext* context) {
  return SmallIntVal(numeric_limits<int16_t>::min());
}

BigIntVal UdfBuiltins::MinBigInt(FunctionContext* context) {
  return BigIntVal(numeric_limits<int64_t>::min());
}

BooleanVal UdfBuiltins::IsNan(FunctionContext* context, const DoubleVal& val) {
  if (val.is_null) return BooleanVal(false);
  return BooleanVal(std::isnan(val.val));
}

BooleanVal UdfBuiltins::IsInf(FunctionContext* context, const DoubleVal& val) {
  if (val.is_null) return BooleanVal(false);
  return BooleanVal(std::isinf(val.val));
}

TimestampVal UdfBuiltins::Trunc(FunctionContext* context, const TimestampVal& tv,
    const StringVal &unit_str) {
  return TruncImpl(context, tv, unit_str);
}

TimestampVal UdfBuiltins::DateTrunc(
    FunctionContext* context, const StringVal& unit_str, const TimestampVal& tv) {
  return DateTruncImpl(context, tv, unit_str);
}

// Maps the user facing name of a unit to a TExtractField
// Returns the TExtractField for the given unit
TExtractField::type StrToExtractField(FunctionContext* ctx, const StringVal& unit_str) {
  StringVal unit = UdfBuiltins::Lower(ctx, unit_str);
  if (UNLIKELY(unit.is_null)) return TExtractField::INVALID_FIELD;
  if (unit == "year") return TExtractField::YEAR;
  if (unit == "quarter") return TExtractField::QUARTER;
  if (unit == "month") return TExtractField::MONTH;
  if (unit == "day") return TExtractField::DAY;
  if (unit == "hour") return TExtractField::HOUR;
  if (unit == "minute") return TExtractField::MINUTE;
  if (unit == "second") return TExtractField::SECOND;
  if (unit == "millisecond") return TExtractField::MILLISECOND;
  if (unit == "epoch") return TExtractField::EPOCH;
  return TExtractField::INVALID_FIELD;
}

IntVal UdfBuiltins::Extract(FunctionContext* context, const StringVal& unit_str,
    const TimestampVal& tv) {
  // resolve extract_field using the prepared state if possible, o.w. parse now
  // ExtractPrepare() can only parse extract_field if user passes it as a string literal
  if (tv.is_null) return IntVal::null();

  TExtractField::type field;
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  if (state != NULL) {
    field = *reinterpret_cast<TExtractField::type*>(state);
  } else {
    field = StrToExtractField(context, unit_str);
    if (field == TExtractField::INVALID_FIELD) {
      string string_unit(reinterpret_cast<char*>(unit_str.ptr), unit_str.len);
      context->SetError(Substitute("invalid extract field: $0", string_unit).c_str());
      return IntVal::null();
    }
  }

  const date& orig_date = *reinterpret_cast<const date*>(&tv.date);
  const time_duration& time = *reinterpret_cast<const time_duration*>(&tv.time_of_day);

  switch (field) {
    case TExtractField::YEAR:
    case TExtractField::QUARTER:
    case TExtractField::MONTH:
    case TExtractField::DAY:
      if (orig_date.is_special()) return IntVal::null();
      break;
    case TExtractField::HOUR:
    case TExtractField::MINUTE:
    case TExtractField::SECOND:
    case TExtractField::MILLISECOND:
      if (time.is_special()) return IntVal::null();
      break;
    case TExtractField::EPOCH:
      if (time.is_special() || orig_date.is_special()) return IntVal::null();
      break;
    case TExtractField::INVALID_FIELD:
      DCHECK(false);
  }

  switch (field) {
    case TExtractField::YEAR: {
      return IntVal(orig_date.year());
    }
    case TExtractField::QUARTER: {
      int m = orig_date.month();
      return IntVal((m - 1) / 3 + 1);
    }
    case TExtractField::MONTH: {
      return IntVal(orig_date.month());
    }
    case TExtractField::DAY: {
      return IntVal(orig_date.day());
    }
    case TExtractField::HOUR: {
      return IntVal(time.hours());
    }
    case TExtractField::MINUTE: {
      return IntVal(time.minutes());
    }
    case TExtractField::SECOND: {
      return IntVal(time.seconds());
    }
    case TExtractField::MILLISECOND: {
      return IntVal(time.total_milliseconds() - time.total_seconds() * 1000);
    }
    case TExtractField::EPOCH: {
      ptime epoch_date(date(1970, 1, 1), time_duration(0, 0, 0));
      ptime cur_date(orig_date, time);
      time_duration diff = cur_date - epoch_date;
      return IntVal(diff.total_seconds());
    }
    default: {
      DCHECK(false) << field;
      return IntVal::null();
    }
  }
}

IntVal UdfBuiltins::Extract(FunctionContext* context, const TimestampVal& tv,
    const StringVal& unit_str) {
  return Extract(context, unit_str, tv);
}

void UdfBuiltins::ExtractPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope, int unit_idx) {
  // Parse the unit up front if we can, otherwise do it on the fly in Extract()
  if (ctx->IsArgConstant(unit_idx)) {
    StringVal* unit_str = reinterpret_cast<StringVal*>(ctx->GetConstantArg(unit_idx));
    TExtractField::type field = StrToExtractField(ctx, *unit_str);
    if (field == TExtractField::INVALID_FIELD) {
      string string_field(reinterpret_cast<char*>(unit_str->ptr), unit_str->len);
      ctx->SetError(Substitute("invalid extract field: $0", string_field).c_str());
    } else {
      TExtractField::type* state = ctx->Allocate<TExtractField::type>();
      RETURN_IF_NULL(ctx, state);
      *state = field;
      ctx->SetFunctionState(scope, state);
    }
  }
}

void UdfBuiltins::ExtractPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  ExtractPrepare(ctx, scope, 0);
}

void UdfBuiltins::SwappedExtractPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  ExtractPrepare(ctx, scope, 1);
}

void UdfBuiltins::ExtractClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  void* state = ctx->GetFunctionState(scope);
  ctx->Free(reinterpret_cast<uint8_t*>(state));
  ctx->SetFunctionState(scope, nullptr);
}

bool ValidateMADlibVector(FunctionContext* context, const StringVal& arr) {
  if (arr.ptr == NULL) {
    context->SetError("MADlib vector is null");
    return false;
  }
  if (arr.len % 8 != 0) {
    context->SetError(Substitute("MADlib vector of incorrect length $0,"
                                 " expected multiple of 8", arr.len).c_str());
    return false;
  }
  return true;
}

StringVal UdfBuiltins::ToVector(FunctionContext* context, int n, const DoubleVal* vals) {
  StringVal s(context, n * sizeof(double));
  if (UNLIKELY(s.is_null)) return StringVal::null();
  double* darr = reinterpret_cast<double*>(s.ptr);
  for (int i = 0; i < n; ++i) {
    if (vals[i].is_null) {
      context->SetError(Substitute("madlib vector entry $0 is NULL", i).c_str());
      return StringVal::null();
    }
    darr[i] = vals[i].val;
  }
  return s;
}

StringVal UdfBuiltins::PrintVector(FunctionContext* context, const StringVal& arr) {
  if (!ValidateMADlibVector(context, arr)) return StringVal::null();
  double* darr = reinterpret_cast<double*>(arr.ptr);
  int len = arr.len / sizeof(double);
  stringstream ss;
  ss << "<";
  for (int i = 0; i < len; ++i) {
    if (i != 0) ss << ", ";
    ss << darr[i];
  }
  ss << ">";
  const string& str = ss.str();
  return StringVal::CopyFrom(context, reinterpret_cast<const uint8_t*>(str.c_str()),
      str.size());
}

DoubleVal UdfBuiltins::VectorGet(FunctionContext* context, const BigIntVal& index,
    const StringVal& arr) {
  if (!ValidateMADlibVector(context, arr)) return DoubleVal::null();
  if (index.is_null) return DoubleVal::null();
  uint64_t i = index.val;
  uint64_t len = arr.len / sizeof(double);
  if (index.val < 0 || len <= i) return DoubleVal::null();
  double* darr = reinterpret_cast<double*>(arr.ptr);
  return DoubleVal(darr[i]);
}

void InplaceDoubleEncode(double* arr, uint64_t len) {
  for (uint64_t i = 0; i < len; ++i) {
    char* hex = reinterpret_cast<char*>(&arr[i]);
    // cast to float so we have 4 bytes to encode but 8 bytes of space
    float float_val = arr[i];
    uint32_t float_as_int = *reinterpret_cast<int32_t*>(&float_val);
    for (int k = 0; k < 8; ++k) {
      // This is a simple hex encoding, 'a' becomes 0, 'b' is 1, ...
      // This isn't a complicated encoding, but it is simple to debug and is
      // a temporary solution until we have nested types
      hex[k] = 'a' + ((float_as_int >> (4*k)) & 0xF);
    }
  }
}

// Inplace conversion from a printable ascii encoding to a double*
void InplaceDoubleDecode(char* arr, uint64_t len) {
  for (uint64_t i = 0; i < len; i += 8) {
    double* dub = reinterpret_cast<double*>(&arr[i]);
    // cast to float so we have 4 bytes to encode but 8 bytes of space
    int32_t float_as_int = 0;
    for (int k = 7; k >= 0; --k) {
      float_as_int = (float_as_int <<4) | ((arr[i+k] - 'a') & 0xF);
    }
    float* float_ptr = reinterpret_cast<float*>(&float_as_int);
    *dub = *float_ptr;
  }
}

StringVal UdfBuiltins::EncodeVector(FunctionContext* context, const StringVal& arr) {
  if (arr.is_null) return StringVal::null();
  double* darr = reinterpret_cast<double*>(arr.ptr);
  int len = arr.len / sizeof(double);
  StringVal result(context, arr.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  memcpy(result.ptr, darr, arr.len);
  InplaceDoubleEncode(reinterpret_cast<double*>(result.ptr), len);
  return result;
}

StringVal UdfBuiltins::DecodeVector(FunctionContext* context, const StringVal& arr) {
  if (arr.is_null) return StringVal::null();
  StringVal result(context, arr.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  memcpy(result.ptr, arr.ptr, arr.len);
  InplaceDoubleDecode(reinterpret_cast<char*>(result.ptr), arr.len);
  return result;
}
