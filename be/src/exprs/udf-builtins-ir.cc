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
#include "udf/udf-internal.h"

#include "common/names.h"

using namespace impala;

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

namespace {

/// Used for closing TRUNC/DATE_TRUNC/EXTRACT/DATE_PART built-in functions.
void CloseImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  void* state = ctx->GetFunctionState(scope);
  ctx->Free(reinterpret_cast<uint8_t*>(state));
  ctx->SetFunctionState(scope, nullptr);
}

}

void UdfBuiltins::TruncForTimestampPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return TruncForTimestampPrepareImpl(ctx, scope);
}

TimestampVal UdfBuiltins::TruncForTimestamp(FunctionContext* context,
    const TimestampVal& tv, const StringVal &unit_str) {
  return TruncForTimestampImpl(context, tv, unit_str);
}

void UdfBuiltins::TruncForTimestampClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::TruncForDatePrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return TruncForDatePrepareImpl(ctx, scope);
}

DateVal UdfBuiltins::TruncForDate(FunctionContext* context, const DateVal& dv,
    const StringVal &unit_str) {
  return TruncForDateImpl(context, dv, unit_str);
}

void UdfBuiltins::TruncForDateClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::DateTruncForTimestampPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return DateTruncForTimestampPrepareImpl(ctx, scope);
}

TimestampVal UdfBuiltins::DateTruncForTimestamp(FunctionContext* context,
    const StringVal &unit_str, const TimestampVal& tv) {
  return DateTruncForTimestampImpl(context, unit_str, tv);
}

void UdfBuiltins::DateTruncForTimestampClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::DateTruncForDatePrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return DateTruncForDatePrepareImpl(ctx, scope);
}

DateVal UdfBuiltins::DateTruncForDate(FunctionContext* context, const StringVal &unit_str,
    const DateVal& dv) {
  return DateTruncForDateImpl(context, unit_str, dv);
}

void UdfBuiltins::DateTruncForDateClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::ExtractForTimestampPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  ExtractForTimestampPrepareImpl(ctx, scope);
}

BigIntVal UdfBuiltins::ExtractForTimestamp(FunctionContext* ctx, const TimestampVal& tv,
    const StringVal& unit_str) {
  return ExtractForTimestampImpl(ctx, tv, unit_str);
}

void UdfBuiltins::ExtractForTimestampClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::ExtractForDatePrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  ExtractForDatePrepareImpl(ctx, scope);
}

BigIntVal UdfBuiltins::ExtractForDate(FunctionContext* ctx, const DateVal& dv,
    const StringVal& unit_str) {
  return ExtractForDateImpl(ctx, dv, unit_str);
}

void UdfBuiltins::ExtractForDateClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::DatePartForTimestampPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  DatePartForTimestampPrepareImpl(ctx, scope);
}

BigIntVal UdfBuiltins::DatePartForTimestamp(FunctionContext* ctx,
    const StringVal& unit_str, const TimestampVal& tv) {
  return DatePartForTimestampImpl(ctx, unit_str, tv);
}

void UdfBuiltins::DatePartForTimestampClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}

void UdfBuiltins::DatePartForDatePrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  DatePartForDatePrepareImpl(ctx, scope);
}

BigIntVal UdfBuiltins::DatePartForDate(FunctionContext* ctx, const StringVal& unit_str,
    const DateVal& dv) {
  return DatePartForDateImpl(ctx, unit_str, dv);
}

void UdfBuiltins::DatePartForDateClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return CloseImpl(ctx, scope);
}
