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

#include "exprs/aggregate-functions.h"

#include <math.h>
#include <algorithm>
#include <map>
#include <sstream>
#include <utility>

#include <boost/random/ranlux.hpp>
#include <boost/random/uniform_int.hpp>

#include "codegen/impala-ir.h"
#include "common/logging.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "exprs/anyval-util.h"
#include "exprs/hll-bias.h"

#include "common/names.h"

using boost::uniform_int;
using boost::ranlux64_3;
using std::make_pair;
using std::map;
using std::nth_element;
using std::pop_heap;
using std::push_heap;

namespace {
// Threshold for each precision where it's better to use linear counting instead
// of the bias corrected estimate.
static float HllThreshold(int p) {
  switch (p) {
    case 4:
      return 10.0;
    case 5:
      return 20.0;
    case 6:
      return 40.0;
    case 7:
      return 80.0;
    case 8:
      return 220.0;
    case 9:
      return 400.0;
    case 10:
      return 900.0;
    case 11:
      return 1800.0;
    case 12:
      return 3100.0;
    case 13:
      return 6500.0;
    case 14:
      return 11500.0;
    case 15:
      return 20000.0;
    case 16:
      return 50000.0;
    case 17:
      return 120000.0;
    case 18:
      return 350000.0;
  }
  return 0.0;
}

// Implements k nearest neighbor interpolation for k=6,
// we choose 6 bassed on the HLL++ paper
int64_t HllEstimateBias(int64_t estimate) {
  const size_t K = 6;

  // Precision index into data arrays
  // We don't have data for precisions less than 4
  DCHECK(impala::AggregateFunctions::HLL_PRECISION >= 4);
  static constexpr size_t idx = impala::AggregateFunctions::HLL_PRECISION - 4;

  // Calculate the square of the difference of this estimate to all
  // precalculated estimates for a particular precision
  map<double, size_t> distances;
  for (size_t i = 0;
      i < impala::HLL_DATA_SIZES[idx] / sizeof(double); ++i) {
    double val = estimate - impala::HLL_RAW_ESTIMATE_DATA[idx][i];
    distances.insert(make_pair(val * val, i));
  }

  size_t nearest[K];
  size_t j = 0;
  // Use a sorted map to find the K closest estimates to our initial estimate
  for (map<double, size_t>::iterator it = distances.begin();
       j < K && it != distances.end(); ++it, ++j) {
    nearest[j] = it->second;
  }

  // Compute the average bias correction the K closest estimates
  double bias = 0.0;
  for (size_t i = 0; i < K; ++i) {
    bias += impala::HLL_BIAS_DATA[idx][nearest[i]];
  }

  return bias / K;
}

}

namespace impala {

// This function initializes StringVal 'dst' with a newly allocated buffer of
// 'buf_len' bytes. The new buffer will be filled with zero. If allocation fails,
// 'dst' will be set to a null string. This allows execution to continue until the
// next time GetQueryStatus() is called (see IMPALA-2756).
static void AllocBuffer(FunctionContext* ctx, StringVal* dst, size_t buf_len) {
  uint8_t* ptr = ctx->Allocate(buf_len);
  if (UNLIKELY(ptr == NULL && buf_len != 0)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    *dst = StringVal::null();
  } else {
    *dst = StringVal(ptr, buf_len);
    // Avoid memset() with NULL ptr as it's undefined.
    if (LIKELY(ptr != NULL)) memset(ptr, 0, buf_len);
  }
}

// This function initializes StringVal 'dst' with a newly allocated buffer of
// 'buf_len' bytes and copies the content of StringVal 'src' into it.
// If allocation fails, 'dst' will be set to a null string.
static void CopyStringVal(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) {
    *dst = StringVal::null();
  } else {
    uint8_t* copy = ctx->Allocate(src.len);
    if (UNLIKELY(copy == NULL)) {
      // Zero-length allocation always returns a hard-coded pointer.
      DCHECK(src.len != 0 && !ctx->impl()->state()->GetQueryStatus().ok());
      *dst = StringVal::null();
    } else {
      *dst = StringVal(copy, src.len);
      memcpy(dst->ptr, src.ptr, src.len);
    }
  }
}

// Converts any UDF Val Type to a string representation
template <typename T>
StringVal ToStringVal(FunctionContext* context, T val) {
  stringstream ss;
  ss << val;
  const string &str = ss.str();
  return StringVal::CopyFrom(
      context, reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
}

constexpr int AggregateFunctions::HLL_PRECISION;
constexpr int AggregateFunctions::HLL_LEN;

void AggregateFunctions::InitNull(FunctionContext*, AnyVal* dst) {
  dst->is_null = true;
}

template<typename T>
void AggregateFunctions::InitZero(FunctionContext*, T* dst) {
  dst->is_null = false;
  dst->val = 0;
}

template<>
void AggregateFunctions::InitZero(FunctionContext*, DecimalVal* dst) {
  dst->is_null = false;
  dst->val16 = 0; // Also initializes val4 and val8 to 0.
}

template <typename T>
void AggregateFunctions::UpdateVal(FunctionContext* ctx, const T& src, T* dst) {
  *dst = src;
}

template <>
void AggregateFunctions::UpdateVal(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  if (src.is_null) {
    if (!dst->is_null) ctx->Free(dst->ptr);
    *dst = StringVal::null();
    return;
  }

  uint8_t* new_ptr;
  if (dst->is_null) {
    new_ptr = ctx->Allocate(src.len);
  } else {
    new_ptr = ctx->Reallocate(dst->ptr, src.len);
  }
  // Note that a zero-length string is not the same as StringVal::null().
  RETURN_IF_NULL(ctx, new_ptr);
  dst->ptr = new_ptr;
  memcpy(dst->ptr, src.ptr, src.len);
  dst->is_null = false;
  dst->len = src.len;
}

StringVal AggregateFunctions::StringValGetValue(
    FunctionContext* ctx, const StringVal& src) {
  if (src.is_null) return src;
  return StringVal::CopyFrom(ctx, src.ptr, src.len);
}

StringVal AggregateFunctions::StringValSerializeOrFinalize(
    FunctionContext* ctx, const StringVal& src) {
  StringVal result = StringValGetValue(ctx, src);
  if (!src.is_null) ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::CountUpdate(
    FunctionContext*, const AnyVal& src, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src.is_null) ++dst->val;
}

void AggregateFunctions::CountStarUpdate(FunctionContext*, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  ++dst->val;
}

void AggregateFunctions::CountRemove(
    FunctionContext*, const AnyVal& src, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src.is_null) {
    --dst->val;
    DCHECK_GE(dst->val, 0);
  }
}

void AggregateFunctions::CountStarRemove(FunctionContext*, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  --dst->val;
  DCHECK_GE(dst->val, 0);
}

void AggregateFunctions::CountMerge(FunctionContext*, const BigIntVal& src,
    BigIntVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK(!src.is_null);
  dst->val += src.val;
}

struct AvgState {
  double sum;
  int64_t count;
};

void AggregateFunctions::AvgInit(FunctionContext* ctx, StringVal* dst) {
  // avg() uses a preallocated FIXED_UDA_INTERMEDIATE intermediate value.
  DCHECK_EQ(dst->len, sizeof(AvgState));
  AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
  avg->sum = 0.0;
  avg->count = 0;
}

template <typename T>
void AggregateFunctions::AvgUpdate(FunctionContext* ctx, const T& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(AvgState), dst->len);
  AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
  avg->sum += src.val;
  ++avg->count;
}

template <typename T>
void AggregateFunctions::AvgRemove(FunctionContext* ctx, const T& src, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0.
  if (src.is_null) return;
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(AvgState), dst->len);
  AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
  avg->sum -= src.val;
  --avg->count;
  DCHECK_GE(avg->count, 0);
}

void AggregateFunctions::AvgMerge(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  const AvgState* src_struct = reinterpret_cast<const AvgState*>(src.ptr);
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(AvgState), dst->len);
  AvgState* dst_struct = reinterpret_cast<AvgState*>(dst->ptr);
  dst_struct->sum += src_struct->sum;
  dst_struct->count += src_struct->count;
}

DoubleVal AggregateFunctions::AvgGetValue(FunctionContext* ctx, const StringVal& src) {
  AvgState* val_struct = reinterpret_cast<AvgState*>(src.ptr);
  if (val_struct->count == 0) return DoubleVal::null();
  return DoubleVal(val_struct->sum / val_struct->count);
}

DoubleVal AggregateFunctions::AvgFinalize(FunctionContext* ctx, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return DoubleVal::null();
  DoubleVal result = AvgGetValue(ctx, src);
  return result;
}

void AggregateFunctions::TimestampAvgUpdate(FunctionContext* ctx,
    const TimestampVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(AvgState), dst->len);
  AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
  const TimestampValue& tm_src = TimestampValue::FromTimestampVal(src);
  double val;
  if (tm_src.ToSubsecondUnixTime(&val)) {
    avg->sum += val;
    ++avg->count;
  }
}

void AggregateFunctions::TimestampAvgRemove(FunctionContext* ctx,
    const TimestampVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(AvgState), dst->len);
  AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
  const TimestampValue& tm_src = TimestampValue::FromTimestampVal(src);
  double val;
  if (tm_src.ToSubsecondUnixTime(&val)) {
    avg->sum -= val;
    --avg->count;
    DCHECK_GE(avg->count, 0);
  }
}

TimestampVal AggregateFunctions::TimestampAvgGetValue(FunctionContext* ctx,
    const StringVal& src) {
  AvgState* val_struct = reinterpret_cast<AvgState*>(src.ptr);
  if (val_struct->count == 0) return TimestampVal::null();
  const TimestampValue& tv = TimestampValue::FromSubsecondUnixTime(
      val_struct->sum / val_struct->count);
  if (tv.HasDate()) {
    TimestampVal result;
    tv.ToTimestampVal(&result);
    return result;
  } else {
    return TimestampVal::null();
  }
}

TimestampVal AggregateFunctions::TimestampAvgFinalize(FunctionContext* ctx,
    const StringVal& src) {
  if (UNLIKELY(src.is_null)) return TimestampVal::null();
  TimestampVal result = TimestampAvgGetValue(ctx, src);
  return result;
}

struct DecimalAvgState {
  __int128_t sum_val16; // Always uses max precision decimal.
  int64_t count;
};

void AggregateFunctions::DecimalAvgInit(FunctionContext* ctx, StringVal* dst) {
  // avg() uses a preallocated FIXED_UDA_INTERMEDIATE intermediate value.
  DCHECK_EQ(dst->len, sizeof(DecimalAvgState));
  DecimalAvgState* avg = reinterpret_cast<DecimalAvgState*>(dst->ptr);
  avg->sum_val16 = 0;
  avg->count = 0;
}

void AggregateFunctions::DecimalAvgUpdate(FunctionContext* ctx, const DecimalVal& src,
    StringVal* dst) {
  DecimalAvgAddOrRemove(ctx, src, dst, false);
}

void AggregateFunctions::DecimalAvgRemove(FunctionContext* ctx, const DecimalVal& src,
    StringVal* dst) {
  DecimalAvgAddOrRemove(ctx, src, dst, true);
}

// Always inline in IR so that constants can be replaced.
IR_ALWAYS_INLINE void AggregateFunctions::DecimalAvgAddOrRemove(FunctionContext* ctx,
    const DecimalVal& src, StringVal* dst, bool remove) {
  if (src.is_null) return;
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(DecimalAvgState), dst->len);
  DecimalAvgState* avg = reinterpret_cast<DecimalAvgState*>(dst->ptr);

  // Since the src and dst are guaranteed to be the same scale, we can just
  // do a simple add.
  int m = remove ? -1 : 1;
  switch (ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0)) {
    case 4:
      avg->sum_val16 += m * src.val4;
      break;
    case 8:
      avg->sum_val16 += m * src.val8;
      break;
    case 16:
      avg->sum_val16 += m * src.val16;
      break;
    default:
      DCHECK(false) << "Invalid byte size";
  }
  if (remove) {
    --avg->count;
    DCHECK_GE(avg->count, 0);
  } else {
    ++avg->count;
  }
}

void AggregateFunctions::DecimalAvgMerge(FunctionContext* ctx,
    const StringVal& src, StringVal* dst) {
  const DecimalAvgState* src_struct =
      reinterpret_cast<const DecimalAvgState*>(src.ptr);
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(DecimalAvgState), dst->len);
  DecimalAvgState* dst_struct = reinterpret_cast<DecimalAvgState*>(dst->ptr);
  dst_struct->sum_val16 += src_struct->sum_val16;
  dst_struct->count += src_struct->count;
}

DecimalVal AggregateFunctions::DecimalAvgGetValue(FunctionContext* ctx,
    const StringVal& src) {
  DecimalAvgState* val_struct = reinterpret_cast<DecimalAvgState*>(src.ptr);
  if (val_struct->count == 0) return DecimalVal::null();
  Decimal16Value sum(val_struct->sum_val16);
  Decimal16Value count(val_struct->count);

  int output_precision =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_PRECISION);
  int output_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SCALE);
  // The scale of the accumulated sum is set to the scale of the input type.
  int sum_scale = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SCALE, 0);
  bool is_nan = false;
  bool overflow = false;
  bool round = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  Decimal16Value result = sum.Divide<int128_t>(sum_scale, count, 0 /* count's scale */,
      output_precision, output_scale, round, &is_nan, &overflow);
  if (UNLIKELY(is_nan)) return DecimalVal::null();
  if (UNLIKELY(overflow)) {
    ctx->AddWarning("Avg computation overflowed, returning NULL");
    return DecimalVal::null();
  }
  return DecimalVal(result.value());
}

DecimalVal AggregateFunctions::DecimalAvgFinalize(FunctionContext* ctx,
    const StringVal& src) {
  if (UNLIKELY(src.is_null)) return DecimalVal::null();
  DecimalVal result = DecimalAvgGetValue(ctx, src);
  return result;
}

template<typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::SumUpdate(FunctionContext* ctx, const SRC_VAL& src,
    DST_VAL* dst) {
  if (src.is_null) {
    // Do not count null values towards the number of updates
    ctx->impl()->IncrementNumUpdates(-1);
    return;
  }
  if (dst->is_null) InitZero<DST_VAL>(ctx, dst);
  dst->val += src.val;
}

template<typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::SumRemove(FunctionContext* ctx, const SRC_VAL& src,
    DST_VAL* dst) {
  // Do not count null values towards the number of removes
  if (src.is_null) ctx->impl()->IncrementNumRemoves(-1);
  if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
    *dst = DST_VAL::null();
    return;
  }
  if (src.is_null) return;
  if (dst->is_null) InitZero<DST_VAL>(ctx, dst);
  dst->val -= src.val;
}

void AggregateFunctions::SumDecimalUpdate(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  SumDecimalAddOrSubtract(ctx, src, dst);
}

void AggregateFunctions::SumDecimalRemove(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
    *dst = DecimalVal::null();
    return;
  }
  SumDecimalAddOrSubtract(ctx, src, dst, true);
}

// Always inline in IR so that constants can be replaced.
IR_ALWAYS_INLINE void AggregateFunctions::SumDecimalAddOrSubtract(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst, bool subtract) {
  if (src.is_null) return;
  if (dst->is_null) InitZero<DecimalVal>(ctx, dst);
  int precision = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  // Since the src and dst are guaranteed to be the same scale, we can just
  // do a simple add.
  int m = subtract ? -1 : 1;
  if (precision <= 9) {
    dst->val16 += m * src.val4;
  } else if (precision <= 19) {
    dst->val16 += m * src.val8;
  } else {
    dst->val16 += m * src.val16;
  }
}

void AggregateFunctions::SumDecimalMerge(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) InitZero<DecimalVal>(ctx, dst);
  dst->val16 += src.val16;
}

template<typename T>
void AggregateFunctions::Min(FunctionContext*, const T& src, T* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val < dst->val) *dst = src;
}

template<typename T>
void AggregateFunctions::Max(FunctionContext*, const T& src, T* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val > dst->val) *dst = src;
}

void AggregateFunctions::InitNullString(FunctionContext* c, StringVal* dst) {
  dst->is_null = true;
  dst->ptr = NULL;
  dst->len = 0;
}

template<>
void AggregateFunctions::Min(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  if (dst->is_null ||
      StringValue::FromStringVal(src) < StringValue::FromStringVal(*dst)) {
    if (!dst->is_null) ctx->Free(dst->ptr);
    CopyStringVal(ctx, src, dst);
  }
}

template<>
void AggregateFunctions::Max(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  if (dst->is_null ||
      StringValue::FromStringVal(src) > StringValue::FromStringVal(*dst)) {
    if (!dst->is_null) ctx->Free(dst->ptr);
    CopyStringVal(ctx, src, dst);
  }
}

template<>
void AggregateFunctions::Min(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  int precision = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  if (precision <= 9) {
    if (dst->is_null || src.val4 < dst->val4) *dst = src;
  } else if (precision <= 19) {
    if (dst->is_null || src.val8 < dst->val8) *dst = src;
  } else {
    if (dst->is_null || src.val16 < dst->val16) *dst = src;
  }
}

template<>
void AggregateFunctions::Max(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  int precision = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_PRECISION, 0);
  if (precision <= 9) {
    if (dst->is_null || src.val4 > dst->val4) *dst = src;
  } else if (precision <= 19) {
    if (dst->is_null || src.val8 > dst->val8) *dst = src;
  } else {
    if (dst->is_null || src.val16 > dst->val16) *dst = src;
  }
}

template<>
void AggregateFunctions::Min(FunctionContext*,
    const TimestampVal& src, TimestampVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) {
    *dst = src;
    return;
  }
  TimestampValue src_tv = TimestampValue::FromTimestampVal(src);
  TimestampValue dst_tv = TimestampValue::FromTimestampVal(*dst);
  if (src_tv < dst_tv) *dst = src;
}

template<>
void AggregateFunctions::Max(FunctionContext*,
    const TimestampVal& src, TimestampVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) {
    *dst = src;
    return;
  }
  TimestampValue src_tv = TimestampValue::FromTimestampVal(src);
  TimestampValue dst_tv = TimestampValue::FromTimestampVal(*dst);
  if (src_tv > dst_tv) *dst = src;
}

// StringConcat intermediate state starts with the length of the first
// separator, followed by the accumulated string.  The accumulated
// string starts with the separator of the first value that arrived in
// StringConcatUpdate().
typedef int StringConcatHeader;

// Delimiter to use if the separator is not provided.
static inline StringVal ALWAYS_INLINE DefaultStringConcatDelim() {
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(", ")), 2);
}

void AggregateFunctions::StringConcatUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* result) {
  StringConcatUpdate(ctx, src, DefaultStringConcatDelim(), result);
}

void AggregateFunctions::StringConcatUpdate(FunctionContext* ctx, const StringVal& src,
    const StringVal& separator, StringVal* result) {
  if (src.is_null) return;
  const StringVal default_delim = DefaultStringConcatDelim();
  const StringVal* sep = separator.is_null ? &default_delim : &separator;
  if (result->is_null) {
    // Header of the intermediate state holds the length of the first separator.
    const int header_len = sizeof(StringConcatHeader);
    DCHECK(header_len == sizeof(sep->len));
    AllocBuffer(ctx, result, header_len);
    if (UNLIKELY(result->is_null)) {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
      return;
    }
    *reinterpret_cast<StringConcatHeader*>(result->ptr) = sep->len;
  }
  unsigned new_len = result->len + sep->len + src.len;
  if (LIKELY(new_len <= StringVal::MAX_LENGTH)) {
    uint8_t* ptr = ctx->Reallocate(result->ptr, new_len);
    if (LIKELY(ptr != NULL)) {
      memcpy(ptr + result->len, sep->ptr, sep->len);
      memcpy(ptr + result->len + sep->len, src.ptr, src.len);
      result->ptr = ptr;
      result->len = new_len;
    } else {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    }
  } else {
    ctx->SetError("Concatenated string length larger than allowed limit of "
        "1 GB character data.");
  }
}

void AggregateFunctions::StringConcatMerge(FunctionContext* ctx,
    const StringVal& src, StringVal* result) {
  if (src.is_null) return;
  const int header_len = sizeof(StringConcatHeader);
  if (result->is_null) {
    AllocBuffer(ctx, result, header_len);
    if (UNLIKELY(result->is_null)) {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
      return;
    }
    // Copy the header from the first intermediate value.
    *reinterpret_cast<StringConcatHeader*>(result->ptr) =
        *reinterpret_cast<StringConcatHeader*>(src.ptr);
  }
  // Append the string portion of the intermediate src to result (omit src's header).
  unsigned buf_len = src.len - header_len;
  unsigned new_len = result->len + buf_len;
  if (LIKELY(new_len <= StringVal::MAX_LENGTH)) {
    uint8_t* ptr = ctx->Reallocate(result->ptr, new_len);
    if (LIKELY(ptr != NULL)) {
      memcpy(ptr + result->len, src.ptr + header_len, buf_len);
      result->ptr = ptr;
      result->len = new_len;
    } else {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    }
  } else {
    ctx->SetError("Concatenated string length larger than allowed limit of "
        "1 GB character data.");
  }
}

StringVal AggregateFunctions::StringConcatFinalize(FunctionContext* ctx,
    const StringVal& src) {
  if (UNLIKELY(src.is_null)) return src;
  const int header_len = sizeof(StringConcatHeader);
  DCHECK(src.len >= header_len);
  int sep_len = *reinterpret_cast<StringConcatHeader*>(src.ptr);
  DCHECK(src.len >= header_len + sep_len);
  // Remove the header and the first separator.
  StringVal result = StringVal::CopyFrom(ctx, src.ptr + header_len + sep_len,
      src.len - header_len - sep_len);
  ctx->Free(src.ptr);
  return result;
}

// Compute distinctpc and distinctpcsa using Flajolet and Martin's algorithm
// (Probabilistic Counting Algorithms for Data Base Applications)
// We have implemented two variants here: one with stochastic averaging (with PCSA
// postfix) and one without.
// There are 4 phases to compute the aggregate:
//   1. allocate a bitmap, stored in the aggregation tuple's output string slot
//   2. update the bitmap per row (UpdateDistinctEstimateSlot)
//   3. for distributed plan, merge the bitmaps from all the nodes
//      (UpdateMergeEstimateSlot)
//   4. compute the estimate using the bitmaps when all the rows are processed
//      (FinalizeEstimateSlot)
const static int NUM_PC_BITMAPS = 64; // number of bitmaps
const static int PC_BITMAP_LENGTH = 32; // the length of each bit map
const static float PC_THETA = 0.77351f; // the magic number to compute the final result
const static float PC_K = -1.75f; // the magic correction for low cardinalities

// Size of the distinct estimate bit map - Probabilistic Counting Algorithms for Data
// Base Applications (Flajolet and Martin)
//
// The bitmap is a 64bit(1st index) x 32bit(2nd index) matrix.
// So, the string length of 256 byte is enough.
// The layout is:
//   row  1: 8bit 8bit 8bit 8bit
//   row  2: 8bit 8bit 8bit 8bit
//   ...     ..
//   ...     ..
//   row 64: 8bit 8bit 8bit 8bit
//
// Using 32bit length, we can count up to 10^8. This will not be enough for Fact table
// primary key, but once we approach the limit, we could interpret the result as
// "every row is distinct".
const static int PC_INTERMEDIATE_BYTES = NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8;

void AggregateFunctions::PcInit(FunctionContext* c, StringVal* dst) {
  // The distinctpc*() functions use a preallocated FIXED_UDA_INTERMEDIATE intermediate
  // value.
  DCHECK_EQ(dst->len, PC_INTERMEDIATE_BYTES);
  memset(dst->ptr, 0, PC_INTERMEDIATE_BYTES);
}

static inline void SetDistinctEstimateBit(uint8_t* bitmap,
    uint32_t row_index, uint32_t bit_index) {
  // We need to convert Bitmap[alpha,index] into the index of the string.
  // alpha tells which of the 32bit we've to jump to.
  // index then lead us to the byte and bit.
  uint32_t *int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
  int_bitmap[row_index] |= (1 << bit_index);
}

static inline bool GetDistinctEstimateBit(uint8_t* bitmap,
    uint32_t row_index, uint32_t bit_index) {
  uint32_t *int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
  return ((int_bitmap[row_index] & (1 << bit_index)) > 0);
}

template<typename T>
void AggregateFunctions::PcUpdate(FunctionContext* c, const T& input, StringVal* dst) {
  DCHECK_EQ(dst->len, PC_INTERMEDIATE_BYTES);
  if (input.is_null) return;
  // Core of the algorithm. This is a direct translation of the code in the paper.
  // Please see the paper for details. For simple averaging, we need to compute hash
  // values NUM_PC_BITMAPS times using NUM_PC_BITMAPS different hash functions (by using a
  // different seed).
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    uint32_t hash_value = AnyValUtil::Hash(input, *c->GetArgType(0), i);
    const int bit_index = BitUtil::CountTrailingZeros(hash_value, PC_BITMAP_LENGTH - 1);
    // Set bitmap[i, bit_index] to 1
    SetDistinctEstimateBit(dst->ptr, i, bit_index);
  }
}

template<typename T>
void AggregateFunctions::PcsaUpdate(FunctionContext* c, const T& input, StringVal* dst) {
  DCHECK_EQ(dst->len, PC_INTERMEDIATE_BYTES);
  if (input.is_null) return;

  // Core of the algorithm. This is a direct translation of the code in the paper.
  // Please see the paper for details. Using stochastic averaging, we only need to
  // the hash value once for each row.
  uint32_t hash_value = AnyValUtil::Hash(input, *c->GetArgType(0), 0);
  uint32_t row_index = hash_value % NUM_PC_BITMAPS;

  // We want the zero-based position of the least significant 1-bit in binary
  // representation of hash_value. BitUtil::CountTrailingZeros(x,y) does exactly this
  // because it returns the number of trailing 0-bits in x (or y if x is zero).
  const int bit_index =
      BitUtil::CountTrailingZeros(hash_value / NUM_PC_BITMAPS, PC_BITMAP_LENGTH - 1);

  // Set bitmap[row_index, bit_index] to 1
  SetDistinctEstimateBit(dst->ptr, row_index, bit_index);
}

string DistinctEstimateBitMapToString(uint8_t* v) {
  stringstream debugstr;
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    for (int j = 0; j < PC_BITMAP_LENGTH; ++j) {
      // print bitmap[i][j]
      debugstr << GetDistinctEstimateBit(v, i, j);
    }
    debugstr << "\n";
  }
  debugstr << "\n";
  return debugstr.str();
}

void AggregateFunctions::PcMerge(FunctionContext* c,
    const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(src.len, PC_INTERMEDIATE_BYTES);
  DCHECK_EQ(dst->len, PC_INTERMEDIATE_BYTES);

  // Merge the bits
  // I think _mm_or_ps can do it, but perf doesn't really matter here. We call this only
  // once group per node.
  for (int i = 0; i < PC_INTERMEDIATE_BYTES; ++i) {
    *(dst->ptr + i) |= *(src.ptr + i);
  }

  VLOG_ROW << "UpdateMergeEstimateSlot Src Bit map:\n"
           << DistinctEstimateBitMapToString(src.ptr);
  VLOG_ROW << "UpdateMergeEstimateSlot Dst Bit map:\n"
           << DistinctEstimateBitMapToString(dst->ptr);
}

static double DistinctEstimateFinalize(const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, PC_INTERMEDIATE_BYTES);
  VLOG_ROW << "FinalizeEstimateSlot Bit map:\n"
           << DistinctEstimateBitMapToString(src.ptr);

  // We haven't processed any rows if none of the bits are set. Therefore, we have zero
  // distinct rows. We're overwriting the result in the same string buffer we've
  // allocated.
  bool is_empty = true;
  for (int i = 0; i < PC_INTERMEDIATE_BYTES; ++i) {
    if (src.ptr[i] != 0) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) return 0;

  // Convert the bitmap to a number, please see the paper for details
  // In short, we count the average number of leading 1s (per row) in the bit map.
  // The number is proportional to the log2(1/NUM_PC_BITMAPS of  the actual number of
  // distinct).
  // To get the actual number of distinct, we'll do 2^avg / PC_THETA.
  // PC_THETA is a magic number.
  int sum = 0;
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    int row_bit_count = 0;
    // Count the number of leading ones for each row in the bitmap
    // We could have used the build in __builtin_clz to count of number of leading zeros
    // but we first need to invert the 1 and 0.
    while (GetDistinctEstimateBit(src.ptr, i, row_bit_count) &&
        row_bit_count < PC_BITMAP_LENGTH) {
      ++row_bit_count;
    }
    sum += row_bit_count;
  }
  double avg = static_cast<double>(sum) / static_cast<double>(NUM_PC_BITMAPS);
  // We apply a correction for small cardinalities based on equation (6) from
  // Scheuermann et al DialM-POMC '07 so the above equation becomes
  // (2^avg - 2^PC_K*avg) / PC_THETA
  double result = (pow(static_cast<double>(2), avg) -
                   pow(static_cast<double>(2), avg * PC_K)) / PC_THETA;
  return result;
}

BigIntVal AggregateFunctions::PcFinalize(FunctionContext* c, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return BigIntVal::null();
  double estimate = DistinctEstimateFinalize(src);
  return static_cast<int64_t>(estimate);
}

BigIntVal AggregateFunctions::PcsaFinalize(FunctionContext* c, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return BigIntVal::null();
  // When using stochastic averaging, the result has to be multiplied by NUM_PC_BITMAPS.
  double estimate = DistinctEstimateFinalize(src) * NUM_PC_BITMAPS;
  return static_cast<int64_t>(estimate);
}

// Histogram constants
// TODO: Expose as constant argument parameters to the UDA.
const static int NUM_BUCKETS = 100;
const static int NUM_SAMPLES_PER_BUCKET = 200;

template <typename T>
struct ReservoirSample {
  // Sample value
  T val;
  // Key on which the samples are sorted.
  double key;

  ReservoirSample() : key(-1) { }
  ReservoirSample(const T& val) : val(val), key(-1) { }

  // Gets a copy of the sample value that allocates memory from ctx, if necessary.
  T GetValue(FunctionContext* ctx) { return val; }
};

// Maximum length of a string sample.
const static int MAX_STRING_SAMPLE_LEN = 10;

// Template specialization for StringVal because we do not store the StringVal itself.
// Instead, we keep fixed size arrays and truncate longer strings if necessary.
template <>
struct ReservoirSample<StringVal> {
  uint8_t val[MAX_STRING_SAMPLE_LEN];
  int len; // Size of string (up to MAX_STRING_SAMPLE_LEN)
  double key;

  ReservoirSample() : len(0), key(-1) { }

  ReservoirSample(const StringVal& string_val) : key(-1) {
    len = min(string_val.len, MAX_STRING_SAMPLE_LEN);
    memcpy(&val[0], string_val.ptr, len);
  }

  // Gets a copy of the sample value that allocates memory from ctx, if necessary.
  StringVal GetValue(FunctionContext* ctx) {
    return StringVal::CopyFrom(ctx, &val[0], len);
  }
};

template <typename T>
bool SampleValLess(const ReservoirSample<T>& i, const ReservoirSample<T>& j) {
  return i.val.val < j.val.val;
}

template <>
bool SampleValLess(const ReservoirSample<StringVal>& i,
    const ReservoirSample<StringVal>& j) {
  int n = min(i.len, j.len);
  int result = memcmp(&i.val[0], &j.val[0], n);
  if (result == 0) return i.len < j.len;
  return result < 0;
}

template <>
bool SampleValLess(const ReservoirSample<DecimalVal>& i,
    const ReservoirSample<DecimalVal>& j) {
  // Also handles val4 and val8 - the DecimalVal memory layout ensures the least
  // significant bits overlap in memory.
  return i.val.val16 < j.val.val16;
}

template <>
bool SampleValLess(const ReservoirSample<TimestampVal>& i,
    const ReservoirSample<TimestampVal>& j) {
  if (i.val.date == j.val.date) return i.val.time_of_day < j.val.time_of_day;
  else return i.val.date < j.val.date;
}

template <typename T>
bool SampleKeyGreater(const ReservoirSample<T>& i, const ReservoirSample<T>& j) {
  return i.key > j.key;
}

// Keeps track of the current state of the reservoir sampling algorithm. The samples are
// stored in a dynamically sized array. Initially, the the samples array is stored in a
// separate memory allocation. This class is responsible for managing the memory of the
// array and reallocating when the array is full. When this object is serialized into an
// output buffer, the samples array is inlined into the output buffer as well.
template <typename T>
class ReservoirSampleState {
 public:
  ReservoirSampleState(FunctionContext* ctx)
    : num_samples_(0),
      capacity_(INIT_CAPACITY),
      source_size_(0),
      sample_array_inline_(false),
      samples_(NULL) {
    // Allocate some initial memory for the samples array.
    size_t buffer_len = sizeof(ReservoirSample<T>) * capacity_;
    uint8_t* ptr = ctx->Allocate(buffer_len);
    if (ptr == NULL) {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
      return;
    }
    samples_ = reinterpret_cast<ReservoirSample<T>*>(ptr);
  }

  // Returns a pointer to a ReservoirSample at idx.
  ReservoirSample<T>* GetSample(int64_t idx) {
    DCHECK(samples_ != NULL);
    DCHECK_LT(idx, num_samples_);
    DCHECK_LE(num_samples_, capacity_);
    DCHECK_GE(idx, 0);
    return &samples_[idx];
  }

  // Adds a sample and increments the source size. Doubles the capacity of the sample
  // array if necessary. If max capacity is reached, randomly evicts a sample (as
  // required by the algorithm). Returns false if the attempt to double the capacity
  // fails, true otherwise.
  bool AddSample(FunctionContext* ctx, const ReservoirSample<T>& s) {
    DCHECK(samples_ != NULL);
    DCHECK_LE(num_samples_, MAX_CAPACITY);
    if (num_samples_ < MAX_CAPACITY) {
      if (num_samples_ == capacity_) {
        bool result = IncreaseCapacity(ctx, capacity_ * 2);
        if (!result) return false;
      }
      DCHECK_LT(num_samples_, capacity_);
      samples_[num_samples_++] = s;
    } else {
      DCHECK_EQ(num_samples_, MAX_CAPACITY);
      DCHECK(!sample_array_inline_);
      int64_t idx = GetNext64(source_size_);
      if (idx < MAX_CAPACITY) samples_[idx] = s;
    }
    ++source_size_;
    return true;
  }

  // Same as above.
  bool AddSample(FunctionContext* ctx, const T& s) {
    return AddSample(ctx, ReservoirSample<T>(s));
  }

  // Returns a buffer with a serialized ReservoirSampleState and the array of samples it
  // contains. The samples array must not be inlined; i.e. it must be in a separate memory
  // allocation. Returns a buffer containing this object and inlined samples array. The
  // memory containing this object and the samples array is freed. The serialized object
  // in the output buffer requires a call to Deserialize() before use.
  StringVal Serialize(FunctionContext* ctx) {
    DCHECK(samples_ != NULL);
    DCHECK(!sample_array_inline_);
    // Assign keys to the samples that haven't been set (i.e. if serializing after
    // Update()). In weighted reservoir sampling the keys are typically assigned as the
    // sources are being sampled, but this requires maintaining the samples in sorted
    // order (by key) and it accomplishes the same thing at this point because all data
    // points coming into Update() get the same weight. When the samples are later merged,
    // they do have different weights (set here) that are proportional to the source_size,
    // i.e. samples selected from a larger stream are more likely to end up in the final
    // sample set. In order to avoid the extra overhead in Update(), we approximate the
    // keys by picking random numbers in the range
    // [(SOURCE_SIZE - SAMPLE_SIZE)/(SOURCE_SIZE), 1]. This weights the keys by
    // SOURCE_SIZE and implies that the samples picked had the highest keys, because
    // values not sampled would have keys between 0 and
    // (SOURCE_SIZE - SAMPLE_SIZE)/(SOURCE_SIZE).
    for (int i = 0; i < num_samples_; ++i) {
      if (samples_[i].key >= 0) continue;
      int r = rand() % num_samples_;
      samples_[i].key = ((double) source_size_ - r) / source_size_;
    }
    capacity_ = num_samples_;
    sample_array_inline_ = true;

    size_t buffer_len = sizeof(ReservoirSampleState<T>) +
        sizeof(ReservoirSample<T>) * num_samples_;
    StringVal dst(ctx, buffer_len);
    if (LIKELY(!dst.is_null)) {
      memcpy(dst.ptr, reinterpret_cast<uint8_t*>(this), sizeof(ReservoirSampleState<T>));
      memcpy(dst.ptr + sizeof(ReservoirSampleState<T>),
          reinterpret_cast<uint8_t*>(samples_),
          sizeof(ReservoirSample<T>) * num_samples_);
    }
    ctx->Free(reinterpret_cast<uint8_t*>(samples_));
    ctx->Free(reinterpret_cast<uint8_t*>(this));
    return dst;
  }

  // Updates the pointer to the samples array. Must be called before using this object in
  // Merge().
  void Deserialize() {
    DCHECK(sample_array_inline_);
    samples_ = reinterpret_cast<ReservoirSample<T>*>(this + 1);
  }

  // Merges the samples in "other_state" into the current state by following the
  // reservoir sampling algorithm. If necessary, increases the capacity to fit the
  // samples from "other_state". In the case of failure to increase the size of the
  // array, returns.
  void Merge(FunctionContext* ctx, ReservoirSampleState<T>* other_state) {
    DCHECK(samples_ != NULL);
    DCHECK_GT(capacity_, 0);
    other_state->Deserialize();
    int src_idx = 0;
    // We can increase the capacity significantly here and skip several doublings because
    // we know the number of elements in the other state up front.
    if (capacity_ < MAX_CAPACITY) {
      int necessary_capacity = num_samples_ + other_state->num_samples();
      if (capacity_ < necessary_capacity) {
        bool result = IncreaseCapacity(ctx, necessary_capacity);
        if (!result) return;
      }
    }

    // First, fill up the dst samples if they don't already exist. The samples are now
    // ordered as a min-heap on the key.
    while (num_samples_ < MAX_CAPACITY && src_idx < other_state->num_samples()) {
      DCHECK_GE(other_state->GetSample(src_idx)->key, 0);
      bool result = AddSample(ctx, *other_state->GetSample(src_idx++));
      if (!result) return;
      push_heap(&samples_[0], &samples_[num_samples_], SampleKeyGreater<T>);
    }

    // Then for every sample from source, take the sample if the key is greater than
    // the minimum key in the min-heap.
    while (src_idx < other_state->num_samples()) {
      DCHECK_GE(other_state->GetSample(src_idx)->key, 0);
      if (other_state->GetSample(src_idx)->key > samples_[0].key) {
        pop_heap(&samples_[0], &samples_[num_samples_], SampleKeyGreater<T>);
        samples_[MAX_CAPACITY - 1] = *other_state->GetSample(src_idx);
        push_heap(&samples_[0], &samples_[num_samples_], SampleKeyGreater<T>);
      }
      ++src_idx;
    }

    source_size_ += other_state->source_size();
  }

  // Returns the median element.
  T GetMedian(FunctionContext* ctx) {
    if (num_samples_ == 0) return T::null();
    ReservoirSample<T>* mid_point = GetSample(num_samples_ / 2);
    nth_element(&samples_[0], mid_point, &samples_[num_samples_], SampleValLess<T>);
    return mid_point->GetValue(ctx);
  }

  // Sorts the samples.
  void SortSamples() {
    sort(&samples_[0], &samples_[num_samples_], SampleValLess<T>);
  }

  // Deletes this object by freeing the memory that contains the array of samples (if not
  // inlined) and itself.
  void Delete(FunctionContext* ctx) {
    if (!sample_array_inline_) ctx->Free(reinterpret_cast<uint8_t*>(samples_));
    ctx->Free(reinterpret_cast<uint8_t*>(this));
  }

  int num_samples() { return num_samples_; }
  int64_t source_size() { return source_size_; }

 private:
  // The initial capacity of the samples array.
  const static int INIT_CAPACITY = 16;

  // Maximum capacity of the samples array.
  const static int MAX_CAPACITY = NUM_BUCKETS * NUM_SAMPLES_PER_BUCKET;

  // Number of collected samples.
  int num_samples_;

  // Size of the "samples_" array.
  int capacity_;

  // Number of values over which the samples were collected.
  int64_t source_size_;

  // Random number generator for generating 64-bit integers
  // TODO: Replace with mt19937_64 when upgrading boost
  ranlux64_3 rng_;

  // True if the array of samples is in the same memory allocation as this object. If
  // false, this object is responsible for freeing the memory.
  bool sample_array_inline_;

  // Points to the array of ReservoirSamples. The array may be located inline (right after
  // this object), or in a separate memory allocation.
  ReservoirSample<T>* samples_;

  // Increases the capacity of the "samples_" array to "new_capacity" rounded up to a
  // power of two by reallocating. Should only be called if the samples array is not
  // inline. Returns false if the operation fails.
  bool IncreaseCapacity(FunctionContext* ctx, int new_capacity) {
    DCHECK(samples_ != NULL);
    DCHECK(!sample_array_inline_);
    DCHECK_LT(capacity_, MAX_CAPACITY);
    DCHECK_GT(new_capacity, capacity_);
    new_capacity = BitUtil::RoundUpToPowerOfTwo(new_capacity);
    if (new_capacity > MAX_CAPACITY) new_capacity = MAX_CAPACITY;
    size_t buffer_len = sizeof(ReservoirSample<T>) * new_capacity;
    uint8_t* ptr = ctx->Reallocate(reinterpret_cast<uint8_t*>(samples_), buffer_len);
    if (ptr == NULL) {
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
      return false;
    }
    samples_ = reinterpret_cast<ReservoirSample<T>*>(ptr);
    capacity_ = new_capacity;
    return true;
  }

  // Returns a random integer in the range [0, max].
  int64_t GetNext64(int64_t max) {
    uniform_int<int64_t> dist(0, max);
    return dist(rng_);
  }
};

template <typename T>
void AggregateFunctions::ReservoirSampleInit(FunctionContext* ctx, StringVal* dst) {
  AllocBuffer(ctx, dst, sizeof(ReservoirSampleState<T>));
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  ReservoirSampleState<T>* dst_state =
      reinterpret_cast<ReservoirSampleState<T>*>(dst->ptr);
  *dst_state = ReservoirSampleState<T>(ctx);
}

template <typename T>
void AggregateFunctions::ReservoirSampleUpdate(FunctionContext* ctx, const T& src,
    StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  ReservoirSampleState<T>* dst_state =
      reinterpret_cast<ReservoirSampleState<T>*>(dst->ptr);
  dst_state->AddSample(ctx, src);
}

template <typename T>
StringVal AggregateFunctions::ReservoirSampleSerialize(FunctionContext* ctx,
    const StringVal& src) {
  if (UNLIKELY(src.is_null)) return src;
  ReservoirSampleState<T>* src_state =
      reinterpret_cast<ReservoirSampleState<T>*>(src.ptr);
  StringVal result = src_state->Serialize(ctx);
  return result;
}

template <typename T>
void AggregateFunctions::ReservoirSampleMerge(FunctionContext* ctx,
    const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK(!src.is_null);
  ReservoirSampleState<T>* src_state =
      reinterpret_cast<ReservoirSampleState<T>*>(src.ptr);
  ReservoirSampleState<T>* dst_state =
      reinterpret_cast<ReservoirSampleState<T>*>(dst->ptr);
  dst_state->Merge(ctx, src_state);
}

template <typename T>
void PrintSample(const ReservoirSample<T>& v, ostream* os) { *os << v.val.val; }

template <>
void PrintSample(const ReservoirSample<TinyIntVal>& v, ostream* os) {
  *os << static_cast<int32_t>(v.val.val);
}

template <>
void PrintSample(const ReservoirSample<StringVal>& v, ostream* os) {
  string s(reinterpret_cast<const char*>(&v.val[0]), v.len);
  *os << s;
}

template <>
void PrintSample(const ReservoirSample<DecimalVal>& v, ostream* os) {
  // Also handles val4 and val8 - the DecimalVal memory layout ensures the least
  // significant bits overlap in memory.
  *os << v.val.val16;
}

template <>
void PrintSample(const ReservoirSample<TimestampVal>& v, ostream* os) {
  *os << TimestampValue::FromTimestampVal(v.val).ToString();
}

template <typename T>
StringVal AggregateFunctions::ReservoirSampleFinalize(FunctionContext* ctx,
    const StringVal& src) {
  if (UNLIKELY(src.is_null)) return src;
  ReservoirSampleState<T>* src_state =
      reinterpret_cast<ReservoirSampleState<T>*>(src.ptr);

  stringstream out;
  for (int i = 0; i < src_state->num_samples(); ++i) {
    PrintSample<T>(*src_state->GetSample(i), &out);
    if (i < (src_state->num_samples() - 1)) out << ", ";
  }
  const string& out_str = out.str();
  StringVal result_str = StringVal::CopyFrom(ctx,
      reinterpret_cast<const uint8_t*>(out_str.c_str()), out_str.size());
  src_state->Delete(ctx);
  return result_str;
}

template <typename T>
StringVal AggregateFunctions::HistogramFinalize(FunctionContext* ctx,
    const StringVal& src) {
  if (UNLIKELY(src.is_null)) return src;

  ReservoirSampleState<T>* src_state =
      reinterpret_cast<ReservoirSampleState<T>*>(src.ptr);
  src_state->SortSamples();

  stringstream out;
  int num_buckets = min(src_state->num_samples(), NUM_BUCKETS);
  int samples_per_bucket = max(src_state->num_samples() / NUM_BUCKETS, 1);
  for (int bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx) {
    int sample_idx = (bucket_idx + 1) * samples_per_bucket - 1;
    PrintSample<T>(*(src_state->GetSample(sample_idx)), &out);
    if (bucket_idx < (num_buckets - 1)) out << ", ";
  }
  const string& out_str = out.str();
  StringVal result_str = StringVal::CopyFrom(ctx,
      reinterpret_cast<const uint8_t*>(out_str.c_str()), out_str.size());
  src_state->Delete(ctx);
  return result_str;
}

template <typename T>
T AggregateFunctions::AppxMedianFinalize(FunctionContext* ctx, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return T::null();
  ReservoirSampleState<T>* src_state =
      reinterpret_cast<ReservoirSampleState<T>*>(src.ptr);
  T result = src_state->GetMedian(ctx);
  src_state->Delete(ctx);
  return result;
}

void AggregateFunctions::HllInit(FunctionContext* ctx, StringVal* dst) {
  // The HLL functions use a preallocated FIXED_UDA_INTERMEDIATE intermediate value.
  DCHECK_EQ(dst->len, HLL_LEN);
  memset(dst->ptr, 0, HLL_LEN);
}

template <typename T>
void AggregateFunctions::HllUpdate(FunctionContext* ctx, const T& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, HLL_LEN);
  uint64_t hash_value =
      AnyValUtil::Hash64(src, *ctx->GetArgType(0), HashUtil::FNV64_SEED);
  // Use the lower bits to index into the number of streams and then find the first 1 bit
  // after the index bits.
  int idx = hash_value & (HLL_LEN - 1);
  const uint8_t first_one_bit =
      1 + BitUtil::CountTrailingZeros(
              hash_value >> HLL_PRECISION, sizeof(hash_value) * CHAR_BIT - HLL_PRECISION);
  dst->ptr[idx] = ::max(dst->ptr[idx], first_one_bit);
}

// Specialize for DecimalVal to allow substituting decimal size.
template <>
void AggregateFunctions::HllUpdate(
    FunctionContext* ctx, const DecimalVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, HLL_LEN);
  int byte_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  uint64_t hash_value = AnyValUtil::HashDecimal64(src, byte_size, HashUtil::FNV64_SEED);
  if (hash_value != 0) {
    // Use the lower bits to index into the number of streams and then
    // find the first 1 bit after the index bits.
    int idx = hash_value & (HLL_LEN - 1);
    uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_PRECISION) + 1;
    dst->ptr[idx] = ::max(dst->ptr[idx], first_one_bit);
  }
}

void AggregateFunctions::HllMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK(!src.is_null);
  DCHECK_EQ(dst->len, HLL_LEN);
  DCHECK_EQ(src.len, HLL_LEN);
  for (int i = 0; i < src.len; ++i) {
    dst->ptr[i] = ::max(dst->ptr[i], src.ptr[i]);
  }
}

uint64_t AggregateFunctions::HllFinalEstimate(const uint8_t* buckets,
    int32_t num_buckets) {
  DCHECK(buckets != NULL);
  DCHECK_EQ(num_buckets, HLL_LEN);

  // Empirical constants for the algorithm.
  float alpha = 0;
  if (HLL_LEN == 16) {
    alpha = 0.673f;
  } else if (HLL_LEN == 32) {
    alpha = 0.697f;
  } else if (HLL_LEN == 64) {
    alpha = 0.709f;
  } else {
    alpha = 0.7213f / (1 + 1.079f / HLL_LEN);
  }

  float harmonic_mean = 0;
  int num_zero_registers = 0;
  // TODO: Consider improving this loop (e.g. replacing 'if' with arithmetic op).
  for (int i = 0; i < num_buckets; ++i) {
    harmonic_mean += powf(2.0f, -buckets[i]);
    if (buckets[i] == 0) ++num_zero_registers;
  }
  harmonic_mean = 1.0f / harmonic_mean;
  int64_t estimate = alpha * HLL_LEN * HLL_LEN * harmonic_mean;
  // Adjust for Hll bias based on Hll++ algorithm
  if (estimate <= 5 * HLL_LEN) {
    estimate -= HllEstimateBias(estimate);
  }

  if (num_zero_registers == 0) return estimate;

  // Estimated cardinality is too low. Hll is too inaccurate here, instead use
  // linear counting.
  int64_t h = HLL_LEN * log(static_cast<float>(HLL_LEN) / num_zero_registers);

  return (h <= HllThreshold(HLL_PRECISION)) ? h : estimate;
}

BigIntVal AggregateFunctions::HllFinalize(FunctionContext* ctx, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return BigIntVal::null();
  uint64_t estimate = HllFinalEstimate(src.ptr, src.len);
  return estimate;
}

// An implementation of a simple single pass variance algorithm. A standard UDA must
// be single pass (i.e. does not scan the table more than once), so the most canonical
// two pass approach is not practical.
struct KnuthVarianceState {
  double mean;
  double m2;
  int64_t count;
};

// Set pop=true for population variance, false for sample variance
static double ComputeKnuthVariance(const KnuthVarianceState& state, bool pop) {
  // Return zero for 1 tuple specified by
  // http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions212.htm
  if (state.count == 1) return 0.0;
  if (pop) return state.m2 / state.count;
  return state.m2 / (state.count - 1);
}

void AggregateFunctions::KnuthVarInit(FunctionContext* ctx, StringVal* dst) {
  // The Knuth variance functions use a preallocated FIXED_UDA_INTERMEDIATE intermediate
  // value.
  DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
  memset(dst->ptr, 0, dst->len);
}

template <typename T>
void AggregateFunctions::KnuthVarUpdate(FunctionContext* ctx, const T& src,
    StringVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
  if (src.is_null) return;
  KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(dst->ptr);
  double temp = 1 + state->count;
  double delta = src.val - state->mean;
  double r = delta / temp;
  state->mean += r;
  state->m2 += state->count * delta * r;
  state->count = temp;
}

void AggregateFunctions::KnuthVarMerge(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(KnuthVarianceState));
  // Reference implementation:
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
  KnuthVarianceState* src_state = reinterpret_cast<KnuthVarianceState*>(src.ptr);
  KnuthVarianceState* dst_state = reinterpret_cast<KnuthVarianceState*>(dst->ptr);
  if (src_state->count == 0) return;
  double delta = dst_state->mean - src_state->mean;
  double sum_count = dst_state->count + src_state->count;
  dst_state->mean = src_state->mean + delta * (dst_state->count / sum_count);
  dst_state->m2 = (src_state->m2) + dst_state->m2 +
      (delta * delta) * (src_state->count * dst_state->count / sum_count);
  dst_state->count = sum_count;
}

DoubleVal AggregateFunctions::KnuthVarFinalize(
    FunctionContext* ctx, const StringVal& state_sv) {
  DCHECK(!state_sv.is_null);
  KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
  if (state->count == 0 || state->count == 1) return DoubleVal::null();
  double variance = ComputeKnuthVariance(*state, false);
  return DoubleVal(variance);
}

DoubleVal AggregateFunctions::KnuthVarPopFinalize(FunctionContext* ctx,
                                                  const StringVal& state_sv) {
  DCHECK(!state_sv.is_null);
  DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
  KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
  if (state->count == 0) return DoubleVal::null();
  return ComputeKnuthVariance(*state, true);
}

DoubleVal AggregateFunctions::KnuthStddevFinalize(FunctionContext* ctx,
    const StringVal& state_sv) {
  DCHECK(!state_sv.is_null);
  DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
  KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
  if (state->count == 0 || state->count == 1) return DoubleVal::null();
  return sqrt(ComputeKnuthVariance(*state, false));
}

DoubleVal AggregateFunctions::KnuthStddevPopFinalize(FunctionContext* ctx,
    const StringVal& state_sv) {
  DCHECK(!state_sv.is_null);
  DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
  KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
  if (state->count == 0) return DoubleVal::null();
  return sqrt(ComputeKnuthVariance(*state, true));
}

struct RankState {
  int64_t rank;
  int64_t count;
  RankState() : rank(1), count(0) { }
};

void AggregateFunctions::RankInit(FunctionContext* ctx, StringVal* dst) {
  // The rank functions use a preallocated FIXED_UDA_INTERMEDIATE intermediate value.
  DCHECK_EQ(dst->len, sizeof(RankState));
  *reinterpret_cast<RankState*>(dst->ptr) = RankState();
}

void AggregateFunctions::RankUpdate(FunctionContext* ctx, StringVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(RankState));
  RankState* state = reinterpret_cast<RankState*>(dst->ptr);
  ++state->count;
}

void AggregateFunctions::DenseRankUpdate(FunctionContext* ctx, StringVal* dst) { }

BigIntVal AggregateFunctions::RankGetValue(FunctionContext* ctx,
    StringVal& src_val) {
  DCHECK(!src_val.is_null);
  DCHECK_EQ(src_val.len, sizeof(RankState));
  RankState* state = reinterpret_cast<RankState*>(src_val.ptr);
  DCHECK_GT(state->count, 0);
  DCHECK_GT(state->rank, 0);
  int64_t result = state->rank;

  // Prepares future calls for the next rank
  state->rank += state->count;
  state->count = 0;
  return BigIntVal(result);
}

BigIntVal AggregateFunctions::DenseRankGetValue(FunctionContext* ctx,
    StringVal& src_val) {
  DCHECK(!src_val.is_null);
  DCHECK_EQ(src_val.len, sizeof(RankState));
  RankState* state = reinterpret_cast<RankState*>(src_val.ptr);
  DCHECK_EQ(state->count, 0);
  DCHECK_GT(state->rank, 0);
  int64_t result = state->rank;

  // Prepares future calls for the next rank
  ++state->rank;
  return BigIntVal(result);
}

BigIntVal AggregateFunctions::RankFinalize(FunctionContext* ctx,
    StringVal& src_val) {
  if (UNLIKELY(src_val.is_null)) return BigIntVal::null();
  DCHECK_EQ(src_val.len, sizeof(RankState));
  RankState* state = reinterpret_cast<RankState*>(src_val.ptr);
  int64_t result = state->rank;
  return BigIntVal(result);
}

template <typename T>
void AggregateFunctions::LastValRemove(FunctionContext* ctx, const T& src, T* dst) {
  if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) *dst = T::null();
}

template <>
void AggregateFunctions::LastValRemove(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
    if (!dst->is_null) ctx->Free(dst->ptr);
    *dst = StringVal::null();
  }
}

// Returns the current size of the window.
inline int GetWindowSize(FunctionContext* ctx) {
  return ctx->impl()->num_updates() - ctx->impl()->num_removes();
}

// LastValIgnoreNulls is a wrapper around LastVal. It works by not calling UpdateVal
// if the value being added to the window is null, so that we will return the most
// recently seen non-null value.
// The one special case to consider is when all of the values in the window are null
// and we therefore need to return null. To handle this, we track the number of nulls
// currently in the window, and set the value to be returned to null if the number of
// nulls is the same as the window size.
template <typename T>
struct LastValIgnoreNullsState {
  T last_val;
  // Number of nulls currently in the window, to detect when the window only has nulls.
  int64_t num_nulls;
};

template <typename T>
void AggregateFunctions::LastValIgnoreNullsInit(FunctionContext* ctx, StringVal* dst) {
  AllocBuffer(ctx, dst, sizeof(LastValIgnoreNullsState<T>));
  LastValIgnoreNullsState<T>* state =
      reinterpret_cast<LastValIgnoreNullsState<T>*>(dst->ptr);
  state->last_val = T::null();
  state->num_nulls = 0;
}

template <typename T>
void AggregateFunctions::LastValIgnoreNullsUpdate(FunctionContext* ctx, const T& src,
      StringVal* dst) {
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(LastValIgnoreNullsState<T>), dst->len);
  LastValIgnoreNullsState<T>* state =
      reinterpret_cast<LastValIgnoreNullsState<T>*>(dst->ptr);

  if (!src.is_null) {
    UpdateVal(ctx, src, &state->last_val);
  } else {
    ++state->num_nulls;
    DCHECK_LE(state->num_nulls, GetWindowSize(ctx));
    if (GetWindowSize(ctx) == state->num_nulls) {
      // Call UpdateVal here to set the value to null because it handles deallocation
      // of StringVals correctly.
      UpdateVal(ctx, T::null(), &state->last_val);
    }
  }
}

template <typename T>
void AggregateFunctions::LastValIgnoreNullsRemove(FunctionContext* ctx, const T& src,
      StringVal* dst) {
  DCHECK(dst->ptr != NULL);
  DCHECK_EQ(sizeof(LastValIgnoreNullsState<T>), dst->len);
  LastValIgnoreNullsState<T>* state =
      reinterpret_cast<LastValIgnoreNullsState<T>*>(dst->ptr);
  LastValRemove(ctx, src, &state->last_val);

  if (src.is_null) --state->num_nulls;
  DCHECK_GE(state->num_nulls, 0);
  if (GetWindowSize(ctx) == state->num_nulls) {
    // Call UpdateVal here to set the value to null because it handles deallocation
    // of StringVals correctly.
    UpdateVal(ctx, T::null(), &state->last_val);
  }
}

template <typename T>
T AggregateFunctions::LastValIgnoreNullsGetValue(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(sizeof(LastValIgnoreNullsState<T>), src.len);
  LastValIgnoreNullsState<T>* state =
      reinterpret_cast<LastValIgnoreNullsState<T>*>(src.ptr);
  return state->last_val;
}

template <>
StringVal AggregateFunctions::LastValIgnoreNullsGetValue(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(sizeof(LastValIgnoreNullsState<StringVal>), src.len);
  LastValIgnoreNullsState<StringVal>* state =
      reinterpret_cast<LastValIgnoreNullsState<StringVal>*>(src.ptr);

  if (state->last_val.is_null) {
    return StringVal::null();
  } else {
    return StringVal::CopyFrom(ctx, state->last_val.ptr, state->last_val.len);
  }
}

template <typename T>
T AggregateFunctions::LastValIgnoreNullsFinalize(FunctionContext* ctx,
      const StringVal& src) {
  DCHECK(!src.is_null);
  T result = LastValIgnoreNullsGetValue<T>(ctx, src);
  ctx->Free(src.ptr);
  return result;
}

template <>
StringVal AggregateFunctions::LastValIgnoreNullsFinalize(FunctionContext* ctx,
      const StringVal& src) {
  DCHECK(!src.is_null);
  LastValIgnoreNullsState<StringVal>* state =
      reinterpret_cast<LastValIgnoreNullsState<StringVal>*>(src.ptr);
  StringVal result = LastValIgnoreNullsGetValue<StringVal>(ctx, src);
  if (!state->last_val.is_null) ctx->Free(state->last_val.ptr);
  ctx->Free(src.ptr);
  return result;
}

template <typename T>
void AggregateFunctions::FirstValUpdate(FunctionContext* ctx, const T& src, T* dst) {
  // The first call to FirstValUpdate sets the value of dst.
  if (ctx->impl()->num_updates() > 1) return;
  // num_updates is incremented before calling Update(), so it should never be 0.
  // Remove() should never be called for FIRST_VALUE.
  DCHECK_GT(ctx->impl()->num_updates(), 0);
  DCHECK_EQ(ctx->impl()->num_removes(), 0);
  *dst = src;
}

template <>
void AggregateFunctions::FirstValUpdate(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  if (ctx->impl()->num_updates() > 1) return;
  DCHECK_GT(ctx->impl()->num_updates(), 0);
  DCHECK_EQ(ctx->impl()->num_removes(), 0);
  if (src.is_null) {
    *dst = StringVal::null();
    return;
  }
  CopyStringVal(ctx, src, dst);
}

template <typename T>
void AggregateFunctions::FirstValRewriteUpdate(FunctionContext* ctx, const T& src,
    const BigIntVal&, T* dst) {
  UpdateVal<T>(ctx, src, dst);
}

template <typename T>
void AggregateFunctions::FirstValIgnoreNullsUpdate(FunctionContext*, const T& src,
    T* dst) {
  // Store the first non-null value encountered, unlike FirstValUpdate which always stores
  // the first value even if it is null.
  if (!dst->is_null || src.is_null) return;
  *dst = src;
}

template <>
void AggregateFunctions::FirstValIgnoreNullsUpdate(FunctionContext* ctx,
    const StringVal& src, StringVal* dst) {
  // Store the first non-null value encountered, unlike FirstValUpdate which always stores
  // the first value even if it is null.
  if (!dst->is_null || src.is_null) return;
  CopyStringVal(ctx, src, dst);
}

template <typename T>
void AggregateFunctions::OffsetFnInit(FunctionContext* ctx, T* dst) {
  DCHECK_EQ(ctx->GetNumArgs(), 3);
  DCHECK(ctx->IsArgConstant(1));
  DCHECK(ctx->IsArgConstant(2));
  DCHECK_EQ(ctx->GetArgType(0)->type, ctx->GetArgType(2)->type);
  *dst = *static_cast<T*>(ctx->GetConstantArg(2));
}

template <>
void AggregateFunctions::OffsetFnInit(FunctionContext* ctx, StringVal* dst) {
  DCHECK_EQ(ctx->GetNumArgs(), 3);
  DCHECK(ctx->IsArgConstant(1));
  DCHECK(ctx->IsArgConstant(2));
  DCHECK_EQ(ctx->GetArgType(0)->type, ctx->GetArgType(2)->type);
  CopyStringVal(ctx, *static_cast<StringVal*>(ctx->GetConstantArg(2)), dst);
}

template <typename T>
void AggregateFunctions::OffsetFnUpdate(FunctionContext* ctx, const T& src,
    const BigIntVal&, const T& default_value, T* dst) {
  UpdateVal(ctx, src, dst);
}

// Stamp out the templates for the types we need.
template void AggregateFunctions::InitZero<BigIntVal>(FunctionContext*, BigIntVal* dst);

template void AggregateFunctions::UpdateVal<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::UpdateVal<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::UpdateVal<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::UpdateVal<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::UpdateVal<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::UpdateVal<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::UpdateVal<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::UpdateVal<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::UpdateVal<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::UpdateVal<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::AvgUpdate<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& input, StringVal* dst);
template void AggregateFunctions::AvgUpdate<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& input, StringVal* dst);
template void AggregateFunctions::AvgRemove<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& input, StringVal* dst);
template void AggregateFunctions::AvgRemove<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& input, StringVal* dst);

template void AggregateFunctions::SumUpdate<TinyIntVal, BigIntVal>(
    FunctionContext*, const TinyIntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumUpdate<SmallIntVal, BigIntVal>(
    FunctionContext*, const SmallIntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumUpdate<IntVal, BigIntVal>(
    FunctionContext*, const IntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumUpdate<BigIntVal, BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumUpdate<FloatVal, DoubleVal>(
    FunctionContext*, const FloatVal& src, DoubleVal* dst);
template void AggregateFunctions::SumUpdate<DoubleVal, DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);

template void AggregateFunctions::SumRemove<TinyIntVal, BigIntVal>(
    FunctionContext*, const TinyIntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumRemove<SmallIntVal, BigIntVal>(
    FunctionContext*, const SmallIntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumRemove<IntVal, BigIntVal>(
    FunctionContext*, const IntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumRemove<BigIntVal, BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::SumRemove<FloatVal, DoubleVal>(
    FunctionContext*, const FloatVal& src, DoubleVal* dst);
template void AggregateFunctions::SumRemove<DoubleVal, DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);

template void AggregateFunctions::Min<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::Min<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::Min<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::Min<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::Min<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Min<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::Min<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::Min<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::Min<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::Max<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::Max<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::Max<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::Max<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::Max<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Max<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::Max<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::Max<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::Max<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::PcUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);

template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);

template void AggregateFunctions::ReservoirSampleInit<BooleanVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<TinyIntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<SmallIntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<IntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<BigIntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<FloatVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<DoubleVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<StringVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<TimestampVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::ReservoirSampleInit<DecimalVal>(
    FunctionContext*, StringVal*);

template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);

template StringVal AggregateFunctions::ReservoirSampleSerialize<BooleanVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<TinyIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<SmallIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<IntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<BigIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<FloatVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<DoubleVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<StringVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<TimestampVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleSerialize<DecimalVal>(
    FunctionContext*, const StringVal&);

template void AggregateFunctions::ReservoirSampleMerge<BooleanVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<TinyIntVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<SmallIntVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<IntVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<BigIntVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<FloatVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<DoubleVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<StringVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<TimestampVal>(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::ReservoirSampleMerge<DecimalVal>(
    FunctionContext*, const StringVal&, StringVal*);

template StringVal AggregateFunctions::ReservoirSampleFinalize<BooleanVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<TinyIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<SmallIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<IntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<BigIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<FloatVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<DoubleVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<StringVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<TimestampVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::ReservoirSampleFinalize<DecimalVal>(
    FunctionContext*, const StringVal&);

template StringVal AggregateFunctions::HistogramFinalize<BooleanVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<TinyIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<SmallIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<IntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<BigIntVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<FloatVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<DoubleVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<StringVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<TimestampVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::HistogramFinalize<DecimalVal>(
    FunctionContext*, const StringVal&);

template BooleanVal AggregateFunctions::AppxMedianFinalize<BooleanVal>(
    FunctionContext*, const StringVal&);
template TinyIntVal AggregateFunctions::AppxMedianFinalize<TinyIntVal>(
    FunctionContext*, const StringVal&);
template SmallIntVal AggregateFunctions::AppxMedianFinalize<SmallIntVal>(
    FunctionContext*, const StringVal&);
template IntVal AggregateFunctions::AppxMedianFinalize<IntVal>(
    FunctionContext*, const StringVal&);
template BigIntVal AggregateFunctions::AppxMedianFinalize<BigIntVal>(
    FunctionContext*, const StringVal&);
template FloatVal AggregateFunctions::AppxMedianFinalize<FloatVal>(
    FunctionContext*, const StringVal&);
template DoubleVal AggregateFunctions::AppxMedianFinalize<DoubleVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::AppxMedianFinalize<StringVal>(
    FunctionContext*, const StringVal&);
template TimestampVal AggregateFunctions::AppxMedianFinalize<TimestampVal>(
    FunctionContext*, const StringVal&);
template DecimalVal AggregateFunctions::AppxMedianFinalize<DecimalVal>(
    FunctionContext*, const StringVal&);

template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);

template void AggregateFunctions::KnuthVarUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::KnuthVarUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::KnuthVarUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::KnuthVarUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::KnuthVarUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::KnuthVarUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);

template void AggregateFunctions::LastValRemove<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::LastValRemove<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::LastValRemove<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::LastValRemove<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::LastValRemove<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::LastValRemove<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::LastValRemove<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::LastValRemove<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::LastValRemove<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::LastValRemove<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::LastValIgnoreNullsInit<BooleanVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<TinyIntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<SmallIntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<IntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<BigIntVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<FloatVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<DoubleVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<StringVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<TimestampVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::LastValIgnoreNullsInit<DecimalVal>(
    FunctionContext*, StringVal*);

template void AggregateFunctions::LastValIgnoreNullsUpdate<BooleanVal>(
    FunctionContext*, const BooleanVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<IntVal>(
    FunctionContext*, const IntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<BigIntVal>(
    FunctionContext*, const BigIntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<FloatVal>(
    FunctionContext*, const FloatVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<DoubleVal>(
    FunctionContext*, const DoubleVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, StringVal* dst);

template void AggregateFunctions::LastValIgnoreNullsRemove<BooleanVal>(
    FunctionContext*, const BooleanVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<IntVal>(
    FunctionContext*, const IntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<BigIntVal>(
    FunctionContext*, const BigIntVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<FloatVal>(
    FunctionContext*, const FloatVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<DoubleVal>(
    FunctionContext*, const DoubleVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<TimestampVal>(
    FunctionContext*, const TimestampVal& src, StringVal* dst);
template void AggregateFunctions::LastValIgnoreNullsRemove<DecimalVal>(
    FunctionContext*, const DecimalVal& src, StringVal* dst);

template BooleanVal AggregateFunctions::LastValIgnoreNullsGetValue<BooleanVal>(
    FunctionContext*, const StringVal&);
template TinyIntVal AggregateFunctions::LastValIgnoreNullsGetValue<TinyIntVal>(
    FunctionContext*, const StringVal&);
template SmallIntVal AggregateFunctions::LastValIgnoreNullsGetValue<SmallIntVal>(
    FunctionContext*, const StringVal&);
template IntVal AggregateFunctions::LastValIgnoreNullsGetValue<IntVal>(
    FunctionContext*, const StringVal&);
template BigIntVal AggregateFunctions::LastValIgnoreNullsGetValue<BigIntVal>(
    FunctionContext*, const StringVal&);
template FloatVal AggregateFunctions::LastValIgnoreNullsGetValue<FloatVal>(
    FunctionContext*, const StringVal&);
template DoubleVal AggregateFunctions::LastValIgnoreNullsGetValue<DoubleVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::LastValIgnoreNullsGetValue<StringVal>(
    FunctionContext*, const StringVal&);
template TimestampVal AggregateFunctions::LastValIgnoreNullsGetValue<TimestampVal>(
    FunctionContext*, const StringVal&);
template DecimalVal AggregateFunctions::LastValIgnoreNullsGetValue<DecimalVal>(
    FunctionContext*, const StringVal&);

template BooleanVal AggregateFunctions::LastValIgnoreNullsFinalize<BooleanVal>(
    FunctionContext*, const StringVal&);
template TinyIntVal AggregateFunctions::LastValIgnoreNullsFinalize<TinyIntVal>(
    FunctionContext*, const StringVal&);
template SmallIntVal AggregateFunctions::LastValIgnoreNullsFinalize<SmallIntVal>(
    FunctionContext*, const StringVal&);
template IntVal AggregateFunctions::LastValIgnoreNullsFinalize<IntVal>(
    FunctionContext*, const StringVal&);
template BigIntVal AggregateFunctions::LastValIgnoreNullsFinalize<BigIntVal>(
    FunctionContext*, const StringVal&);
template FloatVal AggregateFunctions::LastValIgnoreNullsFinalize<FloatVal>(
    FunctionContext*, const StringVal&);
template DoubleVal AggregateFunctions::LastValIgnoreNullsFinalize<DoubleVal>(
    FunctionContext*, const StringVal&);
template StringVal AggregateFunctions::LastValIgnoreNullsFinalize<StringVal>(
    FunctionContext*, const StringVal&);
template TimestampVal AggregateFunctions::LastValIgnoreNullsFinalize<TimestampVal>(
    FunctionContext*, const StringVal&);
template DecimalVal AggregateFunctions::LastValIgnoreNullsFinalize<DecimalVal>(
    FunctionContext*, const StringVal&);

template void AggregateFunctions::FirstValUpdate<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::FirstValUpdate<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::FirstValUpdate<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::FirstValUpdate<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::FirstValUpdate<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::FirstValUpdate<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::FirstValUpdate<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::FirstValUpdate<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::FirstValUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::FirstValUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::FirstValRewriteUpdate<BooleanVal>(
    FunctionContext*, const BooleanVal& src, const BigIntVal&, BooleanVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, const BigIntVal&, TinyIntVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, const BigIntVal&, SmallIntVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<IntVal>(
    FunctionContext*, const IntVal& src, const BigIntVal&, IntVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<BigIntVal>(
    FunctionContext*, const BigIntVal& src, const BigIntVal&, BigIntVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<FloatVal>(
    FunctionContext*, const FloatVal& src, const BigIntVal&, FloatVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<DoubleVal>(
    FunctionContext*, const DoubleVal& src, const BigIntVal&, DoubleVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<StringVal>(
    FunctionContext*, const StringVal& src, const BigIntVal&, StringVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, const BigIntVal&, TimestampVal* dst);
template void AggregateFunctions::FirstValRewriteUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, const BigIntVal&, DecimalVal* dst);

template void AggregateFunctions::FirstValIgnoreNullsUpdate<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::OffsetFnInit<BooleanVal>(
    FunctionContext*, BooleanVal*);
template void AggregateFunctions::OffsetFnInit<TinyIntVal>(
    FunctionContext*, TinyIntVal*);
template void AggregateFunctions::OffsetFnInit<SmallIntVal>(
    FunctionContext*, SmallIntVal*);
template void AggregateFunctions::OffsetFnInit<IntVal>(
    FunctionContext*, IntVal*);
template void AggregateFunctions::OffsetFnInit<BigIntVal>(
    FunctionContext*, BigIntVal*);
template void AggregateFunctions::OffsetFnInit<FloatVal>(
    FunctionContext*, FloatVal*);
template void AggregateFunctions::OffsetFnInit<DoubleVal>(
    FunctionContext*, DoubleVal*);
template void AggregateFunctions::OffsetFnInit<StringVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::OffsetFnInit<TimestampVal>(
    FunctionContext*, TimestampVal*);
template void AggregateFunctions::OffsetFnInit<DecimalVal>(
    FunctionContext*, DecimalVal*);

template void AggregateFunctions::OffsetFnUpdate<BooleanVal>(
    FunctionContext*, const BooleanVal& src, const BigIntVal&, const BooleanVal&,
    BooleanVal* dst);
template void AggregateFunctions::OffsetFnUpdate<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, const BigIntVal&, const TinyIntVal&,
    TinyIntVal* dst);
template void AggregateFunctions::OffsetFnUpdate<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, const BigIntVal&, const SmallIntVal&,
    SmallIntVal* dst);
template void AggregateFunctions::OffsetFnUpdate<IntVal>(
    FunctionContext*, const IntVal& src, const BigIntVal&, const IntVal&, IntVal* dst);
template void AggregateFunctions::OffsetFnUpdate<BigIntVal>(
    FunctionContext*, const BigIntVal& src, const BigIntVal&, const BigIntVal&,
    BigIntVal* dst);
template void AggregateFunctions::OffsetFnUpdate<FloatVal>(
    FunctionContext*, const FloatVal& src, const BigIntVal&, const FloatVal&,
    FloatVal* dst);
template void AggregateFunctions::OffsetFnUpdate<DoubleVal>(
    FunctionContext*, const DoubleVal& src, const BigIntVal&, const DoubleVal&,
    DoubleVal* dst);
template void AggregateFunctions::OffsetFnUpdate<StringVal>(
    FunctionContext*, const StringVal& src, const BigIntVal&, const StringVal&,
    StringVal* dst);
template void AggregateFunctions::OffsetFnUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, const BigIntVal&, const TimestampVal&,
    TimestampVal* dst);
template void AggregateFunctions::OffsetFnUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, const BigIntVal&, const DecimalVal&,
    DecimalVal* dst);
}
