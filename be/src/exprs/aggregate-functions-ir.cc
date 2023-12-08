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

#include <algorithm>
#include <map>
#include <sstream>
#include <utility>
#include <cmath>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>

#include "codegen/impala-ir.h"
#include "common/logging.h"
#include "exprs/anyval-util.h"
#include "exprs/datasketches-common.h"
#include "exprs/hll-bias.h"
#include "gutil/strings/substitute.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/multi-precision.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "thirdparty/datasketches/hll.hpp"
#include "thirdparty/datasketches/cpc_sketch.hpp"
#include "thirdparty/datasketches/cpc_union.hpp"
#include "thirdparty/datasketches/theta_sketch.hpp"
#include "thirdparty/datasketches/theta_union.hpp"
#include "thirdparty/datasketches/theta_intersection.hpp"
#include "thirdparty/datasketches/kll_sketch.hpp"
#include "util/arithmetic-util.h"
#include "util/mpfit-util.h"
#include "util/pretty-printer.h"

#include "common/names.h"

using boost::uniform_int;
using boost::mt19937_64;
using std::make_pair;
using std::map;
using std::min_element;
using std::nth_element;
using std::pop_heap;
using std::push_heap;
using std::string;
using std::stringstream;

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
int64_t HllEstimateBias(int64_t estimate, int precision) {
  const size_t K = 6;

  // Precision index into data arrays
  // We don't have data for precisions less than 4
  DCHECK_IN_RANGE(precision, impala::AggregateFunctions::MIN_HLL_PRECISION,
      impala::AggregateFunctions::MAX_HLL_PRECISION);
  size_t idx = precision - 4;

  // Calculate the square of the difference of this estimate to all
  // precalculated estimates for a particular precision
  map<double, size_t> distances;
  for (size_t i = 0; i < impala::HLL_DATA_SIZES[idx] / sizeof(double); ++i) {
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

const char* ERROR_CONCATENATED_STRING_MAX_SIZE_REACHED =
  "Concatenated string length is larger than allowed limit of $0 character data.";

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

constexpr int AggregateFunctions::DEFAULT_HLL_PRECISION;
constexpr int AggregateFunctions::MIN_HLL_PRECISION;
constexpr int AggregateFunctions::MAX_HLL_PRECISION;

constexpr int AggregateFunctions::DEFAULT_HLL_LEN;
constexpr int AggregateFunctions::MIN_HLL_LEN;
constexpr int AggregateFunctions::MAX_HLL_LEN;

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

// regr_count(y, x) returns an integer that is the number of non-null
// number pairs. It indicates how many observations are included in the
// analysis.
void AggregateFunctions::RegrCountUpdate(
    FunctionContext*, const DoubleVal& src1, const DoubleVal& src2, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src1.is_null && !src2.is_null) ++dst->val;
}

void AggregateFunctions::RegrCountRemove(
    FunctionContext*, const DoubleVal& src1, const DoubleVal& src2, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src1.is_null && !src2.is_null) {
    --dst->val;
    DCHECK_GE(dst->val, 0);
  }
}

void AggregateFunctions::TimestampRegrCountUpdate(FunctionContext*,
    const TimestampVal& src1, const TimestampVal& src2, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src1.is_null && !src2.is_null) ++dst->val;
}

void AggregateFunctions::TimestampRegrCountRemove(FunctionContext*,
    const TimestampVal& src1, const TimestampVal& src2, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src1.is_null && !src2.is_null) {
    --dst->val;
    DCHECK_GE(dst->val, 0);
  }
}

// Implementation of regr_slope() and regr_intercept():
// RegrSlopeState is used for implementing regr_slope() and regr_intercept().
// regr_slope() and regr_intercept() take two arguments of numeric type and return the
// regression slope of the line and the y-intercept of the regression line respectively.
// The linear regression functions fit an ordinary-least-squares regression line to a set
// of number pairs. They can be used both as aggregate and analytic functions.
// Here's a link which contains description of all the regression functions:
// https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/REGR_-Linear-Regression-Functions.html#GUID-A675B68F-2A88-4843-BE2C-FCDE9C65F9A9

// regr_slope() formula used:
// regr_slope(y, x) = covar_pop(x, y) / var_pop(x)
// regr_intercept() formula used:
// regr_intercept(y,x) = avg(y) - regr_slope(y, x) * avg(x)
// where y and x are the dependent and independent variables respectively.
struct RegrSlopeState {
  int64_t count;
  double yavg; // average of y elements
  double xavg; // average of x elements
  double xvar; // count times the variance of x elements
  double covar; // count times the covariance
};

void AggregateFunctions::RegrSlopeInit(FunctionContext* ctx, StringVal* dst) {
  dst->is_null = false;
  dst->len = sizeof(RegrSlopeState);
  AllocBuffer(ctx, dst, dst->len);
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  *(reinterpret_cast<RegrSlopeState*>(dst->ptr)) = {};
}

static inline void RegrSlopeUpdateState(double y, double x, RegrSlopeState* state) {
  double deltaY = y - state->yavg;
  double deltaX = x - state->xavg;
  ++state->count;
  // my_n = my_(n - 1) + [y_n - my_(n - 1)] / n
  state->yavg += deltaY / state->count;
  // mx_n = mx_(n - 1) + [x_n - mx_(n - 1)] / n
  state->xavg += deltaX / state->count;
  if (state->count > 1) {
    // c_n = c_(n - 1) + (y_n - my_n) * (x_n - mx_(n - 1)) OR
    // c_n = c_(n - 1) + (y_n - my_(n - 1)) * (x_n - mx_n)
    // The apparent asymmetry in the equations is due to the fact that,
    // y_n - my_n = (n - 1) * (y_n - my_(n - 1)) / n, so both update terms are equal to
    // (n - 1) * (y_n - my_(n - 1)) * (x_n - mx_(n - 1)) / n
    state->covar += deltaY * (x - state->xavg);
    // vx_n = vx_(n - 1) + (x_n - mx_(n - 1)) * (x_n - mx_n)
    state->xvar += deltaX * (x - state->xavg);
  }
}

static inline void RegrSlopeRemoveState(double y, double x, RegrSlopeState* state) {
  if (state->count <= 1) {
    *(reinterpret_cast<RegrSlopeState*>(sizeof(RegrSlopeState))) = {};
  } else {
    double deltaY = y - state->yavg;
    double deltaX = x - state->xavg;
    --state->count;
    // my_(n - 1) = my_n - (y_n - my_n) / (n - 1)
    state->yavg -= deltaY / state->count;
    // mx_(n - 1) = mx_n - (x_n - mx_n) / (n - 1)
    state->xavg -= deltaX / state->count;
    // c_(n - 1) = c_n - (y_n - mx_n) * (x_n - mx_(n -1))
    state->covar -= deltaY * (x - state->xavg);
    // vx_(n - 1) = vx_n - (x_n - mx_n) * (x_n - mx_(n - 1))
    state->xvar -= deltaX * (x - state->xavg);
  }
}

void AggregateFunctions::RegrSlopeUpdate(FunctionContext* ctx,
    const DoubleVal& src1, const DoubleVal& src2, StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(RegrSlopeState), dst->len);
  RegrSlopeState* state = reinterpret_cast<RegrSlopeState*>(dst->ptr);
  RegrSlopeUpdateState(src1.val, src2.val, state);
}

void AggregateFunctions::RegrSlopeRemove(FunctionContext* ctx,
    const DoubleVal& src1, const DoubleVal& src2, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0. In other words, it's not needed to
  // check if num_removes() >= num_updates() as it's accounted for in Finalize().
  if (src1.is_null || src2.is_null) return;
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(RegrSlopeState), dst->len);
  RegrSlopeState* state = reinterpret_cast<RegrSlopeState*>(dst->ptr);
  RegrSlopeRemoveState(src1.val, src2.val, state);
}

void AggregateFunctions::TimestampRegrSlopeUpdate(FunctionContext* ctx,
    const TimestampVal& src1, const TimestampVal& src2, StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  RegrSlopeState* state = reinterpret_cast<RegrSlopeState*>(dst->ptr);
  const TimestampValue& tm_src1 = TimestampValue::FromTimestampVal(src1);
  const TimestampValue& tm_src2 = TimestampValue::FromTimestampVal(src2);
  double val1, val2;
  if (tm_src1.ToSubsecondUnixTime(UTCPTR, &val1) &&
      tm_src2.ToSubsecondUnixTime(UTCPTR, &val2)) {
    RegrSlopeUpdateState(val1, val2, state);
  }
}

void AggregateFunctions::TimestampRegrSlopeRemove(FunctionContext* ctx,
    const TimestampVal& src1, const TimestampVal& src2, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0. In other words, it's not needed to
  // check if num_removes() >= num_updates() as it's accounted for in Finalize().
  if (src1.is_null || src2.is_null) return;
  RegrSlopeState* state = reinterpret_cast<RegrSlopeState*>(dst->ptr);
  const TimestampValue& tm_src1 = TimestampValue::FromTimestampVal(src1);
  const TimestampValue& tm_src2 = TimestampValue::FromTimestampVal(src2);
  double val1, val2;
  if (tm_src1.ToSubsecondUnixTime(UTCPTR, &val1) &&
      tm_src2.ToSubsecondUnixTime(UTCPTR, &val2)) {
    RegrSlopeRemoveState(val1, val2, state);
  }
}

void AggregateFunctions::RegrSlopeMerge(FunctionContext* ctx,
    const StringVal& src, StringVal* dst) {
  const RegrSlopeState* src_state = reinterpret_cast<RegrSlopeState*>(src.ptr);
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(RegrSlopeState), dst->len);
  RegrSlopeState* dst_state = reinterpret_cast<RegrSlopeState*>(dst->ptr);
  if (src.ptr != nullptr) {
    int64_t nA = dst_state->count;
    if (nA == 0) {
      *dst_state = *src_state;
      return;
    }
    double yavgA = dst_state->yavg;
    double xavgA = dst_state->xavg;

    dst_state->count += src_state->count;
    dst_state->yavg = (yavgA * nA + src_state->yavg * src_state->count) /
        dst_state->count;
    dst_state->xavg = (xavgA * nA + src_state->xavg * src_state->count) /
        dst_state->count;
    // vx_(A,B) = vx_A + vx_B + (mx_A - mx_B) * (mx_A - mx_B) * n_A * n_B / (n_A + n_B)
    dst_state->xvar +=
        src_state->xvar + (xavgA - src_state->xavg) * (xavgA - src_state->xavg) * nA
            * src_state->count / dst_state->count;
    // c_(A,B) = c_A + c_B + (my_A - my_B) * (mx_A - mx_B) * n_A * n_B / (n_A + n_B)
    dst_state->covar += src_state->covar
        + (yavgA - src_state->yavg) * (xavgA - src_state->xavg) * ((double)(nA *
              src_state->count)) / (dst_state->count);
  }
}

DoubleVal AggregateFunctions::RegrSlopeGetValue(FunctionContext* ctx,
    const StringVal& src) {
  const RegrSlopeState* state = reinterpret_cast<RegrSlopeState*>(src.ptr);
  // Calculating Regression slope:
  // xvar becomes negative in certain cases due to floating point rounding error.
  // Since these values are very small, they can be ignored and rounded to 0.
  DCHECK(state->xvar >= FLOATING_POINT_ERROR_THRESHOLD);
  if (state->count < 2 || state->xvar <= 0.0) {
    return DoubleVal::null();
  }
  return DoubleVal(state->covar / state->xvar);
}

DoubleVal AggregateFunctions::RegrSlopeFinalize(FunctionContext* ctx,
    const StringVal& src) {
  DoubleVal r = src.is_null ? DoubleVal::null() :
                              RegrSlopeGetValue(ctx, src);
  ctx->Free(src.ptr);
  return r;
}

DoubleVal AggregateFunctions::RegrInterceptGetValue(FunctionContext* ctx,
    const StringVal& src) {
  RegrSlopeState* state = reinterpret_cast<RegrSlopeState*>(src.ptr);
  // Calculating Regression Intercept
  // xvar becomes negative in certain cases due to floating point rounding error.
  // Since these values are very small, they can be ignored and rounded to 0.
  DCHECK(state->xvar >= FLOATING_POINT_ERROR_THRESHOLD);
  if (state->count < 2 || state->xvar <= 0.0) {
    return DoubleVal::null();
  }
  double regrSlope = state->covar / state->xvar;
  double regrIntercept = state->yavg - (regrSlope * state->xavg);
  return DoubleVal(regrIntercept);
}

DoubleVal AggregateFunctions::RegrInterceptFinalize(FunctionContext* ctx,
    const StringVal& src) {
  DoubleVal r = src.is_null ? DoubleVal::null() :
                              RegrInterceptGetValue(ctx, src);
  ctx->Free(src.ptr);
  return r;
}

// Implementation of CORR() function which takes two arguments of numeric type
// and returns the Pearson's correlation coefficient between them using the Welford's
// online algorithm. This is calculated using a stable one-pass algorithm, based on
// work by Philippe Pébay and Donald Knuth.
// Implementation of CORR() is independent of the implementation of COVAR_SAMP() and
// COVAR_POP() so changes in one would probably need to be reflected in the other as well.
// Few useful links :
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online
// https://www.osti.gov/biblio/1028931
// Correlation coefficient formula used:
// r = covar / (√(xvar * yvar)
struct CorrState {
  int64_t count; // number of elements
  double xavg; // average of x elements
  double yavg; // average of y elements
  double xvar; // n times the variance of x elements
  double yvar; // n times the variance of y elements
  double covar; // n times the covariance
};

void AggregateFunctions::CorrInit(FunctionContext* ctx, StringVal* dst) {
  dst->is_null = false;
  dst->len = sizeof(CorrState);
  AllocBuffer(ctx, dst, dst->len);
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  *(reinterpret_cast<CorrState*>(dst->ptr)) = {};
}

static inline void CorrUpdateState(double x, double y, CorrState* state) {
  double deltaX = x - state->xavg;
  double deltaY = y - state->yavg;
  ++state->count;
  // mx_n = mx_(n - 1) + [x_n - mx_(n - 1)] / n
  state->xavg += deltaX / state->count;
  // my_n = my_(n - 1) + [y_n - my_(n - 1)] / n
  state->yavg += deltaY / state->count;
  if (state->count > 1) {
    // c_n = c_(n - 1) + (x_n - mx_n) * (y_n - my_(n - 1)) OR
    // c_n = c_(n - 1) + (x_n - mx_(n - 1)) * (y_n - my_n)
    // The apparent asymmetry in the equations is due to the fact that,
    // x_n - mx_n = (n - 1) * (x_n - mx_(n - 1)) / n, so both update terms are equal to
    // (n - 1) * (x_n - mx_(n - 1)) * (y_n - my_(n - 1)) / n
    state->covar += deltaX * (y - state->yavg);
    // vx_n = vx_(n - 1) + (x_n - mx_(n - 1)) * (x_n - mx_n)
    state->xvar += deltaX * (x - state->xavg);
    // vy_n = vy_(n - 1) + (y_n - my_(n - 1)) * (y_n - my_n)
    state->yvar += deltaY * (y - state->yavg);
  }
}

static inline void CorrRemoveState(double x, double y, CorrState* state) {
  double deltaX = x - state->xavg;
  double deltaY = y - state->yavg;
  if (state->count <= 1) {
    *(reinterpret_cast<CorrState*>(sizeof(CorrState))) = {};
  } else {
    --state->count;
    // mx_(n - 1) = mx_n - (x_n - mx_n) / (n - 1)
    state->xavg -= deltaX / state->count;
    // my_(n - 1) = my_n - (y_n - my_n) / (n - 1)
    state->yavg -= deltaY / state->count;
    // c_(n - 1) = c_n - (x_n - mx_n) * (y_n - my_(n -1))
    state->covar -= deltaX * (y - state->yavg);
    // vx_(n - 1) = vx_n - (x_n - mx_n) * (x_n - mx_(n - 1))
    state->xvar -= deltaX * (x - state->xavg);
    // vy_(n - 1) = vy_n - (y_n - my_n) * (y_n - my_(n - 1))
    state->yvar -= deltaY * (y - state->yavg);
  }
}

void AggregateFunctions::CorrUpdate(FunctionContext* ctx,
    const DoubleVal& src1, const DoubleVal& src2, StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(CorrState), dst->len);
  CorrState* state = reinterpret_cast<CorrState*>(dst->ptr);
  CorrUpdateState(src1.val, src2.val, state);
}

void AggregateFunctions::CorrRemove(FunctionContext* ctx,
    const DoubleVal& src1, const DoubleVal& src2, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0. In other words, it's not needed to
  // check if num_removes() >= num_updates() as it's accounted for in Finalize().
  if (src1.is_null || src2.is_null) return;
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(CorrState), dst->len);
  CorrState* state = reinterpret_cast<CorrState*>(dst->ptr);
  CorrRemoveState(src1.val, src2.val, state);
}

void AggregateFunctions::TimestampCorrUpdate(FunctionContext* ctx,
    const TimestampVal& src1, const TimestampVal& src2, StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  CorrState* state = reinterpret_cast<CorrState*>(dst->ptr);
  const TimestampValue& tm_src1 = TimestampValue::FromTimestampVal(src1);
  const TimestampValue& tm_src2 = TimestampValue::FromTimestampVal(src2);
  double val1, val2;
  if (tm_src1.ToSubsecondUnixTime(UTCPTR, &val1) &&
      tm_src2.ToSubsecondUnixTime(UTCPTR, &val2)) {
    CorrUpdateState(val1, val2, state);
  }
}

void AggregateFunctions::TimestampCorrRemove(FunctionContext* ctx,
    const TimestampVal& src1, const TimestampVal& src2, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0. In other words, it's not needed to
  // check if num_removes() >= num_updates() as it's accounted for in Finalize().
  if (src1.is_null || src2.is_null) return;
  CorrState* state = reinterpret_cast<CorrState*>(dst->ptr);
  const TimestampValue& tm_src1 = TimestampValue::FromTimestampVal(src1);
  const TimestampValue& tm_src2 = TimestampValue::FromTimestampVal(src2);
  double val1, val2;
  if (tm_src1.ToSubsecondUnixTime(UTCPTR, &val1) &&
      tm_src2.ToSubsecondUnixTime(UTCPTR, &val2)) {
    CorrRemoveState(val1, val2, state);
  }
}

void AggregateFunctions::CorrMerge(FunctionContext* ctx,
    const StringVal& src, StringVal* dst) {
  CorrState* src_state = reinterpret_cast<CorrState*>(src.ptr);
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(CorrState), dst->len);
  CorrState* dst_state = reinterpret_cast<CorrState*>(dst->ptr);
  if (src.ptr != nullptr) {
    int64_t nA = dst_state->count;
    int64_t nB = src_state->count;
    if (nA == 0) {
      memcpy(dst_state, src_state, sizeof(CorrState));
      return;
    }
    if (nA != 0 && nB != 0) {
      double xavgA = dst_state->xavg;
      double yavgA = dst_state->yavg;
      double xavgB = src_state->xavg;
      double yavgB = src_state->yavg;
      double xvarB = src_state->xvar;
      double yvarB = src_state->yvar;
      double covarB = src_state->covar;

      dst_state->count += nB;
      dst_state->xavg = (xavgA * nA + xavgB * nB) / dst_state->count;
      dst_state->yavg = (yavgA * nA + yavgB * nB) / dst_state->count;
      // vx_(A,B) = vx_A + vx_B + (mx_A - mx_B) * (mx_A - mx_B) * n_A * n_B / (n_A + n_B)
      dst_state->xvar +=
          xvarB + (xavgA - xavgB) * (xavgA - xavgB) * nA * nB / dst_state->count;
      // vy_(A,B) = vy_A + vy_B + (my_A - my_B) * (my_A - my_B) * n_A * n_B / (n_A + n_B)
      dst_state->yvar +=
          yvarB + (yavgA - yavgB) * (yavgA - yavgB) * nA * nB / dst_state->count;
      // c_(A,B) = c_A + c_B + (mx_A - mx_B) * (my_A - my_B) * n_A * n_B / (n_A + n_B)
      dst_state->covar += covarB
          + (xavgA - xavgB) * (yavgA - yavgB) * ((double)(nA * nB)) / (dst_state->count);
    }
  }
}

DoubleVal AggregateFunctions::CorrGetValue(FunctionContext* ctx, const StringVal& src) {
  CorrState* state = reinterpret_cast<CorrState*>(src.ptr);
  // Calculating Pearson's correlation coefficient
  // xvar and yvar become negative in certain cases due to floating point rounding error.
  // Since these values are very small, they can be ignored and rounded to 0.
  DCHECK(state->xvar >= FLOATING_POINT_ERROR_THRESHOLD);
  DCHECK(state->yvar >= FLOATING_POINT_ERROR_THRESHOLD);
  if (state->count == 0 || state->count == 1 || state->xvar <= 0.0 ||
      state->yvar <= 0.0) {
    return DoubleVal::null();
  }
  double r = sqrt(state->xvar * state->yvar);
  if (r == 0.0) return DoubleVal::null();
  double corr = state->covar / r;
  return DoubleVal(corr);
}

DoubleVal AggregateFunctions::CorrFinalize(FunctionContext* ctx, const StringVal& src) {
  CorrState* state = reinterpret_cast<CorrState*>(src.ptr);
  if (UNLIKELY(src.is_null) || state->count == 0 || state->count == 1) {
    ctx->Free(src.ptr);
    return DoubleVal::null();
  }
  DoubleVal r = CorrGetValue(ctx, src);
  ctx->Free(src.ptr);
  return r;
}

// Implementation of regr_r2():
// CorrState is reused for implementing regr_r2.
// regr_r2() takes two arguments of numeric type and returns the coefficient of
// determination (also called R-squared or goodness of fit) for the regression.
// regr_r2() formula used:
// regr_2(y, x) = NULL if var_pop(x) = 0, else
//                1 if var_pop(y) = 0 (and var_pop(x) != 0), else
//                power(corr(y, x),2) if (var_pop(y) != 0 and var_pop(x) != 0)
// where y and x are the dependent and independent variables
// respectively. Note that variances can't be negative.
DoubleVal AggregateFunctions::Regr_r2GetValue(FunctionContext* ctx,
    const StringVal& src) {
  const CorrState* state = reinterpret_cast<CorrState*>(src.ptr);
  // Calculating Regression R2:
  // In this function we use 'dependent_var' and 'independent_var' instead of 'y_var' and
  // 'x_var'. This is to avoid confusion, because for regr_r2() the dependent variable is
  // the first parameter and the independent variable is the second parameter, but
  // CorrUpdate(), which we use to produce the intermediate values, has the opposite
  // order. Our aggregate function framework passes the variables in order to
  // CorrUpdate(), so in CorrUpdate() 'x' corresponds to the dependent variable of
  // regr_r2() and 'y' to the independent variable of regr_r2().
  double dependent_var = state->xvar;
  double independent_var = state->yvar;

  // dependent_var and independent_var become negative in certain cases due to floating
  // point rounding error.
  // Since these values are very small, they can be ignored and rounded to 0.
  DCHECK(dependent_var >= FLOATING_POINT_ERROR_THRESHOLD);
  DCHECK(independent_var >= FLOATING_POINT_ERROR_THRESHOLD);
  if (state->count < 2 || (independent_var / state->count) <= 0.0 ||
      (dependent_var / state->count) < 0.0) {
    return DoubleVal::null();
  } else if ((dependent_var / state->count) == 0.0) {
    return 1;
  } else {
    double stddev_prod_squared = dependent_var * independent_var;
    // Mathematically 'stddev_prod_squared' can only be 0 if either 'dependent_var'
    // or 'independent_var' is 0, which we have handled earlier. However, if both
    // 'dependent_var' and 'independent_var' are very small, the result may become
    // 0 because of floating point underflow. In this case we return NULL, i.e. treat
    // it as if 'dependent_var' was 0.
    if (stddev_prod_squared == 0.0) return DoubleVal::null();
    return state->covar * state->covar / stddev_prod_squared;
  }
}

DoubleVal AggregateFunctions::Regr_r2Finalize(FunctionContext* ctx,
    const StringVal& src) {
  DoubleVal r = src.is_null ? DoubleVal::null() :
                              Regr_r2GetValue(ctx, src);
  ctx->Free(src.ptr);
  return r;
}

// Implementation of COVAR_SAMP() and COVAR_POP() which calculates sample and
// population covariance between two columns of numeric types respectively using
// the Welford's online algorithm.
// Sample covariance:
// r = covar / (n-1)
// Population covariance:
// r = covar / (n)
struct CovarState {
  int64_t count; // number of elements
  double xavg; // average of x elements
  double yavg; // average of y elements
  double covar; // n times the covariance
};

void AggregateFunctions::CovarInit(FunctionContext* ctx, StringVal* dst) {
  dst->is_null = false;
  dst->len = sizeof(CovarState);
  AllocBuffer(ctx, dst, dst->len);
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  *(reinterpret_cast<CovarState*>(dst->ptr)) = {};
}

static inline void CovarUpdateState(double x, double y, CovarState* state) {
  ++state->count;
  // my_n = my_(n - 1) + [y_n - my_(n - 1)] / n
  state->yavg += (y - state->yavg) / state->count;
  // c_n = c_(n - 1) + (x_n - mx_(n - 1)) * (y_n - my_n) OR
  // c_n = c_(n - 1) + (x_n - mx_n) * (y_n - my_(n - 1))
  // The apparent asymmetry in the equations is due to the fact that,
  // x_n - mx_n = (n - 1) * (x_n - mx_(n - 1)) / n, so both terms are equal to
  // (n - 1) * (x_n - mx_(n - 1)) * (y_n - my_(n - 1)) / n
  if (state->count > 1) state->covar += (x - state->xavg) * (y - state->yavg);
  // mx_n = mx_(n - 1) + [x_n - mx_(n - 1)] / n
  state->xavg += (x - state->xavg) / state->count;
}

static inline void CovarRemoveState(double x, double y, CovarState* state){
  if (state->count <= 1) {
    memset(state, 0, sizeof(CovarState));
  } else {
    --state->count;
    // my_(n - 1) = my_n - (y_n - my_n) / (n - 1)
    state->yavg -= (y - state->yavg) / state->count;
    // c_(n - 1) = c_n - (x_n - mx_(n - 1)) * (y_n - my_n)
    state->covar -= (x - state->xavg) * (y - state->yavg);
    // mx_(n - 1) = mx_n - (x_n - mx_n) / (n - 1)
    state->xavg -= (x - state->xavg) / state->count;
  }
}

void AggregateFunctions::CovarUpdate(FunctionContext* ctx,
    const DoubleVal& src1, const DoubleVal& src2, StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(CovarState), dst->len);
  CovarState* state = reinterpret_cast<CovarState*>(dst->ptr);
  CovarUpdateState(src1.val, src2.val, state);
}

void AggregateFunctions::CovarRemove(FunctionContext* ctx,
    const DoubleVal& src1, const DoubleVal& src2, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0. In other words, it's not needed to
  // check if num_removes() >= num_updates() as it's accounted for in Finalize().
  if (src1.is_null || src2.is_null) return;
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(CovarState), dst->len);
  CovarState* state = reinterpret_cast<CovarState*>(dst->ptr);
  CovarRemoveState(src1.val, src2.val, state);
}

void AggregateFunctions::TimestampCovarUpdate(FunctionContext* ctx,
    const TimestampVal& src1, const TimestampVal& src2, StringVal* dst) {
  if (src1.is_null || src2.is_null) return;
  CovarState* state = reinterpret_cast<CovarState*>(dst->ptr);
  const TimestampValue& tm_src1 = TimestampValue::FromTimestampVal(src1);
  const TimestampValue& tm_src2 = TimestampValue::FromTimestampVal(src2);
  double val1, val2;
  if (tm_src1.ToSubsecondUnixTime(UTCPTR, &val1) &&
      tm_src2.ToSubsecondUnixTime(UTCPTR, &val2)) {
    CovarUpdateState(val1, val2, state);
  }
}

void AggregateFunctions::TimestampCovarRemove(FunctionContext* ctx,
    const TimestampVal& src1, const TimestampVal& src2, StringVal* dst) {
  // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
  // because Finalize() returns NULL if count is 0. In other words, it's not needed to
  // check if num_removes() >= num_updates() as it's accounted for in Finalize().
  if (src1.is_null || src2.is_null) return;
  CovarState* state = reinterpret_cast<CovarState*>(dst->ptr);
  const TimestampValue& tm_src1 = TimestampValue::FromTimestampVal(src1);
  const TimestampValue& tm_src2 = TimestampValue::FromTimestampVal(src2);
  double val1, val2;
  if (tm_src1.ToSubsecondUnixTime(UTCPTR, &val1) &&
      tm_src2.ToSubsecondUnixTime(UTCPTR, &val2)) {
    CovarRemoveState(val1, val2, state);
  }
}

void AggregateFunctions::CovarMerge(FunctionContext* ctx,
    const StringVal& src, StringVal* dst) {
  CovarState* src_state = reinterpret_cast<CovarState*>(src.ptr);
  DCHECK(dst->ptr != nullptr);
  DCHECK_EQ(sizeof(CovarState), dst->len);
  CovarState* dst_state = reinterpret_cast<CovarState*>(dst->ptr);
  if (src.ptr != nullptr) {
    int64_t nA = dst_state->count;
    int64_t nB = src_state->count;
    if (nA == 0) {
      memcpy(dst_state, src_state, sizeof(CovarState));
      return;
    }
    if (nA != 0 && nB != 0) {
      double xavgA = dst_state->xavg;
      double yavgA = dst_state->yavg;
      double xavgB = src_state->xavg;
      double yavgB = src_state->yavg;
      double covarB = src_state->covar;

      dst_state->count += nB;
      dst_state->xavg = (xavgA * nA + xavgB * nB) / dst_state->count;
      dst_state->yavg = (yavgA * nA + yavgB * nB) / dst_state->count;
      // c_(A,B) = c_A + c_B + (mx_A - mx_B) * (my_A - my_B) * n_A * n_B / (n_A + n_B)
      dst_state->covar += covarB
          + (xavgA - xavgB) * (yavgA - yavgB) * ((double)(nA * nB)) / (dst_state->count);
    }
  }
}

DoubleVal AggregateFunctions::CovarSampleGetValue(FunctionContext* ctx,
    const StringVal& src) {
  // Calculating sample covariance
  CovarState* state = reinterpret_cast<CovarState*>(src.ptr);
  if (state->count == 0 || state->count == 1) return DoubleVal::null();
  double covar_samp = state->covar / (state->count - 1);
  return DoubleVal(covar_samp);
}

DoubleVal AggregateFunctions::CovarPopulationGetValue(FunctionContext* ctx,
    const StringVal& src) {
  // Calculating population covariance
  CovarState* state = reinterpret_cast<CovarState*>(src.ptr);
  if (state->count == 0) return DoubleVal::null();
  double covar_pop = state->covar / (state->count);
  return DoubleVal(covar_pop);
}

DoubleVal AggregateFunctions::CovarSampleFinalize(FunctionContext* ctx,
    const StringVal& src) {
  CovarState* state = reinterpret_cast<CovarState*>(src.ptr);
  if (UNLIKELY(src.is_null) || state->count == 0 || state->count == 1) {
    ctx->Free(src.ptr);
    return DoubleVal::null();
  }
  DoubleVal r = CovarSampleGetValue(ctx, src);
  ctx->Free(src.ptr);
  return r;
}

DoubleVal AggregateFunctions::CovarPopulationFinalize(FunctionContext* ctx,
    const StringVal& src) {
  CovarState* state = reinterpret_cast<CovarState*>(src.ptr);
  if (UNLIKELY(src.is_null) || state->count == 0) {
    ctx->Free(src.ptr);
    return DoubleVal::null();
  }
  DoubleVal r = CovarPopulationGetValue(ctx, src);
  ctx->Free(src.ptr);
  return r;
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
  if (tm_src.ToSubsecondUnixTime(UTCPTR, &val)) {
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
  if (tm_src.ToSubsecondUnixTime(UTCPTR, &val)) {
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
      val_struct->sum / val_struct->count, UTCPTR);
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

// We saw some failures on the release build because GCC was emitting an instruction
// to operate on the int128_t that assumed the pointer was aligned (typically it isn't).
// We mark the struct with a "packed" attribute, so that the compiler does not expect it
// to be aligned. This should not have a negative performance impact on modern CPUs.
struct __attribute__ ((__packed__)) DecimalAvgState {
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
  bool decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);

  // Since the src and dst are guaranteed to be the same scale, we can just
  // do a simple add.
  int m = remove ? -1 : 1;
  switch (ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0)) {
    case 4:
      avg->sum_val16 += m * src.val4;
      if (UNLIKELY(decimal_v2 &&
          abs(avg->sum_val16) > MAX_UNSCALED_DECIMAL16)) {
        ctx->SetError("Avg computation overflowed");
      }
      break;
    case 8:
      avg->sum_val16 += m * src.val8;
      if (UNLIKELY(decimal_v2 &&
          abs(avg->sum_val16) > MAX_UNSCALED_DECIMAL16)) {
        ctx->SetError("Avg computation overflowed");
      }
      break;
    case 16:
      if (UNLIKELY(decimal_v2 && (avg->sum_val16 >= 0) == (src.val16 >= 0) &&
          abs(avg->sum_val16) > MAX_UNSCALED_DECIMAL16 - abs(src.val16))) {
        // We can't check for overflow after performing the addition like in the other
        // cases because the result may not fit into int128.
        ctx->SetError("Avg computation overflowed");
      }
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
  bool decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  bool overflow = decimal_v2 &&
      abs(dst_struct->sum_val16) >
      MAX_UNSCALED_DECIMAL16 - abs(src_struct->sum_val16);
  if (UNLIKELY(overflow)) ctx->SetError("Avg computation overflowed");
  dst_struct->sum_val16 =
      ArithmeticUtil::AsUnsigned<std::plus>(dst_struct->sum_val16, src_struct->sum_val16);
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
  bool decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  Decimal16Value result = sum.Divide<int128_t>(sum_scale, count, 0 /* count's scale */,
      output_precision, output_scale, decimal_v2, &is_nan, &overflow);
  if (UNLIKELY(is_nan)) return DecimalVal::null();
  if (UNLIKELY(overflow)) {
    if (decimal_v2) {
      ctx->SetError("Avg computation overflowed");
    } else {
      ctx->AddWarning("Avg computation overflowed, returning NULL");
    }
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
  dst->val = ArithmeticUtil::Compute<std::plus, decltype(dst->val)>(dst->val, src.val);
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
  bool decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  // Since the src and dst are guaranteed to be the same scale, we can just
  // do a simple add.
  int m = subtract ? -1 : 1;
  if (precision <= 9) {
    dst->val16 += m * src.val4;
    if (UNLIKELY(decimal_v2 &&
        abs(dst->val16) > MAX_UNSCALED_DECIMAL16)) {
      ctx->SetError("Sum computation overflowed");
    }
  } else if (precision <= 19) {
    dst->val16 += m * src.val8;
    if (UNLIKELY(decimal_v2 &&
        abs(dst->val16) > MAX_UNSCALED_DECIMAL16)) {
      ctx->SetError("Sum computation overflowed");
    }
  } else {
    if (UNLIKELY(decimal_v2 && (dst->val16 >= 0) == (src.val16 >= 0) &&
        abs(dst->val16) > MAX_UNSCALED_DECIMAL16 - abs(src.val16))) {
      // We can't check for overflow after performing the addition like in the other
      // cases because the result may not fit into int128.
      ctx->SetError("Sum computation overflowed");
    }
    dst->val16 += m * src.val16;
  }
}

void AggregateFunctions::SumDecimalMerge(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) InitZero<DecimalVal>(ctx, dst);
  bool decimal_v2 = ctx->impl()->GetConstFnAttr(FunctionContextImpl::DECIMAL_V2);
  bool overflow = decimal_v2 &&
      abs(dst->val16) > MAX_UNSCALED_DECIMAL16 - abs(src.val16);
  if (UNLIKELY(overflow)) ctx->SetError("Sum computation overflowed");
  dst->val16 = ArithmeticUtil::AsUnsigned<std::plus>(dst->val16, src.val16);
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

// For DoubleVal and FloatVal, we have to handle NaN specially.  If 'val' != 'val', then
// 'val' must be NaN, and if any of the values that are inserted are NaN, then we return
// NaN. So, if 'src.val != src.val', set 'dst' to it.
template <>
void AggregateFunctions::Min(FunctionContext*, const FloatVal& src, FloatVal* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val < dst->val || src.val != src.val) *dst = src;
}

template <>
void AggregateFunctions::Max(FunctionContext*, const FloatVal& src, FloatVal* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val > dst->val || src.val != src.val) *dst = src;
}

template <>
void AggregateFunctions::Min(FunctionContext*, const DoubleVal& src, DoubleVal* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val < dst->val || src.val != src.val) *dst = src;
}

template <>
void AggregateFunctions::Max(FunctionContext*, const DoubleVal& src, DoubleVal* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val > dst->val || src.val != src.val) *dst = src;
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
    ctx->SetError(Substitute(ERROR_CONCATENATED_STRING_MAX_SIZE_REACHED,
      PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
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
    ctx->SetError(Substitute(ERROR_CONCATENATED_STRING_MAX_SIZE_REACHED,
      PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
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
  // Replace ranlux64_3 with mt19937_64 for better performance. See boost benchmark at
  // https://www.boost.org/doc/libs/1_74_0/doc/html/boost_random/performance.html
  mt19937_64 rng_;

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
  *os << TimestampValue::FromTimestampVal(v.val);
}

template <>
void PrintSample(const ReservoirSample<DateVal>& v, ostream* os) {
  *os << DateValue::FromDateVal(v.val);
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

// Compute the precision from a scale value.
static inline int ComputePrecisionFromScale(int scale) {
  return scale + 8;
}

// Compute the precision from a scale value. This method must be identical
// to function ComputeHllLengthFromScale() defined in FunctionCallExpr.java.
static inline int ComputeHllLengthFromScale(int scale) {
  return 1 << ComputePrecisionFromScale(scale);
}

// Compute the precision from a hll length as log2(len) or # of trailing
// zeros in length. For example, when len is 1024 = 2^10 = 0b10000000000,
// precision = 10.
static inline int ComputePrecisionFromHllLength(int hll_len) {
  return BitUtil::CountTrailingZeros((unsigned int)hll_len, sizeof(hll_len) * CHAR_BIT);
}

// Verify that the length of the intermediate data type is computable from
// the precision as represented in the 2nd argument.
static inline bool CheckHllArgs(FunctionContext* ctx, StringVal* dst) {
  if (ctx->GetNumArgs() == 2) {
    IntVal* int_val = reinterpret_cast<IntVal*>(ctx->GetConstantArg(1));

    // In parallel plan, the merge() function takes only one argument which
    // is the intermediate data. Avoid check in such cases.
    if (int_val) {
      return dst->len == ComputeHllLengthFromScale(int_val->val);
    }
  }
  return true;
}

void AggregateFunctions::HllInit(FunctionContext* ctx, StringVal* dst) {
  // The HLL functions use a preallocated FIXED_UDA_INTERMEDIATE intermediate value.
  DCHECK_IN_RANGE(dst->len, MIN_HLL_LEN, MAX_HLL_LEN);
  memset(dst->ptr, 0, dst->len);
  DCHECK(CheckHllArgs(ctx, dst));
}

// Implementation for update functions. It accepts a precision.
// Always inline in IR so that constants can be replaced.
template <typename T>
IR_ALWAYS_INLINE void AggregateFunctions::HllUpdate(
    FunctionContext* ctx, const T& src, StringVal* dst, int precision) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);

  const int& hll_len = dst->len;
  DCHECK_IN_RANGE(hll_len, MIN_HLL_LEN, MAX_HLL_LEN);

  uint64_t hash_value =
      AnyValUtil::Hash64(src, *ctx->GetArgType(0), HashUtil::FNV64_SEED);
  // Use the lower bits to index into the number of streams and then find the first 1 bit
  // after the index bits.
  int idx = hash_value & (hll_len - 1);
  const uint8_t first_one_bit = 1
      + BitUtil::CountTrailingZeros(
            hash_value >> precision, sizeof(hash_value) * CHAR_BIT - precision);
  dst->ptr[idx] = ::max(dst->ptr[idx], first_one_bit);
}

// Update function for NDV() that accepts an expression only.
template <typename T>
void AggregateFunctions::HllUpdate(FunctionContext* ctx, const T& src, StringVal* dst) {
  HllUpdate(ctx, src, dst, DEFAULT_HLL_PRECISION);
}

// Update function for NDV() that accepts an expression and a scale value.
template <typename T>
void AggregateFunctions::HllUpdate(
    FunctionContext* ctx, const T& src1, const IntVal& src2, StringVal* dst) {
  HllUpdate(ctx, src1, dst, ComputePrecisionFromScale(src2.val));
}

// Implementation for update functions for DecimalVal to allow substituting decimal
// size. It accepts a precision. Always inline in IR so that constants can be replaced.
template <>
IR_ALWAYS_INLINE void AggregateFunctions::HllUpdate(
    FunctionContext* ctx, const DecimalVal& src, StringVal* dst, int precision) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);

  const int& hll_len = dst->len;
  DCHECK_IN_RANGE(hll_len, MIN_HLL_LEN, MAX_HLL_LEN);

  int byte_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  uint64_t hash_value = AnyValUtil::HashDecimal64(src, byte_size, HashUtil::FNV64_SEED);
  if (hash_value != 0) {
    // Use the lower bits to index into the number of streams and then
    // find the first 1 bit after the index bits.
    int idx = hash_value & (hll_len - 1);
    uint8_t first_one_bit = __builtin_ctzl(hash_value >> precision) + 1;
    dst->ptr[idx] = ::max(dst->ptr[idx], first_one_bit);
  }
}

// Specialized update function for NDV() that accepts a decimal typed expression.
template <>
void AggregateFunctions::HllUpdate(
    FunctionContext* ctx, const DecimalVal& src, StringVal* dst) {
  HllUpdate(ctx, src, dst, DEFAULT_HLL_PRECISION);
}

// Specialized update function for NDV() that accepts a decimal typed expression
// and a scale value.
template <>
void AggregateFunctions::HllUpdate(
    FunctionContext* ctx, const DecimalVal& src1, const IntVal& src2, StringVal* dst) {
  HllUpdate(ctx, src1, dst, ComputePrecisionFromScale(src2.val));
}

void AggregateFunctions::HllMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK(!src.is_null);
  DCHECK_IN_RANGE(src.len, MIN_HLL_LEN, MAX_HLL_LEN);
  DCHECK_EQ(src.len, dst->len);

  for (int i = 0; i < src.len; ++i) {
    dst->ptr[i] = ::max(dst->ptr[i], src.ptr[i]);
  }
}

uint64_t AggregateFunctions::HllFinalEstimate(const uint8_t* buckets, int hll_len) {
  DCHECK(buckets != NULL);

  // Empirical constants for the algorithm.
  double alpha = 0;
  DCHECK_IN_RANGE(hll_len, MIN_HLL_LEN, MAX_HLL_LEN);
  int precision = ComputePrecisionFromHllLength(hll_len);

  if (hll_len == 16) {
    alpha = 0.673;
  } else if (hll_len == 32) {
    alpha = 0.697;
  } else if (hll_len == 64) {
    alpha = 0.709;
  } else {
    alpha = 0.7213 / (1 + 1.079 / hll_len);
  }

  double harmonic_mean = 0;
  int num_zero_registers = 0;
  for (int i = 0; i < hll_len; ++i) {
    harmonic_mean += ldexp(1.0, -buckets[i]);
    if (buckets[i] == 0) ++num_zero_registers;
  }
  harmonic_mean = 1.0 / harmonic_mean;

  // The actual harmonic mean is hll_len * harmonic_mean.
  int64_t estimate = alpha * hll_len * hll_len * harmonic_mean;
  // Adjust for Hll bias based on Hll++ algorithm
  if (estimate <= 5 * hll_len) {
    estimate -= HllEstimateBias(estimate, precision);
  }

  if (num_zero_registers == 0) return estimate;

  // Estimated cardinality is too low. Hll is too inaccurate here, instead use
  // linear counting.
  int64_t h = hll_len * log(static_cast<double>(hll_len) / num_zero_registers);

  return (h <= HllThreshold(precision)) ? h : estimate;
}

BigIntVal AggregateFunctions::HllFinalize(FunctionContext* ctx, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return BigIntVal::null();
  uint64_t estimate = HllFinalEstimate(src.ptr, src.len);
  return estimate;
}

/// Auxiliary function that receives a hll_sketch and returns the serialized version of
/// it wrapped into a StringVal.
/// Introducing this function in the .cc to avoid including the whole DataSketches HLL
/// functionality into the header.
StringVal SerializeCompactDsHllSketch(FunctionContext* ctx,
    const datasketches::hll_sketch& sketch) {
  auto bytes = sketch.serialize_compact();
  StringVal result(ctx, bytes.size());
  memcpy(result.ptr, bytes.data(), bytes.size());
  return result;
}

/// Auxiliary function that receives a hll_union, gets the underlying HLL sketch from the
/// union object and returns the serialized, compacted HLL sketch wrapped into StringVal.
/// Introducing this function in the .cc to avoid including the whole DataSketches HLL
/// functionality into the header.
StringVal SerializeDsHllUnion(FunctionContext* ctx,
    const datasketches::hll_union& ds_union) {
  datasketches::hll_sketch sketch = ds_union.get_result(DS_HLL_TYPE);
  return SerializeCompactDsHllSketch(ctx, sketch);
}

/// Auxiliary function that receives a cpc_union, gets the underlying CPC sketch from the
/// union object and returns the serialized, CPC sketch wrapped into StringVal.
/// Introducing this function in the .cc to avoid including the whole DataSketches CPC
/// functionality into the header.
StringVal SerializeDsCpcUnion(
    FunctionContext* ctx, const datasketches::cpc_union& ds_union) {
  datasketches::cpc_sketch sketch = ds_union.get_result();
  return SerializeDsSketch(ctx, sketch);
}

/// Auxiliary function that receives a theta_union, gets the underlying Theta sketch from
/// the union object and returns the serialized, Theta sketch wrapped into StringVal.
/// Introducing this function in the .cc to avoid including the whole DataSketches Theta
/// functionality into the header.
StringVal SerializeDsThetaUnion(
    FunctionContext* ctx, const datasketches::theta_union& ds_union) {
  datasketches::compact_theta_sketch sketch = ds_union.get_result();
  return SerializeDsSketch(ctx, sketch);
}

/// Auxiliary function that receives a theta_intersection, gets the underlying Theta
/// sketch from the intersection object and returns the serialized, compacted Theta sketch
/// wrapped into StringVal (may be null).
/// Introducing this function in the .cc to avoid including the whole DataSketches Theta
/// functionality into the header.
StringVal SerializeDsThetaIntersection(
    FunctionContext* ctx, const datasketches::theta_intersection& ds_intersection) {
  // Calling get_result() before calling update() is undefined, so you need to check.
  if (ds_intersection.has_result()) {
    datasketches::compact_theta_sketch sketch = ds_intersection.get_result();
    return SerializeDsSketch(ctx, sketch);
  }
  return StringVal::null();
}

// This is for functions with different state during update and merge.
enum agg_phase { UPDATE, MERGE };
using agg_state = std::pair<agg_phase, void*>;

void AggregateFunctions::DsHllInit(FunctionContext* ctx, StringVal* dst) {
  AllocBuffer(ctx, dst, sizeof(agg_state));
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  agg_state_ptr->first = agg_phase::UPDATE;
  agg_state_ptr->second = new (ctx->Allocate<datasketches::hll_sketch>())
      datasketches::hll_sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
}

template <typename T>
void AggregateFunctions::DsHllUpdate(FunctionContext* ctx, const T& src,
    StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  DCHECK_EQ(agg_state_ptr->first, agg_phase::UPDATE);
  auto sketch_ptr = reinterpret_cast<datasketches::hll_sketch*>(agg_state_ptr->second);
  sketch_ptr->update(src.val);
}

// Specialize for StringVal
template <>
void AggregateFunctions::DsHllUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null || src.len == 0) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  DCHECK_EQ(agg_state_ptr->first, agg_phase::UPDATE);
  auto sketch_ptr = reinterpret_cast<datasketches::hll_sketch*>(agg_state_ptr->second);
  sketch_ptr->update(reinterpret_cast<char*>(src.ptr), src.len);
}

StringVal AggregateFunctions::DsHllSerialize(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  StringVal dst;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr = reinterpret_cast<datasketches::hll_sketch*>(agg_state_ptr->second);
    dst = SerializeCompactDsHllSketch(ctx, *sketch_ptr);
    sketch_ptr->~hll_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::hll_union*>(agg_state_ptr->second);
    dst = SerializeDsHllUnion(ctx, *union_ptr);
    union_ptr->~hll_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsHllMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  if (agg_state_ptr->first == agg_phase::MERGE) { // was already switched to union
    auto dst_union_ptr =
        reinterpret_cast<datasketches::hll_union*>(agg_state_ptr->second);
    dst_union_ptr->update(datasketches::hll_sketch::deserialize(src.ptr, src.len));
  } else { // must be the first call. the state is still a sketch
    auto dst_sketch_ptr =
        reinterpret_cast<datasketches::hll_sketch*>(agg_state_ptr->second);

    datasketches::hll_union u(DS_SKETCH_CONFIG);
    u.update(*dst_sketch_ptr);
    u.update(datasketches::hll_sketch::deserialize(src.ptr, src.len));

    // swich to union
    dst_sketch_ptr->~hll_sketch_alloc();
    ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
    agg_state_ptr->second = new (ctx->Allocate<datasketches::hll_union>())
        datasketches::hll_union(std::move(u));
    agg_state_ptr->first = agg_phase::MERGE;
  }
}

BigIntVal AggregateFunctions::DsHllFinalize(FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  BigIntVal estimate;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr = reinterpret_cast<datasketches::hll_sketch*>(agg_state_ptr->second);
    estimate = sketch_ptr->get_estimate();
    sketch_ptr->~hll_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::hll_union*>(agg_state_ptr->second);
    estimate = union_ptr->get_result().get_estimate();
    union_ptr->~hll_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return (estimate == 0) ? BigIntVal::null() : estimate;
}

StringVal AggregateFunctions::DsHllFinalizeSketch(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  StringVal result = StringVal::null();
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr = reinterpret_cast<datasketches::hll_sketch*>(agg_state_ptr->second);
    if (!sketch_ptr->is_empty()) {
      result = SerializeCompactDsHllSketch(ctx, *sketch_ptr);
    }
    sketch_ptr->~hll_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::hll_union*>(agg_state_ptr->second);
    auto sketch = union_ptr->get_result(DS_HLL_TYPE);
    if (!sketch.is_empty()) {
      result = SerializeCompactDsHllSketch(ctx, sketch);
    }
    union_ptr->~hll_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsHllUnionInit(FunctionContext* ctx, StringVal* slot) {
  AllocBuffer(ctx, slot, sizeof(datasketches::hll_union));
  if (UNLIKELY(slot->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto union_ptr = reinterpret_cast<datasketches::hll_union*>(slot->ptr);
  new (union_ptr) datasketches::hll_union(DS_SKETCH_CONFIG);
}

void AggregateFunctions::DsHllUnionUpdate(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::hll_union));
  try {
    reinterpret_cast<datasketches::hll_union*>(dst->ptr)
        ->update(datasketches::hll_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsHllUnionSerialize(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::hll_union));
  datasketches::hll_union* union_ptr =
      reinterpret_cast<datasketches::hll_union*>(src.ptr);
  StringVal dst = SerializeDsHllUnion(ctx, *union_ptr);
  union_ptr->~hll_union_alloc();
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsHllUnionMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::hll_union));
  // Note, 'src' is a serialized hll_sketch and not a serialized hll_union.
  try {
    reinterpret_cast<datasketches::hll_union*>(dst->ptr)
        ->update(datasketches::hll_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsHllUnionFinalize(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::hll_union));
  auto union_ptr = reinterpret_cast<datasketches::hll_union*>(src.ptr);
  auto sketch = union_ptr->get_result(DS_HLL_TYPE);
  StringVal result = StringVal::null();
  if (!sketch.is_empty()) {
    result = SerializeCompactDsHllSketch(ctx, sketch);
  }
  union_ptr->~hll_union_alloc();
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsCpcInit(FunctionContext* ctx, StringVal* dst) {
  AllocBuffer(ctx, dst, sizeof(agg_state));
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  agg_state_ptr->first = agg_phase::UPDATE;
  agg_state_ptr->second = new (ctx->Allocate<datasketches::cpc_sketch>())
      datasketches::cpc_sketch(DS_CPC_SKETCH_CONFIG);
}

template <typename T>
void AggregateFunctions::DsCpcUpdate(FunctionContext* ctx, const T& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  DCHECK_EQ(agg_state_ptr->first, agg_phase::UPDATE);
  auto sketch_ptr = reinterpret_cast<datasketches::cpc_sketch*>(agg_state_ptr->second);
  sketch_ptr->update(src.val);
}

// Specialize for StringVal
template <>
void AggregateFunctions::DsCpcUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null || src.len == 0) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  DCHECK(agg_state_ptr->first == agg_phase::UPDATE);
  auto sketch_ptr = reinterpret_cast<datasketches::cpc_sketch*>(agg_state_ptr->second);
  sketch_ptr->update(reinterpret_cast<char*>(src.ptr), src.len);
}

StringVal AggregateFunctions::DsCpcSerialize(FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  StringVal dst;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr = reinterpret_cast<datasketches::cpc_sketch*>(agg_state_ptr->second);
    dst = SerializeDsSketch(ctx, *sketch_ptr);
    sketch_ptr->~cpc_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::cpc_union*>(agg_state_ptr->second);
    dst = SerializeDsCpcUnion(ctx, *union_ptr);
    union_ptr->~cpc_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsCpcMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  if (agg_state_ptr->first == agg_phase::MERGE) { // was already switched to union
    auto dst_union_ptr =
        reinterpret_cast<datasketches::cpc_union*>(agg_state_ptr->second);
    dst_union_ptr->update(datasketches::cpc_sketch::deserialize(src.ptr, src.len));
  } else { // must be the first call. the state is still a sketch
    auto dst_sketch_ptr =
        reinterpret_cast<datasketches::cpc_sketch*>(agg_state_ptr->second);

    datasketches::cpc_union u(DS_CPC_SKETCH_CONFIG);
    u.update(*dst_sketch_ptr);
    try {
      u.update(datasketches::cpc_sketch::deserialize(src.ptr, src.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return;
    }

    // switch to union
    dst_sketch_ptr->~cpc_sketch_alloc();
    ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
    agg_state_ptr->second = new (ctx->Allocate<datasketches::cpc_union>())
        datasketches::cpc_union(std::move(u));
    agg_state_ptr->first = agg_phase::MERGE;
  }
}

BigIntVal AggregateFunctions::DsCpcFinalize(FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  BigIntVal estimate;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr = reinterpret_cast<datasketches::cpc_sketch*>(agg_state_ptr->second);
    estimate = sketch_ptr->get_estimate();
    sketch_ptr->~cpc_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::cpc_union*>(agg_state_ptr->second);
    estimate = union_ptr->get_result().get_estimate();
    union_ptr->~cpc_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return (estimate == 0) ? BigIntVal::null() : estimate;
}

StringVal AggregateFunctions::DsCpcFinalizeSketch(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  StringVal result = StringVal::null();
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr = reinterpret_cast<datasketches::cpc_sketch*>(agg_state_ptr->second);
    if (!sketch_ptr->is_empty()) {
      result = SerializeDsSketch(ctx, *sketch_ptr);
    }
    sketch_ptr->~cpc_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::cpc_union*>(agg_state_ptr->second);
    auto sketch = union_ptr->get_result();
    if (!sketch.is_empty()) {
      result = SerializeDsSketch(ctx, sketch);
    }
    union_ptr->~cpc_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsCpcUnionInit(FunctionContext* ctx, StringVal* slot) {
  AllocBuffer(ctx, slot, sizeof(datasketches::cpc_union));
  if (UNLIKELY(slot->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto union_ptr = reinterpret_cast<datasketches::cpc_union*>(slot->ptr);
  new (union_ptr) datasketches::cpc_union(DS_CPC_SKETCH_CONFIG);
}

void AggregateFunctions::DsCpcUnionUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::cpc_union));
  try {
    reinterpret_cast<datasketches::cpc_union*>(dst->ptr)
        ->update(datasketches::cpc_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsCpcUnionSerialize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::cpc_union));
  auto union_ptr = reinterpret_cast<datasketches::cpc_union*>(src.ptr);
  StringVal dst = SerializeDsCpcUnion(ctx, *union_ptr);
  union_ptr->~cpc_union_alloc();
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsCpcUnionMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::cpc_union));
  // Note, 'src' is a serialized Cpc_sketch and not a serialized Cpc_union.
  try {
    reinterpret_cast<datasketches::cpc_union*>(dst->ptr)
        ->update(datasketches::cpc_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsCpcUnionFinalize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::cpc_union));
  auto union_ptr = reinterpret_cast<datasketches::cpc_union*>(src.ptr);
  auto sketch = union_ptr->get_result();
  StringVal result = StringVal::null();
  if (!sketch.is_empty()) {
    result = SerializeDsSketch(ctx, sketch);
  }
  union_ptr->~cpc_union_alloc();
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsThetaInit(FunctionContext* ctx, StringVal* dst) {
  AllocBuffer(ctx, dst, sizeof(agg_state));
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  agg_state_ptr->first = agg_phase::UPDATE;
  agg_state_ptr->second = new (ctx->Allocate<datasketches::update_theta_sketch>())
      datasketches::update_theta_sketch(
          datasketches::update_theta_sketch::builder().build());
}

template <typename T>
void AggregateFunctions::DsThetaUpdate(
    FunctionContext* ctx, const T& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  DCHECK_EQ(agg_state_ptr->first, agg_phase::UPDATE);
  auto sketch_ptr =
      reinterpret_cast<datasketches::update_theta_sketch*>(agg_state_ptr->second);
  sketch_ptr->update(src.val);
}

// Specialize for StringVal
template <>
void AggregateFunctions::DsThetaUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null || src.len == 0) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);
  DCHECK_EQ(agg_state_ptr->first, agg_phase::UPDATE);
  auto sketch_ptr =
      reinterpret_cast<datasketches::update_theta_sketch*>(agg_state_ptr->second);
  sketch_ptr->update(reinterpret_cast<char*>(src.ptr), src.len);
}

StringVal AggregateFunctions::DsThetaSerialize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  StringVal dst;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr =
        reinterpret_cast<datasketches::update_theta_sketch*>(agg_state_ptr->second);
    dst = SerializeDsSketch(ctx, sketch_ptr->compact());
    sketch_ptr->~update_theta_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::theta_union*>(agg_state_ptr->second);
    dst = SerializeDsThetaUnion(ctx, *union_ptr);
    union_ptr->~theta_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsThetaMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(agg_state));
  auto agg_state_ptr = reinterpret_cast<agg_state*>(dst->ptr);

  // Note, 'src' is a serialized compact_theta_sketch.
  if (agg_state_ptr->first == agg_phase::MERGE) { // was already switched to union
    auto dst_union_ptr =
        reinterpret_cast<datasketches::theta_union*>(agg_state_ptr->second);
    try {
      dst_union_ptr->update(datasketches::compact_theta_sketch::deserialize(src.ptr,
          src.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return;
    }
  } else { // must be the first call. the state is still a sketch
    auto dst_sketch_ptr =
        reinterpret_cast<datasketches::update_theta_sketch*>(agg_state_ptr->second);

    auto u = datasketches::theta_union::builder().build();
    u.update(*dst_sketch_ptr);
    try {
      u.update(datasketches::compact_theta_sketch::deserialize(src.ptr, src.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return;
    }

    // switch to union
    dst_sketch_ptr->~update_theta_sketch_alloc();
    ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
    agg_state_ptr->second = new (ctx->Allocate<datasketches::theta_union>())
        datasketches::theta_union(std::move(u));
    agg_state_ptr->first = agg_phase::MERGE;
  }
}

BigIntVal AggregateFunctions::DsThetaFinalize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  BigIntVal estimate;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr =
        reinterpret_cast<datasketches::update_theta_sketch*>(agg_state_ptr->second);
    estimate = sketch_ptr->get_estimate();
    sketch_ptr->~update_theta_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::theta_union*>(agg_state_ptr->second);
    estimate = union_ptr->get_result().get_estimate();
    union_ptr->~theta_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return estimate;
}

StringVal AggregateFunctions::DsThetaFinalizeSketch(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(agg_state));
  StringVal result;
  auto agg_state_ptr = reinterpret_cast<agg_state*>(src.ptr);
  if (agg_state_ptr->first == agg_phase::UPDATE) { // the agg state is a sketch
    auto sketch_ptr =
        reinterpret_cast<datasketches::update_theta_sketch*>(agg_state_ptr->second);
    result = SerializeDsSketch(ctx, sketch_ptr->compact());
    sketch_ptr->~update_theta_sketch_alloc();
  } else { // the agg state is a union
    auto union_ptr = reinterpret_cast<datasketches::theta_union*>(agg_state_ptr->second);
    result = SerializeDsThetaUnion(ctx, *union_ptr);
    union_ptr->~theta_union_alloc();
  }
  ctx->Free(reinterpret_cast<uint8_t*>(agg_state_ptr->second));
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsThetaUnionInit(FunctionContext* ctx, StringVal* dst) {
  AllocBuffer(ctx, dst, sizeof(datasketches::theta_union));
  if (UNLIKELY(dst->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto union_ptr = reinterpret_cast<datasketches::theta_union*>(dst->ptr);
  new (union_ptr) datasketches::theta_union(datasketches::theta_union::builder().build());
}

void AggregateFunctions::DsThetaUnionUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::theta_union));
  try {
    reinterpret_cast<datasketches::theta_union*>(dst->ptr)
        ->update(datasketches::compact_theta_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsThetaUnionSerialize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::theta_union));
  auto union_ptr = reinterpret_cast<datasketches::theta_union*>(src.ptr);
  StringVal dst = SerializeDsThetaUnion(ctx, *union_ptr);
  union_ptr->~theta_union_alloc();
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsThetaUnionMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::theta_union));
  // Note, 'src' is a serialized compact_theta_sketch and not a serialized theta_union.
  try {
    reinterpret_cast<datasketches::theta_union*>(dst->ptr)
        ->update(datasketches::compact_theta_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsThetaUnionFinalize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::theta_union));
  auto union_ptr = reinterpret_cast<datasketches::theta_union*>(src.ptr);
  auto sketch = union_ptr->get_result();
  StringVal result = StringVal::null();
  if (!sketch.is_empty()) {
    result = SerializeDsSketch(ctx, sketch);
  }
  union_ptr->~theta_union_alloc();
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsThetaIntersectInit(FunctionContext* ctx, StringVal* slot) {
  AllocBuffer(ctx, slot, sizeof(datasketches::theta_intersection));
  if (UNLIKELY(slot->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  auto intersection_ptr = reinterpret_cast<datasketches::theta_intersection*>(slot->ptr);
  new (intersection_ptr) datasketches::theta_intersection();
}

void AggregateFunctions::DsThetaIntersectUpdate(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::theta_intersection));
  try {
    reinterpret_cast<datasketches::theta_intersection*>(dst->ptr)
        ->update(datasketches::compact_theta_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsThetaIntersectSerialize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::theta_intersection));
  auto intersection_ptr = reinterpret_cast<datasketches::theta_intersection*>(src.ptr);
  StringVal dst = SerializeDsThetaIntersection(ctx, *intersection_ptr);
  intersection_ptr->~theta_intersection_alloc();
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsThetaIntersectMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::theta_intersection));
  // Note, 'src' is a serialized compact_theta_sketch and not a serialized
  // theta_intersection.
  try {
    reinterpret_cast<datasketches::theta_intersection*>(dst->ptr)
        ->update(datasketches::compact_theta_sketch::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
  }
}

StringVal AggregateFunctions::DsThetaIntersectFinalize(
    FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::theta_intersection));
  auto intersection_ptr = reinterpret_cast<datasketches::theta_intersection*>(src.ptr);
  StringVal result = StringVal::null();
  if (intersection_ptr->has_result()) {
    result = SerializeDsSketch(ctx, intersection_ptr->get_result());
  }
  intersection_ptr->~theta_intersection_alloc();
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::DsKllInitHelper(FunctionContext* ctx, StringVal* slot) {
  AllocBuffer(ctx, slot, sizeof(datasketches::kll_sketch<float>));
  if (UNLIKELY(slot->is_null)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return;
  }
  // Note, that kll_sketch will always have the same size regardless of the amount of
  // data it keeps track of. This is because it's a wrapper class that holds all the
  // inserted data on heap. Here, we put only the wrapper class into a StringVal.
  auto sketch_ptr = reinterpret_cast<datasketches::kll_sketch<float>*>(slot->ptr);
  new (sketch_ptr) datasketches::kll_sketch<float>();
}

StringVal AggregateFunctions::DsKllSerializeHelper(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::kll_sketch<float>));
  auto sketch_ptr = reinterpret_cast<datasketches::kll_sketch<float>*>(src.ptr);
  StringVal dst = SerializeDsSketch(ctx, *sketch_ptr);
  sketch_ptr->~kll_sketch();
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsKllMergeHelper(FunctionContext* ctx, const StringVal& src,
      StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::kll_sketch<float>));

  auto dst_sketch_ptr = reinterpret_cast<datasketches::kll_sketch<float>*>(dst->ptr);
  try {
    dst_sketch_ptr->merge(datasketches::kll_sketch<float>::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    ctx->SetError(Substitute("Error while merging DataSketches KLL sketches. "
        "Message: $0", e.what()).c_str());
    return;
  }
}

StringVal AggregateFunctions::DsKllFinalizeHelper(FunctionContext* ctx,
    const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, sizeof(datasketches::kll_sketch<float>));
  auto sketch_ptr = reinterpret_cast<datasketches::kll_sketch<float>*>(src.ptr);
  StringVal dst = StringVal::null();
  if (!sketch_ptr->is_empty()) {
    dst = SerializeDsSketch(ctx, *sketch_ptr);
  }
  sketch_ptr->~kll_sketch();
  ctx->Free(src.ptr);
  return dst;
}

void AggregateFunctions::DsKllInit(FunctionContext* ctx, StringVal* dst) {
  DsKllInitHelper(ctx, dst);
}

void AggregateFunctions::DsKllUpdate(FunctionContext* ctx, const FloatVal& src,
    StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::kll_sketch<float>));
  auto sketch_ptr = reinterpret_cast<datasketches::kll_sketch<float>*>(dst->ptr);
  sketch_ptr->update(src.val);
}

StringVal AggregateFunctions::DsKllSerialize(FunctionContext* ctx,
    const StringVal& src) {
  return DsKllSerializeHelper(ctx, src);
}

void AggregateFunctions::DsKllMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  DsKllMergeHelper(ctx, src, dst);
}

StringVal AggregateFunctions::DsKllFinalizeSketch(FunctionContext* ctx,
    const StringVal& src) {
  return DsKllFinalizeHelper(ctx, src);
}

void AggregateFunctions::DsKllUnionInit(FunctionContext* ctx, StringVal* slot) {
  // Note, comparing to HLL Union with hll_union type, for KLL Union there is no such
  // type as kll_union. As a result kll_sketch is used here to store intermediate results
  // of the union operation.
  DsKllInitHelper(ctx, slot);
}

void AggregateFunctions::DsKllUnionUpdate(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, sizeof(datasketches::kll_sketch<float>));
  auto dst_sketch = reinterpret_cast<datasketches::kll_sketch<float>*>(dst->ptr);
  try {
    dst_sketch->merge(datasketches::kll_sketch<float>::deserialize(src.ptr, src.len));
  } catch (const std::exception& e) {
    ctx->SetError(Substitute("Error while merging DataSketches KLL sketches. "
        "Message: $0", e.what()).c_str());
    return;
  }
}

StringVal AggregateFunctions::DsKllUnionSerialize(FunctionContext* ctx,
    const StringVal& src) {
  return DsKllSerializeHelper(ctx, src);
}

void AggregateFunctions::DsKllUnionMerge(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  DsKllMergeHelper(ctx, src, dst);
}

StringVal AggregateFunctions::DsKllUnionFinalize(FunctionContext* ctx,
    const StringVal& src) {
  return DsKllFinalizeHelper(ctx, src);
}

/// Intermediate aggregation state for the SampledNdv() function.
/// Stores NUM_HLL_BUCKETS of the form <row_count, hll_state>.
/// The 'row_count' keeps track of how many input rows were aggregated into that
/// bucket, and the 'hll_state' is an intermediate aggregation state of HyperLogLog.
/// See the header comments on the SampledNdv() function for more details.
class SampledNdvState {
 public:
  /// Empirically determined number of HLL buckets. Power of two for fast modulo.
  static const uint32_t NUM_HLL_BUCKETS = 32;

  /// A bucket contains an update count and an HLL intermediate state.
  static constexpr int64_t BUCKET_SIZE =
      sizeof(int64_t) + AggregateFunctions::DEFAULT_HLL_LEN;

  /// Sampling percent which was given as the second argument to SampledNdv().
  /// Stored here to avoid existing issues with passing constant arguments to all
  /// aggregation phases and because we convert the sampling percent argument from
  /// decimal to double. See IMPALA-6179.
  double sample_perc;

  /// Counts the number of Update() calls. Used for determining which bucket to update.
  int64_t total_row_count;

  /// Array of buckets.
  struct {
    int64_t row_count;
    uint8_t hll[AggregateFunctions::DEFAULT_HLL_LEN];
  } buckets[NUM_HLL_BUCKETS];
};

void AggregateFunctions::SampledNdvInit(FunctionContext* ctx, StringVal* dst) {
  // Uses a preallocated FIXED_UDA_INTERMEDIATE intermediate value.
  DCHECK_EQ(dst->len, sizeof(SampledNdvState));
  memset(dst->ptr, 0, sizeof(SampledNdvState));

  DoubleVal* sample_perc = reinterpret_cast<DoubleVal*>(ctx->GetConstantArg(1));
  if (sample_perc == nullptr) return;
  // Guaranteed by the FE.
  DCHECK(!sample_perc->is_null);
  DCHECK_GE(sample_perc->val, 0.0);
  DCHECK_LE(sample_perc->val, 1.0);
  SampledNdvState* state = reinterpret_cast<SampledNdvState*>(dst->ptr);
  state->sample_perc = sample_perc->val;
}

/// Incorporate the 'src' into one of the intermediate HLLs, which will be used by
/// Finalize() to generate a set of the (x,y) data points.
template <typename T>
void AggregateFunctions::SampledNdvUpdate(FunctionContext* ctx, const T& src,
    const DoubleVal& sample_perc, StringVal* dst) {
  SampledNdvState* state = reinterpret_cast<SampledNdvState*>(dst->ptr);
  int64_t bucket_idx = state->total_row_count % SampledNdvState::NUM_HLL_BUCKETS;
  StringVal hll_dst = StringVal(state->buckets[bucket_idx].hll, DEFAULT_HLL_LEN);
  HllUpdate(ctx, src, &hll_dst);
  ++state->buckets[bucket_idx].row_count;
  ++state->total_row_count;
}

void AggregateFunctions::SampledNdvMerge(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  SampledNdvState* src_state = reinterpret_cast<SampledNdvState*>(src.ptr);
  SampledNdvState* dst_state = reinterpret_cast<SampledNdvState*>(dst->ptr);
  for (int i = 0; i < SampledNdvState::NUM_HLL_BUCKETS; ++i) {
    StringVal src_hll = StringVal(src_state->buckets[i].hll, DEFAULT_HLL_LEN);
    StringVal dst_hll = StringVal(dst_state->buckets[i].hll, DEFAULT_HLL_LEN);
    HllMerge(ctx, src_hll, &dst_hll);
    dst_state->buckets[i].row_count += src_state->buckets[i].row_count;
  }
  // Total count. Not really needed after Update() but kept for sanity checking.
  dst_state->total_row_count += src_state->total_row_count;
  // Propagate sampling percent to Finalize().
  dst_state->sample_perc = src_state->sample_perc;
}

BigIntVal AggregateFunctions::SampledNdvFinalize(FunctionContext* ctx,
    const StringVal& src) {
  SampledNdvState* state = reinterpret_cast<SampledNdvState*>(src.ptr);

  // Generate 'num_points' data points with x=row_count and y=ndv_estimate. These points
  // are used to fit a function for the NDV growth and estimate the real NDV.
  constexpr int num_points =
      SampledNdvState::NUM_HLL_BUCKETS * SampledNdvState::NUM_HLL_BUCKETS;
  int64_t counts[num_points] = { 0 };
  int64_t ndvs[num_points] = { 0 };

  int64_t min_ndv = numeric_limits<int64_t>::max();
  int64_t min_count = numeric_limits<int64_t>::max();
  // We have a fixed number of HLL intermediates to generate data points. Any unique
  // subset of intermediates can be combined to create a new data point. It was
  // empirically determined that 'num_data' points is typically sufficient and there are
  // diminishing returns from generating additional data points.
  // The generation method below was chosen for its simplicity. It successively merges
  // buckets in a rolling window of size NUM_HLL_BUCKETS. Repeating the last data point
  // where all buckets are merged biases the curve fitting to hit that data point which
  // makes sense because that's likely the most accurate one. The number of data points
  // are sufficient for reasonable accuracy.
  int pidx = 0;
  for (int i = 0; i < SampledNdvState::NUM_HLL_BUCKETS; ++i) {
    uint8_t merged_hll_data[DEFAULT_HLL_LEN];
    memset(merged_hll_data, 0, DEFAULT_HLL_LEN);
    StringVal merged_hll(merged_hll_data, DEFAULT_HLL_LEN);
    int64_t merged_count = 0;
    for (int j = 0; j < SampledNdvState::NUM_HLL_BUCKETS; ++j) {
      int bucket_idx = (i + j) % SampledNdvState::NUM_HLL_BUCKETS;
      merged_count += state->buckets[bucket_idx].row_count;
      counts[pidx] = merged_count;
      StringVal hll = StringVal(state->buckets[bucket_idx].hll, DEFAULT_HLL_LEN);
      HllMerge(ctx, hll, &merged_hll);
      ndvs[pidx] = HllFinalEstimate(merged_hll.ptr);
      ++pidx;
    }
    min_count = std::min(min_count, state->buckets[i].row_count);
    min_ndv = std::min(min_ndv, ndvs[i * SampledNdvState::NUM_HLL_BUCKETS]);
  }
  // Based on the point-generation method above the last elements represent the data
  // point where all buckets are merged.
  int64_t max_count = counts[num_points - 1];
  int64_t max_ndv = ndvs[num_points - 1];

  // Scale all values to [0,1] since some objective functions require it (e.g., Sigmoid).
  double count_scale = max_count - min_count;
  double ndv_scale = max_ndv - min_ndv;
  if (count_scale == 0) count_scale = 1.0;
  if (ndv_scale == 0) ndv_scale = 1.0;
  double scaled_counts[num_points];
  double scaled_ndvs[num_points];
  for (int i = 0; i < num_points; ++i) {
    scaled_counts[i] = counts[i] / count_scale;
    scaled_ndvs[i] = ndvs[i] / ndv_scale;
  }

  // List of objective functions. Curve fitting will select the best values for the
  // parameters a, b, c, d.
  vector<ObjectiveFunction> ndv_fns;
  // Linear function: f(x) = a + b * x
  ndv_fns.push_back(ObjectiveFunction("LIN", 2,
      [](double x, const double* params) -> double {
        return params[0] + params[1] * x;
      }
  ));
  // Logarithmic function: f(x) = a + b * log(x)
  ndv_fns.push_back(ObjectiveFunction("LOG", 2,
      [](double x, const double* params) -> double {
        return params[0] + params[1] * log(x);
      }
  ));
  // Power function: f(x) = a + b * pow(x, c)
  ndv_fns.push_back(ObjectiveFunction("POW", 3,
      [](double x, const double* params) -> double {
        return params[0] + params[1] * pow(x, params[2]);
      }
  ));
  // Sigmoid function: f(x) = a + b * (c / (c + pow(d, -x)))
  ndv_fns.push_back(ObjectiveFunction("SIG", 4,
      [](double x, const double* params) -> double {
        return params[0] + params[1] * (params[2] / (params[2] + pow(params[3], -x)));
      }
  ));

  // Perform least mean squares fitting on all objective functions.
  vector<ObjectiveFunction> valid_ndv_fns;
  for (ObjectiveFunction& f: ndv_fns) {
    if(f.LmsFit(scaled_counts, scaled_ndvs, num_points)) {
      valid_ndv_fns.push_back(std::move(f));
    }
  }

  // Select the best-fit function for estimating the NDV.
  auto best_fit_fn = min_element(valid_ndv_fns.begin(), valid_ndv_fns.end(),
      [](const ObjectiveFunction& a, const ObjectiveFunction& b) -> bool {
        return a.GetError() < b.GetError();
      }
  );

  // Compute the extrapolated NDV based on the extrapolated row count.
  double extrap_count = max_count / state->sample_perc;
  double scaled_extrap_count = extrap_count / count_scale;
  double scaled_extrap_ndv = best_fit_fn->GetY(scaled_extrap_count);
  return round(scaled_extrap_ndv * ndv_scale);
}

template <typename T>
void AggregateFunctions::AggIfUpdate(
    FunctionContext* ctx, const BooleanVal& cond, const T& src, T* dst) {
  DCHECK(!cond.is_null);
  if (cond.val) *dst = src;
}

template <>
void AggregateFunctions::AggIfUpdate(
    FunctionContext* ctx, const BooleanVal& cond, const StringVal& src, StringVal* dst) {
  DCHECK(!cond.is_null);
  if (cond.val) CopyStringVal(ctx, src, dst);
}

template <typename T>
void AggregateFunctions::AggIfMerge(FunctionContext*, const T& src, T* dst) {
  *dst = src;
}

template <>
void AggregateFunctions::AggIfMerge(
    FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  CopyStringVal(ctx, src, dst);
}

template <typename T>
T AggregateFunctions::AggIfFinalize(FunctionContext*, const T& src) {
  return src;
}

template <>
StringVal AggregateFunctions::AggIfFinalize(FunctionContext* ctx, const StringVal& src) {
  StringVal result = StringValGetValue(ctx, src);
  if (!src.is_null) ctx->Free(src.ptr);
  return result;
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
template void AggregateFunctions::UpdateVal<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::UpdateVal<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::UpdateVal<DateVal>(
    FunctionContext*, const DateVal& src, DateVal* dst);

template void AggregateFunctions::AvgUpdate<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& input, StringVal* dst);
template void AggregateFunctions::AvgUpdate<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& input, StringVal* dst);
template void AggregateFunctions::AvgUpdate<DateVal>(
    FunctionContext* ctx, const DateVal& input, StringVal* dst);
template void AggregateFunctions::AvgRemove<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& input, StringVal* dst);
template void AggregateFunctions::AvgRemove<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& input, StringVal* dst);
template void AggregateFunctions::AvgRemove<DateVal>(
    FunctionContext* ctx, const DateVal& input, StringVal* dst);

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
template void AggregateFunctions::Min<DateVal>(
    FunctionContext*, const DateVal& src, DateVal* dst);

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
template void AggregateFunctions::Max<DateVal>(
    FunctionContext*, const DateVal& src, DateVal* dst);

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
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const DateVal&, StringVal*);

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
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const DateVal&, StringVal*);

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
template void AggregateFunctions::ReservoirSampleInit<DateVal>(
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
template void AggregateFunctions::ReservoirSampleUpdate(
    FunctionContext*, const DateVal&, StringVal*);

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
template StringVal AggregateFunctions::ReservoirSampleSerialize<DateVal>(
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
template void AggregateFunctions::ReservoirSampleMerge<DateVal>(
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
template StringVal AggregateFunctions::ReservoirSampleFinalize<DateVal>(
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
template StringVal AggregateFunctions::HistogramFinalize<DateVal>(
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
template DateVal AggregateFunctions::AppxMedianFinalize<DateVal>(
    FunctionContext*, const StringVal&);

// Method instantiation for the implementation of the Update
// functions.
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BooleanVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const IntVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BigIntVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const FloatVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DoubleVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const StringVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TimestampVal&, StringVal*, int precision);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DateVal&, StringVal*, int precision);

// Method instantiation for NDV() that accepts a single argument. The
// NDV() is computed with a precision of 10.
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
    FunctionContext*, const DateVal&, StringVal*);

// Method instantiation for NDV() that accepts two arguments. The
// NDV() is computed with a precision indirectly specified through
// the 2nd argument.
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BooleanVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TinyIntVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const SmallIntVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const IntVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BigIntVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const FloatVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DoubleVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const StringVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TimestampVal&, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DateVal&, const IntVal&, StringVal*);

template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::DsHllUpdate(
    FunctionContext*, const DateVal&, StringVal*);

template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::DsCpcUpdate(
    FunctionContext*, const DateVal&, StringVal*);

template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::DsThetaUpdate(
    FunctionContext*, const DateVal&, StringVal*);

template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const BooleanVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const TinyIntVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const SmallIntVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const IntVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const BigIntVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const FloatVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const DoubleVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const StringVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const TimestampVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const DecimalVal&, const DoubleVal&, StringVal*);
template void AggregateFunctions::SampledNdvUpdate(
    FunctionContext*, const DateVal&, const DoubleVal&, StringVal*);

template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const IntVal& src, IntVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::AggIfUpdate(
    FunctionContext*, const BooleanVal& cond, const DateVal& src, DateVal* dst);

template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::AggIfMerge(
    FunctionContext*, const DateVal& src, DateVal* dst);

template BooleanVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const BooleanVal& src);
template TinyIntVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const TinyIntVal& src);
template SmallIntVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const SmallIntVal& src);
template IntVal AggregateFunctions::AggIfFinalize(FunctionContext*, const IntVal& src);
template BigIntVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const BigIntVal& src);
template FloatVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const FloatVal& src);
template DoubleVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const DoubleVal& src);
template TimestampVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const TimestampVal& src);
template DecimalVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const DecimalVal& src);
template DateVal AggregateFunctions::AggIfFinalize(
    FunctionContext*, const DateVal& src);

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
template void AggregateFunctions::LastValRemove<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::LastValRemove<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::LastValRemove<DateVal>(
    FunctionContext*, const DateVal& src, DateVal* dst);

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
template void AggregateFunctions::LastValIgnoreNullsInit<DateVal>(
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
template void AggregateFunctions::LastValIgnoreNullsUpdate<DateVal>(
    FunctionContext*, const DateVal& src, StringVal* dst);

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
template void AggregateFunctions::LastValIgnoreNullsRemove<DateVal>(
    FunctionContext*, const DateVal& src, StringVal* dst);

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
template TimestampVal AggregateFunctions::LastValIgnoreNullsGetValue<TimestampVal>(
    FunctionContext*, const StringVal&);
template DecimalVal AggregateFunctions::LastValIgnoreNullsGetValue<DecimalVal>(
    FunctionContext*, const StringVal&);
template DateVal AggregateFunctions::LastValIgnoreNullsGetValue<DateVal>(
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
template TimestampVal AggregateFunctions::LastValIgnoreNullsFinalize<TimestampVal>(
    FunctionContext*, const StringVal&);
template DecimalVal AggregateFunctions::LastValIgnoreNullsFinalize<DecimalVal>(
    FunctionContext*, const StringVal&);
template DateVal AggregateFunctions::LastValIgnoreNullsFinalize<DateVal>(
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
template void AggregateFunctions::FirstValUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::FirstValUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::FirstValUpdate<DateVal>(
    FunctionContext*, const DateVal& src, DateVal* dst);

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
template void AggregateFunctions::FirstValRewriteUpdate<DateVal>(
    FunctionContext*, const DateVal& src, const BigIntVal&, DateVal* dst);

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
template void AggregateFunctions::FirstValIgnoreNullsUpdate<TimestampVal>(
    FunctionContext*, const TimestampVal& src, TimestampVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::FirstValIgnoreNullsUpdate<DateVal>(
    FunctionContext*, const DateVal& src, DateVal* dst);

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
template void AggregateFunctions::OffsetFnInit<TimestampVal>(
    FunctionContext*, TimestampVal*);
template void AggregateFunctions::OffsetFnInit<DecimalVal>(
    FunctionContext*, DecimalVal*);
template void AggregateFunctions::OffsetFnInit<DateVal>(
    FunctionContext*, DateVal*);

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
template void AggregateFunctions::OffsetFnUpdate<DateVal>(
    FunctionContext*, const DateVal& src, const BigIntVal&, const DateVal&,
    DateVal* dst);
}
