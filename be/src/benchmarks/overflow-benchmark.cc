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

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>

#include "runtime/decimal-value.inline.h"
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/decimal-constants.h"
#include "util/decimal-util.h"
#include "util/string-parser.h"

#include "common/names.h"

#ifndef __has_builtin
#define __has_builtin(x) 0
#endif

using std::numeric_limits;
using namespace impala;

// Machine Info: Intel(R) Xeon(R) CPU E5-2695 v3 @ 2.30GHz
// Decimal16 Add Overflow:Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//         without_check_overflow               31.27                  1X
//           builtin_add_overflow               9.901             0.3167X
//      add_overflow_lookup_table               12.44             0.3979X
//                   add_overflow               12.62             0.4036X
//
// Decimal16 Mul Overflow:Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//         without_check_overflow               20.47                  1X
//           builtin_mul_overflow               7.542             0.3685X
//         mul_overflow_check_msb               2.022            0.09881X
//                   mul_overflow               2.198             0.1074X
//
// Clz of int128_t:      Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                    clz_branchy           1.879e+13                  1X
//                clz_branch_free           4.945e+12             0.2632X

struct TestData {
  int precision;
  int scale;
  double probability_negative;
  vector<Decimal16Value> values;
  vector<Decimal16Value> results;
  vector<bool> overflows;
  vector<int128_t> int128_values;
};

double Rand() {
  return rand() / static_cast<double>(RAND_MAX);
}

void AddTestData(TestData* data, int n) {
  int128_t max_whole = data->precision > data->scale ?
      DecimalUtil::GetScaleMultiplier<int128_t>(data->precision - data->scale) - 1 : 0;
  int128_t max_fraction = data->scale > 0 ?
      DecimalUtil::GetScaleMultiplier<int128_t>(data->scale) - 1 : 0;
  ColumnType column_type = ColumnType::CreateDecimalType(data->precision, data->scale);
  Decimal16Value val;
  for (int i = 0; i < n; ++i) {
    stringstream ss;
    if (data->probability_negative > Rand()) ss << "-";
    if (max_whole > 0) ss << static_cast<int128_t>(max_whole * Rand());
    if (max_fraction > 0) ss << "." << static_cast<int128_t>(max_fraction * Rand());
    string str = ss.str();
    StringParser::ParseResult dummy;
    val = StringParser::StringToDecimal<int128_t>(
        str.c_str(), str.length(), column_type, false, &dummy);
    data->values.push_back(val);
    data->int128_values.push_back(numeric_limits<int128_t>::max() * Rand());
  }
}

template <typename RESULT_T>
static bool AdjustToSameScale(const Decimal16Value& x, int x_scale,
    const Decimal16Value& y, int y_scale, int result_precision, RESULT_T* x_scaled,
    RESULT_T* y_scaled) {
  int delta_scale = x_scale - y_scale;
  RESULT_T scale_factor = DecimalUtil::GetScaleMultiplier<RESULT_T>(abs(delta_scale));
  if (delta_scale == 0) {
    *x_scaled = x.value();
    *y_scaled = y.value();
  } else if (delta_scale > 0) {
    if (sizeof(RESULT_T) == 16 && result_precision == ColumnType::MAX_PRECISION &&
        MAX_UNSCALED_DECIMAL16 / scale_factor < abs(y.value())) {
      return true;
    }
    *x_scaled = x.value();
    *y_scaled = y.value() * scale_factor;
  } else {
    if (sizeof(RESULT_T) == 16 && result_precision == ColumnType::MAX_PRECISION &&
        MAX_UNSCALED_DECIMAL16 / scale_factor < abs(x.value())) {
      return true;
    }
    *x_scaled = x.value() * scale_factor;
    *y_scaled = y.value();
  }
  return false;
}

// Accelerate using lookup table.
template <typename RESULT_T>
static void AdjustToSameScaleLookupTbl(const Decimal16Value& x, int x_scale,
    const Decimal16Value& y, int y_scale, int result_precision, RESULT_T* x_scaled,
    RESULT_T* y_scaled) {
  int delta_scale = x_scale - y_scale;
  RESULT_T scale_factor = DecimalUtil::GetScaleMultiplier<RESULT_T>(abs(delta_scale));
  if (delta_scale == 0) {
    *x_scaled = x.value();
    *y_scaled = y.value();
  } else if (delta_scale > 0) {
    *x_scaled = x.value();
    *y_scaled = y.value() * scale_factor;
  } else {
    *x_scaled = x.value() * scale_factor;
    *y_scaled = y.value();
  }
}

#if 5 <= __GNUC__ || __has_builtin(__builtin_add_overflow)
#define HAVE_BUILTIN_ADD_OVERFLOW
template<typename RESULT_T>
DecimalValue<RESULT_T> BuiltinAdd(const Decimal16Value& val, int this_scale,
    const Decimal16Value& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) {
  DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
  RESULT_T x = 0;
  RESULT_T y = 0;
  *overflow |= AdjustToSameScale(val, this_scale, other, other_scale,
      result_precision, &x, &y);
  if (sizeof(RESULT_T) == 16 && result_precision == ColumnType::MAX_PRECISION) {
    RESULT_T result = 0;
    *overflow |= __builtin_add_overflow(x, y, &result);
    *overflow |= abs(result) > MAX_UNSCALED_DECIMAL16;
    return DecimalValue<RESULT_T>(result);
  } else {
    DCHECK(!*overflow) << "Cannot overflow unless result is Decimal16Value";
  }
  return DecimalValue<RESULT_T>(x + y);
}
#endif

template<typename RESULT_T>
DecimalValue<RESULT_T> AddLookupTbl(const Decimal16Value& val, int this_scale,
    const Decimal16Value& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) {
  DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
  RESULT_T x = 0;
  RESULT_T y = 0;
  AdjustToSameScaleLookupTbl(val, this_scale, other, other_scale,
      result_precision, &x, &y);
  if (sizeof(RESULT_T) == 16) {
    // Check overflow.
    if (!*overflow && (val.value() < 0) == (other.value() < 0) &&
        result_precision == ColumnType::MAX_PRECISION) {
      // Can only overflow if the signs are the same and result precision reaches
      // max precision.
      *overflow |= MAX_UNSCALED_DECIMAL16 - abs(x) < abs(y);
      // TODO: faster to return here? We don't care at all about the perf on
      // the overflow case but what makes the normal path faster?
    }
  } else {
    DCHECK(!*overflow) << "Cannot overflow unless result is Decimal16Value";
  }
  return DecimalValue<RESULT_T>(x + y);
}

template<typename RESULT_T>
DecimalValue<RESULT_T> Add(const Decimal16Value& val, int this_scale,
    const Decimal16Value& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) {
  DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
  RESULT_T x = 0;
  RESULT_T y = 0;
  *overflow |= AdjustToSameScale(val, this_scale, other, other_scale,
      result_precision, &x, &y);
  if (sizeof(RESULT_T) == 16) {
    // Check overflow.
    if (!*overflow && (val.value() < 0) == (other.value() < 0) &&
        result_precision == ColumnType::MAX_PRECISION) {
      // Can only overflow if the signs are the same and result precision reaches
      // max precision.
      *overflow |= MAX_UNSCALED_DECIMAL16 - abs(x) < abs(y);
      // TODO: faster to return here? We don't care at all about the perf on
      // the overflow case but what makes the normal path faster?
    }
  } else {
    DCHECK(!*overflow) << "Cannot overflow unless result is Decimal16Value";
  }
  return DecimalValue<RESULT_T>(x + y);
}

// set DISABLE_CHECK_OVERFLOW true will disable checking overflow.
#define TEST_ADD(NAME, FN, DISABLE_CHECK_OVERFLOW) \
  void NAME(int batch_size, void* d) { \
    TestData* data = reinterpret_cast<TestData*>(d); \
    for (int i = 0; i < batch_size; ++i) { \
      int s1 = data->scale; \
      int s2 = data->scale; \
      int p1 = data->precision; \
      int p2 = data->precision; \
      int s_max = std::max(s1, s2); \
      int max_precision = ColumnType::MAX_PRECISION; \
      int result_precision = std::min(s_max + std::max(p1 - s1, p2 - s2) + 1, \
          max_precision); \
      int result_scale = std::min(result_precision, s_max); \
      for (int j = 0; j < data->values.size() - 1; ++j) { \
        bool overflow = DISABLE_CHECK_OVERFLOW; \
        data->results[j] = FN<int128_t>(data->values[j], data->scale, \
            data->values[j + 1], data->scale, result_precision, \
            result_scale, &overflow); \
        data->overflows[j] = overflow; \
      } \
    } \
  }

TEST_ADD(TestAdd, Add, true);
#ifdef HAVE_BUILTIN_ADD_OVERFLOW
TEST_ADD(TestBuiltinAddOverflow, BuiltinAdd, false);
#endif
TEST_ADD(TestAddOverflowLookupTbl, AddLookupTbl, false);
TEST_ADD(TestAddOverflow, Add, false);

// Disabled __builtin_mul_overflow since Clang emits a call to __muloti4, which is
// not implemented in the GCC runtime library.
#if 5 <= __GNUC__
#define HAVE_BUILTIN_MUL_OVERFLOW
template<typename RESULT_T>
DecimalValue<RESULT_T> BuiltinMultiply(const Decimal16Value& val, int this_scale,
    const Decimal16Value& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) {
  // In the non-overflow case, we don't need to adjust by the scale since
  // that is already handled by the FE when it computes the result decimal type.
  // e.g. 1.23 * .2 (scale 2, scale 1 respectively) is identical to:
  // 123 * 2 with a resulting scale 3. We can do the multiply on the unscaled values.
  // The result scale in this case is the sum of the input scales.
  RESULT_T x = val.value();
  RESULT_T y = other.value();
  if (x == 0 || y == 0) {
    // Handle zero to avoid divide by zero in the overflow check below.
    return DecimalValue<RESULT_T>(0);
  }
  RESULT_T result = 0;
  if (sizeof(RESULT_T) == 16 && result_precision == ColumnType::MAX_PRECISION) {
    // Check overflow
    *overflow |= __builtin_mul_overflow(x, y, &result);
    *overflow |= abs(result) > MAX_UNSCALED_DECIMAL16;
  } else {
    result = x * y;
  }
  int delta_scale = this_scale + other_scale - result_scale;
  if (UNLIKELY(delta_scale != 0)) {
    // In this case, the required resulting scale is larger than the max we support.
    // We cap the resulting scale to the max supported scale (e.g. truncate) in the FE.
    // TODO: we could also return NULL.
    DCHECK_GT(delta_scale, 0);
    result /= DecimalUtil::GetScaleMultiplier<int128_t>(delta_scale);
  }
  return DecimalValue<RESULT_T>(result);
}
#endif

// Check MSB of decimal values to skip checking overflow.
template<typename RESULT_T>
DecimalValue<RESULT_T> MultiplyCheckMSB(const Decimal16Value& val, int this_scale,
    const Decimal16Value& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) {
  // In the non-overflow case, we don't need to adjust by the scale since
  // that is already handled by the FE when it computes the result decimal type.
  // e.g. 1.23 * .2 (scale 2, scale 1 respectively) is identical to:
  // 123 * 2 with a resulting scale 3. We can do the multiply on the unscaled values.
  // The result scale in this case is the sum of the input scales.
  RESULT_T x = val.value();
  RESULT_T y = other.value();
  if (x == 0 || y == 0) {
    // Handle zero to avoid divide by zero in the overflow check below.
    return DecimalValue<RESULT_T>(0);
  }
  if (sizeof(RESULT_T) == 16) {
    // Check overflow
    if (result_precision == ColumnType::MAX_PRECISION &&
        DecimalUtil::Clz(abs(x)) + DecimalUtil::Clz(abs(y)) < 130) {
      *overflow |= MAX_UNSCALED_DECIMAL16 / abs(y) < abs(x);
    }
  }
  RESULT_T result = x * y;
  int delta_scale = this_scale + other_scale - result_scale;
  if (UNLIKELY(delta_scale != 0)) {
    // In this case, the required resulting scale is larger than the max we support.
    // We cap the resulting scale to the max supported scale (e.g. truncate) in the FE.
    // TODO: we could also return NULL.
    DCHECK_GT(delta_scale, 0);
    result /= DecimalUtil::GetScaleMultiplier<int128_t>(delta_scale);
  }
  return DecimalValue<RESULT_T>(result);
}

template<typename RESULT_T>
DecimalValue<RESULT_T> Multiply(const Decimal16Value& val, int this_scale,
    const Decimal16Value& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) {
  // In the non-overflow case, we don't need to adjust by the scale since
  // that is already handled by the FE when it computes the result decimal type.
  // e.g. 1.23 * .2 (scale 2, scale 1 respectively) is identical to:
  // 123 * 2 with a resulting scale 3. We can do the multiply on the unscaled values.
  // The result scale in this case is the sum of the input scales.
  RESULT_T x = val.value();
  RESULT_T y = other.value();
  if (x == 0 || y == 0) {
    // Handle zero to avoid divide by zero in the overflow check below.
    return DecimalValue<RESULT_T>(0);
  }
  if (sizeof(RESULT_T) == 16) {
    // Check overflow
    if (result_precision == ColumnType::MAX_PRECISION) {
      *overflow |= MAX_UNSCALED_DECIMAL16 / abs(y) < abs(x);
    }
  }
  RESULT_T result = x * y;
  int delta_scale = this_scale + other_scale - result_scale;
  if (UNLIKELY(delta_scale != 0)) {
    // In this case, the required resulting scale is larger than the max we support.
    // We cap the resulting scale to the max supported scale (e.g. truncate) in the FE.
    // TODO: we could also return NULL.
    DCHECK_GT(delta_scale, 0);
    result /= DecimalUtil::GetScaleMultiplier<int128_t>(delta_scale);
  }
  return DecimalValue<RESULT_T>(result);
}

// set DISABLE_CHECK_OVERFLOW true will disable checking overflow.
#define TEST_MUL(NAME, FN, DISABLE_CHECK_OVERFLOW) \
  void NAME(int batch_size, void* d) { \
    TestData* data = reinterpret_cast<TestData*>(d); \
    for (int i = 0; i < batch_size; ++i) { \
      int max_precision = ColumnType::MAX_PRECISION; \
      int result_precision = std::min(data->precision + data->precision, \
          max_precision); \
      int result_scale = std::min(result_precision, data->scale + data->scale); \
      for (int j = 0; j < data->values.size() - 1; ++j) { \
        bool overflow = DISABLE_CHECK_OVERFLOW; \
        data->results[j] = FN<int128_t>(data->values[j], data->scale, \
            data->values[j + 1], data->scale, result_precision, \
            result_scale, &overflow); \
        data->overflows[j] = overflow; \
      } \
    } \
  }

TEST_MUL(TestMul, Multiply, true);
#ifdef HAVE_BUILTIN_MUL_OVERFLOW
TEST_MUL(TestBuiltinMulOverflow, BuiltinMultiply, false);
#endif
TEST_MUL(TestMulOverflowCheckMSB, MultiplyCheckMSB, false);
TEST_MUL(TestMulOverflow, Multiply, false);

void TestClzBranchy(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    // Unroll this to focus more on Put performance.
    for (int j = 0; j < data->int128_values.size(); j += 8) {
      DecimalUtil::Clz(data->int128_values[j + 0]);
      DecimalUtil::Clz(data->int128_values[j + 1]);
      DecimalUtil::Clz(data->int128_values[j + 2]);
      DecimalUtil::Clz(data->int128_values[j + 3]);
      DecimalUtil::Clz(data->int128_values[j + 4]);
      DecimalUtil::Clz(data->int128_values[j + 5]);
      DecimalUtil::Clz(data->int128_values[j + 6]);
      DecimalUtil::Clz(data->int128_values[j + 7]);
    }
  }
}

inline int ClzBranchFree(const int128_t& v) {
  // GCC leaves __builtin_clz undefined for zero.
  uint64_t hi = static_cast<uint64_t>(v >> 64);
  uint64_t lo = static_cast<uint64_t>(v);
  int retval[3]={
    __builtin_clzll(hi),
    __builtin_clzll(lo)+64,
    128
  };
  int idx = !hi + ((!lo)&(!hi));
  return retval[idx];
}

void TestClzBranchFree(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    // Unroll this to focus more on Put performance.
    for (int j = 0; j < data->int128_values.size(); j += 8) {
      ClzBranchFree(data->int128_values[j + 0]);
      ClzBranchFree(data->int128_values[j + 1]);
      ClzBranchFree(data->int128_values[j + 2]);
      ClzBranchFree(data->int128_values[j + 3]);
      ClzBranchFree(data->int128_values[j + 4]);
      ClzBranchFree(data->int128_values[j + 5]);
      ClzBranchFree(data->int128_values[j + 6]);
      ClzBranchFree(data->int128_values[j + 7]);
    }
  }
}

int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;
  int size = 10000;
  data.precision = ColumnType::MAX_PRECISION;
  data.scale = data.precision / 2;
  data.probability_negative = 0.25;
  AddTestData(&data, size);
  data.results.resize(size - 1);
  data.overflows.resize(size - 1);

  Benchmark add_overflow_suite("Decimal16 Add Overflow");
  add_overflow_suite.AddBenchmark("without_check_overflow", TestAdd, &data);
#ifdef HAVE_BUILTIN_ADD_OVERFLOW
  add_overflow_suite.AddBenchmark("builtin_add_overflow", TestBuiltinAddOverflow, &data);
#endif
  add_overflow_suite.AddBenchmark("add_overflow_lookup_table",
      TestAddOverflowLookupTbl, &data);
  add_overflow_suite.AddBenchmark("add_overflow", TestAddOverflow, &data);
  cout << add_overflow_suite.Measure() << endl;

  Benchmark mul_overflow_suite("Decimal16 Mul Overflow");
  mul_overflow_suite.AddBenchmark("without_check_overflow", TestMul, &data);
#ifdef HAVE_BUILTIN_MUL_OVERFLOW
  mul_overflow_suite.AddBenchmark("builtin_mul_overflow", TestBuiltinMulOverflow, &data);
#endif
  mul_overflow_suite.AddBenchmark("mul_overflow_check_msb",
      TestMulOverflowCheckMSB, &data);
  mul_overflow_suite.AddBenchmark("mul_overflow", TestMulOverflow, &data);
  cout << mul_overflow_suite.Measure() << endl;

  // Counting the number of leading zeros in a 128-bit integer.
  Benchmark clz_suite("Clz of int128_t");
  clz_suite.AddBenchmark("clz_branchy", TestClzBranchy, &data);
  clz_suite.AddBenchmark("clz_branch_free", TestClzBranchFree, &data);
  cout << clz_suite.Measure() << endl;

  return 0;
}
