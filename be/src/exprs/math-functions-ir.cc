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

#include <iomanip>
#include <random>
#include <sstream>
#include <math.h>
#include <stdint.h>
#include <string>
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/operators.h"
#include "exprs/math-functions.h"
#include "util/string-parser.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/multi-precision.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "thirdparty/pcg-cpp-0.98/include/pcg_random.hpp"
#include "exprs/decimal-operators.h"
#include "common/names.h"

using std::uppercase;

namespace impala {

const char* MathFunctions::ALPHANUMERIC_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

DoubleVal MathFunctions::Pi(FunctionContext* ctx) {
  return DoubleVal(M_PI);
}

DoubleVal MathFunctions::E(FunctionContext* ctx) {
  return DoubleVal(M_E);
}

// Generates a UDF that always calls FN() on the input val and returns it.
#define ONE_ARG_MATH_FN(NAME, RET_TYPE, INPUT_TYPE, FN) \
  RET_TYPE MathFunctions::NAME(FunctionContext* ctx, const INPUT_TYPE& v) { \
    if (v.is_null) return RET_TYPE::null(); \
    return RET_TYPE(FN(v.val)); \
  }

// Generates a UDF that always calls FN() on the input vals and returns it.
#define TWO_ARG_MATH_FN(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, FN) \
  RET_TYPE MathFunctions::NAME(FunctionContext* ctx, \
    const INPUT_TYPE1& v1, const INPUT_TYPE2& v2) { \
    if (v1.is_null || v2.is_null) return RET_TYPE::null(); \
    return RET_TYPE(FN(v1.val, v2.val)); \
  }

BigIntVal MathFunctions::Abs(FunctionContext* ctx, const BigIntVal& v) {
  if (v.is_null) return BigIntVal::null();
  if (UNLIKELY(v.val == std::numeric_limits<BigIntVal::underlying_type_t>::min())) {
      ctx->AddWarning("abs() overflowed, returning NULL");
      return BigIntVal::null();
  }
  return BigIntVal(llabs(v.val));
}

ONE_ARG_MATH_FN(Abs, BigIntVal, IntVal, llabs);
ONE_ARG_MATH_FN(Abs, IntVal, SmallIntVal, abs);
ONE_ARG_MATH_FN(Abs, SmallIntVal, TinyIntVal, abs);
ONE_ARG_MATH_FN(Abs, DoubleVal, DoubleVal, fabs);
ONE_ARG_MATH_FN(Abs, FloatVal, FloatVal, fabs);
ONE_ARG_MATH_FN(Sin, DoubleVal, DoubleVal, sin);
ONE_ARG_MATH_FN(Asin, DoubleVal, DoubleVal, asin);
ONE_ARG_MATH_FN(Cos, DoubleVal, DoubleVal, cos);
ONE_ARG_MATH_FN(Acos, DoubleVal, DoubleVal, acos);
ONE_ARG_MATH_FN(Tan, DoubleVal, DoubleVal, tan);
ONE_ARG_MATH_FN(Atan, DoubleVal, DoubleVal, atan);
ONE_ARG_MATH_FN(Cosh, DoubleVal, DoubleVal, cosh);
ONE_ARG_MATH_FN(Tanh, DoubleVal, DoubleVal, tanh);
ONE_ARG_MATH_FN(Sinh, DoubleVal, DoubleVal, sinh);
ONE_ARG_MATH_FN(Sqrt, DoubleVal, DoubleVal, sqrt);
ONE_ARG_MATH_FN(Ceil, DoubleVal, DoubleVal, ceil);
ONE_ARG_MATH_FN(Floor, DoubleVal, DoubleVal, floor);
ONE_ARG_MATH_FN(Truncate, DoubleVal, DoubleVal, trunc);
ONE_ARG_MATH_FN(Ln, DoubleVal, DoubleVal, log);
ONE_ARG_MATH_FN(Log10, DoubleVal, DoubleVal, log10);
ONE_ARG_MATH_FN(Exp, DoubleVal, DoubleVal, exp);

TWO_ARG_MATH_FN(Atan2, DoubleVal, DoubleVal, DoubleVal, atan2);

DoubleVal MathFunctions::Cot(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return DoubleVal::null();
  return DoubleVal(tan(M_PI_2 - v.val));
}

DoubleVal MathFunctions::Sign(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return DoubleVal::null();
  return DoubleVal((v.val > 0) ? 1 : ((v.val < 0) ? -1 : 0));
}

DoubleVal MathFunctions::Radians(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(v.val * M_PI / 180.0);
}

DoubleVal MathFunctions::Degrees(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(v.val * 180.0 / M_PI);
}

DoubleVal MathFunctions::Round(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return DoubleVal::null();
  return DoubleVal(trunc(v.val + ((v.val < 0) ? -0.5 : 0.5)));
}

static double GetScaleMultiplier(int64_t scale) {
  static const double values[] = {
      1.0,
      10.0,
      100.0,
      1000.0,
      10000.0,
      100000.0,
      1000000.0,
      10000000.0,
      100000000.0,
      1000000000.0,
      10000000000.0,
      100000000000.0,
      1000000000000.0,
      10000000000000.0,
      100000000000000.0,
      1000000000000000.0,
      10000000000000000.0,
      100000000000000000.0,
      1000000000000000000.0,
      10000000000000000000.0};
  if (LIKELY(0 <= scale && scale < 20)) return values[scale];
  return pow(10.0, scale);
}

DoubleVal MathFunctions::RoundUpTo(FunctionContext* ctx, const DoubleVal& v,
    const BigIntVal& scale) {
  if (v.is_null || scale.is_null) return DoubleVal::null();
  return DoubleVal(trunc(
      v.val * GetScaleMultiplier(scale.val) + ((v.val < 0) ? -0.5 : 0.5)) /
      GetScaleMultiplier(scale.val));
}

DoubleVal MathFunctions::Log2(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return DoubleVal::null();
  return DoubleVal(log(v.val) / log(2.0));
}

DoubleVal MathFunctions::Log(FunctionContext* ctx, const DoubleVal& base,
    const DoubleVal& v) {
  if (base.is_null || v.is_null) return DoubleVal::null();
  return DoubleVal(log(v.val) / log(base.val));
}

DoubleVal MathFunctions::Pow(FunctionContext* ctx, const DoubleVal& base,
    const DoubleVal& exp) {
  if (base.is_null || exp.is_null) return DoubleVal::null();
  return DoubleVal(pow(base.val, exp.val));
}

void MathFunctions::RandPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint32_t seed = 0;
    if (ctx->GetNumArgs() == 1) {
      // This is a call to RandSeed, initialize the seed
      // TODO: should we support non-constant seed?
      if (!ctx->IsArgConstant(0)) {
        ctx->SetError("Seed argument to rand() must be constant");
        return;
      }
      DCHECK_EQ(ctx->GetArgType(0)->type, FunctionContext::TYPE_BIGINT);
      BigIntVal* seed_arg = static_cast<BigIntVal*>(ctx->GetConstantArg(0));
      if (!seed_arg->is_null) seed = seed_arg->val;
    }
    pcg32* generator = ctx->Allocate<pcg32>();
    RETURN_IF_NULL(ctx, generator);
    ctx->SetFunctionState(scope, generator);
    new (generator) pcg32(seed);
  }
}

DoubleVal MathFunctions::Rand(FunctionContext* ctx) {
  pcg32* const generator =
      reinterpret_cast<pcg32*>(ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(generator != nullptr);
  static const double min = 0, max = 1;
  std::uniform_real_distribution<double> distribution(min, max);
  return DoubleVal(distribution(*generator));
}

DoubleVal MathFunctions::RandSeed(FunctionContext* ctx, const BigIntVal& seed) {
  if (seed.is_null) return DoubleVal::null();
  return Rand(ctx);
}

void MathFunctions::RandClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint8_t* generator = reinterpret_cast<uint8_t*>(
        ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
    ctx->Free(generator);
    ctx->SetFunctionState(FunctionContext::THREAD_LOCAL, nullptr);
  }
}

StringVal MathFunctions::Bin(FunctionContext* ctx, const BigIntVal& v) {
  if (v.is_null) return StringVal::null();
  // Cast to an unsigned integer because it is compiler dependent
  // whether the sign bit will be shifted like a regular bit.
  // (logical vs. arithmetic shift for signed numbers)
  uint64_t n = static_cast<uint64_t>(v.val);
  const size_t max_bits = sizeof(uint64_t) * 8;
  char result[max_bits];
  uint32_t index = max_bits;
  do {
    result[--index] = '0' + (n & 1);
  } while (n >>= 1);
  return AnyValUtil::FromBuffer(ctx, result + index, max_bits - index);
}

StringVal MathFunctions::HexInt(FunctionContext* ctx, const BigIntVal& v) {
  if (v.is_null) return StringVal::null();
  // TODO: this is probably unreasonably slow
  stringstream ss;
  ss << hex << uppercase << v.val;
  return AnyValUtil::FromString(ctx, ss.str());
}

StringVal MathFunctions::HexString(FunctionContext* ctx, const StringVal& s) {
  if (s.is_null) return StringVal::null();
  stringstream ss;
  ss << hex << uppercase << setfill('0');
  for (int i = 0; i < s.len; ++i) {
    // setw is not sticky. stringstream only converts integral values,
    // so a cast to int is required, but only convert the least significant byte to hex.
    ss << setw(2) << (static_cast<int32_t>(s.ptr[i]) & 0xFF);
  }
  return AnyValUtil::FromString(ctx, ss.str());
}

StringVal MathFunctions::Unhex(FunctionContext* ctx, const StringVal& s) {
  if (s.is_null) return StringVal::null();
  // For uneven number of chars return empty string like Hive does.
  if (s.len % 2 != 0) return StringVal();

  int result_len = s.len / 2;
  StringVal result(ctx, result_len);
  int res_index = 0;
  int s_index = 0;
  while (s_index < s.len) {
    char c = 0;
    for (int j = 0; j < 2; ++j, ++s_index) {
      switch(s.ptr[s_index]) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
          c += (s.ptr[s_index] - '0') * ((j == 0) ? 16 : 1);
          break;
        case 'A':
        case 'B':
        case 'C':
        case 'D':
        case 'E':
        case 'F':
          // Map to decimal values [10, 15]
          c += (s.ptr[s_index] - 'A' + 10) * ((j == 0) ? 16 : 1);
          break;
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
          // Map to decimal [10, 15]
          c += (s.ptr[s_index] - 'a' + 10) * ((j == 0) ? 16 : 1);
          break;
        default:
          // Character not in hex alphabet, return empty string.
          return StringVal();
      }
    }
    result.ptr[res_index] = c;
    ++res_index;
  }
  return result;
}

StringVal MathFunctions::ConvInt(FunctionContext* ctx, const BigIntVal& num,
    const TinyIntVal& src_base, const TinyIntVal& dest_base) {
  if (num.is_null || src_base.is_null || dest_base.is_null) return StringVal::null();
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(src_base.val) < MIN_BASE || abs(src_base.val) > MAX_BASE
      || abs(dest_base.val) < MIN_BASE || abs(dest_base.val) > MAX_BASE) {
    // Return NULL like Hive does.
    return StringVal::null();
  }

  // Invalid input.
  if (src_base.val < 0 && num.val >= 0) return StringVal::null();
  int64_t decimal_num = num.val;
  if (src_base.val != 10) {
    // Convert src_num representing a number in src_base but encoded in decimal
    // into its actual decimal number.
    if (!DecimalInBaseToDecimal(num.val, src_base.val, &decimal_num)) {
      // Handle overflow, setting decimal_num appropriately.
      HandleParseResult(dest_base.val, &decimal_num, StringParser::PARSE_OVERFLOW);
    }
  }
  return DecimalToBase(ctx, decimal_num, dest_base.val);
}

StringVal MathFunctions::ConvString(FunctionContext* ctx, const StringVal& num_str,
    const TinyIntVal& src_base, const TinyIntVal& dest_base) {
  if (num_str.is_null || src_base.is_null || dest_base.is_null) return StringVal::null();
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(src_base.val) < MIN_BASE || abs(src_base.val) > MAX_BASE
      || abs(dest_base.val) < MIN_BASE || abs(dest_base.val) > MAX_BASE) {
    // Return NULL like Hive does.
    return StringVal::null();
  }
  // Convert digits in num_str in src_base to decimal.
  StringParser::ParseResult parse_res;
  int64_t decimal_num = StringParser::StringToInt<int64_t>(
      reinterpret_cast<char*>(num_str.ptr), num_str.len, src_base.val, &parse_res);
  if (src_base.val < 0 && decimal_num >= 0) {
    // Invalid input.
    return StringVal::null();
  }
  if (!HandleParseResult(dest_base.val, &decimal_num, parse_res)) {
    // Return 0 for invalid input strings like Hive does.
    return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>("0")), 1);
  }
  return DecimalToBase(ctx, decimal_num, dest_base.val);
}

StringVal MathFunctions::DecimalToBase(FunctionContext* ctx, int64_t src_num,
    int8_t dest_base) {
  // Max number of digits of any base (base 2 gives max digits), plus sign.
  const size_t max_digits = sizeof(uint64_t) * 8 + 1;
  char buf[max_digits];
  int32_t result_len = 0;
  int32_t buf_index = max_digits - 1;
  uint64_t temp_num;
  if (dest_base < 0) {
    // Dest base is negative, treat src_num as signed.
    temp_num = abs(src_num);
  } else {
    // Dest base is positive. We must interpret src_num in 2's complement.
    // Convert to an unsigned int to properly deal with 2's complement conversion.
    temp_num = static_cast<uint64_t>(src_num);
  }
  int abs_base = abs(dest_base);
  do {
    buf[buf_index] = ALPHANUMERIC_CHARS[temp_num % abs_base];
    temp_num /= abs_base;
    --buf_index;
    ++result_len;
  } while (temp_num > 0);
  // Add optional sign.
  if (src_num < 0 && dest_base < 0) {
    buf[buf_index] = '-';
    ++result_len;
  }
  return AnyValUtil::FromBuffer(ctx, buf + max_digits - result_len, result_len);
}

bool MathFunctions::DecimalInBaseToDecimal(int64_t src_num, int8_t src_base,
    int64_t* result) {
  uint64_t temp_num = abs(src_num);
  int64_t place = 1;
  *result = 0;
  do {
    int32_t digit = temp_num % 10;
    // Reset result if digit is not representable in src_base.
    if (digit >= src_base) {
      *result = 0;
      place = 1;
    } else {
      *result +=
          ArithmeticUtil::AsUnsigned<std::multiplies>(static_cast<int64_t>(digit), place);
      // Overflow.
      if (UNLIKELY(*result < digit)) {
        return false;
      }
      place *= src_base;
    }
    temp_num /= 10;
  } while (temp_num > 0);
  *result = (src_num < 0) ? -(*result) : *result;
  return true;
}

bool MathFunctions::HandleParseResult(int8_t dest_base, int64_t* num,
    StringParser::ParseResult parse_res) {
  // On overflow set special value depending on dest_base.
  // This is consistent with Hive and MySQL's behavior.
  if (parse_res == StringParser::PARSE_OVERFLOW) {
    if (dest_base < 0) {
      *num = -1;
    } else {
      *num = numeric_limits<uint64_t>::max();
    }
  } else if (parse_res == StringParser::PARSE_FAILURE) {
    // Some other error condition.
    return false;
  }
  return true;
}

BigIntVal MathFunctions::PmodBigInt(FunctionContext* ctx, const BigIntVal& a,
    const BigIntVal& b) {
  if (a.is_null || b.is_null || b.val == 0) return BigIntVal::null();
  return BigIntVal(((a.val % b.val) + b.val) % b.val);
}

DoubleVal MathFunctions::PmodDouble(FunctionContext* ctx, const DoubleVal& a,
    const DoubleVal& b) {
  if (a.is_null || b.is_null || b.val == 0) return DoubleVal::null();
  return DoubleVal(fmod(fmod(a.val, b.val) + b.val, b.val));
}

FloatVal MathFunctions::FmodFloat(FunctionContext* ctx, const FloatVal& a,
    const FloatVal& b) {
  if (a.is_null || b.is_null || b.val == 0) return FloatVal::null();
  return FloatVal(fmodf(a.val, b.val));
}

DoubleVal MathFunctions::FmodDouble(FunctionContext* ctx, const DoubleVal& a,
    const DoubleVal& b) {
  if (a.is_null || b.is_null || b.val == 0) return DoubleVal::null();
  return DoubleVal(fmod(a.val, b.val));
}

// The bucket_number is evaluated using the following formula:
//
//   bucket_number = dist_from_min * num_buckets / range_size + 1
//      where -
//        dist_from_min = expr - min_range
//        range_size = max_range - min_range
//        num_buckets = number of buckets
//
// Since expr, min_range, and max_range are all decimals with the same
// byte size, precision, and scale we can interpret them as plain integers
// and still calculate the proper bucket.
//
// There are some possibilities of overflowing during the calculation:
// range_size = max_range - min_range
// dist_from_min = expr - min_range
// dist_from_min * num_buckets
//
// For all the above cases we use a bigger integer type provided by the
// DoubleWidth<> metafunction.
template <class  T1>
BigIntVal MathFunctions::WidthBucketImpl(FunctionContext* ctx,
    const T1& expr, const T1& min_range,
    const T1& max_range, const IntVal& num_buckets) {
  auto expr_val = expr.value();
  using ActualType = decltype(expr_val);
  auto min_range_val = min_range.value();
  auto max_range_val = max_range.value();
  auto num_buckets_val = static_cast<ActualType>(num_buckets.val);

  if (UNLIKELY(num_buckets.val <= 0)) {
    ostringstream error_msg;
    error_msg << "Number of buckets should be greater than zero:" << num_buckets.val;
    ctx->SetError(error_msg.str().c_str());
    return BigIntVal::null();
  }

  if (UNLIKELY(min_range_val >= max_range_val)) {
    ctx->SetError("Lower bound cannot be greater than or equal to the upper bound");
    return BigIntVal::null();
  }

  if (expr_val < min_range_val) return 0;
  if (expr_val >= max_range_val) {
    return BigIntVal(static_cast<int64_t>(num_buckets.val) + 1);
  }

  bool bigger_type_needed = false;
  // It is likely that this if stmt can be evaluated during codegen because
  // 'max_range' and 'min_range' are almost certainly constant expressions:
  if (max_range_val >= 0 && min_range_val < 0) {
    if (static_cast<UnsignedType<ActualType>>(max_range_val) +
        static_cast<UnsignedType<ActualType>>(abs(min_range_val)) >=
        static_cast<UnsignedType<ActualType>>(ArithmeticUtil::Max<ActualType>())) {
      bigger_type_needed = true;
    }
  }

  auto MultiplicationOverflows = [](ActualType lhs, ActualType rhs) {
    DCHECK(lhs > 0 && rhs > 0);
    using ActualType = decltype(lhs);
    return BitUtil::CountLeadingZeros(lhs) + BitUtil::CountLeadingZeros(rhs) <=
        ArithmeticUtil::UnsignedWidth<ActualType>() + 1;
  };

  // It is likely that this can be evaluated during codegen:
  bool multiplication_can_overflow = bigger_type_needed ||  MultiplicationOverflows(
      max_range_val - min_range_val, num_buckets_val);
  // 'expr_val' is not likely to be a constant expression, so this almost certainly
  // needs runtime evaluation if 'bigger_type_needed' is false and
  // 'multiplication_can_overflow' is true:
  bigger_type_needed = bigger_type_needed || (
      multiplication_can_overflow &&
      expr_val != min_range_val &&
      MultiplicationOverflows(expr_val - min_range_val, num_buckets_val));

  auto BucketFunc = [](auto element, auto min_rng, auto max_rng, auto buckets) {
    auto range_size = max_rng - min_rng;
    auto dist_from_min = element - min_rng;
    auto ret = dist_from_min * buckets / range_size;
    return BigIntVal(static_cast<int64_t>(ret) + 1);
  };

  if (bigger_type_needed) {
    using BiggerType = typename DoubleWidth<ActualType>::type;

    return BucketFunc(static_cast<BiggerType>(expr_val),
        static_cast<BiggerType>(min_range_val), static_cast<BiggerType>(max_range_val),
        static_cast<BiggerType>(num_buckets.val));
  } else {
    return BucketFunc(expr_val, min_range_val, max_range_val, num_buckets.val);
  }
}

BigIntVal MathFunctions::WidthBucket(FunctionContext* ctx, const DecimalVal& expr,
    const DecimalVal& min_range, const DecimalVal& max_range,
    const IntVal& num_buckets) {
  if (expr.is_null || min_range.is_null || max_range.is_null || num_buckets.is_null) {
    return BigIntVal::null();
  }

  int arg_size_type = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  switch (arg_size_type) {
    case 4:
      return WidthBucketImpl<Decimal4Value> (ctx, Decimal4Value(expr.val4),
          Decimal4Value(min_range.val4), Decimal4Value(max_range.val4), num_buckets);
    case 8:
      return WidthBucketImpl<Decimal8Value> (ctx, Decimal8Value(expr.val8),
          Decimal8Value(min_range.val8), Decimal8Value(max_range.val8), num_buckets);
    case 16:
      return WidthBucketImpl<Decimal16Value>(ctx, Decimal16Value(expr.val16),
         Decimal16Value(min_range.val16), Decimal16Value(max_range.val16), num_buckets);
    default:
      DCHECK(false);
      return BigIntVal::null();
  }
}

template <typename T> T MathFunctions::Positive(FunctionContext* ctx, const T& val) {
  return val;
}

template <typename T> T MathFunctions::Negative(FunctionContext* ctx, const T& val) {
  if (val.is_null) return val;
  return T(-val.val);
}

template <>
DecimalVal MathFunctions::Negative(FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return val;
  int type_byte_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SIZE);
  switch (type_byte_size) {
    case 4:
      return DecimalVal(-val.val4);
    case 8:
      return DecimalVal(-val.val8);
    case 16:
      return DecimalVal(-val.val16);
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

BigIntVal MathFunctions::QuotientDouble(FunctionContext* ctx, const DoubleVal& x,
    const DoubleVal& y) {
  if (x.is_null || y.is_null || static_cast<int64_t>(y.val)  == 0) {
    return BigIntVal::null();
  }
  return BigIntVal(static_cast<int64_t>(x.val) / static_cast<int64_t>(y.val));
}

BigIntVal MathFunctions::QuotientBigInt(FunctionContext* ctx, const BigIntVal& x,
    const BigIntVal& y) {
  return Operators::Int_divide_BigIntVal_BigIntVal(ctx, x, y);
}

template <typename VAL_TYPE, bool ISLEAST> VAL_TYPE MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const VAL_TYPE* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return VAL_TYPE::null();
  int result_idx = 0;
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return VAL_TYPE::null();
    if (ISLEAST) {
      if (args[i].val < args[result_idx].val) result_idx = i;
    } else {
      if (args[i].val > args[result_idx].val) result_idx = i;
    }
  }
  return VAL_TYPE(args[result_idx].val);
}

template <bool ISLEAST> StringVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const StringVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return StringVal::null();
  StringValue result_val = StringValue::FromStringVal(args[0]);
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return StringVal::null();
    StringValue val = StringValue::FromStringVal(args[i]);
    if (ISLEAST) {
      if (val < result_val) result_val = val;
    } else {
      if (val > result_val) result_val = val;
    }
  }
  StringVal result;
  result_val.ToStringVal(&result);
  return result;
}

template <bool ISLEAST> TimestampVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const TimestampVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return TimestampVal::null();
  TimestampValue result_val = TimestampValue::FromTimestampVal(args[0]);
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return TimestampVal::null();
    TimestampValue val = TimestampValue::FromTimestampVal(args[i]);
    if (ISLEAST) {
      if (val < result_val) result_val = val;
    } else {
      if (val > result_val) result_val = val;
    }
  }
  TimestampVal result;
  result_val.ToTimestampVal(&result);
  return result;
}

template <bool ISLEAST> DecimalVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const DecimalVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return DecimalVal::null();
  DecimalVal result_val = args[0];
  int type_byte_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SIZE);
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return DecimalVal::null();
    switch (type_byte_size) {
      case 4:
        if (ISLEAST) {
          if (args[i].val4 < result_val.val4) result_val = args[i];
        } else {
          if (args[i].val4 > result_val.val4) result_val = args[i];
        }
        break;
      case 8:
        if (ISLEAST) {
          if (args[i].val8 < result_val.val8) result_val = args[i];
        } else {
          if (args[i].val8 > result_val.val8) result_val = args[i];
        }
        break;
      case 16:
        if (ISLEAST) {
          if (args[i].val16 < result_val.val16) result_val = args[i];
        } else {
          if (args[i].val16 > result_val.val16) result_val = args[i];
        }
        break;
      default:
        DCHECK(false);
    }
  }
  return result_val;
}

template <bool ISLEAST> DateVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const DateVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return DateVal::null();
  DateValue result_val = DateValue::FromDateVal(args[0]);
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return DateVal::null();
    DateValue val = DateValue::FromDateVal(args[i]);
    if (ISLEAST) {
      if (val < result_val) result_val = val;
    } else {
      if (val > result_val) result_val = val;
    }
  }
  return result_val.ToDateVal();
}

template TinyIntVal MathFunctions::Positive<TinyIntVal>(
    FunctionContext* ctx, const TinyIntVal& val);
template SmallIntVal MathFunctions::Positive<SmallIntVal>(
    FunctionContext* ctx, const SmallIntVal& val);
template IntVal MathFunctions::Positive<IntVal>(
    FunctionContext* ctx, const IntVal& val);
template BigIntVal MathFunctions::Positive<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& val);
template FloatVal MathFunctions::Positive<FloatVal>(
    FunctionContext* ctx, const FloatVal& val);
template DoubleVal MathFunctions::Positive<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& val);
template DecimalVal MathFunctions::Positive<DecimalVal>(
    FunctionContext* ctx, const DecimalVal& val);

template TinyIntVal MathFunctions::Negative<TinyIntVal>(
    FunctionContext* ctx, const TinyIntVal& val);
template SmallIntVal MathFunctions::Negative<SmallIntVal>(
    FunctionContext* ctx, const SmallIntVal& val);
template IntVal MathFunctions::Negative<IntVal>(
    FunctionContext* ctx, const IntVal& val);
template BigIntVal MathFunctions::Negative<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& val);
template FloatVal MathFunctions::Negative<FloatVal>(
    FunctionContext* ctx, const FloatVal& val);
template DoubleVal MathFunctions::Negative<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& val);

template TinyIntVal MathFunctions::LeastGreatest<TinyIntVal, true>(
    FunctionContext* ctx, int num_args, const TinyIntVal* args);
template SmallIntVal MathFunctions::LeastGreatest<SmallIntVal, true>(
    FunctionContext* ctx, int num_args, const SmallIntVal* args);
template IntVal MathFunctions::LeastGreatest<IntVal, true>(
    FunctionContext* ctx, int num_args, const IntVal* args);
template BigIntVal MathFunctions::LeastGreatest<BigIntVal, true>(
    FunctionContext* ctx, int num_args, const BigIntVal* args);
template FloatVal MathFunctions::LeastGreatest<FloatVal, true>(
    FunctionContext* ctx, int num_args, const FloatVal* args);
template DoubleVal MathFunctions::LeastGreatest<DoubleVal, true>(
    FunctionContext* ctx, int num_args, const DoubleVal* args);

template TinyIntVal MathFunctions::LeastGreatest<TinyIntVal, false>(
    FunctionContext* ctx, int num_args, const TinyIntVal* args);
template SmallIntVal MathFunctions::LeastGreatest<SmallIntVal, false>(
    FunctionContext* ctx, int num_args, const SmallIntVal* args);
template IntVal MathFunctions::LeastGreatest<IntVal, false>(
    FunctionContext* ctx, int num_args, const IntVal* args);
template BigIntVal MathFunctions::LeastGreatest<BigIntVal, false>(
    FunctionContext* ctx, int num_args, const BigIntVal* args);
template FloatVal MathFunctions::LeastGreatest<FloatVal, false>(
    FunctionContext* ctx, int num_args, const FloatVal* args);
template DoubleVal MathFunctions::LeastGreatest<DoubleVal, false>(
    FunctionContext* ctx, int num_args, const DoubleVal* args);

template StringVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const StringVal* args);
template StringVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const StringVal* args);

template TimestampVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const TimestampVal* args);
template TimestampVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const TimestampVal* args);

template DecimalVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const DecimalVal* args);
template DecimalVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const DecimalVal* args);

template DateVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const DateVal* args);
template DateVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const DateVal* args);

}
