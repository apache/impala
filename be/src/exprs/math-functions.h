// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXPRS_MATH_FUNCTIONS_H
#define IMPALA_EXPRS_MATH_FUNCTIONS_H

#include <stdint.h>
/// For StringParser::ParseResult
#include "util/string-parser.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class Expr;
struct ExprValue;
class TupleRow;

class MathFunctions {
 public:
  static DoubleVal Pi(FunctionContext*);
  static DoubleVal E(FunctionContext*);
  static BigIntVal Abs(FunctionContext*, const BigIntVal&);
  static DoubleVal Abs(FunctionContext*, const DoubleVal&);
  static FloatVal Abs(FunctionContext*, const FloatVal&);
  static IntVal Abs(FunctionContext*, const IntVal&);
  static SmallIntVal Abs(FunctionContext*, const SmallIntVal&);
  static TinyIntVal Abs(FunctionContext*, const TinyIntVal&);
  static DoubleVal Sin(FunctionContext*, const DoubleVal&);
  static DoubleVal Asin(FunctionContext*, const DoubleVal&);
  static DoubleVal Cos(FunctionContext*, const DoubleVal&);
  static DoubleVal Acos(FunctionContext*, const DoubleVal&);
  static DoubleVal Tan(FunctionContext*, const DoubleVal&);
  static DoubleVal Cot(FunctionContext*, const DoubleVal&);
  static DoubleVal Atan(FunctionContext*, const DoubleVal&);
  static DoubleVal Atan2(FunctionContext*, const DoubleVal&, const DoubleVal&);
  static DoubleVal Cosh(FunctionContext*, const DoubleVal&);
  static DoubleVal Tanh(FunctionContext*, const DoubleVal&);
  static DoubleVal Sinh(FunctionContext*, const DoubleVal&);
  static DoubleVal Sqrt(FunctionContext*, const DoubleVal&);
  static DoubleVal Exp(FunctionContext*, const DoubleVal&);
  static BigIntVal Ceil(FunctionContext*, const DoubleVal&);
  static BigIntVal Floor(FunctionContext*, const DoubleVal&);
  static BigIntVal Truncate(FunctionContext*, const DoubleVal&);
  static DoubleVal Ln(FunctionContext*, const DoubleVal&);
  static DoubleVal Log10(FunctionContext*, const DoubleVal&);
  static FloatVal Sign(FunctionContext*, const DoubleVal&);
  static DoubleVal Radians(FunctionContext*, const DoubleVal&);
  static DoubleVal Degrees(FunctionContext*, const DoubleVal&);
  static BigIntVal Round(FunctionContext*, const DoubleVal&);
  static DoubleVal RoundUpTo(FunctionContext*, const DoubleVal&, const IntVal&);
  static DoubleVal Log2(FunctionContext*, const DoubleVal&);
  static DoubleVal Log(FunctionContext*, const DoubleVal& base, const DoubleVal& val);
  static DoubleVal Pow(FunctionContext*, const DoubleVal& base, const DoubleVal& val);

  /// Used for both Rand() and RandSeed()
  static void RandPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static DoubleVal Rand(FunctionContext*);
  static DoubleVal RandSeed(FunctionContext*, const BigIntVal& seed);
  static void RandClose(FunctionContext*, FunctionContext::FunctionStateScope);

  static StringVal Bin(FunctionContext*, const BigIntVal&);
  static StringVal HexInt(FunctionContext*, const BigIntVal&);
  static StringVal HexString(FunctionContext*, const StringVal&);
  static StringVal Unhex(FunctionContext*, const StringVal&);
  static StringVal ConvInt(FunctionContext*, const BigIntVal& n,
      const TinyIntVal& src_base, const TinyIntVal& dst_base);
  static StringVal ConvString(FunctionContext*, const StringVal& s,
      const TinyIntVal& src_base, const TinyIntVal& dst_base);
  static BigIntVal PmodBigInt(FunctionContext*, const BigIntVal&, const BigIntVal&);
  static DoubleVal PmodDouble(FunctionContext*, const DoubleVal&, const DoubleVal&);
  static FloatVal FmodFloat(FunctionContext*, const FloatVal&, const FloatVal&);
  static DoubleVal FmodDouble(FunctionContext*, const DoubleVal&, const DoubleVal&);

  template <typename T> static T Positive(FunctionContext*, const T&);
  template <typename T> static T Negative(FunctionContext*, const T&);

  static BigIntVal QuotientDouble(FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BigIntVal QuotientBigInt(FunctionContext*, const BigIntVal&, const BigIntVal&);

  template <typename VAL_TYPE, bool ISLEAST>
  static VAL_TYPE LeastGreatest(FunctionContext*, int num_args, const VAL_TYPE* args);
  template <bool ISLEAST> static StringVal LeastGreatest(
      FunctionContext*, int num_args, const StringVal* args);
  template <bool ISLEAST> static TimestampVal LeastGreatest(
      FunctionContext*, int num_args, const TimestampVal* args);
  template <bool ISLEAST> static DecimalVal LeastGreatest(
      FunctionContext*, int num_args, const DecimalVal* args);

 private:
  static const int32_t MIN_BASE = 2;
  static const int32_t MAX_BASE = 36;
  static const char* ALPHANUMERIC_CHARS;

  /// Converts src_num in decimal to dest_base.
  static StringVal DecimalToBase(FunctionContext*, int64_t src_num, int8_t dest_base);

  /// Converts src_num representing a number in src_base but encoded in decimal
  /// into its actual decimal number.
  /// For example, if src_num is 21 and src_base is 5,
  /// then this function sets *result to 2*5^1 + 1*5^0 = 11.
  /// Returns false if overflow occurred, true upon success.
  static bool DecimalInBaseToDecimal(int64_t src_num, int8_t src_base, int64_t* result);

  /// Helper function used in Conv to implement behavior consistent
  /// with MySQL and Hive in case of numeric overflow during Conv.
  /// Inspects parse_res, and in case of overflow sets num to MAXINT64 if dest_base
  /// is positive, otherwise to -1.
  /// Returns true if no parse_res == PARSE_SUCCESS || parse_res == PARSE_OVERFLOW.
  /// Returns false otherwise, indicating some other error condition.
  static bool HandleParseResult(int8_t dest_base, int64_t* num,
      StringParser::ParseResult parse_res);
};

}

#endif
