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


#ifndef IMPALA_EXPRS_CAST_FUNCTIONS_H
#define IMPALA_EXPRS_CAST_FUNCTIONS_H

#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::DateVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;

/// Some of the numeric (number to number) conversion functions always succeed and have
/// some defined way of handling overflow (e.g. integer to integer conversions). Some of
/// them may fail, such as floating point to integer conversions as well as double to
/// float conversion. See https://en.cppreference.com/w/cpp/language/implicit_conversion.
/// If the conversion fails, an error is raised.
///
/// Other conversion functions either raise an error or return NULL on failure.
class CastFunctions {
 public:
  static BooleanVal CastToBooleanVal(FunctionContext* context, const TinyIntVal& val);
  static BooleanVal CastToBooleanVal(FunctionContext* context, const SmallIntVal& val);
  static BooleanVal CastToBooleanVal(FunctionContext* context, const IntVal& val);
  static BooleanVal CastToBooleanVal(FunctionContext* context, const BigIntVal& val);
  static BooleanVal CastToBooleanVal(FunctionContext* context, const FloatVal& val);
  static BooleanVal CastToBooleanVal(FunctionContext* context, const DoubleVal& val);
  static BooleanVal CastToBooleanVal(FunctionContext* context, const TimestampVal& val);

  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const BooleanVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const SmallIntVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const IntVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const BigIntVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const FloatVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const DoubleVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const StringVal& val);
  static TinyIntVal CastToTinyIntVal(FunctionContext* context, const TimestampVal& val);

  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const BooleanVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const TinyIntVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const IntVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const BigIntVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const FloatVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const DoubleVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const StringVal& val);
  static SmallIntVal CastToSmallIntVal(FunctionContext* context, const TimestampVal& val);

  static IntVal CastToIntVal(FunctionContext* context, const BooleanVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const TinyIntVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const SmallIntVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const BigIntVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const FloatVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const DoubleVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const StringVal& val);
  static IntVal CastToIntVal(FunctionContext* context, const TimestampVal& val);

  static BigIntVal CastToBigIntVal(FunctionContext* context, const BooleanVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const TinyIntVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const SmallIntVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const IntVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const FloatVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const DoubleVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const StringVal& val);
  static BigIntVal CastToBigIntVal(FunctionContext* context, const TimestampVal& val);

  static FloatVal CastToFloatVal(FunctionContext* context, const BooleanVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const TinyIntVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const SmallIntVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const IntVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const BigIntVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const DoubleVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const StringVal& val);
  static FloatVal CastToFloatVal(FunctionContext* context, const TimestampVal& val);

  static DoubleVal CastToDoubleVal(FunctionContext* context, const BooleanVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const TinyIntVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const SmallIntVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const IntVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const BigIntVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const FloatVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const StringVal& val);
  static DoubleVal CastToDoubleVal(FunctionContext* context, const TimestampVal& val);

  static StringVal CastToStringVal(FunctionContext* context, const BooleanVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const TinyIntVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const SmallIntVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const IntVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const BigIntVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const FloatVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const DoubleVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const TimestampVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const DateVal& val);
  static StringVal CastToStringVal(FunctionContext* context, const StringVal& val);

  static StringVal CastToChar(FunctionContext* context, const StringVal& val);
  static StringVal CastToVarchar(FunctionContext* context, const StringVal& val);

  static TimestampVal CastToTimestampVal(FunctionContext* context, const BooleanVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const TinyIntVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const SmallIntVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const IntVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const BigIntVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const FloatVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const DoubleVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const StringVal& val);
  static TimestampVal CastToTimestampVal(FunctionContext* context, const DateVal& val);

  static DateVal CastToDateVal(FunctionContext* context, const StringVal& val);
  static DateVal CastToDateVal(FunctionContext* context, const TimestampVal& val);
};

}

#endif
