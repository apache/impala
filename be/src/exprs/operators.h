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

#ifndef IMPALA_EXPRS_OPERATORS_H
#define IMPALA_EXPRS_OPERATORS_H

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

/// Operators written against the UDF interface.
class Operators {
 public:
  static TinyIntVal Bitnot_TinyIntVal(FunctionContext*, const TinyIntVal&);
  static SmallIntVal Bitnot_SmallIntVal(FunctionContext*, const SmallIntVal&);
  static IntVal Bitnot_IntVal(FunctionContext*, const IntVal&);
  static BigIntVal Bitnot_BigIntVal(FunctionContext*, const BigIntVal&);

  static BigIntVal Factorial_TinyIntVal(FunctionContext*, const TinyIntVal&);
  static BigIntVal Factorial_SmallIntVal(FunctionContext*, const SmallIntVal&);
  static BigIntVal Factorial_IntVal(FunctionContext*, const IntVal&);
  static BigIntVal Factorial_BigIntVal(FunctionContext*, const BigIntVal&);

  static TinyIntVal Add_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Add_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Add_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Add_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static FloatVal Add_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static DoubleVal Add_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);

  static TinyIntVal Subtract_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Subtract_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Subtract_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Subtract_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static FloatVal Subtract_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static DoubleVal Subtract_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);

  static TinyIntVal Multiply_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Multiply_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Multiply_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Multiply_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static FloatVal Multiply_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static DoubleVal Multiply_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);

  static DoubleVal Divide_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);

  static TinyIntVal Int_divide_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Int_divide_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Int_divide_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Int_divide_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);

  static TinyIntVal Mod_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Mod_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Mod_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Mod_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);

  static TinyIntVal Bitand_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Bitand_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Bitand_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Bitand_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);

  static TinyIntVal Bitxor_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Bitxor_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Bitxor_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Bitxor_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);

  static TinyIntVal Bitor_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static SmallIntVal Bitor_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static IntVal Bitor_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BigIntVal Bitor_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);

  static BooleanVal Eq_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal Eq_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal Eq_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal Eq_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal Eq_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal Eq_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal Eq_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal Eq_DateVal_DateVal(FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal Eq_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Eq_Char_Char(FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Eq_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal Ne_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal Ne_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal Ne_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal Ne_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal Ne_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal Ne_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal Ne_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal Ne_DateVal_DateVal(FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal Ne_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Ne_Char_Char(FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Ne_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal DistinctFrom_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal DistinctFrom_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal DistinctFrom_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal DistinctFrom_IntVal_IntVal(
      FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal DistinctFrom_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal DistinctFrom_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal DistinctFrom_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal DistinctFrom_DateVal_DateVal(
      FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal DistinctFrom_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal DistinctFrom_Char_Char(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal DistinctFrom_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal NotDistinct_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal NotDistinct_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal NotDistinct_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal NotDistinct_IntVal_IntVal(
      FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal NotDistinct_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal NotDistinct_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal NotDistinct_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal NotDistinct_DateVal_DateVal(
      FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal NotDistinct_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal NotDistinct_Char_Char(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal NotDistinct_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal Gt_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal Gt_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal Gt_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal Gt_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal Gt_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal Gt_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal Gt_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal Gt_DateVal_DateVal(FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal Gt_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Gt_Char_Char(FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Gt_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal Lt_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal Lt_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal Lt_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal Lt_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal Lt_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal Lt_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal Lt_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal Lt_DateVal_DateVal(FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal Lt_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Lt_Char_Char(FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Lt_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal Ge_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal Ge_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal Ge_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal Ge_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal Ge_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal Ge_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal Ge_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal Ge_DateVal_DateVal(FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal Ge_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Ge_Char_Char( FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Ge_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);

  static BooleanVal Le_BooleanVal_BooleanVal(
      FunctionContext*, const BooleanVal&, const BooleanVal&);
  static BooleanVal Le_TinyIntVal_TinyIntVal(
      FunctionContext*, const TinyIntVal&, const TinyIntVal&);
  static BooleanVal Le_SmallIntVal_SmallIntVal(
      FunctionContext*, const SmallIntVal&, const SmallIntVal&);
  static BooleanVal Le_IntVal_IntVal(FunctionContext*, const IntVal&, const IntVal&);
  static BooleanVal Le_BigIntVal_BigIntVal(
      FunctionContext*, const BigIntVal&, const BigIntVal&);
  static BooleanVal Le_FloatVal_FloatVal(
      FunctionContext*, const FloatVal&, const FloatVal&);
  static BooleanVal Le_DoubleVal_DoubleVal(
      FunctionContext*, const DoubleVal&, const DoubleVal&);
  static BooleanVal Le_DateVal_DateVal(FunctionContext*, const DateVal&, const DateVal&);
  static BooleanVal Le_StringVal_StringVal(
      FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Le_Char_Char(FunctionContext*, const StringVal&, const StringVal&);
  static BooleanVal Le_TimestampVal_TimestampVal(
      FunctionContext*, const TimestampVal&, const TimestampVal&);
};

} // namespace impala
#endif

