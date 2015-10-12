// Copyright 2014 Cloudera Inc.
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


#ifndef IMPALA_EXPRS_DECIMAL_OPERATORS_H
#define IMPALA_EXPRS_DECIMAL_OPERATORS_H

#include <stdint.h>
#include "runtime/decimal-value.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class Expr;
struct ExprValue;
class TupleRow;

/// Implementation of the decimal operators. These include the cast,
/// arithmetic and binary operators.
class DecimalOperators {
 public:
  static DecimalVal CastToDecimalVal(FunctionContext*, const DecimalVal&);

  static DecimalVal CastToDecimalVal(FunctionContext*, const TinyIntVal&);
  static DecimalVal CastToDecimalVal(FunctionContext*, const SmallIntVal&);
  static DecimalVal CastToDecimalVal(FunctionContext*, const IntVal&);
  static DecimalVal CastToDecimalVal(FunctionContext*, const BigIntVal&);
  static DecimalVal CastToDecimalVal(FunctionContext*, const FloatVal&);
  static DecimalVal CastToDecimalVal(FunctionContext*, const DoubleVal&);
  static DecimalVal CastToDecimalVal(FunctionContext*, const StringVal&);

  static BooleanVal CastToBooleanVal(FunctionContext*, const DecimalVal&);
  static TinyIntVal CastToTinyIntVal(FunctionContext*, const DecimalVal&);
  static SmallIntVal CastToSmallIntVal(FunctionContext*, const DecimalVal&);
  static IntVal CastToIntVal(FunctionContext*, const DecimalVal&);
  static BigIntVal CastToBigIntVal(FunctionContext*, const DecimalVal&);
  static FloatVal CastToFloatVal(FunctionContext*, const DecimalVal&);
  static DoubleVal CastToDoubleVal(FunctionContext*, const DecimalVal&);
  static StringVal CastToStringVal(FunctionContext*, const DecimalVal&);
  static TimestampVal CastToTimestampVal(FunctionContext*, const DecimalVal&);

  static DecimalVal Add_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static DecimalVal Subtract_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static DecimalVal Multiply_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static DecimalVal Divide_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static DecimalVal Mod_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);

  static BooleanVal Eq_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static BooleanVal Ne_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static BooleanVal Ge_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static BooleanVal Gt_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static BooleanVal Le_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);
  static BooleanVal Lt_DecimalVal_DecimalVal(
      FunctionContext*, const DecimalVal&, const DecimalVal&);

  /// The rounding rule when converting decimals. These only apply going from a higher
  /// scale to a lower one.
  enum DecimalRoundOp {
    /// Additional digits are dropped.
    TRUNCATE,

    /// Returns largest value not greater than the value. (digits are dropped for
    /// positive values and rounded away from zero for negative values)
    FLOOR,

    /// Returns smallest value not smaller than the value. (rounded away from zero
    /// for positive values and extra digits dropped for negative values)
    CEIL,

    /// Rounded towards zero if the extra digits are less than .5 and away from
    /// zero otherwise.
    ROUND
  };

  /// Evaluates a round from 'val' and returns the result, using the rounding rule of
  /// 'type'.
  static DecimalVal RoundDecimal(FunctionContext* context, const DecimalVal& val,
      const FunctionContext::TypeDesc& val_type,
      const FunctionContext::TypeDesc& output_type, const DecimalRoundOp& op);

  /// Same as above but infers 'val_type' from the first argument type and 'output_type'
  /// from the return type according to 'context'.
  static DecimalVal RoundDecimal(
      FunctionContext* context, const DecimalVal& val, const DecimalRoundOp& op);

  /// Handles the case of rounding to a negative scale. This means rounding to a digit
  /// before the decimal point.
  /// rounding_scale is the number of digits before the decimal to round to.
  /// TODO: can this code be reorganized to combine the two version of RoundDecimal()?
  /// The implementation is similar but not quite the same.
  /// This code is, in general, harder to read because there are multiple input/output
  /// types to handle and all combinations are valid. Another option might be to use
  /// templates to generate each pair:
  ///   Decimal4Value Round(const Decimal4Value&);
  ///   Decimal8Value Round(const Decimal4Value&);
  ///   Decimal4Value Round(const Decimal8Value&);
  ///   etc.
  static DecimalVal RoundDecimalNegativeScale(FunctionContext* context,
      const DecimalVal& val, const FunctionContext::TypeDesc& val_type,
      const FunctionContext::TypeDesc& output_type, const DecimalRoundOp& op,
      int64_t rounding_scale);

 private:
  /// Converts 'val' to a DecimalVal according to 'type'. 'type' must be a decimal type.
  static DecimalVal IntToDecimalVal(
      FunctionContext* context, const FunctionContext::TypeDesc& type, int64_t val);
  static DecimalVal FloatToDecimalVal(
      FunctionContext* context, const FunctionContext::TypeDesc& type, double val);

  /// Returns the value of 'val' scaled to 'output_type'.
  static DecimalVal ScaleDecimalValue(FunctionContext* context, const Decimal4Value& val,
      const FunctionContext::TypeDesc& val_type,
      const FunctionContext::TypeDesc& output_type);
  static DecimalVal ScaleDecimalValue(FunctionContext* context, const Decimal8Value& val,
      const FunctionContext::TypeDesc& val_type,
      const FunctionContext::TypeDesc& output_type);
  static DecimalVal ScaleDecimalValue(FunctionContext* context, const Decimal16Value& val,
      const FunctionContext::TypeDesc& val_type,
      const FunctionContext::TypeDesc& output_type);

  /// Returns the delta that needs to be added when the source decimal is rounded to
  /// target scale. Returns 0, if no rounding is necessary, or -1/1 if rounding
  /// is required.
  template <typename T>
  static T RoundDelta(const DecimalValue<T>& v, int src_scale, int target_scale,
      const DecimalRoundOp& op) {
    if (op == TRUNCATE) return 0;

    // Adding more digits, rounding does not apply. New digits are just 0.
    if (src_scale <= target_scale) return 0;

    // No need to round for floor() and the value is positive or ceil() and the value
    // is negative.
    if (v.value() > 0 && op == FLOOR) return 0;
    if (v.value() < 0 && op == CEIL) return 0;

    // We are removing the decimal places. Extract the value of the digits we are
    // dropping. For example, going from scale 5->2, means we want the last 3 digits.
    int delta_scale = src_scale - target_scale;
    DCHECK_GT(delta_scale, 0);

    // 10^delta_scale
    T trailing_base = DecimalUtil::GetScaleMultiplier<T>(delta_scale);
    T trailing_digits = v.value() % trailing_base;

    // If the trailing digits are zero, never round.
    if (trailing_digits == 0) return 0;

    // Trailing digits are non-zero.
    if (op == CEIL) return 1;
    if (op == FLOOR) return -1;

    DCHECK_EQ(op, ROUND);
    // TODO: > or greater than or equal. i.e. should .500 round up?
    if (abs(trailing_digits) < trailing_base / 2) return 0;
    return v.value() < 0 ? -1 : 1;
  }
};

}

#endif
