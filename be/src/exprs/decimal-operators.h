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

namespace impala {

class Expr;
struct ExprValue;
class TupleRow;

// Implementation of the decimal operators. These include the cast,
// arithmetic and binary operators.
class DecimalOperators {
 public:
  static void* Cast_decimal_decimal(Expr* e, TupleRow* row);

  static void* Cast_char_decimal(Expr* e, TupleRow* row);
  static void* Cast_short_decimal(Expr* e, TupleRow* row);
  static void* Cast_int_decimal(Expr* e, TupleRow* row);
  static void* Cast_long_decimal(Expr* e, TupleRow* row);
  static void* Cast_float_decimal(Expr* e, TupleRow* row);
  static void* Cast_double_decimal(Expr* e, TupleRow* row);
  static void* Cast_StringValue_decimal(Expr* e, TupleRow* row);

  static void* Cast_decimal_char(Expr* e, TupleRow* row);
  static void* Cast_decimal_short(Expr* e, TupleRow* row);
  static void* Cast_decimal_int(Expr* e, TupleRow* row);
  static void* Cast_decimal_long(Expr* e, TupleRow* row);
  static void* Cast_decimal_float(Expr* e, TupleRow* row);
  static void* Cast_decimal_double(Expr* e, TupleRow* row);
  static void* Cast_decimal_StringValue(Expr* e, TupleRow* row);

  static void* Add_decimal_decimal(Expr* e, TupleRow* row);
  static void* Subtract_decimal_decimal(Expr* e, TupleRow* row);
  static void* Multiply_decimal_decimal(Expr* e, TupleRow* row);
  static void* Divide_decimal_decimal(Expr* e, TupleRow* row);
  static void* Mod_decimal_decimal(Expr* e, TupleRow* row);

  static void* Eq_decimal_decimal(Expr* e, TupleRow* row);
  static void* Ne_decimal_decimal(Expr* e, TupleRow* row);
  static void* Ge_decimal_decimal(Expr* e, TupleRow* row);
  static void* Gt_decimal_decimal(Expr* e, TupleRow* row);
  static void* Le_decimal_decimal(Expr* e, TupleRow* row);
  static void* Lt_decimal_decimal(Expr* e, TupleRow* row);

  static void* Case_decimal(Expr* e, TupleRow* row);

  // The rounding rule when converting decimals. These only apply going from a higher
  // scale to a lower one.
  enum DecimalRoundOp {
    // Additional digits are dropped.
    TRUNCATE,

    // Returns largest value not greater than the value. (digits are dropped for
    // positive values and rounded away from zero for negative values)
    FLOOR,

    // Returns smallest value not smaller than the value. (rounded away from zero
    // for positive values and extra digits dropped for negative values)
    CEIL,

    // Rounded towards zero if the extra digits are less than .5 and away from
    // zero otherwise.
    ROUND
  };

  // Evaluates a round from e->children()[0] storing the result in e->result_, using
  // the rounding rule of 'type'.
  static void* RoundDecimal(Expr* e, TupleRow* row, const DecimalRoundOp& op);

  // Handles the case of rounding to a negative scale. This means rounding to
  // a digit before the decimal point.
  // rounding_scale is the number of digits before the decimal to round to.
  // TODO: can this code be reorganized to combine the two version of RoundDecimal()?
  // The implementation is similar but not quite the same.
  // This code is, in general, harder to read because there are multiple input/output
  // types to handle and all combinations are valid. Another option might be to use
  // templates to generate each pair:
  //   Decimal4Value Round(const Decimal4Value&);
  //   Decimal8Value Round(const Decimal4Value&);
  //   Decimal4Value Round(const Decimal8Value&);
  //   etc.
  static void* RoundDecimalNegativeScale(Expr* e, TupleRow* row,
      const DecimalRoundOp& op, int rounding_scale);

 private:
  // Sets the value of v into e->result_ and returns the ptr to the result.
  static void* SetDecimalVal(Expr* e, int64_t v);
  static void* SetDecimalVal(Expr* e, double );

  // Sets the value of v into e->result_. v_type is the type of v and the decimals
  // are scaled as needed.
  static void* SetDecimalVal(Expr* e, const ColumnType& v_type, const Decimal4Value& v);
  static void* SetDecimalVal(Expr* e, const ColumnType& v_type, const Decimal8Value& v);
  static void* SetDecimalVal(Expr* e, const ColumnType& v_type, const Decimal16Value& v);

  // Returns the delta that needs to be added when the source decimal is rounded to
  // target scale. Returns 0, if no rounding is necessary, or -1/1 if rounding
  // is required.
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
