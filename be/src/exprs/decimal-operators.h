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

 private:
  // Sets the value of v into e->result_ and returns the ptr to the result.
  static void* SetDecimalVal(Expr* e, int64_t v);
  static void* SetDecimalVal(Expr* e, double );

  // Sets the value of v into e->result_. v_type is the type of v and the decimals
  // are scaled as needed.
  static void* SetDecimalVal(Expr* e, const ColumnType& v_type, const Decimal4Value& v);
  static void* SetDecimalVal(Expr* e, const ColumnType& v_type, const Decimal8Value& v);
  static void* SetDecimalVal(Expr* e, const ColumnType& v_type, const Decimal16Value& v);
};

}

#endif
