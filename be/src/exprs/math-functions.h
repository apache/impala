// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_MATH_FUNCTIONS_H
#define IMPALA_EXPRS_MATH_FUNCTIONS_H

#include <stdint.h>
// For StringParser::ParseResult
#include "util/string-parser.h"

namespace impala {

class Expr;
class ExprValue;
class TupleRow;

class MathFunctions {
 public:
  static void* Pi(Expr* e, TupleRow* row);
  static void* E(Expr* e, TupleRow* row);
  static void* Abs(Expr* e, TupleRow* row);
  static void* Sign(Expr* e, TupleRow* row);
  static void* Sin(Expr* e, TupleRow* row);
  static void* Asin(Expr* e, TupleRow* row);
  static void* Cos(Expr* e, TupleRow* row);
  static void* Acos(Expr* e, TupleRow* row);
  static void* Tan(Expr* e, TupleRow* row);
  static void* Atan(Expr* e, TupleRow* row);
  static void* Radians(Expr* e, TupleRow* row);
  static void* Degrees(Expr* e, TupleRow* row);
  static void* Ceil(Expr* e, TupleRow* row);
  static void* Floor(Expr* e, TupleRow* row);
  static void* Round(Expr* e, TupleRow* row);
  static void* RoundUpTo(Expr* e, TupleRow* row);
  static void* Exp(Expr* e, TupleRow* row);
  static void* Ln(Expr* e, TupleRow* row);
  static void* Log10(Expr* e, TupleRow* row);
  static void* Log2(Expr* e, TupleRow* row);
  static void* Log(Expr* e, TupleRow* row);
  static void* Pow(Expr* e, TupleRow* row);
  static void* Sqrt(Expr* e, TupleRow* row);
  static void* Rand(Expr* e, TupleRow* row);
  static void* RandSeed(Expr* e, TupleRow* row);
  static void* Bin(Expr* e, TupleRow* row);
  static void* HexInt(Expr* e, TupleRow* row);
  static void* HexString(Expr* e, TupleRow* row);
  static void* Unhex(Expr* e, TupleRow* row);
  static void* ConvInt(Expr* e, TupleRow* row);
  static void* ConvString(Expr* e, TupleRow* row);
  static void* PmodBigInt(Expr* e, TupleRow* row);
  static void* PmodDouble(Expr* e, TupleRow* row);
  static void* PositiveBigInt(Expr* e, TupleRow* row);
  static void* PositiveDouble(Expr* e, TupleRow* row);
  static void* NegativeBigInt(Expr* e, TupleRow* row);
  static void* NegativeDouble(Expr* e, TupleRow* row);

 private:
  static const int32_t MIN_BASE = 2;
  static const int32_t MAX_BASE = 36;
  static const char* ALPHANUMERIC_CHARS;

  // Converts src_num in decimal to dest_base,
  // and fills expr_val.string_val with the result.
  static void DecimalToBase(int64_t src_num, int8_t dest_base, ExprValue* expr_val);

  // Converts src_num representing a number in src_base but encoded in decimal
  // into its actual decimal number.
  // For example, if src_num is 21 and src_base is 5,
  // then this function sets *result to 2*5^1 + 1*5^0 = 11.
  // Returns false if overflow occurred, true upon success.
  static bool DecimalInBaseToDecimal(int64_t src_num, int8_t src_base, int64_t* result);

  // Helper function used in Conv to implement behavior consistent
  // with MySQL and Hive in case of numeric overflow during Conv.
  // Inspects parse_res, and in case of overflow sets num to MAXINT64 if dest_base
  // is positive, otherwise to -1.
  // Returns true if no parse_res == PARSE_SUCCESS || parse_res == PARSE_OVERFLOW.
  // Returns false otherwise, indicating some other error condition.
  static bool HandleParseResult(int8_t dest_base, int64_t* num,
      StringParser::ParseResult parse_res);

};

}

#endif
