// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H
#define IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H

#include <stdint.h>

namespace impala {

class Expr;
class TupleRow;

class ConditionalFunctions {
 public:
  static void* IfBool(Expr* e, TupleRow* row);
  static void* IfInt(Expr* e, TupleRow* row);
  static void* IfFloat(Expr* e, TupleRow* row);
  static void* IfString(Expr* e, TupleRow* row);
  static void* IfTimestamp(Expr* e, TupleRow* row);
  static void* CoalesceBool(Expr* e, TupleRow* row);
  static void* CoalesceInt(Expr* e, TupleRow* row);
  static void* CoalesceFloat(Expr* e, TupleRow* row);
  static void* CoalesceString(Expr* e, TupleRow* row);
  static void* CoalesceTimestamp(Expr* e, TupleRow* row);
  // Compute function of case expr if its has_case_expr_ is false.
  static void* NoCaseComputeFn(Expr* e, TupleRow* row);
};

}

#endif
