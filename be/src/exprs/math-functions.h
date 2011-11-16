// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_MATH_FUNCTIONS_H
#define IMPALA_EXPRS_MATH_FUNCTIONS_H

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class MathFunctions {
 public:
  static void Init(OpcodeRegistry*);

  static void* Pi(Expr* e, TupleRow* row);
};

}

#endif
