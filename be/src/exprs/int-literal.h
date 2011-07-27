// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_INT_LITERAL_H_
#define IMPALA_EXPRS_INT_LITERAL_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class IntLiteral: public Expr {
 protected:
  friend class Expr;

  IntLiteral(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);

 private:
  static void* ReturnTinyintValue(Expr* e, TupleRow* row);
  static void* ReturnSmallintValue(Expr* e, TupleRow* row);
  static void* ReturnIntValue(Expr* e, TupleRow* row);
  static void* ReturnBigintValue(Expr* e, TupleRow* row);
};

}

#endif
