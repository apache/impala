// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_FLOAT_LITERAL_H_
#define IMPALA_EXPRS_FLOAT_LITERAL_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class FloatLiteral: public Expr {
 protected:
  friend class Expr;

  FloatLiteral(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);

 private:
  double value_;

  static void* ComputeFunction(Expr* e, TupleRow* row);
};

}

#endif
