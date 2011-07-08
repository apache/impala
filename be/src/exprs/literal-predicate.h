// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_LITERAL_PREDICATE_H_
#define IMPALA_EXPRS_LITERAL_PREDICATE_H_

#include "exprs/predicate.h"

namespace impala {

class TExprNode;

class LiteralPredicate: public Predicate {
 protected:
  friend class Expr;

  LiteralPredicate(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);

 private:
  ExprValue result_;

  static void* ComputeFunction(Expr* e, TupleRow* row);
};

}

#endif
