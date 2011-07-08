// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_CAST_EXPR_H_
#define IMPALA_EXPRS_CAST_EXPR_H_

#include "expr.h"

namespace impala {

class TExprNode;

class CastExpr: public Expr {
 public:
  virtual void Prepare(RuntimeState* state);

 protected:
  friend class Expr;
  CastExpr(const TExprNode& node);

 private:
  ExprValue result_;
};

}

#endif
