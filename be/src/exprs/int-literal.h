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

 private:
  const long value_;
};

}

#endif
