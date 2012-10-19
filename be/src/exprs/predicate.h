// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_PREDICATE_H_
#define IMPALA_EXPRS_PREDICATE_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class Predicate: public Expr {
 protected:
  friend class Expr;

  Predicate(const TExprNode& node) : Expr(node) {}
};

}

#endif
