// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_BOOL_LITERAL_H_
#define IMPALA_EXPRS_BOOL_LITERAL_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class BoolLiteral: public Expr {
 protected:
  friend class Expr;

  BoolLiteral(const TExprNode& node);
  bool value() const { return value_; }

 private:
  const bool value_;
};

}

#endif
