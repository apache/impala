// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_ARITHMETIC_EXPR_H_
#define IMPALA_EXPRS_ARITHMETIC_EXPR_H_

#include <string>
#include "exprs/expr.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class TExprNode;

class ArithmeticExpr: public Expr {
 protected:
  friend class Expr;

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  ArithmeticExpr(const TExprNode& node);

  virtual std::string DebugString() const;
};

}

#endif
