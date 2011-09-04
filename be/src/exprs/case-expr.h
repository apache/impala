// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_CASE_EXPR_H_
#define IMPALA_EXPRS_CASE_EXPR_H_

#include <string>
#include "expr.h"

namespace impala {

class TExprNode;

class CaseExpr: public Expr {
 protected:
  friend class Expr;

  CaseExpr(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state);
  virtual std::string DebugString() const;

 private:
  const bool has_case_expr_;
  const bool has_else_expr_;

  static void* ComputeFunction(Expr* e, TupleRow* row);
};

}

#endif
