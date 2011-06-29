// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_CASE_EXPR_H_
#define IMPALA_EXPRS_CASE_EXPR_H_

#include "expr.h"

namespace impala {

class TExprNode;

class CaseExpr: public Expr {
 protected:
  friend class Expr;

  CaseExpr(const TExprNode& node);

  bool has_case_expr() const { return has_case_expr_; }
  bool has_else_expr() const { return has_else_expr_; }

 private:
  const bool has_case_expr_;
  const bool has_else_expr_;
};

}

#endif
