// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_CASE_EXPR_H_
#define IMPALA_EXPRS_CASE_EXPR_H_

#include <string>
#include "expr.h"

namespace impala {

class TExprNode;

class CaseExpr: public Expr {
 protected:
  friend class Expr;
  friend class ComputeFunctions;
  friend class ConditionalFunctions;

  CaseExpr(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

  bool has_case_expr() { return has_case_expr_; }
  bool has_else_expr() { return has_else_expr_; }

 private:
  const bool has_case_expr_;
  const bool has_else_expr_;
};

}

#endif
