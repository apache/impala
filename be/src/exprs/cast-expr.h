// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_CAST_EXPR_H_
#define IMPALA_EXPRS_CAST_EXPR_H_

#include <string>
#include "expr.h"

namespace impala {

class TExprNode;

class CastExpr: public Expr {
 public:
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 protected:
  friend class Expr;
  CastExpr(const TExprNode& node);
};

}

#endif
