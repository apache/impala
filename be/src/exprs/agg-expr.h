// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_AGG_EXPR_H_
#define IMPALA_EXPRS_AGG_EXPR_H_

#include <string>
#include "expr.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class TExprNode;

class AggregateExpr: public Expr {
 public:
  TExprOperator::type op() const { return op_; }
  bool is_star() const { return is_star_; }
  bool is_distinct() const { return is_distinct_; }
  virtual std::string DebugString() const;

 protected:
  friend class Expr;

  AggregateExpr(const TExprNode& node);

 private:
  const TExprOperator::type  op_;
  const bool is_star_;
  const bool is_distinct_;
};

}

#endif
