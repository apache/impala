// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_AGG_EXPR_H_
#define IMPALA_EXPRS_AGG_EXPR_H_

#include "expr.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

class TExprNode;

class AggExpr: public Expr {
 protected:
  friend class Expr;

  AggExpr(const TExprNode& node);

  bool is_star() const { return is_star_; }
  bool is_distinct() const { return is_distinct_; }

 private:
  const bool is_star_;
  const bool is_distinct_;
};

}

#endif
