// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "arithmetic-expr.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

ArithmeticExpr::ArithmeticExpr(const TExprNode& node)
  : Expr(node), op_(node.op) {
}

}
