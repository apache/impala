// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "agg-expr.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

AggregateExpr::AggregateExpr(const TExprNode& node)
  : Expr(node),
    op_(node.op),
    is_star_(node.agg_expr.is_star),
    is_distinct_(node.agg_expr.is_distinct) {
}

}
