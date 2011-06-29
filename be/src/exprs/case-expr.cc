// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "case-expr.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

CaseExpr::CaseExpr(const TExprNode& node)
  : Expr(node), has_case_expr_(node.case_expr.has_case_expr),
    has_else_expr_(node.case_expr.has_else_expr) {
}

}
