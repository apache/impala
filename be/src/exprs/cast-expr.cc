// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "cast-expr.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

CastExpr::CastExpr(const TExprNode& node)
  : Expr(node) {
}

}
