// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "cast-expr.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

// TODO: generate cast eval functions between all legal combinations of source
// and target type

CastExpr::CastExpr(const TExprNode& node)
  : Expr(node) {
}

void CastExpr::Prepare(RuntimeState* state) {
}

}
