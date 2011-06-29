// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "bool-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

BoolLiteral::BoolLiteral(const TExprNode& node)
  : Expr(node), value_(node.bool_literal.value) {
}

}

