// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "int-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

IntLiteral::IntLiteral(const TExprNode& node)
  : Expr(node), value_(node.int_literal.value) {
}

}

