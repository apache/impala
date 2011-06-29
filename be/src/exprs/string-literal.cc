// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "string-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

StringLiteral::StringLiteral(const TExprNode& node)
  : Expr(node), value_(node.string_literal.value) {
}

}

