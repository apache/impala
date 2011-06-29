// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "date-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

DateLiteral::DateLiteral(const TExprNode& node)
  : Expr(node), value_(node.date_literal.value) {
}

}
