// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "date-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

DateLiteral::DateLiteral(const TExprNode& node)
  : Expr(node) {
  result_.bigint_val = node.date_literal.value;
}

}
