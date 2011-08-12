// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include "exprs/agg-expr.h"
#include "util/debug-string-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

AggregateExpr::AggregateExpr(const TExprNode& node)
  : Expr(node),
    op_(node.op),
    is_star_(node.agg_expr.is_star),
    is_distinct_(node.agg_expr.is_distinct) {
}

string AggregateExpr::DebugString() const {
  stringstream out;
  out << "AggExpr(op=" << op_ << " star=" << is_star_ << " distinct=" << is_distinct_
      << " " << Expr::DebugString() << ")";
  return out.str();
}

}

