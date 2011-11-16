// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include "exprs/agg-expr.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

AggregateExpr::AggregateExpr(const TExprNode& node)
  : Expr(node),
    agg_op_(node.agg_expr.op),
    is_star_(node.agg_expr.is_star),
    is_distinct_(node.agg_expr.is_distinct) {
}

Status AggregateExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, desc));
  if (agg_op_ == TAggregationOp::INVALID) {
    stringstream out;
    out << "AggregateExpr::Prepare: Invalid aggregation op: " << agg_op_;
    return Status(out.str());
  }
  return Status::OK;
}

string AggregateExpr::DebugString() const {
  stringstream out;
  out << "AggExpr(star=" << is_star_ << " distinct=" << is_distinct_
      << " " << Expr::DebugString() << ")";
  return out.str();
}

}

