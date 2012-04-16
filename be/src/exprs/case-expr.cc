// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/case-expr.h"
#include "exprs/conditional-functions.h"

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

CaseExpr::CaseExpr(const TExprNode& node)
  : Expr(node),
    has_case_expr_(node.case_expr.has_case_expr),
    has_else_expr_(node.case_expr.has_else_expr) {
}

Status CaseExpr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc));
  // Override compute function for this special case.
  // Otherwise keep the one provided by the OpCodeRegistry set in the parent's c'tor.
  if (!has_case_expr_) {
    compute_fn_ = ConditionalFunctions::NoCaseComputeFn;
  }
  return Status::OK;
}

string CaseExpr::DebugString() const {
  stringstream out;
  out << "CaseExpr(has_case_expr=" << has_case_expr_
      << " has_else_expr=" << has_else_expr_
      << " " << Expr::DebugString() << ")";
  return out.str();
}

}
