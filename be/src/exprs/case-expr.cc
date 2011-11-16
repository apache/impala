// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "case-expr.h"

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

void* CaseExpr::ComputeFunction(Expr* e, TupleRow* row) {
  // TODO: implement
  return NULL;
}

CaseExpr::CaseExpr(const TExprNode& node)
  : Expr(node),
    has_case_expr_(node.case_expr.has_case_expr),
    has_else_expr_(node.case_expr.has_else_expr) {
}

Status CaseExpr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  compute_function_ = ComputeFunction;
  return Status::OK;
}

string CaseExpr::DebugString() const {
  return "";
}

}
