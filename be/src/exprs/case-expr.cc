// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
