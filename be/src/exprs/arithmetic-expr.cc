// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/arithmetic-expr.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

ArithmeticExpr::ArithmeticExpr(const TExprNode& node)
  : Expr(node) {
}

Status ArithmeticExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_LE(children_.size(), 2);
  return Expr::Prepare(state, desc);
}

string ArithmeticExpr::DebugString() const {
  stringstream out;
  out << "ArithmeticExpr(" << Expr::DebugString() << ")";
  return out.str();
}

}
