// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/cast-expr.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

CastExpr::CastExpr(const TExprNode& node)
  : Expr(node) {
}

Status CastExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_EQ(children_.size(), 1);
  return Expr::Prepare(state, desc);
}

string CastExpr::DebugString() const {
  stringstream out;
  out << "CastExpr(" << Expr::DebugString() << ")";
  return out.str();
}

}
