// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "null-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

NullLiteral::NullLiteral(const TExprNode& node)
  : Expr(node) {
}

void* NullLiteral::ReturnValue(Expr* e, TupleRow* row) {
  return NULL;
}

Status NullLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  return Status::OK;
}

}

