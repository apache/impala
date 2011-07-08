// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "bool-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

BoolLiteral::BoolLiteral(const TExprNode& node)
  : Expr(node), value_(node.bool_literal.value) {
}


void* BoolLiteral::ReturnValue(Expr* e, TupleRow* row) {
  BoolLiteral* l = static_cast<BoolLiteral*>(e);
  return &l->value_;
}

void BoolLiteral::Prepare(RuntimeState* state) {
  compute_function_ = ReturnValue;
}

}

