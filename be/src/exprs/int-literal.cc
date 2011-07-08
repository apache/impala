// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "int-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

IntLiteral::IntLiteral(const TExprNode& node)
  : Expr(node), value_(node.int_literal.value) {
}

void* IntLiteral::ReturnValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->value_;
}

void IntLiteral::Prepare(RuntimeState* state) {
  compute_function_ = ReturnValue;
}

}

