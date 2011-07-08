// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "float-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

FloatLiteral::FloatLiteral(const TExprNode& node)
  : Expr(node), value_(node.float_literal.value) {
}

void* FloatLiteral::ComputeFunction(Expr* e, TupleRow* row) {
  FloatLiteral* l = static_cast<FloatLiteral*>(e);
  return &l->value_;
}

void FloatLiteral::Prepare(RuntimeState* state) {
  compute_function_ = ComputeFunction;
}

}
