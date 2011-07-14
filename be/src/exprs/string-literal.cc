// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "string-literal.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

StringLiteral::StringLiteral(const TExprNode& node)
  : Expr(node) {
  result_.SetStringVal(node.string_literal.value);
}

void* StringLiteral::ComputeFunction(Expr* e, TupleRow* row) {
  StringLiteral* l = static_cast<StringLiteral*>(e);
  return &l->result_.string_val;
}

void StringLiteral::Prepare(RuntimeState* state) {
  compute_function_ = ComputeFunction;
}

}

