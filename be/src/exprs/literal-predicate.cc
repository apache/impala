// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "literal-predicate.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

void* LiteralPredicate::ComputeFunction(Expr* e, TupleRow* row) {
  LiteralPredicate* p = static_cast<LiteralPredicate*>(e);
  return (p->is_null_) ? NULL : &p->result_.bool_val;
}

LiteralPredicate::LiteralPredicate(const TExprNode& node)
  : Predicate(node), is_null_(node.literal_pred.is_null) {
  result_.bool_val = node.literal_pred.value;
}

void LiteralPredicate::Prepare(RuntimeState* state) {
  compute_function_ = ComputeFunction;
}

}
