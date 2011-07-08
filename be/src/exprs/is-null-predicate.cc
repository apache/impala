// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "is-null-predicate.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

void* IsNullPredicate::ComputeFunction(Expr* e, TupleRow* row) {
  IsNullPredicate* p = static_cast<IsNullPredicate*>(e);
  // assert(p->children_.size() == 1);
  Expr* op = e->children()[0];
  p->result_.bool_val = (op == NULL) == !p->is_not_null_;
  return &p->result_.bool_val;
}

IsNullPredicate::IsNullPredicate(const TExprNode& node)
  : Predicate(node),
    is_not_null_(node.is_null_pred.is_not_null) {
}

void IsNullPredicate::Prepare(RuntimeState* state) {
  compute_function_ = ComputeFunction;
}

}
