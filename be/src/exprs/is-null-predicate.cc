// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/is-null-predicate.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

void* IsNullPredicate::ComputeFunction(Expr* e, TupleRow* row) {
  IsNullPredicate* p = static_cast<IsNullPredicate*>(e);
  // assert(p->children_.size() == 1);
  Expr* op = e->children()[0];
  void* val = op->GetValue(row);
  p->result_.bool_val = (val == NULL) == !p->is_not_null_;
  return &p->result_.bool_val;
}

IsNullPredicate::IsNullPredicate(const TExprNode& node)
  : Predicate(node),
    is_not_null_(node.is_null_pred.is_not_null) {
}

Status IsNullPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  compute_function_ = ComputeFunction;
  return Status::OK;
}

string IsNullPredicate::DebugString() const {
  stringstream out;
  out << "IsNullPredicate(not_null=" << is_not_null_ << Expr::DebugString() << ")";
  return out.str();
}

}
