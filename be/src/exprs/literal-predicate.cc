// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "literal-predicate.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

void* LiteralPredicate::ComputeFunction(Expr* e, TupleRow* row) {
  LiteralPredicate* p = static_cast<LiteralPredicate*>(e);
  return (p->is_null_) ? NULL : &p->result_.bool_val;
}

LiteralPredicate::LiteralPredicate(const TExprNode& node)
  : Predicate(node), is_null_(node.literal_pred.is_null) {
  result_.bool_val = node.literal_pred.value;
}

Status LiteralPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  compute_function_ = ComputeFunction;
  return Status::OK;
}

string LiteralPredicate::DebugString() const {
  stringstream out;
  out << "LiteralPredicate(value=" << result_.bool_val << ")";
  return out.str();
}

}
