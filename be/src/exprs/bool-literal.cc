// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "bool-literal.h"

#include <sstream>
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

BoolLiteral::BoolLiteral(bool b) 
  : Expr(TYPE_BOOLEAN) {
    result_.bool_val = b;
}

BoolLiteral::BoolLiteral(const TExprNode& node)
  : Expr(node) {
  result_.bool_val = node.bool_literal.value;
}


void* BoolLiteral::ReturnValue(Expr* e, TupleRow* row) {
  BoolLiteral* l = static_cast<BoolLiteral*>(e);
  return &l->result_.bool_val;
}

Status BoolLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_function_ = ReturnValue;
  return Status::OK;
}

string BoolLiteral::DebugString() const {
  stringstream out;
  out << "BoolLiteral(value=" << result_.bool_val << ")";
  return out.str();
}

}

