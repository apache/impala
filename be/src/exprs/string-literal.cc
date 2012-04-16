// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "string-literal.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

StringLiteral::StringLiteral(const StringValue& str) 
  : Expr(TYPE_STRING) {
  result_.SetStringVal(str);
}

StringLiteral::StringLiteral(const string& str) 
  : Expr(TYPE_STRING) {
  result_.SetStringVal(str);
}

StringLiteral::StringLiteral(const TExprNode& node)
  : Expr(node) {
  result_.SetStringVal(node.string_literal.value);
}

void* StringLiteral::ComputeFn(Expr* e, TupleRow* row) {
  StringLiteral* l = static_cast<StringLiteral*>(e);
  return &l->result_.string_val;
}

Status StringLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string StringLiteral::DebugString() const {
  stringstream out;
  out << "StringLiteral(value=" << result_.string_data << ")";
  return out.str();
}

}

