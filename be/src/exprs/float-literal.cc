// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "float-literal.h"

#include <sstream>
#include <glog/logging.h>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

FloatLiteral::FloatLiteral(PrimitiveType type, void* value)
  : Expr(type) {
  DCHECK(value != NULL);
  switch (type_) {
    case TYPE_FLOAT:
      result_.float_val = *reinterpret_cast<float*>(value);
      break;
    case TYPE_DOUBLE:
      result_.double_val = *reinterpret_cast<double*>(value);
      break;
    default:
      DCHECK(false) << "FloatLiteral ctor: bad type: " << TypeToString(type_);
  }
}

FloatLiteral::FloatLiteral(const TExprNode& node)
  : Expr(node) {
  switch (type_) {
    case TYPE_FLOAT:
      result_.float_val = node.float_literal.value;
      break;
    case TYPE_DOUBLE:
      result_.double_val = node.float_literal.value;
      break;
    default:
      DCHECK(false) << "FloatLiteral ctor: bad type: " << TypeToString(type_);
  }
}

void* FloatLiteral::ReturnFloatValue(Expr* e, TupleRow* row) {
  FloatLiteral* l = static_cast<FloatLiteral*>(e);
  return &l->result_.float_val;
}

void* FloatLiteral::ReturnDoubleValue(Expr* e, TupleRow* row) {
  FloatLiteral* l = static_cast<FloatLiteral*>(e);
  return &l->result_.double_val;
}

Status FloatLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  Expr::Prepare(state, row_desc);
  switch (type_) {
    case TYPE_FLOAT:
      compute_function_ = ReturnFloatValue;
      break;
    case TYPE_DOUBLE:
      compute_function_ = ReturnDoubleValue;
      break;
    default:
      DCHECK(false) << "FloatLiteral::Prepare(): bad type: " << TypeToString(type_);
  }
  return Status::OK;
}

string FloatLiteral::DebugString() const {
  stringstream out;
  out << "FloatLiteral(value=" << result_.double_val << ")";
  return out.str();
}

}
