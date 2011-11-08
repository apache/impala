// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "int-literal.h"

#include <glog/logging.h>
#include <sstream>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

IntLiteral::IntLiteral(PrimitiveType type, void* value)
  : Expr(type) {
  DCHECK(value != NULL);
  switch (type_) {
    case TYPE_TINYINT:
      result_.tinyint_val = *reinterpret_cast<bool*>(value);
      break;
    case TYPE_SMALLINT:
      result_.smallint_val = *reinterpret_cast<char*>(value);
      break;
    case TYPE_INT:
      result_.int_val = *reinterpret_cast<int*>(value);
      break;
    case TYPE_BIGINT:
      result_.bigint_val = *reinterpret_cast<long*>(value);
      break;
    default:
      DCHECK(false) << "IntLiteral ctor: bad type: " << TypeToString(type_);
  }
}

IntLiteral::IntLiteral(const TExprNode& node)
  : Expr(node) {
  switch (type_) {
    case TYPE_TINYINT:
      result_.tinyint_val = node.int_literal.value;
      break;
    case TYPE_SMALLINT:
      result_.smallint_val = node.int_literal.value;
      break;
    case TYPE_INT:
      result_.int_val = node.int_literal.value;
      break;
    case TYPE_BIGINT:
      result_.bigint_val = node.int_literal.value;
      break;
    default:
      DCHECK(false) << "IntLiteral ctor: bad type: " << TypeToString(type_);
  }
}

void* IntLiteral::ReturnTinyintValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.tinyint_val;
}

void* IntLiteral::ReturnSmallintValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.smallint_val;
}

void* IntLiteral::ReturnIntValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.int_val;
}

void* IntLiteral::ReturnBigintValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.bigint_val;
}

Status IntLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  Expr::Prepare(state, row_desc);
  switch (type_) {
    case TYPE_TINYINT:
      compute_function_ = ReturnTinyintValue;
      break;
    case TYPE_SMALLINT:
      compute_function_ = ReturnSmallintValue;
      break;
    case TYPE_INT:
      compute_function_ = ReturnIntValue;
      break;
    case TYPE_BIGINT:
      compute_function_ = ReturnBigintValue;
      break;
    default:
      DCHECK(false) << "IntLiteral::Prepare(): bad type: " << TypeToString(type_);
  }
  return Status::OK;
}

string IntLiteral::DebugString() const {
  stringstream out;
  out << "IntLiteral(value=" << result_.bigint_val << ")";
  return out.str();
}

}

