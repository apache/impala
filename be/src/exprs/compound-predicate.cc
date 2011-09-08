// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/compound-predicate.h"
#include "util/debug-util.h"

using namespace std;

namespace impala {

CompoundPredicate::CompoundPredicate(const TExprNode& node)
  : Predicate(node), op_(node.op) {
}

void* CompoundPredicate::AndComputeFunction(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  // assert(p->children_.size() == 2);
  // assert(p->op_ == TExprOperator::AND);
  Expr* op1 = e->children()[0];
  bool* val1 = reinterpret_cast<bool*>(op1->GetValue(row));
  Expr* op2 = e->children()[1];
  bool* val2 = reinterpret_cast<bool*>(op2->GetValue(row));
  // <> && false is false
  if ((val1 != NULL && !*val1) || (val2 != NULL && !*val2)) {
    p->result_.bool_val = false;
    return &p->result_.bool_val;
  }
  // true && NULL is NULL
  if (val1 == NULL || val2 == NULL) {
    return NULL;
  }
  p->result_.bool_val = true;
  return &p->result_.bool_val;
}

void* CompoundPredicate::OrComputeFunction(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  // assert(p->children_.size() == 2);
  // assert(p->op_ == TExprOperator::OR);
  Expr* op1 = e->children()[0];
  bool* val1 = reinterpret_cast<bool*>(op1->GetValue(row));
  Expr* op2 = e->children()[1];
  bool* val2 = reinterpret_cast<bool*>(op2->GetValue(row));
  // <> || true is true
  if ((val1 != NULL && *val1) || (val2 != NULL && *val2)) {
    p->result_.bool_val = true;
    return &p->result_.bool_val;
  }
  // false || NULL is NULL
  if (val1 == NULL || val2 == NULL) {
    return NULL;
  }
  p->result_.bool_val = false;
  return &p->result_.bool_val;
}

void* CompoundPredicate::NotComputeFunction(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  // assert(p->children_.size() == 1);
  // assert(p->op_ == TExprOperator::NOT);
  Expr* op = e->children()[0];
  bool* val = reinterpret_cast<bool*>(op->GetValue(row));
  if (val == NULL) return NULL;
  p->result_.bool_val = !*val;
  return &p->result_.bool_val;
}

Status CompoundPredicate::Prepare(RuntimeState* state) {
  Expr::Prepare(state);
  DCHECK(type_ != INVALID_TYPE);
  DCHECK_LE(children_.size(), 2);
  switch (op_) {
    case TExprOperator::AND:
      compute_function_ = AndComputeFunction;
      return Status::OK;
    case TExprOperator::OR:
      compute_function_ = OrComputeFunction;
      return Status::OK;
    case TExprOperator::NOT:
      compute_function_ = NotComputeFunction;
      return Status::OK;
    default:
      DCHECK(false) << "Invalid compound predicate op: " << op_;
  }
  return Status::OK;
}

string CompoundPredicate::DebugString() const {
  stringstream out;
  out << "CompoundPredicate(op=" << op_ << " " << Expr::DebugString() << ")";
  return out.str();
}

}
