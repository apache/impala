// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "compound-predicate.h"

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
  if (val1 != NULL && !*val1 || val2 != NULL && !*val2) {
    p->value_ = false;
    return &p->value_;
  }
  // true && NULL is NULL
  if (val1 == NULL || val2 == NULL) {
    return NULL;
  }
  p->value_ = true;
  return &p->value_;
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
  if (val1 != NULL && *val1 || val2 != NULL && *val2) {
    p->value_ = true;
    return &p->value_;
  }
  // false || NULL is NULL
  if (val1 == NULL || val2 == NULL) {
    return NULL;
  }
  p->value_ = false;
  return &p->value_;
}

void* CompoundPredicate::NotComputeFunction(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  // assert(p->children_.size() == 1);
  // assert(p->op_ == TExprOperator::NOT);
  Expr* op = e->children()[0];
  bool* val = reinterpret_cast<bool*>(op->GetValue(row));
  if (val == NULL) return NULL;
  p->value_ = !*val;
  return &p->value_;
}

void CompoundPredicate::Prepare(RuntimeState* state) {
  switch (op_) {
    case TExprOperator::AND:
      compute_function_ = AndComputeFunction;
      return;
    case TExprOperator::OR:
      compute_function_ = OrComputeFunction;
      return;
    case TExprOperator::NOT:
      compute_function_ = NotComputeFunction;
      return;
  }
}

}
