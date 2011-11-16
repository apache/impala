// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/compound-predicate.h"
#include "util/debug-util.h"

using namespace std;

namespace impala {

CompoundPredicate::CompoundPredicate(const TExprNode& node)
  : Predicate(node) {
}

Status CompoundPredicate::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_LE(children_.size(), 2);
  return Expr::Prepare(state, desc);
}

void* CompoundPredicate::AndComputeFunction(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  DCHECK_EQ(p->children_.size(), 2);
  DCHECK_EQ(p->opcode_, TExprOpcode::COMPOUND_AND);
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
  DCHECK_EQ(p->children_.size(), 2);
  DCHECK_EQ(p->opcode_, TExprOpcode::COMPOUND_OR);
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
  DCHECK_EQ(p->children_.size(), 1);
  DCHECK_EQ(p->opcode_, TExprOpcode::COMPOUND_NOT);
  Expr* op = e->children()[0];
  bool* val = reinterpret_cast<bool*>(op->GetValue(row));
  if (val == NULL) return NULL;
  p->result_.bool_val = !*val;
  return &p->result_.bool_val;
}

string CompoundPredicate::DebugString() const {
  stringstream out;
  out << "CompoundPredicate(" << Expr::DebugString() << ")";
  return out.str();
}

}
