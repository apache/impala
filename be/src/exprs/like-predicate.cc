// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "like-predicate.h"

using namespace std;

namespace impala {

void* LikePredicate::LikeFunction(Expr* e, TupleRow* row) {
  // TODO: implement w/ boost regexp
  return NULL;
}

void* LikePredicate::RegexpFunction(Expr* e, TupleRow* row) {
  // TODO: implement w/ boost regexp
  return NULL;
}

LikePredicate::LikePredicate(const TExprNode& node)
  : Predicate(node), op_(node.op) {
}

void LikePredicate::Prepare(RuntimeState* state) {
  Expr::Prepare(state);
  switch (op_) {
    case TExprOperator::LIKE:
      compute_function_ = LikeFunction;
      break;
    case TExprOperator::RLIKE:
    case TExprOperator::REGEXP:
      compute_function_ = RegexpFunction;
      break;
    default:
      DCHECK(false) << "bad LIKE op: " << op_;
  }
}

string LikePredicate::DebugString() const {
  stringstream out;
  out << "LikePredicate(op=" << op_ << Expr::DebugString() << ")";
  return out.str();
}

}
