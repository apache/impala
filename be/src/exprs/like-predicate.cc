// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "like-predicate.h"

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
  switch (op_) {
    case TExprOperator::LIKE:
      compute_function_ = LikeFunction;
    case TExprOperator::RLIKE:
    case TExprOperator::REGEXP:
      compute_function_ = RegexpFunction;
  }
}

}
