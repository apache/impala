// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_BINARY_PREDICATE_H_
#define IMPALA_EXPRS_BINARY_PREDICATE_H_

#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class BinaryPredicate : public Predicate {
 protected:
  friend class Expr;

  BinaryPredicate(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);

 private:
  const TExprOperator::type  op_;
};

}

#endif
