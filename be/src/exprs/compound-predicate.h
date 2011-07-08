// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_COMPOUND_PREDICATE_H_
#define IMPALA_EXPRS_COMPOUND_PREDICATE_H_

#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class CompoundPredicate: public Predicate {
 protected:
  friend class Expr;

  CompoundPredicate(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);

 private:
  const TExprOperator::type op_;
  bool value_;  // holds result of GetValue()

  static void* AndComputeFunction(Expr* e, TupleRow* row);
  static void* OrComputeFunction(Expr* e, TupleRow* row);
  static void* NotComputeFunction(Expr* e, TupleRow* row);
};

}

#endif
