// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_LIKE_PREDICATE_H_
#define IMPALA_EXPRS_LIKE_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class LikePredicate: public Predicate {
 protected:
  friend class Expr;

  LikePredicate(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);
  virtual std::string DebugString() const;

 private:
  const TExprOperator::type op_;

  static void* LikeFunction(Expr* e, TupleRow* row);
  static void* RegexpFunction(Expr* e, TupleRow* row);
};

}

#endif
