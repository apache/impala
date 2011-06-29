// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_IS_NULL_PREDICATE_H_
#define IMPALA_EXPRS_IS_NULL_PREDICATE_H_

#include "exprs/predicate.h"

namespace impala {

class TExprNode;

class IsNullPredicate: public Predicate {
 protected:
  friend class Expr;

  IsNullPredicate(const TExprNode& node);

 private:
  const bool is_not_null_;
};

}

#endif
