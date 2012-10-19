// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_IN_PREDICATE_H_
#define IMPALA_EXPRS_IN_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"

namespace impala {

class InPredicate : public Predicate {
 protected:
  friend class Expr;

  InPredicate(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  virtual std::string DebugString() const;

 private:
   const bool is_not_in_;
   static void* ComputeFn(Expr* e, TupleRow* row);
};

}

#endif
