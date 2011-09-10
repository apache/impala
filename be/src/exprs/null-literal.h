// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_NULL_LITERAL_H_
#define IMPALA_EXPRS_NULL_LITERAL_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class NullLiteral: public Expr {
 protected:
  friend class Expr;

  NullLiteral(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);

 private:
  static void* ReturnValue(Expr* e, TupleRow* row);
};

}

#endif
