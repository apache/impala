// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_TIMESTAMP_LITERAL_H_
#define IMPALA_EXPRS_TIMESTAMP_LITERAL_H_

#include <string>
#include "exprs/expr.h"

namespace impala {

class TExprNode;

class TimestampLiteral: public Expr {
 protected:
  friend class Expr;

  TimestampLiteral(double d);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  static void* ComputeFn(Expr* e, TupleRow* row);
};

}

#endif
