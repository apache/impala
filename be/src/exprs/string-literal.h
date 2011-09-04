// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_STRING_LITERAL_H_
#define IMPALA_EXPRS_STRING_LITERAL_H_

#include <string>
#include "exprs/expr.h"
#include "runtime/tuple.h"  // for StringValue

namespace impala {

class TExprNode;

class StringLiteral: public Expr {
 protected:
  friend class Expr;

  StringLiteral(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state);
  virtual std::string DebugString() const;

 private:
  static void* ComputeFunction(Expr* e, TupleRow* row);
};

}

#endif
