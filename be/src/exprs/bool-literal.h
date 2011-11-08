// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_BOOL_LITERAL_H_
#define IMPALA_EXPRS_BOOL_LITERAL_H_

#include <string>
#include "exprs/expr.h"

namespace impala {

class TExprNode;

class BoolLiteral: public Expr {
 protected:
  friend class Expr;

  // Construct a BoolLiteral expr from b
  BoolLiteral(bool b);
  BoolLiteral(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  static void* ReturnValue(Expr* e, TupleRow* row);
};

}

#endif
