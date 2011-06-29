// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_FUNCTION_CALL_H_
#define IMPALA_EXPRS_FUNCTION_CALL_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class FunctionCall: public Expr {
 protected:
  friend class Expr;

  FunctionCall(const TExprNode& node);
};

}

#endif
