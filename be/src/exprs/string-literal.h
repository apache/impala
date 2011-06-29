// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_STRING_LITERAL_H_
#define IMPALA_EXPRS_STRING_LITERAL_H_

#include <string>
#include "exprs/expr.h"

namespace impala {

class TExprNode;

class StringLiteral: public Expr {
 protected:
  friend class Expr;

  StringLiteral(const TExprNode& node);

 private:
  const std::string value_;
};

}

#endif
