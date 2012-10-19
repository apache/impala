// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_DATE_LITERAL_H_
#define IMPALA_EXPRS_DATE_LITERAL_H_

#include <string>
#include "exprs/expr.h"

namespace impala {

class TExprNode;

class DateLiteral: public Expr {
 protected:
  friend class Expr;

  DateLiteral(const TExprNode& node);
};

}

#endif
