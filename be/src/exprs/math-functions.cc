// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/math-functions.h"

#include <math.h>

#include "exprs/expr.h"
#include "runtime/tuple-row.h"

namespace impala { 

void* MathFunctions::Pi(Expr* e, TupleRow* row) {
  e->result_.double_val = M_PI;
  return &e->result_.double_val;
}

}

