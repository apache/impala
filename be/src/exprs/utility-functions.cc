// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exprs/utility-functions.h"
#include "exprs/function-call.h"
#include "exprs/expr.h"
#include "util/debug-util.h"
#include "runtime/tuple-row.h"

using namespace std;

namespace impala {

void* UtilityFunctions::Version(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 0);
  e->result_.SetStringVal(GetVersionString());
  return &e->result_.string_val;
}

}
