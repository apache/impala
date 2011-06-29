// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "function-call.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

FunctionCall::FunctionCall(const TExprNode& node)
  : Expr(node) {
}

}
