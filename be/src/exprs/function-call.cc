// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/function-call.h"

using namespace std;

namespace impala {

FunctionCall::FunctionCall(const TExprNode& node)
  : Expr(node) {
}

string FunctionCall::DebugString() const {
  stringstream out;
  out << "FunctionCall(" << Expr::DebugString() << ")";
  return out.str();
}

}
