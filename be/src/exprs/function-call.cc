// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "function-call.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

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
