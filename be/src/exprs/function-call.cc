// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <string>
#include <glog/logging.h>

#include "exprs/function-call.h"

using namespace std;
using namespace boost;

namespace impala {

FunctionCall::FunctionCall(const TExprNode& node)
  : Expr(node), regex_(NULL) {
}

string FunctionCall::DebugString() const {
  stringstream out;
  out << "FunctionCall(" << Expr::DebugString() << ")";
  return out.str();
}

bool FunctionCall::SetRegex(const string& pattern) {
  try {
    regex_.reset(new regex(pattern, regex_constants::extended));
  } catch(bad_expression& e) {
    return false;
  }
  return true;
}

void FunctionCall::SetReplaceStr(const StringValue* str_val) {
  replace_str_.reset(new string(str_val->ptr, str_val->len));
}

}
