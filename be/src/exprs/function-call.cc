// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sstream>
#include <string>

#include "codegen/llvm-codegen.h"
#include "exprs/function-call.h"
#include "runtime/runtime-state.h"

using namespace llvm;

using namespace std;
using namespace boost;

namespace impala {

FunctionCall::FunctionCall(const TExprNode& node)
  : Expr(node), regex_(NULL) {
}

Status FunctionCall::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc));
  // Set now timestamp from runtime state.
  if (opcode_ == TExprOpcode::TIMESTAMP_NOW || opcode_ == TExprOpcode::UNIX_TIMESTAMP) {
    DCHECK(state != NULL);
    DCHECK(!state->now()->NotADateTime());
    result_.timestamp_val = *(state->now());
  } else if (opcode_ == TExprOpcode::UTILITY_USER) {
    // Set username from runtime state.
    DCHECK(state != NULL);
    result_.SetStringVal(state->user());
  }
  return Status::OK;
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
