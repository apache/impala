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
  switch (opcode_) {
    case TExprOpcode::TIMESTAMP_NOW:
    case TExprOpcode::UNIX_TIMESTAMP: {
      DCHECK(state != NULL);
      DCHECK(!state->now()->NotADateTime());
      result_.timestamp_val = *(state->now());
      break;
    }
    case TExprOpcode::UNIX_TIMESTAMP_STRINGVALUE_STRINGVALUE:
    case TExprOpcode::FROM_UNIXTIME_INT_STRINGVALUE:
    case TExprOpcode::FROM_UNIXTIME_LONG_STRINGVALUE: {
      if (children_.size() < 2) return Status::OK;
      if (children_[1]->IsConstant()) {
        StringValue* fmt = reinterpret_cast<StringValue*>(children_[1]->GetValue(NULL));
        if (fmt != NULL && fmt->len > 0) SetDateTimeFormatCtx(fmt);
      } else {
        SetDateTimeFormatCtx(NULL);
      }
      break;
    }
    case TExprOpcode::UTILITY_PID: {
      // Set pid from runtime state.
      DCHECK(state != NULL);
      result_.int_val = state->pid();
      break;
    }
    case TExprOpcode::UTILITY_USER: {
      // Set username from runtime state.
      DCHECK(state != NULL);
      result_.SetStringVal(state->user());
      break;
    }
    default: break;
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

bool FunctionCall::SetDateTimeFormatCtx(StringValue* fmt) {
  if (fmt != NULL) {
    date_time_format_ctx_.reset(new DateTimeFormatContext(fmt->ptr, fmt->len));
    bool parse_result = TimestampParser::ParseFormatTokens(date_time_format_ctx_.get());
    if (!parse_result) date_time_format_ctx_.reset(NULL);
    return parse_result;
  } else {
    date_time_format_ctx_.reset(new DateTimeFormatContext());
    return true;
  }
}

}
