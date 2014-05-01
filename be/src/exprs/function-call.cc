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
  : Expr(node), regex_(NULL), scale_(0) {
}

Status FunctionCall::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc));

  // TODO: remove when UDF allow for an Init() function.
  const string& name = fn_.name.function_name;
  if (name == "now" ||
      name == "current_timestamp" ||
      (name == "unix_timestamp" && fn_.arg_types.empty())) {
    DCHECK(state != NULL);
    DCHECK(!state->now()->NotADateTime());
    result_.timestamp_val = *(state->now());
  } else if (name == "pid") {
    DCHECK(state != NULL);
    result_.int_val = state->query_ctxt().pid;
  } else if (name == "user") {
    // Set username from runtime state.
    DCHECK(state != NULL);
    result_.SetStringVal(state->connected_user());
  } else if (name == "current_database") {
    // Set current database from the session.
    DCHECK(state != NULL);
    result_.SetStringVal(state->query_ctxt().session.database);
  } else if (name == "unix_timestamp" || name == "from_unixtime") {
    if (children_.size() < 2) return Status::OK;
    if (children_[1]->IsConstant()) {
      StringValue* fmt = reinterpret_cast<StringValue*>(children_[1]->GetValue(NULL));
      if (fmt != NULL && fmt->len > 0) SetDateTimeFormatCtx(fmt);
    } else {
      SetDateTimeFormatCtx(NULL);
    }
  } else if (name == "truncate" || name == "round") {
    // round/truncate() for decimal take an optional second argument.
    if (type().type == TYPE_DECIMAL && children_.size() == 2) {
      DCHECK(children_[1]->IsConstant());
      int32_t* scale = reinterpret_cast<int32_t*>(children_[1]->GetValue(NULL));
      DCHECK(scale != NULL) << DebugString();
      scale_ = *scale;
    }
  }
  return Status::OK;
}

string FunctionCall::DebugString() const {
  stringstream out;
  out << "FunctionCall("
      << "name=" << fn_.name.function_name << " "
      << "symbol=" << fn_.scalar_fn.symbol << " "
      << Expr::DebugString() << ")";
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
