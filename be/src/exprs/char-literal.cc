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

#include "exprs/char-literal.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"
#include "runtime/string-value.h"

using namespace std;

namespace impala {

CharLiteral::CharLiteral(const string& str)
  : Expr(ColumnType(TYPE_CHAR, str.size())) {
  result_.SetStringVal(str);
}

CharLiteral::CharLiteral(uint8_t* data, int len)
  : Expr(ColumnType(TYPE_CHAR, len)) {
  result_.string_val = StringValue(reinterpret_cast<char*>(data), len);
}

void* CharLiteral::ComputeFn(Expr* e, TupleRow* row) {
  CharLiteral* l = static_cast<CharLiteral*>(e);
  return l->result_.string_val.ptr;
}

Status CharLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string CharLiteral::DebugString() const {
  stringstream out;
  out << "CharLiteral(value=" << result_.string_data << ")";
  return out.str();
}

}

