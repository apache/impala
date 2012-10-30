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

#include "timestamp-literal.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

TimestampLiteral::TimestampLiteral(double val)
  : Expr(TYPE_TIMESTAMP) {
  result_.timestamp_val = TimestampValue(val);
}

void* TimestampLiteral::ComputeFn(Expr* e, TupleRow* row) {
  TimestampLiteral* l = static_cast<TimestampLiteral*>(e);
  return &l->result_.timestamp_val;
}

Status TimestampLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string TimestampLiteral::DebugString() const {
  stringstream out;
  out << "TimestampLiteral(value=" << result_.string_data << ")";
  return out.str();
}

}
