// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
