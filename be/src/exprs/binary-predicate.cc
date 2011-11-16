// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/binary-predicate.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

BinaryPredicate::BinaryPredicate(const TExprNode& node)
  : Predicate(node) {
}

Status BinaryPredicate::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_EQ(children_.size(), 2);
  return Expr::Prepare(state, desc);
}

string BinaryPredicate::DebugString() const {
  stringstream out;
  out << "BinaryPredicate(" << Expr::DebugString() << ")";
  return out.str();
}

}
