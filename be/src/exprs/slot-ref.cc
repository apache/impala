// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/expr.h"  // contains SlotRef definition

#include "gen-cpp/Exprs_types.h"

namespace impala {

SlotRef::SlotRef(const TExprNode& node)
  : Expr(node, true),
    slot_id_(node.slot_ref.slot_id),
    // slot_/null_indicator_offset_ are set in Prepare()
    null_indicator_offset_(0, 0) {
}

void SlotRef::Prepare(RuntimeState* state) {
}

}
