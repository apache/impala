// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "literal-predicate.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

LiteralPredicate::LiteralPredicate(const TExprNode& node)
  : Predicate(node), value_(node.literal_pred.value) {
}

}
