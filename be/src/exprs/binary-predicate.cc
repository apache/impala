// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "binary-predicate.h"

namespace impala {

BinaryPredicate::BinaryPredicate(const TExprNode& node)
  : Predicate(node), op_(node.op) {
}

}
