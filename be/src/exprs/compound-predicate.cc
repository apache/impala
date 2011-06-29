// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "compound-predicate.h"

namespace impala {

CompoundPredicate::CompoundPredicate(TExprNode node)
  : Predicate(node), op_(node.op) {
}

}
