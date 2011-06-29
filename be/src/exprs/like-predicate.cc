// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "like-predicate.h"

namespace impala {

LikePredicate::LikePredicate(const TExprNode& node)
  : Predicate(node), op_(node.op) {
}

}
