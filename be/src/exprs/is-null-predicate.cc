// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "is-null-predicate.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

IsNullPredicate::IsNullPredicate(const TExprNode& node)
  : Predicate(node),
    is_not_null_(node.is_null_pred.is_not_null) {
}

}
