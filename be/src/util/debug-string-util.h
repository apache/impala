// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <ostream>

#include "gen-cpp/Exprs_types.h"

namespace impala {

std::ostream& operator<<(std::ostream& os, const TExprOperator::type& op);

}
