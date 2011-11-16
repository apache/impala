// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_DEBUG_UTIL_H
#define IMPALA_UTIL_DEBUG_UTIL_H

#include <ostream>
#include <string>

#include "gen-cpp/Opcodes_types.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class RowDescriptor;
class TupleDescriptor;
class Tuple;
class TupleRow;

std::ostream& operator<<(std::ostream& os, const TExprOpcode::type& op);
std::ostream& operator<<(std::ostream& os, const TAggregationOp::type& op);

std::string PrintTuple(const Tuple* t, const TupleDescriptor& d);
std::string PrintRow(TupleRow* row, const RowDescriptor& d);

}

#endif
