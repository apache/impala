// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains type definitions that are used throughout the code base.

#include "sparrow/util.h"

namespace impala {

// for now, these are simply ints; if we find we need to generate ids in the
// backend, we can also introduce separate classes for these to make them
// assignment-incompatible
typedef int TupleId;
typedef int SlotId;
typedef int TableId;
typedef int PlanNodeId;

};
