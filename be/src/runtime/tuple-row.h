// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_TUPLE_ROW_H
#define IMPALA_RUNTIME_TUPLE_ROW_H

#include "runtime/tuple.h"

namespace impala {

// A TupleRow encapsulates a contiguous sequence of Tuple pointers which
// together make up a row. Each TupleRow is part of a RowBatch.
class TupleRow {
 public:
  Tuple* GetTuple(int tuple_idx) {
    return tuples_[tuple_idx];
  }

  void SetTuple(int tuple_idx, Tuple* tuple) {
    tuples_[tuple_idx] = tuple;
  }

 private:
  Tuple* tuples_[1];
};

}

#endif
