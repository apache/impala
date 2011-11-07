// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_TUPLE_ROW_H
#define IMPALA_RUNTIME_TUPLE_ROW_H

#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/tuple.h"

namespace impala {

// A TupleRow encapsulates a contiguous sequence of Tuple pointers which
// together make up a row. 
class TupleRow {
 public:
  Tuple* GetTuple(int tuple_idx) {
    return tuples_[tuple_idx];
  }

  void SetTuple(int tuple_idx, Tuple* tuple) {
    tuples_[tuple_idx] = tuple;
  }

  // Create a deep copy of this TupleRow.  DeepCopy will allocate from the MemPool and copy
  // the tuple pointers, the tuples and the string data in the tuples.
  TupleRow* DeepCopy(const std::vector<TupleDescriptor*> descs, MemPool* pool) {
    int size = descs.size() * sizeof(Tuple*);
    TupleRow* result = reinterpret_cast<TupleRow*>(pool->Allocate(size));
    for (int i = 0; i < descs.size(); ++i) {
      result->SetTuple(i, this->GetTuple(i)->DeepCopy(*descs[i], pool));
    }
    return result;
  }
 
 private:
  Tuple* tuples_[1];
};

}

#endif
