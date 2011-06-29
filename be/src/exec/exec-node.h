// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_EXEC_NODE_H
#define IMPALA_EXEC_EXEC_NODE_H

#include "common/status.h"

namespace impala {

class RowBatch;
struct RuntimeState;

// Superclass of all executor nodes.
class ExecNode {
 public:
  // Sets up internal structures, etc., without doing any actual work.
  // Must be called prior to Open(). Will only be called once in this
  // node's lifetime.
  virtual Status Prepare(RuntimeState* state) = 0;

  // Performs any preparatory work prior to calling GetNext().
  // Can be called repeatedly (after calls to Close()).
  virtual Status Open(RuntimeState* state) = 0;

  // Retrieves rows and returns them via row_batch. If row_batch
  // is not filled to capacity, it means that there are no more rows
  // to retrieve.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch) = 0;

  // Releases all resources that were allocated in Open()/GetNext().
  // Must call Open() again prior to subsequent calls to GetNext().
  virtual Status Close(RuntimeState* state) = 0;
};

}
#endif

