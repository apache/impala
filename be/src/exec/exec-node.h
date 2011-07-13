// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_EXEC_NODE_H
#define IMPALA_EXEC_EXEC_NODE_H

#include <vector>

#include "common/status.h"

namespace impala {

class ObjectPool;
class RowBatch;
struct RuntimeState;
class TPlan;
class TPlanNode;

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
  // All data referenced by any tuples returned in row_batch must be reachable
  // until the next call to GetNext(). The row_batch, including all mempools that
  // are attached to it, will be destroyed after the GetNext() call.
  // (If mempools contain data that will only be returned in subsequent GetNext()
  // calls, they must *not* be attached to this call's row_batch.)
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch) = 0;

  // Releases all resources that were allocated in Open()/GetNext().
  // Must call Open() again prior to subsequent calls to GetNext().
  virtual Status Close(RuntimeState* state) = 0;

  // Creates exec node tree from list of nodes contained in plan via depth-first
  // traversal. All nodes are placed in pool.
  // Returns error if 'plan' is corrupted, otherwise success.
  static Status CreateTree(ObjectPool* pool, const TPlan& plan, ExecNode** root);

 protected:
  std::vector<ExecNode*> children_;

  // Create a single exec node derived from thrift node; place exec node in 'pool'.
  static Status CreateNode(ObjectPool* pool, const TPlanNode& tnode, ExecNode** node);

  static Status CreateTreeHelper(ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
      ExecNode* parent, int* node_idx, ExecNode** root);
};

}
#endif

