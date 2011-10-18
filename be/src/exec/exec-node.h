// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_EXEC_NODE_H
#define IMPALA_EXEC_EXEC_NODE_H

#include <vector>
#include <sstream>

#include "common/status.h"
#include "runtime/descriptors.h"  // for RowDescriptor

namespace impala {

class Expr;
class ObjectPool;
class RowBatch;
struct RuntimeState;
class TPlan;
class TPlanNode;
class TupleRow;

// Superclass of all executor nodes.
class ExecNode {
 public:
  // Init conjuncts.
  ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  // Sets up internal structures, etc., without doing any actual work.
  // Must be called prior to Open(). Will only be called once in this
  // node's lifetime.
  // Default implementation only calls Prepare() on the children.
  virtual Status Prepare(RuntimeState* state);

  // Performs any preparatory work prior to calling GetNext().
  // Can be called repeatedly (after calls to Close()).
  virtual Status Open(RuntimeState* state) = 0;

  // Retrieves rows and returns them via row_batch. If row_batch
  // is not filled to capacity, it means that there are no more rows
  // to retrieve.
  // Data referenced by any tuples returned in row_batch must not be overwritten
  // by the callee until Close() is called. The memory holding that data
  // can be returned via row_batch's tuple_data_pool (in which case it may be deleted
  // by the caller) or held on to by the callee. The row_batch, including its
  // tuple_data_pool, will be destroyed by the caller at some point prior to the final
  // Close() call.
  // In other words, if the memory holding the tuple data will be referenced
  // by the callee in subsequent GetNext() calls, it must *not* be attached to the
  // row_batch's tuple_data_pool.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch) = 0;

  // Releases all resources that were allocated in Open()/GetNext().
  // Must call Open() again prior to subsequent calls to GetNext().
  virtual Status Close(RuntimeState* state) = 0;

  // Creates exec node tree from list of nodes contained in plan via depth-first
  // traversal. All nodes are placed in pool.
  // Returns error if 'plan' is corrupted, otherwise success.
  static Status CreateTree(ObjectPool* pool, const TPlan& plan,
                           const DescriptorTbl& descs, ExecNode** root);

  // Collect all scan nodes that are part of this subtree, and return in 'scan_nodes'.
  void CollectScanNodes(std::vector<ExecNode*>* scan_nodes);

  // Returns a string representation in DFS order of the plan rooted at this.
  std::string DebugString() const;

  // Recursive helper method for generating a string for DebugString().
  // Implementations should call DebugString(int, std::stringstream) on their children.
  // Input parameters:
  //   indentation_level: Current level in plan tree.
  // Output parameters:
  //   out: Stream to accumulate debug string.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

  int id() const { return id_; }
  const RowDescriptor& row_desc() const { return row_descriptor_; }

 protected:
  int id_;  // unique w/in single plan tree
  ObjectPool* pool_;
  std::vector<Expr*> conjuncts_;
  std::vector<ExecNode*> children_;
  RowDescriptor row_descriptor_;

  int64_t limit_;  // -1: no limit
  int64_t num_rows_returned_;

  ExecNode* child(int i) { return children_[i]; }

  // Create a single exec node derived from thrift node; place exec node in 'pool'.
  static Status CreateNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs, ExecNode** node);

  static Status CreateTreeHelper(ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
      const DescriptorTbl& descs, ExecNode* parent, int* node_idx, ExecNode** root);

  void PrepareConjuncts(RuntimeState* state);

  // Evaluate conjuncts. Return true if all conjuncts return true, otherwise false.
  bool EvalConjuncts(const std::vector<Expr*>& conjuncts, TupleRow* row);

  // Evaluate conjuncts_. Return true if all conjuncts return true, otherwise false.
  bool EvalConjuncts(TupleRow* row);

  bool ReachedLimit() { return limit_ != -1 && num_rows_returned_ == limit_; }

  virtual bool IsScanNode() const { return false; }
};

}
#endif

