// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_UNION_NODE_H_
#define IMPALA_EXEC_UNION_NODE_H_

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include <boost/scoped_ptr.hpp>

namespace impala {

class Tuple;
class TupleRow;

/// Node that merges the results of its children by materializing their
/// evaluated expressions into row batches. The UnionNode pulls row batches from its
/// children sequentially, i.e., it exhausts one child completely before moving
/// on to the next one.
class UnionNode : public ExecNode {
 public:
  UnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 private:
  /// Tuple id resolved in Prepare() to set tuple_desc_;
  int tuple_id_;

  /// Descriptor for tuples this union node constructs.
  const TupleDescriptor* tuple_desc_;

  /// Const exprs materialized by this node. These exprs don't refer to any children.
  std::vector<std::vector<ExprContext*> > const_result_expr_ctx_lists_;

  /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
  std::vector<std::vector<ExprContext*> > result_expr_ctx_lists_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Index of current const result expr list.
  int const_result_expr_idx_;

  /// Index of current child.
  int child_idx_;

  /// Current row batch of current child. We reset the pointer to a new RowBatch
  /// when switching to a different child.
  boost::scoped_ptr<RowBatch> child_row_batch_;

  /// Index of current row in child_row_batch_.
  int child_row_idx_;

  /// Saved from the last to GetNext() on the current child.
  bool child_eos_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Opens the child at child_idx_, fetches the first batch into child_row_batch_,
  /// and sets child_row_idx_ to 0. May set child_eos_.
  Status OpenCurrentChild(RuntimeState* state);

  /// Evaluates exprs on all rows in child_row_batch_ starting from child_row_idx_,
  /// and materializes their results into *tuple.
  /// Adds *tuple into row_batch, and increments *tuple.
  /// If const_exprs is true, then the exprs are evaluated exactly once without
  /// fetching rows from child_row_batch_.
  /// Only commits tuples to row_batch if they are not filtered by conjuncts.
  /// Returns an error status if evaluating an expression results in one.
  Status EvalAndMaterializeExprs(const std::vector<ExprContext*>& ctxs,
      bool const_exprs, Tuple** tuple, RowBatch* row_batch);
};

}

#endif
