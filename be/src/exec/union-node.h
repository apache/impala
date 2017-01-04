// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_EXEC_UNION_NODE_H_
#define IMPALA_EXEC_UNION_NODE_H_

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"

namespace impala {

class DescriptorTbl;
class ExprContext;
class Tuple;
class TupleRow;
class TPlanNode;

/// Node that merges the results of its children by either materializing their
/// evaluated expressions into row batches or passing through (forwarding) the
/// batches if the input tuple layout is identical to the output tuple layout
/// and expressions don't need to be evaluated. The children should be ordered
/// such that all passthrough children come before the children that need
/// materialization. The union node pulls from its children sequentially, i.e.
/// it exhausts one child completely before moving on to the next one.
class UnionNode : public ExecNode {
 public:
  UnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 private:
  /// Tuple id resolved in Prepare() to set tuple_desc_;
  const int tuple_id_;

  /// Descriptor for tuples this union node constructs.
  const TupleDescriptor* tuple_desc_;

  /// Index of the first non-passthrough child; i.e. a child that needs materialization.
  /// 0 when all children are materialized, 'children_.size()' when no children are
  /// materialized.
  const int first_materialized_child_idx_;

  /// Const exprs materialized by this node. These exprs don't refer to any children.
  /// Only materialized by the first fragment instance to avoid duplication.
  std::vector<std::vector<ExprContext*>> const_expr_lists_;

  /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
  std::vector<std::vector<ExprContext*>> child_expr_lists_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Index of current child.
  int child_idx_;

  /// Current row batch of current child. We reset the pointer to a new RowBatch
  /// when switching to a different child.
  boost::scoped_ptr<RowBatch> child_batch_;

  /// Index of current row in child_row_batch_.
  int child_row_idx_;

  /// Saved from the last to GetNext() on the current child.
  bool child_eos_;

  /// Index of current const result expr list.
  int const_expr_list_idx_;

  /// Index of the child that needs to be closed on the next GetNext() call. Should be set
  /// to -1 if no child needs to be closed.
  int to_close_child_idx_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// GetNext() for the passthrough case. We pass 'row_batch' directly into the GetNext()
  /// call on the child.
  Status GetNextPassThrough(RuntimeState* state, RowBatch* row_batch);

  /// GetNext() for the materialized case. Materializes and evaluates rows from each
  /// non-passthrough child.
  Status GetNextMaterialized(RuntimeState* state, RowBatch* row_batch);

  /// GetNext() for the constant expression case.
  Status GetNextConst(RuntimeState* state, RowBatch* row_batch);

  /// Evaluates 'exprs' over 'row', materializes the results in 'tuple_buf'.
  /// and appends the new tuple to 'dst_batch'. Increments 'num_rows_returned_'.
  inline void MaterializeExprs(const std::vector<ExprContext*>& exprs,
      TupleRow* row, uint8_t* tuple_buf, RowBatch* dst_batch);

  /// Returns true if the child at 'child_idx' can be passed through.
  inline bool IsChildPassthrough(int child_idx) const {
    DCHECK_LT(child_idx, children_.size());
    return child_idx < first_materialized_child_idx_;
  }

  /// Returns true if there are still rows to be returned from passthrough children.
  inline bool HasMorePassthrough() const {
    return child_idx_ < first_materialized_child_idx_;
  }

  /// Returns true if there are still rows to be returned from children that need
  /// materialization.
  inline bool HasMoreMaterialized() const {
    return first_materialized_child_idx_ != children_.size() &&
        child_idx_ < children_.size();
  }

  /// Returns true if there are still rows to be returned from constant expressions.
  inline bool HasMoreConst(const RuntimeState* state) const {
    return state->instance_ctx().per_fragment_instance_idx == 0 &&
        const_expr_list_idx_ < const_expr_lists_.size();
  }

};

}

#endif
