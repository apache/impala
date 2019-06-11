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

#include "codegen/codegen-fn-ptr.h"
#include "codegen/impala-ir.h"
#include "exec/exec-node.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"

namespace impala {

class DescriptorTbl;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;
class Tuple;
class TupleRow;
class TPlanNode;
class UnionNode;

class UnionPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  virtual void Codegen(FragmentState* state) override;

  ~UnionPlanNode(){}

  /// Descriptor for tuples this union node constructs.
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Const exprs materialized by this node. These exprs don't refer to any children.
  /// Only materialized by the first fragment instance to avoid duplication.
  std::vector<std::vector<ScalarExpr*>> const_exprs_lists_;

  /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
  std::vector<std::vector<ScalarExpr*>> child_exprs_lists_;

  /// Index of the first non-passthrough child; i.e. a child that needs materialization.
  /// 0 when all children are materialized, 'children_.size()' when no children are
  /// materialized.
  int first_materialized_child_idx_ = -1;

  /// Number of const scalar expressions which will be codegened.
  /// This is only used for observability.
  int64_t num_const_scalar_expr_to_be_codegened_ = 0;

  /// Set as TRUE if codegen status is added.
  bool is_codegen_status_added_ = false;

  typedef void (*UnionMaterializeBatchFn)(UnionNode*, RowBatch*, uint8_t**);
  /// Vector of pointers to codegen'ed MaterializeBatch functions. The vector contains one
  /// function for each child. The size of the vector should be equal to the number of
  /// children. If a child is passthrough, there should be a NULL for that child. If
  /// Codegen is disabled, there should be a NULL for every child.
  std::vector<CodegenFnPtr<UnionMaterializeBatchFn>>
      codegend_union_materialize_batch_fns_;

 private:
  /// Returns true if the child at 'child_idx' can be passed through.
  bool IsChildPassthrough(int child_idx) const {
    DCHECK_LT(child_idx, children_.size());
    return child_idx < first_materialized_child_idx_;
  }
};

/// Node that merges the results of its children by either materializing their
/// evaluated expressions into row batches or passing through (forwarding) the
/// batches if the input tuple layout is identical to the output tuple layout
/// and expressions don't need to be evaluated. The children should be ordered
/// such that all passthrough children come before the children that need
/// materialization. The union node pulls from its children sequentially, i.e.
/// it exhausts one child completely before moving on to the next one.

class UnionNode : public ExecNode {
 public:
  UnionNode(ObjectPool* pool, const UnionPlanNode& pnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 private:
  /// Descriptor for tuples this union node constructs.
  const TupleDescriptor* tuple_desc_;

  /// Index of the first non-passthrough child; i.e. a child that needs materialization.
  /// 0 when all children are materialized, 'children_.size()' when no children are
  /// materialized.
  const int first_materialized_child_idx_;

  /// Number of const scalar expressions which will be codegened.
  const int64_t num_const_scalar_expr_to_be_codegened_;

  /// Reference to UnionPlanNode::is_codegen_status_added_.
  const bool& is_codegen_status_added_;

  /// Const exprs materialized by this node. These exprs don't refer to any children.
  /// Only materialized by the first fragment instance to avoid duplication.
  const std::vector<std::vector<ScalarExpr*>>& const_exprs_lists_;
  std::vector<std::vector<ScalarExprEvaluator*>> const_expr_evals_lists_;

  /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
  const std::vector<std::vector<ScalarExpr*>>& child_exprs_lists_;
  std::vector<std::vector<ScalarExprEvaluator*>> child_expr_evals_lists_;

  /// Reference to the codegened vector containing codegened function pointer owned by the
  /// UnionPlanNode object that was used to create this instance.
  const std::vector<CodegenFnPtr<UnionPlanNode::UnionMaterializeBatchFn>>&
      codegend_union_materialize_batch_fns_;

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
  int const_exprs_lists_idx_;

  /// Index of the child that needs to be closed on the next GetNext() call. Should be set
  /// to -1 if no child needs to be closed.
  int to_close_child_idx_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// The following GetNext* functions don't apply the limit. It must be enforced by the
  /// caller.

  /// GetNext() for the passthrough case. We pass 'row_batch' directly into the GetNext()
  /// call on the child.
  Status GetNextPassThrough(RuntimeState* state, RowBatch* row_batch);

  /// GetNext() for the materialized case. Materializes and evaluates rows from each
  /// non-passthrough child.
  Status GetNextMaterialized(RuntimeState* state, RowBatch* row_batch);

  /// GetNext() for the constant expression case.
  Status GetNextConst(RuntimeState* state, RowBatch* row_batch);

  /// Evaluates exprs for the current child and materializes the results into 'tuple_buf',
  /// which is attached to 'dst_batch'. Runs until 'dst_batch' is at capacity, or all rows
  /// have been consumed from the current child batch. Updates 'child_row_idx_'.
  void MaterializeBatch(RowBatch* dst_batch, uint8_t** tuple_buf);

  /// Evaluates 'exprs' over 'row', materializes the results in 'tuple_buf'.
  /// and appends the new tuple to 'dst_batch'. Increments 'num_rows_returned_'.
  void MaterializeExprs(const std::vector<ScalarExprEvaluator*>& evaluators,
      TupleRow* row, uint8_t* tuple_buf, RowBatch* dst_batch);

  /// Returns true if the child at 'child_idx' can be passed through.
  bool IsChildPassthrough(int child_idx) const {
    DCHECK_LT(child_idx, children_.size());
    return child_idx < first_materialized_child_idx_;
  }

  /// Returns true if there are still rows to be returned from passthrough children.
  bool HasMorePassthrough() const {
    return child_idx_ < first_materialized_child_idx_;
  }

  /// Returns true if there are still rows to be returned from children that need
  /// materialization.
  bool HasMoreMaterialized() const {
    return first_materialized_child_idx_ != children_.size() &&
        child_idx_ < children_.size();
  }

  /// Returns true if there are still rows to be returned from constant expressions.
  bool HasMoreConst(const RuntimeState* state) const {
    return (state->instance_ctx().per_fragment_instance_idx == 0 || IsInSubplan()) &&
        const_exprs_lists_idx_ < const_exprs_lists_.size();
  }

};

}

#endif
