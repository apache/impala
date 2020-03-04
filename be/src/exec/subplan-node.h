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

#ifndef IMPALA_EXEC_SUBPLAN_NODE_H_
#define IMPALA_EXEC_SUBPLAN_NODE_H_

#include "exec/exec-node.h"
#include "exprs/scalar-expr.h"

namespace impala {

class TupleRow;

class SubplanPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;

  /// Sets 'ancestor' as the containing Subplan in all plan nodes inside the plan-node
  /// tree rooted at 'node' and does any initialization that is required as a result of
  /// setting the subplan. Doesn't traverse the second child of SubplanPlanNodes within
  /// 'node'.
  Status SetContainingSubplan(
      FragmentState* state, SubplanPlanNode* ancestor, PlanNode* node);

  ~SubplanPlanNode(){}
};

/// For every input row from its first child, a SubplanNode evaluates and pulls all
/// results from its second child, resetting the second child after every input row.
/// A SubplanNode does not create any output rows itself. It merely
/// 'forwards' the output rows produced by its second child, accumulating them
/// into batches before returning them to its parent in GetNext(). Therefore, it
/// may not have any conjuncts, but it may have a limit.
/// A SubplanNode exposes the current input row it is processing to UnnestNodes and
/// SingularRowSrcNodes that are in the exec-node tree of the second child.
///
/// Subplan Memory Management:
/// To ensure that the memory backing rows produced by the subplan tree (second child)
/// remains valid for the lifetime of an output batch produced in GetNext(), we rely on
/// our conventional transfer mechanism. That is, the ownership of memory that is no
/// longer used by an exec node inside the subplan tree (second child) is transferred to
/// an output row batch in that node's GetNext() at a "convenient" point, typically
/// at eos or when the memory usage exceeds some threshold. Note that exec nodes may
/// choose not to transfer memory at eos to amortize the cost of memory allocation over
/// multiple Reset()/Open()/GetNext()* cycles.
/// The resources owned by batches from the first child of this node are always
/// transferred to the output batch right before fetching a new batch from the
/// first child.

class SubplanNode : public ExecNode {
 public:
  SubplanNode(ObjectPool* pool, const SubplanPlanNode& pnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 private:
  friend class SingularRowSrcNode;
  friend class UnnestNode;

  /// Sets 'ancestor' as the containing Subplan in all exec nodes inside the exec-node
  /// tree rooted at 'node'. Doesn't traverse the second child of SubplanNodes within
  /// 'node'. Should be called in Prepare before calling ExecNode's Prepare so that all
  /// the children nodes have this set before prepare is called on them.
  void SetContainingSubplan(SubplanNode* ancestor, ExecNode* node);

  /// Returns the current row from child(0) or NULL if no rows from child(0) have been
  /// retrieved yet (GetNext() has not yet been called). This function is called by
  /// singular-row-src and unnest nodes while evaluating child(1).
  TupleRow* current_row() const { return current_input_row_; }

  /// Current row batch used to get rows from our first child.
  boost::scoped_ptr<RowBatch> input_batch_;

  /// Saved from the last call to GetNext() on our first child.
  bool input_eos_;

  /// Index of current row in input_batch_.
  int input_row_idx_;

  /// Current row from the first child that dependent nodes in the subplan (unnests and
  /// nested row sources) will pick up.
  TupleRow* current_input_row_;

  /// Indicates whether the subplan (second child) is open.
  bool subplan_is_open_;

  /// Saved from the last call to GetNext() on our second child.
  bool subplan_eos_;
};

}

#endif
