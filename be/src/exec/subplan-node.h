// Copyright 2015 Cloudera Inc.
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

#ifndef IMPALA_EXEC_SUBPLAN_NODE_H_
#define IMPALA_EXEC_SUBPLAN_NODE_H_

#include "exec/exec-node.h"
#include "exprs/expr.h"

namespace impala {

class TupleRow;

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
///
/// Poor man's projection: Collection-typed slots are expensive to copy, e.g., during
/// data exchanges or when writing into a buffered-tuple-stream. Such slots are often
/// duplicated many times after unnesting in a SubplanNode. To alleviate this problem,
/// we set all collection-typed slots in the input row that are unnested inside the
/// second child of this SubplanNde to NULL. The FE guarantees that the contents of any
/// collection-typed slot are never referenced outside of a single UnnestNode, so setting
/// those slots to NULL is safe after the unnesting has been performed.
/// TODO: Setting the collection-typed slots to NULL should be replaced by a proper
/// projection at materialization points.
class SubplanNode : public ExecNode {
 public:
  SubplanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 private:
  friend class SingularRowSrcNode;
  friend class UnnestNode;

  /// Sets 'ancestor' as the containing Subplan in all exec nodes inside the exec-node
  /// tree rooted at 'node'. Does not traverse the second child of SubplanNodes
  /// within 'node'. Populates unnested_array_slots_ during the traversal.
  void SetContainingSubplan(SubplanNode* ancestor, ExecNode* node, RuntimeState* state);

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

  /// List of collection-typed slots in the input row from the first child that are
  /// unnested in this subplan. These slots are set to NULL in the current_input_row_
  /// after we are done with the subplan iteration for it (second child is at eos).
  /// Populated in SetContainingSubplan() which is called in Prepare();
  std::vector<const SlotDescriptor*> unnested_array_slots_;

  /// Tuple indexes corresponding to the slots in unnested_array_slots_. Set in Prepare().
  std::vector<int> unnested_array_tuple_idxs_;

  /// Indicates whether the subplan (second child) is open.
  bool subplan_is_open_;

  /// Saved from the last call to GetNext() on our second child.
  bool subplan_eos_;
};

}

#endif
