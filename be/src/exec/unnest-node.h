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

#ifndef IMPALA_EXEC_UNNEST_NODE_H_
#define IMPALA_EXEC_UNNEST_NODE_H_

#include "exec/exec-node.h"
#include "exprs/expr.h"

namespace impala {

class TupleDescriptor;

/// Exec node that scans an in-memory collection of tuples (an ArrayType) producing
/// one output row per tuple in the collection. The output row is composed of a single
/// tuple - the collection's item tuple.
/// An UnnestNode does not have children and can only appear in the right child of a
/// SubplanNode. The UnnestNode gets its 'input' from its containing SubplanNode.
///
/// Projection: Collection-typed slots are expensive to copy, e.g., during data exchanges
/// or when writing into a buffered-tuple-stream. Such slots are often duplicated many
/// times after unnesting in a SubplanNode. To alleviate this problem, we set the
/// collection-typed slot to be unnested in this node to NULL immediately after
/// evaluating its SlotRef. Setting the slot to NULL as early as possible ensures that
/// all rows returned by the containing SubplanNode will have the slot set to NULL.
/// The FE guarantees that the contents of any collection-typed slot are never referenced
/// outside of a single UnnestNode, so setting such a slot to NULL is safe after the
/// UnnestNode has retrieved the array value from the corresponding slot.
/// TODO: Setting the collection-typed slots to NULL should be replaced by a proper
/// projection at materialization points.
class UnnestNode : public ExecNode {
 public:
  UnnestNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 private:
  friend class SubplanNode;

  /// Size of an array item tuple in bytes. Set in Prepare().
  int item_byte_size_;

  /// Expr that produces the ArrayVal to be unnested. The expr is evaluated against the
  /// current row of the containing subplan node, and is currently always a SlotRef into
  /// an array-typed slot.
  ExprContext* array_expr_ctx_;

  /// Descriptor of the array-typed slot referenced by array_expr_ctx_. Set in Prepare().
  /// This slot is set to NULL in Open() immediately after evaluating array_expr_ctx_.
  const SlotDescriptor* array_slot_desc_;

  /// Tuple index corresponding to array_slot_desc_. Set in Prepare().
  int array_tuple_idx_;

  /// Current evaluation of array_expr_ctx_. Set in Open().
  ArrayVal array_val_;

  /// Current item index.
  int item_idx_;

  // Stats for runtime profile
  int64_t num_collections_;
  int64_t total_collection_size_;
  int64_t max_collection_size_;
  int64_t min_collection_size_;
  // TODO: replace with stats or histogram counter
  RuntimeProfile::Counter* avg_collection_size_counter_;
  RuntimeProfile::Counter* max_collection_size_counter_;
  RuntimeProfile::Counter* min_collection_size_counter_;
  // This can be determined by looking at the input cardinality to the subplan node, but
  // it's handy to have it here too.
  RuntimeProfile::Counter* num_collections_counter_;
};

}

#endif
