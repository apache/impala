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

#ifndef IMPALA_EXEC_UNNEST_NODE_H_
#define IMPALA_EXEC_UNNEST_NODE_H_

#include "exec/exec-node.h"
#include "exprs/scalar-expr.h"
#include "runtime/collection-value.h"

namespace impala {

class TupleDescriptor;

class UnnestPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  /// Initializes the expressions that produce the collections to be unnested.
  /// Called by the containing subplan plan-node.
  Status InitCollExprs(FragmentState* state);

  ~UnnestPlanNode(){}

  /// Expressions that produce the collections to be unnested. They are always SlotRefs
  /// into collection-typed slots. We do not evaluate these expressions for setting
  /// 'UnnestNode::coll_values_', but instead manually retrieve the slot values to support
  /// projection (see class comment in UnnestNode).
  std::vector<ScalarExpr*> collection_exprs_;

  /// Descriptors of the collection-typed slots handled by this UnnestPlanNode. Set in
  /// InitCollExpr().
  std::vector<SlotDescriptor*> coll_slot_descs_;

  /// Tuple indexes corresponding to 'coll_slot_descs_'. Set in InitCollExpr().
  std::vector<int> coll_tuple_idxs_;
};

/// Exec node that scans one or more in-memory collections of tuples (CollectionValues).
/// The output row is composed of as many tuples as the number of collections this unnest
/// handles - the collections' item tuples.
/// Produces as many output rows as the size of the longest collection in this unnest and
/// performs a zipping unnest on the collections. If the lenght of the collections is not
/// the same than the missing values from the shorter collections will be null tuples.
///
/// Example:
/// The collections handled by this unnest: coll1: {1,2,3}, coll2: {11}, coll3: {}
/// The output of the unnest:
/// +=======================+
/// | coll1 | coll2 | coll3 |
/// |-----------------------|
/// | 1     | 11    | null  |
/// | 2     | null  | null  |
/// | 3     | null  | null  |
/// +=======================+
///
/// An UnnestNode does not have children and can only appear in the right child of a
/// SubplanNode. The UnnestNode gets its 'input' from its containing SubplanNode.
///
/// Projection: Collection-typed slots are expensive to copy, e.g., during data exchanges
/// or when writing into a buffered-tuple-stream. Such slots are often duplicated many
/// times after unnesting in a SubplanNode. To alleviate this problem, we set the
/// collection-typed slot to be unnested in this node to NULL immediately after retrieving
/// the slot's value. Since the same tuple/slot could be referenced by multiple input
/// rows, we ignore the null bit when retrieving a slot's value because this node itself
/// might have set the bit in a prior Open()/GetNext()*/Reset() cycle.  We rely on the
/// producer of the slot value (scan node) to write an empty collection value into slots
/// that are NULL, in addition to setting the null bit. This breaks/augments the existing
/// semantics of the null bits. Setting the slot to NULL as early as possible ensures
/// that all rows returned by the containing SubplanNode will have the slot set to NULL.
/// The FE guarantees that the contents of any collection-typed slot are never referenced
/// outside of a single UnnestNode, so setting such a slot to NULL is safe after the
/// UnnestNode has retrieved the collection value from the corresponding slot.
///
/// TODO: Setting the collection-typed slots to NULL should be replaced by a proper
/// projection at materialization points. The current solution purposely ignores the
/// conventional NULL semantics of slots - it is a temporary hack which must be removed.

class UnnestNode : public ExecNode {
 public:
  UnnestNode(ObjectPool* pool, const UnnestPlanNode& pnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 private:
  friend class SubplanNode;

  /// Gets a slot descriptor that is expected to refer to a collection and then returns
  /// the tuple index from the output row's row descriptor to indicate where the values
  /// of the given collection belong.
  int GetCollTupleIdx(const SlotDescriptor* slot_desc) const;

  /// Gets the index of a collection and creates a null tuple using mem pool from
  /// 'row_batch' for this collection. Used for filling null values when this UnnestNode
  /// is handling multiple collections for zipping unnest and one of the collections is
  /// shorter then the others.
  /// Returns nullptr if the collection doesn't have an underlying slot, e.g. when not
  /// referenced in the query only for unnesting.
  /// E.g.: SELECT id FROM complextypes_arrays t, t.arr1 where ID = 10;
  Tuple* CreateNullTuple(int coll_idx, RowBatch* row_batch) const;

  static const CollectionValue EMPTY_COLLECTION_VALUE;

  /// Sizes of collection item tuples in bytes. Set in Prepare().
  std::vector<int> item_byte_sizes_;

  /// Descriptors of the collection-typed slots. These slots are always set to NULL in
  /// Open() as a simple projection.
  const std::vector<SlotDescriptor*>* coll_slot_descs_;

  /// Tuple indexes corresponding to 'coll_slot_descs_'. Note, these are tuple indexes in
  /// the source node.
  const std::vector<int>* input_coll_tuple_idxs_;

  /// Tuple indexes corresponding to 'coll_slot_descs_' in the output tuple.
  std::vector<int> output_coll_tuple_idxs_;

  /// The current collection values to be unnested. Set using 'coll_slot_descs_' in
  /// Open().
  std::vector<const CollectionValue*> coll_values_;

  /// Current item index.
  int item_idx_;

  /// Stores the length of the longest collection in 'coll_values_'. Set in Open().
  int64_t longest_collection_size_;

  /// Stats for runtime profile
  int64_t num_collections_;
  int64_t total_collection_size_;
  int64_t max_collection_size_;
  int64_t min_collection_size_;
  /// TODO: replace with stats or histogram counter
  RuntimeProfile::Counter* avg_collection_size_counter_;
  RuntimeProfile::Counter* max_collection_size_counter_;
  RuntimeProfile::Counter* min_collection_size_counter_;
  /// This can be determined by looking at the input cardinality to the subplan node, but
  /// it's handy to have it here too.
  RuntimeProfile::Counter* num_collections_counter_;
};

}

#endif
