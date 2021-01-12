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

#pragma once

#include <memory>
#include <queue>

#include "codegen/codegen-fn-ptr.h"
#include "codegen/impala-ir.h"
#include "exec/exec-node.h"
#include "runtime/descriptors.h"  // for TupleId
#include "util/tuple-row-compare.h"
#include "util/priority-queue.h"

namespace impala {

class MemPool;
class RuntimeState;
class TopNNode;
class Tuple;

class TopNPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  virtual void Codegen(FragmentState* state) override;

  int64_t offset() const {
    return tnode_->sort_node.__isset.offset ? tnode_->sort_node.offset : 0;
  }
  ~TopNPlanNode(){}

  /// Returns the per-heap capacity used for the Heap objects in this node.
  int64_t heap_capacity() const {
    int64_t limit = include_ties() ? tnode_->sort_node.limit_with_ties : tnode_->limit;
    return limit + offset();
  }

  bool include_ties() const {
    return tnode_->sort_node.include_ties;
  }

  /// Ordering expressions used for tuple comparison.
  std::vector<ScalarExpr*> ordering_exprs_;

  /// Cached descriptor for the materialized tuple.
  TupleDescriptor* output_tuple_desc_ = nullptr;

  /// Materialization exprs for the output tuple and their evaluators.
  std::vector<ScalarExpr*> output_tuple_exprs_;

  /// Config used to create a TupleRowComparator instance for 'ordering_exprs_'.
  TupleRowComparatorConfig* row_comparator_config_ = nullptr;

  /// Codegened version of TopNNode::InsertBatch().
  typedef void (*InsertBatchFn)(TopNNode*, RowBatch*);
  CodegenFnPtr<InsertBatchFn> codegend_insert_batch_fn_;
};

/// Node for in-memory TopN operator that sorts input tuples and applies a limit such
/// that only the Top N tuples according to the sort order are returned by the operator.
/// This node materializes its input rows into a new row format comprised of a single
/// tuple using the output_tuple_exprs_.
///
/// In-memory priority queues, represented as binary heaps, are used to compute the Top N
/// efficiently. Maintaining in-memory heaps with the current top rows allows discarding
/// rows that are not in the top N as soon as possible, minimizing the memory requirements
/// and processing time of the operator.
///
/// TODO: currently we only support a single top-n heap per operator. IMPALA-9979 will
/// add a partitioned mode.
///
/// Unpartitioned TopN Implementation Details
/// =========================================
/// Unpartitioned mode uses a single in-memory priority queue and does not spill results
/// to disk. Memory consumption is bounded by the limit. After the input is consumed,
/// rows can be directly outputted from the priority queue by calling
/// Heap::PrepareForOutput().
///
/// Memory Management
/// =================
/// In-memory heaps are backed by 'tuple_pool_' - all tuples in the heaps must reference
/// only memory allocated from this pool. To reclaim memory from tuples that have been
/// evicted from the heaps, the in-memory heaps must be re-materialized with a new
/// MemPool - see ReclaimTuplePool(). In some cases the fixed-length portion of a
/// tuple can be reused to avoid the need to reclaim all the time.
///
/// In unpartitioned mode, reclamation is triggered by 'rows_to_reclaim_' hitting a
/// threshold, which indicates that enough unused memory may have accumulated to
/// be worth reclaiming.
class TopNNode : public ExecNode {
 public:
  TopNNode(ObjectPool* pool, const TopNPlanNode& pnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  class Heap;

  friend class TupleLessThan;

  bool include_ties() const {
    const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
    return pnode.include_ties();
  }

  /// Inserts all the input rows in 'batch' into 'heap_'.
  void InsertBatch(RowBatch* batch);

  /// Prepare to start outputting rows. Called after consuming all rows from the child.
  /// Collects all output rows in 'sorted_top_n_' and initializes 'get_next_iter_' to
  /// point to the first row.
  void PrepareForOutput();

  /// Re-materialize all tuples that reference 'tuple_pool_' and release 'tuple_pool_',
  /// replacing it with a new pool.
  Status ReclaimTuplePool(RuntimeState* state);

  IR_NO_INLINE int tuple_byte_size() const noexcept {
    return output_tuple_desc_->byte_size();
  }

  /// Number of rows to skip.
  int64_t offset_;

  /// Materialization exprs for the output tuple and their evaluators.
  const std::vector<ScalarExpr*>& output_tuple_exprs_;
  std::vector<ScalarExprEvaluator*> output_tuple_expr_evals_;

  /// Cached descriptor for the materialized tuple.
  TupleDescriptor* const output_tuple_desc_;

  /// Comparator for ordering tuples globally.
  std::unique_ptr<TupleRowComparator> tuple_row_less_than_;

  /// Temporary staging vector for sorted tuples extracted from a Heap via
  /// Heap::PrepareForOutput().
  std::vector<Tuple*> sorted_top_n_;

  /// Stores everything referenced in priority_queue_.
  std::unique_ptr<MemPool> tuple_pool_;

  /// Iterator over elements in sorted_top_n_.
  std::vector<Tuple*>::iterator get_next_iter_;

  /// Reference to the codegened function pointer owned by the TopNPlanNode object that
  /// was used to create this instance.
  const CodegenFnPtr<TopNPlanNode::InsertBatchFn>& codegend_insert_batch_fn_;

  /// Timer for time spent in InsertBatch() function (or codegen'd version).
  RuntimeProfile::Counter* insert_batch_timer_;

  /// Number of rows to be reclaimed since tuple_pool_ was last created/reclaimed.
  int64_t rows_to_reclaim_;

  /// Number of times tuple pool memory was reclaimed
  RuntimeProfile::Counter* tuple_pool_reclaim_counter_= nullptr;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Tuple allocated once from tuple_pool_ and reused in InsertTupleRow to
  /// materialize input tuples if necessary. After materialization, tmp_tuple_ may be
  /// copied into the tuple pool and inserted into the priority queue.
  Tuple* tmp_tuple_ = nullptr;

  // Single heap used as the main heap in unpartitioned Top-N.
  std::unique_ptr<Heap> heap_;

  /// Number of rows skipped. Used for adhering to offset_ in unpartitioned Top-N.
  int64_t num_rows_skipped_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};

/// This is the main data structure used for in-memory Top-N: a binary heap containing
/// up to 'capacity' tuples.
class TopNNode::Heap {
 public:
  Heap(const TupleRowComparator& c, int64_t capacity, bool include_ties);

  void Reset();
  void Close();

  /// Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep
  /// copy of 'tuple_row', which it stores in 'tuple_pool'. Always inlined in IR into
  /// TopNNode::InsertBatch() because codegen relies on this for substituting exprs
  /// in the body of TopNNode.
  /// Returns the number of materialized tuples discarded that may need to be reclaimed.
  int IR_ALWAYS_INLINE InsertTupleRow(TopNNode* node, TupleRow* input_row);

  /// Copy the elements in the priority queue into a new tuple pool, and release
  /// the previous pool.
  Status RematerializeTuples(TopNNode* node, RuntimeState* state, MemPool* new_pool);

  /// Put the tuples in the priority queue into 'sorted_top_n' in the correct order
  /// for output.
  void PrepareForOutput(
      const TopNNode& RESTRICT node, std::vector<Tuple*>* sorted_top_n) RESTRICT;

  /// Can be called to invoke DCHECKs if the heap is in an inconsistent state.
  /// Returns a bool so it can be wrapped in a DCHECK() macro.
  bool DCheckConsistency();

  /// Returns number of tuples currently in heap.
  int64_t num_tuples() const { return priority_queue_.Size() + overflowed_ties_.size(); }

  IR_NO_INLINE int64_t heap_capacity() const noexcept { return capacity_; }

  IR_NO_INLINE bool include_ties() const noexcept { return include_ties_; }

private:
  /// Helper for RematerializeTuples() that materializes the tuples in a container in the
  /// range (begin_it, end_it].
  template <class T>
  Status RematerializeTuplesHelper(TopNNode* node, RuntimeState* state,
      MemPool* new_pool, T begin_it, T end_it);

  /// Helper to insert tuple row into a full priority queue with tie handling. This
  /// should not be called until the heap is at capacity and tie handling is needed.
  /// 'materialized_tuple' must be materialized into the output row format, i.e.
  /// output_tuple_desc_. Returns the number of materialized tuples discarded as a result
  /// of this function.
  /// Always inlined in IR because codegen relies on this for substituting exprs in the
  /// body of the function.
  int IR_ALWAYS_INLINE InsertTupleWithTieHandling(
      TopNNode* node, Tuple* materialized_tuple);


  /// Limit on capacity of 'priority_queue_'. If inserting a tuple into the queue
  /// would exceed this, a tuple is popped off the queue.
  const int64_t capacity_;

  /// If true, the heap may include more than 'capacity_' if multiple tuples are
  /// tied to be the head of the heap.
  const bool include_ties_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// The priority queue is represented by a vector and modified using push_heap()/
  /// pop_heap() to maintain ordered heap invariants. It has up to 'capacity_' elements
  /// in the heap.  The order of the queue is the opposite of what the ORDER BY clause
  /// specifies, such that the head of the queue (i.e. index 0) is the last sorted
  /// element.
  PriorityQueue<Tuple*, TupleRowComparator> priority_queue_;

  /// Tuples tied with the head of the priority queue in excess of the heap capacity.
  /// Only used when 'include_ties_' is true.
  std::vector<Tuple*> overflowed_ties_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};

}; // namespace impala
