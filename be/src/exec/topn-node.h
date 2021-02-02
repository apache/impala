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
#include "runtime/sorter.h"
#include "util/tuple-row-compare.h"
#include "util/priority-queue.h"

namespace impala {

class MemPool;
class RuntimeState;
class Sorter;
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

  /// Return true if this is a partitioned Top-N.
  bool is_partitioned() const {
    return tnode_->sort_node.type == TSortType::PARTITIONED_TOPN;
  }

  /// Returns the per-partition limit.
  int64_t per_partition_limit() const {
    DCHECK(is_partitioned());
    return tnode_->sort_node.per_partition_limit;
  }

  /// Returns the per-heap capacity used for the Heap objects in this node.
  int64_t heap_capacity() const {
    int64_t limit;
    if (is_partitioned()) {
      limit = per_partition_limit();
    } else {
      // Without tie handling, the node-level limit and the heap limit are one and the
      // same, but with ties they are different because the heap capacity is not
      // a strict limit on rows.
      limit = include_ties() ? tnode_->sort_node.limit_with_ties : tnode_->limit;
    }
    return limit + offset();
  }

  bool include_ties() const {
    return tnode_->sort_node.include_ties;
  }

  /// Ordering expressions used for tuple comparison.
  std::vector<ScalarExpr*> ordering_exprs_;

  /// Partitioning expressions used for tuple comparison. Non-empty if this is a
  /// partitioned top N.
  std::vector<ScalarExpr*> partition_exprs_;

  /// Ordering expressions used for tuple comparison within partition.
  std::vector<ScalarExpr*> intra_partition_ordering_exprs_;

  /// Cached descriptor for the materialized tuple.
  TupleDescriptor* output_tuple_desc_ = nullptr;

  /// Materialization exprs for the output tuple and their evaluators.
  std::vector<ScalarExpr*> output_tuple_exprs_;

  /// Materialization exprs that materialize the output tuple into itself, i.e. are
  /// no-ops. Non-empty if this is a partitioned top N.
  std::vector<ScalarExpr*> noop_tuple_exprs_;

  /// Config used to create a TupleRowComparator instance for 'ordering_exprs_'.
  TupleRowComparatorConfig* ordering_comparator_config_ = nullptr;

  /// Config used to create a TupleRowComparator instance for 'partition_exprs_'.
  /// Non-NULL iff this is a partitioned top N.
  TupleRowComparatorConfig* partition_comparator_config_ = nullptr;

  /// Config used to create a TupleRowComparator instance for
  /// 'intra_partition_ordering_exprs_'.
  TupleRowComparatorConfig* intra_partition_comparator_config_ = nullptr;

  /// Codegened version of TopNNode::InsertBatchUnpartitioned() or
  /// InsertBatchPartitioned().
  typedef void (*InsertBatchFn)(TopNNode*, RuntimeState*, RowBatch*);
  CodegenFnPtr<InsertBatchFn> codegend_insert_batch_fn_;

  /// Codegened version of Sort::TupleSorter::SortHelper().
  CodegenFnPtr<Sorter::SortHelperFn> codegend_sort_helper_fn_;
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
/// TopNNode supports two modes: unpartitioned and partitioned. In unpartitioned mode,
/// there is a global limit to the rows returned. Whereas in partitioned mode, tuples
/// are divided into different partitions based on 'partition_cmp_' and the Top N from
/// each partition are returned.
///
/// In both unpartitioned and partitioned mode, rows are returned fully sorted according
/// to the sort order (i.e. in the partitioned case, sorted by partition then
/// intra-partition order).
///
/// Unpartitioned TopN Implementation Details
/// =========================================
/// Unpartitioned mode uses a single in-memory priority queue and does not spill results
/// to disk. Memory consumption is bounded by the limit. After the input is consumed,
/// rows can be directly outputted from the priority queue by calling
/// Heap::PrepareForOutput().
///
/// Partitioned Top-N Implementation Details
/// ========================================
/// Partitioned mode needs to support spilling to disk because the number of partitions
/// is not known ahead of time, so memory requirements are not known ahead of time. In
/// partitioned mode, a separate in-memory heap per partition is maintained, until a
/// soft memory limit is reached, in which case rows are moved to an external Sorter
/// to stay under the memory limit. Even if the operator is forced to move rows to
/// the external sorter, the in-memory heaps may still be effective at reducing the
/// input considerably.
///
/// After all the input is consumed, all rows from the in-memory heaps are moved to the
/// sorter, fully sorted by partition and intra-partition order, after which the rows
/// can be fetched in order from the sorter - see PrepareForOutput() and
/// GetNextPartitioned().
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
///
/// In partitioned mode, reclamation is triggered by a memory threshold, after which
/// some in-memory heaps are evicted and the remaining heaps reclaimed. The reclamation
/// thus serves two purposes: to support spilling-to-disk where we can't fit all
/// heaps in memory, and to reclaim unused memory.
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

  /// Return true if this is a partitioned Top-N.
  bool is_partitioned() const {
    const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
    return pnode.is_partitioned();
  }

  int64_t unpartitioned_capacity() const {
    DCHECK(!is_partitioned());
    return limit_ + offset_;
  }

  /// Returns the per-partition limit.
  int64_t per_partition_limit() const {
    DCHECK(is_partitioned());
    const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
    return pnode.per_partition_limit();
  }

  bool include_ties() const {
    const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
    return pnode.include_ties();
  }

  /// Inserts all the input rows in 'batch' into 'heap_'. Used for unpartitioned
  /// Top-N only.
  void InsertBatchUnpartitioned(RuntimeState* state, RowBatch* batch);

  /// Inserts all the input rows in 'batch' into 'partition_heaps_'. Used for partitioned
  /// Top-N only.
  void InsertBatchPartitioned(RuntimeState* state, RowBatch* batch);

  /// Evict some of the partitions from memory, putting the tuples into 'sorter_'. If
  /// 'evict_final' is true, all partitions will be evicted as part of PrepareOutput().
  /// Used for partitioned Top-N only.
  Status EvictPartitions(RuntimeState* state, bool evict_final);

  /// Select a subset of partitions to evict as a result of memory pressure. Removes
  /// the partitions from 'partition_heaps_' and returns them.
  /// Used for partitioned Top-N only.
  std::vector<std::unique_ptr<Heap>> SelectPartitionsToEvict();

  /// Implementation of GetNext() for when is_partitioned() is false.
  Status GetNextUnpartitioned(RuntimeState* state, RowBatch* row_batch, bool* eos);

  /// Implementation of GetNext() for when is_partitioned() is true.
  Status GetNextPartitioned(RuntimeState* state, RowBatch* row_batch, bool* eos);

  /// Prepare to start outputting rows. Called after consuming all rows from the child.
  /// In partitioned mode, this adds all rows to 'sorter_' and sorts them so that
  /// GetNextPartitioned() can fetch the rows in order.
  ///
  /// In unpartitioned mode, this collects all output rows in 'sorted_top_n_' and
  /// initializes 'get_next_iter_' to point to the first row.
  Status PrepareForOutput(RuntimeState* state);

  /// Re-materialize all tuples that reference 'tuple_pool_' and release 'tuple_pool_',
  /// replacing it with a new pool.
  Status ReclaimTuplePool(RuntimeState* state);

  /// Initialize 'tmp_tuple_' with memory from 'pool'.
  Status InitTmpTuple(RuntimeState* state, MemPool* pool);

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
  std::unique_ptr<TupleRowComparator> order_cmp_;

  /// Comparator for partitioning tuples between priority queue. Non-NULL iff this is a
  /// partitioned top-N operator.
  std::unique_ptr<TupleRowComparator> partition_cmp_;

  /// Comparator for partitioning tuples within a partition priority queue. Non-NULL iff
  /// this is a partitioned top-N operator.
  std::unique_ptr<TupleRowComparator> intra_partition_order_cmp_;

  /// Temporary staging vector for sorted tuples extracted from a Heap via
  /// Heap::PrepareForOutput().
  std::vector<Tuple*> sorted_top_n_;

  /// Stores everything referenced in priority_queue_.
  std::unique_ptr<MemPool> tuple_pool_;

  /// Iterator over elements in sorted_top_n_. Only used in unpartitioned Top-N.
  std::vector<Tuple*>::iterator get_next_iter_;

  /// Reference to the codegened function pointer owned by the TopNPlanNode object that
  /// was used to create this instance.
  const CodegenFnPtr<TopNPlanNode::InsertBatchFn>& codegend_insert_batch_fn_;

  /// Timer for time spent in InsertBatch*() function (or codegen'd version).
  RuntimeProfile::Counter* insert_batch_timer_;

  /// Number of rows to be reclaimed since tuple_pool_ was last created/reclaimed.
  /// Only used for unpartitioned Top-N.
  int64_t rows_to_reclaim_ = 0;

  /// Number of times tuple pool memory was reclaimed
  RuntimeProfile::Counter* tuple_pool_reclaim_counter_= nullptr;

  /// Total number of partitions. Only initialized for partitioned Top-N.
  RuntimeProfile::Counter* num_partitions_counter_ = nullptr;

  /// Number of times an in-memory heap was created.
  /// Only initialized for partitioned Top-N.
  RuntimeProfile::Counter* in_mem_heap_created_counter_ = nullptr;

  /// Number of times an in-memory heap was evicted because of memory pressure.
  /// Only initialized for partitioned Top-N.
  RuntimeProfile::Counter* in_mem_heap_evicted_counter_ = nullptr;

  /// Number of rows that the in-memory heaps filtered out.
  /// Only initialized for partitioned Top-N.
  RuntimeProfile::Counter* in_mem_heap_rows_filtered_counter_ = nullptr;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Tuple allocated once from tuple_pool_ and reused in InsertTupleRow to
  /// materialize input tuples if necessary. After materialization, tmp_tuple_ may be
  /// copied into the tuple pool and inserted into the priority queue.
  /// Also used in GetNextPartitioned() to store the last output tuple.
  Tuple* tmp_tuple_ = nullptr;

  // Single heap used as the main heap in unpartitioned Top-N. Not used for partitioned
  // Top-N.
  std::unique_ptr<Heap> heap_;

  /// Per-partition heaps used for partitioned Top-N. The map key is a tuple within the
  /// heap, and 'intra_partition_order_cmp_' is used for comparison.
  using PartitionHeapMap = std::map<const Tuple*, std::unique_ptr<Heap>,
      ComparatorWrapper<TupleRowComparator>>;
  PartitionHeapMap partition_heaps_;

  /// Number of rows skipped. Used for adhering to offset_ in unpartitioned Top-N.
  int64_t num_rows_skipped_ = 0;

  /// Sorter used for external sorting. Only used in partitioned Top-N, where tuples are
  /// sorted by the partition exprs, then the intra-partition ordering exprs.
  /// Initialized in Prepare().
  std::unique_ptr<Sorter> sorter_;

  /// Temporary batch used for processing output from sorter in GetNextPartitioned().
  /// Used only for partitioned Top-N.
  std::unique_ptr<RowBatch> sort_out_batch_;

  /// Position in 'sort_out_batch_'. Used only for partitioned Top-N.
  int64_t sort_out_batch_pos_ = 0;

  /// Number of rows returned from the current partition in GetNextPartitioned().
  /// Used only for partitioned Top-N.
  int64_t num_rows_returned_from_partition_ = 0;

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
  /// Returns the number of rows to be reclaimed.
  int IR_ALWAYS_INLINE InsertTupleRow(
      TopNNode* node, TupleRow* input_row) WARN_UNUSED_RESULT;

  /// Insert a tuple row into the priority queue, similar to InsertTupleRow(), except
  /// 'materialized_row' is already materialized into the output row format, i.e.
  /// output_tuple_desc_. Always inlined in IR into TopNNode::InsertBatchPartitioned()
  /// because codegen relies on this for substituting exprs in the body of TopNNode.
  void IR_ALWAYS_INLINE InsertMaterializedTuple(
      TopNNode* node, Tuple* materialized_tuple);

  /// Copy the elements in the priority queue into a new tuple pool, and release
  /// the previous pool.
  Status RematerializeTuples(TopNNode* node, RuntimeState* state, MemPool* new_pool);

  /// Put the tuples in the priority queue into 'sorted_top_n' in the correct order
  /// for output.
  void PrepareForOutput(
      const TopNNode& RESTRICT node, std::vector<Tuple*>* sorted_top_n) RESTRICT;

  /// Reset stats that are collected about the heap. Called during eviction process in
  /// partitioned top-N.
  void ResetStats(const TopNNode& RESTRICT node);

  /// Can be called to invoke DCHECKs if the heap is in an inconsistent state.
  /// Returns a bool so it can be wrapped in a DCHECK() macro.
  bool DCheckConsistency();

  /// Returns number of tuples currently in heap.
  int64_t num_tuples() const { return priority_queue_.Size() + overflowed_ties_.size(); }

  int64_t num_tuples_discarded() const { return num_tuples_discarded_; }
  int64_t num_tuples_added_since_eviction() const {
    return num_tuples() - num_tuples_at_last_eviction_;
  }

  IR_NO_INLINE int64_t heap_capacity() const noexcept { return capacity_; }

  IR_NO_INLINE bool include_ties() const noexcept { return include_ties_; }

  /// Returns the first element in the priority queue. Should only be called if
  /// num_tuples() > 0.
  const Tuple* top() {
    DCHECK(!priority_queue_.Empty());
    return priority_queue_.Top();
  }

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
      const TupleRowComparator& cmp, TopNNode* node, Tuple* materialized_tuple);

  /// Limit on capacity of 'priority_queue_'. If inserting a tuple into the queue
  /// would exceed this, a tuple is popped off the queue.
  const int64_t capacity_;

  /// If true, the heap may include more than 'capacity_' if multiple tuples are
  /// tied to be the head of the heap.
  const bool include_ties_;

  /// Number of tuples discarded as a result of this heap hitting its capacity and
  /// filtering out tuples. Only updated for the partitioned Top-N.
  int64_t num_tuples_discarded_ = 0;

  /// Number of tuples in the heap at the time of last eviction. Only updated for the
  /// partitioned Top-N.
  int64_t num_tuples_at_last_eviction_ = 0;

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
