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


#ifndef IMPALA_EXEC_TOPN_NODE_H
#define IMPALA_EXEC_TOPN_NODE_H

#include <queue>
#include <boost/scoped_ptr.hpp>

#include "codegen/impala-ir.h"
#include "exec/exec-node.h"
#include "runtime/descriptors.h"  // for TupleId
#include "util/tuple-row-compare.h"

namespace impala {

class MemPool;
class RuntimeState;
class Tuple;

/// Node for in-memory TopN (ORDER BY ... LIMIT)
/// This handles the case where the result fits in memory.
/// This node will materialize its input rows into a new tuple using the expressions
/// in sort_tuple_slot_exprs_ in its sort_exec_exprs_ member.
/// TopN is implemented by storing rows in a priority queue.
class TopNNode : public ExecNode {
 public:
  TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual void Codegen(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:

  friend class TupleLessThan;

  /// Inserts all the rows in 'batch' into the queue.
  void InsertBatch(RowBatch* batch);

  /// Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep
  /// copy of tuple_row, which it stores in tuple_pool_.
  void IR_ALWAYS_INLINE InsertTupleRow(TupleRow* tuple_row);

  /// Flatten and reverse the priority queue.
  void PrepareForOutput();

  // Re-materialize the elements in the priority queue into a new tuple pool, and release
  // the previous pool.
  Status ReclaimTuplePool(RuntimeState* state);

  /// Helper methods for modifying priority_queue while maintaining ordered heap
  /// invariants
  inline static void PushHeap(std::vector<Tuple*>* priority_queue,
      const ComparatorWrapper<TupleRowComparator>& comparator, Tuple* const insert_row) {
    priority_queue->push_back(insert_row);
    std::push_heap(priority_queue->begin(), priority_queue->end(), comparator);
  }

  inline static void PopHeap(std::vector<Tuple*>* priority_queue,
      const ComparatorWrapper<TupleRowComparator>& comparator) {
    std::pop_heap(priority_queue->begin(), priority_queue->end(), comparator);
    priority_queue->pop_back();
  }

  /// Number of rows to skip.
  int64_t offset_;

  /// Ordering expressions used for tuple comparison.
  std::vector<ScalarExpr*> ordering_exprs_;

  /// Materialization exprs for the output tuple and their evaluators.
  std::vector<ScalarExpr*> output_tuple_exprs_;
  std::vector<ScalarExprEvaluator*> output_tuple_expr_evals_;

  std::vector<bool> is_asc_order_;
  std::vector<bool> nulls_first_;

  /// Cached descriptor for the materialized tuple. Assigned in Prepare().
  TupleDescriptor* output_tuple_desc_;

  /// Comparator for priority_queue_.
  boost::scoped_ptr<TupleRowComparator> tuple_row_less_than_;

  /// After computing the TopN in the priority_queue, pop them and put them in this vector
  std::vector<Tuple*> sorted_top_n_;

  /// Tuple allocated once from tuple_pool_ and reused in InsertTupleRow to
  /// materialize input tuples if necessary. After materialization, tmp_tuple_ may be
  /// copied into the tuple pool and inserted into the priority queue.
  Tuple* tmp_tuple_;

  /// Stores everything referenced in priority_queue_.
  boost::scoped_ptr<MemPool> tuple_pool_;

  /// Iterator over elements in sorted_top_n_.
  std::vector<Tuple*>::iterator get_next_iter_;

  typedef void (*InsertBatchFn)(TopNNode*, RowBatch*);
  InsertBatchFn codegend_insert_batch_fn_;

  /// Timer for time spent in InsertBatch() function (or codegen'd version)
  RuntimeProfile::Counter* insert_batch_timer_;

  /// Number of rows to be reclaimed since tuple_pool_ was last created/reclaimed
  int64_t rows_to_reclaim_;

  /// Number of times tuple pool memory was reclaimed
  RuntimeProfile::Counter* tuple_pool_reclaim_counter_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Number of rows skipped. Used for adhering to offset_.
  int64_t num_rows_skipped_;

  /// The priority queue (represented by a vector and modified using
  /// push_heap()/pop_heap() to maintain ordered heap invariants) will never have more
  /// elements in it than the LIMIT + OFFSET. The order of the queue is the opposite of
  /// what the ORDER BY clause specifies, such that the top of the queue is the last
  /// sorted element.
  std::vector<Tuple*> priority_queue_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};

};

#endif
