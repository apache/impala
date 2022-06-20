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

#ifndef IMPALA_EXEC_SORT_NODE_H
#define IMPALA_EXEC_SORT_NODE_H

#include "exec/exec-node.h"
#include "runtime/sorter.h"

namespace impala {

class SortPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  virtual void Codegen(FragmentState* state) override;

  ~SortPlanNode(){}

  /// Expressions used for tuple comparison.
  std::vector<ScalarExpr*> ordering_exprs_;

  /// Expressions used to materialize slots in the tuples to be sorted.
  /// One expr per slot in the materialized tuple.
  std::vector<ScalarExpr*> sort_tuple_slot_exprs_;

  /// Config used to create a TupleRowComparator instance.
  TupleRowComparatorConfig* row_comparator_config_ = nullptr;

  /// Number of backend nodes executing the same sort node plan.
  int32_t num_backends_;

  /// Codegened version of Sorter::TupleSorter::SortHelper().
  CodegenFnPtr<Sorter::SortHelperFn> codegend_sort_helper_fn_;

  /// Codegened version of SortedRunMerger::HeapifyHelper().
  CodegenFnPtr<SortedRunMerger::HeapifyHelperFn> codegend_heapify_helper_fn_;
};

/// Node that implements a full sort of its input with a fixed memory budget, spilling
/// to disk if the input is larger than available memory.
/// Uses Sorter for the external sort implementation.
/// Input rows to SortNode are materialized by the Sorter into a single tuple
/// using the expressions specified in sort_tuple_exprs_.
/// In GetNext(), SortNode passes in the output batch to the sorter instance created
/// in Open() to fill it with sorted rows.
/// If a merge phase was performed in the sort, sorted rows are deep copied into
/// the output batch. Otherwise, the sorter instance owns the sorted data.

class SortNode : public ExecNode {
 public:
  SortNode(ObjectPool* pool, const SortPlanNode& pnode, const DescriptorTbl& descs);
  ~SortNode();

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  /// Fetch input rows and feed them to the sorter until the input is exhausted.
  Status SortInput(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Compute estimated bytes of input that will go into the sort node.
  /// Return -1 if estimate can not be determined.
  int64_t ComputeInputSizeEstimate();

  /// Number of rows to skip.
  int64_t offset_;

  /// Expressions used to materialize slots in the tuples to be sorted.
  /// One expr per slot in the materialized tuple.
  const std::vector<ScalarExpr*>& sort_tuple_exprs_;

  const TupleRowComparatorConfig& tuple_row_comparator_config_;

  /// Whether the previous call to GetNext() returned a buffer attached to the RowBatch.
  /// Used to avoid unnecessary calls to ReleaseUnusedReservation().
  bool returned_buffer_ = false;

  /// Min, max, and avg time spent in AddBatch
  RuntimeProfile::SummaryStatsCounter* add_batch_timer_;

  /// Number of backend nodes executing the same sort node plan.
  int32_t num_backends_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Object used for external sorting.
  boost::scoped_ptr<Sorter> sorter_;

  /// Keeps track of the number of rows skipped for handling offset_.
  int64_t num_rows_skipped_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};

}

#endif
