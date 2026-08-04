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

#include <string>

#include "codegen/codegen-fn-ptr.h"
#include "exec/exec-node.h"
#include "exec/filter-context.h"

namespace impala {

class CTEConsumerNode;
class LocalExchanger;

class CTEConsumerPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  virtual void Codegen(FragmentState* state) override;

  TupleDescriptor* tuple_desc_;
  std::vector<ScalarExpr*> input_exprs_;

  /// Runtime filter expressions, one per filter in tnode.runtime_filters.
  std::vector<ScalarExpr*> runtime_filter_exprs_;

  typedef void (*MaterializeBatchFn)(CTEConsumerNode*, RowBatch*, RowBatch*, uint8_t**);
  /// Vector of pointers to codegen'ed MaterializeBatch functions. The vector contains one
  /// function for each child. The size of the vector should be equal to the number of
  /// children. If a child is passthrough, there should be a NULL for that child. If
  /// Codegen is disabled, there should be a NULL for every child.
  CodegenFnPtr<MaterializeBatchFn> codegend_materialize_batch_fn_;

  bool is_passthrough_;
};

/// Node that scans results of a Common Table Expression from a LocalExchanger
/// produced by a CTEProducerNode.
class CTEConsumerNode : public ExecNode {
 public:
  CTEConsumerNode(
      ObjectPool* pool, const CTEConsumerPlanNode& pnode, const DescriptorTbl& descs);

  Status Prepare(RuntimeState* state) override;
  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;
  void DebugString(int indentation_level, std::stringstream* out) const override;

  /// Evaluates exprs for the input batch and materializes the results into 'tuple_buf',
  /// which is attached to 'dst_batch'. Runs until 'dst_batch' is at capacity, or all rows
  /// have been consumed from 'input_batch'.
  void MaterializeBatch(RowBatch* input_batch, RowBatch* dst_batch, uint8_t** tuple_buf);

  /// Evaluates 'exprs' over 'row', materializes the results in 'tuple_buf'.
  /// and appends the new tuple to 'dst_batch'. Increments 'num_rows_returned_'.
  void MaterializeExprs(const std::vector<ScalarExprEvaluator*>& evaluators,
      TupleRow* row, uint8_t* tuple_buf, RowBatch* dst_batch);

  /// A list of Filter IDs that were effective (rejected rows) at this scan node.
  const std::vector<int32_t>& effective_filter_ids() const {
    return effective_filter_ids_;
  }

 private:
  std::string name_;
  LocalExchanger* exchanger_ = nullptr;
  const TupleDescriptor* tuple_desc_;
  const std::vector<ScalarExpr*>& input_exprs_;
  std::vector<ScalarExprEvaluator*> input_expr_evals_;
  const CodegenFnPtr<CTEConsumerPlanNode::MaterializeBatchFn>&
      codegend_materialize_batch_fn_;
  int32_t consumer_index_;
  bool is_passthrough_;

  /// Runtime filter contexts, one per filter assigned to this node.
  std::vector<FilterContext> filter_ctxs_;

  /// Filter IDs that were effective (rejected rows) at this scan node.
  /// Populated in Close().
  std::vector<int32_t> effective_filter_ids_;

  struct LocalFilterStats {
    int64_t total_possible = 0;
    int64_t considered = 0;
    int64_t rejected = 0;
    bool enabled_for_row = true;
  };

  /// Track cumulative statistics of each filter locally to determine effectiveness.
  std::vector<LocalFilterStats> filter_stats_;

  struct LocalFilterContext {
    LocalFilterContext(const FilterContext& ctx, LocalFilterStats& local_stats)
      : filter_ctx(ctx), stats(local_stats) {}
    const FilterContext& filter_ctx;
    LocalFilterStats& stats;
  };

  /// Returns true if 'row' passes all runtime filters, false if any filter rejects it.
  /// Updates local and batch filter stats and short-circuits on the first filter that
  /// rejects 'row'.
  bool EvalRuntimeFilters(
      TupleRow* row, std::vector<LocalFilterContext>& local_filter_ctxs) noexcept;

  /// Filters rows in 'batch' that fail the runtime filters, compacting the batch
  /// in-place. Rows that pass remain; rows that fail are removed.
  void FilterRowBatch(RowBatch* batch) noexcept;

  /// Disable runtime filters whose rejection ratio is too low to pay off at row level.
  void CheckFiltersEffectiveness() noexcept;

  /// Merge local runtime filter stats into the runtime profile counters, compute
  /// effective filters, and close the FilterContexts.
  void FinalizeFilters(RuntimeState* state) noexcept;

  /// True after WaitForRuntimeFilters() has been called.
  bool filters_waited_ = false;

  /// Number of row batches since the last runtime filter effectiveness check.
  int64_t row_batches_since_filter_check_ = 0;
};

}
