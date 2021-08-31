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

#ifndef IMPALA_EXEC_AGGREGATOR_H
#define IMPALA_EXEC_AGGREGATOR_H

#include <vector>

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "util/runtime-profile.h"

namespace llvm {
class Function;
class Value;
} // namespace llvm

namespace impala {

class AggFn;
class AggFnEvaluator;
class PlanNode;
class CodegenAnyVal;
class DescriptorTbl;
class ExecNode;
class FragmentState;
class LlvmBuilder;
class LlvmCodeGen;
class MemPool;
class MemTracker;
class ObjectPool;
class RowBatch;
class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;
class SlotDescriptor;
class TAggregator;
class Tuple;
class TupleDescriptor;
class TupleRow;

/// AggregatorConfig contains the static state initialized from its corresponding thrift
/// structure. It serves as an input for creating instances of the Aggregator class.
class AggregatorConfig {
 public:
  /// 'agg_idx' is the index of 'TAggregator' in the parent TAggregationNode.
  AggregatorConfig(
      const TAggregator& taggregator, FragmentState* state, PlanNode* pnode, int agg_idx);
  virtual Status Init(
      const TAggregator& taggregator, FragmentState* state, PlanNode* pnode);
  /// Closes the expressions created in Init();
  virtual void Close();
  virtual void Codegen(FragmentState* state) = 0;
  virtual ~AggregatorConfig() {}

  /// The index of this Aggregator within the AggregationNode which is also equivalent to
  /// its corresponding TAggregator in the parent TAggregationNode. When returning output,
  /// this Aggregator should only write tuples at 'agg_idx_' within the row.
  const int agg_idx_;

  /// Tuple into which Update()/Merge()/Serialize() results are stored.
  TupleId intermediate_tuple_id_;
  TupleDescriptor* intermediate_tuple_desc_;

  /// Tuple into which Finalize() results are stored. Possibly the same as
  /// the intermediate tuple.
  TupleId output_tuple_id_;
  TupleDescriptor* output_tuple_desc_;

  /// The RowDescriptor for the exec node this aggregator corresponds to.
  const RowDescriptor& row_desc_;
  /// The RowDescriptor for the child of the exec node this aggregator corresponds to.
  const RowDescriptor& input_row_desc_;

  /// Certain aggregates require a finalize step, which is the final step of the
  /// aggregate after consuming all input rows. The finalize step converts the aggregate
  /// value into its final form. This is true if this aggregator contains aggregate that
  /// requires a finalize step.
  const bool needs_finalize_;

  std::vector<ScalarExpr*> conjuncts_;

  /// The list of all aggregate operations for this aggregator.
  std::vector<AggFn*> aggregate_functions_;

  /// A message that will eventually be added to the aggregator's (spawned using this
  /// config) runtime profile to convey codegen related information. Populated in
  /// Codegen().
  std::string codegen_status_msg_;

  virtual int GetNumGroupingExprs() const = 0;

 protected:
  /// Codegen for updating aggregate expressions aggregate_functions_[agg_fn_idx]
  /// and returns the IR function in 'fn'. Returns non-OK status if codegen
  /// is unsuccessful.
  Status CodegenUpdateSlot(LlvmCodeGen* codegen, int agg_fn_idx,
      SlotDescriptor* slot_desc, llvm::Function** fn) WARN_UNUSED_RESULT;

  /// Codegen a call to a function implementing the UDA interface with input values
  /// from 'input_vals'. 'dst_val' should contain the previous value of the aggregate
  /// function, and 'updated_dst_val' is set to the new value after the Update or Merge
  /// operation is applied. The instruction sequence for the UDA call is inserted at
  /// the insert position of 'builder'.
  Status CodegenCallUda(LlvmCodeGen* codegen, LlvmBuilder* builder, AggFn* agg_fn,
      llvm::Value* agg_fn_ctx_arg, const std::vector<CodegenAnyVal>& input_vals,
      const CodegenAnyVal& dst_val, CodegenAnyVal* updated_dst_val) WARN_UNUSED_RESULT;

  /// Codegen Aggregator::UpdateTuple(). Returns non-OK status if codegen is unsuccessful.
  Status CodegenUpdateTuple(LlvmCodeGen* codegen, llvm::Function** fn) WARN_UNUSED_RESULT;
};

/// Base class for aggregating rows. Used in the AggregationNode and
/// StreamingAggregationNode.
///
/// Rows are added by calling AddBatch(). Once all rows have been added, InputDone() must
/// be called and the results can be fetched with GetNext().
class Aggregator {
 public:
  Aggregator(ExecNode* exec_node, ObjectPool* pool, const AggregatorConfig& config,
      const std::string& name);
  virtual ~Aggregator();

  /// Aggregators follow the same lifecycle as ExecNodes, except that after Open() and
  /// before GetNext() rows should be added with AddBatch(), followed by InputDone()[
  virtual Status Prepare(RuntimeState* state) WARN_UNUSED_RESULT;
  virtual Status Open(RuntimeState* state) WARN_UNUSED_RESULT;
  virtual Status GetNext(
      RuntimeState* state, RowBatch* row_batch, bool* eos) WARN_UNUSED_RESULT = 0;
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) WARN_UNUSED_RESULT = 0;
  virtual void Close(RuntimeState* state);

  /// Adds all of the rows in 'batch' to the aggregation.
  virtual Status AddBatch(RuntimeState* state, RowBatch* batch) = 0;

  /// Used to insert input rows if this is a streaming pre-agg. Tries to aggregate all of
  /// the rows of 'child_batch', but if there isn't enough memory available rows will be
  /// streamed through and returned in 'out_batch'. If 'eos' is true, 'child_batch' was
  /// fully processed, otherwise 'out_batch' was filled up and AddBatchStreaming() should
  /// be called again with the same 'child_batch' and a new 'out_batch'.
  /// AddBatch() and AddBatchStreaming() should not be called on the same Aggregator.
  virtual Status AddBatchStreaming(RuntimeState* state, RowBatch* out_batch,
      RowBatch* child_batch, bool* eos) WARN_UNUSED_RESULT = 0;

  /// Indicates that all batches have been added. Must be called before GetNext().
  virtual Status InputDone() WARN_UNUSED_RESULT = 0;

  virtual int GetNumGroupingExprs() = 0;

  RuntimeProfile* runtime_profile() { return runtime_profile_; }

  virtual void SetDebugOptions(const TDebugOptions& debug_options) = 0;

  virtual std::string DebugString(int indentation_level = 0) const = 0;
  virtual void DebugString(int indentation_level, std::stringstream* out) const = 0;

  static const char* LLVM_CLASS_NAME;

  // The number of unique values that have been aggregated
  virtual int64_t GetNumKeys() const = 0;

 protected:
  /// The id of the ExecNode this Aggregator corresponds to.
  const int id_;
  ExecNode* exec_node_;

  /// Reference to the config object used to generate this instance.
  const AggregatorConfig& config_;

  /// The index of this Aggregator within the AggregationNode. When returning output, this
  /// Aggregator should only write tuples at 'agg_idx_' within the row.
  const int agg_idx_;

  ObjectPool* pool_;

  /// Account for peak memory used by this aggregator.
  std::unique_ptr<MemTracker> mem_tracker_;

  /// MemTracker used by 'expr_perm_pool_' and 'expr_results_pool_'.
  std::unique_ptr<MemTracker> expr_mem_tracker_;

  /// MemPool for allocations made by expression evaluators in this aggregator that are
  /// "permanent" and live until Close() is called. Created in Prepare().
  std::unique_ptr<MemPool> expr_perm_pool_;

  /// MemPool for allocations made by expression evaluators in this aggregator that hold
  /// intermediate or final results of expression evaluation. Should be cleared
  /// periodically to free accumulated memory. QueryMaintenance() clears this pool, but
  /// it may be appropriate for Aggregator implementation to clear it at other points in
  /// execution where the memory is not needed.
  std::unique_ptr<MemPool> expr_results_pool_;

  /// Tuple into which Update()/Merge()/Serialize() results are stored.
  TupleId intermediate_tuple_id_;
  TupleDescriptor* intermediate_tuple_desc_;

  /// Tuple into which Finalize() results are stored. Possibly the same as
  /// the intermediate tuple.
  TupleId output_tuple_id_;
  TupleDescriptor* output_tuple_desc_;

  /// The RowDescriptor for the exec node this aggregator corresponds to.
  const RowDescriptor& row_desc_;
  /// The RowDescriptor for the child of the exec node this aggregator corresponds to.
  const RowDescriptor& input_row_desc_;

  /// Certain aggregates require a finalize step, which is the final step of the
  /// aggregate after consuming all input rows. The finalize step converts the aggregate
  /// value into its final form. This is true if this aggregator contains aggregate that
  /// requires a finalize step.
  const bool needs_finalize_;

  /// The list of all aggregate operations for this aggregator.
  const std::vector<AggFn*>& agg_fns_;

  /// Evaluators for each aggregate function. If this is a grouping aggregation, these
  /// evaluators are only used to create cloned per-partition evaluators. The cloned
  /// evaluators are then used to evaluate the functions. If this is a non-grouping
  /// aggregation these evaluators are used directly to evaluate the functions.
  ///
  /// Permanent and result allocations for these allocators are allocated from
  /// 'expr_perm_pool_' and 'expr_results_pool_' respectively.
  std::vector<AggFnEvaluator*> agg_fn_evals_;

  /// Conjuncts and their evaluators in this aggregator. 'conjuncts_' live in the
  /// query-state's object pool while the evaluators live in this aggregator's
  /// object pool.
  const std::vector<ScalarExpr*>& conjuncts_;
  std::vector<ScalarExprEvaluator*> conjunct_evals_;

  /// Runtime profile for this aggregator. Owned by 'pool_'.
  RuntimeProfile* const runtime_profile_;

  int64_t num_rows_returned_ = 0;
  RuntimeProfile::Counter* rows_returned_counter_ = nullptr;

  /// Time spent processing the child rows
  RuntimeProfile::Counter* build_timer_ = nullptr;

  /// Initializes the aggregate function slots of an intermediate tuple.
  /// Any var-len data is allocated from the FunctionContexts.
  void InitAggSlots(
      const std::vector<AggFnEvaluator*>& agg_fn_evals, Tuple* intermediate_tuple);

  /// Updates the given aggregation intermediate tuple with aggregation values computed
  /// over 'row' using 'agg_fn_evals'. Whether the agg fn evaluator calls Update() or
  /// Merge() is controlled by the evaluator itself, unless enforced explicitly by passing
  /// in is_merge == true.  The override is needed to merge spilled and non-spilled rows
  /// belonging to the same partition independent of whether the agg fn evaluators have
  /// is_merge() == true.
  /// This function is replaced by codegen (which is why we don't use a vector argument
  /// for agg_fn_evals).. Any var-len data is allocated from the FunctionContexts.
  /// TODO: Fix the arguments order. Need to update AggregatorConfig::CodegenUpdateTuple()
  /// too.
  void UpdateTuple(AggFnEvaluator** agg_fn_evals, Tuple* tuple, TupleRow* row,
      bool is_merge = false) noexcept;

  /// Called on the intermediate tuple of each group after all input rows have been
  /// consumed and aggregated. Computes the final aggregate values to be returned in
  /// GetNext() using the agg fn evaluators' Serialize() or Finalize().
  /// For the Finalize() case if the output tuple is different from the intermediate
  /// tuple, then a new tuple is allocated from 'pool' to hold the final result.
  /// Grouping values are copied into the output tuple and the the output tuple holding
  /// the finalized/serialized aggregate values is returned.
  /// TODO: Coordinate the allocation of new tuples with the release of memory
  /// so as not to make memory consumption blow up.
  Tuple* GetOutputTuple(
      const std::vector<AggFnEvaluator*>& agg_fn_evals, Tuple* tuple, MemPool* pool);

  /// Clears 'expr_results_pool_' and returns the result of state->CheckQueryState().
  /// Aggregators should call this periodically, e.g. once per input row batch. This
  /// should not be called outside the main execution thread.
  /// TODO: IMPALA-2399: replace QueryMaintenance() - see JIRA for more details.
  Status QueryMaintenance(RuntimeState* state) WARN_UNUSED_RESULT;
};
} // namespace impala

#endif // IMPALA_EXEC_AGGREGATOR_H
