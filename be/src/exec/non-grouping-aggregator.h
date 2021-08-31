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

#ifndef IMPALA_EXEC_NON_GROUPING_AGGREGATOR_H
#define IMPALA_EXEC_NON_GROUPING_AGGREGATOR_H

#include <memory>
#include <vector>

#include "codegen/codegen-fn-ptr.h"
#include "exec/aggregator.h"
#include "runtime/mem-pool.h"

namespace impala {

class AggFnEvaluator;
class AggregationPlanNode;
class DescriptorTbl;
class ExecNode;
class FragmentState;
class LlvmCodeGen;
class NonGroupingAggregator;
class ObjectPool;
class RowBatch;
class RuntimeState;
class TAggregator;
class Tuple;

class NonGroupingAggregatorConfig : public AggregatorConfig {
 public:
  NonGroupingAggregatorConfig(const TAggregator& taggregator, FragmentState* state,
      PlanNode* pnode, int agg_idx);
  void Codegen(FragmentState* state) override;
  ~NonGroupingAggregatorConfig() override {}

  typedef Status (*AddBatchImplFn)(NonGroupingAggregator*, RowBatch*);
  /// Jitted AddBatchImpl function pointer. Null if codegen is disabled.
  CodegenFnPtr<AddBatchImplFn> add_batch_impl_fn_;

  int GetNumGroupingExprs() const override { return 0; }

 private:
  /// Codegen the non-streaming add row batch loop in NonGroupingAggregator::AddBatch()
  /// (Assuming AGGREGATED_ROWS = false). The loop has already been compiled to IR and
  /// loaded into the codegen object. UpdateAggTuple has also been codegen'd to IR. This
  /// function will modify the loop subsituting the statically compiled functions with
  /// codegen'd ones. 'add_batch_impl_fn_' will be updated with the codegened function.
  Status CodegenAddBatchImpl(
      LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) WARN_UNUSED_RESULT;
};

/// Aggregator for doing non-grouping aggregations. Input is passed to the aggregator
/// through AddBatch(), which generates the single output row. This Aggregator does
/// not support streaming preaggregation.
class NonGroupingAggregator : public Aggregator {
 public:
  NonGroupingAggregator(
      ExecNode* exec_node, ObjectPool* pool, const NonGroupingAggregatorConfig& config);

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) override {
    return Status::OK();
  }
  virtual void Close(RuntimeState* state) override;

  virtual Status AddBatch(RuntimeState* state, RowBatch* batch) override;

  /// NonGroupingAggregators behave the same in streaming and non-streaming contexts, so
  /// this just calls AddBatch.
  virtual Status AddBatchStreaming(RuntimeState* state, RowBatch* out_batch,
      RowBatch* child_batch, bool* eos) override;

  virtual Status InputDone() override { return Status::OK(); }

  virtual int GetNumGroupingExprs() override { return 0; }

  /// NonGroupingAggregator doesn't create a buffer pool client so it doesn't need the
  /// debug options.
  virtual void SetDebugOptions(const TDebugOptions& debug_options) override {}

  virtual std::string DebugString(int indentation_level = 0) const override;
  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

  virtual int64_t GetNumKeys() const override { return 1; }

 private:
  /// MemPool used to allocate memory for 'singleton_output_tuple_'. The ownership of the
  /// pool's memory is transferred to the output batch on eos. The pool should not be
  /// Reset() to allow amortizing memory allocation over a series of
  /// Reset()/Open()/GetNext()* calls.
  std::unique_ptr<MemPool> singleton_tuple_pool_;

  /// Jitted AddBatchImpl function pointer. Null if codegen is disabled.
  const CodegenFnPtr<NonGroupingAggregatorConfig::AddBatchImplFn>& add_batch_impl_fn_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Result of aggregation w/o GROUP BY.
  /// Note: can be NULL even if there is no grouping if the result tuple is 0 width
  /// e.g. select 1 from table group by col.
  Tuple* singleton_output_tuple_ = nullptr;
  bool singleton_output_tuple_returned_ = true;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Constructs singleton output tuple, allocating memory from pool.
  Tuple* ConstructSingletonOutputTuple(
      const std::vector<AggFnEvaluator*>& agg_fn_evals, MemPool* pool);

  /// Do the aggregation for all tuple rows in the batch when there is no grouping.
  /// This function is replaced by codegen.
  Status AddBatchImpl(RowBatch* batch) WARN_UNUSED_RESULT;

  /// Output 'singleton_output_tuple_' and transfer memory to 'row_batch'.
  void GetSingletonOutput(RowBatch* row_batch);
};
} // namespace impala

#endif // IMPALA_EXEC_NON_GROUPING_AGGREGATOR_H
