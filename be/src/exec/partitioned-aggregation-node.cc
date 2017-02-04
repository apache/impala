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

#include "exec/partitioned-aggregation-node.h"

#include <math.h>
#include <algorithm>
#include <set>
#include <sstream>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "exprs/anyval-util.h"
#include "exprs/expr-context.h"
#include "exprs/expr.h"
#include "exprs/slot-ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;
using namespace strings;

namespace impala {

const char* PartitionedAggregationNode::LLVM_CLASS_NAME =
    "class.impala::PartitionedAggregationNode";

/// The minimum reduction factor (input rows divided by output rows) to grow hash tables
/// in a streaming preaggregation, given that the hash tables are currently the given
/// size or above. The sizes roughly correspond to hash table sizes where the bucket
/// arrays will fit in  a cache level. Intuitively, we don't want the working set of the
/// aggregation to expand to the next level of cache unless we're reducing the input
/// enough to outweigh the increased memory latency we'll incur for each hash table
/// lookup.
///
/// Note that the current reduction achieved is not always a good estimate of the
/// final reduction. It may be biased either way depending on the ordering of the
/// input. If the input order is random, we will underestimate the final reduction
/// factor because the probability of a row having the same key as a previous row
/// increases as more input is processed.  If the input order is correlated with the
/// key, skew may bias the estimate. If high cardinality keys appear first, we
/// may overestimate and if low cardinality keys appear first, we underestimate.
/// To estimate the eventual reduction achieved, we estimate the final reduction
/// using the planner's estimated input cardinality and the assumption that input
/// is in a random order. This means that we assume that the reduction factor will
/// increase over time.
struct StreamingHtMinReductionEntry {
  // Use 'streaming_ht_min_reduction' if the total size of hash table bucket directories in
  // bytes is greater than this threshold.
  int min_ht_mem;
  // The minimum reduction factor to expand the hash tables.
  double streaming_ht_min_reduction;
};

// TODO: experimentally tune these values and also programmatically get the cache size
// of the machine that we're running on.
static const StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
  // Expand up to L2 cache always.
  {0, 0.0},
  // Expand into L3 cache if we look like we're getting some reduction.
  {256 * 1024, 1.1},
  // Expand into main memory if we're getting a significant reduction.
  {2 * 1024 * 1024, 2.0},
};

static const int STREAMING_HT_MIN_REDUCTION_SIZE =
    sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

PartitionedAggregationNode::PartitionedAggregationNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_tuple_id_(tnode.agg_node.intermediate_tuple_id),
    intermediate_tuple_desc_(NULL),
    output_tuple_id_(tnode.agg_node.output_tuple_id),
    output_tuple_desc_(NULL),
    needs_finalize_(tnode.agg_node.need_finalize),
    is_streaming_preagg_(tnode.agg_node.use_streaming_preaggregation),
    needs_serialize_(false),
    block_mgr_client_(NULL),
    output_partition_(NULL),
    process_batch_no_grouping_fn_(NULL),
    process_batch_fn_(NULL),
    process_batch_streaming_fn_(NULL),
    build_timer_(NULL),
    ht_resize_timer_(NULL),
    get_results_timer_(NULL),
    num_hash_buckets_(NULL),
    partitions_created_(NULL),
    max_partition_level_(NULL),
    num_row_repartitioned_(NULL),
    num_repartitions_(NULL),
    num_spilled_partitions_(NULL),
    largest_partition_percent_(NULL),
    streaming_timer_(NULL),
    num_passthrough_rows_(NULL),
    preagg_estimated_reduction_(NULL),
    preagg_streaming_ht_min_reduction_(NULL),
    estimated_input_cardinality_(tnode.agg_node.estimated_input_cardinality),
    singleton_output_tuple_(NULL),
    singleton_output_tuple_returned_(true),
    partition_eos_(false),
    child_eos_(false),
    partition_pool_(new ObjectPool()) {
  DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
  if (is_streaming_preagg_) {
    DCHECK(conjunct_ctxs_.empty()) << "Preaggs have no conjuncts";
    DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
    DCHECK(limit_ == -1) << "Preaggs have no limits";
  }
}

Status PartitionedAggregationNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.agg_node.grouping_exprs, &grouping_expr_ctxs_));
  for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(
        AggFnEvaluator::Create(pool_, tnode.agg_node.aggregate_functions[i], &evaluator));
    aggregate_evaluators_.push_back(evaluator);
    ExprContext* const* agg_expr_ctxs;
    if (evaluator->input_expr_ctxs().size() > 0) {
      agg_expr_ctxs = evaluator->input_expr_ctxs().data();
    } else {
      // Some aggregate functions have no input expressions and therefore no ExprContext
      // (e.g. count(*)). In those cases, 'agg_expr_ctxs_' will contain NULL for that
      // entry.
      DCHECK(evaluator->agg_op() == AggFnEvaluator::OTHER || evaluator->is_count_star());
      agg_expr_ctxs = NULL;
    }
    agg_expr_ctxs_.push_back(agg_expr_ctxs);
  }
  return Status::OK();
}

Status PartitionedAggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(ExecNode::Prepare(state));
  state_ = state;

  mem_pool_.reset(new MemPool(mem_tracker()));
  agg_fn_pool_.reset(new MemPool(expr_mem_tracker()));

  ht_resize_timer_ = ADD_TIMER(runtime_profile(), "HTResizeTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  num_hash_buckets_ =
      ADD_COUNTER(runtime_profile(), "HashBuckets", TUnit::UNIT);
  partitions_created_ =
      ADD_COUNTER(runtime_profile(), "PartitionsCreated", TUnit::UNIT);
  largest_partition_percent_ =
      runtime_profile()->AddHighWaterMarkCounter("LargestPartitionPercent", TUnit::UNIT);
  if (is_streaming_preagg_) {
    runtime_profile()->AppendExecOption("Streaming Preaggregation");
    streaming_timer_ = ADD_TIMER(runtime_profile(), "StreamingTime");
    num_passthrough_rows_ =
        ADD_COUNTER(runtime_profile(), "RowsPassedThrough", TUnit::UNIT);
    preagg_estimated_reduction_ = ADD_COUNTER(
        runtime_profile(), "ReductionFactorEstimate", TUnit::DOUBLE_VALUE);
    preagg_streaming_ht_min_reduction_ = ADD_COUNTER(
        runtime_profile(), "ReductionFactorThresholdToExpand", TUnit::DOUBLE_VALUE);
  } else {
    build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
    num_row_repartitioned_ =
        ADD_COUNTER(runtime_profile(), "RowsRepartitioned", TUnit::UNIT);
    num_repartitions_ =
        ADD_COUNTER(runtime_profile(), "NumRepartitions", TUnit::UNIT);
    num_spilled_partitions_ =
        ADD_COUNTER(runtime_profile(), "SpilledPartitions", TUnit::UNIT);
    max_partition_level_ = runtime_profile()->AddHighWaterMarkCounter(
        "MaxPartitionLevel", TUnit::UNIT);
  }

  intermediate_tuple_desc_ =
      state->desc_tbl().GetTupleDescriptor(intermediate_tuple_id_);
  output_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(output_tuple_id_);
  DCHECK_EQ(intermediate_tuple_desc_->slots().size(),
        output_tuple_desc_->slots().size());

  RETURN_IF_ERROR(Expr::Prepare(grouping_expr_ctxs_, state, child(0)->row_desc(),
      expr_mem_tracker()));
  AddExprCtxsToFree(grouping_expr_ctxs_);

  // Construct build exprs from intermediate_agg_tuple_desc_
  for (int i = 0; i < grouping_expr_ctxs_.size(); ++i) {
    SlotDescriptor* desc = intermediate_tuple_desc_->slots()[i];
    DCHECK(desc->type().type == TYPE_NULL ||
        desc->type() == grouping_expr_ctxs_[i]->root()->type());
    // Hack to avoid TYPE_NULL SlotRefs.
    Expr* expr = desc->type().type != TYPE_NULL ?
        new SlotRef(desc) : new SlotRef(desc, TYPE_BOOLEAN);
    state->obj_pool()->Add(expr);
    build_expr_ctxs_.push_back(new ExprContext(expr));
    state->obj_pool()->Add(build_expr_ctxs_.back());
    if (expr->type().IsVarLenStringType()) {
      string_grouping_exprs_.push_back(i);
    }
  }
  // Construct a new row desc for preparing the build exprs because neither the child's
  // nor this node's output row desc may contain the intermediate tuple, e.g.,
  // in a single-node plan with an intermediate tuple different from the output tuple.
  intermediate_row_desc_.reset(new RowDescriptor(intermediate_tuple_desc_, false));
  RETURN_IF_ERROR(
      Expr::Prepare(build_expr_ctxs_, state, *intermediate_row_desc_,
                    expr_mem_tracker()));
  AddExprCtxsToFree(build_expr_ctxs_);

  int j = grouping_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    SlotDescriptor* intermediate_slot_desc = intermediate_tuple_desc_->slots()[j];
    SlotDescriptor* output_slot_desc = output_tuple_desc_->slots()[j];
    FunctionContext* agg_fn_ctx = NULL;
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_slot_desc, output_slot_desc, agg_fn_pool_.get(), &agg_fn_ctx));
    agg_fn_ctxs_.push_back(agg_fn_ctx);
    state->obj_pool()->Add(agg_fn_ctx);
    needs_serialize_ |= aggregate_evaluators_[i]->SupportsSerialize();
  }

  if (!grouping_expr_ctxs_.empty()) {
    RETURN_IF_ERROR(HashTableCtx::Create(state, build_expr_ctxs_, grouping_expr_ctxs_,
        true, vector<bool>(build_expr_ctxs_.size(), true), state->fragment_hash_seed(),
        MAX_PARTITION_DEPTH, 1, mem_tracker(), &ht_ctx_));
    RETURN_IF_ERROR(state_->block_mgr()->RegisterClient(
        Substitute("PartitionedAggregationNode id=$0 ptr=$1", id_, this),
        MinRequiredBuffers(), true, mem_tracker(), state, &block_mgr_client_));
    RETURN_IF_ERROR(CreateHashPartitions(0));
  }

  // TODO: Is there a need to create the stream here? If memory reservations work we may
  // be able to create this stream lazily and only whenever we need to spill.
  if (!is_streaming_preagg_ && needs_serialize_ && block_mgr_client_ != NULL) {
    serialize_stream_.reset(new BufferedTupleStream(state, *intermediate_row_desc_,
        state->block_mgr(), block_mgr_client_, false /* use_initial_small_buffers */,
        false /* read_write */));
    RETURN_IF_ERROR(serialize_stream_->Init(id(), runtime_profile(), false));
    bool got_buffer;
    RETURN_IF_ERROR(serialize_stream_->PrepareForWrite(&got_buffer));
    if (!got_buffer) {
      return state_->block_mgr()->MemLimitTooLowError(block_mgr_client_, id());
    }
    DCHECK(serialize_stream_->has_write_block());
  }
  AddCodegenDisabledMessage(state);
  return Status::OK();
}

void PartitionedAggregationNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  TPrefetchMode::type prefetch_mode = state_->query_options().prefetch_mode;
  Status codegen_status = is_streaming_preagg_ ?
      CodegenProcessBatchStreaming(codegen, prefetch_mode) :
      CodegenProcessBatch(codegen, prefetch_mode);
  runtime_profile()->AddCodegenMsg(codegen_status.ok(), codegen_status);
}

Status PartitionedAggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(Expr::Open(grouping_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));

  DCHECK_EQ(aggregate_evaluators_.size(), agg_fn_ctxs_.size());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Open(state, agg_fn_ctxs_[i]));
  }

  if (grouping_expr_ctxs_.empty()) {
    // Create the single output tuple for this non-grouping agg. This must happen after
    // opening the aggregate evaluators.
    singleton_output_tuple_ =
        ConstructSingletonOutputTuple(agg_fn_ctxs_, mem_pool_.get());
    // Check for failures during AggFnEvaluator::Init().
    RETURN_IF_ERROR(state_->GetQueryStatus());
    singleton_output_tuple_returned_ = false;
  }

  RETURN_IF_ERROR(children_[0]->Open(state));

  // Streaming preaggregations do all processing in GetNext().
  if (is_streaming_preagg_) return Status::OK();

  RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  // Read all the rows from the child and process them.
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));

    if (UNLIKELY(VLOG_ROW_IS_ON)) {
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        VLOG_ROW << "input row: " << PrintRow(row, children_[0]->row_desc());
      }
    }

    TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
    SCOPED_TIMER(build_timer_);
    if (grouping_expr_ctxs_.empty()) {
      if (process_batch_no_grouping_fn_ != NULL) {
        RETURN_IF_ERROR(process_batch_no_grouping_fn_(this, &batch));
      } else {
        RETURN_IF_ERROR(ProcessBatchNoGrouping(&batch));
      }
    } else {
      // There is grouping, so we will do partitioned aggregation.
      if (process_batch_fn_ != NULL) {
        RETURN_IF_ERROR(process_batch_fn_(this, &batch, prefetch_mode, ht_ctx_.get()));
      } else {
        RETURN_IF_ERROR(ProcessBatch<false>(&batch, prefetch_mode, ht_ctx_.get()));
      }
    }
    batch.Reset();
  } while (!eos);

  // The child can be closed at this point in most cases because we have consumed all of
  // the input from the child and transfered ownership of the resources we need. The
  // exception is if we are inside a subplan expecting to call Open()/GetNext() on the
  // child again,
  if (!IsInSubplan()) child(0)->Close(state);
  child_eos_ = true;

  // Done consuming child(0)'s input. Move all the partitions in hash_partitions_
  // to spilled_partitions_ or aggregated_partitions_. We'll finish the processing in
  // GetNext().
  if (!grouping_expr_ctxs_.empty()) {
    RETURN_IF_ERROR(MoveHashPartitions(child(0)->rows_returned()));
  }
  return Status::OK();
}

Status PartitionedAggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch,
    bool* eos) {
  int first_row_idx = row_batch->num_rows();
  RETURN_IF_ERROR(GetNextInternal(state, row_batch, eos));
  RETURN_IF_ERROR(HandleOutputStrings(row_batch, first_row_idx));
  return Status::OK();
}

Status PartitionedAggregationNode::HandleOutputStrings(RowBatch* row_batch,
    int first_row_idx) {
  if (!needs_finalize_ && !needs_serialize_) return Status::OK();
  // String data returned by Serialize() or Finalize() is from local expr allocations in
  // the agg function contexts, and will be freed on the next GetNext() call by
  // FreeLocalAllocations(). The data either needs to be copied out now or sent up the
  // plan and copied out by a blocking ancestor. (See IMPALA-3311)
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    const SlotDescriptor* slot_desc = aggregate_evaluators_[i]->output_slot_desc();
    DCHECK(!slot_desc->type().IsCollectionType()) << "producing collections NYI";
    if (!slot_desc->type().IsVarLenStringType()) continue;
    if (IsInSubplan()) {
      // Copy string data to the row batch's pool. This is more efficient than
      // MarkNeedsDeepCopy() in a subplan since we are likely producing many small
      // batches.
      RETURN_IF_ERROR(CopyStringData(
          slot_desc, row_batch, first_row_idx, row_batch->tuple_data_pool()));
    } else {
      row_batch->MarkNeedsDeepCopy();
      break;
    }
  }
  return Status::OK();
}

Status PartitionedAggregationNode::CopyStringData(const SlotDescriptor* slot_desc,
    RowBatch* row_batch, int first_row_idx, MemPool* pool) {
  DCHECK(slot_desc->type().IsVarLenStringType());
  DCHECK_EQ(row_batch->row_desc().tuple_descriptors().size(), 1);
  FOREACH_ROW(row_batch, first_row_idx, batch_iter) {
    Tuple* tuple = batch_iter.Get()->GetTuple(0);
    StringValue* sv = reinterpret_cast<StringValue*>(
        tuple->GetSlot(slot_desc->tuple_offset()));
    if (sv == NULL || sv->len == 0) continue;
    char* new_ptr = reinterpret_cast<char*>(pool->TryAllocate(sv->len));
    if (UNLIKELY(new_ptr == NULL)) {
      string details = Substitute("Cannot perform aggregation at node with id $0."
          " Failed to allocate $1 output bytes.", id_, sv->len);
      return pool->mem_tracker()->MemLimitExceeded(state_, details, sv->len);
    }
    memcpy(new_ptr, sv->ptr, sv->len);
    sv->ptr = new_ptr;
  }
  return Status::OK();
}

Status PartitionedAggregationNode::GetNextInternal(RuntimeState* state,
    RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  if (grouping_expr_ctxs_.empty()) {
    // There was no grouping, so evaluate the conjuncts and return the single result row.
    // We allow calling GetNext() after eos, so don't return this row again.
    if (!singleton_output_tuple_returned_) GetSingletonOutput(row_batch);
    singleton_output_tuple_returned_ = true;
    *eos = true;
    return Status::OK();
  }

  if (!child_eos_) {
    // For streaming preaggregations, we process rows from the child as we go.
    DCHECK(is_streaming_preagg_);
    RETURN_IF_ERROR(GetRowsStreaming(state, row_batch));
  } else if (!partition_eos_) {
    RETURN_IF_ERROR(GetRowsFromPartition(state, row_batch));
  }

  *eos = partition_eos_ && child_eos_;
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

void PartitionedAggregationNode::GetSingletonOutput(RowBatch* row_batch) {
  DCHECK(grouping_expr_ctxs_.empty());
  int row_idx = row_batch->AddRow();
  TupleRow* row = row_batch->GetRow(row_idx);
  Tuple* output_tuple = GetOutputTuple(
      agg_fn_ctxs_, singleton_output_tuple_, row_batch->tuple_data_pool());
  row->SetTuple(0, output_tuple);
  if (ExecNode::EvalConjuncts(&conjunct_ctxs_[0], conjunct_ctxs_.size(), row)) {
    row_batch->CommitLastRow();
    ++num_rows_returned_;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  // Keep the current chunk to amortize the memory allocation over a series
  // of Reset()/Open()/GetNext()* calls.
  row_batch->tuple_data_pool()->AcquireData(mem_pool_.get(), true);
  // This node no longer owns the memory for singleton_output_tuple_.
  singleton_output_tuple_ = NULL;
}

Status PartitionedAggregationNode::GetRowsFromPartition(RuntimeState* state,
    RowBatch* row_batch) {
  DCHECK(!row_batch->AtCapacity());
  if (output_iterator_.AtEnd()) {
    // Done with this partition, move onto the next one.
    if (output_partition_ != NULL) {
      output_partition_->Close(false);
      output_partition_ = NULL;
    }
    if (aggregated_partitions_.empty() && spilled_partitions_.empty()) {
      // No more partitions, all done.
      partition_eos_ = true;
      return Status::OK();
    }
    // Process next partition.
    RETURN_IF_ERROR(NextPartition());
    DCHECK(output_partition_ != NULL);
  }

  SCOPED_TIMER(get_results_timer_);
  int count = 0;
  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());
  // Keeping returning rows from the current partition.
  while (!output_iterator_.AtEnd()) {
    // This loop can go on for a long time if the conjuncts are very selective. Do query
    // maintenance every N iterations.
    if ((count++ & (N - 1)) == 0) {
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    }

    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* intermediate_tuple = output_iterator_.GetTuple();
    Tuple* output_tuple = GetOutputTuple(
        output_partition_->agg_fn_ctxs, intermediate_tuple, row_batch->tuple_data_pool());
    output_iterator_.Next();
    row->SetTuple(0, output_tuple);
    if (ExecNode::EvalConjuncts(&conjunct_ctxs_[0], conjunct_ctxs_.size(), row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit() || row_batch->AtCapacity()) {
        break;
      }
    }
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  partition_eos_ = ReachedLimit();
  if (output_iterator_.AtEnd()) row_batch->MarkNeedsDeepCopy();

  return Status::OK();
}

Status PartitionedAggregationNode::GetRowsStreaming(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(!child_eos_);
  DCHECK(is_streaming_preagg_);

  if (child_batch_ == NULL) {
    child_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
        mem_tracker()));
  }

  do {
    DCHECK_EQ(out_batch->num_rows(), 0);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));

    RETURN_IF_ERROR(child(0)->GetNext(state, child_batch_.get(), &child_eos_));

    SCOPED_TIMER(streaming_timer_);

    int remaining_capacity[PARTITION_FANOUT];
    bool ht_needs_expansion = false;
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      HashTable* hash_tbl = GetHashTable(i);
      DCHECK(hash_tbl != NULL);
      remaining_capacity[i] = hash_tbl->NumInsertsBeforeResize();
      ht_needs_expansion |= remaining_capacity[i] < child_batch_->num_rows();
    }

    // Stop expanding hash tables if we're not reducing the input sufficiently. As our
    // hash tables expand out of each level of cache hierarchy, every hash table lookup
    // will take longer. We also may not be able to expand hash tables because of memory
    // pressure. In this case HashTable::CheckAndResize() will fail. In either case we
    // should always use the remaining space in the hash table to avoid wasting memory.
    if (ht_needs_expansion && ShouldExpandPreaggHashTables()) {
      for (int i = 0; i < PARTITION_FANOUT; ++i) {
        HashTable* ht = GetHashTable(i);
        if (remaining_capacity[i] < child_batch_->num_rows()) {
          SCOPED_TIMER(ht_resize_timer_);
          if (ht->CheckAndResize(child_batch_->num_rows(), ht_ctx_.get())) {
            remaining_capacity[i] = ht->NumInsertsBeforeResize();
          }
        }
      }
    }

    TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
    if (process_batch_streaming_fn_ != NULL) {
      RETURN_IF_ERROR(process_batch_streaming_fn_(this, needs_serialize_, prefetch_mode,
          child_batch_.get(), out_batch, ht_ctx_.get(), remaining_capacity));
    } else {
      RETURN_IF_ERROR(ProcessBatchStreaming(needs_serialize_, prefetch_mode,
          child_batch_.get(), out_batch, ht_ctx_.get(), remaining_capacity ));
    }

    child_batch_->Reset(); // All rows from child_batch_ were processed.
  } while (out_batch->num_rows() == 0 && !child_eos_);

  if (child_eos_) {
    child(0)->Close(state);
    child_batch_.reset();
    MoveHashPartitions(child(0)->rows_returned());
  }

  num_rows_returned_ += out_batch->num_rows();
  COUNTER_SET(num_passthrough_rows_, num_rows_returned_);
  return Status::OK();
}

bool PartitionedAggregationNode::ShouldExpandPreaggHashTables() const {
  int64_t ht_mem = 0;
  int64_t ht_rows = 0;
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    HashTable* ht = hash_partitions_[i]->hash_tbl.get();
    ht_mem += ht->CurrentMemSize();
    ht_rows += ht->size();
  }

  // Need some rows in tables to have valid statistics.
  if (ht_rows == 0) return true;

  // Find the appropriate reduction factor in our table for the current hash table sizes.
  int cache_level = 0;
  while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
      ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
    ++cache_level;
  }

  // Compare the number of rows in the hash table with the number of input rows that
  // were aggregated into it. Exclude passed through rows from this calculation since
  // they were not in hash tables.
  const int64_t input_rows = children_[0]->rows_returned();
  const int64_t aggregated_input_rows = input_rows - num_rows_returned_;
  const int64_t expected_input_rows = estimated_input_cardinality_ - num_rows_returned_;
  double current_reduction = static_cast<double>(aggregated_input_rows) / ht_rows;

  // TODO: workaround for IMPALA-2490: subplan node rows_returned counter may be
  // inaccurate, which could lead to a divide by zero below.
  if (aggregated_input_rows <= 0) return true;

  // Extrapolate the current reduction factor (r) using the formula
  // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
  // set, N is the number of input rows, excluding passed-through rows, and n is the
  // number of rows inserted or merged into the hash tables. This is a very rough
  // approximation but is good enough to be useful.
  // TODO: consider collecting more statistics to better estimate reduction.
  double estimated_reduction = aggregated_input_rows >= expected_input_rows
      ? current_reduction
      : 1 + (expected_input_rows / aggregated_input_rows) * (current_reduction - 1);
  double min_reduction =
    STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;

  COUNTER_SET(preagg_estimated_reduction_, estimated_reduction);
  COUNTER_SET(preagg_streaming_ht_min_reduction_, min_reduction);
  return estimated_reduction > min_reduction;
}

void PartitionedAggregationNode::CleanupHashTbl(
    const vector<FunctionContext*>& agg_fn_ctxs, HashTable::Iterator it) {
  if (!needs_finalize_ && !needs_serialize_) return;

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs.
  if (needs_finalize_) {
    // Finalize() requires a dst tuple but we don't actually need the result,
    // so allocate a single dummy tuple to avoid accumulating memory.
    Tuple* dummy_dst = NULL;
    dummy_dst = Tuple::Create(output_tuple_desc_->byte_size(), mem_pool_.get());
    while (!it.AtEnd()) {
      Tuple* tuple = it.GetTuple();
      AggFnEvaluator::Finalize(aggregate_evaluators_, agg_fn_ctxs, tuple, dummy_dst);
      it.Next();
    }
  } else {
    while (!it.AtEnd()) {
      Tuple* tuple = it.GetTuple();
      AggFnEvaluator::Serialize(aggregate_evaluators_, agg_fn_ctxs, tuple);
      it.Next();
    }
  }
}

Status PartitionedAggregationNode::Reset(RuntimeState* state) {
  DCHECK(!is_streaming_preagg_) << "Cannot reset preaggregation";
  if (!grouping_expr_ctxs_.empty()) {
    child_eos_ = false;
    partition_eos_ = false;
    // Reset the HT and the partitions for this grouping agg.
    ht_ctx_->set_level(0);
    ClosePartitions();
    RETURN_IF_ERROR(CreateHashPartitions(0));
  }
  return ExecNode::Reset(state);
}

void PartitionedAggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;

  if (!singleton_output_tuple_returned_) {
    DCHECK_EQ(agg_fn_ctxs_.size(), aggregate_evaluators_.size());
    GetOutputTuple(agg_fn_ctxs_, singleton_output_tuple_, mem_pool_.get());
  }

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs
  if (output_partition_ != NULL) {
    CleanupHashTbl(output_partition_->agg_fn_ctxs, output_iterator_);
    output_partition_->Close(false);
  }

  ClosePartitions();

  child_batch_.reset();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    aggregate_evaluators_[i]->Close(state);
  }
  agg_expr_ctxs_.clear();
  for (int i = 0; i < agg_fn_ctxs_.size(); ++i) {
    agg_fn_ctxs_[i]->impl()->Close();
  }
  if (agg_fn_pool_.get() != NULL) agg_fn_pool_->FreeAll();
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  if (ht_ctx_.get() != NULL) ht_ctx_->Close();
  if (serialize_stream_.get() != NULL) {
    serialize_stream_->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }

  if (block_mgr_client_ != NULL) {
    state->block_mgr()->ClearReservations(block_mgr_client_);
  }

  Expr::Close(grouping_expr_ctxs_, state);
  Expr::Close(build_expr_ctxs_, state);
  ExecNode::Close(state);
}

PartitionedAggregationNode::Partition::~Partition() {
  DCHECK(is_closed);
}

Status PartitionedAggregationNode::Partition::InitStreams() {
  agg_fn_pool.reset(new MemPool(parent->expr_mem_tracker()));
  DCHECK_EQ(agg_fn_ctxs.size(), 0);
  for (int i = 0; i < parent->agg_fn_ctxs_.size(); ++i) {
    agg_fn_ctxs.push_back(parent->agg_fn_ctxs_[i]->impl()->Clone(agg_fn_pool.get()));
    parent->partition_pool_->Add(agg_fn_ctxs[i]);
  }

  // Varlen aggregate function results are stored outside of aggregated_row_stream because
  // BufferedTupleStream doesn't support relocating varlen data stored in the stream.
  auto agg_slot = parent->intermediate_tuple_desc_->slots().begin() +
      parent->grouping_expr_ctxs_.size();
  set<SlotId> external_varlen_slots;
  for (; agg_slot != parent->intermediate_tuple_desc_->slots().end(); ++agg_slot) {
    if ((*agg_slot)->type().IsVarLenStringType()) {
      external_varlen_slots.insert((*agg_slot)->id());
    }
  }

  aggregated_row_stream.reset(new BufferedTupleStream(parent->state_,
      *parent->intermediate_row_desc_, parent->state_->block_mgr(),
      parent->block_mgr_client_, true /* use_initial_small_buffers */,
      false /* read_write */, external_varlen_slots));
  RETURN_IF_ERROR(
      aggregated_row_stream->Init(parent->id(), parent->runtime_profile(), true));
  bool got_buffer;
  RETURN_IF_ERROR(aggregated_row_stream->PrepareForWrite(&got_buffer));
  if (!got_buffer) {
    return parent->state_->block_mgr()->MemLimitTooLowError(
        parent->block_mgr_client_, parent->id());
  }

  if (!parent->is_streaming_preagg_) {
    unaggregated_row_stream.reset(new BufferedTupleStream(parent->state_,
        parent->child(0)->row_desc(), parent->state_->block_mgr(),
      parent->block_mgr_client_, true /* use_initial_small_buffers */,
        false /* read_write */));
    // This stream is only used to spill, no need to ever have this pinned.
    RETURN_IF_ERROR(unaggregated_row_stream->Init(parent->id(), parent->runtime_profile(),
        false));
    // TODO: allocate this buffer later only if we spill the partition.
    RETURN_IF_ERROR(unaggregated_row_stream->PrepareForWrite(&got_buffer));
    if (!got_buffer) {
      return parent->state_->block_mgr()->MemLimitTooLowError(
          parent->block_mgr_client_, parent->id());
    }
    DCHECK(unaggregated_row_stream->has_write_block());
  }
  return Status::OK();
}

bool PartitionedAggregationNode::Partition::InitHashTable() {
  DCHECK(hash_tbl.get() == NULL);
  // We use the upper PARTITION_FANOUT num bits to pick the partition so only the
  // remaining bits can be used for the hash table.
  // TODO: we could switch to 64 bit hashes and then we don't need a max size.
  // It might be reasonable to limit individual hash table size for other reasons
  // though. Always start with small buffers.
  hash_tbl.reset(HashTable::Create(parent->state_, parent->block_mgr_client_,
      false, 1, NULL, 1L << (32 - NUM_PARTITIONING_BITS),
      PAGG_DEFAULT_HASH_TABLE_SZ));
  // Please update the error message in CreateHashPartitions() if initial size of
  // hash table changes.
  return hash_tbl->Init();
}

Status PartitionedAggregationNode::Partition::SerializeStreamForSpilling() {
  DCHECK(!parent->is_streaming_preagg_);
  if (parent->needs_serialize_ && aggregated_row_stream->num_rows() != 0) {
    // We need to do a lot more work in this case. This step effectively does a merge
    // aggregation in this node. We need to serialize the intermediates, spill the
    // intermediates and then feed them into the aggregate function's merge step.
    // This is often used when the intermediate is a string type, meaning the current
    // (before serialization) in-memory layout is not the on-disk block layout.
    // The disk layout does not support mutable rows. We need to rewrite the stream
    // into the on disk format.
    // TODO: if it happens to not be a string, we could serialize in place. This is
    // a future optimization since it is very unlikely to have a serialize phase
    // for those UDAs.
    DCHECK(parent->serialize_stream_.get() != NULL);
    DCHECK(!parent->serialize_stream_->is_pinned());
    DCHECK(parent->serialize_stream_->has_write_block());

    const vector<AggFnEvaluator*>& evaluators = parent->aggregate_evaluators_;

    // Serialize and copy the spilled partition's stream into the new stream.
    Status status = Status::OK();
    bool failed_to_add = false;
    BufferedTupleStream* new_stream = parent->serialize_stream_.get();
    HashTable::Iterator it = hash_tbl->Begin(parent->ht_ctx_.get());
    while (!it.AtEnd()) {
      Tuple* tuple = it.GetTuple();
      it.Next();
      AggFnEvaluator::Serialize(evaluators, agg_fn_ctxs, tuple);
      if (UNLIKELY(!new_stream->AddRow(reinterpret_cast<TupleRow*>(&tuple), &status))) {
        failed_to_add = true;
        break;
      }
    }

    // Even if we can't add to new_stream, finish up processing this agg stream to make
    // clean up easier (someone has to finalize this stream and we don't want to remember
    // where we are).
    if (failed_to_add) {
      parent->CleanupHashTbl(agg_fn_ctxs, it);
      hash_tbl->Close();
      hash_tbl.reset();
      aggregated_row_stream->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
      RETURN_IF_ERROR(status);
      return parent->state_->block_mgr()->MemLimitTooLowError(parent->block_mgr_client_,
          parent->id());
    }
    DCHECK(status.ok());

    aggregated_row_stream->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    aggregated_row_stream.swap(parent->serialize_stream_);
    // Recreate the serialize_stream (and reserve 1 buffer) now in preparation for
    // when we need to spill again. We need to have this available before we need
    // to spill to make sure it is available. This should be acquirable since we just
    // freed at least one buffer from this partition's (old) aggregated_row_stream.
    parent->serialize_stream_.reset(new BufferedTupleStream(parent->state_,
        *parent->intermediate_row_desc_, parent->state_->block_mgr(),
        parent->block_mgr_client_, false /* use_initial_small_buffers */,
        false /* read_write */));
    status = parent->serialize_stream_->Init(parent->id(), parent->runtime_profile(),
        false);
    if (status.ok()) {
      bool got_buffer;
      status = parent->serialize_stream_->PrepareForWrite(&got_buffer);
      if (status.ok() && !got_buffer) {
        status = parent->state_->block_mgr()->MemLimitTooLowError(
            parent->block_mgr_client_, parent->id());
      }
    }
    if (!status.ok()) {
      hash_tbl->Close();
      hash_tbl.reset();
      return status;
    }
    DCHECK(parent->serialize_stream_->has_write_block());
  }
  return Status::OK();
}

Status PartitionedAggregationNode::Partition::Spill() {
  DCHECK(!is_closed);
  DCHECK(!is_spilled());

  RETURN_IF_ERROR(SerializeStreamForSpilling());

  // Free the in-memory result data.
  for (int i = 0; i < agg_fn_ctxs.size(); ++i) {
    agg_fn_ctxs[i]->impl()->Close();
  }

  if (agg_fn_pool.get() != NULL) {
    agg_fn_pool->FreeAll();
    agg_fn_pool.reset();
  }

  hash_tbl->Close();
  hash_tbl.reset();

  // Try to switch both streams to IO-sized buffers to avoid allocating small buffers
  // for spilled partition.
  bool got_buffer = true;
  if (aggregated_row_stream->using_small_buffers()) {
    RETURN_IF_ERROR(aggregated_row_stream->SwitchToIoBuffers(&got_buffer));
  }
  // Unpin the stream as soon as possible to increase the chances that the
  // SwitchToIoBuffers() call below will succeed.  If we're repartitioning, rows that
  // were already aggregated (rows from the input partition's aggregated stream) will
  // need to be added to this hash partition's aggregated stream, so we need to leave
  // the write block pinned.
  // TODO: when not repartitioning, don't leave the write block pinned.
  DCHECK(!got_buffer || aggregated_row_stream->has_write_block())
      << aggregated_row_stream->DebugString();
  RETURN_IF_ERROR(
      aggregated_row_stream->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));

  if (got_buffer && unaggregated_row_stream->using_small_buffers()) {
    RETURN_IF_ERROR(unaggregated_row_stream->SwitchToIoBuffers(&got_buffer));
  }
  if (!got_buffer) {
    // We'll try again to get the buffers when the stream fills up the small buffers.
    VLOG_QUERY << "Not enough memory to switch to IO-sized buffer for partition "
               << this << " of agg=" << parent->id_ << " agg small buffers="
               << aggregated_row_stream->using_small_buffers()
               << " unagg small buffers="
               << unaggregated_row_stream->using_small_buffers();
    VLOG_FILE << GetStackTrace();
  }

  COUNTER_ADD(parent->num_spilled_partitions_, 1);
  if (parent->num_spilled_partitions_->value() == 1) {
    parent->runtime_profile()->AppendExecOption("Spilled");
  }
  return Status::OK();
}

void PartitionedAggregationNode::Partition::Close(bool finalize_rows) {
  if (is_closed) return;
  is_closed = true;
  if (aggregated_row_stream.get() != NULL) {
    if (finalize_rows && hash_tbl.get() != NULL) {
      // We need to walk all the rows and Finalize them here so the UDA gets a chance
      // to cleanup. If the hash table is gone (meaning this was spilled), the rows
      // should have been finalized/serialized in Spill().
      parent->CleanupHashTbl(agg_fn_ctxs, hash_tbl->Begin(parent->ht_ctx_.get()));
    }
    aggregated_row_stream->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  if (hash_tbl.get() != NULL) hash_tbl->Close();
  if (unaggregated_row_stream.get() != NULL) {
    unaggregated_row_stream->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }

  for (int i = 0; i < agg_fn_ctxs.size(); ++i) {
    agg_fn_ctxs[i]->impl()->Close();
  }
  if (agg_fn_pool.get() != NULL) agg_fn_pool->FreeAll();
}

Tuple* PartitionedAggregationNode::ConstructSingletonOutputTuple(
    const vector<FunctionContext*>& agg_fn_ctxs, MemPool* pool) {
  DCHECK(grouping_expr_ctxs_.empty());
  Tuple* output_tuple = Tuple::Create(intermediate_tuple_desc_->byte_size(), pool);
  InitAggSlots(agg_fn_ctxs, output_tuple);
  return output_tuple;
}

Tuple* PartitionedAggregationNode::ConstructIntermediateTuple(
    const vector<FunctionContext*>& agg_fn_ctxs, MemPool* pool, Status* status) noexcept {
  const int fixed_size = intermediate_tuple_desc_->byte_size();
  const int varlen_size = GroupingExprsVarlenSize();
  const int tuple_data_size = fixed_size + varlen_size;
  uint8_t* tuple_data = pool->TryAllocate(tuple_data_size);
  if (UNLIKELY(tuple_data == NULL)) {
    string details = Substitute("Cannot perform aggregation at node with id $0. Failed "
        "to allocate $1 bytes for intermediate tuple.", id_, tuple_data_size);
    *status = pool->mem_tracker()->MemLimitExceeded(state_, details, tuple_data_size);
    return NULL;
  }
  memset(tuple_data, 0, fixed_size);
  Tuple* intermediate_tuple = reinterpret_cast<Tuple*>(tuple_data);
  uint8_t* varlen_data = tuple_data + fixed_size;
  CopyGroupingValues(intermediate_tuple, varlen_data, varlen_size);
  InitAggSlots(agg_fn_ctxs, intermediate_tuple);
  return intermediate_tuple;
}

Tuple* PartitionedAggregationNode::ConstructIntermediateTuple(
    const vector<FunctionContext*>& agg_fn_ctxs, BufferedTupleStream* stream,
    Status* status) noexcept {
  DCHECK(stream != NULL && status != NULL);
  // Allocate space for the entire tuple in the stream.
  const int fixed_size = intermediate_tuple_desc_->byte_size();
  const int varlen_size = GroupingExprsVarlenSize();
  uint8_t* varlen_buffer;
  uint8_t* fixed_buffer = stream->AllocateRow(fixed_size, varlen_size, &varlen_buffer,
      status);
  if (UNLIKELY(fixed_buffer == NULL)) {
    if (!status->ok() || !stream->using_small_buffers()) return NULL;
    // IMPALA-2352: Make a best effort to switch to IO buffers and re-allocate.
    // If SwitchToIoBuffers() fails the caller of this function can try to free
    // some space, e.g. through spilling, and re-attempt to allocate space for
    // this row.
    bool got_buffer;
    *status = stream->SwitchToIoBuffers(&got_buffer);
    if (!status->ok() || !got_buffer) return NULL;
    fixed_buffer = stream->AllocateRow(fixed_size, varlen_size, &varlen_buffer, status);
    if (fixed_buffer == NULL) return NULL;
  }

  Tuple* intermediate_tuple = reinterpret_cast<Tuple*>(fixed_buffer);
  intermediate_tuple->Init(fixed_size);
  CopyGroupingValues(intermediate_tuple, varlen_buffer, varlen_size);
  InitAggSlots(agg_fn_ctxs, intermediate_tuple);
  return intermediate_tuple;
}

int PartitionedAggregationNode::GroupingExprsVarlenSize() {
  int varlen_size = 0;
  // TODO: The hash table could compute this as it hashes.
  for (int expr_idx: string_grouping_exprs_) {
    StringValue* sv = reinterpret_cast<StringValue*>(ht_ctx_->ExprValue(expr_idx));
    // Avoid branching by multiplying length by null bit.
    varlen_size += sv->len * !ht_ctx_->ExprValueNull(expr_idx);
  }
  return varlen_size;
}

// TODO: codegen this function.
void PartitionedAggregationNode::CopyGroupingValues(Tuple* intermediate_tuple,
    uint8_t* buffer, int varlen_size) {
  // Copy over all grouping slots (the variable length data is copied below).
  for (int i = 0; i < grouping_expr_ctxs_.size(); ++i) {
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[i];
    if (ht_ctx_->ExprValueNull(i)) {
      intermediate_tuple->SetNull(slot_desc->null_indicator_offset());
    } else {
      void* src = ht_ctx_->ExprValue(i);
      void* dst = intermediate_tuple->GetSlot(slot_desc->tuple_offset());
      memcpy(dst, src, slot_desc->slot_size());
    }
  }

  for (int expr_idx: string_grouping_exprs_) {
    if (ht_ctx_->ExprValueNull(expr_idx)) continue;

    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[expr_idx];
    // ptr and len were already copied to the fixed-len part of string value
    StringValue* sv = reinterpret_cast<StringValue*>(
        intermediate_tuple->GetSlot(slot_desc->tuple_offset()));
    memcpy(buffer, sv->ptr, sv->len);
    sv->ptr = reinterpret_cast<char*>(buffer);
    buffer += sv->len;
  }
}

// TODO: codegen this function.
void PartitionedAggregationNode::InitAggSlots(
    const vector<FunctionContext*>& agg_fn_ctxs, Tuple* intermediate_tuple) {
  vector<SlotDescriptor*>::const_iterator slot_desc =
      intermediate_tuple_desc_->slots().begin() + grouping_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++slot_desc) {
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    // To minimize branching on the UpdateTuple path, initialize the result value so that
    // the Add() UDA function can ignore the NULL bit of its destination value. E.g. for
    // SUM(), if we initialize the destination value to 0 (with the NULL bit set), we can
    // just start adding to the destination value (rather than repeatedly checking the
    // destination NULL bit. The codegen'd version of UpdateSlot() exploits this to
    // eliminate a branch per value.
    //
    // For boolean and numeric types, the default values are false/0, so the nullable
    // aggregate functions SUM() and AVG() produce the correct result. For MIN()/MAX(),
    // initialize the value to max/min possible value for the same effect.
    evaluator->Init(agg_fn_ctxs[i], intermediate_tuple);

    AggFnEvaluator::AggregationOp agg_op = evaluator->agg_op();
    if ((agg_op == AggFnEvaluator::MIN || agg_op == AggFnEvaluator::MAX)
        && !evaluator->intermediate_type().IsStringType()
        && !evaluator->intermediate_type().IsTimestampType()) {
      ExprValue default_value;
      void* default_value_ptr = NULL;
      if (evaluator->agg_op() == AggFnEvaluator::MIN) {
        default_value_ptr = default_value.SetToMax((*slot_desc)->type());
      } else {
        DCHECK_EQ(evaluator->agg_op(), AggFnEvaluator::MAX);
        default_value_ptr = default_value.SetToMin((*slot_desc)->type());
      }
      RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
    }
  }
}

void PartitionedAggregationNode::UpdateTuple(
    FunctionContext** agg_fn_ctxs, Tuple* tuple, TupleRow* row, bool is_merge) noexcept {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    if (is_merge) {
      aggregate_evaluators_[i]->Merge(agg_fn_ctxs[i], row->GetTuple(0), tuple);
    } else {
      aggregate_evaluators_[i]->Add(agg_fn_ctxs[i], row, tuple);
    }
  }
}

Tuple* PartitionedAggregationNode::GetOutputTuple(
    const vector<FunctionContext*>& agg_fn_ctxs, Tuple* tuple, MemPool* pool) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty()) << tuple;
  Tuple* dst = tuple;
  if (needs_finalize_ && intermediate_tuple_id_ != output_tuple_id_) {
    dst = Tuple::Create(output_tuple_desc_->byte_size(), pool);
  }
  if (needs_finalize_) {
    AggFnEvaluator::Finalize(aggregate_evaluators_, agg_fn_ctxs, tuple, dst);
  } else {
    AggFnEvaluator::Serialize(aggregate_evaluators_, agg_fn_ctxs, tuple);
  }
  // Copy grouping values from tuple to dst.
  // TODO: Codegen this.
  if (dst != tuple) {
    int num_grouping_slots = grouping_expr_ctxs_.size();
    for (int i = 0; i < num_grouping_slots; ++i) {
      SlotDescriptor* src_slot_desc = intermediate_tuple_desc_->slots()[i];
      SlotDescriptor* dst_slot_desc = output_tuple_desc_->slots()[i];
      bool src_slot_null = tuple->IsNull(src_slot_desc->null_indicator_offset());
      void* src_slot = NULL;
      if (!src_slot_null) src_slot = tuple->GetSlot(src_slot_desc->tuple_offset());
      RawValue::Write(src_slot, dst, dst_slot_desc, NULL);
    }
  }
  return dst;
}

Status PartitionedAggregationNode::AppendSpilledRow(BufferedTupleStream* stream,
    TupleRow* row) {
  DCHECK(stream != NULL);
  DCHECK(!stream->is_pinned());
  DCHECK(stream->has_write_block());
  if (LIKELY(stream->AddRow(row, &process_batch_status_))) return Status::OK();

  // Adding fails iff either we hit an error or haven't switched to I/O buffers.
  RETURN_IF_ERROR(process_batch_status_);
  while (true) {
    bool got_buffer;
    RETURN_IF_ERROR(stream->SwitchToIoBuffers(&got_buffer));
    if (got_buffer) break;
    RETURN_IF_ERROR(SpillPartition());
  }

  // Adding the row should succeed after the I/O buffer switch.
  if (stream->AddRow(row, &process_batch_status_)) return Status::OK();
  DCHECK(!process_batch_status_.ok());
  return process_batch_status_;
}

void PartitionedAggregationNode::DebugString(int indentation_level,
    stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "PartitionedAggregationNode("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_
       << " needs_finalize=" << needs_finalize_
       << " grouping_exprs=" << Expr::DebugString(grouping_expr_ctxs_)
       << " agg_exprs=" << AggFnEvaluator::DebugString(aggregate_evaluators_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status PartitionedAggregationNode::CreateHashPartitions(int level) {
  if (is_streaming_preagg_) DCHECK_EQ(level, 0);
  if (UNLIKELY(level >= MAX_PARTITION_DEPTH)) {
    return Status(TErrorCode::PARTITIONED_AGG_MAX_PARTITION_DEPTH, id_, MAX_PARTITION_DEPTH);
  }
  ht_ctx_->set_level(level);

  DCHECK(hash_partitions_.empty());
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* new_partition = new Partition(this, level);
    DCHECK(new_partition != NULL);
    hash_partitions_.push_back(partition_pool_->Add(new_partition));
    RETURN_IF_ERROR(new_partition->InitStreams());
    hash_tbls_[i] = NULL;
  }
  if (!is_streaming_preagg_) {
    DCHECK_GT(state_->block_mgr()->num_reserved_buffers_remaining(block_mgr_client_), 0);
  }

  // Now that all the streams are reserved (meaning we have enough memory to execute
  // the algorithm), allocate the hash tables. These can fail and we can still continue.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    if (UNLIKELY(!hash_partitions_[i]->InitHashTable())) {
      // We don't spill on preaggregations. If we have so little memory that we can't
      // allocate small hash tables, the mem limit is just too low.
      if (is_streaming_preagg_) {
        int64_t alloc_size = PAGG_DEFAULT_HASH_TABLE_SZ * HashTable::BucketSize();
        string details = Substitute("Cannot perform aggregation at node with id $0."
            " Failed to initialize hash table in preaggregation. The memory limit"
            " is too low to execute the query.", id_);
        return mem_tracker()->MemLimitExceeded(state_, details, alloc_size);
      }
      RETURN_IF_ERROR(hash_partitions_[i]->Spill());
    }
    hash_tbls_[i] = hash_partitions_[i]->hash_tbl.get();
  }

  COUNTER_ADD(partitions_created_, hash_partitions_.size());
  if (!is_streaming_preagg_) {
    COUNTER_SET(max_partition_level_, level);
  }
  return Status::OK();
}

Status PartitionedAggregationNode::CheckAndResizeHashPartitions(int num_rows,
    const HashTableCtx* ht_ctx) {
  DCHECK(!is_streaming_preagg_);
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* partition = hash_partitions_[i];
    while (!partition->is_spilled()) {
      {
        SCOPED_TIMER(ht_resize_timer_);
        if (partition->hash_tbl->CheckAndResize(num_rows, ht_ctx)) break;
      }
      RETURN_IF_ERROR(SpillPartition());
    }
  }
  return Status::OK();
}

int64_t PartitionedAggregationNode::LargestSpilledPartition() const {
  int64_t max_rows = 0;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition->is_closed || !partition->is_spilled()) continue;
    int64_t rows = partition->aggregated_row_stream->num_rows() +
        partition->unaggregated_row_stream->num_rows();
    if (rows > max_rows) max_rows = rows;
  }
  return max_rows;
}

Status PartitionedAggregationNode::NextPartition() {
  DCHECK(output_partition_ == NULL);

  // Keep looping until we get to a partition that fits in memory.
  Partition* partition = NULL;
  while (true) {
    partition = NULL;
    // First return partitions that are fully aggregated (and in memory).
    if (!aggregated_partitions_.empty()) {
      partition = aggregated_partitions_.front();
      DCHECK(!partition->is_spilled());
      aggregated_partitions_.pop_front();
      break;
    }

    if (partition == NULL) {
      DCHECK(!spilled_partitions_.empty());
      DCHECK(!is_streaming_preagg_);
      DCHECK_EQ(state_->block_mgr()->num_pinned_buffers(block_mgr_client_),
          needs_serialize_ ? 1 : 0);

      // TODO: we can probably do better than just picking the first partition. We
      // can base this on the amount written to disk, etc.
      partition = spilled_partitions_.front();
      DCHECK(partition->is_spilled());

      // Create the new hash partitions to repartition into.
      // TODO: we don't need to repartition here. We are now working on 1 / FANOUT
      // of the input so it's reasonably likely it can fit. We should look at this
      // partitions size and just do the aggregation if it fits in memory.
      RETURN_IF_ERROR(CreateHashPartitions(partition->level + 1));
      COUNTER_ADD(num_repartitions_, 1);

      // Rows in this partition could have been spilled into two streams, depending
      // on if it is an aggregated intermediate, or an unaggregated row.
      // Note: we must process the aggregated rows first to save a hash table lookup
      // in ProcessBatch().
      RETURN_IF_ERROR(ProcessStream<true>(partition->aggregated_row_stream.get()));
      RETURN_IF_ERROR(ProcessStream<false>(partition->unaggregated_row_stream.get()));

      COUNTER_ADD(num_row_repartitioned_, partition->aggregated_row_stream->num_rows());
      COUNTER_ADD(num_row_repartitioned_,
          partition->unaggregated_row_stream->num_rows());

      partition->Close(false);
      spilled_partitions_.pop_front();

      // Done processing this partition. Move the new partitions into
      // spilled_partitions_/aggregated_partitions_.
      int64_t num_input_rows = partition->aggregated_row_stream->num_rows() +
          partition->unaggregated_row_stream->num_rows();

      // Check if there was any reduction in the size of partitions after repartitioning.
      int64_t largest_partition = LargestSpilledPartition();
      DCHECK_GE(num_input_rows, largest_partition) << "Cannot have a partition with "
          "more rows than the input";
      if (UNLIKELY(num_input_rows == largest_partition)) {
        return Status(TErrorCode::PARTITIONED_AGG_REPARTITION_FAILS, id_,
            partition->level + 1, num_input_rows);
      }
      RETURN_IF_ERROR(MoveHashPartitions(num_input_rows));
    }
  }

  DCHECK(partition->hash_tbl.get() != NULL);
  DCHECK(partition->aggregated_row_stream->is_pinned());

  output_partition_ = partition;
  output_iterator_ = output_partition_->hash_tbl->Begin(ht_ctx_.get());
  COUNTER_ADD(num_hash_buckets_, output_partition_->hash_tbl->num_buckets());
  return Status::OK();
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessStream(BufferedTupleStream* input_stream) {
  DCHECK(!is_streaming_preagg_);
  if (input_stream->num_rows() > 0) {
    while (true) {
      bool got_buffer = false;
      RETURN_IF_ERROR(input_stream->PrepareForRead(true, &got_buffer));
      if (got_buffer) break;
      // Did not have a buffer to read the input stream. Spill and try again.
      RETURN_IF_ERROR(SpillPartition());
    }

    TPrefetchMode::type prefetch_mode = state_->query_options().prefetch_mode;
    bool eos = false;
    RowBatch batch(AGGREGATED_ROWS ? *intermediate_row_desc_ : children_[0]->row_desc(),
                   state_->batch_size(), mem_tracker());
    do {
      RETURN_IF_ERROR(input_stream->GetNext(&batch, &eos));
      RETURN_IF_ERROR(
          ProcessBatch<AGGREGATED_ROWS>(&batch, prefetch_mode, ht_ctx_.get()));
      RETURN_IF_ERROR(state_->GetQueryStatus());
      FreeLocalAllocations();
      batch.Reset();
    } while (!eos);
  }
  input_stream->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  return Status::OK();
}

Status PartitionedAggregationNode::SpillPartition() {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick the largest partition that is not spilled.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed) continue;
    if (hash_partitions_[i]->is_spilled()) continue;
    // Pass 'true' because we need to keep the write block pinned. See Partition::Spill().
    int64_t mem = hash_partitions_[i]->aggregated_row_stream->bytes_in_mem(true);
    mem += hash_partitions_[i]->hash_tbl->ByteSize();
    mem += hash_partitions_[i]->agg_fn_pool->total_reserved_bytes();
    DCHECK_GT(mem, 0); // At least the hash table buckets should occupy memory.
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }
  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too low.
    return state_->block_mgr()->MemLimitTooLowError(block_mgr_client_, id());
  }

  hash_tbls_[partition_idx] = NULL;
  return hash_partitions_[partition_idx]->Spill();
}

Status PartitionedAggregationNode::MoveHashPartitions(int64_t num_input_rows) {
  DCHECK(!hash_partitions_.empty());
  stringstream ss;
  ss << "PA(node_id=" << id() << ") partitioned(level="
     << hash_partitions_[0]->level << ") "
     << num_input_rows << " rows into:" << endl;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    int64_t aggregated_rows = partition->aggregated_row_stream->num_rows();
    int64_t unaggregated_rows = 0;
    if (partition->unaggregated_row_stream != NULL) {
      unaggregated_rows = partition->unaggregated_row_stream->num_rows();
    }
    double total_rows = aggregated_rows + unaggregated_rows;
    double percent = total_rows * 100 / num_input_rows;
    ss << "  " << i << " "  << (partition->is_spilled() ? "spilled" : "not spilled")
       << " (fraction=" << fixed << setprecision(2) << percent << "%)" << endl
       << "    #aggregated rows:" << aggregated_rows << endl
       << "    #unaggregated rows: " << unaggregated_rows << endl;

    // TODO: update counters to support doubles.
    COUNTER_SET(largest_partition_percent_, static_cast<int64_t>(percent));

    if (total_rows == 0) {
      partition->Close(false);
    } else if (partition->is_spilled()) {
      DCHECK(partition->hash_tbl.get() == NULL);
      // We need to unpin all the spilled partitions to make room to allocate new
      // hash_partitions_ when we repartition the spilled partitions.
      // TODO: we only need to do this when we have memory pressure. This might be
      // okay though since the block mgr should only write these to disk if there
      // is memory pressure.
      RETURN_IF_ERROR(
          partition->aggregated_row_stream->UnpinStream(BufferedTupleStream::UNPIN_ALL));
      RETURN_IF_ERROR(partition->unaggregated_row_stream->UnpinStream(
          BufferedTupleStream::UNPIN_ALL));

      // Push new created partitions at the front. This means a depth first walk
      // (more finely partitioned partitions are processed first). This allows us
      // to delete blocks earlier and bottom out the recursion earlier.
      spilled_partitions_.push_front(partition);
    } else {
      aggregated_partitions_.push_back(partition);
    }

  }
  VLOG(2) << ss.str();
  hash_partitions_.clear();
  return Status::OK();
}

void PartitionedAggregationNode::ClosePartitions() {
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    hash_partitions_[i]->Close(true);
  }
  for (list<Partition*>::iterator it = aggregated_partitions_.begin();
      it != aggregated_partitions_.end(); ++it) {
    (*it)->Close(true);
  }
  for (list<Partition*>::iterator it = spilled_partitions_.begin();
      it != spilled_partitions_.end(); ++it) {
    (*it)->Close(true);
  }
  aggregated_partitions_.clear();
  spilled_partitions_.clear();
  hash_partitions_.clear();
  memset(hash_tbls_, 0, sizeof(hash_tbls_));
  partition_pool_->Clear();
}

Status PartitionedAggregationNode::QueryMaintenance(RuntimeState* state) {
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    ExprContext::FreeLocalAllocations(aggregate_evaluators_[i]->input_expr_ctxs());
  }
  ExprContext::FreeLocalAllocations(agg_fn_ctxs_);
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    ExprContext::FreeLocalAllocations(hash_partitions_[i]->agg_fn_ctxs);
  }
  return ExecNode::QueryMaintenance(state);
}

// IR Generation for updating a single aggregation slot. Signature is:
// void UpdateSlot(FunctionContext* agg_fn_ctx, ExprContext* agg_expr_ctx,
//     AggTuple* agg_tuple, char** row)
//
// The IR for sum(double_col), which is constructed directly with the IRBuilder, is:
//
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %agg_fn_ctx,
//    %"class.impala::ExprContext"** %agg_expr_ctxs,
//    { i8, [7 x i8], double }* %agg_tuple, %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %expr_ctx_ptr = getelementptr %"class.impala::ExprContext"*,
//      %"class.impala::ExprContext"** %agg_expr_ctxs, i32 0
//   %expr_ctx = load %"class.impala::ExprContext"*,
//      %"class.impala::ExprContext"** %expr_ctx_ptr
//   %input0 = call { i8, double } @GetSlotRef(%"class.impala::ExprContext"* %expr_ctx,
//      %"class.impala::TupleRow"* %row)
//   %dst_slot_ptr = getelementptr inbounds { i8, [7 x i8], double },
//       { i8, [7 x i8], double }* %agg_tuple, i32 0, i32 2
//   %dst_val = load double, double* %dst_slot_ptr
//   %0 = extractvalue { i8, double } %input0, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %not_null
//
// ret:                                              ; preds = %not_null, %entry
//   ret void
//
// not_null:                                         ; preds = %entry
//   %val = extractvalue { i8, double } %input0, 1
//   %1 = fadd double %dst_val, %val
//   %2 = bitcast { i8, [7 x i8], double }* %agg_tuple to i8*
//   %null_byte_ptr = getelementptr i8, i8* %2, i32 0
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_bit_cleared = and i8 %null_byte, -2
//   store i8 %null_bit_cleared, i8* %null_byte_ptr
//   store double %1, double* %dst_slot_ptr
//   br label %ret
// }
//
// The IR for min(timestamp_col), which uses the UDA interface, is:
//
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %agg_fn_ctx,
//      %"class.impala::ExprContext"** %agg_expr_ctxs,
//      { i8, [7 x i8], %"class.impala::TimestampValue" }* %agg_tuple,
//      %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %dst_lowered_ptr = alloca { i64, i64 }
//   %input_lowered_ptr = alloca { i64, i64 }
//   %expr_ctx_ptr = getelementptr %"class.impala::ExprContext"*,
//        %"class.impala::ExprContext"** %agg_expr_ctxs, i32 0
//   %expr_ctx = load %"class.impala::ExprContext"*,
//        %"class.impala::ExprContext"** %expr_ctx_ptr
//   %input0 = call { i64, i64 } @GetSlotRef(%"class.impala::ExprContext"* %expr_ctx,
//        %"class.impala::TupleRow"* %row)
//   %dst_slot_ptr = getelementptr inbounds { i8, [7 x i8],
//        %"class.impala::TimestampValue" }, { i8, [7 x i8],
//        %"class.impala::TimestampValue" }* %agg_tuple, i32 0, i32 2
//   %dst_val = load %"class.impala::TimestampValue",
//        %"class.impala::TimestampValue"* %dst_slot_ptr
//   %0 = bitcast { i8, [7 x i8], %"class.impala::TimestampValue" }* %agg_tuple to i8*
//   %null_byte_ptr = getelementptr i8, i8* %0, i32 0
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_mask = and i8 %null_byte, 1
//   %is_null = icmp ne i8 %null_mask, 0
//   %is_null_ext = zext i1 %is_null to i64
//   %1 = or i64 0, %is_null_ext
//   %dst = insertvalue { i64, i64 } zeroinitializer, i64 %1, 0
//   %time_of_day = extractvalue %"class.impala::TimestampValue" %dst_val, 0, 0, 0, 0
//   %dst1 = insertvalue { i64, i64 } %dst, i64 %time_of_day, 1
//   %date = extractvalue %"class.impala::TimestampValue" %dst_val, 1, 0, 0
//   %2 = extractvalue { i64, i64 } %dst1, 0
//   %3 = zext i32 %date to i64
//   %4 = shl i64 %3, 32
//   %5 = and i64 %2, 4294967295
//   %6 = or i64 %5, %4
//   %dst2 = insertvalue { i64, i64 } %dst1, i64 %6, 0
//   store { i64, i64 } %input0, { i64, i64 }* %input_lowered_ptr
//   %input_unlowered_ptr = bitcast { i64, i64 }* %input_lowered_ptr
//        to %"struct.impala_udf::TimestampVal"*
//   store { i64, i64 } %dst2, { i64, i64 }* %dst_lowered_ptr
//   %dst_unlowered_ptr = bitcast { i64, i64 }* %dst_lowered_ptr
//        to %"struct.impala_udf::TimestampVal"*
//   call void
//        @_ZN6impala18AggregateFunctions3MinIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_.2(
//        %"class.impala_udf::FunctionContext"* %agg_fn_ctx,
//        %"struct.impala_udf::TimestampVal"* %input_unlowered_ptr,
//        %"struct.impala_udf::TimestampVal"* %dst_unlowered_ptr)
//   %anyval_result = load { i64, i64 }, { i64, i64 }* %dst_lowered_ptr
//   %7 = extractvalue { i64, i64 } %anyval_result, 1
//   %8 = insertvalue %"class.impala::TimestampValue" zeroinitializer, i64 %7, 0, 0, 0, 0
//   %9 = extractvalue { i64, i64 } %anyval_result, 0
//   %10 = ashr i64 %9, 32
//   %11 = trunc i64 %10 to i32
//   %12 = insertvalue %"class.impala::TimestampValue" %8, i32 %11, 1, 0, 0
//   %13 = extractvalue { i64, i64 } %anyval_result, 0
//   %result_is_null = trunc i64 %13 to i1
//   %14 = bitcast { i8, [7 x i8], %"class.impala::TimestampValue" }* %agg_tuple to i8*
//   %null_byte_ptr3 = getelementptr i8, i8* %14, i32 0
//   %null_byte4 = load i8, i8* %null_byte_ptr3
//   %null_bit_cleared = and i8 %null_byte4, -2
//   %15 = sext i1 %result_is_null to i8
//   %null_bit = and i8 %15, 1
//   %null_bit_set = or i8 %null_bit_cleared, %null_bit
//   store i8 %null_bit_set, i8* %null_byte_ptr3
//   store %"class.impala::TimestampValue" %12,
//        %"class.impala::TimestampValue"* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %entry
//   ret void
// }
//
Status PartitionedAggregationNode::CodegenUpdateSlot(LlvmCodeGen* codegen,
    AggFnEvaluator* evaluator, int evaluator_idx, SlotDescriptor* slot_desc,
    Function** fn) {
  PointerType* fn_ctx_type =
      codegen->GetPtrType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  PointerType* expr_ctxs_type =
      codegen->GetPtrPtrType(codegen->GetType(ExprContext::LLVM_CLASS_NAME));
  StructType* tuple_struct = intermediate_tuple_desc_->GetLlvmStruct(codegen);
  if (tuple_struct == NULL) {
    return Status("PartitionedAggregationNode::CodegenUpdateSlot(): failed to generate "
                  "intermediate tuple desc");
  }
  PointerType* tuple_ptr_type = codegen->GetPtrType(tuple_struct);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);

  // Create UpdateSlot prototype
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_fn_ctx", fn_ctx_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_expr_ctxs", expr_ctxs_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmBuilder builder(codegen->context());
  Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, &args[0]);
  Value* agg_fn_ctx_arg = args[0];
  Value* agg_expr_ctxs_arg = args[1];
  Value* agg_tuple_arg = args[2];
  Value* row_arg = args[3];

  DCHECK_GE(evaluator->input_expr_ctxs().size(), 1);
  vector<CodegenAnyVal> input_vals;
  for (int i = 0; i < evaluator->input_expr_ctxs().size(); ++i) {
    ExprContext* agg_expr_ctx = evaluator->input_expr_ctxs()[i];
    Expr* agg_expr = agg_expr_ctx->root();
    Function* agg_expr_fn;
    RETURN_IF_ERROR(agg_expr->GetCodegendComputeFn(codegen, &agg_expr_fn));
    DCHECK(agg_expr_fn != NULL);

    // Call expr function with the matching expr context to get src slot value.
    Value* expr_ctx_ptr = builder.CreateInBoundsGEP(
        agg_expr_ctxs_arg, codegen->GetIntConstant(TYPE_INT, i), "expr_ctx_ptr");
    Value* expr_ctx = builder.CreateLoad(expr_ctx_ptr, "expr_ctx");
    string input_name = Substitute("input$0", i);
    input_vals.push_back(
        CodegenAnyVal::CreateCallWrapped(codegen, &builder, agg_expr->type(), agg_expr_fn,
            ArrayRef<Value*>({expr_ctx, row_arg}), input_name.c_str()));
  }

  AggFnEvaluator::AggregationOp agg_op = evaluator->agg_op();
  const ColumnType& dst_type = evaluator->intermediate_type();
  bool dst_is_int_or_float_or_bool = dst_type.IsIntegerType()
      || dst_type.IsFloatingPointType() || dst_type.IsBooleanType();
  bool dst_is_numeric_or_bool = dst_is_int_or_float_or_bool || dst_type.IsDecimalType();

  BasicBlock* ret_block = BasicBlock::Create(codegen->context(), "ret", *fn);

  // Emit the code to compute 'result' and set the NULL indicator if needed. First check
  // for special cases where we can emit a very simple instruction sequence, then fall
  // back to the general-purpose approach of calling the cross-compiled builtin UDA.
  CodegenAnyVal& src = input_vals[0];
  // 'dst_slot_ptr' points to the slot in the aggregate tuple to update.
  Value* dst_slot_ptr = builder.CreateStructGEP(
      NULL, agg_tuple_arg, slot_desc->llvm_field_idx(), "dst_slot_ptr");
  Value* result = NULL;
  Value* dst_value = builder.CreateLoad(dst_slot_ptr, "dst_val");
  if (agg_op == AggFnEvaluator::COUNT) {
    src.CodegenBranchIfNull(&builder, ret_block);
    if (evaluator->is_merge()) {
      result = builder.CreateAdd(dst_value, src.GetVal(), "count_sum");
    } else {
      result = builder.CreateAdd(
          dst_value, codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
    }
    DCHECK(!slot_desc->is_nullable());
  } else if ((agg_op == AggFnEvaluator::MIN || agg_op == AggFnEvaluator::MAX)
      && dst_is_numeric_or_bool) {
    bool is_min = agg_op == AggFnEvaluator::MIN;
    src.CodegenBranchIfNull(&builder, ret_block);
    Function* min_max_fn = codegen->CodegenMinMax(slot_desc->type(), is_min);
    Value* min_max_args[] = {dst_value, src.GetVal()};
    result =
        builder.CreateCall(min_max_fn, min_max_args, is_min ? "min_value" : "max_value");
    // Dst may have been NULL, make sure to unset the NULL bit.
    DCHECK(slot_desc->is_nullable());
    slot_desc->CodegenSetNullIndicator(
        codegen, &builder, agg_tuple_arg, codegen->false_value());
  } else if (agg_op == AggFnEvaluator::SUM && dst_is_int_or_float_or_bool) {
    src.CodegenBranchIfNull(&builder, ret_block);
    if (dst_type.IsFloatingPointType()) {
      result = builder.CreateFAdd(dst_value, src.GetVal());
    } else {
      result = builder.CreateAdd(dst_value, src.GetVal());
    }
    // Dst may have been NULL, make sure to unset the NULL bit.
    DCHECK(slot_desc->is_nullable());
    slot_desc->CodegenSetNullIndicator(
        codegen, &builder, agg_tuple_arg, codegen->false_value());
  } else {
    // The remaining cases are implemented using the UDA interface.
    // Create intermediate argument 'dst' from 'dst_value'
    CodegenAnyVal dst = CodegenAnyVal::GetNonNullVal(codegen, &builder, dst_type, "dst");

    // For a subset of builtins we generate a different code sequence that exploits two
    // properties of the builtins. First, NULL input values can be skipped. Second, the
    // value of the slot was initialized in the right way in InitAggSlots() (e.g. 0 for
    // SUM) that we get the right result if UpdateSlot() pretends that the NULL bit of
    // 'dst' is unset. Empirically this optimisation makes TPC-H Q1 5-10% faster.
    bool special_null_handling = !evaluator->intermediate_type().IsStringType()
        && !evaluator->intermediate_type().IsTimestampType()
        && (agg_op == AggFnEvaluator::MIN || agg_op == AggFnEvaluator::MAX
               || agg_op == AggFnEvaluator::SUM || agg_op == AggFnEvaluator::AVG
               || agg_op == AggFnEvaluator::NDV);
    if (slot_desc->is_nullable()) {
      if (special_null_handling) {
        src.CodegenBranchIfNull(&builder, ret_block);
        slot_desc->CodegenSetNullIndicator(
            codegen, &builder, agg_tuple_arg, codegen->false_value());
      } else {
        dst.SetIsNull(slot_desc->CodegenIsNull(codegen, &builder, agg_tuple_arg));
      }
    }
    dst.SetFromRawValue(dst_value);

    // Call the UDA to update/merge 'src' into 'dst', with the result stored in
    // 'updated_dst_val'.
    CodegenAnyVal updated_dst_val;
    RETURN_IF_ERROR(CodegenCallUda(codegen, &builder, evaluator, agg_fn_ctx_arg,
        input_vals, dst, &updated_dst_val));
    result = updated_dst_val.ToNativeValue();

    if (slot_desc->is_nullable() && !special_null_handling) {
      // Set NULL bit in the slot based on the return value.
      Value* result_is_null = updated_dst_val.GetIsNull("result_is_null");
      slot_desc->CodegenSetNullIndicator(
          codegen, &builder, agg_tuple_arg, result_is_null);
    }
  }

  // TODO: Store to register in the loop and store once to memory at the end of the loop.
  builder.CreateStore(result, dst_slot_ptr);
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  // Avoid producing huge UpdateTuple() function after inlining - LLVM's optimiser
  // memory/CPU usage scales super-linearly with function size.
  // E.g. compute stats on all columns of a 1000-column table previously took 4 minutes to
  // codegen because all the UpdateSlot() functions were inlined.
  if (evaluator_idx >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("PartitionedAggregationNode::CodegenUpdateSlot(): codegen'd "
                  "UpdateSlot() function failed verification, see log");
  }
  return Status::OK();
}

Status PartitionedAggregationNode::CodegenCallUda(LlvmCodeGen* codegen,
    LlvmBuilder* builder, AggFnEvaluator* evaluator, Value* agg_fn_ctx_arg,
    const vector<CodegenAnyVal>& input_vals, const CodegenAnyVal& dst,
    CodegenAnyVal* updated_dst_val) {
  DCHECK_EQ(evaluator->input_expr_ctxs().size(), input_vals.size());
  Function* uda_fn;
  RETURN_IF_ERROR(evaluator->GetUpdateOrMergeFunction(codegen, &uda_fn));

  // Set up arguments for call to UDA, which are the FunctionContext*, followed by
  // pointers to all input values, followed by a pointer to the destination value.
  vector<Value*> uda_fn_args;
  uda_fn_args.push_back(agg_fn_ctx_arg);

  // Create pointers to input args to pass to uda_fn. We must use the unlowered type,
  // e.g. IntVal, because the UDA interface expects the values to be passed as const
  // references to the classes.
  for (int i = 0; i < evaluator->input_expr_ctxs().size(); ++i) {
    uda_fn_args.push_back(input_vals[i].GetUnloweredPtr("input_unlowered_ptr"));
  }

  // Create pointer to dst to pass to uda_fn. We must use the unlowered type for the
  // same reason as above.
  Value* dst_lowered_ptr = dst.GetLoweredPtr("dst_lowered_ptr");
  const ColumnType& dst_type = evaluator->intermediate_type();
  Type* dst_unlowered_ptr_type = CodegenAnyVal::GetUnloweredPtrType(codegen, dst_type);
  Value* dst_unlowered_ptr = builder->CreateBitCast(
      dst_lowered_ptr, dst_unlowered_ptr_type, "dst_unlowered_ptr");
  uda_fn_args.push_back(dst_unlowered_ptr);

  // Call 'uda_fn'
  builder->CreateCall(uda_fn, uda_fn_args);

  // Convert intermediate 'dst_arg' back to the native type.
  Value* anyval_result = builder->CreateLoad(dst_lowered_ptr, "anyval_result");

  *updated_dst_val = CodegenAnyVal(codegen, builder, dst_type, anyval_result);
  return Status::OK();
}

// IR codegen for the UpdateTuple loop.  This loop is query specific and based on the
// aggregate functions.  The function signature must match the non- codegen'd UpdateTuple
// exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// ; Function Attrs: alwaysinline
// define void @UpdateTuple(%"class.impala::PartitionedAggregationNode"* %this_ptr,
//      %"class.impala_udf::FunctionContext"** %agg_fn_ctxs, %"class.impala::Tuple"*
//      %tuple,
//      %"class.impala::TupleRow"* %row, i1 %is_merge) #34 {
// entry:
//   %tuple1 =
//      bitcast %"class.impala::Tuple"* %tuple to { i8, [7 x i8], i64, i64, double }*
//   %src_slot = getelementptr inbounds { i8, [7 x i8], i64, i64, double },
//      { i8, [7 x i8], i64, i64, double }* %tuple1, i32 0, i32 2
//   %count_star_val = load i64, i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   %0 = getelementptr %"class.impala_udf::FunctionContext"*,
//      %"class.impala_udf::FunctionContext"** %agg_fn_ctxs, i32 1
//   %agg_fn_ctx = load %"class.impala_udf::FunctionContext"*,
//      %"class.impala_udf::FunctionContext"** %0
//   %1 = call %"class.impala::ExprContext"**
//      @_ZNK6impala26PartitionedAggregationNode18GetAggExprContextsEi(
//      %"class.impala::PartitionedAggregationNode"* %this_ptr, i32 1)
//   call void @UpdateSlot(%"class.impala_udf::FunctionContext"* %agg_fn_ctx,
//      %"class.impala::ExprContext"** %1, { i8, [7 x i8], i64, i64, double }* %tuple1,
//      %"class.impala::TupleRow"* %row)
//   %2 = getelementptr %"class.impala_udf::FunctionContext"*,
//      %"class.impala_udf::FunctionContext"** %agg_fn_ctxs, i32 2
//   %agg_fn_ctx2 = load %"class.impala_udf::FunctionContext"*,
//      %"class.impala_udf::FunctionContext"** %2
//   %3 = call %"class.impala::ExprContext"**
//      @_ZNK6impala26PartitionedAggregationNode18GetAggExprContextsEi(
//      %"class.impala::PartitionedAggregationNode"* %this_ptr, i32 2)
//   call void @UpdateSlot.4(%"class.impala_udf::FunctionContext"* %agg_fn_ctx2,
//      %"class.impala::ExprContext"** %3, { i8, [7 x i8], i64, i64, double }* %tuple1,
//      %"class.impala::TupleRow"* %row)
//   ret void
// }
Status PartitionedAggregationNode::CodegenUpdateTuple(
    LlvmCodeGen* codegen, Function** fn) {
  SCOPED_TIMER(codegen->codegen_timer());

  for (const SlotDescriptor* slot_desc : intermediate_tuple_desc_->slots()) {
    if (slot_desc->type().type == TYPE_CHAR) {
      return Status("PartitionedAggregationNode::CodegenUpdateTuple(): cannot codegen"
                    "CHAR in aggregations");
    }
  }

  if (intermediate_tuple_desc_->GetLlvmStruct(codegen) == NULL) {
    return Status("PartitionedAggregationNode::CodegenUpdateTuple(): failed to generate "
                  "intermediate tuple desc");
  }

  // Get the types to match the UpdateTuple signature
  Type* agg_node_type = codegen->GetType(PartitionedAggregationNode::LLVM_CLASS_NAME);
  Type* fn_ctx_type = codegen->GetType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  Type* tuple_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);

  PointerType* agg_node_ptr_type = codegen->GetPtrType(agg_node_type);
  PointerType* fn_ctx_ptr_ptr_type = codegen->GetPtrPtrType(fn_ctx_type);
  PointerType* tuple_ptr_type = codegen->GetPtrType(tuple_type);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(tuple_row_type);

  StructType* tuple_struct = intermediate_tuple_desc_->GetLlvmStruct(codegen);
  PointerType* tuple_ptr = codegen->GetPtrType(tuple_struct);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_fn_ctxs", fn_ctx_ptr_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("is_merge", codegen->boolean_type()));

  LlvmBuilder builder(codegen->context());
  Value* args[5];
  *fn = prototype.GeneratePrototype(&builder, &args[0]);
  Value* this_arg = args[0];
  Value* agg_fn_ctxs_arg = args[1];
  Value* tuple_arg = args[2];
  Value* row_arg = args[3];

  // Cast the parameter types to the internal llvm runtime types.
  // TODO: get rid of this by using right type in function signature
  tuple_arg = builder.CreateBitCast(tuple_arg, tuple_ptr, "tuple");

  Function* get_expr_ctxs_fn =
      codegen->GetFunction(IRFunction::PART_AGG_NODE_GET_EXPR_CTXS, false);
  DCHECK(get_expr_ctxs_fn != NULL);

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  int j = grouping_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    if (evaluator->is_count_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      int field_idx = slot_desc->llvm_field_idx();
      Value* const_one = codegen->GetIntConstant(TYPE_BIGINT, 1);
      Value* slot_ptr = builder.CreateStructGEP(NULL, tuple_arg, field_idx, "src_slot");
      Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
      Function* update_slot_fn;
      RETURN_IF_ERROR(
          CodegenUpdateSlot(codegen, evaluator, i, slot_desc, &update_slot_fn));
      Value* agg_fn_ctx_ptr = builder.CreateConstGEP1_32(agg_fn_ctxs_arg, i);
      Value* agg_fn_ctx = builder.CreateLoad(agg_fn_ctx_ptr, "agg_fn_ctx");
      // Call GetExprCtx() to get the expression context.
      DCHECK(agg_expr_ctxs_[i] != NULL);
      Value* get_expr_ctxs_args[] = {this_arg, codegen->GetIntConstant(TYPE_INT, i)};
      Value* agg_expr_ctxs = builder.CreateCall(get_expr_ctxs_fn, get_expr_ctxs_args);
      Value* update_slot_args[] = {agg_fn_ctx, agg_expr_ctxs, tuple_arg, row_arg};
      builder.CreateCall(update_slot_fn, update_slot_args);
    }
  }
  builder.CreateRetVoid();

  // Avoid inlining big UpdateTuple function into outer loop - we're unlikely to get
  // any benefit from it since the function call overhead will be amortized.
  if (aggregate_evaluators_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  // CodegenProcessBatch() does the final optimizations.
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("PartitionedAggregationNode::CodegenUpdateTuple(): codegen'd "
                  "UpdateTuple() function failed verification, see log");
  }
  return Status::OK();
}

Status PartitionedAggregationNode::CodegenProcessBatch(LlvmCodeGen* codegen,
    TPrefetchMode::type prefetch_mode) {
  SCOPED_TIMER(codegen->codegen_timer());

  Function* update_tuple_fn;
  RETURN_IF_ERROR(CodegenUpdateTuple(codegen, &update_tuple_fn));

  // Get the cross compiled update row batch function
  IRFunction::Type ir_fn = (!grouping_expr_ctxs_.empty() ?
      IRFunction::PART_AGG_NODE_PROCESS_BATCH_UNAGGREGATED :
      IRFunction::PART_AGG_NODE_PROCESS_BATCH_NO_GROUPING);
  Function* process_batch_fn = codegen->GetFunction(ir_fn, true);
  DCHECK(process_batch_fn != NULL);

  int replaced;
  if (!grouping_expr_ctxs_.empty()) {
    // Codegen for grouping using hash table

    // Replace prefetch_mode with constant so branches can be optimised out.
    Value* prefetch_mode_arg = codegen->GetArgument(process_batch_fn, 3);
    prefetch_mode_arg->replaceAllUsesWith(
        ConstantInt::get(Type::getInt32Ty(codegen->context()), prefetch_mode));

    // The codegen'd ProcessBatch function is only used in Open() with level_ = 0,
    // so don't use murmur hash
    Function* hash_fn;
    RETURN_IF_ERROR(ht_ctx_->CodegenHashRow(codegen, /* use murmur */ false, &hash_fn));

    // Codegen HashTable::Equals<true>
    Function* build_equals_fn;
    RETURN_IF_ERROR(ht_ctx_->CodegenEquals(codegen, true, &build_equals_fn));

    // Codegen for evaluating input rows
    Function* eval_grouping_expr_fn;
    RETURN_IF_ERROR(ht_ctx_->CodegenEvalRow(codegen, false, &eval_grouping_expr_fn));

    // Replace call sites
    replaced = codegen->ReplaceCallSites(process_batch_fn, eval_grouping_expr_fn,
        "EvalProbeRow");
    DCHECK_EQ(replaced, 1);

    replaced = codegen->ReplaceCallSites(process_batch_fn, hash_fn, "HashRow");
    DCHECK_EQ(replaced, 1);

    replaced = codegen->ReplaceCallSites(process_batch_fn, build_equals_fn, "Equals");
    DCHECK_EQ(replaced, 1);

    HashTableCtx::HashTableReplacedConstants replaced_constants;
    const bool stores_duplicates = false;
    RETURN_IF_ERROR(ht_ctx_->ReplaceHashTableConstants(codegen, stores_duplicates, 1,
        process_batch_fn, &replaced_constants));
    DCHECK_GE(replaced_constants.stores_nulls, 1);
    DCHECK_GE(replaced_constants.finds_some_nulls, 1);
    DCHECK_GE(replaced_constants.stores_duplicates, 1);
    DCHECK_GE(replaced_constants.stores_tuples, 1);
    DCHECK_GE(replaced_constants.quadratic_probing, 1);
  }

  replaced = codegen->ReplaceCallSites(process_batch_fn, update_tuple_fn, "UpdateTuple");
  DCHECK_GE(replaced, 1);
  process_batch_fn = codegen->FinalizeFunction(process_batch_fn);
  if (process_batch_fn == NULL) {
    return Status("PartitionedAggregationNode::CodegenProcessBatch(): codegen'd "
        "ProcessBatch() function failed verification, see log");
  }

  void **codegened_fn_ptr = grouping_expr_ctxs_.empty() ?
      reinterpret_cast<void**>(&process_batch_no_grouping_fn_) :
      reinterpret_cast<void**>(&process_batch_fn_);
  codegen->AddFunctionToJit(process_batch_fn, codegened_fn_ptr);
  return Status::OK();
}

Status PartitionedAggregationNode::CodegenProcessBatchStreaming(
    LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) {
  DCHECK(is_streaming_preagg_);
  SCOPED_TIMER(codegen->codegen_timer());

  IRFunction::Type ir_fn = IRFunction::PART_AGG_NODE_PROCESS_BATCH_STREAMING;
  Function* process_batch_streaming_fn = codegen->GetFunction(ir_fn, true);
  DCHECK(process_batch_streaming_fn != NULL);

  // Make needs_serialize arg constant so dead code can be optimised out.
  Value* needs_serialize_arg = codegen->GetArgument(process_batch_streaming_fn, 2);
  needs_serialize_arg->replaceAllUsesWith(
      ConstantInt::get(Type::getInt1Ty(codegen->context()), needs_serialize_));

  // Replace prefetch_mode with constant so branches can be optimised out.
  Value* prefetch_mode_arg = codegen->GetArgument(process_batch_streaming_fn, 3);
  prefetch_mode_arg->replaceAllUsesWith(
      ConstantInt::get(Type::getInt32Ty(codegen->context()), prefetch_mode));

  Function* update_tuple_fn;
  RETURN_IF_ERROR(CodegenUpdateTuple(codegen, &update_tuple_fn));

  // We only use the top-level hash function for streaming aggregations.
  Function* hash_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenHashRow(codegen, false, &hash_fn));

  // Codegen HashTable::Equals
  Function* equals_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenEquals(codegen, true, &equals_fn));

  // Codegen for evaluating input rows
  Function* eval_grouping_expr_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenEvalRow(codegen, false, &eval_grouping_expr_fn));

  // Replace call sites
  int replaced = codegen->ReplaceCallSites(process_batch_streaming_fn, update_tuple_fn,
      "UpdateTuple");
  DCHECK_EQ(replaced, 2);

  replaced = codegen->ReplaceCallSites(process_batch_streaming_fn, eval_grouping_expr_fn,
      "EvalProbeRow");
  DCHECK_EQ(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_batch_streaming_fn, hash_fn, "HashRow");
  DCHECK_EQ(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_batch_streaming_fn, equals_fn, "Equals");
  DCHECK_EQ(replaced, 1);

  HashTableCtx::HashTableReplacedConstants replaced_constants;
  const bool stores_duplicates = false;
  RETURN_IF_ERROR(ht_ctx_->ReplaceHashTableConstants(codegen, stores_duplicates, 1,
      process_batch_streaming_fn, &replaced_constants));
  DCHECK_GE(replaced_constants.stores_nulls, 1);
  DCHECK_GE(replaced_constants.finds_some_nulls, 1);
  DCHECK_GE(replaced_constants.stores_duplicates, 1);
  DCHECK_GE(replaced_constants.stores_tuples, 1);
  DCHECK_GE(replaced_constants.quadratic_probing, 1);

  DCHECK(process_batch_streaming_fn != NULL);
  process_batch_streaming_fn = codegen->FinalizeFunction(process_batch_streaming_fn);
  if (process_batch_streaming_fn == NULL) {
    return Status("PartitionedAggregationNode::CodegenProcessBatchStreaming(): codegen'd "
        "ProcessBatchStreaming() function failed verification, see log");
  }

  codegen->AddFunctionToJit(process_batch_streaming_fn,
      reinterpret_cast<void**>(&process_batch_streaming_fn_));
  return Status::OK();
}

}
