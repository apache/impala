// Copyright 2012 Cloudera Inc.
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

#include "exec/partitioned-aggregation-node.h"

#include <math.h>
#include <sstream>
#include <gutil/strings/substitute.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;
using namespace strings;

namespace impala {

const char* PartitionedAggregationNode::LLVM_CLASS_NAME =
    "class.impala::PartitionedAggregationNode";

PartitionedAggregationNode::PartitionedAggregationNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_tuple_id_(tnode.agg_node.intermediate_tuple_id),
    intermediate_tuple_desc_(NULL),
    output_tuple_id_(tnode.agg_node.output_tuple_id),
    output_tuple_desc_(NULL),
    needs_finalize_(tnode.agg_node.need_finalize),
    needs_serialize_(false),
    block_mgr_client_(NULL),
    output_partition_(NULL),
    process_row_batch_fn_(NULL),
    build_timer_(NULL),
    ht_resize_timer_(NULL),
    get_results_timer_(NULL),
    num_hash_buckets_(NULL),
    partitions_created_(NULL),
    max_partition_level_(NULL),
    num_row_repartitioned_(NULL),
    num_repartitions_(NULL),
    singleton_output_tuple_(NULL),
    singleton_output_tuple_returned_(true),
    partition_pool_(new ObjectPool()) {
  DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
}

Status PartitionedAggregationNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.agg_node.grouping_exprs, &probe_expr_ctxs_));
  for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
        pool_, tnode.agg_node.aggregate_functions[i], &evaluator));
    aggregate_evaluators_.push_back(evaluator);
  }
  return Status::OK();
}

Status PartitionedAggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // Create the codegen object before preparing conjunct_ctxs_ and children_, so that any
  // ScalarFnCalls will use codegen.
  // TODO: this is brittle and hard to reason about, revisit
  if (state->codegen_enabled()) {
    LlvmCodeGen* codegen;
    RETURN_IF_ERROR(state->GetCodegen(&codegen));
  }

  RETURN_IF_ERROR(ExecNode::Prepare(state));
  state_ = state;

  mem_pool_.reset(new MemPool(mem_tracker()));
  agg_fn_pool_.reset(new MemPool(expr_mem_tracker()));

  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  ht_resize_timer_ = ADD_TIMER(runtime_profile(), "HTResizeTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  num_hash_buckets_ =
      ADD_COUNTER(runtime_profile(), "HashBuckets", TUnit::UNIT);
  partitions_created_ =
      ADD_COUNTER(runtime_profile(), "PartitionsCreated", TUnit::UNIT);
  max_partition_level_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxPartitionLevel", TUnit::UNIT);
  num_row_repartitioned_ =
      ADD_COUNTER(runtime_profile(), "RowsRepartitioned", TUnit::UNIT);
  num_repartitions_ =
      ADD_COUNTER(runtime_profile(), "NumRepartitions", TUnit::UNIT);
  num_spilled_partitions_ =
      ADD_COUNTER(runtime_profile(), "SpilledPartitions", TUnit::UNIT);
  largest_partition_percent_ = runtime_profile()->AddHighWaterMarkCounter(
      "LargestPartitionPercent", TUnit::UNIT);

  intermediate_tuple_desc_ =
      state->desc_tbl().GetTupleDescriptor(intermediate_tuple_id_);
  output_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(output_tuple_id_);
  DCHECK_EQ(intermediate_tuple_desc_->slots().size(),
        output_tuple_desc_->slots().size());

  RETURN_IF_ERROR(
      Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc(), expr_mem_tracker()));
  AddExprCtxsToFree(probe_expr_ctxs_);

  contains_var_len_grouping_exprs_ = false;

  // Construct build exprs from intermediate_agg_tuple_desc_
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
    SlotDescriptor* desc = intermediate_tuple_desc_->slots()[i];
    DCHECK(desc->type().type == TYPE_NULL ||
        desc->type() == probe_expr_ctxs_[i]->root()->type());
    // Hack to avoid TYPE_NULL SlotRefs.
    Expr* expr = desc->type().type != TYPE_NULL ?
        new SlotRef(desc) : new SlotRef(desc, TYPE_BOOLEAN);
    state->obj_pool()->Add(expr);
    build_expr_ctxs_.push_back(new ExprContext(expr));
    state->obj_pool()->Add(build_expr_ctxs_.back());
    contains_var_len_grouping_exprs_ |= expr->type().IsVarLenStringType();
  }
  // Construct a new row desc for preparing the build exprs because neither the child's
  // nor this node's output row desc may contain the intermediate tuple, e.g.,
  // in a single-node plan with an intermediate tuple different from the output tuple.
  intermediate_row_desc_.reset(new RowDescriptor(intermediate_tuple_desc_, false));
  RETURN_IF_ERROR(
      Expr::Prepare(build_expr_ctxs_, state, *intermediate_row_desc_,
                    expr_mem_tracker()));
  AddExprCtxsToFree(build_expr_ctxs_);

  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // Skip non-materialized slots; we don't have evaluators instantiated for those.
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1)
                << "#eval= " << aggregate_evaluators_.size()
                << " #probe=" << probe_expr_ctxs_.size();
      ++j;
    }
    SlotDescriptor* intermediate_slot_desc = intermediate_tuple_desc_->slots()[j];
    SlotDescriptor* output_slot_desc = output_tuple_desc_->slots()[j];
    FunctionContext* agg_fn_ctx = NULL;
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_slot_desc, output_slot_desc, agg_fn_pool_.get(), &agg_fn_ctx));
    agg_fn_ctxs_.push_back(agg_fn_ctx);
    state->obj_pool()->Add(agg_fn_ctx);
    needs_serialize_ |= aggregate_evaluators_[i]->SupportsSerialize();
  }

  if (probe_expr_ctxs_.empty()) {
    // Create single output tuple now; we need to output something
    // even if our input is empty.
    singleton_output_tuple_ =
        ConstructIntermediateTuple(agg_fn_ctxs_, mem_pool_.get(), NULL, NULL);
    singleton_output_tuple_returned_ = false;
  } else {
    ht_ctx_.reset(new HashTableCtx(build_expr_ctxs_, probe_expr_ctxs_, true, true,
        state->fragment_hash_seed(), MAX_PARTITION_DEPTH, 1));
    RETURN_IF_ERROR(state_->block_mgr()->RegisterClient(
        MinRequiredBuffers(), mem_tracker(), state, &block_mgr_client_));
    RETURN_IF_ERROR(CreateHashPartitions(0));
  }

  // TODO: Is there a need to create the stream here? If memory reservations work we may
  // be able to create this stream lazily and only whenever we need to spill.
  if (needs_serialize_ && block_mgr_client_ != NULL) {
    serialize_stream_.reset(new BufferedTupleStream(state, *intermediate_row_desc_,
        state->block_mgr(), block_mgr_client_,
        false, /* use initial small buffers */
        true  /* delete on read */));
    RETURN_IF_ERROR(serialize_stream_->Init(id(), runtime_profile(), false));
    DCHECK(serialize_stream_->has_write_block());
  }

  if (state->codegen_enabled()) {
    LlvmCodeGen* codegen;
    RETURN_IF_ERROR(state->GetCodegen(&codegen));
    Function* codegen_process_row_batch_fn = CodegenProcessBatch();
    if (codegen_process_row_batch_fn != NULL) {
      codegen->AddFunctionToJit(codegen_process_row_batch_fn,
          reinterpret_cast<void**>(&process_row_batch_fn_));
      AddRuntimeExecOption("Codegen Enabled");
    }
  }
  return Status::OK();
}

Status PartitionedAggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(Expr::Open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));

  DCHECK_EQ(aggregate_evaluators_.size(), agg_fn_ctxs_.size());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Open(state, agg_fn_ctxs_[i]));
  }

  // Read all the rows from the child and process them.
  RETURN_IF_ERROR(children_[0]->Open(state));
  RowBatch batch(children_[0]->row_desc(), state->batch_size(), mem_tracker());
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

    SCOPED_TIMER(build_timer_);
    if (process_row_batch_fn_ != NULL) {
      RETURN_IF_ERROR(process_row_batch_fn_(this, &batch, ht_ctx_.get()));
    } else if (probe_expr_ctxs_.empty()) {
      RETURN_IF_ERROR(ProcessBatchNoGrouping(&batch));
    } else {
      // There is grouping, so we will do partitioned aggregation.
      RETURN_IF_ERROR(ProcessBatch<false>(&batch, ht_ctx_.get()));
    }
    batch.Reset();
  } while (!eos);

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point. We have consumed all of the input
  // from the child and transfered ownership of the resources we need.
  if (!IsInSubplan()) child(0)->Close(state);

  // Done consuming child(0)'s input. Move all the partitions in hash_partitions_
  // to spilled_partitions_/aggregated_partitions_. We'll finish the processing in
  // GetNext().
  if (!probe_expr_ctxs_.empty()) {
    RETURN_IF_ERROR(MoveHashPartitions(child(0)->rows_returned()));
  }
  return Status::OK();
}

Status PartitionedAggregationNode::GetNext(RuntimeState* state,
    RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();
  if (probe_expr_ctxs_.empty()) {
    // There was grouping, so evaluate the conjuncts and return the single result row.
    // We allow calling GetNext() after eos, so don't return this row again.
    if (!singleton_output_tuple_returned_) {
      int row_idx = row_batch->AddRow();
      TupleRow* row = row_batch->GetRow(row_idx);
      Tuple* output_tuple = GetOutputTuple(
          agg_fn_ctxs_, singleton_output_tuple_, row_batch->tuple_data_pool());
      row->SetTuple(0, output_tuple);
      if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
        row_batch->CommitLastRow();
        ++num_rows_returned_;
      }
      singleton_output_tuple_returned_ = true;
    }
    // Keep the current chunk to amortize the memory allocation over a series
    // of Reset()/Open()/GetNext()* calls.
    row_batch->tuple_data_pool()->AcquireData(mem_pool_.get(), true);
    *eos = true;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
    return Status::OK();
  }

  if (output_iterator_.AtEnd()) {
    // Done with this partition, move onto the next one.
    if (output_partition_ != NULL) {
      output_partition_->Close(false);
      output_partition_ = NULL;
    }
    if (aggregated_partitions_.empty() && spilled_partitions_.empty()) {
      // No more partitions, all done.
      *eos = true;
      return Status::OK();
    }
    // Process next partition.
    RETURN_IF_ERROR(NextPartition());
    DCHECK(output_partition_ != NULL);
  }

  SCOPED_TIMER(get_results_timer_);
  int count = 0;
  const int N = BitUtil::NextPowerOfTwo(state->batch_size());
  // Keeping returning rows from the current partition.
  while (!output_iterator_.AtEnd() && !row_batch->AtCapacity()) {
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
    if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) break; // TODO: remove this check? is this expensive?
    }
  }
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  *eos = ReachedLimit();
  if (output_iterator_.AtEnd()) row_batch->MarkNeedToReturn();
  return Status::OK();
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
  if (probe_expr_ctxs_.empty()) {
    // Re-create the single output tuple for this non-grouping agg.
    singleton_output_tuple_ =
        ConstructIntermediateTuple(agg_fn_ctxs_, mem_pool_.get(), NULL, NULL);
    singleton_output_tuple_returned_ = false;
  } else {
    // Reset the HT and the partitions for this grouping agg.
    ht_ctx_->set_level(0);
    ClosePartitions();
    CreateHashPartitions(0);
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

  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    aggregate_evaluators_[i]->Close(state);
  }
  for (int i = 0; i < agg_fn_ctxs_.size(); ++i) {
    agg_fn_ctxs_[i]->impl()->Close();
  }
  if (agg_fn_pool_.get() != NULL) agg_fn_pool_->FreeAll();
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  if (ht_ctx_.get() != NULL) ht_ctx_->Close();
  if (serialize_stream_.get() != NULL) serialize_stream_->Close();

  if (block_mgr_client_ != NULL) {
    state->block_mgr()->ClearReservations(block_mgr_client_);
  }

  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(build_expr_ctxs_, state);
  ExecNode::Close(state);
}

Status PartitionedAggregationNode::Partition::InitStreams() {
  agg_fn_pool.reset(new MemPool(parent->expr_mem_tracker()));
  DCHECK_EQ(agg_fn_ctxs.size(), 0);
  for (int i = 0; i < parent->agg_fn_ctxs_.size(); ++i) {
    agg_fn_ctxs.push_back(parent->agg_fn_ctxs_[i]->impl()->Clone(agg_fn_pool.get()));
    parent->partition_pool_->Add(agg_fn_ctxs[i]);
  }

  aggregated_row_stream.reset(new BufferedTupleStream(parent->state_,
      *parent->intermediate_row_desc_, parent->state_->block_mgr(),
      parent->block_mgr_client_,
      true, /* use small buffers */
      false /* delete on read */));
  RETURN_IF_ERROR(
      aggregated_row_stream->Init(parent->id(), parent->runtime_profile(), true));

  unaggregated_row_stream.reset(new BufferedTupleStream(parent->state_,
      parent->child(0)->row_desc(), parent->state_->block_mgr(),
      parent->block_mgr_client_,
      true, /* use small buffers */
      true /* delete on read */));
  // This stream is only used to spill, no need to ever have this pinned.
  RETURN_IF_ERROR(unaggregated_row_stream->Init(parent->id(), parent->runtime_profile(),
      false));
  DCHECK(unaggregated_row_stream->has_write_block());
  return Status::OK();
}

bool PartitionedAggregationNode::Partition::InitHashTable() {
  DCHECK(hash_tbl.get() == NULL);
  // We use the upper PARTITION_FANOUT num bits to pick the partition so only the
  // remaining bits can be used for the hash table.
  // TODO: how many buckets?
  // TODO: we could switch to 64 bit hashes and then we don't need a max size.
  // It might be reasonable to limit individual hash table size for other reasons
  // though. Always start with small buffers.
  hash_tbl.reset(new HashTable(parent->state_, parent->block_mgr_client_, 1, NULL,
      1 << (32 - NUM_PARTITIONING_BITS)));
  return hash_tbl->Init();
}

Status PartitionedAggregationNode::Partition::CleanUp() {
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
      aggregated_row_stream->Close();
      RETURN_IF_ERROR(status);
      return parent->state_->block_mgr()->MemLimitTooLowError(parent->block_mgr_client_,
          parent->id());
    }
    DCHECK(status.ok());

    aggregated_row_stream->Close();
    aggregated_row_stream.swap(parent->serialize_stream_);
    // Recreate the serialize_stream (and reserve 1 buffer) now in preparation for
    // when we need to spill again. We need to have this available before we need
    // to spill to make sure it is available. This should be acquirable since we just
    // freed at least one buffer from this partition's (old) aggregated_row_stream.
    parent->serialize_stream_.reset(new BufferedTupleStream(parent->state_,
        *parent->intermediate_row_desc_, parent->state_->block_mgr(),
        parent->block_mgr_client_,
        false, /* use small buffers */
        true   /* delete on read */));
    status = parent->serialize_stream_->Init(parent->id(), parent->runtime_profile(),
        false);
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

  RETURN_IF_ERROR(CleanUp());

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
  // Unpin the stream as soon as possible to increase the changes that the
  // SwitchToIoBuffers() call below will succeed.
  DCHECK(!got_buffer || aggregated_row_stream->has_write_block())
      << aggregated_row_stream->DebugString();
  RETURN_IF_ERROR(aggregated_row_stream->UnpinStream(false));

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
    parent->AddRuntimeExecOption("Spilled");
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
    aggregated_row_stream->Close();
  }
  if (hash_tbl.get() != NULL) hash_tbl->Close();
  if (unaggregated_row_stream.get() != NULL) unaggregated_row_stream->Close();

  for (int i = 0; i < agg_fn_ctxs.size(); ++i) {
    agg_fn_ctxs[i]->impl()->Close();
  }
  if (agg_fn_pool.get() != NULL) agg_fn_pool->FreeAll();
}

Tuple* PartitionedAggregationNode::ConstructIntermediateTuple(
    const vector<FunctionContext*>& agg_fn_ctxs, MemPool* pool,
    BufferedTupleStream* stream, Status* status) {
  Tuple* intermediate_tuple = NULL;
  uint8_t* buffer = NULL;
  if (pool != NULL) {
    DCHECK(stream == NULL && status == NULL);
    intermediate_tuple = Tuple::Create(intermediate_tuple_desc_->byte_size(), pool);
  } else {
    DCHECK(stream != NULL && status != NULL);
    // Figure out how big it will be to copy the entire tuple. We need the tuple to end
    // up in one block in the stream.
    int size = intermediate_tuple_desc_->byte_size();
    if (contains_var_len_grouping_exprs_) {
      // TODO: This is likely to be too slow. The hash table could maintain this as
      // it hashes.
      for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
        if (!probe_expr_ctxs_[i]->root()->type().IsVarLenStringType()) continue;
        if (ht_ctx_->last_expr_value_null(i)) continue;
        StringValue* sv = reinterpret_cast<StringValue*>(ht_ctx_->last_expr_value(i));
        size += sv->len;
      }
    }

    // Now that we know the size of the row, allocate space for it in the stream.
    buffer = stream->AllocateRow(size, status);
    if (buffer == NULL) {
      if (!status->ok() || !stream->using_small_buffers()) return NULL;
      // IMPALA-2352: Make a best effort to switch to IO buffers and re-allocate.
      // If SwitchToIoBuffers() fails the caller of this function can try to free
      // some space, e.g. through spilling, and re-attempt to allocate space for
      // this row.
      bool got_buffer;
      *status = stream->SwitchToIoBuffers(&got_buffer);
      if (!status->ok() || !got_buffer) return NULL;
      buffer = stream->AllocateRow(size, status);
      if (buffer == NULL) return NULL;
    }
    intermediate_tuple = reinterpret_cast<Tuple*>(buffer);
    // TODO: remove this. we shouldn't need to zero the entire tuple.
    intermediate_tuple->Init(size);
    buffer += intermediate_tuple_desc_->byte_size();
  }

  // Copy grouping values.
  vector<SlotDescriptor*>::const_iterator slot_desc =
      intermediate_tuple_desc_->slots().begin();
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i, ++slot_desc) {
    if (ht_ctx_->last_expr_value_null(i)) {
      intermediate_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } else {
      void* src = ht_ctx_->last_expr_value(i);
      void* dst = intermediate_tuple->GetSlot((*slot_desc)->tuple_offset());
      if (stream == NULL) {
        RawValue::Write(src, dst, (*slot_desc)->type(), pool);
      } else {
        RawValue::Write(src, (*slot_desc)->type(), dst, &buffer);
      }
    }
  }

  // Initialize aggregate output.
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++slot_desc) {
    while (!(*slot_desc)->is_materialized()) ++slot_desc;
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    evaluator->Init(agg_fn_ctxs[i], intermediate_tuple);
    // Codegen specific path for min/max.
    // To minimize branching on the UpdateTuple path, initialize the result value
    // so that UpdateTuple doesn't have to check if the aggregation
    // dst slot is null.
    // TODO: remove when we don't use the irbuilder for codegen here.  This optimization
    // will no longer be necessary when all aggregates are implemented with the UDA
    // interface.
    if ((*slot_desc)->type().type != TYPE_STRING &&
        (*slot_desc)->type().type != TYPE_VARCHAR &&
        (*slot_desc)->type().type != TYPE_TIMESTAMP &&
        (*slot_desc)->type().type != TYPE_CHAR &&
        (*slot_desc)->type().type != TYPE_DECIMAL) {
      ExprValue default_value;
      void* default_value_ptr = NULL;
      switch (evaluator->agg_op()) {
        case AggFnEvaluator::MIN:
          default_value_ptr = default_value.SetToMax((*slot_desc)->type());
          RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
          break;
        case AggFnEvaluator::MAX:
          default_value_ptr = default_value.SetToMin((*slot_desc)->type());
          RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
          break;
        default:
          break;
      }
    }
  }
  return intermediate_tuple;
}

void PartitionedAggregationNode::UpdateTuple(FunctionContext** agg_fn_ctxs,
    Tuple* tuple, TupleRow* row, bool is_merge) {
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
    int num_grouping_slots = probe_expr_ctxs_.size();
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
       << " probe_exprs=" << Expr::DebugString(probe_expr_ctxs_)
       << " agg_exprs=" << AggFnEvaluator::DebugString(aggregate_evaluators_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status PartitionedAggregationNode::CreateHashPartitions(int level) {
  if (level >= MAX_PARTITION_DEPTH) {
    return state_->SetMemLimitExceeded(ErrorMsg(
        TErrorCode::PARTITIONED_AGG_MAX_PARTITION_DEPTH, id_, MAX_PARTITION_DEPTH));
  }
  ht_ctx_->set_level(level);

  DCHECK(hash_partitions_.empty());
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* new_partition = new Partition(this, level);
    DCHECK(new_partition != NULL);
    hash_partitions_.push_back(partition_pool_->Add(new_partition));
    RETURN_IF_ERROR(new_partition->InitStreams());
  }
  DCHECK_GT(state_->block_mgr()->num_reserved_buffers_remaining(block_mgr_client_), 0);

  // Now that all the streams are reserved (meaning we have enough memory to execute
  // the algorithm), allocate the hash tables. These can fail and we can still continue.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    if (!hash_partitions_[i]->InitHashTable()) {
      RETURN_IF_ERROR(hash_partitions_[i]->Spill());
    }
  }
  COUNTER_ADD(partitions_created_, PARTITION_FANOUT);
  COUNTER_SET(max_partition_level_, level);
  return Status::OK();
}

Status PartitionedAggregationNode::CheckAndResizeHashPartitions(int num_rows,
    HashTableCtx* ht_ctx) {
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* partition = hash_partitions_[i];
    while (!partition->is_spilled()) {
      {
        SCOPED_TIMER(ht_resize_timer_);
        if (partition->hash_tbl->CheckAndResize(num_rows, ht_ctx)) break;
      }
      // There was not enough memory for the resize. Spill a partition and retry.
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
      if (num_input_rows == largest_partition) {
        Status status = Status::MemLimitExceeded();
        status.AddDetail(Substitute("Cannot perform aggregation at node with id $0. "
            "Repartitioning did not reduce the size of a spilled partition. "
            "Repartitioning level $1. Number of rows $2.",
            id_, partition->level + 1, num_input_rows));
        state_->SetMemLimitExceeded();
        return status;
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
  if (input_stream->num_rows() > 0) {
    while (true) {
      bool got_buffer = false;
      RETURN_IF_ERROR(input_stream->PrepareForRead(&got_buffer));
      if (got_buffer) break;
      // Did not have a buffer to read the input stream. Spill and try again.
      RETURN_IF_ERROR(SpillPartition());
    }

    bool eos = false;
    RowBatch batch(AGGREGATED_ROWS ? *intermediate_row_desc_ : children_[0]->row_desc(),
                   state_->batch_size(), mem_tracker());
    do {
      RETURN_IF_ERROR(input_stream->GetNext(&batch, &eos));
      RETURN_IF_ERROR(ProcessBatch<AGGREGATED_ROWS>(&batch, ht_ctx_.get()));
      RETURN_IF_ERROR(state_->GetQueryStatus());
      FreeLocalAllocations();
      batch.Reset();
    } while (!eos);
  }
  input_stream->Close();
  return Status::OK();
}

Status PartitionedAggregationNode::SpillPartition() {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick the largest partition that is not spilled.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed) continue;
    if (hash_partitions_[i]->is_spilled()) continue;
    // TODO: In PHJ the bytes_in_mem() call also calculates the mem used by the
    // write_block_, why do we ignore it here?
    int64_t mem = hash_partitions_[i]->aggregated_row_stream->bytes_in_mem(true);
    mem += hash_partitions_[i]->hash_tbl->byte_size();
    mem += hash_partitions_[i]->agg_fn_pool->total_reserved_bytes();
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }
  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too low.
    return state_->block_mgr()->MemLimitTooLowError(block_mgr_client_, id());
  }

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
    int64_t unaggregated_rows = partition->unaggregated_row_stream->num_rows();
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
      RETURN_IF_ERROR(partition->aggregated_row_stream->UnpinStream(true));
      RETURN_IF_ERROR(partition->unaggregated_row_stream->UnpinStream(true));

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
// void UpdateSlot(FunctionContext* fn_ctx, AggTuple* agg_tuple, char** row)
//
// The IR for sum(double_col) is:
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         { i8, double }* %agg_tuple,
//                         %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %src = call { i8, double } @GetSlotRef(%"class.impala::ExprContext"* inttoptr
//     (i64 128241264 to %"class.impala::ExprContext"*), %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, double }* %agg_tuple)
//   %dst_val = load double* %dst_slot_ptr
//   %val = extractvalue { i8, double } %src, 1
//   %1 = fadd double %dst_val, %val
//   store double %1, double* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
//
// The IR for ndv(double_col) is:
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         { i8, %"struct.impala::StringValue" }* %agg_tuple,
//                         %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %dst_lowered_ptr = alloca { i64, i8* }
//   %src_lowered_ptr = alloca { i8, double }
//   %src = call { i8, double } @GetSlotRef(%"class.impala::ExprContext"* inttoptr
//     (i64 120530832 to %"class.impala::ExprContext"*), %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds
//     { i8, %"struct.impala::StringValue" }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, %"struct.impala::StringValue" }* %agg_tuple)
//   %dst_val = load %"struct.impala::StringValue"* %dst_slot_ptr
//   store { i8, double } %src, { i8, double }* %src_lowered_ptr
//   %src_unlowered_ptr = bitcast { i8, double }* %src_lowered_ptr
//                        to %"struct.impala_udf::DoubleVal"*
//   %ptr = extractvalue %"struct.impala::StringValue" %dst_val, 0
//   %dst_stringval = insertvalue { i64, i8* } zeroinitializer, i8* %ptr, 1
//   %len = extractvalue %"struct.impala::StringValue" %dst_val, 1
//   %1 = extractvalue { i64, i8* } %dst_stringval, 0
//   %2 = zext i32 %len to i64
//   %3 = shl i64 %2, 32
//   %4 = and i64 %1, 4294967295
//   %5 = or i64 %4, %3
//   %dst_stringval1 = insertvalue { i64, i8* } %dst_stringval, i64 %5, 0
//   store { i64, i8* } %dst_stringval1, { i64, i8* }* %dst_lowered_ptr
//   %dst_unlowered_ptr = bitcast { i64, i8* }* %dst_lowered_ptr
//                        to %"struct.impala_udf::StringVal"*
//   call void @HllUpdate(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                        %"struct.impala_udf::DoubleVal"* %src_unlowered_ptr,
//                        %"struct.impala_udf::StringVal"* %dst_unlowered_ptr)
//   %anyval_result = load { i64, i8* }* %dst_lowered_ptr
//   %6 = extractvalue { i64, i8* } %anyval_result, 1
//   %7 = insertvalue %"struct.impala::StringValue" zeroinitializer, i8* %6, 0
//   %8 = extractvalue { i64, i8* } %anyval_result, 0
//   %9 = ashr i64 %8, 32
//   %10 = trunc i64 %9 to i32
//   %11 = insertvalue %"struct.impala::StringValue" %7, i32 %10, 1
//   store %"struct.impala::StringValue" %11, %"struct.impala::StringValue"* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
llvm::Function* PartitionedAggregationNode::CodegenUpdateSlot(
    AggFnEvaluator* evaluator, SlotDescriptor* slot_desc) {
  DCHECK(slot_desc->is_materialized());
  LlvmCodeGen* codegen;
  if (!state_->GetCodegen(&codegen).ok()) return NULL;

  DCHECK_EQ(evaluator->input_expr_ctxs().size(), 1);
  ExprContext* input_expr_ctx = evaluator->input_expr_ctxs()[0];
  Expr* input_expr = input_expr_ctx->root();

  // TODO: implement timestamp
  if (input_expr->type().type == TYPE_TIMESTAMP &&
      evaluator->agg_op() != AggFnEvaluator::AVG) {
    return NULL;
  }

  Function* agg_expr_fn;
  Status status = input_expr->GetCodegendComputeFn(state_, &agg_expr_fn);
  if (!status.ok()) {
    VLOG_QUERY << "Could not codegen UpdateSlot(): " << status.GetDetail();
    return NULL;
  }
  DCHECK(agg_expr_fn != NULL);

  PointerType* fn_ctx_type =
      codegen->GetPtrType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  StructType* tuple_struct = intermediate_tuple_desc_->GenerateLlvmStruct(codegen);
  if (tuple_struct == NULL) return NULL; // Could not generate tuple struct
  PointerType* tuple_ptr_type = PointerType::get(tuple_struct, 0);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);

  // Create UpdateSlot prototype
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fn_ctx", fn_ctx_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);
  Value* fn_ctx_arg = args[0];
  Value* agg_tuple_arg = args[1];
  Value* row_arg = args[2];

  BasicBlock* src_not_null_block =
      BasicBlock::Create(codegen->context(), "src_not_null", fn);
  BasicBlock* ret_block = BasicBlock::Create(codegen->context(), "ret", fn);

  // Call expr function to get src slot value
  Value* expr_ctx = codegen->CastPtrToLlvmPtr(
      codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME), input_expr_ctx);
  Value* agg_expr_fn_args[] = { expr_ctx, row_arg };
  CodegenAnyVal src = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, input_expr->type(), agg_expr_fn, agg_expr_fn_args, "src");

  Value* src_is_null = src.GetIsNull();
  builder.CreateCondBr(src_is_null, ret_block, src_not_null_block);

  // Src slot is not null, update dst_slot
  builder.SetInsertPoint(src_not_null_block);
  Value* dst_ptr =
      builder.CreateStructGEP(agg_tuple_arg, slot_desc->field_idx(), "dst_slot_ptr");
  Value* result = NULL;

  if (slot_desc->is_nullable()) {
    // Dst is NULL, just update dst slot to src slot and clear null bit
    Function* clear_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_struct, false);
    builder.CreateCall(clear_null_fn, agg_tuple_arg);
  }

  // Update the slot
  Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
  switch (evaluator->agg_op()) {
    case AggFnEvaluator::COUNT:
      if (evaluator->is_merge()) {
        result = builder.CreateAdd(dst_value, src.GetVal(), "count_sum");
      } else {
        result = builder.CreateAdd(dst_value,
            codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
      }
      break;
    case AggFnEvaluator::MIN: {
      Function* min_fn = codegen->CodegenMinMax(slot_desc->type(), true);
      Value* min_args[] = { dst_value, src.GetVal() };
      result = builder.CreateCall(min_fn, min_args, "min_value");
      break;
    }
    case AggFnEvaluator::MAX: {
      Function* max_fn = codegen->CodegenMinMax(slot_desc->type(), false);
      Value* max_args[] = { dst_value, src.GetVal() };
      result = builder.CreateCall(max_fn, max_args, "max_value");
      break;
    }
    case AggFnEvaluator::SUM:
      if (slot_desc->type().type != TYPE_DECIMAL) {
        if (slot_desc->type().type == TYPE_FLOAT ||
            slot_desc->type().type == TYPE_DOUBLE) {
          result = builder.CreateFAdd(dst_value, src.GetVal());
        } else {
          result = builder.CreateAdd(dst_value, src.GetVal());
        }
        break;
      }
      DCHECK_EQ(slot_desc->type().type, TYPE_DECIMAL);
      // Fall through to xcompiled case
    case AggFnEvaluator::AVG:
    case AggFnEvaluator::NDV: {
      // Get xcompiled update/merge function from IR module
      const string& symbol = evaluator->is_merge() ?
                             evaluator->merge_symbol() : evaluator->update_symbol();
      Function* ir_fn = codegen->module()->getFunction(symbol);
      DCHECK(ir_fn != NULL);

      // Create pointer to src to pass to ir_fn. We must use the unlowered type.
      Value* src_lowered_ptr = codegen->CreateEntryBlockAlloca(
          fn, LlvmCodeGen::NamedVariable("src_lowered_ptr", src.value()->getType()));
      builder.CreateStore(src.value(), src_lowered_ptr);
      Type* unlowered_ptr_type =
          CodegenAnyVal::GetUnloweredPtrType(codegen, input_expr->type());
      Value* src_unlowered_ptr =
          builder.CreateBitCast(src_lowered_ptr, unlowered_ptr_type, "src_unlowered_ptr");

      // Create intermediate argument 'dst' from 'dst_value'
      const ColumnType& dst_type = evaluator->intermediate_type();
      CodegenAnyVal dst = CodegenAnyVal::GetNonNullVal(
          codegen, &builder, dst_type, "dst");
      dst.SetFromRawValue(dst_value);
      // Create pointer to dst to pass to ir_fn. We must use the unlowered type.
      Value* dst_lowered_ptr = codegen->CreateEntryBlockAlloca(
          fn, LlvmCodeGen::NamedVariable("dst_lowered_ptr", dst.value()->getType()));
      builder.CreateStore(dst.value(), dst_lowered_ptr);
      unlowered_ptr_type = CodegenAnyVal::GetUnloweredPtrType(codegen, dst_type);
      Value* dst_unlowered_ptr =
          builder.CreateBitCast(dst_lowered_ptr, unlowered_ptr_type, "dst_unlowered_ptr");

      // Call 'ir_fn'
      builder.CreateCall3(ir_fn, fn_ctx_arg, src_unlowered_ptr, dst_unlowered_ptr);

      // Convert StringVal intermediate 'dst_arg' back to StringValue
      Value* anyval_result = builder.CreateLoad(dst_lowered_ptr, "anyval_result");
      result = CodegenAnyVal(codegen, &builder, dst_type, anyval_result).ToNativeValue();
      break;
    }
    default:
      DCHECK(false) << "bad aggregate operator: " << evaluator->agg_op();
  }

  builder.CreateStore(result, dst_ptr);
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

// IR codegen for the UpdateTuple loop.  This loop is query specific and based on the
// aggregate functions.  The function signature must match the non- codegen'd UpdateTuple
// exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//

// ; Function Attrs: alwaysinline
// define void @UpdateTuple(%"class.impala::PartitionedAggregationNode"* %this_ptr,
//                          %"class.impala_udf::FunctionContext"** %agg_fn_ctxs,
//                          %"class.impala::Tuple"* %tuple,
//                          %"class.impala::TupleRow"* %row,
//                          i1 %is_merge) #20 {
// entry:
//   %tuple1 = bitcast %"class.impala::Tuple"* %tuple to { i8, i64, i64, double }*
//   %src_slot = getelementptr inbounds { i8, i64, i64, double }* %tuple1, i32 0, i32 1
//   %count_star_val = load i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   %0 = getelementptr %"class.impala_udf::FunctionContext"** %agg_fn_ctxs, i32 1
//   %fn_ctx = load %"class.impala_udf::FunctionContext"** %0
//   call void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         { i8, i64, i64, double }* %tuple1,
//                         %"class.impala::TupleRow"* %row)
//   %1 = getelementptr %"class.impala_udf::FunctionContext"** %agg_fn_ctxs, i32 2
//   %fn_ctx2 = load %"class.impala_udf::FunctionContext"** %1
//   call void @UpdateSlot5(%"class.impala_udf::FunctionContext"* %fn_ctx2,
//                          { i8, i64, i64, double }* %tuple1,
//                          %"class.impala::TupleRow"* %row)
//   ret void
// }
Function* PartitionedAggregationNode::CodegenUpdateTuple() {
  LlvmCodeGen* codegen;
  if (!state_->GetCodegen(&codegen).ok()) return NULL;
  SCOPED_TIMER(codegen->codegen_timer());

  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1);
      ++j;
    }
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];

    // Don't codegen things that aren't builtins (for now)
    if (!evaluator->is_builtin()) return NULL;

    bool supported = true;
    AggFnEvaluator::AggregationOp op = evaluator->agg_op();
    PrimitiveType type = slot_desc->type().type;
    // Char and timestamp intermediates aren't supported
    if (type == TYPE_TIMESTAMP || type == TYPE_CHAR) supported = false;
    // Only AVG and NDV support string intermediates
    if ((type == TYPE_STRING || type == TYPE_VARCHAR) &&
        !(op == AggFnEvaluator::AVG || op == AggFnEvaluator::NDV)) {
      supported = false;
    }
    // Only SUM, AVG, and NDV support decimal intermediates
    if (type == TYPE_DECIMAL &&
        !(op == AggFnEvaluator::SUM || op == AggFnEvaluator::AVG ||
          op == AggFnEvaluator::NDV)) {
      supported = false;
    }
    if (!supported) {
      VLOG_QUERY << "Could not codegen UpdateTuple because intermediate type "
                 << slot_desc->type()
                 << " is not yet supported for aggregate function \""
                 << evaluator->fn_name() << "()\"";
      return NULL;
    }
  }

  if (intermediate_tuple_desc_->GenerateLlvmStruct(codegen) == NULL) {
    VLOG_QUERY << "Could not codegen UpdateTuple because we could"
               << "not generate a matching llvm struct for the intermediate tuple.";
    return NULL;
  }

  // Get the types to match the UpdateTuple signature
  Type* agg_node_type = codegen->GetType(PartitionedAggregationNode::LLVM_CLASS_NAME);
  Type* fn_ctx_type = codegen->GetType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  Type* tuple_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);

  PointerType* agg_node_ptr_type = agg_node_type->getPointerTo();
  PointerType* fn_ctx_ptr_ptr_type = fn_ctx_type->getPointerTo()->getPointerTo();
  PointerType* tuple_ptr_type = tuple_type->getPointerTo();
  PointerType* tuple_row_ptr_type = tuple_row_type->getPointerTo();

  StructType* tuple_struct = intermediate_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_fn_ctxs", fn_ctx_ptr_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("is_merge", codegen->boolean_type()));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[5];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  Value* agg_fn_ctxs_arg = args[1];
  Value* tuple_arg = args[2];
  Value* row_arg = args[3];

  // Cast the parameter types to the internal llvm runtime types.
  // TODO: get rid of this by using right type in function signature
  tuple_arg = builder.CreateBitCast(tuple_arg, tuple_ptr, "tuple");

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1);
      ++j;
    }
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    if (evaluator->is_count_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      int field_idx = slot_desc->field_idx();
      Value* const_one = codegen->GetIntConstant(TYPE_BIGINT, 1);
      Value* slot_ptr = builder.CreateStructGEP(tuple_arg, field_idx, "src_slot");
      Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
      Function* update_slot_fn = CodegenUpdateSlot(evaluator, slot_desc);
      if (update_slot_fn == NULL) return NULL;
      Value* fn_ctx_ptr = builder.CreateConstGEP1_32(agg_fn_ctxs_arg, i);
      Value* fn_ctx = builder.CreateLoad(fn_ctx_ptr, "fn_ctx");
      builder.CreateCall3(update_slot_fn, fn_ctx, tuple_arg, row_arg);
    }
  }
  builder.CreateRetVoid();

  // CodegenProcessBatch() does the final optimizations.
  return codegen->FinalizeFunction(fn);
}

Function* PartitionedAggregationNode::CodegenProcessBatch() {
  LlvmCodeGen* codegen;
  if (!state_->GetCodegen(&codegen).ok()) return NULL;
  SCOPED_TIMER(codegen->codegen_timer());

  Function* update_tuple_fn = CodegenUpdateTuple();
  if (update_tuple_fn == NULL) return NULL;

  // Get the cross compiled update row batch function
  IRFunction::Type ir_fn = (!probe_expr_ctxs_.empty() ?
      IRFunction::PART_AGG_NODE_PROCESS_BATCH_FALSE :
      IRFunction::PART_AGG_NODE_PROCESS_BATCH_NO_GROUPING);
  Function* process_batch_fn = codegen->GetFunction(ir_fn);
  DCHECK(process_batch_fn != NULL);

  int replaced = 0;
  if (!probe_expr_ctxs_.empty()) {
    // Aggregation w/o grouping does not use a hash table.

    // Codegen for hash
    // The codegen'd ProcessBatch function is only used in Open() with level_ = 0,
    // so don't use murmur hash
    Function* hash_fn = ht_ctx_->CodegenHashCurrentRow(state_, /* use murmur */ false);
    if (hash_fn == NULL) return NULL;

    // Codegen HashTable::Equals
    Function* equals_fn = ht_ctx_->CodegenEquals(state_);
    if (equals_fn == NULL) return NULL;

    // Codegen for evaluating probe rows
    Function* eval_probe_row_fn = ht_ctx_->CodegenEvalRow(state_, false);
    if (eval_probe_row_fn == NULL) return NULL;

    // Replace call sites
    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        eval_probe_row_fn, "EvalProbeRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, true,
        hash_fn, "HashCurrentRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, true,
        equals_fn, "Equals", &replaced);
    DCHECK_EQ(replaced, 1);
  }

  process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
      update_tuple_fn, "UpdateTuple", &replaced);
  DCHECK_GE(replaced, 1);
  DCHECK(process_batch_fn != NULL);
  return codegen->OptimizeFunctionWithExprs(process_batch_fn);
}

}
