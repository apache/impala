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

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
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
    block_mgr_client_(NULL),
    singleton_output_tuple_(NULL),
    singleton_output_tuple_returned_(true),
    output_partition_(NULL),
    process_row_batch_fn_(NULL),
    build_timer_(NULL),
    get_results_timer_(NULL),
    num_hash_buckets_(NULL),
    partitions_created_(NULL),
    max_partition_level_(NULL),
    num_row_repartitioned_(NULL),
    num_repartitions_(NULL) {
  DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
  // TODO: remove when aggregation-node is removed (too easy to get confused which
  // node is running otherwise).
  LOG(ERROR) << "Partitioned aggregation";
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
  return Status::OK;
}

Status PartitionedAggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  state_ = state;

  mem_pool_.reset(new MemPool(mem_tracker()));
  agg_fn_pool_.reset(new MemPool(state->udf_mem_tracker()));

  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  num_hash_buckets_ =
      ADD_COUNTER(runtime_profile(), "HashBuckets", TCounterType::UNIT);
  partitions_created_ =
      ADD_COUNTER(runtime_profile(), "PartitionsCreated", TCounterType::UNIT);
  max_partition_level_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxPartitionLevel", TCounterType::UNIT);
  num_row_repartitioned_ =
      ADD_COUNTER(runtime_profile(), "RowsRepartitioned", TCounterType::UNIT);
  num_repartitions_ =
      ADD_COUNTER(runtime_profile(), "NumRepartitions", TCounterType::UNIT);
  num_spilled_partitions_ =
      ADD_COUNTER(runtime_profile(), "SpilledPartitions", TCounterType::UNIT);
  largest_partition_percent_ = runtime_profile()->AddHighWaterMarkCounter(
      "LargestPartitionPercent", TCounterType::UNIT);

  intermediate_tuple_desc_ =
      state->desc_tbl().GetTupleDescriptor(intermediate_tuple_id_);
  output_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(output_tuple_id_);
  DCHECK_EQ(intermediate_tuple_desc_->slots().size(),
        output_tuple_desc_->slots().size());

  RETURN_IF_ERROR(Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc()));

  contains_var_len_agg_exprs_ = contains_var_len_grouping_exprs_ = false;

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
    contains_var_len_grouping_exprs_ |= (expr->type().type == TYPE_STRING);
  }
  // Construct a new row desc for preparing the build exprs because neither the child's
  // nor this node's output row desc may contain the intermediate tuple, e.g.,
  // in a single-node plan with an intermediate tuple different from the output tuple.
  intermediate_row_desc_.reset(new RowDescriptor(intermediate_tuple_desc_, false));
  RETURN_IF_ERROR(Expr::Prepare(build_expr_ctxs_, state, *intermediate_row_desc_));

  agg_fn_ctxs_.resize(aggregate_evaluators_.size());
  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1)
                << "#eval= " << aggregate_evaluators_.size()
                << " #probe=" << probe_expr_ctxs_.size();
      ++j;
    }
    SlotDescriptor* intermediate_slot_desc = intermediate_tuple_desc_->slots()[j];
    SlotDescriptor* output_slot_desc = output_tuple_desc_->slots()[j];
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_slot_desc, output_slot_desc, agg_fn_pool_.get(), &agg_fn_ctxs_[i]));
    state->obj_pool()->Add(agg_fn_ctxs_[i]);
    contains_var_len_agg_exprs_ |= (intermediate_slot_desc->type().type == TYPE_STRING);
  }

  if (probe_expr_ctxs_.empty()) {
    // create single output tuple now; we need to output something
    // even if our input is empty
    singleton_output_tuple_ =
        ConstructIntermediateTuple(agg_fn_ctxs_, mem_pool_.get(), NULL);
    singleton_output_tuple_returned_ = false;
  } else {
    ht_ctx_.reset(new HashTableCtx(build_expr_ctxs_, probe_expr_ctxs_, true, true,
                                   state->fragment_hash_seed(), MAX_PARTITION_DEPTH));
    RETURN_IF_ERROR(state_->block_mgr()->RegisterClient(
        MinRequiredBuffers(), mem_tracker(), state, &block_mgr_client_));
    RETURN_IF_ERROR(CreateHashPartitions(state, 0));
  }

  if (state->codegen_enabled()) {
    DCHECK(state->codegen() != NULL);
    Function* update_tuple_fn = CodegenUpdateTuple(state);
    if (update_tuple_fn != NULL) {
      Function* codegen_process_row_batch_fn =
          CodegenProcessBatch(state, update_tuple_fn);
      if (codegen_process_row_batch_fn != NULL) {
        state->codegen()->AddFunctionToJit(codegen_process_row_batch_fn,
            reinterpret_cast<void**>(&process_row_batch_fn_));
        AddRuntimeExecOption("Codegen Enabled");
      }
    }
  }
  return Status::OK;
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
  while (!eos) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));

    if (VLOG_ROW_IS_ON) {
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
  }

  // We have consumed all of the input from the child and transfered ownership of the
  // resources we need, so the child can be closed safely to release its resources.
  child(0)->Close(state);

  // Done consuming child(0)'s input. Move all the partitions in hash_partitions_
  // to spilled_partitions_/aggregated_partitions_. We'll finish the processing in
  // GetNext().
  if (!probe_expr_ctxs_.empty()) {
    RETURN_IF_ERROR(MoveHashPartitions(child(0)->rows_returned()));
  }

  return Status::OK;
}

Status PartitionedAggregationNode::GetNext(RuntimeState* state,
    RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();
  if (probe_expr_ctxs_.empty()) {
    // There was grouping, so evaluate the conjuncts and return the single result row.
    // We allow calling GetNext() after eos, so don't return this row again.
    if (!singleton_output_tuple_returned_) {
      int row_idx = row_batch->AddRow();
      TupleRow* row = row_batch->GetRow(row_idx);
      Tuple* output_tuple = FinalizeTuple(
          agg_fn_ctxs_, singleton_output_tuple_, row_batch->tuple_data_pool());
      row->SetTuple(0, output_tuple);
      if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
        row_batch->CommitLastRow();
        ++num_rows_returned_;
      }
      singleton_output_tuple_returned_ = true;
    }
    *eos = true;
    return Status::OK;
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
      return Status::OK;
    }
    // Process next partition.
    RETURN_IF_ERROR(NextPartition(state));
    DCHECK(output_partition_ != NULL);
  }

  SCOPED_TIMER(get_results_timer_);
  // Keeping returning rows from the current partition.
  while (!output_iterator_.AtEnd() && !row_batch->AtCapacity()) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* intermediate_tuple = output_iterator_.GetTuple();
    Tuple* output_tuple = FinalizeTuple(
        output_partition_->agg_fn_ctxs, intermediate_tuple, row_batch->tuple_data_pool());
    output_iterator_.Next<false>(ht_ctx_.get());
    row->SetTuple(0, output_tuple);
    if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) break; // TODO: remove this check? is this expensive?
    }
  }
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  *eos = ReachedLimit();
  return Status::OK;
}

void PartitionedAggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;

  if (!singleton_output_tuple_returned_) {
    FinalizeTuple(agg_fn_ctxs_, singleton_output_tuple_, mem_pool_.get());
  }

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs
  if (output_partition_ != NULL) {
    while (!output_iterator_.AtEnd()) {
      Tuple* intermediate_tuple = output_iterator_.GetTuple();
      Tuple* output_tuple = FinalizeTuple(
          output_partition_->agg_fn_ctxs, intermediate_tuple, mem_pool_.get());
      if (output_tuple != intermediate_tuple) {
        // Avoid consuming excessive memory.
        mem_pool_->FreeAll();
      }
      output_iterator_.Next<false>(ht_ctx_.get());
    }
    output_partition_->aggregated_row_stream->Close();
    output_partition_->aggregated_row_stream.reset();
    output_partition_->Close(false);
  }

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

  DCHECK(aggregate_evaluators_.size() == agg_fn_ctxs_.size() || agg_fn_ctxs_.empty());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    aggregate_evaluators_[i]->Close(state);
    if (!agg_fn_ctxs_.empty()) agg_fn_ctxs_[i]->impl()->Close();
  }
  if (agg_fn_pool_.get() != NULL) agg_fn_pool_->FreeAll();
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  if (ht_ctx_.get() != NULL) ht_ctx_->Close();

  if (block_mgr_client_ != NULL) {
    state->block_mgr()->LowerBufferReservation(block_mgr_client_, 0);
  }

  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(build_expr_ctxs_, state);
  ExecNode::Close(state);
}

Status PartitionedAggregationNode::Partition::InitStreams() {
  agg_fn_pool.reset(new MemPool(parent->state_->udf_mem_tracker()));
  for (int i = 0; i < parent->agg_fn_ctxs_.size(); ++i) {
    agg_fn_ctxs.push_back(parent->agg_fn_ctxs_[i]->impl()->Clone(agg_fn_pool.get()));
  }

  aggregated_row_stream.reset(new BufferedTupleStream(parent->state_, parent->row_desc(),
      parent->state_->block_mgr(), parent->block_mgr_client_));
  RETURN_IF_ERROR(aggregated_row_stream->Init());

  unaggregated_row_stream.reset(new BufferedTupleStream(parent->state_,
      parent->child(0)->row_desc(), parent->state_->block_mgr(),
      parent->block_mgr_client_));
  // This stream is only used to spill, no need to ever have this pinned.
  RETURN_IF_ERROR(unaggregated_row_stream->Init(false));
  return Status::OK;
}

bool PartitionedAggregationNode::Partition::InitHashTable() {
  DCHECK(hash_tbl.get() == NULL);
  // TODO: how many buckets?
  hash_tbl.reset(new HashTable(parent->state_, parent->block_mgr_client_, 1, true));
  return hash_tbl->Init();
}

void PartitionedAggregationNode::Partition::Close(bool finalize_rows) {
  if (is_closed) return;
  is_closed = true;
  if (hash_tbl.get() != NULL) hash_tbl->Close();
  if (aggregated_row_stream.get() != NULL) {
    if (finalize_rows) {
      // We need to walk all the rows and Finalize them here so the UDA gets a chance
      // to cleanup.
      aggregated_row_stream->PrepareForRead();
      RowBatch batch(parent->row_desc(),
          parent->state_->batch_size(), parent->mem_tracker());
      bool eos = false;
      while (!eos) {
        aggregated_row_stream->GetNext(&batch, &eos);
        for (int i = 0; i < batch.num_rows(); ++i) {
          parent->FinalizeTuple(
              agg_fn_ctxs, batch.GetRow(i)->GetTuple(0), batch.tuple_data_pool());
        }
        batch.Reset();
      }
    }
    aggregated_row_stream->Close();
  }
  if (unaggregated_row_stream.get() != NULL) unaggregated_row_stream->Close();

  for (int i = 0; i < agg_fn_ctxs.size(); ++i) {
    agg_fn_ctxs[i]->impl()->Close();
  }
  if (agg_fn_pool.get() != NULL) agg_fn_pool->FreeAll();
}

Tuple* PartitionedAggregationNode::ConstructIntermediateTuple(
    const vector<FunctionContext*>& agg_fn_ctxs, MemPool* pool,
    BufferedTupleStream* stream) {
  DCHECK(stream == NULL || pool == NULL);
  DCHECK(stream != NULL || pool != NULL);

  Tuple* intermediate_tuple = NULL;
  uint8_t* buffer = NULL;
  if (pool != NULL) {
    intermediate_tuple = Tuple::Create(
        intermediate_tuple_desc_->byte_size(), mem_pool_.get());
  } else {
    // Figure out how big it will be to copy the entire tuple. We need the tuple to end
    // up on one block in the stream.
    int size = intermediate_tuple_desc_->byte_size();
    if (contains_var_len_grouping_exprs_) {
      // TODO: This is likely to be too slow. The hash table could maintain this as
      // it hashes.
      for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
        if (probe_expr_ctxs_[i]->root()->type().type != TYPE_STRING) continue;
        if (ht_ctx_->last_expr_value_null(i)) continue;
        StringValue* sv = reinterpret_cast<StringValue*>(ht_ctx_->last_expr_value(i));
        size += sv->len;
      }
    }
    buffer = stream->AllocateRow(size);
    if (buffer == NULL) return NULL;
    intermediate_tuple = reinterpret_cast<Tuple*>(buffer);
    // TODO: remove this. we shouldn't need to zero the entire tuple.
    intermediate_tuple->Init(size);
    buffer += intermediate_tuple_desc_->byte_size();
  }

  // copy grouping values
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
    if (aggregate_evaluators_[i]->is_merge() || is_merge) {
      aggregate_evaluators_[i]->Merge(agg_fn_ctxs[i], row, tuple);
    } else {
      aggregate_evaluators_[i]->Update(agg_fn_ctxs[i], row, tuple);
    }
  }
}

Tuple* PartitionedAggregationNode::FinalizeTuple(
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

Status PartitionedAggregationNode::CreateHashPartitions(RuntimeState* state, int level) {
  if (level >= MAX_PARTITION_DEPTH) {
    // TODO: better error msg.
    return Status("Cannot perform hash aggregation. Partitioned input data too many"
       " times. This could mean there is too much skew in the data or the memory"
       " limit is set too low.");
  }
  ht_ctx_->set_level(level);

  DCHECK(hash_partitions_.empty());
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_partitions_.push_back(state_->obj_pool()->Add(new Partition(this, level)));
    RETURN_IF_ERROR(hash_partitions_[i]->InitStreams());
  }
  DCHECK_GT(state->block_mgr()->num_reserved_buffers_remaining(block_mgr_client_), 0);

  // Now that all the streams are reserved (meaning we have enough memory to execute
  // the algorithm), allocate the hash tables. These can fail and we can still continue.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    if (!hash_partitions_[i]->InitHashTable()) {
      hash_partitions_[i]->hash_tbl->Close();
      hash_partitions_[i]->hash_tbl.reset();
      RETURN_IF_ERROR(hash_partitions_[i]->aggregated_row_stream->UnpinStream(true));
      COUNTER_UPDATE(num_spilled_partitions_, 1);
    }
  }

  COUNTER_UPDATE(partitions_created_, PARTITION_FANOUT);
  COUNTER_SET(max_partition_level_, level);
  return Status::OK;
}

Status PartitionedAggregationNode::NextPartition(RuntimeState* state) {
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
      DCHECK_EQ(state->block_mgr()->num_pinned_buffers(block_mgr_client_), 0);

      // TODO: we can probably do better than just picking the first partition. We
      // can base this on the amount written to disk, etc.
      partition = spilled_partitions_.front();
      DCHECK(partition->is_spilled());

      // Create the new hash partitions to repartition into.
      // TODO: we don't need to repartition here. We are now working on 1 / FANOUT
      // of the input so it's reasonably likely it can fit. We should look at this
      // partitions size and just do the aggregation if it fits in memory.
      RETURN_IF_ERROR(CreateHashPartitions(state, partition->level + 1));
      COUNTER_UPDATE(num_repartitions_, 1);

      // Rows in this partition could have been spilled into two streams, depending
      // on if it is an aggregated intermediate, or an unaggregated row.
      // Note: we must process the aggregated rows first to save a hash table lookup
      // in ProcessBatch().
      RETURN_IF_ERROR(ProcessStream<true>(partition->aggregated_row_stream.get()));
      RETURN_IF_ERROR(ProcessStream<false>(partition->unaggregated_row_stream.get()));

      COUNTER_UPDATE(num_row_repartitioned_,
          partition->aggregated_row_stream->num_rows());
      COUNTER_UPDATE(num_row_repartitioned_,
          partition->unaggregated_row_stream->num_rows());

      partition->Close(false);
      spilled_partitions_.pop_front();

      // Done processing this partition. Move the new partitions into
      // spilled_partitions_/aggregated_partitions_.
      int64_t num_input_rows = 0;
      num_input_rows += partition->aggregated_row_stream->num_rows();
      num_input_rows += partition->unaggregated_row_stream->num_rows();
      RETURN_IF_ERROR(MoveHashPartitions(num_input_rows));
    }
  }

  DCHECK(partition->hash_tbl.get() != NULL);
  DCHECK(partition->aggregated_row_stream->is_pinned());

  output_partition_ = partition;
  output_iterator_ = output_partition_->hash_tbl->Begin();
  COUNTER_UPDATE(num_hash_buckets_, output_partition_->hash_tbl->num_buckets());
  return Status::OK;
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessStream(BufferedTupleStream* input_stream) {
  bool got_buffer = false;
  RETURN_IF_ERROR(input_stream->PrepareForRead(&got_buffer));
  if (!got_buffer) {
    // Did not have a buffer to read the input stream. Spill and try again.
    RETURN_IF_ERROR(SpillPartition());
    RETURN_IF_ERROR(input_stream->PrepareForRead());
  }

  bool eos = false;
  RowBatch batch(AGGREGATED_ROWS ? *intermediate_row_desc_ : children_[0]->row_desc(),
      state_->batch_size(), mem_tracker());
  while (!eos) {
    RETURN_IF_ERROR(input_stream->GetNext(&batch, &eos));
    RETURN_IF_ERROR(ProcessBatch<AGGREGATED_ROWS>(&batch, ht_ctx_.get()));
    batch.Reset();
  }
  input_stream->Close();
  return Status::OK;
}

Status PartitionedAggregationNode::SpillPartition() {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick the largest partition that is not spilled.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed) continue;
    if (hash_partitions_[i]->is_spilled()) continue;
    int64_t mem = hash_partitions_[i]->aggregated_row_stream->bytes_in_mem(true);
    mem += hash_partitions_[i]->hash_tbl->byte_size();
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }
  if (partition_idx == -1) {
    DCHECK(false) << "This should never happen due to the reservation. This is "
      "defense on release builds";
    // Could not find a partition to spill. This means the mem limit was just too
    // low.
    Status status = Status::MEM_LIMIT_EXCEEDED;
    status.AddErrorMsg("Mem limit is too low to perform partitioned aggregation");
    return status;
  }

  Partition* partition = hash_partitions_[partition_idx];
  DCHECK(partition->hash_tbl.get() != NULL);
  partition->hash_tbl->Close();
  partition->hash_tbl.reset();
  RETURN_IF_ERROR(partition->aggregated_row_stream->UnpinStream(true));
  COUNTER_UPDATE(num_spilled_partitions_, 1);

  // We need to do a lot more work in this case. The result tuple contains var-len
  // strings, meaning the current in memory layout is not the on disk block layout.
  // The disk layout does not support mutable rows. We need to rewrite the stream
  // into the on disk format.
  if (contains_var_len_agg_exprs_) {
    // TODO: We can't do this without expr-refactoring. All var-len data currently comes
    // from the same mem pool. Remove this check after expr refactoring rebase.
    DCHECK(!contains_var_len_agg_exprs_) << "Not implemented without expr-refactoring";

    // Make a new stream to copy the rows into.
    scoped_ptr<BufferedTupleStream> new_stream(new BufferedTupleStream(
        state_, row_desc(), state_->block_mgr(), block_mgr_client_));
    RETURN_IF_ERROR(new_stream->Init(false));

    // Copy the spilled partition's stream into the new stream. This compacts the var-len
    // result data.
    RETURN_IF_ERROR(partition->aggregated_row_stream->PrepareForRead());
    RowBatch batch(row_desc(), state_->batch_size(), mem_tracker());
    bool eos = false;
    while (!eos) {
      RETURN_IF_ERROR(partition->aggregated_row_stream->GetNext(&batch, &eos));
      for (int i = 0; i < batch.num_rows(); ++i) {
        bool result = new_stream->AddRow(batch.GetRow(i));
        if (!result) return Status::MEM_LIMIT_EXCEEDED;
      }
    }
    partition->aggregated_row_stream->Close();
    partition->aggregated_row_stream.swap(new_stream);

    // Free the in-memory result data
    // TODO: this isn't right. We need to finalize each tuple and close the partition's
    // FunctionContext
    for (int i = 0; i < partition->agg_fn_ctxs.size(); ++i) {
      partition->agg_fn_ctxs[i]->impl()->FreeLocalAllocations();
    }
  }
  return Status::OK;
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
      // We need to unpin all the spilled partitions to make room to allocate new
      // hash_partitions_ when we repartition the spilled partitions.
      // TODO: we only need to do this when we have memory pressure. This might be
      // okay though since the block mgr should only write these to disk if there
      // is memory pressure.
      RETURN_IF_ERROR(partition->aggregated_row_stream->UnpinStream(true));
      RETURN_IF_ERROR(partition->unaggregated_row_stream->UnpinStream(true));
      spilled_partitions_.push_back(partition);
    } else {
      aggregated_partitions_.push_back(partition);
    }

  }
  LOG(ERROR) << ss.str();
  hash_partitions_.clear();
  return Status::OK;
}

IRFunction::Type GetHllUpdateFunction(const ColumnType& type) {
  switch (type.type) {
    case TYPE_BOOLEAN: return IRFunction::HLL_UPDATE_BOOLEAN;
    case TYPE_TINYINT: return IRFunction::HLL_UPDATE_TINYINT;
    case TYPE_SMALLINT: return IRFunction::HLL_UPDATE_SMALLINT;
    case TYPE_INT: return IRFunction::HLL_UPDATE_INT;
    case TYPE_BIGINT: return IRFunction::HLL_UPDATE_BIGINT;
    case TYPE_FLOAT: return IRFunction::HLL_UPDATE_FLOAT;
    case TYPE_DOUBLE: return IRFunction::HLL_UPDATE_DOUBLE;
    case TYPE_STRING: return IRFunction::HLL_UPDATE_STRING;
    case TYPE_DECIMAL: return IRFunction::HLL_UPDATE_DECIMAL;
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return IRFunction::FN_END;
  }
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
    RuntimeState* state, AggFnEvaluator* evaluator, SlotDescriptor* slot_desc) {
  DCHECK(slot_desc->is_materialized());
  LlvmCodeGen* codegen = state->codegen();

  DCHECK_EQ(evaluator->input_expr_ctxs().size(), 1);
  ExprContext* input_expr_ctx = evaluator->input_expr_ctxs()[0];
  Expr* input_expr = input_expr_ctx->root();
  // TODO: implement timestamp
  if (input_expr->type().type == TYPE_TIMESTAMP) return NULL;
  Function* agg_expr_fn;
  Status status = input_expr->GetCodegendComputeFn(state, &agg_expr_fn);
  if (!status.ok()) {
    VLOG_QUERY << "Could not codegen UpdateSlot(): " << status.GetErrorMsg();
    return NULL;
  }
  DCHECK(agg_expr_fn != NULL);

  PointerType* fn_ctx_type =
      codegen->GetPtrType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  StructType* tuple_struct = intermediate_tuple_desc_->GenerateLlvmStruct(codegen);
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
  Value* ctx_arg = codegen->CastPtrToLlvmPtr(
      codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME), input_expr_ctx);
  Value* agg_expr_fn_args[] = { ctx_arg, row_arg };
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
      if (slot_desc->type().type == TYPE_FLOAT || slot_desc->type().type == TYPE_DOUBLE) {
        result = builder.CreateFAdd(dst_value, src.GetVal());
      } else {
        result = builder.CreateAdd(dst_value, src.GetVal());
      }
      break;
    case AggFnEvaluator::NDV: {
      DCHECK_EQ(slot_desc->type().type, TYPE_STRING);
      IRFunction::Type ir_function_type = evaluator->is_merge() ? IRFunction::HLL_MERGE
                                          : GetHllUpdateFunction(input_expr->type());
      Function* hll_fn = codegen->GetFunction(ir_function_type);

      // Create pointer to src_anyval to pass to HllUpdate() function. We must use the
      // unlowered type.
      Value* src_lowered_ptr = codegen->CreateEntryBlockAlloca(
          fn, LlvmCodeGen::NamedVariable("src_lowered_ptr", src.value()->getType()));
      builder.CreateStore(src.value(), src_lowered_ptr);
      Type* unlowered_ptr_type =
          CodegenAnyVal::GetUnloweredType(codegen, input_expr->type())->getPointerTo();
      Value* src_unlowered_ptr =
          builder.CreateBitCast(src_lowered_ptr, unlowered_ptr_type, "src_unlowered_ptr");

      // Create StringVal* intermediate argument from dst_value
      CodegenAnyVal dst_stringval = CodegenAnyVal::GetNonNullVal(
          codegen, &builder, TYPE_STRING, "dst_stringval");
      dst_stringval.SetFromRawValue(dst_value);
      // Create pointer to dst_stringval to pass to HllUpdate() function. We must use
      // the unlowered type.
      Value* dst_lowered_ptr = codegen->CreateEntryBlockAlloca(
          fn, LlvmCodeGen::NamedVariable("dst_lowered_ptr",
                                         dst_stringval.value()->getType()));
      builder.CreateStore(dst_stringval.value(), dst_lowered_ptr);
      unlowered_ptr_type =
          codegen->GetPtrType(CodegenAnyVal::GetUnloweredType(codegen, TYPE_STRING));
      Value* dst_unlowered_ptr =
          builder.CreateBitCast(dst_lowered_ptr, unlowered_ptr_type, "dst_unlowered_ptr");

      // Call 'hll_fn'
      builder.CreateCall3(hll_fn, fn_ctx_arg, src_unlowered_ptr, dst_unlowered_ptr);

      // Convert StringVal intermediate 'dst_arg' back to StringValue
      Value* anyval_result = builder.CreateLoad(dst_lowered_ptr, "anyval_result");
      result = CodegenAnyVal(codegen, &builder, TYPE_STRING, anyval_result)
               .ToNativeValue();
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
Function* PartitionedAggregationNode::CodegenUpdateTuple(RuntimeState* state) {
  LlvmCodeGen* codegen = state->codegen();
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

    // Timestamp and char are never supported. NDV supports decimal and string but no
    // other functions.
    // TODO: the other aggregate functions might work with decimal as-is
    if (slot_desc->type().type == TYPE_TIMESTAMP || slot_desc->type().type == TYPE_CHAR ||
        (evaluator->agg_op() != AggFnEvaluator::NDV &&
         (slot_desc->type().type == TYPE_DECIMAL ||
          slot_desc->type().type == TYPE_STRING ||
          slot_desc->type().type == TYPE_VARCHAR))) {
      VLOG_QUERY << "Could not codegen UpdateTuple because "
                 << "string, char, timestamp and decimal are not yet supported.";
      return NULL;
    }

    // Don't codegen things that aren't builtins (for now)
    if (!evaluator->is_builtin()) return NULL;
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
      Function* update_slot_fn = CodegenUpdateSlot(state, evaluator, slot_desc);
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

Function* PartitionedAggregationNode::CodegenProcessBatch(
    RuntimeState* state, Function* update_tuple_fn) {
  LlvmCodeGen* codegen = state->codegen();
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(update_tuple_fn != NULL);

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
    Function* hash_fn = ht_ctx_->CodegenHashCurrentRow(state);
    if (hash_fn == NULL) return NULL;

    // Codegen HashTable::Equals
    Function* equals_fn = ht_ctx_->CodegenEquals(state);
    if (equals_fn == NULL) return NULL;

    // Codegen for evaluating probe rows
    Function* eval_probe_row_fn = ht_ctx_->CodegenEvalRow(state, false);
    if (eval_probe_row_fn == NULL) return NULL;

    // Replace call sites
    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        eval_probe_row_fn, "EvalProbeRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        hash_fn, "HashCurrentRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
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
