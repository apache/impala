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
#include <thrift/protocol/TDebugProtocol.h>

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
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace llvm;

// Must be a power of 2.
const int PARTITION_FAN_OUT = 4;

// Maximum number of times we will repartition. The maximum build table we
// can process is:
// MEM_LIMIT * (PARTITION_FANOUT ^ MAX_PARTITION_DEPTH). With a (low) 1GB
// limit and 64 fanout, we can support 256TB build tables in the case where
// there is no skew.
// In the case where there is skew, repartitioning is unlikely to help (assuming a
// reasonable hash function).
// TODO: we can revisit and try harder to explicitly detect skew.
static const int MAX_PARTITION_DEPTH = 3;

namespace impala {

PartitionedAggregationNode::PartitionedAggregationNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    agg_tuple_id_(tnode.agg_node.agg_tuple_id),
    agg_tuple_desc_(NULL),
    is_merge_(tnode.agg_node.__isset.is_merge ? tnode.agg_node.is_merge : false),
    needs_finalize_(tnode.agg_node.need_finalize),
    singleton_output_tuple_(NULL),
    singleton_output_tuple_returned_(true),
    output_partition_(NULL),
    build_timer_(NULL),
    get_results_timer_(NULL) {
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

  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");

  agg_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(agg_tuple_id_);
  RETURN_IF_ERROR(Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc()));

  contains_var_len_agg_exprs_ = contains_var_len_grouping_exprs_ = false;

  // Construct build exprs from agg_tuple_desc_
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
    SlotDescriptor* desc = agg_tuple_desc_->slots()[i];
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
  RETURN_IF_ERROR(Expr::Prepare(build_expr_ctxs_, state, row_desc()));

  RETURN_IF_ERROR(state_->CreateBlockMgr(mem_tracker()->SpareCapacity() * 0.8));
  int min_buffers = PARTITION_FAN_OUT * 2;
  RETURN_IF_ERROR(state_->block_mgr()->RegisterClient(
        min_buffers, mem_tracker(), &block_mgr_client_));

  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!agg_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, agg_tuple_desc_->slots().size() - 1)
          << "#eval= " << aggregate_evaluators_.size()
          << " #probe=" << probe_expr_ctxs_.size();
      ++j;
    }
    SlotDescriptor* desc = agg_tuple_desc_->slots()[j];
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(), desc));
    contains_var_len_agg_exprs_ |= (desc->type().type == TYPE_STRING);
  }

  hash_tbl_.reset(new HashTable(state_, build_expr_ctxs_, probe_expr_ctxs_,
      1, true, true, id(), mem_tracker(), true));

  if (probe_expr_ctxs_.empty()) {
    // create single output tuple now; we need to output something
    // even if our input is empty
    singleton_output_tuple_ = ConstructAggTuple(mem_pool_.get(), NULL);
    singleton_output_tuple_returned_ = false;
  } else {
    RETURN_IF_ERROR(CreateHashPartitions(0));
  }
  return Status::OK;
}

void PartitionedAggregationNode::ProcessRowBatchNoGrouping(RowBatch* batch) {
  for (int i = 0; i < batch->num_rows(); ++i) {
    UpdateAggTuple(singleton_output_tuple_, batch->GetRow(i), is_merge_);
  }
}

Status PartitionedAggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(Expr::Open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));

  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Open(state));
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
    if (probe_expr_ctxs_.empty()) {
      ProcessRowBatchNoGrouping(&batch);
    } else {
      // There is grouping, so we will do partitioned aggregation.
      RETURN_IF_ERROR(ProcessBatch(&batch, false, 0));
    }
    batch.Reset();
  }

  // We have consumed all of the input from the child and transfered ownership of the
  // resources we need, so the child can be closed safely to release its resources.
  child(0)->Close(state);

  // Done consuming child(0)'s input. Move all the partitions in hash_partitions_
  // to spilled_partitions_/aggregated_partitions_. We'll finish the processing in
  // GetNext().
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_spilled()) {
      spilled_partitions_.push_back(hash_partitions_[i]);
    } else {
      aggregated_partitions_.push_back(hash_partitions_[i]);
    }
  }
  hash_partitions_.clear();

  return Status::OK;
}

Status PartitionedAggregationNode::GetNext(RuntimeState* state,
    RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(get_results_timer_);

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
      FinalizeAggTuple(singleton_output_tuple_);
      row->SetTuple(0, singleton_output_tuple_);
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
    RETURN_IF_ERROR(NextPartition());
    DCHECK(output_partition_ != NULL);
  }

  // Keeping returning rows from the current partition.
  while (!output_iterator_.AtEnd() && !row_batch->AtCapacity()) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* agg_tuple = output_iterator_.GetTuple();
    FinalizeAggTuple(agg_tuple);
    output_iterator_.Next<false>();
    row->SetTuple(0, agg_tuple);
    if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
      VLOG_ROW << "output row: " << PrintRow(row, row_desc());
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
    FinalizeAggTuple(singleton_output_tuple_);
  }

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs
  if (output_partition_ != NULL) {
    while (!output_iterator_.AtEnd()) {
      Tuple* agg_tuple = output_iterator_.GetTuple();
      FinalizeAggTuple(agg_tuple);
      output_iterator_.Next<false>();
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

  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    aggregate_evaluators_[i]->Close(state);
  }
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(build_expr_ctxs_, state);
  ExecNode::Close(state);
}

Status PartitionedAggregationNode::Partition::Init() {
  // TODO: how many buckets?
  hash_tbl.reset(new HashTable(parent->state_, parent->build_expr_ctxs_,
      parent->probe_expr_ctxs_, 1, true, true, parent->id(),
      parent->mem_tracker(), true));
  aggregated_row_stream.reset(new BufferedTupleStream(parent->state_, parent->row_desc(),
      parent->state_->block_mgr(), parent->block_mgr_client_));
  RETURN_IF_ERROR(aggregated_row_stream->Init());

  unaggregated_row_stream.reset(new BufferedTupleStream(parent->state_,
      parent->child(0)->row_desc(), parent->state_->block_mgr(),
      parent->block_mgr_client_));
  // TODO: this can be lazily allocated.
  RETURN_IF_ERROR(unaggregated_row_stream->Init());
  return Status::OK;
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
          parent->FinalizeAggTuple(batch.GetRow(i)->GetTuple(0));
        }
      }
    }
    aggregated_row_stream->Close();
  }
  if (unaggregated_row_stream.get() != NULL) unaggregated_row_stream->Close();
}

Tuple* PartitionedAggregationNode::ConstructAggTuple(MemPool* pool,
    BufferedTupleStream* stream) {
  DCHECK(stream == NULL || pool == NULL);
  DCHECK(stream != NULL || pool != NULL);

  Tuple* agg_tuple = NULL;
  uint8_t* buffer = NULL;
  if (pool != NULL) {
    agg_tuple = Tuple::Create(agg_tuple_desc_->byte_size(), mem_pool_.get());
  } else {
    // Figure out how big it will be to copy the entire tuple. We need the tuple to end
    // up on one block in the stream.
    int size = agg_tuple_desc_->byte_size();
    if (contains_var_len_grouping_exprs_) {
      // TODO: This is likely to be too slow. The hash table could maintain this as
      // it hashes.
      for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
        if (probe_expr_ctxs_[i]->root()->type().type != TYPE_STRING) continue;
        if (hash_tbl_->last_expr_value_null(i)) continue;
        StringValue* sv = reinterpret_cast<StringValue*>(hash_tbl_->last_expr_value(i));
        size += sv->len;
      }
    }
    buffer = stream->AllocateRow(size);
    if (buffer == NULL) return NULL;
    agg_tuple = reinterpret_cast<Tuple*>(buffer);
    // TODO: remove this. we shouldn't need to zero the entire tuple.
    agg_tuple->Init(size);
    buffer += agg_tuple_desc_->byte_size();
  }

  // copy grouping values
  vector<SlotDescriptor*>::const_iterator slot_desc = agg_tuple_desc_->slots().begin();
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i, ++slot_desc) {
    if (hash_tbl_->last_expr_value_null(i)) {
      agg_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } else {
      void* src = hash_tbl_->last_expr_value(i);
      void* dst = agg_tuple->GetSlot((*slot_desc)->tuple_offset());
      if (stream == NULL) {
        RawValue::Write(src, dst, (*slot_desc)->type(), pool);
      } else {
        RawValue::Write(src, (*slot_desc)->type(), dst, &buffer);
      }
    }
  }

  // Initialize aggregate output.
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    aggregate_evaluators_[i]->Init(agg_tuple);
  }
  return agg_tuple;
}

void PartitionedAggregationNode::UpdateAggTuple(
    Tuple* tuple, TupleRow* row, bool is_merge) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  for (vector<AggFnEvaluator*>::const_iterator evaluator = aggregate_evaluators_.begin();
      evaluator != aggregate_evaluators_.end(); ++evaluator) {
    if (is_merge) {
      (*evaluator)->Merge(row, tuple);
    } else {
      (*evaluator)->Update(row, tuple);
    }
  }
}

void PartitionedAggregationNode::FinalizeAggTuple(Tuple* tuple) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  for (vector<AggFnEvaluator*>::const_iterator evaluator = aggregate_evaluators_.begin();
      evaluator != aggregate_evaluators_.end(); ++evaluator) {
    if (needs_finalize_) {
      (*evaluator)->Finalize(tuple);
    } else {
      (*evaluator)->Serialize(tuple);
    }
  }
}

void PartitionedAggregationNode::DebugString(int indentation_level,
    stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "PartitionedAggregationNode(tuple_id=" << agg_tuple_id_
       << " is_merge=" << is_merge_ << " needs_finalize=" << needs_finalize_
       << " probe_exprs=" << Expr::DebugString(probe_expr_ctxs_)
       << " agg_exprs=" << AggFnEvaluator::DebugString(aggregate_evaluators_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status PartitionedAggregationNode::CreateHashPartitions(int level) {
  if (level > MAX_PARTITION_DEPTH) {
    // TODO: better error msg.
    return Status("Cannot perform hash aggregation. Input that has too much skew");
  }
  DCHECK(hash_partitions_.empty());
  for (int i = 0; i < PARTITION_FAN_OUT; ++i) {
    hash_partitions_.push_back(state_->obj_pool()->Add(new Partition(this, level)));
    RETURN_IF_ERROR(hash_partitions_[i]->Init());
  }
  return Status::OK;
}

// TODO: template aggregated_rows.
Status PartitionedAggregationNode::ProcessBatch(RowBatch* batch,
    bool aggregated_rows, int level) {
  DCHECK(!hash_partitions_.empty());

  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    uint32_t hash;
    if (aggregated_rows) {
      hash_tbl_->EvalAndHashBuild(row, &hash);
    } else {
      hash_tbl_->EvalAndHashProbe(row, &hash);
    }

    Partition* dst_partition = hash_partitions_[hash & (PARTITION_FAN_OUT - 1)];

    // To process this row, we first see if it can be aggregated or inserted into this
    // partition's hash table. If we need to insert it and that fails, due to OOM, we
    // spill the partition. The partition to spill is not necessarily dst_partition,
    // so we can try again to insert the row.
    if (!dst_partition->is_spilled()) {
      HashTable* ht = dst_partition->hash_tbl.get();
      DCHECK(ht != NULL);
      HashTable::Iterator it = ht->Find(row);
      if (!it.AtEnd()) {
        // Row is already in hash table. Do the aggregation and we're done.
        UpdateAggTuple(it.GetTuple(), row, aggregated_rows);
        continue;
      }

      // Row was not in hash table, we need to (optionally) construct the agg tuple
      // and then insert it into the hash table.
      Tuple* agg_tuple = NULL;
      if (aggregated_rows) {
        agg_tuple = row->GetTuple(0);
        DCHECK(agg_tuple != NULL);
        if (ht->Insert(agg_tuple)) continue;
      } else {
        agg_tuple = ConstructAggTuple(
            NULL, dst_partition->aggregated_row_stream.get());
        if (agg_tuple != NULL && ht->Insert(agg_tuple)) {
          UpdateAggTuple(agg_tuple, row, is_merge_);
          continue;
        }
      }

      // In this case, we either didn't have enough memory to allocate the result
      // tuple or we didn't have enough memory to insert it into the hash table.
      // We need to spill.
      RETURN_IF_ERROR(SpillPartition());
      if (!dst_partition->is_spilled()) {
        // We spilled a different partition, try to insert this tuple.
        if (ht->Insert(agg_tuple)) continue;
        DCHECK(false) << "How can we get here. We spilled a different partition but "
          " still did not have enough memory.";
      }

      // In this case, we already constructed the agg tuple in the spill stream,
      // no need to do any more work.
      if (!aggregated_rows && agg_tuple != NULL) continue;
    }

    // This partition is already spilled, just append the row.
    BufferedTupleStream* dst_stream = aggregated_rows ?
        dst_partition->aggregated_row_stream.get() :
        dst_partition->unaggregated_row_stream.get();
    DCHECK(dst_stream != NULL);
    if (dst_stream->AddRow(row)) continue;
    Status status = dst_stream->status();
    DCHECK(!status.ok());
    status.AddErrorMsg("Could not append row even after spilling a partition.");
    return status;
  }

  return Status::OK;
}

Status PartitionedAggregationNode::NextPartition() {
  DCHECK(output_partition_ == NULL);

  // Keep looping until we get to a partition that fits in memory.
  Partition* partition = NULL;
  while (true) {
    // First return partitions that are fully aggregated (and in memory).
    if (!aggregated_partitions_.empty()) {
      partition = aggregated_partitions_.front();
      DCHECK(!partition->is_spilled());
      aggregated_partitions_.pop_front();
      break;
    }

    while (partition == NULL) {
      DCHECK(!spilled_partitions_.empty());
      // TODO: we can probably do better than just picking the first partition. We
      // can base this on the amount written to disk, etc.
      partition = spilled_partitions_.front();
      DCHECK(partition->is_spilled());
      spilled_partitions_.pop_front();

      // Create the new hash partitions to repartition into.
      // TODO: we don't need to repartition here. We are now working on 1 / FANOUT
      // of the input so it's very likely it can fit. We should look at this partitions
      // size and just do the aggregation if it fits in memory.
      RETURN_IF_ERROR(CreateHashPartitions(partition->level + 1));

      // Rows in this partition could have been spilled into two streams, depending
      // on if it is an aggregated intermediate, or an unaggregated row.
      RETURN_IF_ERROR(ProcessStream(partition->aggregated_row_stream.get(), true,
          partition->level + 1));
      RETURN_IF_ERROR(ProcessStream(partition->unaggregated_row_stream.get(), false,
          partition->level + 1));

      // Done processing this partition. Move the new partitions into
      // spilled_partitions_/aggregated_partitions_.
      partition->Close(false);
      for (int i = 0; i < hash_partitions_.size(); ++i) {
        if (hash_partitions_[i]->is_spilled()) {
          spilled_partitions_.push_back(hash_partitions_[i]);
        } else {
          aggregated_partitions_.push_back(hash_partitions_[i]);
        }
      }
      hash_partitions_.clear();
    }
  }

  DCHECK(partition->hash_tbl.get() != NULL);
  output_partition_ = partition;
  output_iterator_ = output_partition_->hash_tbl->Begin();
  return Status::OK;
}

Status PartitionedAggregationNode::ProcessStream(
    BufferedTupleStream* input_stream, bool aggregated_rows, int level) {
  RETURN_IF_ERROR(input_stream->PrepareForRead());
  bool eos = false;
  RowBatch batch(aggregated_rows ? row_desc() : children_[0]->row_desc(),
      state_->batch_size(), mem_tracker());
  while (!eos) {
    RETURN_IF_ERROR(input_stream->GetNext(&batch, &eos));
    RETURN_IF_ERROR(ProcessBatch(&batch, aggregated_rows, level));
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
  RETURN_IF_ERROR(partition->aggregated_row_stream->UnpinAllBlocks());

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
    RETURN_IF_ERROR(new_stream->Init());
    RETURN_IF_ERROR(new_stream->UnpinAllBlocks());

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
  }
  return Status::OK;
}

}
