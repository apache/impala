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

#include "exec/partitioned-hash-join-node.h"

#include <sstream>

#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace boost;
using namespace impala;
using namespace std;

// Number of initial partitions to create. Must be a power of two.
// TODO: this is set to a lower than actual value for testing.
static const int PARTITION_FANOUT = 4;

// Needs to be the log(PARTITION_FANOUT)
static const int NUM_PARTITIONING_BITS = 2;

// Maximum number of times we will repartition. The maximum build table we
// can process is:
// MEM_LIMIT * (PARTITION_FANOUT ^ MAX_PARTITION_DEPTH). With a (low) 1GB
// limit and 64 fanout, we can support 256TB build tables in the case where
// there is no skew.
// In the case where there is skew, repartitioning is unlikely to help (assuming a
// reasonable hash function).
// TODO: we can revisit and try harder to explicitly detect skew.
static const int MAX_PARTITION_DEPTH = 3;

// Maximum number of build tables that can be in memory at any time. This is in
// addition to the memory constraints and is used for testing to trigger code paths
// for small tables.
// Note: In order to test the spilling paths more easily, set it to PARTITION_FANOUT / 2.
// TODO: Eventually remove.
static const int MAX_IN_MEM_BUILD_TABLES = PARTITION_FANOUT;

PartitionedHashJoinNode::PartitionedHashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("PartitionedHashJoinNode", tnode.hash_join_node.join_op,
        pool, tnode, descs),
    state_(PARTITIONING_BUILD),
    input_partition_(NULL) {
}

PartitionedHashJoinNode::~PartitionedHashJoinNode() {
}

Status PartitionedHashJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
    ExprContext* ctx;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].left, &ctx));
    probe_expr_ctxs_.push_back(ctx);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].right, &ctx));
    build_expr_ctxs_.push_back(ctx);
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.hash_join_node.other_join_conjuncts,
                            &other_join_conjunct_ctxs_));
  return Status::OK;
}

Status PartitionedHashJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));

  // Note: For easier testing of the spilling paths, use just 80% of what is left.
  // That is, multiple SpareCapacity() * 0.8f.
  int64_t max_join_mem = mem_tracker()->SpareCapacity();
  if (state->query_options().max_join_memory > 0) {
    max_join_mem = state->query_options().max_join_memory;
  }
  LOG(ERROR) << "Max join memory: "
             << PrettyPrinter::Print(max_join_mem, TCounterType::BYTES);
  join_node_mem_tracker_.reset(new MemTracker(
      state->query_options().max_join_memory, -1, "Hash Join Mem Limit", mem_tracker()));

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(Expr::Prepare(build_expr_ctxs_, state, child(1)->row_desc()));
  RETURN_IF_ERROR(Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc()));

  // other_join_conjunct_ctxs_ are evaluated in the context of the rows produced by this
  // node
  RETURN_IF_ERROR(Expr::Prepare(other_join_conjunct_ctxs_, state, row_descriptor_));

  RETURN_IF_ERROR(state->CreateBlockMgr(join_node_mem_tracker()->SpareCapacity()));
  // We need one output buffer per partition and one additional buffer either for the
  // input (while repartitioning) or to contain the hash table.
  int num_reserved_buffers = PARTITION_FANOUT + 1;
  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(
      num_reserved_buffers, join_node_mem_tracker(), &block_mgr_client_));

  // Construct the dummy hash table used to evaluate hashes of rows.
  // TODO: this is obviously not the right abstraction. We need a Hash utility class of
  // some kind.
  bool should_store_nulls = join_op_ == TJoinOp::RIGHT_OUTER_JOIN ||
      join_op_ == TJoinOp::FULL_OUTER_JOIN;
  hash_tbl_.reset(new HashTable(state, build_expr_ctxs_, probe_expr_ctxs_,
      child(1)->row_desc().tuple_descriptors().size(), should_store_nulls, false,
      state->fragment_hash_seed(), mem_tracker()));
  return Status::OK;
}

void PartitionedHashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    hash_partitions_[i]->Close();
  }
  for (list<Partition*>::iterator it = spilled_partitions_.begin();
      it != spilled_partitions_.end(); ++it) {
    (*it)->Close();
  }
  if (input_partition_ != NULL) input_partition_->Close();
  Expr::Close(build_expr_ctxs_, state);
  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(other_join_conjunct_ctxs_, state);
  BlockingJoinNode::Close(state);
}

PartitionedHashJoinNode::Partition::Partition(RuntimeState* state,
        PartitionedHashJoinNode* parent, int level)
  : parent_(parent),
    is_closed_(false),
    level_(level),
    build_rows_(new BufferedTupleStream(
        state, parent_->child(1)->row_desc(), state->block_mgr(),
        parent_->block_mgr_client_)),
    probe_rows_(new BufferedTupleStream(
        state, parent_->child(0)->row_desc(),
        state->block_mgr(), parent_->block_mgr_client_)) {
}

PartitionedHashJoinNode::Partition::~Partition() {
  DCHECK(is_closed());
}

int64_t PartitionedHashJoinNode::Partition::EstimatedInMemSize() const {
  return build_rows_->byte_size() + HashTable::EstimateSize(build_rows_->num_rows());
}

int64_t PartitionedHashJoinNode::Partition::InMemSize() const {
  DCHECK(hash_tbl_.get() != NULL);
  return build_rows_->byte_size() + hash_tbl_->byte_size();
}

void PartitionedHashJoinNode::Partition::Close() {
  if (is_closed()) return;
  is_closed_ = true;
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  if (build_rows_.get() != NULL) build_rows_->Close();
  if (probe_rows_.get() != NULL) probe_rows_->Close();
}

Status PartitionedHashJoinNode::Partition::BuildHashTable(
    RuntimeState* state, bool* built) {
  DCHECK(build_rows_.get() != NULL);
  *built = false;
  // First pin the entire build stream in memory.
  RETURN_IF_ERROR(build_rows_->PinAllBlocks(built));
  if (!*built) return Status::OK;
  RETURN_IF_ERROR(build_rows_->PrepareForRead());

  bool should_store_nulls = parent_->join_op_ == TJoinOp::RIGHT_OUTER_JOIN ||
      parent_->join_op_ == TJoinOp::FULL_OUTER_JOIN;
  hash_tbl_.reset(new HashTable(state, parent_->build_expr_ctxs_,
      parent_->probe_expr_ctxs_, parent_->child(1)->row_desc().tuple_descriptors().size(),
      should_store_nulls, false, state->fragment_hash_seed(),
      parent_->join_node_mem_tracker()));

  bool eos = false;
  RowBatch batch(parent_->child(1)->row_desc(), state->batch_size(),
      parent_->mem_tracker());
  while (!eos) {
    RETURN_IF_ERROR(build_rows_->GetNext(&batch, &eos));
    for (int i = 0; i < batch.num_rows(); ++i) {
      hash_tbl_->Insert(batch.GetRow(i));
    }
    parent_->build_pool_->AcquireData(batch.tuple_data_pool(), false);
    batch.Reset();

    if (hash_tbl_->mem_limit_exceeded()) {
      hash_tbl_->Close();
      RETURN_IF_ERROR(build_rows_->UnpinAllBlocks());
      *built = false;
      return Status::OK;
    }
  }
  return Status::OK;
}

// TODO: can we do better with the spilling heuristic.
Status PartitionedHashJoinNode::SpillPartitions() {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick a partition that is already spilled.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->hash_tbl() != NULL) continue;
    int64_t mem = hash_partitions_[i]->build_rows()->bytes_in_mem(true);
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }

  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too
    // low. e.g. mem_limit too small that we can't put a buffer in front of each
    // partition.
    Status status = Status::MEM_LIMIT_EXCEEDED;
    status.AddErrorMsg("Mem limit is too low to perform partitioned join. We do not "
        "have enough memory to maintain a buffer per partition.");
    return status;
  }
  LOG(ERROR) << "Spilling partition: " << partition_idx << endl << DebugString();
  return hash_partitions_[partition_idx]->build_rows()->UnpinAllBlocks();
}

Status PartitionedHashJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(other_join_conjunct_ctxs_, state));

  // Do a full scan of child(1) and partition the rows.
  RETURN_IF_ERROR(child(1)->Open(state));
  RETURN_IF_ERROR(ProcessBuildInput(state));
  UpdateState(PROCESSING_PROBE);
  return Status::OK;
}

Status PartitionedHashJoinNode::ProcessBuildInput(RuntimeState* state) {
  int level = input_partition_ == NULL ? 0 : input_partition_->level_;
  DCHECK(hash_partitions_.empty());
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_partitions_.push_back(pool_->Add(new Partition(state, this, level + 1)));
    RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->Init());
  }

  if (input_partition_ != NULL) {
    DCHECK(input_partition_->build_rows() != NULL);
    RETURN_IF_ERROR(input_partition_->build_rows()->PrepareForRead());
  }
  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
  bool eos = false;
  while (!eos) {
    RETURN_IF_ERROR(state->CheckQueryState());
    if (input_partition_ == NULL) {
      RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
      COUNTER_UPDATE(build_row_counter_, build_batch.num_rows());
    } else {
      RETURN_IF_ERROR(input_partition_->build_rows()->GetNext(&build_batch, &eos));
    }
    SCOPED_TIMER(build_timer_);
    RETURN_IF_ERROR(ProcessBuildBatch(&build_batch, level));
    build_batch.Reset();
    DCHECK(!build_batch.AtCapacity());
  }
  RETURN_IF_ERROR(BuildHashTables(state));
  return Status::OK;
}

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch, int level) {
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    TupleRow* build_row = build_batch->GetRow(i);
    uint32_t hash;
    // TODO: plumb level through when we change the hashing interface. We need different
    // hash functions.
    if (!hash_tbl_->EvalAndHashBuild(build_row, &hash)) continue;
    Partition* partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];
    // TODO: Should we maintain a histogram with the size of each partition?
    bool result = AppendRow(partition->build_rows(), build_row);
    if (UNLIKELY(!result)) return status_;
  }
  return Status::OK;
}

inline bool PartitionedHashJoinNode::AppendRow(BufferedTupleStream* stream,
    TupleRow* row) {
  if (LIKELY(stream->AddRow(row))) return true;
  status_ = stream->status();
  if (!status_.ok()) return false;
  // We ran out of memory. Pick a partition to spill.
  status_ = SpillPartitions();
  if (!status_.ok()) return false;
  if (!stream->AddRow(row)) {
    // Can this happen? we just spilled a partition so this shouldn't fail.
    status_ = Status("Could not spill row.");
    return false;
  }
  return true;
}

Status PartitionedHashJoinNode::InitGetNext(TupleRow* first_probe_row) {
  // TODO: Move this reset to blocking-join. Not yet though because of hash-join.
  ResetForProbe();
  return Status::OK;
}

Status PartitionedHashJoinNode::NextProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK_EQ(probe_batch_pos_, probe_batch_->num_rows());
  while (true) {
    // Loop until we find a non-empty row batch.
    if (UNLIKELY(probe_batch_pos_ == probe_batch_->num_rows())) {
      probe_batch_->TransferResourceOwnership(out_batch);
      ResetForProbe();
      if (probe_side_eos_) break;
      RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
      continue;
    }
    ResetForProbe();
    return Status::OK;
  }
  current_probe_row_ = NULL;
  probe_batch_pos_ = -1;
  return Status::OK;
}

Status PartitionedHashJoinNode::NextSpilledProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(input_partition_ != NULL);
  BufferedTupleStream* probe_rows = input_partition_->probe_rows();
  probe_batch_->Reset();
  if (LIKELY(probe_rows->rows_returned() < probe_rows->num_rows())) {
    // Continue from the current probe stream.
    bool eos = false;
    RETURN_IF_ERROR(input_partition_->probe_rows()->GetNext(probe_batch_.get(), &eos));
    DCHECK_GT(probe_batch_->num_rows(), 0);
    ResetForProbe();
  } else {
    // Done with this partition.
    if (join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN) {
      // In case of right-outer or full-outer joins, we move this partition to the list
      // of partitions that we need to output their unmatched build rows.
      DCHECK(output_build_partitions_.empty());
      DCHECK(input_partition_->hash_tbl_.get() != NULL);
      hash_tbl_iterator_ = input_partition_->hash_tbl_->FirstUnmatched();
      output_build_partitions_.push_back(input_partition_);
    } else {
      // In any other case, just close the input partition.
      input_partition_->Close();
      input_partition_ = NULL;
    }
    current_probe_row_ = NULL;
    probe_batch_pos_ = -1;
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::PrepareNextPartition(RuntimeState* state) {
  DCHECK(input_partition_ == NULL);
  if (spilled_partitions_.empty()) return Status::OK;
  LOG(ERROR) << "PrepareNextPartition\n" << DebugString();

  int64_t mem_limit = join_node_mem_tracker()->SpareCapacity();
  mem_limit -= state->block_mgr()->block_size();

  input_partition_ = spilled_partitions_.front();
  spilled_partitions_.pop_front();

  DCHECK(input_partition_->hash_tbl() == NULL);
  bool built = false;
  if (input_partition_->EstimatedInMemSize() < mem_limit) {
    RETURN_IF_ERROR(input_partition_->BuildHashTable(state, &built));
  }

  if (!built) {
    if (input_partition_->level_ == MAX_PARTITION_DEPTH) {
      return Status("Build rows have too much skew. Cannot perform join.");
    }
    // This build partition still does not fit in memory. Recurse the algorithm.
    RETURN_IF_ERROR(ProcessBuildInput(state));
    UpdateState(REPARTITIONING);
  } else {
    UpdateState(PROBING_SPILLED_PARTITION);
  }

  RETURN_IF_ERROR(input_partition_->probe_rows()->PrepareForRead());
  return Status::OK;
}

template<int const JoinOp>
Status PartitionedHashJoinNode::ProcessProbeBatch(RowBatch* out_batch) {
  ExprContext* const* other_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_other_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  int num_conjuncts = conjunct_ctxs_.size();

  int num_rows_added = 0;
  while (probe_batch_pos_ >= 0) {
    while (!hash_tbl_iterator_.AtEnd()) {
      DCHECK(current_probe_row_ != NULL);
      TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
      int idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(idx);
      CreateOutputRow(out_row, current_probe_row_, matched_build_row);

      if (!ExecNode::EvalConjuncts(other_conjunct_ctxs, num_other_conjuncts, out_row)) {
        hash_tbl_iterator_.Next<true>();
        continue;
      }

      // At this point the probe is considered matched.
      matched_probe_ = true;
      if (JoinOp == TJoinOp::LEFT_ANTI_JOIN) {
        // In this case we can safely ignore this probe row for {left anti, left outer,
        // full outer} joins.
        break;
      }
      if (JoinOp == TJoinOp::RIGHT_OUTER_JOIN || JoinOp == TJoinOp::FULL_OUTER_JOIN) {
        // There is a match for this row, mark it as matched in case of right-outer and
        // full-outer joins.
        hash_tbl_iterator_.set_matched(true);
      }
      if (JoinOp == TJoinOp::LEFT_SEMI_JOIN) {
        hash_tbl_iterator_ = hash_tbl_->End();
      } else {
        hash_tbl_iterator_.Next<true>();
      }

      if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
        out_batch->CommitLastRow();
        ++num_rows_added;
        if (out_batch->AtCapacity()) goto end;
      }
    }

    if ((JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_OUTER_JOIN ||
         JoinOp == TJoinOp::FULL_OUTER_JOIN) &&
        !matched_probe_) {
      // No match for this row, we need to output it in the case of anti, left-outer and
      // full-outer joins.
      int idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(idx);
      CreateOutputRow(out_row, current_probe_row_, NULL);
      if (ExecNode::EvalConjuncts(other_conjunct_ctxs, num_other_conjuncts, out_row) &&
          ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
        out_batch->CommitLastRow();
        ++num_rows_added;
        matched_probe_ = true;
        if (out_batch->AtCapacity()) goto end;
      }
    }

    if (UNLIKELY(probe_batch_pos_ == probe_batch_->num_rows())) {
      // Finished this batch.
      current_probe_row_ = NULL;
      goto end;
    }

    // Establish current_probe_row_ and find its corresponding partition.
    current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
    matched_probe_ = false;
    Partition* partition = NULL;
    if (input_partition_ != NULL && input_partition_->hash_tbl() != NULL) {
      // In this case we are working on a spilled partition (input_partition_ != NULL).
      // If the input partition has a hash table built, it means we are *not*
      // repartitioning and simply probing into input_partition_'s hash table.
      partition = input_partition_;
    } else {
      // We don't know which partition this probe row should go to.
      uint32_t hash;
      if (!hash_tbl_->EvalAndHashProbe(current_probe_row_, &hash)) continue;
      partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];
    }
    DCHECK(partition != NULL);

    if (partition->hash_tbl() == NULL) {
      if (partition->is_closed()) {
        // This partition is closed, meaning the build side for this partition was empty.
        DCHECK_EQ(state_, PROCESSING_PROBE);
        continue;
      }
      // This partition is not in memory, spill the probe row.
      if (UNLIKELY(!AppendRow(partition->probe_rows(), current_probe_row_))) {
        return status_;
      }
    } else {
      // Perform the actual probe in the hash table for the current probe (left) row.
      // TODO: At this point it would be good to do some prefetching.
      hash_tbl_iterator_= partition->hash_tbl()->Find(current_probe_row_);
    }
  }

end:
  num_rows_returned_ += num_rows_added;
  return Status::OK;
}

Status PartitionedHashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch,
    bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));

  while (true) {
    DCHECK_NE(state_, PARTITIONING_BUILD) << "Should not be in GetNext()";
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());

    if ((join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty())  {
      // In case of right-outer and full-outer joins, flush the remaining unmatched build
      // rows of any partition we are done processing, before processing the next batch.
      RETURN_IF_ERROR(OutputUnmatchedBuild(out_batch));
      if (!output_build_partitions_.empty()) break;

      // Finished to output unmatched build rows, move to next partition.
      DCHECK(hash_partitions_.empty());
      RETURN_IF_ERROR(PrepareNextPartition(state));
      if (input_partition_ == NULL) {
        *eos = true;
        break;
      }
    }

    // Finish up the current batch.
    switch (join_op_) {
      case TJoinOp::LEFT_ANTI_JOIN:
        RETURN_IF_ERROR(ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(out_batch));
        break;
      case TJoinOp::INNER_JOIN:
        RETURN_IF_ERROR(ProcessProbeBatch<TJoinOp::INNER_JOIN>(out_batch));
        break;
      case TJoinOp::LEFT_OUTER_JOIN:
        RETURN_IF_ERROR(ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(out_batch));
        break;
      case TJoinOp::LEFT_SEMI_JOIN:
        RETURN_IF_ERROR(ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(out_batch));
        break;
      case TJoinOp::RIGHT_OUTER_JOIN:
        RETURN_IF_ERROR(ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(out_batch));
        break;
      case TJoinOp::FULL_OUTER_JOIN:
        RETURN_IF_ERROR(ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(out_batch));
        break;
      default:
        stringstream ss;
        ss << "Unknown join type: " << join_op_;
        return Status(ss.str());
    }

    if (out_batch->AtCapacity() || ReachedLimit()) break;
    DCHECK(current_probe_row_ == NULL);

    // Try to continue from the current probe side input.
    if (input_partition_ == NULL) {
      RETURN_IF_ERROR(NextProbeRowBatch(state, out_batch));
    } else {
      RETURN_IF_ERROR(NextSpilledProbeRowBatch(state, out_batch));
    }

    // Got a batch, just keep going.
    if (probe_batch_pos_ == 0) continue;
    DCHECK_EQ(probe_batch_pos_, -1);

    // Finished up all probe rows for hash_partitions_.
    RETURN_IF_ERROR(CleanUpHashPartitions());
    if ((join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty()) {
      // There are some partitions that need to flush their unmatched build rows.
      break;
    }
    // Move onto the next partition.
    RETURN_IF_ERROR(PrepareNextPartition(state));
    if (input_partition_ == NULL) {
      *eos = true;
      break;
    }
  }

  if (ReachedLimit()) *eos = true;
  return Status::OK;
}

Status PartitionedHashJoinNode::OutputUnmatchedBuild(RowBatch* out_batch) {
  DCHECK(join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN);
  DCHECK(!output_build_partitions_.empty());
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  int num_conjuncts = conjunct_ctxs_.size();
  int num_rows_added = 0;

  while (!out_batch->AtCapacity() && !hash_tbl_iterator_.AtEnd()) {
    // Output remaining unmatched build rows.
    DCHECK(!hash_tbl_iterator_.matched());
    TupleRow* build_row = hash_tbl_iterator_.GetRow();
    int row_idx = out_batch->AddRow();
    TupleRow* out_row = out_batch->GetRow(row_idx);
    CreateOutputRow(out_row, NULL, build_row);
    if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      out_batch->CommitLastRow();
      ++num_rows_added;
    }
    // Move to the next unmatched entry
    hash_tbl_iterator_.NextUnmatched();
  }

  // If we reached the end of the hash table, then there are no other unmatched build
  // rows for this partition. In that case we need to close the partition, and move to the
  // next. If we have not reached the end of the hash table, it means that we reached
  // out_batch capacity and we need to continue to output unmatched build rows, without
  // closing the partition.
  if (hash_tbl_iterator_.AtEnd()) {
    output_build_partitions_.front()->Close();
    output_build_partitions_.pop_front();
    // Move to the next partition to output unmatched rows.
    if (!output_build_partitions_.empty()) {
      hash_tbl_iterator_ = output_build_partitions_.front()->hash_tbl_->FirstUnmatched();
    }
  }

  num_rows_returned_ += num_rows_added;
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK;
}

Status PartitionedHashJoinNode::BuildHashTables(RuntimeState* state) {
  int num_remaining_partitions = 0;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->build_rows()->num_rows() == 0) {
      hash_partitions_[i]->Close();
      continue;
    }
    // TODO: this unpin is unnecessary but makes it simple. We should pick the partitions
    // to keep in memory based on how much of the build tuples in that partition are on
    // disk/in memory.
    RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->UnpinAllBlocks());
    ++num_remaining_partitions;
  }
  if (num_remaining_partitions == 0) {
    eos_ = true;
    return Status::OK;
  }

  int64_t max_mem_build_tables = join_node_mem_tracker()->SpareCapacity();
  int num_tables_built = 0;

  if (max_mem_build_tables == -1) max_mem_build_tables = numeric_limits<int64_t>::max();
  // Reserve memory for the buffer needed to handle spilled probe rows.
  int max_num_spilled_partitions = hash_partitions_.size();
  max_mem_build_tables -=
      max_num_spilled_partitions * state->io_mgr()->max_read_buffer_size();

  // Greedily pick the first N partitions until we run out of memory.
  // TODO: We could do better. We know exactly how many rows are in the partition now so
  // this is an optimization problem (i.e. 0-1 knapsack) to find the subset of partitions
  // that fit and result in the least amount of IO.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->EstimatedInMemSize() < max_mem_build_tables) {
      bool built = false;
      RETURN_IF_ERROR(hash_partitions_[i]->BuildHashTable(state, &built));
      if (!built) {
        // Estimate was wrong, cleanup hash table.
        RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->UnpinAllBlocks());
        if (hash_partitions_[i]->hash_tbl_.get() != NULL) {
          hash_partitions_[i]->hash_tbl_->Close();
          hash_partitions_[i]->hash_tbl_.reset();
        }
        continue;
      }
      max_mem_build_tables -= hash_partitions_[i]->InMemSize();
      if (++num_tables_built == MAX_IN_MEM_BUILD_TABLES) break;
    }
  }

  // Initialize (reserve one buffer) for each probe partition that needs to spill
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->hash_tbl() != NULL) continue;
    RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->Init());
    RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->UnpinAllBlocks());
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::CleanUpHashPartitions() {
  DCHECK_EQ(probe_batch_pos_, -1);
  // At this point all the rows have been read from the probe side for all partitions in
  // hash_partitions_.
  LOG(ERROR) << "Probe Side Consumed\n" << DebugString();

  // Walk the partitions that had hash tables built for the probe phase and close them.
  // In the case of right outer and full outer joins, instead of closing those partitions,
  // add them to the list of partitions that need to output any unmatched build rows.
  // Thise partition will be closed by the function that actually outputs unmatched build
  // rows.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->hash_tbl() == NULL) {
      // Unpin the probe stream to free up more memory.
      RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->UnpinAllBlocks());
      spilled_partitions_.push_back(hash_partitions_[i]);
    } else {
      DCHECK_EQ(hash_partitions_[i]->probe_rows()->num_rows(), 0)
        << "No probe rows should have been spilled for this partition.";
      if (join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN) {
        if (output_build_partitions_.empty()) {
          hash_tbl_iterator_ = hash_partitions_[i]->hash_tbl_->FirstUnmatched();
        }
        output_build_partitions_.push_back(hash_partitions_[i]);
      } else {
        hash_partitions_[i]->Close();
      }
    }
  }
  hash_partitions_.clear();
  input_partition_ = NULL;
  return Status::OK;
}

void PartitionedHashJoinNode::AddToDebugString(int indent, stringstream* out) const {
  *out << " hash_tbl=";
  *out << string(indent * 2, ' ');
  *out << "HashTbl("
       << " build_exprs=" << Expr::DebugString(build_expr_ctxs_)
       << " probe_exprs=" << Expr::DebugString(probe_expr_ctxs_);
  *out << ")";
}

void PartitionedHashJoinNode::UpdateState(State s) {
  state_ = s;
  LOG(ERROR) << "Transitioned State:" << endl << DebugString();
}

string PartitionedHashJoinNode::PrintState() const {
  switch (state_) {
    case PARTITIONING_BUILD: return "PartitioningBuild";
    case PROCESSING_PROBE: return "ProcessingProbe";
    case PROBING_SPILLED_PARTITION: return "ProbingSpilledPartitions";
    case REPARTITIONING: return "Repartioning";
    default: DCHECK(false);
  }
  return "";
}

string PartitionedHashJoinNode::DebugString() const {
  stringstream ss;
  ss << "PartitionedHashJoinNode (op=" << join_op_ << " state=" << PrintState()
     << " #partitions=" << hash_partitions_.size()
     << " #spilled_partitions=" << spilled_partitions_.size()
     << ")" << endl;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    ss << i << ": ptr=" << hash_partitions_[i];
    if (hash_partitions_[i]->is_closed()) {
      ss << " closed" << endl;
      continue;
    }
    ss << endl
       << "   (Spilled) Build Rows: "
       << hash_partitions_[i]->build_rows()->num_rows() << endl;
    if (hash_partitions_[i]->hash_tbl() != NULL) {
      ss << "   Hash Table Rows: " << hash_partitions_[i]->hash_tbl()->size() << endl;
    }
    ss << "   (Spilled) Probe Rows: "
       << hash_partitions_[i]->probe_rows()->num_rows() << endl;
  }
  if (!spilled_partitions_.empty()) {
    ss << "SpilledPartitions" << endl;
    for (list<Partition*>::const_iterator it = spilled_partitions_.begin();
        it != spilled_partitions_.end(); ++it) {
      ss << "  Partition=" << *it << endl
        << "   Spilled Build Rows: "
        << (*it)->build_rows()->num_rows() << endl
        << "   Spilled Probe Rows: "
        << (*it)->probe_rows()->num_rows() << endl;
    }
  }
  return ss.str();
}

