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

#include "exec/partitioned-hash-join-builder.h"

#include <iomanip>
#include <numeric>

#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/runtime-filter.h"
#include "runtime/runtime-state.h"
#include "service/hs2-util.h"
#include "util/bloom-filter.h"
#include "util/cyclic-barrier.h"
#include "util/debug-util.h"
#include "util/min-max-filter.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

static const string PREPARE_FOR_READ_FAILED_ERROR_MSG =
    "Failed to acquire initial read "
    "buffer for stream in hash join node $0. Reducing query concurrency or increasing "
    "the memory limit may help this query to complete successfully.";

using namespace impala;
using strings::Substitute;

DataSink* PhjBuilderConfig::CreateSink(RuntimeState* state) const {
  // We have one fragment per sink, so we can use the fragment index as the sink ID.
  TDataSinkId sink_id = state->fragment().idx;
  ObjectPool* pool = state->obj_pool();
  return pool->Add(new PhjBuilder(sink_id, *this, state));
}

PhjBuilder* PhjBuilderConfig::CreateSink(BufferPool::ClientHandle* buffer_pool_client,
    int64_t spillable_buffer_size, int64_t max_row_buffer_size,
    RuntimeState* state) const {
  ObjectPool* pool = state->obj_pool();
  return pool->Add(new PhjBuilder(*this, buffer_pool_client, spillable_buffer_size,
        max_row_buffer_size, state));
}

Status PhjBuilderConfig::CreateConfig(FragmentState* state, int join_node_id,
    TJoinOp::type join_op, const RowDescriptor* build_row_desc,
    const std::vector<TEqJoinCondition>& eq_join_conjuncts,
    const std::vector<TRuntimeFilterDesc>& filters, uint32_t hash_seed,
    PhjBuilderConfig** sink) {
  ObjectPool* pool = state->obj_pool();
  TDataSink* tsink = pool->Add(new TDataSink());
  PhjBuilderConfig* data_sink = pool->Add(new PhjBuilderConfig());
  RETURN_IF_ERROR(data_sink->Init(state, join_node_id, join_op, build_row_desc,
      eq_join_conjuncts, filters, hash_seed, tsink));
  *sink = data_sink;
  return Status::OK();
}

void PhjBuilderConfig::Close() {
  ScalarExpr::Close(build_exprs_);
  ScalarExpr::Close(filter_exprs_);
  DataSinkConfig::Close();
}

Status PhjBuilderConfig::InitExprsAndFilters(FragmentState* state,
    const vector<TEqJoinCondition>& eq_join_conjuncts,
    const vector<TRuntimeFilterDesc>& filter_descs) {
  for (const TEqJoinCondition& eq_join_conjunct : eq_join_conjuncts) {
    ScalarExpr* build_expr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(eq_join_conjunct.right, *input_row_desc_, state, &build_expr));
    build_exprs_.push_back(build_expr);
    is_not_distinct_from_.push_back(eq_join_conjunct.is_not_distinct_from);
  }

  for (const TRuntimeFilterDesc& filter_desc : filter_descs) {
    DCHECK(state->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL ||
        filter_desc.is_broadcast_join || state->query_options().num_nodes == 1);
    DCHECK(!state->query_options().disable_row_runtime_filtering ||
        filter_desc.applied_on_partition_columns);
    // Skip over filters that are not produced by the instances of the builder, i.e.
    // broadcast filters where this instance was not selected as a filter producer.
    const vector<const TPlanFragmentInstanceCtx*>& instance_ctxs = state->instance_ctxs();
    // We can pick any instance since the filters produced should be the same for all
    // instances.
    const vector<TRuntimeFilterSource>& filters_produced =
        instance_ctxs[0]->filters_produced;
    auto it = std::find_if(filters_produced.begin(), filters_produced.end(),
        [this, &filter_desc](const TRuntimeFilterSource f) {
          return f.src_node_id == join_node_id_ && f.filter_id == filter_desc.filter_id;
        });
    if (it == filters_produced.end()) continue;
    ScalarExpr* filter_expr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(filter_desc.src_expr, *input_row_desc_, state, &filter_expr));
    filter_exprs_.push_back(filter_expr);
    filter_descs_.push_back(filter_desc);
  }

  hash_table_config_ = state->obj_pool()->Add(new HashTableConfig(build_exprs_,
      build_exprs_, PhjBuilder::HashTableStoresNulls(join_op_, is_not_distinct_from_),
      is_not_distinct_from_));
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

Status PhjBuilderConfig::Init(FragmentState* state, int join_node_id,
    TJoinOp::type join_op, const RowDescriptor* build_row_desc,
    const vector<TEqJoinCondition>& eq_join_conjuncts,
    const vector<TRuntimeFilterDesc>& filters, uint32_t hash_seed, TDataSink* tsink) {
  tsink->__isset.join_build_sink = true;
  tsink->join_build_sink.__set_dest_node_id(join_node_id);
  tsink->join_build_sink.__set_join_op(join_op);
  RETURN_IF_ERROR(JoinBuilderConfig::Init(*tsink, build_row_desc, state));
  hash_seed_ = hash_seed;
  return InitExprsAndFilters(state, eq_join_conjuncts, filters);
}

Status PhjBuilderConfig::Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
    FragmentState* state) {
  RETURN_IF_ERROR(JoinBuilderConfig::Init(tsink, input_row_desc, state));
  const TJoinBuildSink& build_sink = tsink.join_build_sink;
  hash_seed_ = build_sink.hash_seed;
  resource_profile_ = &tsink.resource_profile;
  return InitExprsAndFilters(
      state, tsink.join_build_sink.eq_join_conjuncts, build_sink.runtime_filters);
}

const char* PhjBuilder::LLVM_CLASS_NAME = "class.impala::PhjBuilder";

PhjBuilder::PhjBuilder(
    TDataSinkId sink_id, const PhjBuilderConfig& sink_config, RuntimeState* state)
  : JoinBuilder(sink_id, sink_config,
      ConstructBuilderName("Hash", sink_config.join_node_id_), state),
    runtime_state_(state),
    hash_seed_(sink_config.hash_seed_),
    resource_profile_(sink_config.resource_profile_),
    reservation_manager_(),
    buffer_pool_client_(reservation_manager_.buffer_pool_client()),
    spillable_buffer_size_(resource_profile_->spillable_buffer_size),
    max_row_buffer_size_(resource_profile_->max_row_buffer_size),
    build_exprs_(sink_config.build_exprs_),
    is_not_distinct_from_(sink_config.is_not_distinct_from_),
    filter_exprs_(sink_config.filter_exprs_),
    hash_table_config_(*sink_config.hash_table_config_),
    probe_barrier_(num_probe_threads_ <= 1 ?
            nullptr :
            make_unique<CyclicBarrier>(state->instance_ctx().num_join_build_outputs)),
    process_build_batch_fn_(sink_config.process_build_batch_fn_),
    process_build_batch_fn_level0_(sink_config.process_build_batch_fn_level0_),
    insert_batch_fn_(sink_config.insert_batch_fn_),
    insert_batch_fn_level0_(sink_config.insert_batch_fn_level0_) {
  DCHECK_GT(sink_config.hash_seed_, 0);
  DCHECK(num_probe_threads_ <= 1 || !NeedToProcessUnmatchedBuildRows(join_op_))
      << "Returning rows with build partitions is not supported with shared builds";
  for (const TRuntimeFilterDesc& filter_desc : sink_config.filter_descs_) {
    filter_ctxs_.emplace_back();
    filter_ctxs_.back().filter = state->filter_bank()->RegisterProducer(filter_desc);
  }
  // Ensure threads get unblocked from probe_barrier_ when the query is cancelled. Using
  // the AddBarrierToCancel() mechanism ensures that cancellation happens after the
  // overall error for this backend has already been set in QueryState. Otherwise this
  // status and the original status could race with each other to become this backend's
  // status.
  if (probe_barrier_ != nullptr) state->AddBarrierToCancel(probe_barrier_.get());
}

PhjBuilder::PhjBuilder(const PhjBuilderConfig& sink_config,
    BufferPool::ClientHandle* buffer_pool_client, int64_t spillable_buffer_size,
    int64_t max_row_buffer_size, RuntimeState* state)
  : JoinBuilder(
      -1, sink_config, ConstructBuilderName("Hash", sink_config.join_node_id_), state),
    runtime_state_(state),
    hash_seed_(sink_config.hash_seed_),
    resource_profile_(nullptr),
    reservation_manager_(),
    buffer_pool_client_(buffer_pool_client),
    spillable_buffer_size_(spillable_buffer_size),
    max_row_buffer_size_(max_row_buffer_size),
    build_exprs_(sink_config.build_exprs_),
    is_not_distinct_from_(sink_config.is_not_distinct_from_),
    filter_exprs_(sink_config.filter_exprs_),
    hash_table_config_(*sink_config.hash_table_config_),
    probe_barrier_(nullptr),
    process_build_batch_fn_(sink_config.process_build_batch_fn_),
    process_build_batch_fn_level0_(sink_config.process_build_batch_fn_level0_),
    insert_batch_fn_(sink_config.insert_batch_fn_),
    insert_batch_fn_level0_(sink_config.insert_batch_fn_level0_) {
  DCHECK_GT(sink_config.hash_seed_, 0);
  DCHECK_EQ(1, num_probe_threads_) << "Embedded builders cannot be shared";
  for (const TRuntimeFilterDesc& filter_desc : sink_config.filter_descs_) {
    filter_ctxs_.emplace_back();
    filter_ctxs_.back().filter = state->filter_bank()->RegisterProducer(filter_desc);
  }
}

PhjBuilder::~PhjBuilder() {}

Status PhjBuilder::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));

  partitions_created_ = ADD_COUNTER(profile(), "PartitionsCreated", TUnit::UNIT);
  largest_partition_percent_ =
      profile()->AddHighWaterMarkCounter("LargestPartitionPercent", TUnit::UNIT);
  max_partition_level_ =
      profile()->AddHighWaterMarkCounter("MaxPartitionLevel", TUnit::UNIT);
  num_build_rows_ = ADD_COUNTER(profile(), "BuildRows", TUnit::UNIT);
  ht_stats_profile_ = HashTable::AddHashTableCounters(profile());
  num_spilled_partitions_ = ADD_COUNTER(profile(), "SpilledPartitions", TUnit::UNIT);
  num_repartitions_ = ADD_COUNTER(profile(), "NumRepartitions", TUnit::UNIT);
  partition_build_rows_timer_ = ADD_TIMER(profile(), "BuildRowsPartitionTime");
  build_hash_table_timer_ = ADD_TIMER(profile(), "HashTablesBuildTime");
  num_hash_table_builds_skipped_ =
      ADD_COUNTER(profile(), "NumHashTableBuildsSkipped", TUnit::UNIT);
  repartition_timer_ = ADD_TIMER(profile(), "RepartitionTime");

  if (is_separate_build_) {
    const TDebugOptions& instance_debug_options = state->instance_ctx().debug_options;
    bool debug_option_enabled = instance_debug_options.node_id == -1
        || instance_debug_options.node_id == join_node_id_;
    // SET_DENY_RESERVATION_PROBABILITY should behave the same as if it were applied to
    // the join node.
    reservation_manager_.Init(Substitute("$0 ptr=$1", name_, this), profile(),
        state->instance_buffer_reservation(), mem_tracker_.get(), *resource_profile_,
        debug_option_enabled ? instance_debug_options : TDebugOptions());
  }

  RETURN_IF_ERROR(HashTableCtx::Create(&obj_pool_, state, hash_table_config_, hash_seed_,
      MAX_PARTITION_DEPTH, row_desc_->tuple_descriptors().size(), expr_perm_pool_.get(),
      expr_results_pool_.get(), expr_results_pool_.get(), &ht_ctx_));

  RETURN_IF_ERROR(DebugAction(state->query_options(), "PHJ_BUILDER_PREPARE"));

  DCHECK_EQ(filter_exprs_.size(), filter_ctxs_.size());
  for (int i = 0; i < filter_exprs_.size(); ++i) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(*filter_exprs_[i], state, &obj_pool_,
        expr_perm_pool_.get(), expr_results_pool_.get(), &filter_ctxs_[i].expr_eval));
  }
  return Status::OK();
}

Status PhjBuilder::Open(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(DataSink::Open(state));
  if (!buffer_pool_client_->is_registered()) {
    DCHECK(is_separate_build_) << "Client is registered by PhjNode if not separate";
    DCHECK_GE(resource_profile_->min_reservation, MinReservation().second);
    RETURN_IF_ERROR(reservation_manager_.ClaimBufferReservation(state));
  }
  // Need to init here instead of constructor so that buffer_pool_client_ is registered.
  if (probe_stream_reservation_.is_closed()) {
    probe_stream_reservation_.Init(buffer_pool_client_);
  }

  RETURN_IF_ERROR(ht_ctx_->Open(state));

  for (const FilterContext& ctx : filter_ctxs_) {
    RETURN_IF_ERROR(ctx.expr_eval->Open(state));
  }
  if (ht_allocator_ == nullptr) {
    // Create 'ht_allocator_' on the first call to Open().
    ht_allocator_.reset(new Suballocator(ExecEnv::GetInstance()->buffer_pool(),
        buffer_pool_client_, spillable_buffer_size_));
  }
  RETURN_IF_ERROR(CreateHashPartitions(0));
  AllocateRuntimeFilters();

  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    RETURN_IF_ERROR(CreateAndPreparePartition(0, &null_aware_partition_));
  }
  return Status::OK();
}

Status PhjBuilder::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  SCOPED_TIMER(partition_build_rows_timer_);
  RETURN_IF_ERROR(AddBatch(batch));
  COUNTER_ADD(num_build_rows_, batch->num_rows());
  return Status::OK();
}

Status PhjBuilder::AddBatch(RowBatch* batch) {
  bool build_filters = ht_ctx_->level() == 0 && filter_ctxs_.size() > 0;

  PhjBuilderConfig::ProcessBuildBatchFn process_build_batch_fn;
  if (ht_ctx_->level() == 0) {
    process_build_batch_fn = process_build_batch_fn_level0_.load();
  } else {
    process_build_batch_fn = process_build_batch_fn_.load();
  }

  if (process_build_batch_fn != nullptr) {
    RETURN_IF_ERROR(process_build_batch_fn(this, batch, ht_ctx_.get(), build_filters,
          join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN));
  } else {
    RETURN_IF_ERROR(ProcessBuildBatch(batch, ht_ctx_.get(), build_filters,
          join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN));
  }
  // Free any expr result allocations made during partitioning.
  expr_results_pool_->Clear();
  DetermineUsefulnessForMinmaxFilters();
  return Status::OK();
}

// Set AlwaysTrue to true for each of minmax filter that is not useful.
void PhjBuilder::DetermineUsefulnessForMinmaxFilters() {
  // Loop over all remaining min/max filters still enabled (i.e. AlwaysTrue flag is false)
  for (std::vector<FilterContext*>::const_iterator it = minmax_filter_ctxs_.begin();
       it != minmax_filter_ctxs_.end();) {
    DCHECK((*it)->filter->is_min_max_filter() && (*it)->local_min_max_filter);
    // The filter can be in alwaysTrue state if not a single not-NULL value has been
    // inserted.
    if (UNLIKELY((*it)->local_min_max_filter->AlwaysTrue())) {
      ++it;
      continue;
    }
    const TRuntimeFilterDesc& filter_desc = (*it)->filter->filter_desc();
    bool useful = true;
    for (const auto& target_desc : filter_desc.targets) {
      if (FilterContext::ShouldRejectFilterBasedOnColumnStats(
              target_desc, (*it)->local_min_max_filter, minmax_filter_threshold_)) {
        useful = false;
        break;
      }
    }
    if (LIKELY(!useful)) {
      (*it)->local_min_max_filter->SetAlwaysTrue();
      VLOG(3) << "Filter with id " << (*it)->filter->id()
              << " at join builder (join_node_id_=" << join_node_id_ << ")"
              << " is not useful. Disabled."
              << " Fillter details=" << (*it)->local_min_max_filter->DebugString()
              << ", column stats:"
              << " low=" << PrintTColumnValue(filter_desc.targets[0].low_value)
              << ", high=" << PrintTColumnValue(filter_desc.targets[0].high_value)
              << ", threshold=" << minmax_filter_threshold_;
      it = minmax_filter_ctxs_.erase(it);
    } else {
      ++it;
    }
  }
}

Status PhjBuilder::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  return FinalizeBuild(state);
}

Status PhjBuilder::FinalizeBuild(RuntimeState* state) {
  int64_t num_build_rows = 0;
  for (const unique_ptr<PhjBuilderPartition>& partition : hash_partitions_) {
    num_build_rows += partition->build_rows()->num_rows();
    partition->build_rows()->DoneWriting();
  }

  if (num_build_rows > 0) {
    double largest_fraction = 0.0;
    for (const unique_ptr<PhjBuilderPartition>& partition : hash_partitions_) {
      largest_fraction = max(largest_fraction,
          partition->build_rows()->num_rows() / static_cast<double>(num_build_rows));
    }
    COUNTER_SET(largest_partition_percent_, static_cast<int64_t>(largest_fraction * 100));
  }

  if (VLOG_IS_ON(2)) {
    stringstream ss;
    ss << Substitute("PHJ(node_id=$0) partitioned(level=$1) $2 rows into:", join_node_id_,
        hash_partitions_[0]->level(), num_build_rows);
    for (int i = 0; i < hash_partitions_.size(); ++i) {
      PhjBuilderPartition* partition = hash_partitions_[i].get();
      double percent = num_build_rows == 0 ? 0.0 : partition->build_rows()->num_rows()
              * 100 / static_cast<double>(num_build_rows);
      ss << "  " << i << " " << (partition->is_spilled() ? "spilled" : "not spilled")
         << " (fraction=" << fixed << setprecision(2) << percent << "%)" << endl
         << "    #rows:" << partition->build_rows()->num_rows() << endl;
    }
    if (null_aware_partition_ != nullptr) {
      ss << " Null-aware partition: " << null_aware_partition_->DebugString();
    }
    VLOG(2) << ss.str();
  }

  if (ht_ctx_->level() == 0) {
    PublishRuntimeFilters(num_build_rows);
    non_empty_build_ |= (num_build_rows > 0);

    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      if (null_aware_partition_->is_spilled()) {
        // Free up memory for the hash tables of other partitions by unpinning the
        // last block of the null aware partition's stream.
        RETURN_IF_ERROR(null_aware_partition_->Spill(BufferedTupleStream::UNPIN_ALL));
      } else {
        // Invalidate the write iterator so we can safely do concurrent reads later.
        null_aware_partition_->build_rows()->DoneWriting();
      }
    }
  }

  HashJoinState next_state;
  if (state_ == HashJoinState::PARTITIONING_BUILD) {
    next_state = HashJoinState::PARTITIONING_PROBE;
  } else {
    DCHECK_ENUM_EQ(state_, HashJoinState::REPARTITIONING_BUILD);
    next_state = HashJoinState::REPARTITIONING_PROBE;
  }
  RETURN_IF_ERROR(BuildHashTablesAndReserveProbeBuffers(next_state));
  UpdateState(next_state);
  if (state_ == HashJoinState::PARTITIONING_PROBE && is_separate_build_) {
    HandoffToProbesAndWait(state);
  }
  return Status::OK();
}

void PhjBuilder::Close(RuntimeState* state) {
  if (closed_) return;
  CloseAndDeletePartitions(nullptr);
  if (ht_ctx_ != nullptr) {
    ht_ctx_->StatsCountersAdd(ht_stats_profile_.get());
    ht_ctx_->Close(state);
    ht_ctx_.reset();
  }
  for (const FilterContext& ctx : filter_ctxs_) {
    if (ctx.expr_eval != nullptr) ctx.expr_eval->Close(state);
  }
  obj_pool_.Clear();
  probe_stream_reservation_.Close();
  if (is_separate_build_) reservation_manager_.Close(state);
  DataSink::Close(state);
  closed_ = true;
}

void PhjBuilder::Reset(RowBatch* row_batch) {
  DCHECK(!is_separate_build_);
  DCHECK_EQ(0, probe_stream_reservation_.GetReservation());
  state_ = HashJoinState::PARTITIONING_BUILD;
  expr_results_pool_->Clear();
  non_empty_build_ = false;
  next_partition_id_ = 0;
  CloseAndDeletePartitions(row_batch);
}

void PhjBuilder::UpdateState(HashJoinState next_state) {
  // Validate the state transition.
  switch (state_) {
    case HashJoinState::PARTITIONING_BUILD:
      DCHECK_ENUM_EQ(next_state, HashJoinState::PARTITIONING_PROBE);
      break;
    case HashJoinState::PARTITIONING_PROBE:
    case HashJoinState::REPARTITIONING_PROBE:
    case HashJoinState::PROBING_SPILLED_PARTITION:
      DCHECK(next_state == HashJoinState::REPARTITIONING_BUILD
          || next_state == HashJoinState::PROBING_SPILLED_PARTITION);
      break;
    case HashJoinState::REPARTITIONING_BUILD:
      DCHECK_ENUM_EQ(next_state, HashJoinState::REPARTITIONING_PROBE);
      break;
    default:
      DCHECK(false) << "Invalid state " << static_cast<int>(state_);
  }
  state_ = next_state;
  VLOG(2) << "Transitioned State:" << endl << DebugString();
}

string PhjBuilder::PrintState(HashJoinState state) {
  switch (state) {
    case HashJoinState::PARTITIONING_BUILD:
      return "PartitioningBuild";
    case HashJoinState::PARTITIONING_PROBE:
      return "PartitioningProbe";
    case HashJoinState::PROBING_SPILLED_PARTITION:
      return "ProbingSpilledPartition";
    case HashJoinState::REPARTITIONING_BUILD:
      return "RepartitioningBuild";
    case HashJoinState::REPARTITIONING_PROBE:
      return "RepartitioningProbe";
    default:
      DCHECK(false);
  }
  return "";
}

Status PhjBuilder::CreateAndPreparePartition(
    int level, unique_ptr<PhjBuilderPartition>* partition) {
  *partition = make_unique<PhjBuilderPartition>(runtime_state_, this, level);
  Status status = (*partition)->build_rows()->Init(name_, true);
  if (!status.ok()) goto error;
  bool got_buffer;
  status = (*partition)->build_rows()->PrepareForWrite(&got_buffer);
  if (!status.ok()) goto error;
  DCHECK(got_buffer)
      << "Accounted in min reservation" << buffer_pool_client_->DebugString();
  return Status::OK();
 error:
  (*partition)->Close(nullptr);
  partition->reset();
  return status;
}

Status PhjBuilder::CreateHashPartitions(int level) {
  DCHECK(hash_partitions_.empty());
  ht_ctx_->set_level(level); // Set the hash function for partitioning input.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    unique_ptr<PhjBuilderPartition> new_partition;
    RETURN_IF_ERROR(CreateAndPreparePartition(level, &new_partition));
    hash_partitions_.push_back(std::move(new_partition));
  }
  COUNTER_ADD(partitions_created_, PARTITION_FANOUT);
  COUNTER_SET(max_partition_level_, level);
  return Status::OK();
}

bool PhjBuilder::AppendRowStreamFull(
    BufferedTupleStream* stream, TupleRow* row, Status* status) noexcept {
  while (true) {
    // We ran out of memory. Pick a partition to spill. If we ran out of unspilled
    // partitions, SpillPartition() will return an error status.
    *status = SpillPartition(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
    if (!status->ok()) return false;
    if (stream->AddRow(row, status)) return true;
    if (!status->ok()) return false;
    // Spilling one partition does not guarantee we can append a row. Keep
    // spilling until we can append this row.
  }
}

// TODO: can we do better with a different spilling heuristic?
Status PhjBuilder::SpillPartition(BufferedTupleStream::UnpinMode mode,
    PhjBuilderPartition** spilled_partition) {
  DCHECK_EQ(hash_partitions_.size(), PARTITION_FANOUT);
  PhjBuilderPartition* best_candidate = nullptr;
  if (null_aware_partition_ != nullptr && null_aware_partition_->CanSpill()) {
    // Spill null-aware partition first if possible - it is always processed last.
    best_candidate = null_aware_partition_.get();
  } else {
    // Iterate over the partitions and pick the largest partition to spill.
    int64_t max_freed_mem = 0;
    for (const unique_ptr<PhjBuilderPartition>& candidate : hash_partitions_) {
      if (!candidate->CanSpill()) continue;
      int64_t mem = candidate->build_rows()->BytesPinned(false);
      if (candidate->hash_tbl() != nullptr) {
        // The hash table should not have matches, since we have not probed it yet.
        // Losing match info would lead to incorrect results (IMPALA-1488).
        DCHECK(!candidate->hash_tbl()->HasMatches());
        mem += candidate->hash_tbl()->ByteSize();
      }
      if (mem > max_freed_mem) {
        max_freed_mem = mem;
        best_candidate = candidate.get();
      }
    }
  }

  if (best_candidate == nullptr) {
    return Status(Substitute("Internal error: could not find a partition to spill in "
                             " hash join $0: \n$1\nClient:\n$2",
        join_node_id_, DebugString(), buffer_pool_client_->DebugString()));
  }

  VLOG(2) << "Spilling partition: " << best_candidate->DebugString() << endl
          << DebugString();
  RETURN_IF_ERROR(best_candidate->Spill(mode));
  if (spilled_partition != nullptr) *spilled_partition = best_candidate;
  return Status::OK();
}

// When this function is called, we've finished processing the current build input
// (either from the child ExecNode or from repartitioning a spilled partition). The build
// rows have only been partitioned, we still need to build hash tables over them. Some
// of the partitions could have already been spilled and attempting to build hash
// tables over the non-spilled ones can cause them to spill.
//
// At the end of the function all partitions either have a hash table (and therefore are
// not spilled) or are spilled. Partitions that have hash tables don't need to spill on
// the probe side.
//
// This maps perfectly to a 0-1 knapsack where the weight is the memory to keep the
// build rows and hash table and the value is the expected IO savings.
// For now, we go with a greedy solution.
//
// TODO: implement the knapsack solution.
Status PhjBuilder::BuildHashTablesAndReserveProbeBuffers(HashJoinState next_state) {
  DCHECK_EQ(PARTITION_FANOUT, hash_partitions_.size());

  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilderPartition* partition = hash_partitions_[i].get();
    if (partition->build_rows()->num_rows() == 0) {
      // This partition is empty, no need to do anything else.
      partition->Close(nullptr);
    } else if (partition->is_spilled()) {
      // We don't need any build-side data for spilled partitions in memory.
      RETURN_IF_ERROR(
          partition->build_rows()->UnpinStream(BufferedTupleStream::UNPIN_ALL));
    }
  }

  // TODO: the below logic could be improved calculating upfront how much memory is needed
  // for each hash table, and only building hash tables that will eventually fit in
  // memory. In some cases now we could build a hash table, then spill the partition
  // later.

  // Allocate probe buffers for all partitions that are already spilled. Do this before
  // building hash tables because allocating probe buffers may cause some more partitions
  // to be spilled. This avoids wasted work on building hash tables for partitions that
  // won't fit in memory alongside the required probe buffers.
  RETURN_IF_ERROR(ReserveProbeBuffers(next_state));

  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilderPartition* partition = hash_partitions_[i].get();
    if (partition->IsClosed() || partition->is_spilled()) continue;

    bool built = false;
    DCHECK(partition->build_rows()->is_pinned());
    RETURN_IF_ERROR(partition->BuildHashTable(&built));
    // If we did not have enough memory to build this hash table, we need to spill this
    // partition (clean up the hash table, unpin build).
    if (!built) RETURN_IF_ERROR(partition->Spill(BufferedTupleStream::UNPIN_ALL));
  }
  // We may have spilled additional partitions while building hash tables, we need to
  // reserve memory for the probe buffers for those additional spilled partitions.
  RETURN_IF_ERROR(ReserveProbeBuffers(next_state));
  if (is_separate_build_) {
    // The builder may have some surplus reservation. Release it so that it can be
    // used by the probe side or by other operators.
    RETURN_IF_ERROR(reservation_manager_.ReleaseUnusedReservation());
  }
  return Status::OK();
}

Status PhjBuilder::ReserveProbeBuffers(HashJoinState next_state) {
  DCHECK_EQ(PARTITION_FANOUT, hash_partitions_.size());
  int64_t curr_reservation = probe_stream_reservation_.GetReservation();
  int64_t addtl_reservation =
      CalcProbeStreamReservation(next_state) * num_probe_threads_ - curr_reservation;

  // Loop until either we get enough reservation or all partitions are spilled (in which
  // case SpillPartition() returns an error).
  while (addtl_reservation > buffer_pool_client_->GetUnusedReservation()) {
    PhjBuilderPartition* spilled_partition;
    RETURN_IF_ERROR(SpillPartition(
          BufferedTupleStream::UNPIN_ALL, &spilled_partition));
    // Increase reservation to reflect the additional spilled partition.
    // Don't need to create a probe stream for the null-aware partition.
    if (spilled_partition != null_aware_partition_.get()) {
      addtl_reservation += spillable_buffer_size_ * num_probe_threads_;
    }
  }
  VLOG(3) << "PHJ(node_id=" << join_node_id_ << ") saved reservation "
          << addtl_reservation;
  buffer_pool_client_->SaveReservation(&probe_stream_reservation_, addtl_reservation);
  return Status::OK();
}

Status PhjBuilder::BeginInitialProbe(
    BufferPool::ClientHandle* probe_client, HashPartitions* partitions) {
  DCHECK_EQ(is_separate_build_, probe_client != buffer_pool_client_);
  DCHECK_ENUM_EQ(state_, HashJoinState::PARTITIONING_PROBE);
  DCHECK_EQ(PARTITION_FANOUT, hash_partitions_.size());
  RETURN_IF_ERROR(TransferProbeStreamReservation(probe_client));
  *partitions = HashPartitions(ht_ctx_->level(), &hash_partitions_, non_empty_build_);
  return Status::OK();
}

Status PhjBuilder::TransferProbeStreamReservation(
    BufferPool::ClientHandle* probe_client) {
  DCHECK_EQ(is_separate_build_, probe_client != buffer_pool_client_);
  int64_t probe_reservation = CalcProbeStreamReservation(state_);
  VLOG(3) << "PHJ(node_id=" << join_node_id_ << ") will transfer " << probe_reservation
          << " to probe client.";
  if (probe_reservation == 0) return Status::OK();
  DCHECK_GE(probe_stream_reservation_.GetReservation(), probe_reservation);

  buffer_pool_client_->RestoreReservation(&probe_stream_reservation_, probe_reservation);
  if (is_separate_build_) {
    bool success;
    RETURN_IF_ERROR(buffer_pool_client_->TransferReservationTo(
          probe_client, probe_reservation, &success));
    DCHECK(success) << "Transferring within query shouldn't violate reservation limits.";
  }
  return Status::OK();
}

int64_t PhjBuilder::CalcProbeStreamReservation(HashJoinState next_state) const {
  // We need a read buffer if the input is a spilled partition (i.e. we are repartitioning
  // the input).
  bool need_probe_buffer;
  if (next_state == HashJoinState::PARTITIONING_PROBE) {
    need_probe_buffer = false;
  } else {
    DCHECK(next_state == HashJoinState::PROBING_SPILLED_PARTITION
        || next_state == HashJoinState::REPARTITIONING_PROBE)
      << static_cast<int>(next_state);
    DCHECK_GT(spilled_partitions_.size(), 0);
    need_probe_buffer = spilled_partitions_.back()->num_spilled_probe_rows() > 0;
  }
  DCHECK(next_state == HashJoinState::PROBING_SPILLED_PARTITION
      || hash_partitions_.size() > 0);
  int num_spilled_partitions = GetNumSpilledPartitions(hash_partitions_);
  int num_buffers = num_spilled_partitions + (need_probe_buffer ? 1 : 0);
  int num_max_sized_buffers =
      (num_spilled_partitions > 0 ? 1 : 0) + (need_probe_buffer ? 1 : 0);
  DCHECK_LE(num_max_sized_buffers, num_buffers);
  return num_max_sized_buffers * max_row_buffer_size_ +
         (num_buffers - num_max_sized_buffers) * spillable_buffer_size_;
}

int PhjBuilder::GetNumSpilledPartitions(
    const vector<unique_ptr<PhjBuilderPartition>>& partitions) {
  int num_spilled = 0;
  for (int i = 0; i < partitions.size(); ++i) {
    PhjBuilderPartition* partition = partitions[i].get();
    DCHECK(partition != nullptr);
    if (!partition->IsClosed() && partition->is_spilled()) ++num_spilled;
  }
  return num_spilled;
}

Status PhjBuilder::DoneProbingHashPartitions(
    const int64_t num_spilled_probe_rows[PARTITION_FANOUT],
    BufferPool::ClientHandle* probe_client, RuntimeProfile* probe_profile,
    deque<unique_ptr<PhjBuilderPartition>>* output_partitions, RowBatch* batch) {
  DCHECK_EQ(is_separate_build_, probe_client != buffer_pool_client_);
  DCHECK(output_partitions->empty());
  VLOG(3) << "PHJ(node_id=" << join_node_id_ << ") done probing hash partitions.";
  // Calculate the reservation before cleaning up 'hash_partitions_' and
  // 'spilled_partitions_', which the calculation depends on.
  int64_t probe_reservation = CalcProbeStreamReservation(state_);
  DCHECK_GE(probe_client->GetUnusedReservation(), probe_reservation);

  // Merge together num_spilled_probe_rows to include info from all threads.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilderPartition* partition = hash_partitions_[i].get();
    if (partition->IsClosed()) continue;
    partition->IncrementNumSpilledProbeRows(num_spilled_probe_rows[i]);
  }

  if (num_probe_threads_ > 1) {
    SCOPED_TIMER(probe_profile->inactive_timer());
    // TODO: IMPALA-9411: consider reworking to attach buffers to all output batches.
    RETURN_IF_ERROR(probe_barrier_->Wait([&]() {
      CleanUpHashPartitions(output_partitions, nullptr);
      DCHECK_EQ(0, output_partitions->size())
          << "Cannot share build for join modes that return rows from build partitions";
      return Status::OK();
    }));
  } else if (is_separate_build_) {
    SCOPED_TIMER(probe_profile->inactive_timer());
    CleanUpHashPartitions(output_partitions, batch);
  } else {
    // No need to activate probe's inactive timer, since the builder will be a child of
    // the probe and its time will be subtracted from probe's total time.
    CleanUpHashPartitions(output_partitions, batch);
  }

  if (is_separate_build_) {
    bool success;
    RETURN_IF_ERROR(probe_client->TransferReservationTo(
        buffer_pool_client_, probe_reservation, &success));
    DCHECK(success) << "Transferring within query shouldn't violate reservation limits.";
  }
  return Status::OK();
}

void PhjBuilder::CleanUpHashPartitions(
    deque<unique_ptr<PhjBuilderPartition>>* output_partitions, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  if (state_ == HashJoinState::REPARTITIONING_PROBE) {
    // Finished repartitioning this partition. Discard before pushing more spilled
    // partitions onto 'spilled_partitions_'.
    DCHECK(!spilled_partitions_.empty());
    spilled_partitions_.pop_back();
  }

  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    unique_ptr<PhjBuilderPartition> partition = std::move(hash_partitions_[i]);
    if (partition->IsClosed()) continue;
    if (partition->is_spilled()) {
      DCHECK(partition->hash_tbl() == nullptr) << DebugString();
      DCHECK_EQ(partition->build_rows()->BytesPinned(false), 0)
          << "Build was fully unpinned in BuildHashTablesAndPrepareProbeStreams()";
      if (partition->num_spilled_probe_rows() == 0
          && !NeedToProcessUnmatchedBuildRows(join_op_)
          && num_probe_threads_ == 1) {
        COUNTER_ADD(num_hash_table_builds_skipped_, 1);
        partition->Close(nullptr);
      } else {
        // For shared builds, always add the partition to keep the spilled partitions
        // in sync across all the builders and join nodes.
        spilled_partitions_.push_back(std::move(partition));
      }
    } else if (NeedToProcessUnmatchedBuildRows(join_op_)) {
      output_partitions->push_back(std::move(partition));
    } else {
      // No more processing is required for this partition.
      partition->Close(batch);
    }
  }
  hash_partitions_.clear();
}

Status PhjBuilder::DoneProbingSinglePartition(BufferPool::ClientHandle* probe_client,
    RuntimeProfile* probe_profile,
    deque<unique_ptr<PhjBuilderPartition>>* output_partitions, RowBatch* batch) {
  VLOG(3) << "PHJ(node_id=" << join_node_id_ << ") done probing single partition.";
  DCHECK_EQ(is_separate_build_, probe_client != buffer_pool_client_);
  // Calculate before popping off the last 'spilled_partition_'.
  int64_t probe_reservation = CalcProbeStreamReservation(state_);
  DCHECK_GE(probe_client->GetUnusedReservation(), probe_reservation);
  if (num_probe_threads_ > 1) {
    SCOPED_TIMER(probe_profile->inactive_timer());
    // TODO: IMPALA-9411: consider reworking to attach buffers to all output batches.
    RETURN_IF_ERROR(probe_barrier_->Wait([&]() {
      CleanUpSinglePartition(output_partitions, nullptr);
      DCHECK_EQ(0, output_partitions->size())
          << "Cannot share build for join modes that return rows from build partitions";
      return Status::OK();
    }));
  } else if (is_separate_build_) {
    SCOPED_TIMER(probe_profile->inactive_timer());
    CleanUpSinglePartition(output_partitions, batch);
  } else {
    // No need to activate probe's inactive timer, since the builder will be a child of
    // the probe and its time will be subtracted from probe's total time.
    CleanUpSinglePartition(output_partitions, batch);
  }
  if (is_separate_build_) {
    bool success;
    RETURN_IF_ERROR(probe_client->TransferReservationTo(
          buffer_pool_client_, probe_reservation, &success));
    DCHECK(success) << "Transferring within query shouldn't violate reservation limits.";
  }
  return Status::OK();
}

void PhjBuilder::CleanUpSinglePartition(
    deque<unique_ptr<PhjBuilderPartition>>* output_partitions, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  if (NeedToProcessUnmatchedBuildRows(join_op_)) {
    DCHECK_LE(num_probe_threads_, 1)
        << "Don't support returning build partitions with shared build";
    // If the build partition was in memory, we are done probing this partition.
    // In case of right-outer, right-anti and full-outer joins, we move this partition
    // to the list of partitions that we need to output their unmatched build rows.
    output_partitions->push_back(std::move(spilled_partitions_.back()));
  } else {
    // In any other case, just close the input build partition.
    spilled_partitions_.back()->Close(IsLeftSemiJoin(join_op_) ? nullptr : batch);
  }
  spilled_partitions_.pop_back();
}

Status PhjBuilder::BeginNullAwareProbe() {
  DCHECK(null_aware_partition_ != nullptr);
  if (num_probe_threads_ > 1) {
    return probe_barrier_->Wait([&]() {
      return BeginNullAwareProbeSerial();
    });
  } else {
    return BeginNullAwareProbeSerial();
  }
}

Status PhjBuilder::BeginNullAwareProbeSerial() {
  BufferedTupleStream* build_rows = null_aware_partition_->build_rows();
  bool pinned;
  RETURN_IF_ERROR(build_rows->PinStream(&pinned));
  if (!pinned) {
    return Status(TErrorCode::NAAJ_OUT_OF_MEMORY, build_rows->num_rows(),
        PrettyPrinter::PrintBytes(build_rows->byte_size()),
        PrettyPrinter::PrintBytes(
            buffer_pool_client_->GetUnusedReservation() + build_rows->BytesPinned(false)),
        PrettyPrinter::PrintBytes(buffer_pool_client_->GetReservation()));
  }
  return Status::OK();
}

Status PhjBuilder::DoneProbingNullAwarePartition() {
  DCHECK(null_aware_partition_ != nullptr);
  if (num_probe_threads_ > 1) {
    RETURN_IF_ERROR(probe_barrier_->Wait([&]() {
      CloseNullAwarePartition();
      return Status::OK();
    }));
  } else {
    CloseNullAwarePartition();
  }
  return Status::OK();
}

void PhjBuilder::CloseNullAwarePartition() {
  if (null_aware_partition_ == nullptr) return;
  // We don't need to pass in a batch because the anti-join only returns tuple data
  // from the probe side - i.e. the RowDescriptor for PartitionedHashJoinNode does
  // not include the build tuple.
  null_aware_partition_->Close(nullptr);
  null_aware_partition_.reset();
}

void PhjBuilder::CloseAndDeletePartitions(RowBatch* row_batch) {
  // Close all the partitions and clean up all references to them.
  for (unique_ptr<PhjBuilderPartition>& partition : hash_partitions_) {
    partition->Close(row_batch);
  }
  hash_partitions_.clear();
  for (unique_ptr<PhjBuilderPartition>& partition : spilled_partitions_) {
    partition->Close(row_batch);
  }
  spilled_partitions_.clear();
  CloseNullAwarePartition();
}

void PhjBuilder::AllocateRuntimeFilters() {
  DCHECK(join_op_ != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN || filter_ctxs_.size() == 0)
      << "Runtime filters not supported with NULL_AWARE_LEFT_ANTI_JOIN";
  DCHECK(ht_ctx_ != nullptr);
  for (FilterContext& filter_ctx : filter_ctxs_) {
    if (filter_ctx.filter->is_bloom_filter()) {
      filter_ctx.local_bloom_filter =
          runtime_state_->filter_bank()->AllocateScratchBloomFilter(
              filter_ctx.filter->id());
    } else if (filter_ctx.filter->is_min_max_filter()) {
      filter_ctx.local_min_max_filter =
          runtime_state_->filter_bank()->AllocateScratchMinMaxFilter(
              filter_ctx.filter->id(), filter_ctx.expr_eval->root().type());
      minmax_filter_ctxs_.push_back(&filter_ctx);
    } else {
      DCHECK(filter_ctx.filter->is_in_list_filter());
      filter_ctx.local_in_list_filter =
          runtime_state_->filter_bank()->AllocateScratchInListFilter(
              filter_ctx.filter->id(), filter_ctx.expr_eval->root().type());
    }
  }

  minmax_filter_threshold_ =
      (float)(runtime_state_->query_options().minmax_filter_threshold);
}

void PhjBuilder::InsertRuntimeFilters(
    FilterContext filter_ctxs[], TupleRow* build_row) noexcept {
  // For the interpreted path we can directly use the filter_ctxs_ member variable.
  DCHECK_EQ(filter_ctxs_.data(), filter_ctxs);
  for (const FilterContext& ctx : filter_ctxs_) ctx.Insert(build_row);
}

void PhjBuilder::PublishRuntimeFilters(int64_t num_build_rows) {
  JoinBuilder::PublishRuntimeFilters(
      filter_ctxs_, runtime_state_, minmax_filter_threshold_, num_build_rows);
}

Status PhjBuilder::BeginSpilledProbe(BufferPool::ClientHandle* probe_client,
    RuntimeProfile* probe_profile, bool* repartitioned,
    PhjBuilderPartition** input_partition, HashPartitions* new_partitions) {
  DCHECK_EQ(is_separate_build_, probe_client != buffer_pool_client_);
  DCHECK(!spilled_partitions_.empty());
  DCHECK_EQ(0, hash_partitions_.size());

  if (num_probe_threads_ > 1) {
    SCOPED_TIMER(probe_profile->inactive_timer());
    RETURN_IF_ERROR(probe_barrier_->Wait([&]() { return BeginSpilledProbeSerial(); }));
  } else if (is_separate_build_) {
    SCOPED_TIMER(probe_profile->inactive_timer());
    RETURN_IF_ERROR(BeginSpilledProbeSerial());
  } else {
    // No need to activate probe's inactive timer, since the builder will be a child of
    // the probe and its time will be subtracted from probe's total time.
    RETURN_IF_ERROR(BeginSpilledProbeSerial());
  }

  RETURN_IF_ERROR(TransferProbeStreamReservation(probe_client));
  *input_partition = spilled_partitions_.back().get();
  if (state_ == HashJoinState::PROBING_SPILLED_PARTITION) {
    *repartitioned = false;
  } else {
    DCHECK_ENUM_EQ(HashJoinState::REPARTITIONING_PROBE, state_);
    *repartitioned = true;
    *new_partitions =
        HashPartitions(ht_ctx_->level(), &hash_partitions_, non_empty_build_);
  }
  return Status::OK();
}

Status PhjBuilder::BeginSpilledProbeSerial() {
  SCOPED_TIMER(profile()->total_time_counter());
  DCHECK_EQ(0, probe_stream_reservation_.GetReservation());
  if (is_separate_build_ || join_op_ != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    DCHECK_EQ(0, buffer_pool_client_->GetUsedReservation())
        << "All memory should be available to bring the spilled partition into memory: "
        << "all build and probe data shuld be spilled to disk. THe only exception is "
        << "NAAJ probe streams, which are accounted for in the PHJ node separately.";
  }

  // Pick the next spilled partition to process. The partition will stay in
  // 'spilled_partitions_' until we are done probing it or repartitioning its probe.
  // Thus it will remain valid as long as it's needed and always get cleaned up in
  // Close(), even if an error occurs.
  PhjBuilderPartition* partition = spilled_partitions_.back().get();
  DCHECK(partition->is_spilled()) << partition->DebugString();

  if (partition->num_spilled_probe_rows() == 0 && num_probe_threads_ == 1) {
    // If there are no probe rows, there's no need to build the hash table, and
    // only partitions with NeedToProcessUnmatchedBuildRows() will have been added
    // to 'spilled_partitions_' in DoneProbingHashPartitions().
    DCHECK(NeedToProcessUnmatchedBuildRows(join_op_));
    bool got_read_buffer = false;
    RETURN_IF_ERROR(partition->build_rows()->PrepareForRead(true, &got_read_buffer));
    if (!got_read_buffer) {
      return mem_tracker()->MemLimitExceeded(
          runtime_state_, Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, join_node_id_));
    }
    COUNTER_ADD(num_hash_table_builds_skipped_, 1);
    UpdateState(HashJoinState::PROBING_SPILLED_PARTITION);
    return Status::OK();
  }

  // Set aside memory required for reading the probe stream, so that we don't use
  // it for the hash table.
  bool need_probe_buffer = partition->num_spilled_probe_rows() > 0;
  int64_t saved_probe_reservation =
      need_probe_buffer ? max_row_buffer_size_ * num_probe_threads_ : 0;
  buffer_pool_client_->SaveReservation(
      &probe_stream_reservation_, saved_probe_reservation);
  VLOG(3) << "PHJ(node_id=" << join_node_id_ << ") saved " << saved_probe_reservation
          << " for probe clients.";

  // Try to build a hash table for the spilled build partition.
  bool built;
  RETURN_IF_ERROR(partition->BuildHashTable(&built));
  if (built) {
    UpdateState(HashJoinState::PROBING_SPILLED_PARTITION);
    return Status::OK();
  }
  // This build partition still does not fit in memory, repartition.
  UpdateState(HashJoinState::REPARTITIONING_BUILD);

  int next_partition_level = partition->level() + 1;
  if (UNLIKELY(next_partition_level >= MAX_PARTITION_DEPTH)) {
    return Status(TErrorCode::PARTITIONED_HASH_JOIN_MAX_PARTITION_DEPTH, join_node_id_,
        MAX_PARTITION_DEPTH);
  }

  // Spill to free memory from hash tables and pinned streams for use in new partitions.
  RETURN_IF_ERROR(partition->Spill(BufferedTupleStream::UNPIN_ALL));
  // Temporarily free up the probe reservation to use when repartitioning. Repartitioning
  // will reserve as much memory as needed for the probe streams.
  buffer_pool_client_->RestoreReservation(
      &probe_stream_reservation_, saved_probe_reservation);
  VLOG(3) << "PHJ(node_id=" << join_node_id_ << ") unsave " << saved_probe_reservation
          << " for probe clients.";
  // All reservation should be available for repartitioning.
  DCHECK_EQ(0, probe_stream_reservation_.GetReservation());
  DCHECK(buffer_pool_client_->GetUsedReservation() == 0
        || (!is_separate_build_ && join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN))
      << "Only NAAJ probe streams should be consuming memory" << DebugString();

  DCHECK_EQ(partition->build_rows()->BytesPinned(false), 0) << DebugString();
  int64_t num_input_rows = partition->build_rows()->num_rows();
  RETURN_IF_ERROR(RepartitionBuildInput(partition));

  // Check if there was any reduction in the size of partitions after repartitioning.
  int64_t largest_partition_rows = LargestPartitionRows();
  DCHECK_GE(num_input_rows, largest_partition_rows) << "Cannot have a partition with "
                                                       "more rows than the input";
  if (UNLIKELY(num_input_rows == largest_partition_rows)) {
    return Status(TErrorCode::PARTITIONED_HASH_JOIN_REPARTITION_FAILS, join_node_id_,
        next_partition_level, num_input_rows, DebugString(),
        buffer_pool_client_->DebugString());
  }
  return Status::OK();
}

Status PhjBuilder::RepartitionBuildInput(PhjBuilderPartition* input_partition) {
  int new_level = input_partition->level() + 1;
  DCHECK_GE(new_level, 1);
  SCOPED_TIMER(repartition_timer_);
  COUNTER_ADD(num_repartitions_, 1);
  RuntimeState* state = runtime_state_;

  // Setup the read buffer and the new partitions.
  BufferedTupleStream* build_rows = input_partition->build_rows();
  DCHECK(build_rows != nullptr);
  bool got_read_buffer;
  RETURN_IF_ERROR(build_rows->PrepareForRead(true, &got_read_buffer));
  if (!got_read_buffer) {
    return mem_tracker()->MemLimitExceeded(
        state, Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, join_node_id_));
  }
  RETURN_IF_ERROR(CreateHashPartitions(new_level));

  // Repartition 'input_stream' into 'hash_partitions_'.
  RowBatch build_batch(row_desc_, state->batch_size(), mem_tracker());
  bool eos = false;
  while (!eos) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());

    RETURN_IF_ERROR(build_rows->GetNext(&build_batch, &eos));
    RETURN_IF_ERROR(AddBatch(&build_batch));
    build_batch.Reset();
  }

  // Done reading the input, we can safely close it now to free memory.
  input_partition->Close(nullptr);
  RETURN_IF_ERROR(FinalizeBuild(state));
  return Status::OK();
}

int64_t PhjBuilder::LargestPartitionRows() const {
  int64_t max_rows = 0;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    PhjBuilderPartition* partition = hash_partitions_[i].get();
    DCHECK(partition != nullptr);
    if (partition->IsClosed()) continue;
    int64_t rows = partition->build_rows()->num_rows();
    if (rows > max_rows) max_rows = rows;
  }
  return max_rows;
}

bool PhjBuilder::HashTableStoresNulls(
    TJoinOp::type join_op, const vector<bool>& is_not_distinct_from) {
  return join_op == TJoinOp::RIGHT_OUTER_JOIN || join_op == TJoinOp::RIGHT_ANTI_JOIN
      || join_op == TJoinOp::FULL_OUTER_JOIN
      || std::accumulate(is_not_distinct_from.begin(), is_not_distinct_from.end(), false,
             std::logical_or<bool>());
}

void PhjBuilder::ReturnReservation(
    BufferPool::ClientHandle* probe_client, int64_t bytes) {
  DCHECK(is_separate_build_);
  DCHECK(buffer_pool_client_ != probe_client);
  bool success;
  Status status =
      probe_client->TransferReservationTo(buffer_pool_client_, bytes, &success);
  DCHECK(status.ok()) << status.GetDetail() << " shouldn't have any dirty pages to flush";
  DCHECK(success) << "Transferring within query shouldn't violate reservation limits.";
}

PhjBuilderPartition::PhjBuilderPartition(RuntimeState* state, PhjBuilder* parent,
    int level)
  : parent_(parent),
    id_(parent->next_partition_id_++),
    is_spilled_(false),
    level_(level) {
  build_rows_ = make_unique<BufferedTupleStream>(state, parent_->row_desc_,
      parent_->buffer_pool_client_, parent->spillable_buffer_size_,
      parent->max_row_buffer_size_);
}

PhjBuilderPartition::~PhjBuilderPartition() {
  DCHECK(IsClosed());
}

int64_t PhjBuilderPartition::EstimatedInMemSize() const {
  return build_rows_->byte_size() + HashTable::EstimateSize(build_rows_->num_rows());
}

void PhjBuilderPartition::Close(RowBatch* batch) {
  if (IsClosed()) return;

  if (hash_tbl_ != nullptr) {
    hash_tbl_->StatsCountersAdd(parent_->ht_stats_profile_.get());
    hash_tbl_->Close();
  }

  // Transfer ownership of 'build_rows_' memory to 'batch' if 'batch' is not NULL.
  // Flush out the resources to free up memory for subsequent partitions.
  if (build_rows_ != nullptr) {
    build_rows_->Close(batch, RowBatch::FlushMode::FLUSH_RESOURCES);
    build_rows_.reset();
  }
}

Status PhjBuilderPartition::Spill(BufferedTupleStream::UnpinMode mode) {
  DCHECK(!IsClosed());
  RETURN_IF_ERROR(parent_->runtime_state_->StartSpilling(parent_->mem_tracker()));
  // Close the hash table and unpin the stream backing it to free memory.
  if (hash_tbl() != nullptr) {
    hash_tbl_->Close();
    hash_tbl_.reset();
  }
  RETURN_IF_ERROR(build_rows_->UnpinStream(mode));
  if (!is_spilled_) {
    COUNTER_ADD(parent_->num_spilled_partitions_, 1);
    if (parent_->num_spilled_partitions_->value() == 1) {
      parent_->profile()->AppendExecOption("Spilled");
    }
  }
  is_spilled_ = true;
  return Status::OK();
}

Status PhjBuilderPartition::BuildHashTable(bool* built) {
  SCOPED_TIMER(parent_->build_hash_table_timer_);
  DCHECK(build_rows_ != nullptr);
  *built = false;

  // Before building the hash table, we need to pin the rows in memory.
  RETURN_IF_ERROR(build_rows_->PinStream(built));
  if (!*built) return Status::OK();

  RuntimeState* state = parent_->runtime_state_;
  HashTableCtx* ctx = parent_->ht_ctx_.get();
  ctx->set_level(level()); // Set the hash function for building the hash table.
  RowBatch batch(parent_->row_desc_, state->batch_size(), parent_->mem_tracker());
  vector<BufferedTupleStream::FlatRowPtr> flat_rows;
  bool eos = false;

  // Allocate the partition-local hash table. Initialize the number of buckets based on
  // the number of build rows (the number of rows is known at this point). This assumes
  // there are no duplicates which can be wrong. However, the upside in the common case
  // (few/no duplicates) is large and the downside when there are is low (a bit more
  // memory; the bucket memory is small compared to the memory needed for all the build
  // side allocations).
  // One corner case is if the stream contains tuples with zero footprint (no materialized
  // slots). If the tuples occupy no space, this implies all rows will be duplicates, so
  // create a small hash table, IMPALA-2256.
  //
  // TODO: Try to allocate the hash table before pinning the stream to avoid needlessly
  // reading all of the spilled rows from disk when we won't succeed anyway.
  int64_t estimated_num_buckets = HashTable::EstimateNumBuckets(build_rows()->num_rows());
  hash_tbl_.reset(HashTable::Create(parent_->ht_allocator_.get(),
      true /* store_duplicates */, parent_->row_desc_->tuple_descriptors().size(),
      build_rows(), 1 << (32 - PhjBuilder::NUM_PARTITIONING_BITS),
      estimated_num_buckets));
  bool success;
  Status status = hash_tbl_->Init(&success);
  if (!status.ok() || !success) goto not_built;
  status = build_rows_->PrepareForRead(false, &success);
  if (!status.ok()) goto not_built;
  DCHECK(success) << "Stream was already pinned.";

  do {
    status = build_rows_->GetNext(&batch, &eos, &flat_rows);
    if (!status.ok()) goto not_built;
    DCHECK_EQ(batch.num_rows(), flat_rows.size());
    DCHECK_LE(batch.num_rows(), hash_tbl_->EmptyBuckets());
    TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;

    InsertBatchFn insert_batch_fn;
    if (level() == 0) {
      insert_batch_fn = parent_->insert_batch_fn_level0_.load();
    } else {
      insert_batch_fn = parent_->insert_batch_fn_.load();
    }

    if (insert_batch_fn != nullptr) {
      if (UNLIKELY(
            !insert_batch_fn(this, prefetch_mode, ctx, &batch, flat_rows, &status))) {
        goto not_built;
      }
    } else if (UNLIKELY(!InsertBatch(prefetch_mode, ctx, &batch, flat_rows, &status))) {
      goto not_built;
    }

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->GetQueryStatus());
    // Free any expr result allocations made while inserting.
    parent_->expr_results_pool_->Clear();
    batch.Reset();
  } while (!eos);

  // The hash table fits in memory and is built.
  DCHECK(*built);
  DCHECK(hash_tbl_ != nullptr);
  is_spilled_ = false;
  COUNTER_ADD(parent_->ht_stats_profile_->num_hash_buckets_,
      hash_tbl_->num_buckets());
  return Status::OK();

not_built:
  *built = false;
  if (hash_tbl_ != nullptr) {
    hash_tbl_->Close();
    hash_tbl_.reset();
  }
  return status;
}

std::string PhjBuilderPartition::DebugString() {
  stringstream ss;
  ss << "<Partition>: ptr=" << this << " id=" << id_;
  if (IsClosed()) {
    ss << " Closed";
    return ss.str();
  }
  if (is_spilled()) {
    ss << " Spilled";
  }
  DCHECK(build_rows() != nullptr);
  ss << endl
     << "    Build Rows: " << build_rows_->num_rows()
     << " (Bytes pinned: " << build_rows_->BytesPinned(false) << ")"
     << endl;
  if (hash_tbl_ != nullptr) {
    ss << "    Hash Table Rows: " << hash_tbl_->size();
  }
  ss << "    Spilled Probe Rows: " << num_spilled_probe_rows_.Load() << endl;
  return ss.str();
}

void PhjBuilderConfig::Codegen(FragmentState* state) {
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  Status build_codegen_status;
  Status insert_codegen_status;
  Status codegen_status;

  // Codegen for hashing rows with the builder's hash table context.
  llvm::Function* hash_fn;
  codegen_status =
      HashTableCtx::CodegenHashRow(codegen, false, *hash_table_config_, &hash_fn);
  llvm::Function* murmur_hash_fn;
  codegen_status.MergeStatus(
      HashTableCtx::CodegenHashRow(codegen, true, *hash_table_config_, &murmur_hash_fn));

  // Codegen for evaluating build rows
  llvm::Function* eval_build_row_fn;
  codegen_status.MergeStatus(HashTableCtx::CodegenEvalRow(
      codegen, true, *hash_table_config_, &eval_build_row_fn));

  llvm::Function* insert_filters_fn;
  codegen_status.MergeStatus(
      CodegenInsertRuntimeFilters(codegen, filter_exprs_, &insert_filters_fn));

  if (codegen_status.ok()) {
    TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
    build_codegen_status = CodegenProcessBuildBatch(
        codegen, hash_fn, murmur_hash_fn, eval_build_row_fn, insert_filters_fn);
    insert_codegen_status = CodegenInsertBatch(
        codegen, hash_fn, murmur_hash_fn, eval_build_row_fn, prefetch_mode);
  } else {
    build_codegen_status = codegen_status;
    insert_codegen_status = codegen_status;
  }
  AddCodegenStatus(build_codegen_status, "Build Side");
  AddCodegenStatus(insert_codegen_status, "Hash Table Construction");
}

string PhjBuilder::DebugString() const {
  stringstream ss;
  ss << " PhjBuilder op=" << join_op_
     << " is_separate_build=" << is_separate_build_
     << " num_probe_threads=" << num_probe_threads_
     << " state=" << PrintState(state_)
     << " Hash partitions: " << hash_partitions_.size() << ":" << endl;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    ss << " Hash partition " << i << " " << hash_partitions_[i]->DebugString() << endl;
  }
  ss << " Spilled partitions: " << spilled_partitions_.size() << ":" << endl;
  for (int i = 0; i < spilled_partitions_.size(); ++i) {
    ss << " Spilled partition " << i << " "
       << spilled_partitions_[i]->DebugString() << endl;
  }
  if (null_aware_partition_ != nullptr) {
    ss << "Null-aware partition: " << null_aware_partition_->DebugString();
  }
  ss << " buffer_pool_client=" << buffer_pool_client_->DebugString();
  return ss.str();
}
void PhjBuilder::UnregisterThreadFromBarrier() const {
  DCHECK(probe_barrier_ != nullptr);
  probe_barrier_->Unregister();
}

Status PhjBuilderConfig::CodegenProcessBuildBatch(LlvmCodeGen* codegen,
    llvm::Function* hash_fn, llvm::Function* murmur_hash_fn, llvm::Function* eval_row_fn,
    llvm::Function* insert_filters_fn) {
  llvm::Function* process_build_batch_fn =
      codegen->GetFunction(IRFunction::PHJ_PROCESS_BUILD_BATCH, true);
  DCHECK(process_build_batch_fn != nullptr);

  // Replace call sites
  int replaced =
      codegen->ReplaceCallSites(process_build_batch_fn, eval_row_fn, "EvalBuildRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSites(
      process_build_batch_fn, insert_filters_fn, "InsertRuntimeFilters");
  DCHECK_REPLACE_COUNT(replaced, 1);

  HashTableCtx::HashTableReplacedConstants replaced_constants;
  const bool stores_duplicates = true;
  const int num_build_tuples = input_row_desc_->tuple_descriptors().size();
  // Replace some hash table parameters with constants.
  RETURN_IF_ERROR(HashTableCtx::ReplaceHashTableConstants(codegen, *hash_table_config_,
      stores_duplicates, num_build_tuples, process_build_batch_fn, &replaced_constants));
  DCHECK_GE(replaced_constants.stores_nulls, 1);
  DCHECK_EQ(replaced_constants.finds_some_nulls, 0);
  DCHECK_EQ(replaced_constants.stores_duplicates, 0);
  DCHECK_EQ(replaced_constants.stores_tuples, 0);
  DCHECK_EQ(replaced_constants.quadratic_probing, 0);

  llvm::Value* is_null_aware_arg = codegen->GetArgument(process_build_batch_fn, 5);
  is_null_aware_arg->replaceAllUsesWith(
      codegen->GetBoolConstant(join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN));

  llvm::Function* process_build_batch_fn_level0 =
      codegen->CloneFunction(process_build_batch_fn);

  // Always build runtime filters at level0 (if there are any).
  // Note that the first argument of this function is the return value.
  llvm::Value* build_filter_l0_arg =
      codegen->GetArgument(process_build_batch_fn_level0, 4);
  build_filter_l0_arg->replaceAllUsesWith(
      codegen->GetBoolConstant(filter_descs_.size() > 0));

  // process_build_batch_fn_level0 uses CRC hash if available,
  replaced =
      codegen->ReplaceCallSites(process_build_batch_fn_level0, hash_fn, "HashRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  // process_build_batch_fn uses murmur
  replaced =
      codegen->ReplaceCallSites(process_build_batch_fn, murmur_hash_fn, "HashRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  // Never build filters after repartitioning, as all rows have already been added to the
  // filters during the level0 build. Note that the first argument of this function is the
  // return value.
  llvm::Value* build_filter_arg = codegen->GetArgument(process_build_batch_fn, 4);
  build_filter_arg->replaceAllUsesWith(codegen->false_value());

  // Finalize ProcessBuildBatch functions
  process_build_batch_fn = codegen->FinalizeFunction(process_build_batch_fn);
  if (process_build_batch_fn == nullptr) {
    return Status(
        "Codegen'd PhjBuilder::ProcessBuildBatch() function "
        "failed verification, see log");
  }
  process_build_batch_fn_level0 =
      codegen->FinalizeFunction(process_build_batch_fn_level0);
  if (process_build_batch_fn == nullptr) {
    return Status(
        "Codegen'd level-zero PhjBuilder::ProcessBuildBatch() "
        "function failed verification, see log");
  }

  // Register native function pointers
  codegen->AddFunctionToJit(process_build_batch_fn, &process_build_batch_fn_);
  codegen->AddFunctionToJit(process_build_batch_fn_level0,
      &process_build_batch_fn_level0_);
  return Status::OK();
}

Status PhjBuilderConfig::CodegenInsertBatch(LlvmCodeGen* codegen, llvm::Function* hash_fn,
    llvm::Function* murmur_hash_fn, llvm::Function* eval_row_fn,
    TPrefetchMode::type prefetch_mode) {
  llvm::Function* insert_batch_fn =
      codegen->GetFunction(IRFunction::PHJ_INSERT_BATCH, true);
  // Context required to generate hash table codegened methods.
  llvm::Function* build_equals_fn;
  RETURN_IF_ERROR(
      HashTableCtx::CodegenEquals(codegen, true, *hash_table_config_, &build_equals_fn));

  // Replace the parameter 'prefetch_mode' with constant.
  llvm::Value* prefetch_mode_arg = codegen->GetArgument(insert_batch_fn, 1);
  DCHECK_GE(prefetch_mode, TPrefetchMode::NONE);
  DCHECK_LE(prefetch_mode, TPrefetchMode::HT_BUCKET);
  prefetch_mode_arg->replaceAllUsesWith(codegen->GetI32Constant(prefetch_mode));

  // Use codegen'd EvalBuildRow() function
  int replaced = codegen->ReplaceCallSites(insert_batch_fn, eval_row_fn, "EvalBuildRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  // Use codegen'd Equals() function
  replaced = codegen->ReplaceCallSites(insert_batch_fn, build_equals_fn, "Equals");
  DCHECK_REPLACE_COUNT(replaced, 1);

  // Replace hash-table parameters with constants.
  HashTableCtx::HashTableReplacedConstants replaced_constants;
  const bool stores_duplicates = true;
  const int num_build_tuples = input_row_desc_->tuple_descriptors().size();
  RETURN_IF_ERROR(HashTableCtx::ReplaceHashTableConstants(codegen, *hash_table_config_,
      stores_duplicates, num_build_tuples, insert_batch_fn, &replaced_constants));
  DCHECK_GE(replaced_constants.stores_nulls, 1);
  DCHECK_EQ(replaced_constants.finds_some_nulls, 0);
  DCHECK_GE(replaced_constants.stores_duplicates, 1);
  DCHECK_GE(replaced_constants.stores_tuples, 1);
  DCHECK_GE(replaced_constants.quadratic_probing, 1);

  llvm::Function* insert_batch_fn_level0 = codegen->CloneFunction(insert_batch_fn);

  // Use codegen'd hash functions
  replaced = codegen->ReplaceCallSites(insert_batch_fn_level0, hash_fn, "HashRow");
  DCHECK_REPLACE_COUNT(replaced, 1);
  replaced = codegen->ReplaceCallSites(insert_batch_fn, murmur_hash_fn, "HashRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  insert_batch_fn = codegen->FinalizeFunction(insert_batch_fn);
  if (insert_batch_fn == nullptr) {
    return Status(
        "PartitionedHashJoinNode::CodegenInsertBatch(): codegen'd "
        "InsertBatch() function failed verification, see log");
  }
  insert_batch_fn_level0 = codegen->FinalizeFunction(insert_batch_fn_level0);
  if (insert_batch_fn_level0 == nullptr) {
    return Status(
        "PartitionedHashJoinNode::CodegenInsertBatch(): codegen'd zero-level "
        "InsertBatch() function failed verification, see log");
  }

  codegen->AddFunctionToJit(insert_batch_fn, &insert_batch_fn_);
  codegen->AddFunctionToJit(insert_batch_fn_level0, &insert_batch_fn_level0_);
  return Status::OK();
}

// An example of the generated code for a query with two filters built by this node.
//
//  ; Function Attrs: noinline
//  define void @InsertRuntimeFilters(%"class.impala::PhjBuilder"* %this,
//      %"struct.impala::FilterContext"* %filter_ctxs,
//      %"class.impala::TupleRow"* %row) #49 {
//  entry:
//    %0 = getelementptr %"struct.impala::FilterContext",
//        %"struct.impala::FilterContext"* %filter_ctxs, i32 0
//    call void @FilterContextInsert(%"struct.impala::FilterContext"* %0,
//        %"class.impala::TupleRow"* %row)
//    %1 = getelementptr %"struct.impala::FilterContext",
//        %"struct.impala::FilterContext"* %filter_ctxs, i32 1
//    call void @FilterContextInsert.3(%"struct.impala::FilterContext"* %1,
//        %"class.impala::TupleRow"* %row)
//    ret void
//  }
Status PhjBuilderConfig::CodegenInsertRuntimeFilters(
    LlvmCodeGen* codegen, const vector<ScalarExpr*>& filter_exprs, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  *fn = nullptr;
  llvm::Type* this_type = codegen->GetStructPtrType<PhjBuilder>();
  llvm::PointerType* filters_ctx_arr_type = codegen->GetStructPtrType<FilterContext>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(
      codegen, "InsertRuntimeFilters", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("filter_ctxs", filters_ctx_arr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  llvm::Value* args[3];
  llvm::Function* insert_runtime_filters_fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* filter_ctxs = args[1];
  llvm::Value* row_arg = args[2];

  int num_filters = filter_exprs.size();
  for (int i = 0; i < num_filters; ++i) {
    llvm::Function* insert_fn;
    RETURN_IF_ERROR(FilterContext::CodegenInsert(
        codegen, filter_exprs[i], filter_descs_[i], &insert_fn));
    llvm::Value* filter_context_ptr = builder.CreateConstGEP1_32(filter_ctxs, i);
    llvm::Value* insert_args[] = {filter_context_ptr, row_arg};
    builder.CreateCall(insert_fn, insert_args);
  }

  builder.CreateRetVoid();

  if (num_filters > 0) {
    // Don't inline this function to avoid code bloat in ProcessBuildBatch().
    // If there is any filter, InsertRuntimeFilters() is large enough to not benefit
    // much from inlining.
    insert_runtime_filters_fn->addFnAttr(llvm::Attribute::NoInline);
  }

  *fn = codegen->FinalizeFunction(insert_runtime_filters_fn);
  if (*fn == nullptr) {
    return Status("Codegen'd PhjBuilder::InsertRuntimeFilters() failed "
                  "verification, see log");
  }
  return Status::OK();
}
