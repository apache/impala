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

#include "exec/iceberg-delete-node.h"

#include <sstream>

#include "exec/blocking-join-node.inline.h"
#include "exec/exec-node-util.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

Status IcebergDeletePlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(BlockingJoinPlanNode::Init(tnode, state));

  DCHECK(tnode.__isset.join_node);
  DCHECK(tnode.join_node.__isset.iceberg_delete_node);

  // TODO: IMPALA-12265: create the config only if it is necessary
  RETURN_IF_ERROR(IcebergDeleteBuilderConfig::CreateConfig(state, tnode_->node_id,
      tnode_->join_node.join_op, &build_row_desc(), &id_builder_config_));
  return Status::OK();
}

void IcebergDeletePlanNode::Close() {
  if (id_builder_config_ != nullptr) id_builder_config_->Close();
  PlanNode::Close();
}

Status IcebergDeletePlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new IcebergDeleteNode(state, *this, state->desc_tbl()));
  return Status::OK();
}

IcebergDeleteNode::IcebergDeleteNode(
    RuntimeState* state, const IcebergDeletePlanNode& pnode, const DescriptorTbl& descs)
  : BlockingJoinNode("IcebergDeleteNode", state->obj_pool(), pnode, descs) {}

IcebergDeleteNode::~IcebergDeleteNode() {}

Status IcebergDeleteNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  runtime_state_ = state;
  if (!UseSeparateBuild(state->query_options())) {
    const IcebergDeleteBuilderConfig& builder_config =
        *static_cast<const IcebergDeletePlanNode&>(plan_node_).id_builder_config_;
    builder_ = builder_config.CreateSink(buffer_pool_client(),
        resource_profile_.spillable_buffer_size, resource_profile_.max_row_buffer_size,
        state);
    RETURN_IF_ERROR(builder_->Prepare(state, mem_tracker()));
    runtime_profile()->PrependChild(builder_->profile());
  }

  auto& tuple_descs = probe_row_desc().tuple_descriptors();
  auto& slot_descs = tuple_descs[0]->slots();

  for (auto& slot : slot_descs) {
    if (slot->virtual_column_type() == TVirtualColumnType::FILE_POSITION) {
      pos_offset_ = slot->tuple_offset();
    }
    if (slot->virtual_column_type() == TVirtualColumnType::INPUT_FILE_NAME) {
      file_path_offset_ = slot->tuple_offset();
    }
  }

  return Status::OK();
}

Status IcebergDeleteNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  JoinBuilder* tmp_builder = nullptr;
  RETURN_IF_ERROR(BlockingJoinNode::OpenImpl(state, &tmp_builder));
  if (builder_ == nullptr) {
    DCHECK(UseSeparateBuild(state->query_options()));
    builder_ = dynamic_cast<IcebergDeleteBuilder*>(tmp_builder);
    DCHECK(builder_ != nullptr);
  }

  // Check for errors and free expr result allocations before opening children.
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  // The prepare functions of probe expressions may have made result allocations
  // implicitly (e.g. calling UdfBuiltins::Lower()). The probe expressions' expr result
  // allocations need to be cleared now as they don't get cleared again till probing.
  // Other exprs' result allocations are cleared in QueryMaintenance().

  RETURN_IF_ERROR(BlockingJoinNode::ProcessBuildInputAndOpenProbe(state, builder_));
  RETURN_IF_ERROR(BlockingJoinNode::GetFirstProbeRow(state));
  ResetForProbe();
  probe_state_ = ProbeState::PROBING_IN_BATCH;
  iceberg_delete_state_.Init(builder_);
  return Status::OK();
}

Status IcebergDeleteNode::AcquireResourcesForBuild(RuntimeState* state) {
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }
  return Status::OK();
}

Status IcebergDeleteNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  builder_->Reset(nullptr);
  iceberg_delete_state_.Reset();
  return BlockingJoinNode::Reset(state, row_batch);
}

void IcebergDeleteNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // IMPALA-9737: free batches in case attached buffers need to be freed to
  // transfer reservation to 'builder_'.
  if (build_batch_ != nullptr) build_batch_->Reset();
  if (probe_batch_ != nullptr) probe_batch_->Reset();
  if (builder_ != nullptr) {
    bool separate_build = UseSeparateBuild(state->query_options());
    if (!separate_build || waited_for_build_) {
      builder_->CloseFromProbe(state);
      waited_for_build_ = false;
    }
  }
  iceberg_delete_state_.Reset();
  BlockingJoinNode::Close(state);
}

Status IcebergDeleteNode::NextProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch, bool* eos) {
  DCHECK(probe_batch_pos_ == probe_batch_->num_rows() || probe_batch_pos_ == -1);
  RETURN_IF_ERROR(NextProbeRowBatchFromChild(state, out_batch, eos));
  return Status::OK();
}

Status IcebergDeleteNode::NextProbeRowBatchFromChild(
    RuntimeState* state, RowBatch* out_batch, bool* eos) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_END_BATCH);
  DCHECK(probe_batch_pos_ == probe_batch_->num_rows() || probe_batch_pos_ == -1);
  *eos = false;
  do {
    // Loop until we find a non-empty row batch.
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) {
      // This out batch is full. Need to return it before getting the next batch.
      probe_batch_pos_ = -1;
      return Status::OK();
    }
    if (probe_side_eos_) {
      current_probe_row_ = nullptr;
      probe_batch_pos_ = -1;
      *eos = true;
      return Status::OK();
    }
    RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
    COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
  } while (probe_batch_->num_rows() == 0);

  ResetForProbe();
  return Status::OK();
}

Status IcebergDeleteNode::ProcessProbeBatch(RowBatch* out_batch) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_IN_BATCH);
  DCHECK_NE(probe_batch_pos_, -1);
  int rows_added = 0;
  TPrefetchMode::type prefetch_mode = runtime_state_->query_options().prefetch_mode;
  SCOPED_TIMER(probe_timer_);

  rows_added = ProcessProbeBatch(prefetch_mode, out_batch);
  out_batch->CommitRows(rows_added);
  return Status::OK();
}

Status IcebergDeleteNode::GetNext(RuntimeState* state, RowBatch* out_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  DCHECK(!out_batch->AtCapacity());

  Status status = Status::OK();
  *eos = false;
  // Save the number of rows in case GetNext() is called with a non-empty batch,
  // which can happen in a subplan.
  int num_rows_before = out_batch->num_rows();

  // This loop executes the 'probe_state_' state machine until either a full batch is
  // produced, resources are attached to 'out_batch' that require flushing, or eos
  // is reached (i.e. all rows are returned). The next call into GetNext() will resume
  // execution of the state machine where the current call into GetNext() left off.
  // See the definition of ProbeState for description of the state machine and states.
  do {
    DCHECK(status.ok());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    switch (probe_state_) {
      case ProbeState::PROBING_IN_BATCH: {
        // Finish processing rows in the current probe batch.
        RETURN_IF_ERROR(ProcessProbeBatch(out_batch));
        if (probe_batch_pos_ == probe_batch_->num_rows()
            && current_probe_row_ == nullptr) {
          probe_state_ = ProbeState::PROBING_END_BATCH;
        }
        break;
      }
      case ProbeState::PROBING_END_BATCH: {
        // Try to get the next row batch from the current probe input.
        bool probe_eos;
        RETURN_IF_ERROR(NextProbeRowBatch(state, out_batch, &probe_eos));
        if (probe_batch_pos_ == 0) {
          // Got a batch, need to process it.
          probe_state_ = ProbeState::PROBING_IN_BATCH;
        } else if (probe_eos) {
          DCHECK_EQ(probe_batch_pos_, -1);
          // Finished processing all the probe rows
          RETURN_IF_ERROR(DoneProbing(state, out_batch));
          probe_state_ = ProbeState::EOS;
        } else {
          // Got an empty batch with resources that we need to flush before getting the
          // next batch.
          DCHECK_EQ(probe_batch_pos_, -1);
        }
        break;
      }
      case ProbeState::EOS: {
        // Ensure that all potential sources of output rows are exhausted.
        DCHECK(probe_side_eos_);
        *eos = true;
        break;
      }
      default:
        DCHECK(false) << "invalid probe_state_" << static_cast<int>(probe_state_);
        break;
    }
  } while (!out_batch->AtCapacity() && !*eos);

  int num_rows_added = out_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);

  if (limit_ != -1 && rows_returned() + num_rows_added > limit_) {
    // Truncate the row batch if we went over the limit.
    num_rows_added = limit_ - rows_returned();
    DCHECK_GE(num_rows_added, 0);
    out_batch->set_num_rows(num_rows_before + num_rows_added);
    probe_batch_->TransferResourceOwnership(out_batch);
    *eos = true;
  }

  IncrementNumRowsReturned(num_rows_added);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status IcebergDeleteNode::DoneProbing(RuntimeState* state, RowBatch* batch) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_END_BATCH);
  DCHECK_EQ(probe_batch_pos_, -1);
  VLOG(2) << "Probe Side Consumed\n" << NodeDebugString();
  return Status::OK();
}

string IcebergDeleteNode::NodeDebugString() const {
  stringstream ss;
  ss << "IcebergDeleteNode (id=" << id() << " op=" << join_op_ << ")" << endl;

  if (builder_ != nullptr) {
    ss << "IcebergDeleteBuilder: " << builder_->DebugString();
  }

  return ss.str();
}

void IcebergDeleteNode::IcebergDeleteState::Init(IcebergDeleteBuilder* builder) {
  builder_ = builder;
  current_file_path_ = nullptr;
  previous_file_path_ = nullptr;
  current_delete_row_ = nullptr;
  current_deleted_pos_row_id_ = INVALID_ROW_ID;
  current_probe_pos_ = INVALID_ROW_ID;
}

void IcebergDeleteNode::IcebergDeleteState::UpdateImpl() {
  DCHECK(current_delete_row_ != nullptr);
  // We need to use binary search to find the next delete candidate in 2 cases:
  //   1. new file (or start of row batch)
  //   2. discontinuity in probe row ids
  auto next_deleted_pos_it_ = std::lower_bound(
      current_delete_row_->begin(), current_delete_row_->end(), current_probe_pos_);
  if (next_deleted_pos_it_ == current_delete_row_->end()) {
    current_deleted_pos_row_id_ = INVALID_ROW_ID;
  } else {
    current_deleted_pos_row_id_ =
        std::distance(current_delete_row_->begin(), next_deleted_pos_it_);
  }
}

void IcebergDeleteNode::IcebergDeleteState::Update(
    impala::StringValue* file_path, int64_t* next_probe_pos) {
  DCHECK(builder_ != nullptr);
  // Making sure the row ids are in ascending order inside a row batch in DIRECTED mode
  DCHECK(current_probe_pos_ == INVALID_ROW_ID || current_probe_pos_ < *next_probe_pos);
  current_probe_pos_ = *next_probe_pos;

  if (previous_file_path_ != nullptr) {
    // We already have a file path, no need to hash again
    if (current_deleted_pos_row_id_ != INVALID_ROW_ID
        && current_probe_pos_ > (*current_delete_row_)[current_deleted_pos_row_id_]) {
      UpdateImpl();
    }
  } else {
    auto it = builder_->deleted_rows().find(*file_path);
    if (it != builder_->deleted_rows().end()) {
      current_file_path_ = &it->first;
      current_delete_row_ = &it->second;
      UpdateImpl();
      previous_file_path_ = current_file_path_;
    }
  }
}

bool IcebergDeleteNode::IcebergDeleteState::IsDeleted() const {
  DCHECK(builder_ != nullptr);

  if (current_deleted_pos_row_id_ == INVALID_ROW_ID) return false;

  DCHECK(current_probe_pos_ != INVALID_ROW_ID);
  DCHECK(current_delete_row_ != nullptr);
  DCHECK(current_deleted_pos_row_id_ < current_delete_row_->size());

  return current_probe_pos_ == (*current_delete_row_)[current_deleted_pos_row_id_];
}

void IcebergDeleteNode::IcebergDeleteState::Delete() {
  DCHECK(builder_ != nullptr);

  current_deleted_pos_row_id_++;
  if (current_deleted_pos_row_id_ == current_delete_row_->size()) {
    current_deleted_pos_row_id_ = INVALID_ROW_ID;
  }
}

bool IcebergDeleteNode::IcebergDeleteState::NeedCheck() const {
  return current_deleted_pos_row_id_ != INVALID_ROW_ID
      || current_probe_pos_ == INVALID_ROW_ID;
}

void IcebergDeleteNode::IcebergDeleteState::Clear() {
  current_file_path_ = nullptr;
  previous_file_path_ = nullptr;
  current_delete_row_ = nullptr;
  current_deleted_pos_row_id_ = INVALID_ROW_ID;
  current_probe_pos_ = INVALID_ROW_ID;
}

void IcebergDeleteNode::IcebergDeleteState::Reset() {
  builder_ = nullptr;
  Clear();
}

bool IR_ALWAYS_INLINE IcebergDeleteNode::ProcessProbeRow(
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(current_probe_row_ != nullptr);
  TupleRow* out_row = out_batch_iterator->Get();
  if (!iceberg_delete_state_.IsDeleted()) {
    out_batch_iterator->parent()->CopyRow(current_probe_row_, out_row);
    matched_probe_ = true;
    --(*remaining_capacity);
    if (*remaining_capacity == 0) return false;
    out_row = out_batch_iterator->Next();
  } else {
    iceberg_delete_state_.Delete();
  }
  return true;
}

bool IR_ALWAYS_INLINE IcebergDeleteNode::ProcessProbeRowNoCheck(
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(current_probe_row_ != nullptr);
  TupleRow* out_row = out_batch_iterator->Get();
  out_batch_iterator->parent()->CopyRow(current_probe_row_, out_row);
  matched_probe_ = true;
  --(*remaining_capacity);
  if (*remaining_capacity == 0) return false;
  out_row = out_batch_iterator->Next();
  return true;
}

int IcebergDeleteNode::ProcessProbeBatch(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch) {
  DCHECK(!out_batch->AtCapacity());
  DCHECK_GE(probe_batch_pos_, 0);
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  // Note that 'probe_batch_pos_' is the row no. of the row after 'current_probe_row_'.
  RowBatch::Iterator probe_batch_iterator(probe_batch_.get(), probe_batch_pos_);
  int remaining_capacity = max_rows;

  while (!probe_batch_iterator.AtEnd() && remaining_capacity > 0) {
    current_probe_row_ = probe_batch_iterator.Get();
    if (iceberg_delete_state_.NeedCheck()) {
      impala::StringValue* file_path =
          current_probe_row_->GetTuple(0)->GetStringSlot(file_path_offset_);
      int64_t* current_probe_pos =
          current_probe_row_->GetTuple(0)->GetBigIntSlot(pos_offset_);

      iceberg_delete_state_.Update(file_path, current_probe_pos);
      if (!ProcessProbeRow(&out_batch_iterator, &remaining_capacity)) {
        DCHECK_EQ(remaining_capacity, 0);
      }
    } else {
      if (!ProcessProbeRowNoCheck(&out_batch_iterator, &remaining_capacity)) {
        DCHECK_EQ(remaining_capacity, 0);
      }
    }

    probe_batch_iterator.Next();

    // Update where we are in the probe batch.
    probe_batch_pos_ = (probe_batch_iterator.Get() - probe_batch_->GetRow(0));
  }

  int num_rows_added = max_rows - remaining_capacity;

  // Clear state as ascending order of row ids are not guaranteed between probe row
  // batches
  if (probe_batch_iterator.AtEnd()) {
    current_probe_row_ = nullptr;
    iceberg_delete_state_.Clear();
  }

  DCHECK_GE(probe_batch_pos_, 0);
  DCHECK_LE(probe_batch_pos_, probe_batch_->capacity());
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}
} // namespace impala
