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

#include "exec/iceberg-delete-builder.h"

#include <filesystem>

#include "exec/exec-node.h"
#include "exec/join-op.h"
#include "runtime/fragment-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

static const string PREPARE_FOR_READ_FAILED_ERROR_MSG =
    "Failed to acquire initial read "
    "buffer for stream in hash join node $0. Reducing query concurrency or increasing "
    "the memory limit may help this query to complete successfully.";

using namespace impala;

DataSink* IcebergDeleteBuilderConfig::CreateSink(RuntimeState* state) const {
  // We have one fragment per sink, so we can use the fragment index as the sink ID.
  TDataSinkId sink_id = state->fragment().idx;
  ObjectPool* pool = state->obj_pool();
  return pool->Add(new IcebergDeleteBuilder(sink_id, *this, state));
}

IcebergDeleteBuilder* IcebergDeleteBuilderConfig::CreateSink(
    BufferPool::ClientHandle* buffer_pool_client, int64_t spillable_buffer_size,
    int64_t max_row_buffer_size, RuntimeState* state) const {
  ObjectPool* pool = state->obj_pool();
  return pool->Add(new IcebergDeleteBuilder(
      *this, buffer_pool_client, spillable_buffer_size, max_row_buffer_size, state));
}

Status IcebergDeleteBuilderConfig::CreateConfig(FragmentState* state, int join_node_id,
    TJoinOp::type join_op, const RowDescriptor* build_row_desc,
    IcebergDeleteBuilderConfig** sink) {
  ObjectPool* pool = state->obj_pool();
  TDataSink* tsink = pool->Add(new TDataSink());
  IcebergDeleteBuilderConfig* data_sink = pool->Add(new IcebergDeleteBuilderConfig());
  RETURN_IF_ERROR(data_sink->Init(state, join_node_id, join_op, build_row_desc, tsink));
  *sink = data_sink;
  return Status::OK();
}

void IcebergDeleteBuilderConfig::Close() {
  DataSinkConfig::Close();
}

Status IcebergDeleteBuilderConfig::Init(FragmentState* state, int join_node_id,
    TJoinOp::type join_op, const RowDescriptor* build_row_desc, TDataSink* tsink) {
  DCHECK(join_op == TJoinOp::ICEBERG_DELETE_JOIN);
  tsink->__isset.join_build_sink = true;
  tsink->join_build_sink.__set_dest_node_id(join_node_id);
  tsink->join_build_sink.__set_join_op(join_op);
  RETURN_IF_ERROR(JoinBuilderConfig::Init(*tsink, build_row_desc, state));
  build_row_desc_ = build_row_desc;
  return Status::OK();
}

Status IcebergDeleteBuilderConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  DCHECK(tsink.join_build_sink.runtime_filters.empty());
  RETURN_IF_ERROR(JoinBuilderConfig::Init(tsink, input_row_desc, state));
  build_row_desc_ = input_row_desc;
  return Status::OK();
}

IcebergDeleteBuilder::IcebergDeleteBuilder(TDataSinkId sink_id,
    const IcebergDeleteBuilderConfig& sink_config, RuntimeState* state)
  : JoinBuilder(sink_id, sink_config,
        ConstructBuilderName("IcebergDelete", sink_config.join_node_id_), state),
    runtime_state_(state),
    runtime_profile_(state->runtime_profile()),
    build_row_desc_(sink_config.build_row_desc_) {
  DCHECK(num_probe_threads_ <= 1 || !NeedToProcessUnmatchedBuildRows(join_op_))
      << "Returning rows with build partitions is not supported with shared builds";
}

IcebergDeleteBuilder::IcebergDeleteBuilder(const IcebergDeleteBuilderConfig& sink_config,
    BufferPool::ClientHandle* buffer_pool_client, int64_t spillable_buffer_size,
    int64_t max_row_buffer_size, RuntimeState* state)
  : JoinBuilder(-1, sink_config,
        ConstructBuilderName("IcebergDelete", sink_config.join_node_id_), state),
    runtime_state_(state),
    runtime_profile_(state->runtime_profile()),
    build_row_desc_(sink_config.build_row_desc_) {
  DCHECK_EQ(1, num_probe_threads_) << "Embedded builders cannot be shared";
}

IcebergDeleteBuilder::~IcebergDeleteBuilder() {}

Status IcebergDeleteBuilder::CalculateDataFiles() {
  auto& fragment_state_map = runtime_state_->query_state()->FragmentStateMap();
  auto fragment_it = fragment_state_map.end();
  PlanNode* delete_scan_node = nullptr;
  bool found = false;
  std::queue<const PlanNode*> q;
  for (auto it = fragment_state_map.begin(); !found && it != fragment_state_map.end();
       it++) {
    q.push(it->second->plan_tree());
    while (!q.empty()) {
      auto* current = q.front();
      q.pop();
      if (current->tnode_->node_id == join_node_id_) {
        fragment_it = it;
        delete_scan_node = current->children_[0];
        found = true;
        while (!q.empty()) q.pop();
        break;
      }
      for (auto* child : current->children_) {
        q.push(child);
      }
    }
  }

  const vector<const PlanFragmentInstanceCtxPB*>& instance_ctx_pbs =
      fragment_it->second->instance_ctx_pbs();
  for (auto ctx : instance_ctx_pbs) {
    auto ranges = ctx->per_node_scan_ranges().find(delete_scan_node->tnode_->node_id);
    if (ranges == ctx->per_node_scan_ranges().end()) continue;

    auto tuple_id = delete_scan_node->tnode_->hdfs_scan_node.tuple_id;
    auto tuple_desc = runtime_state_->desc_tbl().GetTupleDescriptor(tuple_id);
    DCHECK(tuple_desc->table_desc() != nullptr);
    auto hdfs_table = static_cast<const HdfsTableDescriptor*>(tuple_desc->table_desc());
    DCHECK(hdfs_table->IsIcebergTable());

    for (const ScanRangeParamsPB& params : ranges->second.scan_ranges()) {
      DCHECK(params.scan_range().has_hdfs_file_split());
      const HdfsFileSplitPB& split = params.scan_range().hdfs_file_split();

      HdfsPartitionDescriptor* partition_desc =
          hdfs_table->GetPartition(split.partition_id());

      std::filesystem::path file_path;
      if (split.relative_path().empty()) {
        file_path.append(split.absolute_path());
      } else {
        file_path.append(partition_desc->location()).append(split.relative_path());
      }
      auto& file_path_str = file_path.native();
      char* ptr_copy =
          reinterpret_cast<char*>(expr_results_pool_->Allocate(file_path_str.length()));

      if (ptr_copy == nullptr) {
        return Status("Failed to allocate memory.");
      }

      memcpy(ptr_copy, file_path_str.c_str(), file_path_str.length());

      std::pair<DeleteRowHashTable::iterator, bool> retval =
          deleted_rows_.emplace(std::piecewise_construct,
              std::forward_as_tuple(ptr_copy, file_path_str.length()),
              std::forward_as_tuple());

      // emplace succeeded, reserve capacity for the new file
      if (retval.second) retval.first->second.reserve(INITIAL_DELETE_VECTOR_CAPACITY);
    }
  }

  return Status::OK();
}

Status IcebergDeleteBuilder::Prepare(
    RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));

  RETURN_IF_ERROR(CalculateDataFiles());

  num_build_rows_ = ADD_COUNTER(profile(), "BuildRows", TUnit::UNIT);

  RETURN_IF_ERROR(DebugAction(state->query_options(), "ID_BUILDER_PREPARE"));

  const auto& tuple_descs = build_row_desc_->tuple_descriptors();
  const auto& slot_descs = tuple_descs[0]->slots();

  file_path_offset_ = slot_descs[0]->tuple_offset();
  pos_offset_ = slot_descs[1]->tuple_offset();

  position_sort_timer_ = ADD_TIMER(runtime_profile_, "IcebergDeletePositionSortTimer");

  return Status::OK();
}

Status IcebergDeleteBuilder::Open(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(DataSink::Open(state));
  return Status::OK();
}

Status IcebergDeleteBuilder::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(AddBatch(state, batch));
  COUNTER_ADD(num_build_rows_, batch->num_rows());
  return Status::OK();
}

Status IcebergDeleteBuilder::AddBatch(RuntimeState* state, RowBatch* batch) {
  RETURN_IF_ERROR(ProcessBuildBatch(state, batch));
  return Status::OK();
}

Status IcebergDeleteBuilder::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  return FinalizeBuild(state);
}

Status IcebergDeleteBuilder::FinalizeBuild(RuntimeState* state) {
  {
    SCOPED_TIMER(position_sort_timer_);
    for (auto& ids : deleted_rows_) {
      DeleteRowVector& vec = ids.second;
      std::sort(vec.begin(), vec.end());

      // Iceberg allows concurrent deletes, there can be duplicates
      vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
    }
  }

  if (is_separate_build_) {
    HandoffToProbesAndWait(state);
  }
  return Status::OK();
}

void IcebergDeleteBuilder::Close(RuntimeState* state) {
  if (closed_) return;
  obj_pool_.Clear();
  DataSink::Close(state);
  closed_ = true;
}

void IcebergDeleteBuilder::Reset(RowBatch* row_batch) {
  DCHECK(!is_separate_build_);
  deleted_rows_.clear();
  expr_results_pool_->Clear();
}

string IcebergDeleteBuilder::DebugString() const {
  stringstream ss;
  ss << " IcebergDeleteBuilder op=" << join_op_
     << " is_separate_build=" << is_separate_build_
     << " num_probe_threads=" << num_probe_threads_ << endl;
  return ss.str();
}

Status IcebergDeleteBuilder::ProcessBuildBatch(RuntimeState* state,
    RowBatch* build_batch) {
  FOREACH_ROW(build_batch, 0, build_batch_iter) {
    TupleRow* build_row = build_batch_iter.Get();

    impala::StringValue* file_path =
        build_row->GetTuple(0)->GetStringSlot(file_path_offset_);

    const int length = file_path->Len();
    if (UNLIKELY(length == 0)) {
      state->LogError(
          ErrorMsg(TErrorCode::GENERAL, "NULL found as file_path in delete file"));
      continue;
    }
    int64_t* id = build_row->GetTuple(0)->GetBigIntSlot(pos_offset_);
    auto it = deleted_rows_.find(*file_path);
    // deleted_rows_ filled with the relevant data file names, processing only those.
    if (it != deleted_rows_.end()) {
      it->second.emplace_back(*id);
    }
  }

  return Status::OK();
}
