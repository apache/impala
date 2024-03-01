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

#include "exec/iceberg-buffered-delete-sink.h"

#include <boost/algorithm/string.hpp>

#include "common/object-pool.h"
#include "exec/iceberg-delete-sink-config.h"
#include "exec/parquet/hdfs-parquet-table-writer.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "kudu/util/url-coding.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/coding-util.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/iceberg-utility-functions.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

class IcebergBufferedDeleteSink::FilePositionsIterator {
  public:
  FilePositionsIterator(const FilePositions& file_pos) {
    DCHECK(!file_pos.empty());
    file_level_it_ = file_pos.begin();
    file_level_end_ = file_pos.end();
    DCHECK(!file_level_it_->second.empty());
    pos_level_it_ = file_level_it_->second.begin();
  }

  bool HasNext() { return file_level_it_ != file_level_end_; }

  std::pair<StringValue, int64_t> Next() {
    /// It is only valid to call Next() if HasNext() returns true.
    DCHECK(HasNext());
    StringValue filepath = file_level_it_->first;
    DCHECK(pos_level_it_ != file_level_it_->second.end());
    int64_t pos = *pos_level_it_;
    NextPos();
    return {filepath, pos};
  }

  private:
  void NextPos() {
    DCHECK(pos_level_it_ != file_level_it_->second.end());
    ++pos_level_it_;
    if (pos_level_it_ == file_level_it_->second.end()) {
      NextFile();
    }
  }

  void NextFile() {
    DCHECK(file_level_it_ != file_level_end_);
    ++file_level_it_;
    if (file_level_it_ != file_level_end_){
      DCHECK(!file_level_it_->second.empty());
      pos_level_it_ = file_level_it_->second.begin();
    }
  }

  FilePositions::const_iterator file_level_it_;
  FilePositions::const_iterator file_level_end_;
  std::vector<int64_t>::const_iterator pos_level_it_;
};

IcebergBufferedDeleteSink::IcebergBufferedDeleteSink(TDataSinkId sink_id,
    const IcebergDeleteSinkConfig& sink_config,
    RuntimeState* state) :
    IcebergDeleteSinkBase(sink_id, sink_config, "IcebergBufferedDeleteSink", state) {
}

Status IcebergBufferedDeleteSink::Prepare(RuntimeState* state,
    MemTracker* parent_mem_tracker) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(IcebergDeleteSinkBase::Prepare(state, parent_mem_tracker));

  position_sort_timer_ = ADD_TIMER(state->runtime_profile(),
      "IcebergDeletePositionSortTimer");
  flush_timer_ = ADD_TIMER(state->runtime_profile(), "IcebergDeleteRecordsFlushTime");

  buffered_delete_pool_.reset(new MemPool(mem_tracker()));

  return Status::OK();
}

Status IcebergBufferedDeleteSink::Open(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(IcebergDeleteSinkBase::Open(state));
  return Status::OK();
}

Status IcebergBufferedDeleteSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  // We don't do any work for an empty batch.
  if (batch->num_rows() == 0) return Status::OK();

  RETURN_IF_ERROR(BufferDeleteRecords(batch));
  return Status::OK();
}

IcebergBufferedDeleteSink::PartitionInfo IcebergBufferedDeleteSink::GetPartitionInfo(
    TupleRow* row) {
  if (partition_key_expr_evals_.empty()) {
    return {table_desc_->IcebergSpecId(), ""};
  }
  DCHECK_EQ(partition_key_expr_evals_.size(), 2);
  ScalarExprEvaluator* spec_id_eval = partition_key_expr_evals_[0];
  ScalarExprEvaluator* partitions_eval = partition_key_expr_evals_[1];
  int spec_id = spec_id_eval->GetIntVal(row).val;
  StringVal partitions_strings_val = partitions_eval->GetStringVal(row);
  string partition_values(reinterpret_cast<char*>(partitions_strings_val.ptr),
      partitions_strings_val.len);
  return {spec_id, partition_values};
}

std::pair<StringVal, int64_t> IcebergBufferedDeleteSink::GetDeleteRecord(TupleRow* row) {
  auto filepath_eval = output_expr_evals_[0];
  auto position_eval = output_expr_evals_[1];
  StringVal filepath_sv = filepath_eval->GetStringVal(row);
  DCHECK(!filepath_sv.is_null);
  BigIntVal position_bi = position_eval->GetBigIntVal(row).val;
  DCHECK(!position_bi.is_null);
  int64_t position = position_bi.val;
  return {filepath_sv, position};
}

Status IcebergBufferedDeleteSink::BufferDeleteRecords(RowBatch* batch) {
  StringVal prev_filepath;
  vector<int64_t>* prev_vector = nullptr;
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    StringVal filepath_sv;
    int64_t position;
    std::tie(filepath_sv, position) = GetDeleteRecord(row);

    if (filepath_sv == prev_filepath) {
      DCHECK(prev_vector != nullptr);
      prev_vector->push_back(position);
      continue;
    }
    StringValue filepath(reinterpret_cast<char*>(DCHECK_NOTNULL(filepath_sv.ptr)),
        filepath_sv.len);
    PartitionInfo part_info = GetPartitionInfo(row);
    FilePositions& files_and_positions = partitions_to_file_positions_[part_info];

    if (files_and_positions.find(filepath) == files_and_positions.end()) {
      // The file path is not in the map yet, and 'filepath_sv.ptr' points to a memory
      // location that is owned by 'batch'. Therefore we need to deep copy this
      // file path and put the copied StringValue into 'files_and_positions'.
      uint8_t* new_ptr;
      RETURN_IF_ERROR(TryAllocateUnalignedBuffer(filepath_sv.len, &new_ptr));
      memcpy(new_ptr, filepath_sv.ptr, filepath_sv.len);
      filepath.Assign(reinterpret_cast<char*>(new_ptr), filepath_sv.len);
    }
    auto& positions_vec = files_and_positions[filepath];
    positions_vec.push_back(position);

    prev_filepath = filepath_sv;
    prev_vector = &positions_vec;
  }

  return Status::OK();
}

void IcebergBufferedDeleteSink::SortBufferedRecords() {
  SCOPED_TIMER(position_sort_timer_);
  for (auto& parts_and_file_posistions : partitions_to_file_positions_) {
    FilePositions& files_and_positions = parts_and_file_posistions.second;
    for (auto& file_to_positions : files_and_positions) {
      std::vector<int64_t>& positions = file_to_positions.second;
      sort(positions.begin(), positions.end());
    }
  }
}

void IcebergBufferedDeleteSink::VLogBufferedRecords() {
  if (!VLOG_ROW_IS_ON) return;
  stringstream ss;
  for (auto& entry : partitions_to_file_positions_) {
    const PartitionInfo& part_info = entry.first;
    int32_t spec_id = part_info.first;
    string part_encoded;
    bool succ = kudu::Base64Decode(part_info.second, &part_encoded);
    DCHECK(succ);
    ss << endl;
    ss << Substitute("Entries for (spec_id=$0, partition=$1):", spec_id, part_encoded)
       << endl;
    for (auto& file_and_pos : entry.second) {
      ss << "  " << file_and_pos.first << ": [";
      std::vector<int64_t>& positions = file_and_pos.second;
      for (int i = 0; i < positions.size(); ++i) {
        int64_t pos = positions[i];
        ss << pos;
        if (i != positions.size() - 1) ss << ", ";
      }
      ss << "]" << endl;
    }
  }
  VLOG_ROW << "IcebergBufferedDeleteSink's buffered entries:" << ss.str();
}

Status IcebergBufferedDeleteSink::VerifyBufferedRecords() {
  for (auto& entry : partitions_to_file_positions_) {
    StringValue prev_file;
    for (auto& file_and_pos : entry.second) {
      StringValue file = file_and_pos.first;
      DCHECK_LT(prev_file, file);
      prev_file = file;
      std::vector<int64_t>& positions = file_and_pos.second;
      DCHECK(!positions.empty());
      int64_t prev_pos = positions[0];
      for (int i = 1; i < positions.size(); ++i) {
        int64_t pos = positions[i];
        DCHECK_GE(pos, prev_pos);
        if (pos == prev_pos) {
          string filepath(file.Ptr(), file.Len());
          return Status(Substitute(
              "Duplicated row in DELETE sink. file_path='$0', pos='$1'. "
              "If this is coming from an UPDATE statement, please check if there are "
              "multiple matches in the JOIN condition.", filepath, pos));
        }
        prev_pos = pos;
      }
    }
  }
  return Status::OK();
}

Status IcebergBufferedDeleteSink::FlushBufferedRecords(RuntimeState* state) {
  SCOPED_TIMER(flush_timer_);

  int capacity = state->batch_size();
  RowBatch row_batch(row_desc_, capacity, mem_tracker());
  RETURN_IF_ERROR(InitializeOutputRowBatch(&row_batch));

  for (auto& entry : partitions_to_file_positions_) {
    int32_t spec_id = entry.first.first;
    const string& partition_encoded = entry.first.second;
    RETURN_IF_ERROR(SetCurrentPartition(state, spec_id, partition_encoded));
    FilePositionsIterator it(entry.second);
    while (it.HasNext()) {
      row_batch.Reset();
      RETURN_IF_ERROR(GetNextRowBatch(&row_batch, &it));
      row_batch.VLogRows("IcebergBufferedDeleteSink");
      RETURN_IF_ERROR(WriteRowsToPartition(state, &row_batch, current_partition_.get()));
    }
    DCHECK(current_partition_ != nullptr);
    RETURN_IF_ERROR(FinalizePartitionFile(state, current_partition_.get(),
        /*is_delete=*/true, &dml_exec_state_));
    current_partition_->writer->Close();
  }
  return Status::OK();
}

Status IcebergBufferedDeleteSink::InitializeOutputRowBatch(RowBatch* batch) {
  SlotRef* filepath_ref = DCHECK_NOTNULL(dynamic_cast<SlotRef*>(output_exprs_[0]));
  int tuple_idx = filepath_ref->GetTupleIdx();

  int capacity = batch->capacity();
  TupleDescriptor* tuple_desc = row_desc_->tuple_descriptors()[tuple_idx];
  int rows_buffer_size = capacity * tuple_desc->byte_size();
  uint8_t* rows_buffer;
  RETURN_IF_ERROR(TryAllocateUnalignedBuffer(rows_buffer_size, &rows_buffer));
  memset(rows_buffer, 0, rows_buffer_size);

  for (int i = 0; i < capacity; ++i) {
    TupleRow* row = batch->GetRow(i);
    row->SetTuple(tuple_idx,
        reinterpret_cast<Tuple*>(rows_buffer + i * tuple_desc->byte_size()));
  }
  return Status::OK();
}

Status IcebergBufferedDeleteSink::SetCurrentPartition(RuntimeState* state,
    int32_t spec_id, const std::string& partition_encoded) {
  current_partition_.reset(new OutputPartition());
  // Build the unique name for this partition from the partition keys, e.g. "j=1/f=foo/"
  // etc.
  RETURN_IF_ERROR(ConstructPartitionInfo(
      spec_id, partition_encoded, current_partition_.get()));
  Status status = InitOutputPartition(state, *prototype_partition_,
      current_partition_.get(), false);
  if (!status.ok()) {
    // We failed to create the output partition successfully. Clean it up now.
    if (current_partition_->writer != nullptr) {
      current_partition_->writer->Close();
    }
    return status;
  }

  // With partition evolution it's possible that we have the same partition names
  // with different spec ids. E.g. in case of TRUNCATE(1000, col) => TRUNCATE(500, col),
  // we might need to delete rows from partition "col_trunc=1000" with both spec ids. In
  // this case we might already have "col_trunc=1000" in dml_exec_state, so no need to
  // add it.
  if (!dml_exec_state_.PartitionExists(current_partition_->partition_name)) {
    // Save the partition name so that the coordinator can create the partition
    // directory structure if needed.
    dml_exec_state_.AddPartition(
        current_partition_->partition_name, prototype_partition_->id(),
        &table_desc_->hdfs_base_dir(),
        nullptr);
  }
  return Status::OK();
}

Status IcebergBufferedDeleteSink::TryAllocateUnalignedBuffer(int buffer_size,
    uint8_t** buffer) {
  *buffer = buffered_delete_pool_->TryAllocateUnaligned(buffer_size);
  if (*buffer == nullptr) {
    return Status(Substitute("Could not allocate $0 bytes for IcebergBufferedDeleteSink",
        buffer_size));
  }
  return Status::OK();
}

Status IcebergBufferedDeleteSink::GetNextRowBatch(
    RowBatch* batch, FilePositionsIterator* iterator) {
  DCHECK_EQ(batch->num_rows(), 0);
  int capacity = batch->capacity();
  while (batch->num_rows() < capacity && iterator->HasNext()) {
    const auto& next_entry = iterator->Next();
    int row_idx = batch->AddRow();
    TupleRow* row = batch->GetRow(row_idx);
    WriteRow(next_entry.first, next_entry.second, row);
    batch->CommitRows(1);
  }
  return Status::OK();
}

void IcebergBufferedDeleteSink::WriteRow(
    StringValue filepath, int64_t offset, TupleRow* row) {
  SlotRef* filepath_ref = DCHECK_NOTNULL(dynamic_cast<SlotRef*>(output_exprs_[0]));
  SlotRef* position_ref = DCHECK_NOTNULL(dynamic_cast<SlotRef*>(output_exprs_[1]));

  DCHECK(filepath_ref->type().IsStringType());
  DCHECK(position_ref->type().IsIntegerType());

  int filepath_tuple_idx = filepath_ref->GetTupleIdx();
  int position_tuple_idx = position_ref->GetTupleIdx();
  DCHECK_EQ(filepath_tuple_idx, position_tuple_idx);

  StringValue* filepath_slot = row->GetTuple(filepath_tuple_idx)->
      GetStringSlot(filepath_ref->GetSlotOffset());
  int64_t* pos_slot = row->GetTuple(position_tuple_idx)->
      GetBigIntSlot(position_ref->GetSlotOffset());

  filepath_slot->Assign(filepath);
  *pos_slot = offset;
}

Status IcebergBufferedDeleteSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  SCOPED_TIMER(profile()->total_time_counter());

  SortBufferedRecords();
  VLogBufferedRecords();
  RETURN_IF_ERROR(VerifyBufferedRecords());
  RETURN_IF_ERROR(FlushBufferedRecords(state));
  RegisterDataFilesInDmlExecState();
  return Status::OK();
}

void IcebergBufferedDeleteSink::RegisterDataFilesInDmlExecState() {
  int capacity = 0;
  for (const auto& entry : partitions_to_file_positions_) {
    const FilePositions& file_positions = entry.second;
    capacity += file_positions.size();
  }
  dml_exec_state_.reserveReferencedDataFiles(capacity);
  for (const auto& entry : partitions_to_file_positions_) {
    const FilePositions& file_positions = entry.second;
    for (const auto& file_pos_entry : file_positions) {
      const StringValue& sv = file_pos_entry.first;
      string filepath(sv.Ptr(), sv.Len());
      dml_exec_state_.addReferencedDataFile(std::move(filepath));
    }
  }
}

void IcebergBufferedDeleteSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(profile()->total_time_counter());

  DmlExecStatusPB dml_exec_proto;
  dml_exec_state_.ToProto(&dml_exec_proto);
  state->dml_exec_state()->Update(dml_exec_proto);

  current_partition_.reset();
  buffered_delete_pool_->FreeAll();

  IcebergDeleteSinkBase::Close(state);
  DCHECK(closed_);
}

string IcebergBufferedDeleteSink::DebugString() const {
  stringstream out;
  out << "IcebergBufferedDeleteSink("
      << " table_desc=" << table_desc_->DebugString()
      << " output_exprs=" << ScalarExpr::DebugString(output_exprs_);
  if (!partition_key_exprs_.empty()) {
    out << " partition_key_exprs=" << ScalarExpr::DebugString(partition_key_exprs_);
  }
  out << ")";
  return out.str();
}

} // namespace impala
