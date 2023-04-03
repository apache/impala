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

#include "exec/iceberg-delete-sink.h"

#include "common/object-pool.h"
#include "exec/parquet/hdfs-parquet-table-writer.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

IcebergDeleteSink::IcebergDeleteSink(TDataSinkId sink_id,
    const IcebergDeleteSinkConfig& sink_config, const TIcebergDeleteSink& ice_del_sink,
    RuntimeState* state) : TableSinkBase(sink_id, sink_config,
    "IcebergDeleteSink", state) {
}

DataSink* IcebergDeleteSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
      new IcebergDeleteSink(sink_id, *this,
          this->tsink_->table_sink.iceberg_delete_sink, state));
}

void IcebergDeleteSinkConfig::Close() {
  DataSinkConfig::Close();
}

Status IcebergDeleteSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  DCHECK(tsink_->__isset.table_sink);
  return Status::OK();
}

Status IcebergDeleteSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(TableSinkBase::Prepare(state, parent_mem_tracker));
  unique_id_str_ = "delete-" + PrintId(state->fragment_instance_id(), "-");

  // Resolve table id and set input tuple descriptor.
  table_desc_ = static_cast<const HdfsTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));
  if (table_desc_ == nullptr) {
    stringstream error_msg("Failed to get table descriptor for table id: ");
    error_msg << table_id_;
    return Status(error_msg.str());
  }

  DCHECK_GE(output_expr_evals_.size(),
      table_desc_->num_cols() - table_desc_->num_clustering_cols()) << DebugString();

  return Status::OK();
}

Status IcebergDeleteSink::Open(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(TableSinkBase::Open(state));
  prototype_partition_ = CHECK_NOTNULL(table_desc_->prototype_partition_descriptor());
  output_partition_ = make_pair(make_unique<OutputPartition>(), std::vector<int32_t>());
  state->dml_exec_state()->AddPartition(
      output_partition_.first->partition_name, prototype_partition_->id(),
      &table_desc_->hdfs_base_dir(), nullptr);
  return Status::OK();
}

Status IcebergDeleteSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(state->CheckQueryState());
  // We don't do any work for an empty batch.
  if (batch->num_rows() == 0) return Status::OK();

  if (output_partition_.first->writer == nullptr) {
    RETURN_IF_ERROR(InitOutputPartition(state));
  }

  RETURN_IF_ERROR(WriteRowsToPartition(state, batch, &output_partition_));

  return Status();
}

Status IcebergDeleteSink::InitOutputPartition(RuntimeState* state) {
  // Build the unique name for this partition from the partition keys, e.g. "j=1/f=foo/"
  // etc.
  stringstream partition_name_ss;

  // partition_name_ss now holds the unique descriptor for this partition,
  output_partition_.first->partition_name = partition_name_ss.str();
  BuildHdfsFileNames(*prototype_partition_, output_partition_.first.get(), "");

  // We will be writing to the final file if we're skipping staging, so get a connection
  // to its filesystem.
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      output_partition_.first->final_hdfs_file_name_prefix,
      &output_partition_.first->hdfs_connection));

  output_partition_.first->partition_descriptor = prototype_partition_;

  output_partition_.first->writer.reset(
      new HdfsParquetTableWriter(
          this, state, output_partition_.first.get(), prototype_partition_, table_desc_));
  RETURN_IF_ERROR(output_partition_.first->writer->Init());
  COUNTER_ADD(partitions_created_counter_, 1);
  return CreateNewTmpFile(state, output_partition_.first.get());
}

Status IcebergDeleteSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  SCOPED_TIMER(profile()->total_time_counter());

  RETURN_IF_ERROR(FinalizePartitionFile(state, output_partition_.first.get()));
  return Status::OK();
}

void IcebergDeleteSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(profile()->total_time_counter());

  if (output_partition_.first->writer != nullptr) {
    output_partition_.first->writer->Close();
  }
  Status close_status = ClosePartitionFile(state, output_partition_.first.get());
  if (!close_status.ok()) state->LogError(close_status.msg());

  output_partition_.first.reset();
  TableSinkBase::Close(state);
  closed_ = true;
}

string IcebergDeleteSink::DebugString() const {
  stringstream out;
  out << "IcebergDeleteSink("
      << " table_desc=" << table_desc_->DebugString()
      << " output_exprs=" << ScalarExpr::DebugString(output_exprs_)
      << ")";
  return out.str();
}

} // namespace impala
