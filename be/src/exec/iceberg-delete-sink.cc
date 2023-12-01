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
#include "exec/iceberg-delete-sink-config.h"
#include "exec/parquet/hdfs-parquet-table-writer.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/coding-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

IcebergDeleteSink::IcebergDeleteSink(TDataSinkId sink_id,
    const IcebergDeleteSinkConfig& sink_config, RuntimeState* state) :
    IcebergDeleteSinkBase(sink_id, sink_config, "IcebergDeleteSink", state) {
}

Status IcebergDeleteSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(IcebergDeleteSinkBase::Prepare(state, parent_mem_tracker));
  return Status::OK();
}

Status IcebergDeleteSink::Open(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(IcebergDeleteSinkBase::Open(state));
  return Status::OK();
}

Status IcebergDeleteSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  // We don't do any work for an empty batch.
  if (batch->num_rows() == 0) return Status::OK();

  RETURN_IF_ERROR(VerifyRowsNotDuplicated(batch));

  // If there are no partition keys then just pass the whole batch to one partition.
  if (dynamic_partition_key_expr_evals_.empty()) {
    if (current_partition_.first == nullptr) {
      RETURN_IF_ERROR(SetCurrentPartition(state, nullptr, ROOT_PARTITION_KEY));
    }
    DCHECK(current_partition_.second.empty());
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_partition_.first.get()));
  } else {
    RETURN_IF_ERROR(WriteClusteredRowBatch(state, batch));
  }
  return Status::OK();
}

Status IcebergDeleteSink::VerifyRowsNotDuplicated(RowBatch* batch) {
  DCHECK_EQ(output_exprs_.size(), 2);
  DCHECK_EQ(output_expr_evals_.size(), 2);

  ScalarExpr* filepath_expr = output_exprs_[0];
  ScalarExpr* position_expr = output_exprs_[1];
  DCHECK(filepath_expr->type().IsStringType());
  DCHECK(position_expr->type().IsIntegerType());

  ScalarExprEvaluator* filepath_eval = output_expr_evals_[0];
  ScalarExprEvaluator* position_eval = output_expr_evals_[1];
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    StringVal filepath_sv = filepath_eval->GetStringVal(row);
    DCHECK(!filepath_sv.is_null);
    BigIntVal position_bi = position_eval->GetBigIntVal(row);
    DCHECK(!position_bi.is_null);
    string filepath(reinterpret_cast<char*>(filepath_sv.ptr), filepath_sv.len);
    int64_t position = position_bi.val;
    if (prev_file_path_ == filepath && prev_position_ == position) {
      return Status(Substitute("Duplicated row in DELETE sink. file_path='$0', pos='$1'. "
          "If this is coming from an UPDATE statement with a JOIN, please check if there "
          "multiple matches in the JOIN condition.", filepath, position));
    }
    prev_file_path_ = filepath;
    prev_position_ = position;
  }
  return Status::OK();
}

inline Status IcebergDeleteSink::SetCurrentPartition(RuntimeState* state,
    const TupleRow* row, const string& key) {
  DCHECK(row != nullptr || key == ROOT_PARTITION_KEY);
  if (current_partition_.first != nullptr &&
      key == current_clustered_partition_key_) {
    return Status::OK();
  }

  current_partition_.first.reset(new OutputPartition());
  current_partition_.second.clear();
  // Build the unique name for this partition from the partition keys, e.g. "j=1/f=foo/"
  // etc.
  RETURN_IF_ERROR(ConstructPartitionInfo(row, current_partition_.first.get()));
  Status status = InitOutputPartition(state, *prototype_partition_,
      current_partition_.first.get(), false);
  if (!status.ok()) {
    // We failed to create the output partition successfully. Clean it up now.
    if (current_partition_.first->writer != nullptr) {
      current_partition_.first->writer->Close();
    }
    return status;
  }

  // Save the partition name so that the coordinator can create the partition
  // directory structure if needed.
  state->dml_exec_state()->AddPartition(
      current_partition_.first->partition_name, prototype_partition_->id(),
      &table_desc_->hdfs_base_dir(),
      nullptr);
  return Status::OK();
}

Status IcebergDeleteSink::WriteClusteredRowBatch(RuntimeState* state, RowBatch* batch) {
  DCHECK_GT(batch->num_rows(), 0);
  DCHECK_EQ(partition_key_expr_evals_.size(), 2);
  DCHECK(!dynamic_partition_key_expr_evals_.empty());

  // Initialize the clustered partition and key.
  if (current_partition_.first == nullptr) {
    TupleRow* current_row = batch->GetRow(0);
    GetHashTblKey(current_row, dynamic_partition_key_expr_evals_,
        &current_clustered_partition_key_);
    RETURN_IF_ERROR(SetCurrentPartition(state, current_row,
        current_clustered_partition_key_));
  }

  // Compare the last row of the batch to the last current partition key. If they match,
  // then all the rows in the batch have the same key and can be written as a whole.
  string last_row_key;
  GetHashTblKey(batch->GetRow(batch->num_rows() - 1),
      dynamic_partition_key_expr_evals_, &last_row_key);
  if (last_row_key == current_clustered_partition_key_) {
    DCHECK(current_partition_.second.empty());
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_partition_.first.get()));
    return Status::OK();
  }

  // Not all rows in this batch match the previously written partition key, so we process
  // them individually.
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* current_row = batch->GetRow(i);

    string key;
    GetHashTblKey(current_row, dynamic_partition_key_expr_evals_, &key);

    if (current_clustered_partition_key_ != key) {
      DCHECK(current_partition_.first->writer != nullptr);
      // Done with previous partition - write rows and close.
      if (!current_partition_.second.empty()) {
        RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_partition_.first.get(),
            current_partition_.second));
        current_partition_.second.clear();
      }
      RETURN_IF_ERROR(FinalizePartitionFile(state,
          current_partition_.first.get(), /*is_delete=*/true));
      if (current_partition_.first->writer.get() != nullptr) {
        current_partition_.first->writer->Close();
      }
      RETURN_IF_ERROR(SetCurrentPartition(state, current_row, key));
      current_clustered_partition_key_ = std::move(key);
    }
#ifdef DEBUG
    string debug_row_key;
    GetHashTblKey(current_row, dynamic_partition_key_expr_evals_, &debug_row_key);
    DCHECK_EQ(current_clustered_partition_key_, debug_row_key);
#endif
    DCHECK(current_partition_.first->writer != nullptr);
    current_partition_.second.push_back(i);
  }
  // Write final set of rows to the partition but keep its file open.
  RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_partition_.first.get(),
      current_partition_.second));
  current_partition_.second.clear();
  return Status::OK();
}

Status IcebergDeleteSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  SCOPED_TIMER(profile()->total_time_counter());

  if (current_partition_.first != nullptr) {
    RETURN_IF_ERROR(FinalizePartitionFile(state, current_partition_.first.get(),
        /*is_delete=*/true));
  }
  return Status::OK();
}

void IcebergDeleteSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(profile()->total_time_counter());

  if (current_partition_.first != nullptr) {
    if (current_partition_.first->writer != nullptr) {
      current_partition_.first->writer->Close();
    }
    Status close_status = ClosePartitionFile(state, current_partition_.first.get());
    if (!close_status.ok()) state->LogError(close_status.msg());
  }

  current_partition_.first.reset();
  IcebergDeleteSinkBase::Close(state);
  DCHECK(closed_);
}

string IcebergDeleteSink::DebugString() const {
  stringstream out;
  out << "IcebergDeleteSink("
      << " table_desc=" << table_desc_->DebugString()
      << " output_exprs=" << ScalarExpr::DebugString(output_exprs_);
  if (!partition_key_exprs_.empty()) {
    out << " partition_key_exprs=" << ScalarExpr::DebugString(partition_key_exprs_);
  }
  out << ")";
  return out.str();
}

} // namespace impala
