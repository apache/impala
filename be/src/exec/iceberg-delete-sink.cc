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

#include <boost/algorithm/string.hpp>

#include "common/object-pool.h"
#include "exec/parquet/hdfs-parquet-table-writer.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "kudu/util/url-coding.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/coding-util.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

DataSink* IcebergDeleteSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
      new IcebergDeleteSink(sink_id, *this,
          this->tsink_->table_sink.iceberg_delete_sink, state));
}

Status IcebergDeleteSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  DCHECK(tsink_->__isset.table_sink);
  DCHECK(tsink_->table_sink.__isset.iceberg_delete_sink);
  RETURN_IF_ERROR(
      ScalarExpr::Create(tsink_->table_sink.iceberg_delete_sink.partition_key_exprs,
          *input_row_desc_, state, &partition_key_exprs_));
  return Status::OK();
}

IcebergDeleteSink::IcebergDeleteSink(TDataSinkId sink_id,
    const IcebergDeleteSinkConfig& sink_config, const TIcebergDeleteSink& ice_del_sink,
    RuntimeState* state) :
    TableSinkBase(sink_id, sink_config, "IcebergDeleteSink", state) {
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
  DCHECK_EQ(partition_key_expr_evals_.size(), dynamic_partition_key_expr_evals_.size());
  return Status::OK();
}

Status IcebergDeleteSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  // We don't do any work for an empty batch.
  if (batch->num_rows() == 0) return Status::OK();

  // If there are no partition keys then just pass the whole batch to one partition.
  if (dynamic_partition_key_expr_evals_.empty()) {
    if (current_partition_.first == nullptr) {
      RETURN_IF_ERROR(SetCurrentPartition(state, nullptr, ROOT_PARTITION_KEY));
    }
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, &current_partition_));
  } else {
    RETURN_IF_ERROR(WriteClusteredRowBatch(state, batch));
  }
  return Status::OK();
}

inline Status IcebergDeleteSink::SetCurrentPartition(RuntimeState* state,
    const TupleRow* row, const string& key) {
  DCHECK(row != nullptr || key == ROOT_PARTITION_KEY);
  PartitionMap::iterator existing_partition;
  if (current_partition_.first != nullptr &&
      key == current_clustered_partition_key_) {
    return Status::OK();
  }

  current_partition_.first.reset(new OutputPartition());
  current_partition_.second.clear();
  Status status = InitOutputPartition(state, *prototype_partition_, row,
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
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, &current_partition_));
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
        RETURN_IF_ERROR(WriteRowsToPartition(state, batch, &current_partition_));
        current_partition_.second.clear();
      }
      RETURN_IF_ERROR(FinalizePartitionFile(state,
          current_partition_.first.get()));
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
  RETURN_IF_ERROR(WriteRowsToPartition(state, batch, &current_partition_));
  return Status::OK();
}

Status IcebergDeleteSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  SCOPED_TIMER(profile()->total_time_counter());

  if (current_partition_.first != nullptr) {
    RETURN_IF_ERROR(FinalizePartitionFile(state, current_partition_.first.get()));
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
  TableSinkBase::Close(state);
  closed_ = true;
}

void IcebergDeleteSink::ConstructPartitionInfo(
    const TupleRow* row,
    OutputPartition* output_partition) {
  DCHECK(output_partition != nullptr);
  DCHECK(output_partition->raw_partition_names.empty());

  if (partition_key_expr_evals_.empty()) {
    output_partition->iceberg_spec_id = table_desc_->IcebergSpecId();
    return;
  }

  DCHECK_EQ(partition_key_expr_evals_.size(), 2);

  ScalarExprEvaluator* spec_id_eval = partition_key_expr_evals_[0];
  ScalarExprEvaluator* partitions_eval = partition_key_expr_evals_[1];

  int spec_id = spec_id_eval->GetIntVal(row).val;
  output_partition->iceberg_spec_id = spec_id;

  StringVal partitions_strings_val = partitions_eval->GetStringVal(row);
  string partition_values_str((const char*)partitions_strings_val.ptr,
      partitions_strings_val.len);

  vector<string> non_void_partition_names;
  const vector<string>* non_void_partition_names_ptr = nullptr;
  if (LIKELY(spec_id == table_desc_->IcebergSpecId())) {
    // If 'spec_id' is the default spec id, then point 'non_void_partition_names_ptr'
    // to the already existing vector 'table_desc_->IcebergNonVoidPartitionNames()'.
    non_void_partition_names_ptr = &table_desc_->IcebergNonVoidPartitionNames();
  } else {
    // Otherwise collect the non-void partition names belonging to 'spec_id' in
    // 'non_void_partition_names' and point 'non_void_partition_names_ptr' to it.
    const TIcebergPartitionSpec& partition_spec =
        table_desc_->IcebergPartitionSpecs()[spec_id];
    for (const TIcebergPartitionField& spec_field : partition_spec.partition_fields) {
      if (spec_field.transform.transform_type != TIcebergPartitionTransformType::VOID) {
        non_void_partition_names.push_back(spec_field.field_name);
      }
    }
    non_void_partition_names_ptr = &non_void_partition_names;
  }
  DCHECK(non_void_partition_names_ptr != nullptr);

  if (non_void_partition_names_ptr->empty()) {
    DCHECK(partition_values_str.empty());
    return;
  }

  vector<string> partition_values_encoded;
  boost::split(partition_values_encoded, partition_values_str, boost::is_any_of("."));
  vector<string> partition_values_decoded;
  partition_values_decoded.reserve(partition_values_encoded.size());
  for (const string& encoded_part_val : partition_values_encoded) {
    string decoded_val;
    bool success = kudu::Base64Decode(encoded_part_val, &decoded_val);
    // We encoded it, we must succeed decoding it.
    DCHECK(success);
    partition_values_decoded.push_back(std::move(decoded_val));
  }

  DCHECK_EQ(partition_values_decoded.size(), non_void_partition_names_ptr->size());

  stringstream url_encoded_partition_name_ss;
  stringstream external_partition_name_ss;

  for (int i = 0; i < partition_values_decoded.size(); ++i) {
    stringstream raw_partition_key_value_ss;
    stringstream encoded_partition_key_value_ss;

    raw_partition_key_value_ss << (*non_void_partition_names_ptr)[i] << "=";
    encoded_partition_key_value_ss << (*non_void_partition_names_ptr)[i] << "=";

    string& value_str = partition_values_decoded[i];
    raw_partition_key_value_ss << value_str;

    string part_key_value = UrlEncodePartitionValue(value_str);
    encoded_partition_key_value_ss << part_key_value;
    if (i < partition_key_expr_evals_.size() - 1) encoded_partition_key_value_ss << "/";

    url_encoded_partition_name_ss << encoded_partition_key_value_ss.str();

    output_partition->raw_partition_names.push_back(raw_partition_key_value_ss.str());
  }

  output_partition->partition_name = url_encoded_partition_name_ss.str();
  output_partition->external_partition_name = external_partition_name_ss.str();
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
