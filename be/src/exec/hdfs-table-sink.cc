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

#include "exec/hdfs-table-sink.h"
#include "exec/exec-node.h"
#include "exec/hdfs-table-writer.h"
#include "exec/hdfs-text-table-writer.h"
#include "exec/parquet/hdfs-parquet-table-writer.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gutil/stringprintf.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/coding-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include <limits>
#include <vector>
#include <sstream>
#include <gutil/strings/substitute.h>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <stdlib.h>

#include "gen-cpp/ImpalaInternalService_constants.h"

#include "common/names.h"

using boost::posix_time::microsec_clock;
using boost::posix_time::ptime;
using namespace strings;

namespace impala {

Status HdfsTableSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  DCHECK(tsink_->__isset.table_sink);
  DCHECK(tsink_->table_sink.__isset.hdfs_table_sink);
  RETURN_IF_ERROR(
      ScalarExpr::Create(tsink_->table_sink.hdfs_table_sink.partition_key_exprs,
          *input_row_desc_, state, &partition_key_exprs_));
  return Status::OK();
}

DataSink* HdfsTableSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
      new HdfsTableSink(sink_id, *this, this->tsink_->table_sink.hdfs_table_sink, state));
}

HdfsTableSink::HdfsTableSink(TDataSinkId sink_id, const HdfsTableSinkConfig& sink_config,
    const THdfsTableSink& hdfs_sink, RuntimeState* state)
  : TableSinkBase(sink_id, sink_config, "HdfsTableSink", state),
    skip_header_line_count_(
        hdfs_sink.__isset.skip_header_line_count ? hdfs_sink.skip_header_line_count : 0),
    overwrite_(hdfs_sink.overwrite),
    input_is_clustered_(hdfs_sink.input_is_clustered),
    sort_columns_(hdfs_sink.sort_columns),
    sorting_order_((TSortingOrder::type)hdfs_sink.sorting_order),
    is_result_sink_(hdfs_sink.is_result_sink) {
  if (hdfs_sink.__isset.write_id) {
    write_id_ = hdfs_sink.write_id;
    DCHECK_GT(write_id_, 0);
  }

  // Optional output path provided by an external FE
  if (hdfs_sink.__isset.external_output_dir) {
    external_output_dir_ = hdfs_sink.external_output_dir;
  }

  if (hdfs_sink.__isset.external_output_partition_depth) {
    external_output_partition_depth_ = hdfs_sink.external_output_partition_depth;
  }

  if (hdfs_sink.__isset.parquet_bloom_filter_col_info) {
    parquet_bloom_filter_columns_ = hdfs_sink.parquet_bloom_filter_col_info;
  }
}

Status HdfsTableSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(TableSinkBase::Prepare(state, parent_mem_tracker));
  unique_id_str_ = PrintId(state->fragment_instance_id(), "-");

  // Resolve table id and set input tuple descriptor.
  table_desc_ = static_cast<const HdfsTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));

  if (table_desc_ == nullptr) {
    stringstream error_msg;
    error_msg << "Failed to get table descriptor for table id: " << table_id_;
    return Status(error_msg.str());
  }

  staging_dir_ = Substitute("$0/_impala_insert_staging/$1", table_desc_->hdfs_base_dir(),
      PrintId(state->query_id(), "_"));

  // Sanity check.
  if (!IsIceberg()) {
    DCHECK_LE(partition_key_expr_evals_.size(), table_desc_->num_cols())
        << DebugString();
    DCHECK_EQ(partition_key_expr_evals_.size(), table_desc_->num_clustering_cols())
        << DebugString();
  }
  DCHECK_GE(output_expr_evals_.size(),
      table_desc_->num_cols() - table_desc_->num_clustering_cols()) << DebugString();

  return Status::OK();
}

void HdfsTableSink::BuildPartitionDescMap() {
  for (const HdfsTableDescriptor::PartitionIdToDescriptorMap::value_type& id_to_desc:
       table_desc_->partition_descriptors()) {
    // Build a map whose key is computed from the value of dynamic partition keys for a
    // particular partition, and whose value is the descriptor for that partition.

    // True if this partition might be written to, false otherwise.
    // A partition may be written to iff:
    // For all partition key exprs e, either:
    //   1. e is not constant
    //   2. The value supplied by the query for this partition key is equal to e's
    //   constant value.
    // Only relevant partitions are remembered in partition_descriptor_map_.
    bool relevant_partition = true;
    HdfsPartitionDescriptor* partition = id_to_desc.second;
    DCHECK_EQ(partition->partition_key_value_evals().size(),
        partition_key_expr_evals_.size());
    vector<ScalarExprEvaluator*> dynamic_partition_key_value_evals;
    for (size_t i = 0; i < partition_key_expr_evals_.size(); ++i) {
      // Remember non-constant partition key exprs for building hash table of Hdfs files
      DCHECK(&partition_key_expr_evals_[i]->root() == partition_key_exprs_[i]);
      if (!partition_key_exprs_[i]->is_constant()) {
        dynamic_partition_key_value_evals.push_back(
            partition->partition_key_value_evals()[i]);
      } else {
        // Deal with the following: one partition has (year=2009, month=3); another has
        // (year=2010, month=3).
        // A query like: INSERT INTO TABLE... PARTITION(year=2009) SELECT month FROM...
        // would lead to both partitions having the same key modulo ignored constant
        // partition keys. So only keep a reference to the partition which matches
        // partition_key_values for constant values, since only that is written to.
        void* table_partition_key_value =
            partition->partition_key_value_evals()[i]->GetValue(nullptr);
        void* target_partition_key_value =
            partition_key_expr_evals_[i]->GetValue(nullptr);
        if (table_partition_key_value == nullptr
            && target_partition_key_value == nullptr) {
          continue;
        }
        if (table_partition_key_value == nullptr
            || target_partition_key_value == nullptr
            || !RawValue::Eq(table_partition_key_value, target_partition_key_value,
                   partition_key_expr_evals_[i]->root().type())) {
          relevant_partition = false;
          break;
        }
      }
    }
    if (relevant_partition) {
      string key;
      // Pass nullptr as row, since all of these expressions are constant, and can
      // therefore be evaluated without a valid row context.
      GetHashTblKey(nullptr, dynamic_partition_key_value_evals, &key);
      DCHECK(partition_descriptor_map_.find(key) == partition_descriptor_map_.end())
          << "Partitions with duplicate 'static' keys found during INSERT";
      partition_descriptor_map_[key] = partition;
    }
  }
}

Status HdfsTableSink::Open(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(TableSinkBase::Open(state));
  if (!IsIceberg()) BuildPartitionDescMap();
  return Status::OK();
}

Status HdfsTableSink::WriteClusteredRowBatch(RuntimeState* state, RowBatch* batch) {
  DCHECK_GT(batch->num_rows(), 0);
  DCHECK(!dynamic_partition_key_expr_evals_.empty());
  DCHECK(input_is_clustered_);

  // Initialize the clustered partition and key.
  if (current_clustered_partition_ == nullptr) {
    const TupleRow* current_row = batch->GetRow(0);
    GetHashTblKey(current_row, dynamic_partition_key_expr_evals_,
        &current_clustered_partition_key_);
    RETURN_IF_ERROR(GetOutputPartition(state, current_row,
        current_clustered_partition_key_, &current_clustered_partition_, false));
  }

  // Compare the last row of the batch to the last current partition key. If they match,
  // then all the rows in the batch have the same key and can be written as a whole.
  string last_row_key;
  GetHashTblKey(batch->GetRow(batch->num_rows() - 1),
      dynamic_partition_key_expr_evals_, &last_row_key);
  if (last_row_key == current_clustered_partition_key_) {
    DCHECK(current_clustered_partition_->second.empty());
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch,
        current_clustered_partition_->first.get()));
    return Status::OK();
  }

  // Not all rows in this batch match the previously written partition key, so we process
  // them individually.
  for (int i = 0; i < batch->num_rows(); ++i) {
    const TupleRow* current_row = batch->GetRow(i);

    string key;
    GetHashTblKey(current_row, dynamic_partition_key_expr_evals_, &key);

    if (current_clustered_partition_key_ != key) {
      DCHECK(current_clustered_partition_ != nullptr);
      // Done with previous partition - write rows and close.
      if (!current_clustered_partition_->second.empty()) {
        RETURN_IF_ERROR(WriteRowsToPartition(state, batch,
            current_clustered_partition_->first.get(),
            current_clustered_partition_->second));
        current_clustered_partition_->second.clear();
      }
      RETURN_IF_ERROR(FinalizePartitionFile(state,
          current_clustered_partition_->first.get()));
      if (current_clustered_partition_->first->writer.get() != nullptr) {
        current_clustered_partition_->first->writer->Close();
      }
      partition_keys_to_output_partitions_.erase(current_clustered_partition_key_);
      current_clustered_partition_key_ = std::move(key);
      RETURN_IF_ERROR(GetOutputPartition(state, current_row,
          current_clustered_partition_key_, &current_clustered_partition_, false));
    }
#ifdef DEBUG
    string debug_row_key;
    GetHashTblKey(current_row, dynamic_partition_key_expr_evals_, &debug_row_key);
    DCHECK_EQ(current_clustered_partition_key_, debug_row_key);
#endif
    DCHECK(current_clustered_partition_ != nullptr);
    current_clustered_partition_->second.push_back(i);
  }
  // Write final set of rows to the partition but keep its file open.
  RETURN_IF_ERROR(WriteRowsToPartition(state, batch,
      current_clustered_partition_->first.get(), current_clustered_partition_->second));
  current_clustered_partition_->second.clear();
  return Status::OK();
}

Status HdfsTableSink::ConstructPartitionInfo(
    const TupleRow* row,
    OutputPartition* output_partition) {
  DCHECK(output_partition != nullptr);
  DCHECK(output_partition->raw_partition_names.empty());

  stringstream url_encoded_partition_name_ss;
  stringstream external_partition_name_ss;
  for (int i = 0; i < partition_key_expr_evals_.size(); ++i) {
    stringstream raw_partition_key_value_ss;
    stringstream encoded_partition_key_value_ss;

    raw_partition_key_value_ss << GetPartitionName(i) << "=";
    encoded_partition_key_value_ss << GetPartitionName(i) << "=";

    void* value = partition_key_expr_evals_[i]->GetValue(row);
    if (value == nullptr) {
      raw_partition_key_value_ss << table_desc_->null_partition_key_value();
      encoded_partition_key_value_ss << table_desc_->null_partition_key_value();
    } else {
      string value_str;
      partition_key_expr_evals_[i]->PrintValue(value, &value_str);

      raw_partition_key_value_ss << value_str;

      string part_key_value = UrlEncodePartitionValue(value_str);
      encoded_partition_key_value_ss << part_key_value;
    }
    if (i < partition_key_expr_evals_.size() - 1) encoded_partition_key_value_ss << "/";

    url_encoded_partition_name_ss << encoded_partition_key_value_ss.str();
    if (HasExternalOutputDir() && i >= external_output_partition_depth_) {
      external_partition_name_ss << encoded_partition_key_value_ss.str();
    }

    output_partition->raw_partition_names.push_back(raw_partition_key_value_ss.str());
  }

  output_partition->partition_name = url_encoded_partition_name_ss.str();
  output_partition->external_partition_name = external_partition_name_ss.str();
  if (IsIceberg()) {
    // Use default partition spec id.
    output_partition->iceberg_spec_id = table_desc_->IcebergSpecId();
  }
  return Status::OK();
}

inline const HdfsPartitionDescriptor* HdfsTableSink::GetPartitionDescriptor(
    const string& key) {
  if (IsIceberg()) return table_desc_->partition_descriptors().begin()->second;
  const HdfsPartitionDescriptor* ret = prototype_partition_;
  PartitionDescriptorMap::const_iterator it = partition_descriptor_map_.find(key);
  if (it != partition_descriptor_map_.end()) ret = it->second;
  return ret;
}

inline Status HdfsTableSink::GetOutputPartition(RuntimeState* state, const TupleRow* row,
    const string& key, PartitionPair** partition_pair, bool no_more_rows) {
  DCHECK(row != nullptr || key == ROOT_PARTITION_KEY);
  PartitionMap::iterator existing_partition;
  existing_partition = partition_keys_to_output_partitions_.find(key);
  if (existing_partition == partition_keys_to_output_partitions_.end()) {
    // Create a new OutputPartition, and add it to partition_keys_to_output_partitions.
    const HdfsPartitionDescriptor* partition_descriptor = GetPartitionDescriptor(key);
    std::unique_ptr<OutputPartition> partition(new OutputPartition());
    // Build the unique name for this partition from the partition keys, e.g. "j=1/f=foo/"
    // etc.
    RETURN_IF_ERROR(ConstructPartitionInfo(row, partition.get()));
    Status status =
        InitOutputPartition(state, *partition_descriptor, partition.get(),
            no_more_rows);
    if (!status.ok()) {
      // We failed to create the output partition successfully. Clean it up now
      // as it is not added to partition_keys_to_output_partitions_ so won't be
      // cleaned up in Close().
      if (partition->writer.get() != nullptr) partition->writer->Close();
      return status;
    }

    // Indicate that temporary directory is to be deleted after execution.
    bool clean_up_staging_dir =
        !no_more_rows && !ShouldSkipStaging(state, partition.get());

    // Save the partition name so that the coordinator can create the partition
    // directory structure if needed.
    state->dml_exec_state()->AddPartition(
        partition->partition_name, partition_descriptor->id(),
        &table_desc_->hdfs_base_dir(),
        clean_up_staging_dir ? &partition->tmp_hdfs_dir_name : nullptr);

    partition_keys_to_output_partitions_[key].first = std::move(partition);
    *partition_pair = &partition_keys_to_output_partitions_[key];
  } else {
    // Use existing output_partition partition.
    *partition_pair = &existing_partition->second;
  }
  return Status::OK();
}

Status HdfsTableSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  // We don't do any work for an empty batch.
  if (batch->num_rows() == 0) return Status::OK();

  // If there are no partition keys then just pass the whole batch to one partition.
  if (dynamic_partition_key_expr_evals_.empty()) {
    // If there are no dynamic keys just use an empty key.
    PartitionPair* partition_pair;
    RETURN_IF_ERROR(
        GetOutputPartition(state, nullptr, ROOT_PARTITION_KEY, &partition_pair, false));
    DCHECK(partition_pair->second.empty());
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, partition_pair->first.get()));
  } else if (input_is_clustered_) {
    RETURN_IF_ERROR(WriteClusteredRowBatch(state, batch));
  } else {
    for (int i = 0; i < batch->num_rows(); ++i) {
      const TupleRow* current_row = batch->GetRow(i);

      string key;
      GetHashTblKey(current_row, dynamic_partition_key_expr_evals_, &key);
      PartitionPair* partition_pair = nullptr;
      RETURN_IF_ERROR(
          GetOutputPartition(state, current_row, key, &partition_pair, false));
      partition_pair->second.push_back(i);
    }
    for (PartitionMap::value_type& partition : partition_keys_to_output_partitions_) {
      PartitionPair& partition_pair = partition.second;
      if (!partition_pair.second.empty()) {
        RETURN_IF_ERROR(WriteRowsToPartition(state, batch, partition_pair.first.get(),
            partition_pair.second));
        partition_pair.second.clear();
      }
    }
  }

  return Status::OK();
}

Status HdfsTableSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  SCOPED_TIMER(profile()->total_time_counter());

  if (dynamic_partition_key_expr_evals_.empty()) {
    // Make sure we create an output partition even if the input is empty because we need
    // it to delete the existing data for 'insert overwrite'.
    PartitionPair* dummy;
    RETURN_IF_ERROR(GetOutputPartition(state, nullptr, ROOT_PARTITION_KEY, &dummy, true));
  }

  // Close Hdfs files, and update stats in runtime state.
  for (PartitionMap::iterator cur_partition =
          partition_keys_to_output_partitions_.begin();
      cur_partition != partition_keys_to_output_partitions_.end();
      ++cur_partition) {
    RETURN_IF_ERROR(FinalizePartitionFile(state, cur_partition->second.first.get()));
  }
  // Returns OK if there is no debug action.
  return DebugAction(state->query_options(), "FIS_FAIL_HDFS_TABLE_SINK_FLUSH_FINAL");
}

void HdfsTableSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(profile()->total_time_counter());
  for (PartitionMap::iterator cur_partition =
          partition_keys_to_output_partitions_.begin();
      cur_partition != partition_keys_to_output_partitions_.end();
      ++cur_partition) {
    if (cur_partition->second.first->writer.get() != nullptr) {
      cur_partition->second.first->writer->Close();
    }
    Status close_status = ClosePartitionFile(state, cur_partition->second.first.get());
    if (!close_status.ok()) state->LogError(close_status.msg());
  }
  partition_keys_to_output_partitions_.clear();
  TableSinkBase::Close(state);
  closed_ = true;
}

string HdfsTableSink::DebugString() const {
  stringstream out;
  out << "HdfsTableSink(overwrite=" << (overwrite_ ? "true" : "false")
      << " table_desc=" << table_desc_->DebugString()
      << " partition_key_exprs="
      << ScalarExpr::DebugString(partition_key_exprs_)
      << " output_exprs=" << ScalarExpr::DebugString(output_exprs_)
      << " write_id=" << write_id_
      << ")";
  return out.str();
}

}
