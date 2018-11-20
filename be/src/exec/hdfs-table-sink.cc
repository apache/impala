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
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/coding-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"

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

HdfsTableSink::HdfsTableSink(TDataSinkId sink_id, const RowDescriptor* row_desc,
    const TDataSink& tsink, RuntimeState* state)
  : DataSink(sink_id, row_desc, "HdfsTableSink", state),
    table_desc_(nullptr),
    prototype_partition_(nullptr),
    table_id_(tsink.table_sink.target_table_id),
    skip_header_line_count_(
        tsink.table_sink.hdfs_table_sink.__isset.skip_header_line_count ?
            tsink.table_sink.hdfs_table_sink.skip_header_line_count :
            0),
    overwrite_(tsink.table_sink.hdfs_table_sink.overwrite),
    input_is_clustered_(tsink.table_sink.hdfs_table_sink.input_is_clustered),
    sort_columns_(tsink.table_sink.hdfs_table_sink.sort_columns),
    current_clustered_partition_(nullptr) {
  DCHECK(tsink.__isset.table_sink);
}

OutputPartition::OutputPartition()
  : hdfs_connection(nullptr),
    tmp_hdfs_file(nullptr),
    num_rows(0),
    num_files(0),
    partition_descriptor(nullptr),
    block_size(0) {}

Status HdfsTableSink::Init(const vector<TExpr>& thrift_output_exprs,
    const TDataSink& tsink, RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Init(thrift_output_exprs, tsink, state));
  DCHECK(tsink.__isset.table_sink);
  RETURN_IF_ERROR(ScalarExpr::Create(tsink.table_sink.hdfs_table_sink.partition_key_exprs,
      *row_desc_, state, &partition_key_exprs_));
  return Status::OK();
}

Status HdfsTableSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  unique_id_str_ = PrintId(state->fragment_instance_id(), "-");
  SCOPED_TIMER(profile()->total_time_counter());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_key_exprs_, state,
      state->obj_pool(), expr_perm_pool_.get(), expr_results_pool_.get(),
      &partition_key_expr_evals_));

  // Resolve table id and set input tuple descriptor.
  table_desc_ = static_cast<const HdfsTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));

  if (table_desc_ == nullptr) {
    stringstream error_msg("Failed to get table descriptor for table id: ");
    error_msg << table_id_;
    return Status(error_msg.str());
  }

  staging_dir_ = Substitute("$0/_impala_insert_staging/$1", table_desc_->hdfs_base_dir(),
      PrintId(state->query_id(), "_"));

  // Prepare partition key exprs and gather dynamic partition key exprs.
  for (size_t i = 0; i < partition_key_expr_evals_.size(); ++i) {
    // Remember non-constant partition key exprs for building hash table of Hdfs files.
    if (!partition_key_expr_evals_[i]->root().is_constant()) {
      dynamic_partition_key_expr_evals_.push_back(partition_key_expr_evals_[i]);
    }
  }
  // Sanity check.
  DCHECK_LE(partition_key_expr_evals_.size(), table_desc_->num_cols())
      << DebugString();
  DCHECK_EQ(partition_key_expr_evals_.size(), table_desc_->num_clustering_cols())
      << DebugString();
  DCHECK_GE(output_expr_evals_.size(),
      table_desc_->num_cols() - table_desc_->num_clustering_cols()) << DebugString();

  partitions_created_counter_ = ADD_COUNTER(profile(), "PartitionsCreated", TUnit::UNIT);
  files_created_counter_ = ADD_COUNTER(profile(), "FilesCreated", TUnit::UNIT);
  rows_inserted_counter_ = ADD_COUNTER(profile(), "RowsInserted", TUnit::UNIT);
  bytes_written_counter_ = ADD_COUNTER(profile(), "BytesWritten", TUnit::BYTES);
  encode_timer_ = ADD_TIMER(profile(), "EncodeTimer");
  hdfs_write_timer_ = ADD_TIMER(profile(), "HdfsWriteTimer");
  compress_timer_ = ADD_TIMER(profile(), "CompressTimer");

  return Status::OK();
}

Status HdfsTableSink::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));
  DCHECK_EQ(partition_key_exprs_.size(), partition_key_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(partition_key_expr_evals_, state));

  // Build a map from partition key values to partition descriptor for multiple output
  // format support. The map is keyed on the concatenation of the non-constant keys of
  // the PARTITION clause of the INSERT statement.
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
  prototype_partition_ = CHECK_NOTNULL(table_desc_->prototype_partition_descriptor());
  return Status::OK();
}

void HdfsTableSink::BuildHdfsFileNames(
    const HdfsPartitionDescriptor& partition_descriptor,
    OutputPartition* output_partition) {

  // Create final_hdfs_file_name_prefix and tmp_hdfs_file_name_prefix.
  // Path: <hdfs_base_dir>/<partition_values>/<unique_id_str>

  // Temporary files are written under the following path which is unique to this sink:
  // <table_dir>/_impala_insert_staging/<query_id>/<per_fragment_unique_id>_dir/
  // Both the temporary directory and the file name, when moved to the real partition
  // directory must be unique.
  // Prefix the directory name with "." to make it hidden and append "_dir" at the end
  // of the directory to avoid name clashes for unpartitioned tables.
  // The files are located in <partition_values>/<random_value>_data under
  // tmp_hdfs_file_name_prefix.

  // Use the query id as filename.
  const string& query_suffix = Substitute("$0_$1_data", unique_id_str_, rand());

  output_partition->tmp_hdfs_dir_name =
      Substitute("$0/.$1_$2_dir/", staging_dir_, unique_id_str_, rand());
  output_partition->tmp_hdfs_file_name_prefix = Substitute("$0$1$2",
      output_partition->tmp_hdfs_dir_name, output_partition->partition_name,
      query_suffix);

  if (partition_descriptor.location().empty()) {
    output_partition->final_hdfs_file_name_prefix = Substitute("$0/$1$2",
        table_desc_->hdfs_base_dir(), output_partition->partition_name, query_suffix);
  } else {
    // If the partition descriptor has a location (as set by alter table add partition
    // with a location clause), that provides the complete directory path for this
    // partition. No partition key suffix ("p=1/j=foo/") should be added.
    output_partition->final_hdfs_file_name_prefix =
        Substitute("$0/$1", partition_descriptor.location(), query_suffix);
  }

  output_partition->num_files = 0;
}

Status HdfsTableSink::WriteRowsToPartition(
    RuntimeState* state, RowBatch* batch, PartitionPair* partition_pair) {
  // The rows of this batch may span multiple files. We repeatedly pass the row batch to
  // the writer until it sets new_file to false, indicating that all rows have been
  // written. The writer tracks where it is in the batch when it returns with new_file
  // set.
  bool new_file;
  while (true) {
    OutputPartition* output_partition = partition_pair->first.get();
    RETURN_IF_ERROR(
        output_partition->writer->AppendRows(batch, partition_pair->second, &new_file));
    if (!new_file) break;
    RETURN_IF_ERROR(FinalizePartitionFile(state, output_partition));
    RETURN_IF_ERROR(CreateNewTmpFile(state, output_partition));
  }
  partition_pair->second.clear();
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
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_clustered_partition_));
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
        RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_clustered_partition_));
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
  RETURN_IF_ERROR(WriteRowsToPartition(state, batch, current_clustered_partition_));
  return Status::OK();
}

Status HdfsTableSink::CreateNewTmpFile(RuntimeState* state,
    OutputPartition* output_partition) {
  SCOPED_TIMER(ADD_TIMER(profile(), "TmpFileCreateTimer"));
  string final_location = Substitute("$0.$1.$2",
      output_partition->final_hdfs_file_name_prefix, output_partition->num_files,
      output_partition->writer->file_extension());

  // If ShouldSkipStaging() is true, then the table sink will write the file(s) for this
  // partition to the final location directly. If it is false, the file(s) will be written
  // to a temporary staging location which will be moved by the coordinator to the final
  // location.
  if (ShouldSkipStaging(state, output_partition)) {
    output_partition->current_file_name = final_location;
  } else {
    output_partition->current_file_name = Substitute("$0.$1.$2",
      output_partition->tmp_hdfs_file_name_prefix, output_partition->num_files,
      output_partition->writer->file_extension());
  }
  // Check if tmp_hdfs_file_name exists.
  const char* tmp_hdfs_file_name_cstr =
      output_partition->current_file_name.c_str();

  if (hdfsExists(output_partition->hdfs_connection, tmp_hdfs_file_name_cstr) == 0) {
    return Status(GetHdfsErrorMsg("Temporary HDFS file already exists: ",
        output_partition->current_file_name));
  }

  // This is the block size from the HDFS partition metadata.
  uint64_t block_size = output_partition->partition_descriptor->block_size();
  // hdfsOpenFile takes a 4 byte integer as the block size.
  if (block_size > numeric_limits<int32_t>::max()) {
    return Status(Substitute("HDFS block size must be smaller than 2GB but is configured "
        "in the HDFS partition to $0.", block_size));
  }

  if (block_size == 0) block_size = output_partition->writer->default_block_size();
  if (block_size > numeric_limits<int32_t>::max()) {
    return Status(Substitute("HDFS block size must be smaller than 2GB but the target "
        "table requires $0.", block_size));
  }

  DCHECK_LE(block_size, numeric_limits<int32_t>::max());
  output_partition->tmp_hdfs_file = hdfsOpenFile(output_partition->hdfs_connection,
      tmp_hdfs_file_name_cstr, O_WRONLY, 0, 0, block_size);

  VLOG_FILE << "hdfsOpenFile() file=" << tmp_hdfs_file_name_cstr;
  if (output_partition->tmp_hdfs_file == nullptr) {
    return Status(GetHdfsErrorMsg("Failed to open HDFS file for writing: ",
        output_partition->current_file_name));
  }

  if (IsS3APath(output_partition->current_file_name.c_str()) ||
      IsABFSPath(output_partition->current_file_name.c_str()) ||
      IsADLSPath(output_partition->current_file_name.c_str())) {
    // On S3A, the file cannot be stat'ed until after it's closed, and even so, the block
    // size reported will be just the filesystem default. Similarly, the block size
    // reported for ADLS will be the filesystem default. So, remember the requested block
    // size.
    output_partition->block_size = block_size;
  } else {
    // HDFS may choose to override the block size that we've recommended, so for non-S3
    // files, we get the block size by stat-ing the file.
    hdfsFileInfo* info = hdfsGetPathInfo(output_partition->hdfs_connection,
        output_partition->current_file_name.c_str());
    if (info == nullptr) {
      return Status(GetHdfsErrorMsg("Failed to get info on temporary HDFS file: ",
          output_partition->current_file_name));
    }
    output_partition->block_size = info->mBlockSize;
    hdfsFreeFileInfo(info, 1);
  }

  ImpaladMetrics::NUM_FILES_OPEN_FOR_INSERT->Increment(1);
  COUNTER_ADD(files_created_counter_, 1);

  if (!ShouldSkipStaging(state, output_partition)) {
    // Save the ultimate destination for this file (it will be moved by the coordinator).
    state->dml_exec_state()->AddFileToMove(
        output_partition->current_file_name, final_location);
  }

  ++output_partition->num_files;
  output_partition->num_rows = 0;
  Status status = output_partition->writer->InitNewFile();
  if (!status.ok()) {
    status.MergeStatus(ClosePartitionFile(state, output_partition));
    hdfsDelete(output_partition->hdfs_connection,
        output_partition->current_file_name.c_str(), 0);
  }
  return status;
}

Status HdfsTableSink::InitOutputPartition(RuntimeState* state,
    const HdfsPartitionDescriptor& partition_descriptor, const TupleRow* row,
    OutputPartition* output_partition, bool empty_partition) {
  // Build the unique name for this partition from the partition keys, e.g. "j=1/f=foo/"
  // etc.
  stringstream partition_name_ss;
  for (int j = 0; j < partition_key_expr_evals_.size(); ++j) {
    partition_name_ss << table_desc_->col_descs()[j].name() << "=";
    void* value = partition_key_expr_evals_[j]->GetValue(row);
    // nullptr partition keys get a special value to be compatible with Hive.
    if (value == nullptr) {
      partition_name_ss << table_desc_->null_partition_key_value();
    } else {
      string value_str;
      partition_key_expr_evals_[j]->PrintValue(value, &value_str);
      // Directory names containing partition-key values need to be UrlEncoded, in
      // particular to avoid problems when '/' is part of the key value (which might
      // occur, for example, with date strings). Hive will URL decode the value
      // transparently when Impala's frontend asks the metastore for partition key values,
      // which makes it particularly important that we use the same encoding as Hive. It's
      // also not necessary to encode the values when writing partition metadata. You can
      // check this with 'show partitions <tbl>' in Hive, followed by a select from a
      // decoded partition key value.
      string encoded_str;
      UrlEncode(value_str, &encoded_str, true);
      // If the string is empty, map it to nullptr (mimicking Hive's behaviour)
      partition_name_ss << (encoded_str.empty() ?
                        table_desc_->null_partition_key_value() : encoded_str);
    }
    partition_name_ss << "/";
  }

  // partition_name_ss now holds the unique descriptor for this partition,
  output_partition->partition_name = partition_name_ss.str();
  BuildHdfsFileNames(partition_descriptor, output_partition);

  if (ShouldSkipStaging(state, output_partition)) {
    // We will be writing to the final file if we're skipping staging, so get a connection
    // to its filesystem.
    RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
        output_partition->final_hdfs_file_name_prefix,
        &output_partition->hdfs_connection));
  } else {
    // Else get a connection to the filesystem of the tmp file.
    RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
        output_partition->tmp_hdfs_file_name_prefix, &output_partition->hdfs_connection));
  }

  output_partition->partition_descriptor = &partition_descriptor;

  if (partition_descriptor.file_format() == THdfsFileFormat::SEQUENCE_FILE ||
      partition_descriptor.file_format() == THdfsFileFormat::AVRO) {
    stringstream error_msg;
    map<int, const char*>::const_iterator i =
        _THdfsFileFormat_VALUES_TO_NAMES.find(partition_descriptor.file_format());
    error_msg << "Writing to table format " << i->second << " is not supported.";
    return Status(error_msg.str());
  }
  if (partition_descriptor.file_format() == THdfsFileFormat::TEXT &&
      state->query_options().__isset.compression_codec &&
      state->query_options().compression_codec != THdfsCompression::NONE) {
    stringstream error_msg;
    error_msg << "Writing to compressed text table is not supported. ";
    return Status(error_msg.str());
  }

  // It is incorrect to initialize a writer if there are no rows to feed it. The writer
  // could incorrectly create an empty file or empty partition.
  if (empty_partition) return Status::OK();

  switch (partition_descriptor.file_format()) {
    case THdfsFileFormat::TEXT:
      output_partition->writer.reset(
          new HdfsTextTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_));
      break;
    case THdfsFileFormat::PARQUET:
      output_partition->writer.reset(
          new HdfsParquetTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_));
      break;
    default:
      stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _THdfsFileFormat_VALUES_TO_NAMES.find(partition_descriptor.file_format());
      if (i != _THdfsFileFormat_VALUES_TO_NAMES.end()) {
        error_msg << "Cannot write to table with format " << i->second << ". "
            << "Impala only supports writing to TEXT and PARQUET.";
      } else {
        error_msg << "Cannot write to table. Impala only supports writing to TEXT"
                  << " and PARQUET tables. (Unknown file format: "
                  << partition_descriptor.file_format() << ")";
      }
      return Status(error_msg.str());
  }
  RETURN_IF_ERROR(output_partition->writer->Init());
  COUNTER_ADD(partitions_created_counter_, 1);
  return CreateNewTmpFile(state, output_partition);
}

void HdfsTableSink::GetHashTblKey(const TupleRow* row,
    const vector<ScalarExprEvaluator*>& evals, string* key) {
  stringstream hash_table_key;
  for (int i = 0; i < evals.size(); ++i) {
    RawValue::PrintValueAsBytes(
        evals[i]->GetValue(row), evals[i]->root().type(), &hash_table_key);
    // Additionally append "/" to avoid accidental key collisions.
    hash_table_key << "/";
  }
  *key = hash_table_key.str();
}

inline Status HdfsTableSink::GetOutputPartition(RuntimeState* state, const TupleRow* row,
    const string& key, PartitionPair** partition_pair, bool no_more_rows) {
  DCHECK(row != nullptr || key == ROOT_PARTITION_KEY);
  PartitionMap::iterator existing_partition;
  existing_partition = partition_keys_to_output_partitions_.find(key);
  if (existing_partition == partition_keys_to_output_partitions_.end()) {
    // Create a new OutputPartition, and add it to partition_keys_to_output_partitions.
    const HdfsPartitionDescriptor* partition_descriptor = prototype_partition_;
    PartitionDescriptorMap::const_iterator it = partition_descriptor_map_.find(key);
    if (it != partition_descriptor_map_.end()) {
      partition_descriptor = it->second;
    }

    std::unique_ptr<OutputPartition> partition(new OutputPartition());
    Status status =
        InitOutputPartition(state, *partition_descriptor, row, partition.get(),
            no_more_rows);
    if (!status.ok()) {
      // We failed to create the output partition successfully. Clean it up now
      // as it is not added to partition_keys_to_output_partitions_ so won't be
      // cleaned up in Close().
      if (partition->writer.get() != nullptr) partition->writer->Close();
      return status;
    }

    // Save the partition name so that the coordinator can create the partition
    // directory structure if needed.
    state->dml_exec_state()->AddPartition(
        partition->partition_name, partition_descriptor->id(),
        &table_desc_->hdfs_base_dir());

    if (!no_more_rows && !ShouldSkipStaging(state, partition.get())) {
      // Indicate that temporary directory is to be deleted after execution.
      state->dml_exec_state()->AddFileToMove(partition->tmp_hdfs_dir_name, "");
    }

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
    RETURN_IF_ERROR(WriteRowsToPartition(state, batch, partition_pair));
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
      if (!partition.second.second.empty()) {
        RETURN_IF_ERROR(WriteRowsToPartition(state, batch, &partition.second));
      }
    }
  }
  return Status::OK();
}

Status HdfsTableSink::FinalizePartitionFile(
    RuntimeState* state, OutputPartition* partition) {
  if (partition->tmp_hdfs_file == nullptr && !overwrite_) return Status::OK();
  SCOPED_TIMER(ADD_TIMER(profile(), "FinalizePartitionFileTimer"));

  // OutputPartition writer could be nullptr if there is no row to output.
  if (partition->writer.get() != nullptr) {
    RETURN_IF_ERROR(partition->writer->Finalize());
    state->dml_exec_state()->UpdatePartition(
        partition->partition_name, partition->num_rows, &partition->writer->stats());
  }

  RETURN_IF_ERROR(ClosePartitionFile(state, partition));
  return Status::OK();
}

Status HdfsTableSink::ClosePartitionFile(
    RuntimeState* state, OutputPartition* partition) {
  if (partition->tmp_hdfs_file == nullptr) return Status::OK();
  int hdfs_ret = hdfsCloseFile(partition->hdfs_connection, partition->tmp_hdfs_file);
  VLOG_FILE << "hdfsCloseFile() file=" << partition->current_file_name;
  partition->tmp_hdfs_file = nullptr;
  ImpaladMetrics::NUM_FILES_OPEN_FOR_INSERT->Increment(-1);
  if (hdfs_ret != 0) {
    return Status(ErrorMsg(TErrorCode::GENERAL,
        GetHdfsErrorMsg("Failed to close HDFS file: ",
        partition->current_file_name)));
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

  return Status::OK();
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
  ScalarExprEvaluator::Close(partition_key_expr_evals_, state);
  ScalarExpr::Close(partition_key_exprs_);
  DataSink::Close(state);
  closed_ = true;
}

bool HdfsTableSink::ShouldSkipStaging(RuntimeState* state, OutputPartition* partition) {
  return IsS3APath(partition->final_hdfs_file_name_prefix.c_str()) && !overwrite_ &&
      state->query_options().s3_skip_insert_staging;
}

string HdfsTableSink::DebugString() const {
  stringstream out;
  out << "HdfsTableSink(overwrite=" << (overwrite_ ? "true" : "false")
      << " table_desc=" << table_desc_->DebugString()
      << " partition_key_exprs="
      << ScalarExpr::DebugString(partition_key_exprs_)
      << " output_exprs=" << ScalarExpr::DebugString(output_exprs_)
      << ")";
  return out.str();
}

}
