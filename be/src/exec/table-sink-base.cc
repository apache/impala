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

#include "exec/table-sink-base.h"

#include "exec/hdfs-text-table-writer.h"
#include "exec/output-partition.h"
#include "exec/parquet/hdfs-parquet-table-writer.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/coding-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/string-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

void TableSinkBaseConfig::Close() {
  ScalarExpr::Close(partition_key_exprs_);
  DataSinkConfig::Close();
}

Status TableSinkBase::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  partitions_created_counter_ = ADD_COUNTER(profile(), "PartitionsCreated", TUnit::UNIT);
  files_created_counter_ = ADD_COUNTER(profile(), "FilesCreated", TUnit::UNIT);
  rows_inserted_counter_ = ADD_COUNTER(profile(), "RowsInserted", TUnit::UNIT);
  bytes_written_counter_ = ADD_COUNTER(profile(), "BytesWritten", TUnit::BYTES);
  encode_timer_ = ADD_TIMER(profile(), "EncodeTimer");
  hdfs_write_timer_ = ADD_TIMER(profile(), "HdfsWriteTimer");
  compress_timer_ = ADD_TIMER(profile(), "CompressTimer");

  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_key_exprs_, state,
      state->obj_pool(), expr_perm_pool_.get(), expr_results_pool_.get(),
      &partition_key_expr_evals_));

  // Prepare partition key exprs and gather dynamic partition key exprs.
  for (size_t i = 0; i < partition_key_expr_evals_.size(); ++i) {
    // Remember non-constant partition key exprs for building hash table of Hdfs files.
    if (!partition_key_expr_evals_[i]->root().is_constant()) {
      dynamic_partition_key_expr_evals_.push_back(partition_key_expr_evals_[i]);
    }
  }

  return Status::OK();
}

Status TableSinkBase::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));
  DCHECK_EQ(partition_key_exprs_.size(), partition_key_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(partition_key_expr_evals_, state));
  prototype_partition_ = CHECK_NOTNULL(table_desc_->prototype_partition_descriptor());
  return Status::OK();
}

void TableSinkBase::Close(RuntimeState* state) {
  ScalarExprEvaluator::Close(partition_key_expr_evals_, state);
  DataSink::Close(state);
}

Status TableSinkBase::ClosePartitionFile(
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

string TableSinkBase::GetPartitionName(int i) {
  if (IsIceberg()) {
    DCHECK_LT(i, partition_key_expr_evals_.size());
    return table_desc_->IcebergNonVoidPartitionFields()[i].field_name;
  } else {
    DCHECK_LT(i, table_desc_->num_clustering_cols());
    return table_desc_->col_descs()[i].name();
  }
}

string TableSinkBase::UrlEncodePartitionValue(const string& raw_str) {
  string encoded_str;
  UrlEncode(raw_str, &encoded_str, true);
  return encoded_str.empty() ? table_desc_->null_partition_key_value() : encoded_str;
}

void TableSinkBase::BuildHdfsFileNames(
    const HdfsPartitionDescriptor& partition_descriptor,
    OutputPartition* output_partition) {

  // Create final_hdfs_file_name_prefix and tmp_hdfs_file_name_prefix.
  // Path: <hdfs_base_dir>/<partition_values>/<unique_id_str>
  // Or, for transactional tables:
  // Path: <hdfs_base_dir>/<partition_values>/<transaction_directory>/<unique_id_str>
  // Where <transaction_directory> is either a 'base' or a 'delta' directory in Hive ACID
  // terminology.

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
      Substitute("$0/.$1_$2_dir/", staging_dir(), unique_id_str_, rand());
  output_partition->tmp_hdfs_file_name_prefix = Substitute("$0$1/$2",
      output_partition->tmp_hdfs_dir_name, output_partition->partition_name,
      query_suffix);

  if (HasExternalOutputDir()) {
    // When an external FE has provided a staging directory we use that directly.
    // We are trusting that the external frontend implementation has done appropriate
    // authorization checks on the external output directory.
    output_partition->final_hdfs_file_name_prefix = Substitute("$0/$1/",
        external_output_dir_, output_partition->external_partition_name);
  } else if (partition_descriptor.location().empty()) {
    output_partition->final_hdfs_file_name_prefix = Substitute("$0/$1/",
        table_desc_->hdfs_base_dir(), output_partition->partition_name);
  } else {
    // If the partition descriptor has a location (as set by alter table add partition
    // with a location clause), that provides the complete directory path for this
    // partition. No partition key suffix ("p=1/j=foo/") should be added.
    output_partition->final_hdfs_file_name_prefix =
        Substitute("$0/", partition_descriptor.location());
  }
  if (IsHiveAcid()) {
    if (HasExternalOutputDir()) {
      // The 0 padding on base and delta is to match the behavior of Hive since various
      // systems will expect a certain format for dynamic partition creation. Additionally
      // include an 0 statement id for delta directory so various Hive AcidUtils detect
      // the directory (such as AcidUtils.baseOrDeltaSubdir()). Multiple statements in a
      // single transaction is not supported.
      if (is_overwrite()) {
        output_partition->final_hdfs_file_name_prefix += StringPrintf("/base_%07ld/",
            write_id());
      } else {
        output_partition->final_hdfs_file_name_prefix += StringPrintf(
            "/delta_%07ld_%07ld_0000/", write_id(), write_id());
      }
    } else {
      string acid_dir = Substitute(
          is_overwrite() ? "/base_$0/" : "/delta_$0_$0/", write_id());
      output_partition->final_hdfs_file_name_prefix += acid_dir;
    }
  }
  if (IsIceberg()) {
    //TODO: implement LocationProviders.
    if (output_partition->partition_name.empty()) {
      output_partition->final_hdfs_file_name_prefix =
          Substitute("$0/data/", table_desc_->IcebergTableLocation());
    } else {
      output_partition->final_hdfs_file_name_prefix =
          Substitute("$0/data/$1/", table_desc_->IcebergTableLocation(),
              output_partition->partition_name);
    }
  }
  output_partition->final_hdfs_file_name_prefix += query_suffix;
  output_partition->num_files = 0;
}

Status TableSinkBase::InitOutputPartition(RuntimeState* state,
    const HdfsPartitionDescriptor& partition_descriptor,
    OutputPartition* output_partition, bool empty_partition) {
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
      state->query_options().compression_codec.codec != THdfsCompression::NONE) {
    stringstream error_msg;
    error_msg << "Writing to compressed text table is not supported. ";
    return Status(error_msg.str());
  }

  // It is incorrect to initialize a writer if there are no rows to feed it. The writer
  // could incorrectly create an empty file or empty partition.
  // However, for transactional tables we should create a new empty base directory in
  // case of INSERT OVERWRITEs.
  if (empty_partition && (!is_overwrite() || !IsTransactional())) return Status::OK();

  switch (partition_descriptor.file_format()) {
    case THdfsFileFormat::TEXT:
      output_partition->writer.reset(
          new HdfsTextTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_));
      break;
    case THdfsFileFormat::ICEBERG:
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

Status TableSinkBase::CreateNewTmpFile(RuntimeState* state,
    OutputPartition* output_partition) {
  SCOPED_TIMER(ADD_TIMER(profile(), "TmpFileCreateTimer"));
  string file_name_pattern =
      output_partition->writer->file_extension().empty() ? "$0.$1" : "$0.$1.$2";
  string final_location = Substitute(file_name_pattern,
      output_partition->final_hdfs_file_name_prefix, output_partition->num_files,
      output_partition->writer->file_extension());

  // If ShouldSkipStaging() is true, then the table sink will write the file(s) for this
  // partition to the final location directly. If it is false, the file(s) will be written
  // to a temporary staging location which will be moved by the coordinator to the final
  // location.
  if (ShouldSkipStaging(state, output_partition)) {
    output_partition->current_file_name = final_location;
    output_partition->current_file_final_name = "";
  } else {
    output_partition->current_file_name = Substitute(file_name_pattern,
        output_partition->tmp_hdfs_file_name_prefix, output_partition->num_files,
        output_partition->writer->file_extension());
    // Save the ultimate destination for this file (it will be moved by the coordinator).
    output_partition->current_file_final_name = final_location;
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

  if (IsS3APath(tmp_hdfs_file_name_cstr) ||
      IsABFSPath(tmp_hdfs_file_name_cstr) ||
      IsADLSPath(tmp_hdfs_file_name_cstr) ||
      IsOSSPath(tmp_hdfs_file_name_cstr) ||
      IsGcsPath(tmp_hdfs_file_name_cstr) ||
      IsCosPath(tmp_hdfs_file_name_cstr) ||
      IsSFSPath(tmp_hdfs_file_name_cstr) ||
      IsOzonePath(tmp_hdfs_file_name_cstr)) {
    // On S3A, the file cannot be stat'ed until after it's closed, and even so, the block
    // size reported will be just the filesystem default. Similarly, the block size
    // reported for ADLS will be the filesystem default. So, remember the requested block
    // size.
    // TODO: IMPALA-9437: Ozone does not support stat'ing a file until after it's closed,
    // so for now skip the call to hdfsGetPathInfo.
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

  ++output_partition->num_files;
  output_partition->current_file_rows = 0;
  Status status = output_partition->writer->InitNewFile();
  if (!status.ok()) {
    status.MergeStatus(ClosePartitionFile(state, output_partition));
    hdfsDelete(output_partition->hdfs_connection,
        output_partition->current_file_name.c_str(), 0);
  }
  return status;
}

Status TableSinkBase::WriteRowsToPartition(
    RuntimeState* state, RowBatch* batch, OutputPartition* output_partition,
    const std::vector<int32_t>& indices) {
  // The rows of this batch may span multiple files. We repeatedly pass the row batch to
  // the writer until it sets new_file to false, indicating that all rows have been
  // written. The writer tracks where it is in the batch when it returns with new_file
  // set.
  bool new_file;
  while (true) {
    Status status =
        output_partition->writer->AppendRows(batch, indices, &new_file);
    if (!status.ok()) {
      // IMPALA-10607: Deletes partition file if staging is skipped when appending rows
      // fails. Otherwise, it leaves the file in un-finalized state.
      if (ShouldSkipStaging(state, output_partition)) {
        status.MergeStatus(ClosePartitionFile(state, output_partition));
        hdfsDelete(output_partition->hdfs_connection,
            output_partition->current_file_name.c_str(), 0);
      }
      return status;
    }
    if (!new_file) break;
    RETURN_IF_ERROR(FinalizePartitionFile(state, output_partition));
    RETURN_IF_ERROR(CreateNewTmpFile(state, output_partition));
  }
  return Status::OK();
}

void TableSinkBase::GetHashTblKey(const TupleRow* row,
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

bool TableSinkBase::ShouldSkipStaging(RuntimeState* state, OutputPartition* partition) {
  if (IsTransactional() || HasExternalOutputDir() || is_result_sink()) return true;
  // We skip staging if we are writing query results
  return (IsS3APath(partition->final_hdfs_file_name_prefix.c_str()) && !is_overwrite() &&
      state->query_options().s3_skip_insert_staging);
}

Status TableSinkBase::FinalizePartitionFile(
    RuntimeState* state, OutputPartition* partition, bool is_delete,
    DmlExecState* dml_exec_state) {
  if (dml_exec_state == nullptr) dml_exec_state = state->dml_exec_state();
  if (partition->tmp_hdfs_file == nullptr && !is_overwrite()) return Status::OK();
  SCOPED_TIMER(ADD_TIMER(profile(), "FinalizePartitionFileTimer"));

  // OutputPartition writer could be nullptr if there is no row to output.
  if (partition->writer.get() != nullptr) {
    RETURN_IF_ERROR(partition->writer->Finalize());
    dml_exec_state->UpdatePartition(
        partition->partition_name, partition->current_file_rows,
        &partition->writer->stats(), is_delete);
    if (is_delete) {
      DCHECK(IsIceberg());
      dml_exec_state->AddCreatedDeleteFile(*partition,
          partition->writer->iceberg_file_stats());
    } else {
      dml_exec_state->AddCreatedFile(*partition, IsIceberg(),
          partition->writer->iceberg_file_stats());
    }
  }

  RETURN_IF_ERROR(ClosePartitionFile(state, partition));
  return Status::OK();
}

}

