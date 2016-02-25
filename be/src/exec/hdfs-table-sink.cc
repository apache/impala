// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/hdfs-table-sink.h"
#include "exec/hdfs-text-table-writer.h"
#include "exec/hdfs-sequence-table-writer.h"
#include "exec/hdfs-avro-table-writer.h"
#include "exec/hdfs-parquet-table-writer.h"
#include "exec/exec-node.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "util/hdfs-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/impalad-metrics.h"
#include "runtime/mem-tracker.h"
#include "util/url-coding.h"

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

const static string& ROOT_PARTITION_KEY =
    g_ImpalaInternalService_constants.ROOT_PARTITION_KEY;

HdfsTableSink::HdfsTableSink(const RowDescriptor& row_desc,
    const vector<TExpr>& select_list_texprs,
    const TDataSink& tsink)
    :  row_desc_(row_desc),
       table_id_(tsink.table_sink.target_table_id),
       select_list_texprs_(select_list_texprs),
       partition_key_texprs_(tsink.table_sink.hdfs_table_sink.partition_key_exprs),
       overwrite_(tsink.table_sink.hdfs_table_sink.overwrite) {
  DCHECK(tsink.__isset.table_sink);
}

OutputPartition::OutputPartition()
    : hdfs_connection(NULL), tmp_hdfs_file(NULL), num_rows(0), num_files(0),
      partition_descriptor(NULL) {
}

Status HdfsTableSink::PrepareExprs(RuntimeState* state) {
  // Prepare select list expressions.
  // Disable codegen for these - they would be unused anyway.
  // TODO: codegen table sink
  RETURN_IF_ERROR(
      Expr::Prepare(output_expr_ctxs_, state, row_desc_, expr_mem_tracker_.get()));
  RETURN_IF_ERROR(
      Expr::Prepare(partition_key_expr_ctxs_, state, row_desc_, expr_mem_tracker_.get()));

  // Prepare partition key exprs and gather dynamic partition key exprs.
  for (size_t i = 0; i < partition_key_expr_ctxs_.size(); ++i) {
    // Remember non-constant partition key exprs for building hash table of Hdfs files.
    if (!partition_key_expr_ctxs_[i]->root()->IsConstant()) {
      dynamic_partition_key_expr_ctxs_.push_back(partition_key_expr_ctxs_[i]);
    }
  }
  // Sanity check.
  DCHECK_LE(partition_key_expr_ctxs_.size(), table_desc_->num_cols())
    << DebugString();
  DCHECK_EQ(partition_key_expr_ctxs_.size(), table_desc_->num_clustering_cols())
    << DebugString();
  DCHECK_GE(output_expr_ctxs_.size(),
      table_desc_->num_cols() - table_desc_->num_clustering_cols()) << DebugString();

  // Prepare literal partition key exprs
  BOOST_FOREACH(
      const HdfsTableDescriptor::PartitionIdToDescriptorMap::value_type& id_to_desc,
      table_desc_->partition_descriptors()) {
    HdfsPartitionDescriptor* partition = id_to_desc.second;
    RETURN_IF_ERROR(partition->PrepareExprs(state));
  }

  return Status::OK();
}

Status HdfsTableSink::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Prepare(state));
  unique_id_str_ = PrintId(state->fragment_instance_id(), "-");
  runtime_profile_ = state->obj_pool()->Add(
      new RuntimeProfile(state->obj_pool(), "HdfsTableSink"));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // TODO: Consider a system-wide random number generator, initialised in a single place.
  ptime now = microsec_clock::local_time();
  long seed = (now.time_of_day().seconds() * 1000)
    + (now.time_of_day().total_microseconds() / 1000);
  VLOG_QUERY << "Random seed: " << seed;
  srand(seed);

  RETURN_IF_ERROR(Expr::CreateExprTrees(
      state->obj_pool(), partition_key_texprs_, &partition_key_expr_ctxs_));
  RETURN_IF_ERROR(Expr::CreateExprTrees(
      state->obj_pool(), select_list_texprs_, &output_expr_ctxs_));

  // Resolve table id and set input tuple descriptor.
  table_desc_ = static_cast<const HdfsTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));

  if (table_desc_ == NULL) {
    stringstream error_msg("Failed to get table descriptor for table id: ");
    error_msg << table_id_;
    return Status(error_msg.str());
  }

  staging_dir_ = Substitute("$0/_impala_insert_staging/$1/", table_desc_->hdfs_base_dir(),
      PrintId(state->query_id(), "_"));

  RETURN_IF_ERROR(PrepareExprs(state));
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      staging_dir_, &hdfs_connection_));
  mem_tracker_.reset(new MemTracker(profile(), -1, -1, profile()->name(),
      state->instance_mem_tracker()));

  partitions_created_counter_ =
      ADD_COUNTER(profile(), "PartitionsCreated", TUnit::UNIT);
  files_created_counter_ =
      ADD_COUNTER(profile(), "FilesCreated", TUnit::UNIT);
  rows_inserted_counter_ =
      ADD_COUNTER(profile(), "RowsInserted", TUnit::UNIT);
  bytes_written_counter_ =
      ADD_COUNTER(profile(), "BytesWritten", TUnit::BYTES);
  encode_timer_ = ADD_TIMER(profile(), "EncodeTimer");
  hdfs_write_timer_ = ADD_TIMER(profile(), "HdfsWriteTimer");
  compress_timer_ = ADD_TIMER(profile(), "CompressTimer");

  return Status::OK();
}

Status HdfsTableSink::Open(RuntimeState* state) {
  RETURN_IF_ERROR(Expr::Open(output_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(partition_key_expr_ctxs_, state));
  // Open literal partition key exprs
  BOOST_FOREACH(
      const HdfsTableDescriptor::PartitionIdToDescriptorMap::value_type& id_to_desc,
      table_desc_->partition_descriptors()) {
    HdfsPartitionDescriptor* partition = id_to_desc.second;
    RETURN_IF_ERROR(partition->OpenExprs(state));
  }

  // Get file format for default partition in table descriptor, and build a map from
  // partition key values to partition descriptor for multiple output format support. The
  // map is keyed on the concatenation of the non-constant keys of the PARTITION clause of
  // the INSERT statement.
  BOOST_FOREACH(
      const HdfsTableDescriptor::PartitionIdToDescriptorMap::value_type& id_to_desc,
      table_desc_->partition_descriptors()) {
    if (id_to_desc.first == g_ImpalaInternalService_constants.DEFAULT_PARTITION_ID) {
      default_partition_ = id_to_desc.second;
    } else {
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
      DCHECK_EQ(partition->partition_key_value_ctxs().size(),
                partition_key_expr_ctxs_.size());
      vector<ExprContext*> dynamic_partition_key_value_ctxs;
      for (size_t i = 0; i < partition_key_expr_ctxs_.size(); ++i) {
        // Remember non-constant partition key exprs for building hash table of Hdfs files
        if (!partition_key_expr_ctxs_[i]->root()->IsConstant()) {
          dynamic_partition_key_value_ctxs.push_back(
              partition->partition_key_value_ctxs()[i]);
        } else {
          // Deal with the following: one partition has (year=2009, month=3); another has
          // (year=2010, month=3).
          // A query like: INSERT INTO TABLE... PARTITION(year=2009) SELECT month FROM...
          // would lead to both partitions having the same key modulo ignored constant
          // partition keys. So only keep a reference to the partition which matches
          // partition_key_values for constant values, since only that is written to.
          void* table_partition_key_value =
              partition->partition_key_value_ctxs()[i]->GetValue(NULL);
          void* target_partition_key_value = partition_key_expr_ctxs_[i]->GetValue(NULL);
          if (table_partition_key_value == NULL && target_partition_key_value == NULL) {
            continue;
          }
          if (table_partition_key_value == NULL || target_partition_key_value == NULL
              || !RawValue::Eq(table_partition_key_value, target_partition_key_value,
                               partition_key_expr_ctxs_[i]->root()->type())) {
            relevant_partition = false;
            break;
          }
        }
      }
      if (relevant_partition) {
        string key;
        // It's ok if current_row_ is NULL (which it should be here), since all of these
        // expressions are constant, and can therefore be evaluated without a valid row
        // context.
        GetHashTblKey(dynamic_partition_key_value_ctxs, &key);
        DCHECK(partition_descriptor_map_.find(key) == partition_descriptor_map_.end())
            << "Partitions with duplicate 'static' keys found during INSERT";
        partition_descriptor_map_[key] = partition;
      }
    }
  }
  if (default_partition_ == NULL) {
    return Status("No default partition found for HdfsTextTableSink");
  }
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

Status HdfsTableSink::CreateNewTmpFile(RuntimeState* state,
    OutputPartition* output_partition) {
  SCOPED_TIMER(ADD_TIMER(profile(), "TmpFileCreateTimer"));
  stringstream filename;
  filename << output_partition->tmp_hdfs_file_name_prefix
           << "." << output_partition->num_files
           << "." << output_partition->writer->file_extension();
  output_partition->current_file_name = filename.str();
  // Check if tmp_hdfs_file_name exists.
  const char* tmp_hdfs_file_name_cstr =
      output_partition->current_file_name.c_str();
  if (hdfsExists(hdfs_connection_, tmp_hdfs_file_name_cstr) == 0) {
    return Status(GetHdfsErrorMsg("Temporary HDFS file already exists: ",
        output_partition->current_file_name));
  }
  uint64_t block_size = output_partition->partition_descriptor->block_size();
  if (block_size == 0) block_size = output_partition->writer->default_block_size();

  output_partition->tmp_hdfs_file = hdfsOpenFile(hdfs_connection_,
      tmp_hdfs_file_name_cstr, O_WRONLY, 0, 0, block_size);
  VLOG_FILE << "hdfsOpenFile() file=" << tmp_hdfs_file_name_cstr;
  if (output_partition->tmp_hdfs_file == NULL) {
    return Status(GetHdfsErrorMsg("Failed to open HDFS file for writing: ",
        output_partition->current_file_name));
  }

  ImpaladMetrics::NUM_FILES_OPEN_FOR_INSERT->Increment(1);
  COUNTER_ADD(files_created_counter_, 1);

  // Save the ultimate destination for this file (it will be moved by the coordinator)
  stringstream dest;
  dest << output_partition->final_hdfs_file_name_prefix
       << "." << output_partition->num_files
       << "." << output_partition->writer->file_extension();
  (*state->hdfs_files_to_move())[output_partition->current_file_name] = dest.str();

  ++output_partition->num_files;
  output_partition->num_rows = 0;
  Status status = output_partition->writer->InitNewFile();
  if (!status.ok()) {
    ClosePartitionFile(state, output_partition);
    hdfsDelete(hdfs_connection_, output_partition->current_file_name.c_str(), 0);
  }
  return status;
}

Status HdfsTableSink::InitOutputPartition(RuntimeState* state,
    const HdfsPartitionDescriptor& partition_descriptor,
    OutputPartition* output_partition, bool empty_partition) {
  // Build the unique name for this partition from the partition keys, e.g. "j=1/f=foo/"
  // etc.
  stringstream partition_name_ss;
  for (int j = 0; j < partition_key_expr_ctxs_.size(); ++j) {
    partition_name_ss << table_desc_->col_descs()[j].name() << "=";
    void* value = partition_key_expr_ctxs_[j]->GetValue(current_row_);
    // NULL partition keys get a special value to be compatible with Hive.
    if (value == NULL) {
      partition_name_ss << table_desc_->null_partition_key_value();
    } else {
      string value_str;
      partition_key_expr_ctxs_[j]->PrintValue(value, &value_str);
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
      // If the string is empty, map it to NULL (mimicking Hive's behaviour)
      partition_name_ss << (encoded_str.empty() ?
                        table_desc_->null_partition_key_value() : encoded_str);
    }
    partition_name_ss << "/";
  }

  // partition_name_ss now holds the unique descriptor for this partition,
  output_partition->partition_name = partition_name_ss.str();
  BuildHdfsFileNames(partition_descriptor, output_partition);

  output_partition->hdfs_connection = hdfs_connection_;
  output_partition->partition_descriptor = &partition_descriptor;

  bool allow_unsupported_formats =
      state->query_options().__isset.allow_unsupported_formats &&
      state->query_options().allow_unsupported_formats;
  if (!allow_unsupported_formats) {
    if (partition_descriptor.file_format() == THdfsFileFormat::SEQUENCE_FILE ||
        partition_descriptor.file_format() == THdfsFileFormat::AVRO) {
      stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _THdfsFileFormat_VALUES_TO_NAMES.find(partition_descriptor.file_format());
      error_msg << "Writing to table format " << i->second
          << " is not supported. Use query option ALLOW_UNSUPPORTED_FORMATS"
          " to override.";
      return Status(error_msg.str());
    }
    if (partition_descriptor.file_format() == THdfsFileFormat::TEXT &&
        state->query_options().__isset.compression_codec &&
        state->query_options().compression_codec != THdfsCompression::NONE) {
      stringstream error_msg;
      error_msg << "Writing to compressed text table is not supported. "
          "Use query option ALLOW_UNSUPPORTED_FORMATS to override.";
      return Status(error_msg.str());
    }
  }

  // It is incorrect to initialize a writer if there are no rows to feed it. The writer
  // could incorrectly create an empty file or empty partition.
  if (empty_partition) return Status::OK();

  switch (partition_descriptor.file_format()) {
    case THdfsFileFormat::TEXT:
      output_partition->writer.reset(
          new HdfsTextTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_,
              output_expr_ctxs_));
      break;
    case THdfsFileFormat::PARQUET:
      output_partition->writer.reset(
          new HdfsParquetTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_,
              output_expr_ctxs_));
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      output_partition->writer.reset(
          new HdfsSequenceTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_,
              output_expr_ctxs_));
      break;
    case THdfsFileFormat::AVRO:
      output_partition->writer.reset(
          new HdfsAvroTableWriter(
              this, state, output_partition, &partition_descriptor, table_desc_,
              output_expr_ctxs_));
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

void HdfsTableSink::GetHashTblKey(const vector<ExprContext*>& ctxs, string* key) {
  stringstream hash_table_key;
  for (int i = 0; i < ctxs.size(); ++i) {
    RawValue::PrintValueAsBytes(
        ctxs[i]->GetValue(current_row_), ctxs[i]->root()->type(), &hash_table_key);
    // Additionally append "/" to avoid accidental key collisions.
    hash_table_key << "/";
  }
  *key = hash_table_key.str();
}

inline Status HdfsTableSink::GetOutputPartition(RuntimeState* state,
    const string& key, PartitionPair** partition_pair, bool no_more_rows) {
  PartitionMap::iterator existing_partition;
  existing_partition = partition_keys_to_output_partitions_.find(key);
  if (existing_partition == partition_keys_to_output_partitions_.end()) {
    // Create a new OutputPartition, and add it to
    // partition_keys_to_output_partitions.
    const HdfsPartitionDescriptor* partition_descriptor = default_partition_;
    PartitionDescriptorMap::const_iterator it = partition_descriptor_map_.find(key);
    if (it != partition_descriptor_map_.end()) {
      partition_descriptor = it->second;
    }

    OutputPartition* partition = state->obj_pool()->Add(new OutputPartition());
    Status status = InitOutputPartition(state, *partition_descriptor, partition,
        no_more_rows);
    if (!status.ok()) {
      // We failed to create the output partition successfully. Clean it up now
      // as it is not added to partition_keys_to_output_partitions_ so won't be
      // cleaned up in Close().
      if (partition->writer.get() != NULL) partition->writer->Close();
      return status;
    }

    // Save the partition name so that the coordinator can create the partition directory
    // structure if needed
    DCHECK(state->per_partition_status()->find(partition->partition_name) ==
        state->per_partition_status()->end());
    TInsertPartitionStatus partition_status;
    partition_status.__set_num_appended_rows(0L);
    partition_status.__set_id(partition_descriptor->id());
    partition_status.__set_stats(TInsertStats());
    state->per_partition_status()->insert(
        make_pair(partition->partition_name, partition_status));

    if (!no_more_rows) {
      // Indicate that temporary directory is to be deleted after execution
      (*state->hdfs_files_to_move())[partition->tmp_hdfs_dir_name] = "";
    }

    partition_keys_to_output_partitions_[key].first = partition;
    *partition_pair = &partition_keys_to_output_partitions_[key];
  } else {
    // Use existing output_partition partition.
    *partition_pair = &existing_partition->second;
  }
  return Status::OK();
}

Status HdfsTableSink::Send(RuntimeState* state, RowBatch* batch, bool eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ExprContext::FreeLocalAllocations(output_expr_ctxs_);
  ExprContext::FreeLocalAllocations(partition_key_expr_ctxs_);
  RETURN_IF_ERROR(state->CheckQueryState());
  bool empty_input_batch = batch->num_rows() == 0;
  // We don't do any work for an empty batch aside from end of stream finalization.
  if (empty_input_batch && !eos) return Status::OK();

  // If there are no partition keys then just pass the whole batch to one partition.
  if (dynamic_partition_key_expr_ctxs_.empty()) {
    // If there are no dynamic keys just use an empty key.
    PartitionPair* partition_pair;
    // Populate the partition_pair even if the input is empty because we need it to
    // delete the existing data for 'insert overwrite'. We need to handle empty input
    // batches carefully so that empty partitions are correctly created at eos.
    RETURN_IF_ERROR(GetOutputPartition(state, ROOT_PARTITION_KEY, &partition_pair,
        empty_input_batch));
    if (!empty_input_batch) {
      // Pass the row batch to the writer. If new_file is returned true then the current
      // file is finalized and a new file is opened.
      // The writer tracks where it is in the batch when it returns with new_file set.
      OutputPartition* output_partition = partition_pair->first;
      bool new_file;
      do {
        RETURN_IF_ERROR(output_partition->writer->AppendRowBatch(
            batch, partition_pair->second, &new_file));
        if (new_file) {
          RETURN_IF_ERROR(FinalizePartitionFile(state, output_partition));
          RETURN_IF_ERROR(CreateNewTmpFile(state, output_partition));
        }
      } while (new_file);
    }
  } else {
    for (int i = 0; i < batch->num_rows(); ++i) {
      current_row_ = batch->GetRow(i);

      string key;
      GetHashTblKey(dynamic_partition_key_expr_ctxs_, &key);
      PartitionPair* partition_pair = NULL;
      RETURN_IF_ERROR(GetOutputPartition(state, key, &partition_pair, false));
      partition_pair->second.push_back(i);
    }
    for (PartitionMap::iterator partition = partition_keys_to_output_partitions_.begin();
         partition != partition_keys_to_output_partitions_.end(); ++partition) {
      OutputPartition* output_partition = partition->second.first;
      if (partition->second.second.empty()) continue;

      bool new_file;
      do {
        RETURN_IF_ERROR(output_partition->writer->AppendRowBatch(
            batch, partition->second.second, &new_file));
        if (new_file) {
          RETURN_IF_ERROR(FinalizePartitionFile(state, output_partition));
          RETURN_IF_ERROR(CreateNewTmpFile(state, output_partition));
        }
      } while (new_file);
      partition->second.second.clear();
    }
  }

  if (eos) {
    // Close Hdfs files, and update stats in runtime state.
    for (PartitionMap::iterator cur_partition =
            partition_keys_to_output_partitions_.begin();
        cur_partition != partition_keys_to_output_partitions_.end();
        ++cur_partition) {
      RETURN_IF_ERROR(FinalizePartitionFile(state, cur_partition->second.first));
    }
  }
  return Status::OK();
}

Status HdfsTableSink::FinalizePartitionFile(RuntimeState* state,
                                            OutputPartition* partition) {
  if (partition->tmp_hdfs_file == NULL && !overwrite_) return Status::OK();
  SCOPED_TIMER(ADD_TIMER(profile(), "FinalizePartitionFileTimer"));

  // OutputPartition writer could be NULL if there is no row to output.
  if (partition->writer.get() != NULL) {
    RETURN_IF_ERROR(partition->writer->Finalize());

    // Track total number of appended rows per partition in runtime
    // state. partition->num_rows counts number of rows appended is per-file.
    PartitionStatusMap::iterator it =
        state->per_partition_status()->find(partition->partition_name);

    // Should have been created in GetOutputPartition() when the partition was initialised.
    DCHECK(it != state->per_partition_status()->end());
    it->second.num_appended_rows += partition->num_rows;
    DataSink::MergeInsertStats(partition->writer->stats(), &it->second.stats);
  }

  ClosePartitionFile(state, partition);
  return Status::OK();
}

void HdfsTableSink::ClosePartitionFile(RuntimeState* state, OutputPartition* partition) {
  if (partition->tmp_hdfs_file == NULL) return;
  int hdfs_ret = hdfsCloseFile(hdfs_connection_, partition->tmp_hdfs_file);
  VLOG_FILE << "hdfsCloseFile() file=" << partition->current_file_name;
  if (hdfs_ret != 0) {
    state->LogError(ErrorMsg(TErrorCode::GENERAL,
        GetHdfsErrorMsg("Failed to close HDFS file: ",
        partition->current_file_name)));
  }
  partition->tmp_hdfs_file = NULL;
  ImpaladMetrics::NUM_FILES_OPEN_FOR_INSERT->Increment(-1);
}

void HdfsTableSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  for (PartitionMap::iterator cur_partition =
          partition_keys_to_output_partitions_.begin();
      cur_partition != partition_keys_to_output_partitions_.end();
      ++cur_partition) {
    if (cur_partition->second.first->writer.get() != NULL) {
      cur_partition->second.first->writer->Close();
    }
    ClosePartitionFile(state, cur_partition->second.first);
  }
  partition_keys_to_output_partitions_.clear();

  // Close literal partition key exprs
  BOOST_FOREACH(
      const HdfsTableDescriptor::PartitionIdToDescriptorMap::value_type& id_to_desc,
      table_desc_->partition_descriptors()) {
    HdfsPartitionDescriptor* partition = id_to_desc.second;
    partition->CloseExprs(state);
  }
  Expr::Close(output_expr_ctxs_, state);
  Expr::Close(partition_key_expr_ctxs_, state);
  if (mem_tracker_.get() != NULL) {
    mem_tracker_->UnregisterFromParent();
    mem_tracker_.reset();
  }
  DataSink::Close(state);
  closed_ = true;
}

Status HdfsTableSink::GetFileBlockSize(OutputPartition* output_partition, int64_t* size) {
  hdfsFileInfo* info = hdfsGetPathInfo(output_partition->hdfs_connection,
      output_partition->current_file_name.c_str());

  if (info == NULL) {
    return Status(GetHdfsErrorMsg("Failed to get info on temporary HDFS file: ",
        output_partition->current_file_name));
  }

  *size = info->mBlockSize;
  hdfsFreeFileInfo(info, 1);

  return Status::OK();
}

string HdfsTableSink::DebugString() const {
  stringstream out;
  out << "HdfsTableSink(overwrite=" << (overwrite_ ? "true" : "false")
      << " table_desc=" << table_desc_->DebugString()
      << " partition_key_exprs=" << Expr::DebugString(partition_key_expr_ctxs_)
      << " output_exprs=" << Expr::DebugString(output_expr_ctxs_)
      << ")";
  return out.str();
}

}
