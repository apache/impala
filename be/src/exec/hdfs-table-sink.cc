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
#include "exec/hdfs-parquet-table-writer.h"
#include "exec/exec-node.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "util/hdfs-util.h"
#include "exprs/expr.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/url-coding.h"

#include <vector>
#include <sstream>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <stdlib.h>

#include "gen-cpp/Data_types.h"

using namespace std;
using namespace boost::posix_time;

namespace impala {

HdfsTableSink::HdfsTableSink(const RowDescriptor& row_desc,
    const TUniqueId& unique_id, const vector<TExpr>& select_list_texprs,
    const TDataSink& tsink)
    :  row_desc_(row_desc),
       table_id_(tsink.table_sink.target_table_id),
       select_list_texprs_(select_list_texprs),
       partition_key_texprs_(tsink.table_sink.hdfs_table_sink.partition_key_exprs),
       overwrite_(tsink.table_sink.hdfs_table_sink.overwrite) {
  DCHECK(tsink.__isset.table_sink);
  stringstream unique_id_ss;
  unique_id_ss << unique_id.hi << "-" << unique_id.lo;
  unique_id_str_ = unique_id_ss.str();
}

Status HdfsTableSink::PrepareExprs(RuntimeState* state) {
  // Prepare select list expressions.
  // Disable codegen for these - they would be unused anyway.
  // TODO: codegen table sink
  RETURN_IF_ERROR(Expr::Prepare(output_exprs_, state, row_desc_, true));
  RETURN_IF_ERROR(Expr::Prepare(partition_key_exprs_, state, row_desc_, true));

  // Prepare partition key exprs and gather dynamic partition key exprs.
  for (size_t i = 0; i < partition_key_exprs_.size(); ++i) {
    // Remember non-constant partition key exprs for building hash table of Hdfs files.
    if (!partition_key_exprs_[i]->IsConstant()) {
      dynamic_partition_key_exprs_.push_back(partition_key_exprs_[i]);
    }
  }
  // Sanity check.
  DCHECK_LE(partition_key_exprs_.size(), table_desc_->col_names().size());

  return Status::OK;
}

Status HdfsTableSink::Init(RuntimeState* state) {
  runtime_profile_ = state->obj_pool()->Add(
      new RuntimeProfile(state->obj_pool(), "HdfsTableSink"));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // TODO: Consider a system-wide random number generator, initialised in a single place.
  ptime now = microsec_clock::local_time();
  long seed = (now.time_of_day().seconds() * 1000)
    + (now.time_of_day().total_microseconds() / 1000);
  VLOG_QUERY << "Random seed: " << seed;
  srand(seed);

  RETURN_IF_ERROR(Expr::CreateExprTrees(state->obj_pool(),
      partition_key_texprs_, &partition_key_exprs_));
  RETURN_IF_ERROR(Expr::CreateExprTrees(state->obj_pool(), select_list_texprs_,
      &output_exprs_));

  // Resolve table id and set input tuple descriptor.
  table_desc_ = static_cast<const HdfsTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));

  if (table_desc_ == NULL) {
    stringstream error_msg("Failed to get table descriptor for table id: ");
    error_msg << table_id_;
    return Status(error_msg.str());
  }

  PrepareExprs(state);

  // Get file format for default partition in table descriptor, and
  // build a map from partition key values to partition descriptor for
  // multiple output format support. The map is keyed on the
  // concatenation of the non-constant keys of the PARTITION clause of
  // the INSERT statement.
  HdfsTableDescriptor::PartitionIdToDescriptorMap::const_iterator it;
  for (it = table_desc_->partition_descriptors().begin();
       it != table_desc_->partition_descriptors().end();
       ++it) {
    if (it->first == g_ImpalaInternalService_constants.DEFAULT_PARTITION_ID) {
      default_partition_ = it->second;
    } else {
      // Evaluate non-constant partition keys and build a map from hash value to
      // partition descriptor
      bool relevant_partition = true;
      HdfsPartitionDescriptor* partition = it->second;
      partition->PrepareExprs(state);
      DCHECK_EQ(partition->partition_key_values().size(), partition_key_exprs_.size());
      vector<Expr*> dynamic_partition_key_values;
      for (size_t i = 0; i < partition_key_exprs_.size(); ++i) {
        // Remember non-constant partition key exprs for building hash table of Hdfs files
        if (!partition_key_exprs_[i]->IsConstant()) {
          dynamic_partition_key_values.push_back(
              partition->partition_key_values()[i]);
        } else {
          // Deal with the following: one partition has (year=2009, month=3); another has
          // (year=2010, month=3).
          // A query like: INSERT INTO TABLE... PARTITION(year=2009) SELECT month FROM...
          // would lead to both partitions having the same key modulo ignored constant
          // partition keys. So only keep a reference to the partition which matches
          // partition_key_values for constant values, since only that is written to.
          void* table_partition_key_value =
              partition->partition_key_values()[i]->GetValue(NULL);
          void* target_partition_key_value = partition_key_exprs_[i]->GetValue(NULL);
          if (table_partition_key_value == NULL && target_partition_key_value == NULL) {
            break;
          }
          if (table_partition_key_value == NULL || target_partition_key_value == NULL
              || !RawValue::Eq(table_partition_key_value, target_partition_key_value,
                               partition_key_exprs_[i]->type())) {
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
        GetHashTblKey(dynamic_partition_key_values, &key);
        partition_descriptor_map_[key] = partition;
      }
    }
  }

  if (default_partition_ == NULL) {
    return Status("No default partition found for HdfsTextTableSink");
  }

  // Get Hdfs connection from runtime state.
  hdfs_connection_ = state->fs_cache()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to connect to HDFS."));
  }

  rows_inserted_counter_ =
      ADD_COUNTER(profile(), "RowsInserted", TCounterType::UNIT);
  memory_used_counter_ =
      ADD_COUNTER(profile(), "MemoryUsed", TCounterType::BYTES);
  encode_timer_ = ADD_TIMER(profile(), "EncodeTimer");
  hdfs_write_timer_ = ADD_TIMER(profile(), "HdfsWriteTimer");

  return Status::OK;
}

// Note - this injects a random value into the directory name, so cannot be called
// repeatedly to give the same answer.
static void MakeTmpHdfsDirectoryName(const string& base_dir, const string& unique_id,
                                     stringstream* ss) {
  // Append "_dir" at the end of directory to avoid name clashes for unpartitioned tables.
  (*ss) << base_dir << "/" << unique_id << "_" << rand() << "_dir/";
}

void HdfsTableSink::BuildHdfsFileNames(OutputPartition* output_partition) {
  // Create hdfs_file_name_template and tmp_hdfs_file_name_template.
  // Path: <hdfs_base_dir>/<partition_values>/<unique_id_str>
  stringstream hdfs_file_name_template;
  hdfs_file_name_template << table_desc_->hdfs_base_dir() << "/";

  // Path: <hdfs_base_dir>/<unique_id>_dir/<partition_values>/<unique_id_str>
  // Both the temporary directory and the file name, when moved to the
  // real partition directory must be unique.
  stringstream tmp_hdfs_file_name_template;
  MakeTmpHdfsDirectoryName(table_desc_->hdfs_base_dir(), unique_id_str_,
                           &tmp_hdfs_file_name_template);
  output_partition->tmp_hdfs_dir_name = tmp_hdfs_file_name_template.str();

  stringstream common_suffix;

  for (int j = 0; j < partition_key_exprs_.size(); ++j) {
    common_suffix << table_desc_->col_names()[j] << "=";
    void* value = partition_key_exprs_[j]->GetValue(current_row_);
    // NULL partition keys get a special value to be compatible with Hive.
    if (value == NULL) {
      common_suffix << table_desc_->null_partition_key_value();
    } else {
      string value_str;
      partition_key_exprs_[j]->PrintValue(value, &value_str);
      // Directory names containing partition-key values need to be
      // UrlEncoded, in particular to avoid problems when '/' is part
      // of the key value (which might occur, for example, with date
      // strings). Hive will URL decode the value transparently when
      // Impala's frontend asks the metastore for partition key
      // values, which makes it particularly important that we use the
      // same encoding as Hive. It's also not necessary to encode the
      // values when writing partition metadata. You can check this
      // with 'show partitions <tbl>' in Hive, followed by a select
      // from a decoded partition key value.
      string encoded_str;
      // The final parameter forces compatibility with Hive, which
      // doesn't URL-encode every character.
      UrlEncode(value_str, &encoded_str, true);
      common_suffix << encoded_str;
    }
    common_suffix << "/";
  }

  // common_suffix now holds the unique descriptor for this partition,
  // save it before we build the suffix out further
  output_partition->partition_name = common_suffix.str();

  // Use the query id as filename.
  common_suffix << unique_id_str_ << "_" << rand();
  hdfs_file_name_template << common_suffix.str() << "_data";
  tmp_hdfs_file_name_template << common_suffix.str() << "_data";
  output_partition->hdfs_file_name_template = hdfs_file_name_template.str();
  output_partition->tmp_hdfs_file_name_template =
      tmp_hdfs_file_name_template.str();
  output_partition->num_files = 0;
}

//TODO: Clean up temporary files on error.
Status HdfsTableSink::CreateNewTmpFile(RuntimeState* state,
    OutputPartition* output_partition) {
  stringstream filename;
  filename << output_partition->tmp_hdfs_file_name_template
      << "." << output_partition->num_files;
  output_partition->current_file_name = filename.str();
  // Check if tmp_hdfs_file_name_template exists.
  const char* tmp_hdfs_file_name_template_cstr =
      output_partition->current_file_name.c_str();
  if (hdfsExists(hdfs_connection_, tmp_hdfs_file_name_template_cstr) == 0) {
    return Status(AppendHdfsErrorMessage("Temporary HDFS file already exists: ",
        output_partition->current_file_name));
  }
  uint64_t block_size = output_partition->partition_descriptor->block_size();
  if (block_size == 0) block_size = output_partition->writer->default_block_size();

  output_partition->tmp_hdfs_file = hdfsOpenFile(hdfs_connection_,
      tmp_hdfs_file_name_template_cstr, O_WRONLY, 0, 0, block_size);
  if (output_partition->tmp_hdfs_file == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to open HDFS file for writing: ",
        output_partition->current_file_name));
  }

  // Save the ultimate destination for this file (it will be moved by the coordinator)
  stringstream dest;
  dest << output_partition->hdfs_file_name_template << "." << output_partition->num_files;
  (*state->hdfs_files_to_move())[output_partition->current_file_name] = dest.str();

  ++output_partition->num_files;
  output_partition->num_rows = 0;
  RETURN_IF_ERROR(output_partition->writer->InitNewFile());
  return Status::OK;
}

Status HdfsTableSink::InitOutputPartition(RuntimeState* state,
    const HdfsPartitionDescriptor& partition_descriptor,
    OutputPartition* output_partition) {
  output_partition->hdfs_connection = hdfs_connection_;

  switch (partition_descriptor.file_format()) {
    case THdfsFileFormat::TEXT: {
      output_partition->writer.reset(
          new HdfsTextTableWriter(this, state, output_partition,
                                  &partition_descriptor, table_desc_, output_exprs_));
      break;
    }
    case THdfsFileFormat::PARQUET: {
      output_partition->writer.reset(
          new HdfsParquetTableWriter(this, state, output_partition,
                                    &partition_descriptor, table_desc_, output_exprs_));
      break;
    }
    default:
      stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _THdfsFileFormat_VALUES_TO_NAMES.find(partition_descriptor.file_format());
      const char* str = "Unknown data sink type ";
      if (i != _THdfsFileFormat_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      error_msg << str << " not implemented.";
      return Status(error_msg.str());
  }
  RETURN_IF_ERROR(output_partition->writer->Init());
  output_partition->partition_descriptor = &partition_descriptor;
  return CreateNewTmpFile(state, output_partition);
}

void HdfsTableSink::GetHashTblKey(const vector<Expr*>& exprs, string* key) {
  stringstream hash_table_key;
  for (int i = 0; i < dynamic_partition_key_exprs_.size(); ++i) {
    RawValue::PrintValueAsBytes(exprs[i]->GetValue(current_row_),
                                exprs[i]->type(), &hash_table_key);
    // Additionally append "/" to avoid accidental key collisions.
    hash_table_key << "/";
  }
  *key = hash_table_key.str();
}

inline Status HdfsTableSink::GetOutputPartition(
   RuntimeState* state, const string& key, PartitionPair** partition_pair) {
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
    BuildHdfsFileNames(partition);
    RETURN_IF_ERROR(InitOutputPartition(state, *partition_descriptor, partition));

    // Save the partition name so that the coordinator can create partition
    // directory structure if needed
    if (overwrite_) {
      DCHECK(state->num_appended_rows()->find(partition->partition_name) ==
          state->num_appended_rows()->end());
      (*state->num_appended_rows())[partition->partition_name] = 0L;
    }

    // Indicate that temporary directory is to be deleted after execution
    (*state->hdfs_files_to_move())[partition->tmp_hdfs_dir_name] = "";

    partition_keys_to_output_partitions_[key].first = partition;
    *partition_pair = &partition_keys_to_output_partitions_[key];
  } else {
    // Use existing output_partition partition.
    *partition_pair = &existing_partition->second;
  }
  return Status::OK;
}

Status HdfsTableSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // If there are no partition keys then just pass the whole batch to one partition.
  if (dynamic_partition_key_exprs_.empty()) {
    // If there are no dynamic keys just use an empty key.
    PartitionPair* partition_pair;
    RETURN_IF_ERROR(GetOutputPartition(state, "", &partition_pair));
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
  } else {
    for (int i = 0; i < batch->num_rows(); ++i) {
      current_row_ = batch->GetRow(i);

      string key;
      GetHashTblKey(dynamic_partition_key_exprs_, &key);
      PartitionPair* partition_pair = NULL;
      RETURN_IF_ERROR(GetOutputPartition(state, key, &partition_pair));
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
  return Status::OK;
}

Status HdfsTableSink::FinalizePartitionFile(RuntimeState* state,
                                            OutputPartition* partition) {
  RETURN_IF_ERROR(partition->writer->Finalize());

  // Track total number of appended rows per partition in runtime
  // state. partition->num_rows counts number of rows appended is per-file.
  (*state->num_appended_rows())[partition->partition_name] += partition->num_rows;

  // Close file.
  int hdfs_ret = hdfsCloseFile(hdfs_connection_, partition->tmp_hdfs_file);
  if (hdfs_ret != 0) {
    return Status(AppendHdfsErrorMessage("Failed to close HDFS file: ",
                                         partition->current_file_name));
  }

  return Status::OK;
}

Status HdfsTableSink::Close(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Close Hdfs files, and copy return stats to runtime state.
  for (PartitionMap::iterator cur_partition =
           partition_keys_to_output_partitions_.begin();
       cur_partition != partition_keys_to_output_partitions_.end();
       ++cur_partition) {
    RETURN_IF_ERROR(FinalizePartitionFile(state, cur_partition->second.first));
  }
  return Status::OK;
}

Status HdfsTableSink::GetFileBlockSize(OutputPartition* output_partition, int64_t* size) {
  hdfsFileInfo* info = hdfsGetPathInfo(output_partition->hdfs_connection,
      output_partition->current_file_name.c_str());

  if (info == NULL) {
    stringstream msg;
    msg << "Failed to get info on temporary HDFS file."
        << output_partition->current_file_name;
    return Status(AppendHdfsErrorMessage(msg.str()));
  }

  *size = info->mBlockSize;
  hdfsFreeFileInfo(info, 1);

  return Status::OK;
}

string HdfsTableSink::DebugString() const {
  stringstream out;
  out << "HdfsTableSink(overwrite=" << (overwrite_ ? "true" : "false")
      << " partition_key_exprs=" << Expr::DebugString(partition_key_exprs_)
      << ")";
  return out.str();
}

}
