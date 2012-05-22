// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-text-table-sink.h"
#include "exec/exec-node.h"
#include "util/hdfs-util.h"
#include "exprs/expr.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"

#include <vector>
#include <sstream>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <stdlib.h>

#include "gen-cpp/ImpalaService_types.h"

using namespace std;
using namespace boost::posix_time;

namespace impala {

// Records the temporary and final Hdfs file name,
// the opened temporary Hdfs file, and the number of appended rows
// of an output partition.
class OutputPartition {
 public:
  // Full path to target Hdfs file.
  // Path: <hdfs_base_dir>/<partition_values>/<query_id_str>
  string hdfs_file_name;

  // Temporary file: queryId/hdfs_file_name
  // The file is moved to hdfs_file_name in Close().
  // If overwrite is true, then we move the directory instead of the file.
  // Path: <hdfs_base_dir>/<query_id>_dir/<partition_values>/<query_id_str>
  string tmp_hdfs_file_name;

  // Hdfs file at tmp_hdfs_file_name.
  hdfsFile tmp_hdfs_file;

  // Records number of rows appended to this partition.
  int64_t num_rows;
};


HdfsTextTableSink::HdfsTextTableSink(const RowDescriptor& row_desc,
    const TUniqueId& query_id, const vector<TExpr>& select_list_texprs,
    const TDataSink& tsink)
    :  row_desc_(row_desc),
       table_id_(tsink.tableSink.targetTableId),
       select_list_texprs_(select_list_texprs),
       partition_key_texprs_(tsink.tableSink.hdfsTextTableSink.partitionKeyExprs),
       overwrite_(tsink.tableSink.hdfsTextTableSink.overwrite),
       tuple_delim_(DELIM_INIT),
       field_delim_(DELIM_INIT),
       escape_char_(DELIM_INIT),
       static_partition_(new OutputPartition()) {
  stringstream query_id_ss;
  query_id_ss << query_id.hi << "-" << query_id.lo;
  query_id_str_ = query_id_ss.str();
}

Status HdfsTextTableSink::PrepareExprs(RuntimeState* state) {
  // Prepare select list expressions.
  RETURN_IF_ERROR(Expr::Prepare(select_list_exprs_, state, row_desc_));
  RETURN_IF_ERROR(Expr::Prepare(partition_key_exprs_, state, row_desc_));

  // Prepare partition key exprs and gather dynamic partition key exprs.
  for (size_t i = 0; i < partition_key_exprs_.size(); ++i) {
    // Remember non-constant partition key exprs for building hash table of Hdfs files.
    if (!partition_key_exprs_[i]->IsConstant()) {
      dynamic_partition_key_exprs_.push_back(partition_key_exprs_[i]);
    }
  }
  // Sanity check.
  DCHECK_EQ(partition_key_exprs_.size(), table_desc_->partition_key_names().size());

  return Status::OK;
}

Status HdfsTextTableSink::Init(RuntimeState* state) {
  // TODO: Consider a system-wide random number generator, initialised in a single place.
  ptime now = microsec_clock::local_time();
  long seed = (now.time_of_day().seconds() * 1000)
    + (now.time_of_day().total_microseconds() / 1000);
  VLOG(1) << "Random seed: " << seed;
  srand(seed);

  RETURN_IF_ERROR(Expr::CreateExprTrees(state->obj_pool(),
      partition_key_texprs_, &partition_key_exprs_));
  RETURN_IF_ERROR(Expr::CreateExprTrees(state->obj_pool(), select_list_texprs_,
      &select_list_exprs_));

  // Resolve table id and set input tuple descriptor.
  table_desc_ = static_cast<const HdfsTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));
  if (table_desc_ == NULL) {
    stringstream error_msg("Failed to get table descriptor for table id: ");
    error_msg << table_id_;
    return Status(error_msg.str());
  }
  // Set delimiters from table descriptor.
  tuple_delim_ = table_desc_->line_delim();
  field_delim_ = table_desc_->field_delim();
  escape_char_ = table_desc_->escape_char();
  null_partition_key_value_ = table_desc_->null_partition_key_value();

  PrepareExprs(state);

  // Get Hdfs connection from runtime state.
  hdfs_connection_ = state->fs_cache()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to connect to HDFS."));
  }

  // Open the single output file if we are writing to a static partition.
  if (dynamic_partition_key_exprs_.empty()) {
    RETURN_IF_ERROR(InitOutputPartition(static_partition_.get()));
  }
  return Status::OK;
}

// Note - this injects a random value into the directory name, so cannot be called
// repeatedly to give the same answer.
static void MakeTmpHdfsDirectoryName(const string& base_dir, const string& query_id,
                                     stringstream* ss) {
  // Append "_dir" at the end of directory to avoid name clashes for unpartitioned tables.
  (*ss) << base_dir << "/" << query_id << "_" << rand() << "_dir/";
}

void HdfsTextTableSink::BuildHdfsFileNames(OutputPartition* output) {
  // Create hdfs_file_name and tmp_hdfs_file_name.
  // Path: <hdfs_base_dir>/<partition_values>/<query_id_str>
  stringstream hdfs_file_name;
  hdfs_file_name << table_desc_->hdfs_base_dir() << "/";

  // Path: <hdfs_base_dir>/<query_id>_dir/<partition_values>/<query_id_str>
  stringstream tmp_hdfs_file_name;
  MakeTmpHdfsDirectoryName(table_desc_->hdfs_base_dir(), query_id_str_,
                           &tmp_hdfs_file_name);

  stringstream common_suffix;

  for (int j = 0; j < partition_key_exprs_.size(); ++j) {
    common_suffix << table_desc_->partition_key_names()[j] << "=";
    void* value = partition_key_exprs_[j]->GetValue(current_row_);
    // NULL partition keys get a special value to be compatible with Hive.
    if (value == NULL) {
      common_suffix << null_partition_key_value_;
    } else {
      string value_str;
      partition_key_exprs_[j]->PrintValue(value, &value_str);
      common_suffix << value_str;
    }
    common_suffix << "/";
  }
  // Use the query id as filename.
  common_suffix << query_id_str_ << "_" << rand();
  hdfs_file_name << common_suffix.str() << "_data";
  tmp_hdfs_file_name << common_suffix.str() << "_data";
  output->hdfs_file_name = hdfs_file_name.str();
  output->tmp_hdfs_file_name = tmp_hdfs_file_name.str();
}

Status HdfsTextTableSink::InitOutputPartition(OutputPartition* output) {
  BuildHdfsFileNames(output);

  // Check if tmp_hdfs_file_name exists.
  const char* tmp_hdfs_file_name_cstr = output->tmp_hdfs_file_name.c_str();
  if (hdfsExists(hdfs_connection_, tmp_hdfs_file_name_cstr) == 0) {
    return Status(AppendHdfsErrorMessage("Temporary HDFS file already exists: ",
          output->tmp_hdfs_file_name));
  }
  // Open tmp_hdfs_file_name.
  output->tmp_hdfs_file = hdfsOpenFile(hdfs_connection_,
      tmp_hdfs_file_name_cstr, O_WRONLY, 0, 0, 0);
  if (output->tmp_hdfs_file == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to open HDFS file for writing: ",
          output->tmp_hdfs_file_name));
  }
  output->num_rows = 0;
  return Status::OK;
}

void HdfsTextTableSink::GetHashTblKey(string* key) {
  stringstream hash_table_key;
  TColumnValue col_val;
  for (int i = 0; i < dynamic_partition_key_exprs_.size(); ++i) {
    RawValue::PrintValueAsBytes(dynamic_partition_key_exprs_[i]->GetValue(current_row_),
                                dynamic_partition_key_exprs_[0]->type(), &hash_table_key);
    // Additionally append "/" to avoid accidental key collisions.
    hash_table_key << "/";
  }
  *key = hash_table_key.str();
}

Status HdfsTextTableSink::AppendCurrentRow(OutputPartition* output) {
  stringstream row_stringstream;
  int num_non_partition_cols =
      table_desc_->num_cols() - table_desc_->num_clustering_cols();
  // There might be a select expr for partition cols as well, but we shouldn't be writing
  // their values to the row. Since there must be at least num_non_partition_cols select
  // exprs, and we assume that by convention any partition col exprs are the last in
  // select_list_exprs_, it's ok to just write the first num_non_partition_cols values.
  for (int j = 0; j < num_non_partition_cols; ++j) {
    void* value = select_list_exprs_[j]->GetValue(current_row_);
    // NULL values become empty strings
    if (value != NULL) {
      select_list_exprs_[j]->PrintValue(value, &row_stringstream);
    }
    // Append field delimiter.
    if (j + 1 < num_non_partition_cols) {
      row_stringstream << field_delim_;
    }
  }
  // Append tuple delimiter.
  row_stringstream << tuple_delim_;
  ++output->num_rows;
  // Write line to Hdfs file.
  // HDFS does some buffering to fill a packet which is ~64kb in size. TODO: Determine if
  // there's any throughput benefit in batching larger writes together.
  string row_string = row_stringstream.str();
  int ret = hdfsWrite(hdfs_connection_, output->tmp_hdfs_file, row_string.data(),
                      row_string.length());
  if (ret == -1) {
    stringstream msg;
    msg << "Failed to write row (length: " << row_stringstream.tellp()
        << " to Hdfs file: " << output->tmp_hdfs_file_name;
    return Status(AppendHdfsErrorMessage(msg.str()));
  }
  return Status::OK;
}

Status HdfsTextTableSink::Send(RuntimeState* state, RowBatch* batch) {
  string key;
  for (int i = 0; i < batch->num_rows(); ++i) {
    current_row_ = batch->GetRow(i);
    OutputPartition* output = NULL;
    if (dynamic_partition_key_exprs_.empty()) {
      output = static_partition_.get();
    } else {
      GetHashTblKey(&key);
      HashTable::iterator existing_partition =
          partition_keys_to_output_partitions_.find(key);
      if (existing_partition == partition_keys_to_output_partitions_.end()) {
        // Create a new OutputPartition, and add it to
        // partition_keys_to_output_partitions.
        output = state->obj_pool()->Add(new OutputPartition());
        RETURN_IF_ERROR(InitOutputPartition(output));
        partition_keys_to_output_partitions_[key] = output;
      } else {
        // Use existing output partition.
        output = existing_partition->second;
      }
    }
    // Append current line to output partition.
    RETURN_IF_ERROR(AppendCurrentRow(output));
  }
  return Status::OK;
}

Status HdfsTextTableSink::FinalizePartition(RuntimeState* state,
                                            OutputPartition* partition) {
  state->created_hdfs_files().push_back(partition->hdfs_file_name);
  state->num_appended_rows().push_back(partition->num_rows);
  // Close file.
  int hdfs_ret = hdfsCloseFile(hdfs_connection_, partition->tmp_hdfs_file);
  if (hdfs_ret != 0) {
    return Status(AppendHdfsErrorMessage("Failed to close HDFS file: ", 
          partition->tmp_hdfs_file_name));
  }

  return Status::OK;
}

Status HdfsTextTableSink::Close(RuntimeState* state) {
  // Close Hdfs files, and copy return stats to runtime state.
  if (dynamic_partition_key_exprs_.empty()) {
    RETURN_IF_ERROR(FinalizePartition(state, static_partition_.get()));
  } else {
    for (HashTable::iterator cur_partition =
             partition_keys_to_output_partitions_.begin();
         cur_partition != partition_keys_to_output_partitions_.end();
         ++cur_partition) {
      RETURN_IF_ERROR(FinalizePartition(state, cur_partition->second));
    }
  }
  // Move tmp Hdfs files to their final destination.
  RETURN_IF_ERROR(MoveTmpHdfsFiles());
  return Status::OK;
}

Status HdfsTextTableSink::MoveTmpHdfsFiles() {
  // 1. Move all tmp Hdfs files to their final destinations.
  // 2. If overwrite_ is true, delete all the original files.
  const string* tmp_file = NULL;
  if (dynamic_partition_key_exprs_.empty()) {
    RETURN_IF_ERROR(MoveTmpHdfsFile(static_partition_.get()));
    tmp_file = &static_partition_->tmp_hdfs_file_name;
    if (overwrite_) {
      RETURN_IF_ERROR(DeleteOriginalFiles(static_partition_.get()));
    }
  } else {
    for (HashTable::iterator output_partition_ =
           partition_keys_to_output_partitions_.begin();
           output_partition_ != partition_keys_to_output_partitions_.end();
           ++output_partition_) {
      RETURN_IF_ERROR(MoveTmpHdfsFile(output_partition_->second));
      tmp_file = &output_partition_->second->tmp_hdfs_file_name;
      if (overwrite_) {
        RETURN_IF_ERROR(DeleteOriginalFiles(output_partition_->second));
      }
    }
  }
  // Delete temporary Hdfs dir.
  if (tmp_file != NULL) {
    string tmp_dir = tmp_file->substr(0, tmp_file->rfind('/') + 1);
    if (hdfsDelete(hdfs_connection_, tmp_dir.c_str(), 1) == -1) {
      // For unpartitioned tables, the dir will be deleted as part of the file move.
      if (!dynamic_partition_key_exprs_.empty()) {
        return Status(AppendHdfsErrorMessage("Failed to delete temporary HDFS dir: ",
              tmp_dir));
      }
    }
  }
  return Status::OK;
}

Status HdfsTextTableSink::DeleteOriginalFiles(OutputPartition* output) {
  DCHECK(overwrite_ == true);
  const char* dest = output->hdfs_file_name.c_str();
  // Get the original files in the target dir.
  int num_orig_files = 0;
  hdfsFileInfo* orig_files = NULL;
  string dest_dir = output->hdfs_file_name.substr(0,
                                                  output->hdfs_file_name.rfind('/') + 1);
  orig_files = hdfsListDirectory(hdfs_connection_, dest_dir.c_str(), &num_orig_files);
  // Delete the original files from the target dir (if any, and if overwrite was set)
  for (int i = 0; i < num_orig_files; ++i) {
    // Don't delete the original file if it has the same name as the file we just moved.
    if (strcmp(orig_files[i].mName, dest) == 0) {
      continue;
    }
    VLOG(1) << "Overwrite INSERT - deleting: " <<  orig_files[i].mName << endl;
    if (hdfsDelete(hdfs_connection_, orig_files[i].mName, -1)) {
      string error = AppendHdfsErrorMessage("Failed to delete existing HDFS file "
          "as part of overwriting: ", orig_files[i].mName);
      hdfsFreeFileInfo(orig_files, num_orig_files);
      return Status(error);
    }
  }
  hdfsFreeFileInfo(orig_files, num_orig_files);
  return Status::OK;
}

Status HdfsTextTableSink::MoveTmpHdfsFile(OutputPartition* output) {
  const char* src = output->tmp_hdfs_file_name.c_str();
  const char* dest = output->hdfs_file_name.c_str();
  if (!overwrite_ && hdfsExists(hdfs_connection_, dest) == 0) {
    return Status(AppendHdfsErrorMessage("Target HDFS file already exists: ",
                                         output->hdfs_file_name));
  }
  // Move the file/dir. Note that it is not necessary to create the target first.
  if (hdfsMove(hdfs_connection_, src, hdfs_connection_, dest)) {
    stringstream msg;
    msg << "Failed to move temporary HDFS file/dir to final destination. "
        << "(src: " << src << " / dst: " << dest << ")";
    return Status(AppendHdfsErrorMessage(msg.str()));
  }
  return Status::OK;
}

string HdfsTextTableSink::DebugString() const {
  stringstream out;
  out << "HdfsTextTableSink(overwrite=" << (overwrite_ ? "true" : "false")
      << " partition_key_exprs=" << Expr::DebugString(partition_key_exprs_)
      << ")";
  return out.str();
}

}
