// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-table-sink.h"
#include "exec/hdfs-text-table-writer.h"
#include "exec/exec-node.h"
#include "gen-cpp/JavaConstants_constants.h"
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

#include "gen-cpp/Data_types.h"

using namespace std;
using namespace boost::posix_time;

namespace impala {

HdfsTableSink::HdfsTableSink(const RowDescriptor& row_desc,
    const TUniqueId& query_id, const vector<TExpr>& select_list_texprs,
    const TDataSink& tsink)
    :  row_desc_(row_desc),
       table_id_(tsink.tableSink.targetTableId),
       select_list_texprs_(select_list_texprs),
       partition_key_texprs_(tsink.tableSink.hdfsTableSink.partitionKeyExprs),
       overwrite_(tsink.tableSink.hdfsTableSink.overwrite) {
  stringstream query_id_ss;
  query_id_ss << query_id.hi << "-" << query_id.lo;
  query_id_str_ = query_id_ss.str();
}

Status HdfsTableSink::PrepareExprs(RuntimeState* state) {
  // Prepare select list expressions.
  RETURN_IF_ERROR(Expr::Prepare(output_exprs_, state, row_desc_));
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

Status HdfsTableSink::Init(RuntimeState* state) {
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
    if (it->first == g_JavaConstants_constants.DEFAULT_PARTITION_ID) {
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
          // partition_key_values for constant values, since only that will be written to.
          if (!RawValue::Eq(partition->partition_key_values()[i]->GetValue(NULL),
                           partition_key_exprs_[i]->GetValue(NULL),
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

  return Status::OK;
}

// Note - this injects a random value into the directory name, so cannot be called
// repeatedly to give the same answer.
static void MakeTmpHdfsDirectoryName(const string& base_dir, const string& query_id,
                                     stringstream* ss) {
  // Append "_dir" at the end of directory to avoid name clashes for unpartitioned tables.
  (*ss) << base_dir << "/" << query_id << "_" << rand() << "_dir/";
}

void HdfsTableSink::BuildHdfsFileNames(OutputPartition* output_partition) {
  // Create hdfs_file_name_template and tmp_hdfs_file_name_template.
  // Path: <hdfs_base_dir>/<partition_values>/<query_id_str>
  stringstream hdfs_file_name_template;
  hdfs_file_name_template << table_desc_->hdfs_base_dir() << "/";

  // Path: <hdfs_base_dir>/<query_id>_dir/<partition_values>/<query_id_str>
  // Both the temporary directory and the file name, when moved to the
  // real partition directory must be unique.
  stringstream tmp_hdfs_file_name_template;
  MakeTmpHdfsDirectoryName(table_desc_->hdfs_base_dir(), query_id_str_,
                           &tmp_hdfs_file_name_template);

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
  hdfs_file_name_template << common_suffix.str() << "_data";
  tmp_hdfs_file_name_template << common_suffix.str() << "_data";
  output_partition->hdfs_file_name_template = hdfs_file_name_template.str();
  output_partition->tmp_hdfs_file_name_template =
      tmp_hdfs_file_name_template.str();
  output_partition->num_files = 0;
}

//TODO: Clean up temporary files on error.
Status HdfsTableSink::CreateNewTmpFile(OutputPartition* output_partition) {
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
  output_partition->tmp_hdfs_file = hdfsOpenFile(hdfs_connection_,
      tmp_hdfs_file_name_template_cstr, O_WRONLY, 0, 0,
      output_partition->partition_descriptor->block_size());
  if (output_partition->tmp_hdfs_file == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to open HDFS file for writing: ",
        output_partition->current_file_name));
  }
  ++output_partition->num_files;
  output_partition->num_rows = 0;
  return Status::OK;
}

Status HdfsTableSink::InitOutputPartition(
    const HdfsPartitionDescriptor& partition_descriptor,
    OutputPartition* output_partition) {
  output_partition->hdfs_connection = hdfs_connection_;

  switch (partition_descriptor.file_format()) {
    case THdfsFileFormat::TEXT: {
      output_partition->writer.reset(
          new HdfsTextTableWriter(output_partition, &partition_descriptor, table_desc_,
                                  output_exprs_));
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
  output_partition->partition_descriptor = &partition_descriptor;
  return CreateNewTmpFile(output_partition);
}

void HdfsTableSink::GetHashTblKey(const vector<Expr*>& exprs, string* key) {
  stringstream hash_table_key;
  TColumnValue col_val;
  for (int i = 0; i < dynamic_partition_key_exprs_.size(); ++i) {
    RawValue::PrintValueAsBytes(exprs[i]->GetValue(current_row_),
                                exprs[0]->type(), &hash_table_key);
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
    RETURN_IF_ERROR(InitOutputPartition(*partition_descriptor, partition));
    partition_keys_to_output_partitions_[key].first = partition;
    *partition_pair = &partition_keys_to_output_partitions_[key];
  } else {
    // Use existing output_partition partition.
    *partition_pair = &existing_partition->second;
  }
  return Status::OK;
}

Status HdfsTableSink::Send(RuntimeState* state, RowBatch* batch) {
  // If there are no partition keys then just pass the whole batch to one partition.
  if (dynamic_partition_key_exprs_.empty()) {
    // If there are no dynamic keys just use an empty key.
    PartitionPair *partition_pair;
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
        RETURN_IF_ERROR(CreateNewTmpFile(output_partition));
      }
    } while (new_file);
  } else {
    for (int i = 0; i < batch->num_rows(); ++i) {
      current_row_ = batch->GetRow(i);

      string key;
      GetHashTblKey(dynamic_partition_key_exprs_, &key);
      PartitionPair* partition_pair;
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
          RETURN_IF_ERROR(CreateNewTmpFile(output_partition));
        }
      } while (new_file);
      partition->second.second.clear();
    }
  }
  return Status::OK;
}

Status HdfsTableSink::FinalizePartitionFile(RuntimeState* state,
                                            OutputPartition* partition) {
  partition->writer->Finalize();
  stringstream filename;
  filename << partition->hdfs_file_name_template << "." << (partition->num_files - 1);
  state->created_hdfs_files().push_back(filename.str());
  state->num_appended_rows().push_back(partition->num_rows);
  // Close file.
  int hdfs_ret = hdfsCloseFile(hdfs_connection_, partition->tmp_hdfs_file);
  if (hdfs_ret != 0) {
    return Status(AppendHdfsErrorMessage("Failed to close HDFS file: ",
                                         partition->current_file_name));
  }

  return Status::OK;
}

Status HdfsTableSink::Close(RuntimeState* state) {
  // Close Hdfs files, and copy return stats to runtime state.
  for (PartitionMap::iterator cur_partition =
           partition_keys_to_output_partitions_.begin();
       cur_partition != partition_keys_to_output_partitions_.end();
       ++cur_partition) {
    RETURN_IF_ERROR(FinalizePartitionFile(state, cur_partition->second.first));
  }
  // Move tmp Hdfs files to their final destination.
  RETURN_IF_ERROR(MoveTmpHdfsFiles());
  return Status::OK;
}

Status HdfsTableSink::MoveTmpHdfsFiles() {
  // 1. Move all tmp Hdfs files to their final destinations.
  // 2. If overwrite_ is true, delete all the original files.
  const string* tmp_file = NULL;
  for (PartitionMap::iterator partition =
       partition_keys_to_output_partitions_.begin();
       partition != partition_keys_to_output_partitions_.end();
       ++partition) {
    RETURN_IF_ERROR(MoveTmpHdfsFile(partition->second.first));
    tmp_file = &partition->second.first->tmp_hdfs_file_name_template;
    if (overwrite_) {
      RETURN_IF_ERROR(DeleteOriginalFiles(partition->second.first));
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

Status HdfsTableSink::DeleteOriginalFiles(OutputPartition* output_partition) {
  DCHECK(overwrite_ == true);
  const char* dest = output_partition->hdfs_file_name_template.c_str();
  // Get the original files in the target dir.
  int num_orig_files = 0;
  hdfsFileInfo* orig_files = NULL;
  string dest_dir = output_partition->hdfs_file_name_template.substr(0,
      output_partition->hdfs_file_name_template.rfind('/') + 1);
  orig_files = hdfsListDirectory(hdfs_connection_, dest_dir.c_str(), &num_orig_files);
  // Delete the original files from the target dir (if any, and if overwrite was set)
  Status status = Status::OK;
  for (int i = 0; i < num_orig_files; ++i) {
    // Don't delete the original file if it has the same name as the file we just moved.
    // Just compare the base name, the file will have a file number appended.
    if (strncmp(orig_files[i].mName, dest, strlen(dest)) == 0) {
      continue;
    }
    VLOG_FILE << "Overwrite INSERT - deleting: " <<  orig_files[i].mName << endl;
    if (hdfsDelete(hdfs_connection_, orig_files[i].mName, 1) == -1) {
      status =  Status(AppendHdfsErrorMessage("Failed to delete existing Hdfs file"
          " as part of overwriting:" + string(orig_files[i].mName)));
      break;
    }
  }
  hdfsFreeFileInfo(orig_files, num_orig_files);
  return status;
}

Status HdfsTableSink::MoveTmpHdfsFile(OutputPartition* output_partition) {
  for (int file_num = 0; file_num < output_partition->num_files; ++file_num) {
    stringstream src;
    src << output_partition->tmp_hdfs_file_name_template << "." << file_num;
    stringstream dest;
    dest << output_partition->hdfs_file_name_template << "." << file_num;
    if (!overwrite_ && hdfsExists(hdfs_connection_, dest.str().c_str()) == 0) {
      return Status(
          AppendHdfsErrorMessage("Target HDFS file already exists: ", dest.str()));
    }
    // Move the file/dir. Note that it is not necessary to create the target first.
    if (hdfsMove(hdfs_connection_,
         src.str().c_str(), hdfs_connection_, dest.str().c_str())) {
      stringstream msg;
      msg << "Failed to move temporary HDFS file/dir to final destination. "
          << "(src: " << src << " / dst: " << dest << ")";
      return Status(AppendHdfsErrorMessage(msg.str()));
    }
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
