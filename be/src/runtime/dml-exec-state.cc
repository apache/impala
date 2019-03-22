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

#include "runtime/dml-exec-state.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "exec/data-sink.h"
#include "util/pretty-printer.h"
#include "util/container-util.h"
#include "util/hdfs-bulk-ops.h"
#include "util/hdfs-util.h"
#include "util/runtime-profile-counters.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/exec-env.h"
#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Frontend_types.h"

#include "common/names.h"

DEFINE_bool(insert_inherit_permissions, false, "If true, new directories created by "
    "INSERTs will inherit the permissions of their parent directories");

using namespace impala;
using boost::algorithm::is_any_of;
using boost::algorithm::split;

typedef google::protobuf::Map<string, int64> PerColumnSizePBMap;
typedef google::protobuf::Map<string, DmlPartitionStatusPB> PerPartitionStatusPBMap;

string DmlExecState::OutputPartitionStats(const string& prefix) {
  lock_guard<mutex> l(lock_);
  const char* indent = "  ";
  stringstream ss;
  ss << prefix;
  bool first = true;
  for (const PartitionStatusMap::value_type& val: per_partition_status_) {
    if (!first) ss << endl;
    first = false;
    ss << "Partition: ";
    const string& partition_key = val.first;
    if (partition_key == DataSink::ROOT_PARTITION_KEY) {
      ss << "Default" << endl;
    } else {
      ss << partition_key << endl;
    }
    if (val.second.has_num_modified_rows()) {
      ss << "NumModifiedRows: " << val.second.num_modified_rows() << endl;
    }

    if (!val.second.has_stats()) continue;
    const DmlStatsPB& stats = val.second.stats();
    if (stats.has_kudu_stats()) {
      ss << "NumRowErrors: " << stats.kudu_stats().num_row_errors() << endl;
    }

    ss << indent << "BytesWritten: "
       << PrettyPrinter::Print(stats.bytes_written(), TUnit::BYTES);
    if (stats.has_parquet_stats()) {
      const ParquetDmlStatsPB& parquet_stats = stats.parquet_stats();
      ss << endl << indent << "Per Column Sizes:";
      for (const PerColumnSizePBMap::value_type& i : parquet_stats.per_column_size()) {
        ss << endl << indent << indent << i.first << ": "
           << PrettyPrinter::Print(i.second, TUnit::BYTES);
      }
    }
  }
  return ss.str();
}

void DmlExecState::Update(const DmlExecStatusPB& dml_exec_status) {
  lock_guard<mutex> l(lock_);
  const PerPartitionStatusPBMap& new_partition_status_map =
      dml_exec_status.per_partition_status();
  for (const PerPartitionStatusPBMap::value_type& part : new_partition_status_map) {
    DmlPartitionStatusPB* status = &(per_partition_status_[part.first]);
    status->set_num_modified_rows(
        status->num_modified_rows() + part.second.num_modified_rows());
    status->set_kudu_latest_observed_ts(max<uint64_t>(
        part.second.kudu_latest_observed_ts(), status->kudu_latest_observed_ts()));
    status->set_id(part.second.id());
    status->set_partition_base_dir(part.second.partition_base_dir());
    if (part.second.has_stats()) {
      MergeDmlStats(part.second.stats(), status->mutable_stats());
    }
  }
  files_to_move_.insert(
      dml_exec_status.files_to_move().begin(), dml_exec_status.files_to_move().end());
}

void DmlExecState::AddFileToMove(const string& file_name, const string& location) {
  lock_guard<mutex> l(lock_);
  files_to_move_[file_name] = location;
}

uint64_t DmlExecState::GetKuduLatestObservedTimestamp() {
  lock_guard<mutex> l(lock_);
  uint64_t max_ts = 0;
  for (const PartitionStatusMap::value_type& p : per_partition_status_) {
    max_ts = max<uint64_t>(max_ts, p.second.kudu_latest_observed_ts());
  }
  return max_ts;
}

int64_t DmlExecState::GetNumModifiedRows() {
  lock_guard<mutex> l(lock_);
  int64_t result = 0;
  for (const PartitionStatusMap::value_type& p : per_partition_status_) {
    result += p.second.num_modified_rows();
  }
  return result;
}

bool DmlExecState::PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update) {
  lock_guard<mutex> l(lock_);
  for (const PartitionStatusMap::value_type& partition : per_partition_status_) {
    catalog_update->created_partitions.insert(partition.first);
  }
  return catalog_update->created_partitions.size() != 0;
}

Status DmlExecState::FinalizeHdfsInsert(const TFinalizeParams& params,
    bool s3_skip_insert_staging, HdfsTableDescriptor* hdfs_table,
    RuntimeProfile* profile) {
  lock_guard<mutex> l(lock_);
  PermissionCache permissions_cache;
  HdfsFsCache::HdfsFsMap filesystem_connection_cache;
  HdfsOperationSet partition_create_ops(&filesystem_connection_cache);

  // INSERT finalization happens in the five following steps
  // 1. If OVERWRITE, remove all the files in the target directory
  // 2. Create all the necessary partition directories.

  // Loop over all partitions that were updated by this insert, and create the set of
  // filesystem operations required to create the correct partition structure on disk.
  for (const PartitionStatusMap::value_type& partition : per_partition_status_) {
    SCOPED_TIMER(ADD_CHILD_TIMER(profile, "Overwrite/PartitionCreationTimer",
            "FinalizationTimer"));
    // INSERT allows writes to tables that have partitions on multiple filesystems.
    // So we need to open connections to different filesystems as necessary. We use a
    // local connection cache and populate it with one connection per filesystem that the
    // partitions are on.
    hdfsFS partition_fs_connection;
    RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
        partition.second.partition_base_dir(), &partition_fs_connection,
        &filesystem_connection_cache));

    // Look up the partition in the descriptor table.
    stringstream part_path_ss;
    if (partition.second.id() == -1) {
      // If this is a non-existant partition, use the default partition location of
      // <base_dir>/part_key_1=val/part_key_2=val/...
      part_path_ss << params.hdfs_base_dir << "/" << partition.first;
    } else {
      HdfsPartitionDescriptor* part = hdfs_table->GetPartition(partition.second.id());
      DCHECK(part != nullptr)
          << "table_id=" << hdfs_table->id() << " partition_id=" << partition.second.id();
      part_path_ss << part->location();
    }
    const string& part_path = part_path_ss.str();
    bool is_s3_path = IsS3APath(part_path.c_str());

    // If this is an overwrite insert, we will need to delete any updated partitions
    if (params.is_overwrite) {
      if (partition.first.empty()) {
        // If the root directory is written to, then the table must not be partitioned
        DCHECK(per_partition_status_.size() == 1);
        // We need to be a little more careful, and only delete data files in the root
        // because the tmp directories the sink(s) wrote are there also.
        // So only delete files in the table directory - all files are treated as data
        // files by Hive and Impala, but directories are ignored (and may legitimately
        // be used to store permanent non-table data by other applications).
        int num_files = 0;
        // hfdsListDirectory() only sets errno if there is an error, but it doesn't set
        // it to 0 if the call succeed. When there is no error, errno could be any
        // value. So need to clear errno before calling it.
        // Once HDFS-8407 is fixed, the errno reset won't be needed.
        errno = 0;
        hdfsFileInfo* existing_files =
            hdfsListDirectory(partition_fs_connection, part_path.c_str(), &num_files);
        if (existing_files == nullptr && errno == EAGAIN) {
          errno = 0;
          existing_files =
              hdfsListDirectory(partition_fs_connection, part_path.c_str(), &num_files);
        }
        // hdfsListDirectory() returns nullptr not only when there is an error but also
        // when the directory is empty(HDFS-8407). Need to check errno to make sure
        // the call fails.
        if (existing_files == nullptr && errno != 0) {
          return Status(GetHdfsErrorMsg("Could not list directory: ", part_path));
        }
        for (int i = 0; i < num_files; ++i) {
          const string filename =
              boost::filesystem::path(existing_files[i].mName).filename().string();
          if (existing_files[i].mKind == kObjectKindFile && !IsHiddenFile(filename)) {
            partition_create_ops.Add(DELETE, existing_files[i].mName);
          }
        }
        hdfsFreeFileInfo(existing_files, num_files);
      } else {
        // This is a partition directory, not the root directory; we can delete
        // recursively with abandon, after checking that it ever existed.
        // TODO: There's a potential race here between checking for the directory
        // and a third-party deleting it.
        if (FLAGS_insert_inherit_permissions && !is_s3_path) {
          // There is no directory structure in S3, so "inheriting" permissions is not
          // possible.
          // TODO: Try to mimic inheriting permissions for S3.
          PopulatePathPermissionCache(
              partition_fs_connection, part_path, &permissions_cache);
        }
        // S3 doesn't have a directory structure, so we technically wouldn't need to
        // CREATE_DIR on S3. However, libhdfs always checks if a path exists before
        // carrying out an operation on that path. So we still need to call CREATE_DIR
        // before we access that path due to this limitation.
        if (hdfsExists(partition_fs_connection, part_path.c_str()) != -1) {
          partition_create_ops.Add(DELETE_THEN_CREATE, part_path);
        } else {
          // Otherwise just create the directory.
          partition_create_ops.Add(CREATE_DIR, part_path);
        }
      }
    } else if (!is_s3_path || !s3_skip_insert_staging) {
      // If the S3_SKIP_INSERT_STAGING query option is set, then the partition directories
      // would have already been created by the table sinks.
      if (FLAGS_insert_inherit_permissions && !is_s3_path) {
        PopulatePathPermissionCache(
            partition_fs_connection, part_path, &permissions_cache);
      }
      if (hdfsExists(partition_fs_connection, part_path.c_str()) == -1) {
        partition_create_ops.Add(CREATE_DIR, part_path);
      }
    }
  }

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(profile, "Overwrite/PartitionCreationTimer",
            "FinalizationTimer"));
    if (!partition_create_ops.Execute(
            ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
      for (const HdfsOperationSet::Error& err : partition_create_ops.errors()) {
        // It's ok to ignore errors creating the directories, since they may already
        // exist. If there are permission errors, we'll run into them later.
        if (err.first->op() != CREATE_DIR) {
          return Status(Substitute(
                  "Error(s) deleting partition directories. First error (of $0) was: $1",
                  partition_create_ops.errors().size(), err.second));
        }
      }
    }
  }

  // 3. Move all tmp files
  HdfsOperationSet move_ops(&filesystem_connection_cache);
  HdfsOperationSet dir_deletion_ops(&filesystem_connection_cache);

  for (const FileMoveMap::value_type& move : files_to_move_) {
    // Empty destination means delete, so this is a directory. These get deleted in a
    // separate pass to ensure that we have moved all the contents of the directory first.
    if (move.second.empty()) {
      VLOG_ROW << "Deleting file: " << move.first;
      dir_deletion_ops.Add(DELETE, move.first);
    } else {
      VLOG_ROW << "Moving tmp file: " << move.first << " to " << move.second;
      if (FilesystemsMatch(move.first.c_str(), move.second.c_str())) {
        move_ops.Add(RENAME, move.first, move.second);
      } else {
        move_ops.Add(MOVE, move.first, move.second);
      }
    }
  }

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(profile, "FileMoveTimer", "FinalizationTimer"));
    if (!move_ops.Execute(ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
      stringstream ss;
      ss << "Error(s) moving partition files. First error (of "
         << move_ops.errors().size() << ") was: " << move_ops.errors()[0].second;
      return Status(ss.str());
    }
  }

  // 4. Delete temp directories
  {
    SCOPED_TIMER(ADD_CHILD_TIMER(profile, "FileDeletionTimer", "FinalizationTimer"));
    if (!dir_deletion_ops.Execute(ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
      stringstream ss;
      ss << "Error(s) deleting staging directories. First error (of "
         << dir_deletion_ops.errors().size() << ") was: "
         << dir_deletion_ops.errors()[0].second;
      return Status(ss.str());
    }
  }

  // 5. Optionally update the permissions of the created partition directories
  // Do this last so that we don't make a dir unwritable before we write to it.
  if (FLAGS_insert_inherit_permissions) {
    HdfsOperationSet chmod_ops(&filesystem_connection_cache);
    for (const PermissionCache::value_type& perm : permissions_cache) {
      bool new_dir = perm.second.first;
      if (new_dir) {
        short permissions = perm.second.second;
        VLOG_QUERY << "INSERT created new directory: " << perm.first
                   << ", inherited permissions are: " << oct << permissions;
        chmod_ops.Add(CHMOD, perm.first, permissions);
      }
    }
    if (!chmod_ops.Execute(ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
      stringstream ss;
      ss << "Error(s) setting permissions on newly created partition directories. First"
         << " error (of " << chmod_ops.errors().size() << ") was: "
         << chmod_ops.errors()[0].second;
      return Status(ss.str());
    }
  }
  return Status::OK();
}

void DmlExecState::PopulatePathPermissionCache(hdfsFS fs, const string& path_str,
    PermissionCache* permissions_cache) {
  // Find out if the path begins with a hdfs:// -style prefix, and remove it and the
  // location (e.g. host:port) if so.
  int scheme_end = path_str.find("://");
  string stripped_str;
  if (scheme_end != string::npos) {
    // Skip past the subsequent location:port/ prefix.
    stripped_str = path_str.substr(path_str.find("/", scheme_end + 3));
  } else {
    stripped_str = path_str;
  }

  // Get the list of path components, used to build all path prefixes.
  vector<string> components;
  split(components, stripped_str, is_any_of("/"));

  // Build a set of all prefixes (including the complete string) of stripped_path. So
  // /a/b/c/d leads to a vector of: /a, /a/b, /a/b/c, /a/b/c/d
  vector<string> prefixes;
  // Stores the current prefix
  stringstream accumulator;
  for (const string& component : components) {
    if (component.empty()) continue;
    accumulator << "/" << component;
    prefixes.push_back(accumulator.str());
  }

  // Now for each prefix, stat() it to see if a) it exists and b) if so what its
  // permissions are. When we meet a directory that doesn't exist, we record the fact that
  // we need to create it, and the permissions of its parent dir to inherit.
  //
  // Every prefix is recorded in the PermissionCache so we don't do more than one stat()
  // for each path. If we need to create the directory, we record it as the pair (true,
  // perms) so that the caller can identify which directories need their permissions
  // explicitly set.

  // Set to the permission of the immediate parent (i.e. the permissions to inherit if the
  // current dir doesn't exist).
  short permissions = 0;
  for (const string& path : prefixes) {
    PermissionCache::const_iterator it = permissions_cache->find(path);
    if (it == permissions_cache->end()) {
      hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
      if (info != nullptr) {
        // File exists, so fill the cache with its current permissions.
        permissions_cache->insert(
            make_pair(path, make_pair(false, info->mPermissions)));
        permissions = info->mPermissions;
        hdfsFreeFileInfo(info, 1);
      } else {
        // File doesn't exist, so we need to set its permissions to its immediate parent
        // once it's been created.
        permissions_cache->insert(make_pair(path, make_pair(true, permissions)));
      }
    } else {
      permissions = it->second.second;
    }
  }
}

void DmlExecState::ToProto(DmlExecStatusPB* dml_status) {
  dml_status->Clear();
  lock_guard<mutex> l(lock_);
  for (const FileMoveMap::value_type& file : files_to_move_) {
    (*dml_status->mutable_files_to_move())[file.first] = file.second;
  }
  for (const PartitionStatusMap::value_type& part : per_partition_status_) {
    (*dml_status->mutable_per_partition_status())[part.first] = part.second;
  }
}

void DmlExecState::ToTDmlResult(TDmlResult* dml_result) {
  lock_guard<mutex> l(lock_);
  int64_t num_row_errors = 0;
  bool has_kudu_stats = false;
  for (const PartitionStatusMap::value_type& v: per_partition_status_) {
    dml_result->rows_modified[v.first] = v.second.num_modified_rows();
    if (v.second.has_stats() && v.second.stats().has_kudu_stats()) {
      has_kudu_stats = true;
    }
    num_row_errors += v.second.stats().kudu_stats().num_row_errors();
  }
  if (has_kudu_stats) dml_result->__set_num_row_errors(num_row_errors);
}

void DmlExecState::AddPartition(
    const string& name, int64_t id, const string* base_dir) {
  lock_guard<mutex> l(lock_);
  DCHECK(per_partition_status_.find(name) == per_partition_status_.end());
  DmlPartitionStatusPB status;
  status.set_num_modified_rows(0L);
  status.set_id(id);
  status.mutable_stats()->set_bytes_written(0L);
  status.set_partition_base_dir(base_dir != nullptr ? *base_dir : "");
  per_partition_status_.insert(make_pair(name, status));
}

void DmlExecState::UpdatePartition(const string& partition_name,
    int64_t num_modified_rows_delta, const DmlStatsPB* insert_stats) {
  lock_guard<mutex> l(lock_);
  PartitionStatusMap::iterator entry = per_partition_status_.find(partition_name);
  DCHECK(entry != per_partition_status_.end());
  entry->second.set_num_modified_rows(
      entry->second.num_modified_rows() + num_modified_rows_delta);
  if (insert_stats == nullptr) return;
  MergeDmlStats(*insert_stats, entry->second.mutable_stats());
}

void DmlExecState::MergeDmlStats(const DmlStatsPB& src, DmlStatsPB* dst) {
  dst->set_bytes_written(dst->bytes_written() + src.bytes_written());
  if (src.has_kudu_stats()) {
    KuduDmlStatsPB* kudu_stats = dst->mutable_kudu_stats();
    kudu_stats->set_num_row_errors(
        kudu_stats->num_row_errors() + src.kudu_stats().num_row_errors());
  }
  if (src.has_parquet_stats()) {
    if (dst->has_parquet_stats()) {
      MergeMapValues(src.parquet_stats().per_column_size(),
          dst->mutable_parquet_stats()->mutable_per_column_size());
    } else {
      *dst->mutable_parquet_stats() = src.parquet_stats();
    }
  }
}

void DmlExecState::InitForKuduDml() {
  // For Kudu, track only one set of DML stats, so use the ROOT_PARTITION_KEY.
  const string& partition_name = DataSink::ROOT_PARTITION_KEY;
  lock_guard<mutex> l(lock_);
  DCHECK(per_partition_status_.find(partition_name) == per_partition_status_.end());
  DmlPartitionStatusPB status;
  status.set_id(-1L);
  status.set_num_modified_rows(0L);
  status.mutable_stats()->set_bytes_written(0L);
  status.mutable_stats()->mutable_kudu_stats()->set_num_row_errors(0L);
  status.set_partition_base_dir("");
  per_partition_status_.insert(make_pair(partition_name, status));
}

void DmlExecState::SetKuduDmlStats(int64_t num_modified_rows, int64_t num_row_errors,
    int64_t latest_ts) {
  // For Kudu, track only one set of DML stats, so use the ROOT_PARTITION_KEY.
  const string& partition_name = DataSink::ROOT_PARTITION_KEY;
  lock_guard<mutex> l(lock_);
  PartitionStatusMap::iterator entry = per_partition_status_.find(partition_name);
  DCHECK(entry != per_partition_status_.end());
  entry->second.set_num_modified_rows(num_modified_rows);
  entry->second.mutable_stats()->mutable_kudu_stats()->set_num_row_errors(num_row_errors);
  entry->second.set_kudu_latest_observed_ts(latest_ts);
}
