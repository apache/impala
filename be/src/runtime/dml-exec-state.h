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

#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>
#include <boost/unordered_map.hpp>

#include "common/hdfs.h"
#include "common/status.h"
#include "exec/hdfs-table-writer.h"

namespace impala {

class DmlExecStatusPB;
class DmlPartitionStatusPB;
class DmlStatsPB;
class DmlDataFileStatsPB;
struct OutputPartition;
class TDmlResult;
class TFinalizeParams;
class TUpdateCatalogRequest;
class RuntimeProfile;
class HdfsTableDescriptor;

/// DmlExecState manages the state related to the execution of a DML statement
/// (creation of new files, new partitions, etc.).
///
/// During DML execution, the table sink adds per-partition status using AddPartition()
/// and then UpdatePartition() for non-Kudu tables.  For Kudu tables, the sink adds DML
/// stats using InitForKuduDml() followed by SetKuduDmlStats().  In the case of the
/// HDFS sink, it will also record the collection of files with AddCreatedFile().
///
/// The state is then serialized to thrift and merged at the coordinator using
/// Update().  The coordinator will then use OutputPartitionStats(),
/// GetKuduLatestObservedTimestamp(), PrepareCatalogUpdate() and FinalizeHdfsInsert()
/// to perform various finalization tasks.
///

/// Thread-safe.
class DmlExecState {
 public:
  /// Merge values from 'dml_exec_status'.
  void Update(const DmlExecStatusPB& dml_exec_status);

  /// Add a new partition with the given parameters. Ignores 'base_dir' if nullptr.
  /// If not-nullptr, staging_dir_to_clean_up will be sent to the coordinator which
  /// will delete it.
  /// It is an error to call this for an existing partition.
  void AddPartition(const std::string& name, int64_t id, const std::string* base_dir,
      const std::string* staging_dir_to_clean_up);

  /// Returns true if partition with 'name' already exists.
  bool PartitionExists(const std::string& name) {
    return per_partition_status_.find(name) != per_partition_status_.end();
  }

  /// Merge given values into stats for partition with name 'partition_name'.
  /// Ignores 'insert_stats' if nullptr.
  /// Requires that the partition already exist.
  void UpdatePartition(const std::string& partition_name, int64_t num_rows_delta,
      const DmlStatsPB* insert_stats, bool is_delete = false);

  /// Extract information from 'partition', and add a new data file.
  /// 'insert_stats' contains stats for the Iceberg data file.
  void AddCreatedFile(const OutputPartition& partition, bool is_iceberg,
      const IcebergFileStats& insert_stats);

  /// Extract information from 'partition', and add a new delete file. This function
  /// can only be called for Iceberg tables.
  /// 'insert_stats' contains stats for the Iceberg delete file.
  void AddCreatedDeleteFile(const OutputPartition& partition,
      const IcebergFileStats& insert_stats);

  /// Used to initialize this state when execute Kudu DML. Must be called before
  /// SetKuduDmlStats().
  void InitForKuduDml();

  /// Update stats for a Kudu DML sink. Requires that InitForKuduDml() was already called.
  void SetKuduDmlStats(int64_t num_modified_rows, int64_t num_row_errors,
      int64_t latest_ts);

  /// Outputs the partition stats to a string.
  std::string OutputPartitionStats(const std::string& prefix);

  /// Returns the latest Kudu timestamp observed across any backends where DML into Kudu
  /// was executed, or 0 if there were no Kudu timestamps reported.
  uint64_t GetKuduLatestObservedTimestamp();

  /// Return the total number of modified rows across all partitions.
  int64_t GetNumModifiedRows();

  /// Populates 'catalog_update' with PartitionStatusMap data.
  /// Returns true if a catalog update is required, false otherwise.
  bool PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update);

  /// For HDFS (and other Hadoop FileSystem) INSERT, moves all temporary staging files
  /// to their final destinations, as indicated by 'params', and creates new partitions
  /// for 'hdfs_table' as required.  Adds child timers to profile for the various
  /// stages of finalization.  If the table is on an S3 path and
  /// 's3_skip_insert_staging' is true, does not create new partition directories.
  Status FinalizeHdfsInsert(const TFinalizeParams& params, bool s3_skip_insert_staging,
      HdfsTableDescriptor* hdfs_table, RuntimeProfile* profile) WARN_UNUSED_RESULT;

  /// Serialize to protobuf and stores the result in 'dml_status'.
  void ToProto(DmlExecStatusPB* dml_status);

  /// Populates 'dml_result' with PartitionStatusMap data, for Impala's extension of
  /// Beeswax.
  void ToTDmlResult(TDmlResult* dml_result);

  // Encodes data file list info in flatbuffer format expected by Iceberg API.
  std::vector<std::string> CreateIcebergDataFilesVector();

  // Encodes delete file list info in flatbuffer format expected by Iceberg API.
  std::vector<std::string> CreateIcebergDeleteFilesVector();

  // Returns vector of Iceberg data files referenced by position delete records by
  // this DML statement.
  const std::vector<std::string>& DataFilesReferencedByPositionDeletes() const {
    return data_files_referenced_by_position_deletes_;
  }

  // Reserves capacity for 'data_files_referenced_by_position_deletes_'.
  void reserveReferencedDataFiles(int capacity) {
    data_files_referenced_by_position_deletes_.reserve(capacity);
  }

  // Adds file_path to the list of data files referenced by position delete records.
  void addReferencedDataFile(std::string&& file_path) {
    data_files_referenced_by_position_deletes_.emplace_back(std::move(file_path));
  }

 private:
  /// Auxiliary function used by 'AddCreatedFile' and 'AddCreatedDeleteFile'.
  void AddFileAux(const OutputPartition& partition, bool is_iceberg,
    const IcebergFileStats& insert_stats, bool is_delete);

  /// protects all fields below
  std::mutex lock_;

  /// Counts how many rows an DML query has added to a particular partition (partitions
  /// are identified by their partition keys: k1=v1/k2=v2 etc. Unpartitioned tables
  /// have a single 'default' partition which is identified by ROOT_PARTITION_KEY.
  /// Uses ordered map so that iteration order is deterministic.
  typedef std::map<std::string, DmlPartitionStatusPB> PartitionStatusMap;
  PartitionStatusMap per_partition_status_;

  /// Tracks files to move from a temporary (key) to a final destination (value) as
  /// part of query finalization. If the destination is empty, the file is to be
  /// deleted.  Uses ordered map so that iteration order is deterministic.
  typedef std::map<std::string, std::string> FileMoveMap;
  FileMoveMap files_to_move_;

  /// In case of Iceberg modify statements it contains the data files referenced
  /// by position delete records.
  std::vector<std::string> data_files_referenced_by_position_deletes_;

  /// Determines what the permissions of directories created by INSERT statements should
  /// be if permission inheritance is enabled. Populates a map from all prefixes of
  /// 'path_str' (including the full path itself) which is a path in Hdfs, to pairs
  /// (does_not_exist, permissions), where does_not_exist is true if the path does not
  /// exist in Hdfs. If does_not_exist is true, permissions is set to the permissions of
  /// the most immediate ancestor of the path that does exist, i.e. the permissions that
  /// the path should inherit when created. Otherwise permissions is set to the actual
  /// permissions of the path. The PermissionCache argument is also used to cache the
  /// output across repeated calls, to avoid repeatedly calling hdfsGetPathInfo() on the
  /// same path.
  typedef boost::unordered_map<std::string, std::pair<bool, short>> PermissionCache;
  void PopulatePathPermissionCache(hdfsFS fs, const std::string& path_str,
      PermissionCache* permissions_cache);

  /// Merge 'src' into 'dst'. Not thread-safe.
  void MergeDmlStats(const DmlStatsPB& src, DmlStatsPB* dst);
};

}
