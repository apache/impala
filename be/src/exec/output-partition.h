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

#include <string>

#include <hdfs.h>
#include <boost/scoped_ptr.hpp>

/// Needed for scoped_ptr to work on ObjectPool
#include "common/object-pool.h"

#include "exec/hdfs-table-writer.h"

namespace impala {

/// Records the temporary and final Hdfs file name, the opened temporary Hdfs file, and
/// the number of appended rows of an output partition.
struct OutputPartition {
  /// In the below, <unique_id_str> is the unique ID passed to HdfsTableSink in string
  /// form. It is typically the fragment ID that owns the sink.

  /// Full path to root of the group of files that will be created for this partition.
  /// Each file will have a sequence number appended.  A table writer may produce multiple
  /// files per partition. The root is either partition_descriptor->location (if
  /// non-empty, i.e. the partition has a custom location) or table_dir/partition_name/
  /// Path: <root>/<unique_id_str>
  std::string final_hdfs_file_name_prefix;

  /// File name for current output, with sequence number appended.
  /// This can be a temporary file that will get moved to a permanent location
  /// when we commit the insert.
  /// Path: <hdfs_base_dir>/<partition_values>/<unique_id_str>.<sequence number>[.ext]
  std::string current_file_name;

  // Final location of the currently written file. If empty, then the file won't be moved
  // from current_file_name.
  std::string current_file_final_name;

  /// Name of the temporary directory that files for this partition are staged to before
  /// the coordinator moves them to their permanent location once the query completes.
  /// Not used if 'skip_staging' is true.
  /// Path: <base_table_dir/<staging_dir>/<unique_id>_dir/
  std::string tmp_hdfs_dir_name;

  /// Base prefix for temporary files, to save building it every time a temporary file is
  /// created.
  /// Path: tmp_hdfs_dir_name/partition_name/<unique_id_str>
  std::string tmp_hdfs_file_name_prefix;

  /// key1=val1/key2=val2/ etc. Used to identify partitions to the metastore. Note, the
  /// value in this member is URL encoded for the sake of e.g. data file name creation.
  std::string partition_name;

  /// Used when an external Frontend specifies the staging directory and how partitions
  /// should be created. See IMPALA-10553 for details.
  std::string external_partition_name;

  /// This is a split of the 'partition_name' variable by '/'. Note, the partition keys
  /// and values in this variable are not URL encoded.
  std::vector<std::string> raw_partition_names;

  int32_t iceberg_spec_id = -1;

  /// Connection to hdfs.
  hdfsFS hdfs_connection = nullptr;

  /// Hdfs file at tmp_hdfs_file_name.
  hdfsFile tmp_hdfs_file = nullptr;

  /// Records number of rows appended to the current file in this partition.
  int64_t current_file_rows = 0;

  /// Bytes written to the current file in this partition.
  int64_t current_file_bytes = 0;

  /// Number of files created in this partition.
  int32_t num_files = 0;

  /// Table format specific writer functions.
  boost::scoped_ptr<HdfsTableWriter> writer;

  /// The descriptor for this partition.
  const HdfsPartitionDescriptor* partition_descriptor = nullptr;

  /// The block size decided on for this file.
  int64_t block_size = 0;
};

}
