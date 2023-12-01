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


#ifndef IMPALA_EXEC_HDFS_TABLE_SINK_H
#define IMPALA_EXEC_HDFS_TABLE_SINK_H

#include <hdfs.h>
#include <boost/unordered_map.hpp>

#include "exec/output-partition.h"
#include "exec/table-sink-base.h"
#include "runtime/descriptors.h"

namespace impala {

class Expr;
class TupleDescriptor;
class RuntimeState;
class MemTracker;

class HdfsTableSinkConfig : public TableSinkBaseConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  ~HdfsTableSinkConfig() override {}

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;
};

/// The sink consumes all row batches of its child execution tree, and writes the
/// evaluated output_exprs into temporary Hdfs files. The query coordinator moves the
/// temporary files into their final locations after the sinks have finished executing.
//
/// This sink supports static and dynamic partition inserts (Hive terminology),
/// as well as inserting into unpartitioned tables,
/// and optional overwriting of partitions/tables.
//
/// Files and partitions:
/// This sink writes one or more Hdfs files per output partition,
/// corresponding to an Hdfs directory.
/// The Hdfs file names depend on unique_id, and therefore, we rely on the *global*
/// uniqueness of unique_id, ie, no two HdfsTableSinks must be constructed with
/// the same unique_id.
/// For each row, its target partition is computed based on the
/// partition_key_exprs from tsink.
/// A map of opened Hdfs files (corresponding to partitions) is maintained.
/// Each row may belong to different partition than the one before it.
//
/// Failure behavior:
/// In Exec() all data is written to Hdfs files in a temporary directory.
/// In Close() all temporary Hdfs files are moved to their final locations,
/// while also removing the original files if overwrite was specified, as follows:
/// 1. We move all temporary files to their final destinations.
/// 2. After all tmp files have been moved,
///    we delete the original files if overwrite was specified.
/// There is a possibility of data inconsistency,
/// e.g., if a failure occurs while moving the Hdfs files.
/// The temporary directory is <table base dir>/<unique_id.hi>-<unique_id.lo>_data
/// such that an external tool can easily clean up incomplete inserts.
/// This is consistent with Hive's behavior.
//
/// ACID tables:
/// In case of ACID tables the sink writes the files into their final destination which
/// is an ACID base or delta directory. No additional moves are required at the end, only
/// a commit for the ACID transaction.
/// The name of the output directory will be
/// <table base dir>/<partition dirs>/<ACID base or delta directory>
class HdfsTableSink : public TableSinkBase {
 public:
  HdfsTableSink(TDataSinkId sink_id, const HdfsTableSinkConfig& sink_config,
    const THdfsTableSink& hdfs_sink, RuntimeState* state);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Opens output_exprs and partition_key_exprs, prepares the single output partition for
  /// static inserts, and populates partition_descriptor_map_.
  Status Open(RuntimeState* state) override;

  /// Append all rows in batch to the temporary Hdfs files corresponding to partitions.
  Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Finalize any open files.
  /// TODO: IMPALA-2988: Move calls to functions that can fail in Close() to FlushFinal()
  Status FlushFinal(RuntimeState* state) override;

  /// Closes writers, output_exprs and partition_key_exprs and releases resources.
  /// The temporary files will be moved to their final destination by the Coordinator.
  void Close(RuntimeState* state) override;

  const vector<int32_t>& sort_columns() const override { return sort_columns_; }
  TSortingOrder::type sorting_order() const override { return sorting_order_; }
  const HdfsTableDescriptor& TableDesc() { return *table_desc_; }

  const std::map<string, int64_t>& GetParquetBloomFilterColumns() const override {
    return parquet_bloom_filter_columns_;
  }

  bool is_overwrite() const override { return overwrite_; }
  bool is_result_sink() const override { return is_result_sink_; }
  std::string staging_dir() const override { return staging_dir_; }
  int skip_header_line_count() const override { return skip_header_line_count_; }
  int64_t write_id() const override { return write_id_; }

  std::string DebugString() const override;

 private:
  /// Build a map from partition key values to partition descriptor for multiple output
  /// format support. The map is keyed on the concatenation of the non-constant keys of
  /// the PARTITION clause of the INSERT statement.
  void BuildPartitionDescMap();

  /// Fille 'output_partition' using 'partition_key_expr_evals_'.
  /// 'output_partition->partition_name' will contain the full partition name in URL
  /// encoded form. E.g.:
  /// It's "a=12%2F31%2F11/b=10" if we have 2 partition columns "a" and "b", and "a" has
  /// the value of "12/31/11" and "b" has the value of 10. Since this is URL encoded,
  /// can be used for paths.
  /// 'output_partition->raw_partition_names' is a vector of partition key-values in a
  /// non-encoded format.
  /// Staying with the above example this would hold ["a=12/31/11", "b=10"].
  Status ConstructPartitionInfo(
      const TupleRow* row,
      OutputPartition* output_partition);

  /// Returns partition descriptor object for the given key.
  const HdfsPartitionDescriptor* GetPartitionDescriptor(const std::string& key);

  /// Given a hashed partition key, get the output partition structure from
  /// the 'partition_keys_to_output_partitions_'. 'no_more_rows' indicates that no more
  /// rows will be added to the partition.
  Status GetOutputPartition(RuntimeState* state, const TupleRow* row,
      const std::string& key, PartitionPair** partition_pair, bool no_more_rows)
      WARN_UNUSED_RESULT;

  /// Maps all rows in 'batch' to partitions and appends them to their temporary Hdfs
  /// files. The input must be ordered by the partition key expressions.
  Status WriteClusteredRowBatch(RuntimeState* state, RowBatch* batch) WARN_UNUSED_RESULT;

  /// Returns TRUE for Hive ACID tables.
  bool IsHiveAcid() const override { return write_id_ != -1; }

  /// The 'skip.header.line.count' property of the target Hdfs table. We will insert this
  /// many empty lines at the beginning of new text files, which will be skipped by the
  /// scanners while reading from the files.
  int skip_header_line_count_;

  /// Indicates whether the existing partitions should be overwritten.
  bool overwrite_;

  /// The allocated write ID for the target table. -1 means that the target table is
  /// a plain, non-ACID table.
  int64_t write_id_ = -1;

  /// Indicates whether the input is ordered by the partition keys, meaning partitions can
  /// be opened, written, and closed one by one.
  bool input_is_clustered_;

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is used in the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  const std::vector<int32_t>& sort_columns_;

  // Represents the sorting order used in SORT BY queries.
  const TSortingOrder::type sorting_order_;

  /// The directory in which to write intermediate results. Set to
  /// <hdfs_table_base_dir>/_impala_insert_staging/ during Prepare()
  std::string staging_dir_;

  /// How deep into the partition specification in which to start creating partition
  // directories. Used in conjunction with external_output_dir_ to inform the table
  // sink which directories are pre-created.
  int external_output_partition_depth_ = 0;

  /// Map from column names to Parquet Bloom filter bitset sizes. Columns for which
  /// Parquet Bloom filtering is not enabled are not listed.
  std::map<std::string, int64_t> parquet_bloom_filter_columns_;

  /// Hash table of generated output partitions.
  /// Maps from a string representation of the dynamic_partition_key_exprs_
  /// generated by GetHashTblKey() to its corresponding OutputPartition.
  /// If there are no partitions (and no partition keys) we store a single
  /// OutputPartition in the map to simplify the code.
  PartitionMap partition_keys_to_output_partitions_;

  /// Map from row key (i.e. concatenated non-constant partition keys) to
  /// partition descriptor. We don't own the HdfsPartitionDescriptors, they
  /// belong to the table descriptor.  The key is generated by GetHashTblKey()
  /// from the keys in a row.
  typedef boost::unordered_map<std::string, HdfsPartitionDescriptor*>
      PartitionDescriptorMap;
  PartitionDescriptorMap partition_descriptor_map_;
  /// Will the output of this sink be used for query results
  const bool is_result_sink_;
};

}
#endif
