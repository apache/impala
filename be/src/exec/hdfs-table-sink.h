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
#include <boost/scoped_ptr.hpp>

/// needed for scoped_ptr to work on ObjectPool
#include "common/object-pool.h"
#include "exec/data-sink.h"
#include "exec/hdfs-table-writer.h"
#include "runtime/descriptors.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class MemTracker;

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
  /// This is a temporary file that will get moved to a  permanent location
  /// when we commit the insert.
  /// Path: <hdfs_base_dir>/<partition_values>/<unique_id_str>.<sequence number>
  std::string current_file_name;

  /// Name of the temporary directory that files for this partition are staged to before
  /// the coordinator moves them to their permanent location once the query completes.
  /// Not used if 'skip_staging' is true.
  /// Path: <base_table_dir/<staging_dir>/<unique_id>_dir/
  std::string tmp_hdfs_dir_name;

  /// Base prefix for temporary files, to save building it every time a temporary file is
  /// created.
  /// Path: tmp_hdfs_dir_name/partition_name/<unique_id_str>
  std::string tmp_hdfs_file_name_prefix;

  /// key1=val1/key2=val2/ etc. Used to identify partitions to the metastore.
  std::string partition_name;

  /// Connection to hdfs.
  hdfsFS hdfs_connection;

  /// Hdfs file at tmp_hdfs_file_name.
  hdfsFile tmp_hdfs_file;

  /// Records number of rows appended to the current file in this partition.
  int64_t num_rows;

  /// Number of files created in this partition.
  int32_t num_files;

  /// Table format specific writer functions.
  boost::scoped_ptr<HdfsTableWriter> writer;

  /// The descriptor for this partition.
  const HdfsPartitionDescriptor* partition_descriptor;

  /// The block size decided on for this file.
  int64_t block_size;

  OutputPartition();
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
class HdfsTableSink : public DataSink {
 public:
  HdfsTableSink(TDataSinkId sink_id, const RowDescriptor* row_desc,
      const TDataSink& tsink, RuntimeState* state);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Opens output_exprs and partition_key_exprs, prepares the single output partition for
  /// static inserts, and populates partition_descriptor_map_.
  virtual Status Open(RuntimeState* state);

  /// Append all rows in batch to the temporary Hdfs files corresponding to partitions.
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// Finalize any open files.
  /// TODO: IMPALA-2988: Move calls to functions that can fail in Close() to FlushFinal()
  virtual Status FlushFinal(RuntimeState* state);

  /// Move temporary Hdfs files to final locations.
  /// Remove original Hdfs files if overwrite was specified.
  /// Closes output_exprs and partition_key_exprs.
  virtual void Close(RuntimeState* state);

  int skip_header_line_count() const { return skip_header_line_count_; }
  const vector<int32_t>& sort_columns() const { return sort_columns_; }
  const HdfsTableDescriptor& TableDesc() { return *table_desc_; }

  RuntimeProfile::Counter* rows_inserted_counter() { return rows_inserted_counter_; }
  RuntimeProfile::Counter* bytes_written_counter() { return bytes_written_counter_; }
  RuntimeProfile::Counter* encode_timer() { return encode_timer_; }
  RuntimeProfile::Counter* hdfs_write_timer() { return hdfs_write_timer_; }
  RuntimeProfile::Counter* compress_timer() { return compress_timer_; }

  std::string DebugString() const;

 protected:
  virtual Status Init(const std::vector<TExpr>& thrift_output_exprs,
      const TDataSink& tsink, RuntimeState* state) WARN_UNUSED_RESULT;

 private:
  /// Initialises the filenames of a given output partition, and opens the temporary file.
  /// The partition key is derived from 'row'. If the partition will not have any rows
  /// added to it, empty_partition must be true.
  Status InitOutputPartition(RuntimeState* state,
      const HdfsPartitionDescriptor& partition_descriptor, const TupleRow* row,
      OutputPartition* output_partition, bool empty_partition) WARN_UNUSED_RESULT;

  /// Add a temporary file to an output partition.  Files are created in a
  /// temporary directory and then moved to the real partition directory by the
  /// coordinator in a finalization step. The temporary file's current location
  /// and final destination are recorded in the state parameter.
  /// If this function fails, the tmp file is cleaned up.
  Status CreateNewTmpFile(RuntimeState* state, OutputPartition* output_partition)
      WARN_UNUSED_RESULT;

  /// Key is the concatenation of the evaluated dynamic_partition_key_exprs_ generated by
  /// GetHashTblKey(). Maps to an OutputPartition and a vector of indices of the rows in
  /// the current batch to insert into the partition. The PartitionPair owns the
  /// OutputPartition via a unique_ptr so that the memory is freed as soon as the
  /// PartitionPair is removed from the map. This is important, because the
  /// PartitionPairs can have different lifetimes. For example, a clustered insert into a
  /// partitioned table iterates over the partitions, so only one PartitionPairs is
  /// in the map at any given time.
  typedef std::pair<std::unique_ptr<OutputPartition>, std::vector<int32_t>> PartitionPair;
  typedef boost::unordered_map<std::string, PartitionPair> PartitionMap;

  /// Generates string key for hash_tbl_ as a concatenation of all evaluated exprs,
  /// evaluated against 'row'. The generated string is much shorter than the full Hdfs
  /// file name.
  void GetHashTblKey(const TupleRow* row,
      const std::vector<ScalarExprEvaluator*>& evals, std::string* key);

  /// Given a hashed partition key, get the output partition structure from
  /// the 'partition_keys_to_output_partitions_'. 'no_more_rows' indicates that no more
  /// rows will be added to the partition.
  Status GetOutputPartition(RuntimeState* state, const TupleRow* row,
      const std::string& key, PartitionPair** partition_pair, bool no_more_rows)
      WARN_UNUSED_RESULT;

  /// Sets hdfs_file_name and tmp_hdfs_file_name of given output partition.
  /// The Hdfs directory is created from the target table's base Hdfs dir,
  /// the partition_key_names_ and the evaluated partition_key_exprs_.
  /// The Hdfs file name is the unique_id_str_.
  void BuildHdfsFileNames(const HdfsPartitionDescriptor& partition_descriptor,
      OutputPartition* output);

  /// Writes all rows referenced by the row index vector in 'partition_pair' to the
  /// partition's writer and clears the row index vector afterwards.
  Status WriteRowsToPartition(
      RuntimeState* state, RowBatch* batch, PartitionPair* partition_pair)
      WARN_UNUSED_RESULT;

  /// Maps all rows in 'batch' to partitions and appends them to their temporary Hdfs
  /// files. The input must be ordered by the partition key expressions.
  Status WriteClusteredRowBatch(RuntimeState* state, RowBatch* batch) WARN_UNUSED_RESULT;

  /// Updates runtime stats of HDFS with rows written, then closes the file associated
  /// with the partition by calling ClosePartitionFile()
  Status FinalizePartitionFile(RuntimeState* state, OutputPartition* partition)
      WARN_UNUSED_RESULT;

  /// Closes the hdfs file for this partition as well as the writer.
  Status ClosePartitionFile(RuntimeState* state, OutputPartition* partition)
      WARN_UNUSED_RESULT;

  // Returns TRUE if the staging step should be skipped for this partition. This allows
  // for faster INSERT query completion time for the S3A filesystem as the coordinator
  // does not have to copy the file(s) from the staging locaiton to the final location. We
  // do not skip for INSERT OVERWRITEs because the coordinator will delete all files in
  // the final location before moving the staged files there, so we cannot write directly
  // to the final location and need to write to the temporary staging location.
  bool ShouldSkipStaging(RuntimeState* state, OutputPartition* partition);

  /// Descriptor of target table. Set in Prepare().
  const HdfsTableDescriptor* table_desc_;

  /// The partition descriptor used when creating new partitions from this sink.
  /// Currently we don't support multi-format sinks.
  const HdfsPartitionDescriptor* prototype_partition_;

  /// Table id resolved in Prepare() to set tuple_desc_;
  TableId table_id_;

  /// The 'skip.header.line.count' property of the target Hdfs table. We will insert this
  /// many empty lines at the beginning of new text files, which will be skipped by the
  /// scanners while reading from the files.
  int skip_header_line_count_;

  /// Indicates whether the existing partitions should be overwritten.
  bool overwrite_;

  /// Indicates whether the input is ordered by the partition keys, meaning partitions can
  /// be opened, written, and closed one by one.
  bool input_is_clustered_;

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is used in the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  const std::vector<int32_t>& sort_columns_;

  /// Stores the current partition during clustered inserts across subsequent row batches.
  /// Only set if 'input_is_clustered_' is true.
  PartitionPair* current_clustered_partition_;

  /// Stores the current partition key during clustered inserts across subsequent row
  /// batches. Only set if 'input_is_clustered_' is true.
  std::string current_clustered_partition_key_;

  /// The directory in which to write intermediate results. Set to
  /// <hdfs_table_base_dir>/_impala_insert_staging/ during Prepare()
  std::string staging_dir_;

  /// string representation of the unique fragment instance id. Used for per-partition
  /// Hdfs file names, and for tmp Hdfs directories. Set in Prepare();
  std::string unique_id_str_;

  /// Hash table of generated output partitions.
  /// Maps from a string representation of the dynamic_partition_key_exprs_
  /// generated by GetHashTblKey() to its corresponding OutputPartition.
  /// If there are no partitions (and no partition keys) we store a single
  /// OutputPartition in the map to simplify the code.
  PartitionMap partition_keys_to_output_partitions_;

  /// Expressions for computing the target partitions to which a row is written.
  std::vector<ScalarExpr*> partition_key_exprs_;
  std::vector<ScalarExprEvaluator*> partition_key_expr_evals_;

  /// Subset of partition_key_expr_evals_ which are not constant. Set in Prepare().
  /// Used for generating the string key of hash_tbl_.
  std::vector<ScalarExprEvaluator*> dynamic_partition_key_expr_evals_;

  /// Map from row key (i.e. concatenated non-constant partition keys) to
  /// partition descriptor. We don't own the HdfsPartitionDescriptors, they
  /// belong to the table descriptor.  The key is generated by GetHashTblKey()
  /// from the keys in a row.
  typedef boost::unordered_map<std::string, HdfsPartitionDescriptor*>
      PartitionDescriptorMap;
  PartitionDescriptorMap partition_descriptor_map_;

  RuntimeProfile::Counter* partitions_created_counter_;
  RuntimeProfile::Counter* files_created_counter_;
  RuntimeProfile::Counter* rows_inserted_counter_;
  RuntimeProfile::Counter* bytes_written_counter_;

  /// Time spent converting tuple to on disk format.
  RuntimeProfile::Counter* encode_timer_;
  /// Time spent writing to hdfs
  RuntimeProfile::Counter* hdfs_write_timer_;
  /// Time spent compressing data
  RuntimeProfile::Counter* compress_timer_;
};

}
#endif
