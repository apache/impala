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


#ifndef IMPALA_EXEC_HDFS_TABLE_SINK_H
#define IMPALA_EXEC_HDFS_TABLE_SINK_H

#include <hdfs.h>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

/// needed for scoped_ptr to work on ObjectPool
#include "common/object-pool.h"
#include "exec/data-sink.h"
#include "runtime/descriptors.h"
#include "util/runtime-profile.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class HdfsTableWriter;
class MemTracker;

/// Records the temporary and final Hdfs file name, the opened temporary Hdfs file, and the
/// number of appended rows of an output partition.
struct OutputPartition {
  /// In the below, <unique_id_str> is the unique ID passed to HdfsTableSink in string
  /// form. It is typically the fragment ID that owns the sink.

  /// Full path to root of the group of files that will be created for this partition.
  /// Each file will have a sequence number appended.  A table writer may produce multiple
  /// files per partition. The root is either partition_descriptor->location (if non-empty,
  /// i.e. the partition has a custom location) or table_dir/partition_name/
  /// Path: <root>/<unique_id_str>
  std::string final_hdfs_file_name_prefix;

  /// File name for current output, with sequence number appended.
  /// This is a temporary file that will get moved to a  permanent location
  /// when we commit the insert.
  /// Path: <hdfs_base_dir>/<partition_values>/<unique_id_str>.<sequence number>
  std::string current_file_name;

  /// Name of the temporary directory that files for this partition are staged to before
  /// the coordinator moves them to their permanent location once the query completes.
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

  OutputPartition();
};

/// The sink consumes all row batches of its child execution tree, and writes the evaluated
/// output_exprs into temporary Hdfs files. The query coordinator moves the temporary files
/// into their final locations after the sinks have finished executing.
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
  HdfsTableSink(const RowDescriptor& row_desc,
      const std::vector<TExpr>& select_list_texprs, const TDataSink& tsink);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  virtual Status Prepare(RuntimeState* state);

  /// Opens output_exprs and partition_key_exprs, prepares the single output partition for
  /// static inserts, and populates partition_descriptor_map_.
  virtual Status Open(RuntimeState* state);

  /// Append all rows in batch to the temporary Hdfs files corresponding to partitions.
  virtual Status Send(RuntimeState* state, RowBatch* batch, bool eos);

  /// Currently a no-op function.
  /// TODO: Move calls to functions that can fail in Close() to FlushFinal()
  virtual Status FlushFinal(RuntimeState* state);

  /// Move temporary Hdfs files to final locations.
  /// Remove original Hdfs files if overwrite was specified.
  /// Closes output_exprs and partition_key_exprs.
  virtual void Close(RuntimeState* state);

  /// Get the block size of the current file opened for this partition.
  /// This is a utility routine that can be called by specific table
  /// writers.  Currently used by the parquet writer.
  static Status GetFileBlockSize(OutputPartition* output_partition, int64_t* size);

  virtual RuntimeProfile* profile() { return runtime_profile_; }
  const HdfsTableDescriptor& TableDesc() { return *table_desc_; }
  MemTracker* mem_tracker() { return mem_tracker_.get(); }

  RuntimeProfile::Counter* rows_inserted_counter() { return rows_inserted_counter_; }
  RuntimeProfile::Counter* bytes_written_counter() { return bytes_written_counter_; }
  RuntimeProfile::Counter* encode_timer() { return encode_timer_; }
  RuntimeProfile::Counter* hdfs_write_timer() { return hdfs_write_timer_; }
  RuntimeProfile::Counter* compress_timer() { return compress_timer_; }

  std::string DebugString() const;

 private:
  /// Initialises the filenames of a given output partition, and opens the temporary file.
  /// If the partition will not have any rows added to it, empty_partition must be true.
  Status InitOutputPartition(RuntimeState* state,
                             const HdfsPartitionDescriptor& partition_descriptor,
                             OutputPartition* output_partition, bool empty_partition);

  /// Add a temporary file to an output partition.  Files are created in a
  /// temporary directory and then moved to the real partition directory by the
  /// coordinator in a finalization step. The temporary file's current location
  /// and final destination are recorded in the state parameter.
  /// If this function fails, the tmp file is cleaned up.
  Status CreateNewTmpFile(RuntimeState* state, OutputPartition* output_partition);

  /// Key is the concatenation of the evaluated
  /// dynamic_partition_key_exprs_ generated by GetHashTblKey().
  /// Maps to an OutputPartition, which are owned by the object pool and
  /// a vector of rows to insert into this partition from the current row batch.
  typedef std::pair<OutputPartition*, std::vector<int32_t> > PartitionPair;
  typedef boost::unordered_map<std::string, PartitionPair> PartitionMap;


  /// Generates string key for hash_tbl_ as a concatenation
  /// of all evaluated exprs, evaluated against current_row_.
  /// The generated string is much shorter than the full Hdfs file name.
  void GetHashTblKey(const std::vector<ExprContext*>& ctxs, std::string* key);

  /// Given a hashed partition key, get the output partition structure from
  /// the partition_keys_to_output_partitions_.
  /// no_more_rows indicates that no more rows will be added to the partition.
  Status GetOutputPartition(RuntimeState* state, const std::string& key,
                  PartitionPair** partition_pair, bool no_more_rows);

  /// Initialise and prepare select and partition key expressions
  Status PrepareExprs(RuntimeState* state);

  /// Sets hdfs_file_name and tmp_hdfs_file_name of given output partition.
  /// The Hdfs directory is created from the target table's base Hdfs dir,
  /// the partition_key_names_ and the evaluated partition_key_exprs_.
  /// The Hdfs file name is the unique_id_str_.
  void BuildHdfsFileNames(const HdfsPartitionDescriptor& partition_descriptor,
      OutputPartition* output);

  /// Updates runtime stats of HDFS with rows written, then closes the file associated with
  /// the partition by calling ClosePartitionFile()
  Status FinalizePartitionFile(RuntimeState* state, OutputPartition* partition);

  /// Closes the hdfs file for this partition as well as the writer.
  void ClosePartitionFile(RuntimeState* state, OutputPartition* partition);

  /// Descriptor of target table. Set in Prepare().
  const HdfsTableDescriptor* table_desc_;

  /// Currently this is the default partition since we don't support multi-format sinks.
  const HdfsPartitionDescriptor* default_partition_;

  /// Exprs that materialize output values
  std::vector<ExprContext*> output_expr_ctxs_;

  /// Current row from the current RowBatch to output
  TupleRow* current_row_;

  /// Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  /// Row descriptor of row batches passed in Send(). Set in c'tor.
  const RowDescriptor& row_desc_;

  /// Table id resolved in Prepare() to set tuple_desc_;
  TableId table_id_;

  /// Thrift representation of select list exprs, saved in the constructor
  /// to be used to initialise output_exprs_ in Init
  const std::vector<TExpr>& select_list_texprs_;

  /// Thrift representation of partition keys, saved in the constructor
  /// to be used to initialise partition_key_exprs_ in Init
  const std::vector<TExpr>& partition_key_texprs_;

  /// Exprs of partition keys.
  std::vector<ExprContext*> partition_key_expr_ctxs_;

  /// Indicates whether the existing partitions should be overwritten.
  bool overwrite_;

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

  /// Subset of partition_key_expr_ctxs_ which are not constant. Set in Prepare().
  /// Used for generating the string key of hash_tbl_.
  std::vector<ExprContext*> dynamic_partition_key_expr_ctxs_;

  /// Map from row key (i.e. concatenated non-constant partition keys) to
  /// partition descriptor. We don't own the HdfsPartitionDescriptors, they
  /// belong to the table descriptor.  The key is generated by GetHashTblKey()
  /// from the keys in a row.
  typedef boost::unordered_map<std::string, HdfsPartitionDescriptor*>
      PartitionDescriptorMap;
  PartitionDescriptorMap partition_descriptor_map_;

  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// Allocated from runtime state's pool.
  RuntimeProfile* runtime_profile_;
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
