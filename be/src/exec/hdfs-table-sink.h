// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TABLE_SINK_H
#define IMPALA_EXEC_HDFS_TABLE_SINK_H

#include <hdfs.h>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

// needed for scoped_ptr to work on ObjectPool
#include "common/object-pool.h"
#include "runtime/descriptors.h"
#include "exec/data-sink.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class HdfsTableWriter;

// Records the temporary and final Hdfs file name,
// the opened temporary Hdfs file, and the number of appended rows
// of an output partition.
struct OutputPartition {
  // Full path to target Hdfs file.
  // Path: <hdfs_base_dir>/<partition_values>/<query_id_str>
  std::string hdfs_file_name;

  // Temporary file: queryId/hdfs_file_name
  // The file is moved to hdfs_file_name in Close().
  // If overwrite is true, then we move the directory instead of the file.
  // Path: <hdfs_base_dir>/<query_id>_dir/<partition_values>/<query_id_str>
  std::string tmp_hdfs_file_name;

  // Connection to hdfs.
  hdfsFS hdfs_connection;

  // Hdfs file at tmp_hdfs_file_name.
  // TODO: We may need multiple files per partition.
  hdfsFile tmp_hdfs_file;

  // Records number of rows appended to this partition.
  int64_t num_rows;

  // Table format specific writer functions.
  boost::scoped_ptr<HdfsTableWriter> writer;
};

// The sink consumes all row batches of its child execution tree,
// and writes the evaluated output_exprs into Hdfs files.
// This sink supports static and dynamic partition inserts (Hive terminology),
// as well as inserting into unpartitioned tables,
// and optional overwriting of partitions/tables.
//
// Files and partitions:
// This sink writes a single Hdfs file per output partition,
// corresponding to an Hdfs directory.
// The Hdfs file name is the queryId, and therefore,
// we rely on the uniqueness of queryIds.
// For each row, its target partition is computed based on the
// partition_key_exprs from tsink.
// A map of opened Hdfs files (corresponding to partitions) is maintained.
// Each row may belong to different partition than the one before it.
//
// Failure behavior:
// In Exec() all data is written to Hdfs files in a temporary directory.
// In Close() all temporary Hdfs files are moved to their final locations,
// while also removing the original files if overwrite was specified, as follows:
// 1. We move all temporary files to their final destinations.
// 2. After all tmp files have been moved,
//    we delete the original files if overwrite was specified.
// There is a possibility of data inconsistency,
// e.g., if a failure occurs while moving the Hdfs files.
// The temporary directory is <table base dir>/<query_id.hi>-<query_id.lo>_data
// such that an external tool can easily clean up incomplete inserts.
// This is consistent with Hive's behavior.
class HdfsTableSink : public DataSink {
 public:
  HdfsTableSink(const RowDescriptor& row_desc, const TUniqueId& query_id,
      const std::vector<TExpr>& select_list_texprs, const TDataSink& tsink);

  // Prepares output_exprs and partition_key_exprs. 
  // Also, connects to Hdfs, and prepares the single output partition for static inserts.
  virtual Status Init(RuntimeState* state);

  // Append all rows in batch to the temporary Hdfs files corresponding to partitions.
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  // Move temporary Hdfs files to final locations.
  // Remove original Hdfs files if overwrite was specified.
  virtual Status Close(RuntimeState* state);

  // Initialises the filenames of a given output partition, and opens the temporary file.
  Status InitOutputPartition(OutputPartition* output);

  std::string DebugString() const;

 private:
  // Generates string key for hash_tbl_ as a concatenation
  // of all dynamic_partition_key_exprs_.
  // The generated string is much shorter than the full Hdfs file name.
  void GetHashTblKey(std::string* key);

  // Initialise and prepare select and partition key expressions
  Status PrepareExprs(RuntimeState* state);

  // Sets hdfs_file_name and tmp_hdfs_file_name of given output partition.
  // The Hdfs directory is created from the target table's base Hdfs dir,
  // the partition_key_names_ and the evaluated partition_key_exprs_.
  // The Hdfs file name is the query_id_.
  void BuildHdfsFileNames(OutputPartition* output);

  // Move tmp Hdfs files from output partitions to their final destinations.
  // If overwrite is true, we move the tmp Hdfs directories,
  // otherwise we move the tmp Hdfs files.
  // Also deletes the tmp directory,
  // and optionally the original Hdfs files if overwrite_ is true.
  Status MoveTmpHdfsFiles();

  // Move the tmp Hdfs file of a single output partition to its final destination.
  Status MoveTmpHdfsFile(OutputPartition* output);

  // Deletes all files in the Hdfs directory of the given OutputPartition,
  // except output->hdfs_file_name which was just created.
  Status DeleteOriginalFiles(OutputPartition* output);

  // Updates runtime stats of HDFS files created and rows written, then closes the file
  // associated with the partition
  Status FinalizePartition(RuntimeState* state, OutputPartition* partition);

  // Descriptor of target table. Set in Init().
  const HdfsTableDescriptor* table_desc_;

  // Currently this is the default partition since we don't support multi-format sinks.
  const HdfsPartitionDescriptor* default_partition_;

  // Exprs that materialize output values
  std::vector<Expr*> output_exprs_;

  // Current row from the current RowBatch to output
  TupleRow* current_row_;

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // Key is the concatenation of the evaluated
  // dynamic_partition_key_exprs_ generated by GetHashTblKey().
  // Maps to an OutputPartition, which are owned by the object pool.
  typedef boost::unordered_map<std::string, OutputPartition* > HashTable;

  // Row descriptor of row batches passed in Send(). Set in c'tor.
  const RowDescriptor& row_desc_;

  // Table id resolved in Prepare() to set tuple_desc_;
  TableId table_id_;

  // Format of table for sink.
  THdfsFileFormat::type table_format_;

  // Thrift representation of select list exprs, saved in the constructor
  // to be used to initialise output_exprs_ in Init
  const std::vector<TExpr>& select_list_texprs_;

  // Thrift representation of partition keys, saved in the constructor
  // to be used to initialise partition_key_exprs_ in Init
  const std::vector<TExpr>& partition_key_texprs_;

  // Exprs of partition keys.
  std::vector<Expr*> partition_key_exprs_;

  // Indicates whether the existing partitions should be overwritten.
  bool overwrite_;

  // string representation of query_id_. Used for per-partition Hdfs file names,
  // and for tmp Hdfs directories. Set in Prepare();
  std::string query_id_str_;

  // Special value for NULL partition keys to be compatible with Hive.
  std::string null_partition_key_value_;

  // Hash table of generated output partitions.
  // Maps from a string representation of the dynamic_partition_key_exprs_
  // generated by GetHashTblKey() to its corresponding OutputPartition.
  // If there are no parititions (and no partition keys) we store a single
  // OutputPartition in the map to simplify the code.
  HashTable partition_keys_to_output_partitions_;

  // Subset of partition_key_exprs_ which are not constant. Set in Prepare().
  // Used for generating the string key of hash_tbl_.
  std::vector<Expr*> dynamic_partition_key_exprs_;
};
}
#endif
