// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_HDFS_RCFILE_SCAN_NODE_H_
#define IMPALA_HDFS_RCFILE_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>
#include <hdfs.h>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>
#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "exec/rcfile-reader.h"

namespace impala {

class DescriptorTbl;
class TPlanNode;
class RowBatch;
class Status;
class TupleDescriptor;
class MemPool;
class Tuple;
class SlotDescriptor;
class stringstream;
class Expr;
class TextConverter;

// This execution node parses RCFiles located in HDFS, and writes their
// content as tuples in the Impala in-memory representation of data, e.g.
// (tuples, rows, row batches).
// Each RCFile is parsed using the RCFileReader, which writes row data into
// RCFileRowGroup objects. Fixed-size fields are copied from the RowGroups
// into a fixed-size tuple buffer that is allocated in Prepare().
// Variable length fields (e.g. strings) remain in the RowGroup objects,
// and pointers in the tuples are initialized to point to these fields.
// TODO: Overlap between HdfsRCFileScanNode and HdfsTextScanNode needs to be captured
//       in a superclass.
class HdfsRCFileScanNode : public ScanNode {
 public:
  HdfsRCFileScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  // Allocates tuple buffer.
  // Sets tuple_desc_ using RuntimeState.
  // Creates mapping from field index in table to slot index in output tuple.
  virtual Status Prepare(RuntimeState* state);

  // Connects to HDFS and initializes RCFileReader
  virtual Status Open(RuntimeState* state);

  // Writes parsed tuples into tuple buffer,
  // and sets pointers in row_batch to point to them.
  // row_batch will be non-full when all blocks of all files have been read and parsed.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch);

  // Cleanup the RCFileReader and disconnect from HDFS.
  virtual Status Close(RuntimeState* state);

  virtual void SetScanRange(const TScanRange& scan_range);
  
 protected:
  // Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  const static int SKIP_COLUMN = -1;

  // List of HDFS paths to read.
  std::vector<std::string> files_;

  // Index of current file being processed.
  int file_idx_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;

  // Regular expressions to evaluate over file paths to extract partition key values
  boost::regex partition_key_regex_;

  // Partition key values.  This is computed once for each file and valid for the
  // duration of that file.  The vector contains only literal exprs.
  std::vector<Expr*> partition_key_values_;

  // Descriptor of tuples in input files.
  const TupleDescriptor* tuple_desc_;

  // Memory pools created in c'tor and destroyed in d'tor. These are auto_ptrs, as opposed
  // to scoped_ptrs, because we need to be able to pass them up via the output RowBatch
  // of GetNext().

  // Contains all memory for tuple data.
  std::auto_ptr<MemPool> tuple_buf_pool_;

  // Pool for allocating objects.
  // Currently only used for creating LiteralExprs from TExpr for partition keys in Prepare().
  // TODO: Add an objectpool to runtimestate so that we don't have it in each exec node.
  boost::scoped_ptr<ObjectPool> obj_pool_;
  
  // Parser internal state:

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // Size of tuple buffer determined by size of tuples and capacity of row batches.
  int tuple_buf_size_;

  // Buffer where tuples are written into.
  // Must be valid until next GetNext().
  char* tuple_buf_;

  // Current tuple.
  Tuple* tuple_;

  // Mapping from column index in table to slot index in output tuple.
  // Created in Prepare() from SlotDescriptors.
  std::vector<int> column_idx_to_slot_idx_;

  // Maps column idx to a boolean indicating whether or not the column
  // needs to be read. Created in Prepare() and used to initialize
  // RCFileReader.
  std::vector<bool> column_idx_read_mask_;

  // Number of non-partition key columns for the files_. Set in Prepare().
  int num_cols_;
  
  // Number of partition keys for the files_. Set in Prepare().
  int num_partition_keys_;

  // Mapping from partition key index to slot index
  // for materializing the virtual partition keys (if any) into Tuples.
  // pair.first refers to the partition key index.
  // pair.second refers to the target slot index.
  // Created in Prepare() from the TableDescriptor and SlotDescriptors.
  std::vector<std::pair<int, int> > key_idx_to_slot_idx_;

  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  // RCFileReader used for reading the list of RCFiles
  boost::scoped_ptr<RCFileReader> rc_reader_;

  // The current row group
  boost::scoped_ptr<RCFileRowGroup> row_group_;

  // Initializes the scan node.  
  //  - initialize partition key regex from fe input
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  // Updates 'partition_key_values_' by extracting the values from the current file path
  // using 'partition_key_regex_'
  Status ExtractPartitionKeyValues(RuntimeState* state);
};

}

#endif // IMPALA_HDFS_RCFILE_SCAN_NODE_H_
