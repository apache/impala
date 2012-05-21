// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>
#include <hdfs.h>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>

#include "exec/scan-node.h"
#include "exec/byte-stream.h"
#include "exec/hdfs-scanner.h"
#include "runtime/descriptors.h"
#include "runtime/string-buffer.h"
#include "util/sse-util.h"
#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class TScanRange;
class RowBatch;
class Status;
class TPlanNode;
class DescriptorTbl;
class ByteStream;

struct HdfsScanRange {
  int offset;
  int length;

  HdfsScanRange(int o, int l) {
    offset = o;
    length = l;
  }
};


// A ScanNode implementation that is used for all tables read directly
// from HDFS-serialised data. An HdfsScanNode iterates over a set of
// scan ranges, and constructs an appropriate HdfsScanner for each
// one.  The scan node also deals with opening and closing files in
// HDFS and passing them to the scanners it creates; this way files
// can be cached across scan ranges.
class HdfsScanNode : public ScanNode {
 public:
  HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  // ExecNode methods

  virtual Status Prepare(RuntimeState* state);

  virtual Status Open(RuntimeState* state);

  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  virtual Status Close(RuntimeState* state);

  // ScanNode methods

  virtual void SetScanRange(const TScanRange& scan_range);

  // Public so the scanner can call this.
  bool EvalConjunctsForScanner(TupleRow* row) { return EvalConjuncts(row); }

  // Made public so that scanner can call this to decide whether a
  // limit has been reached. TODO: revisit.
  int ReachedLimit() { return ExecNode::ReachedLimit(); }

  int limit() const { return limit_; }

  const static int SKIP_COLUMN = -1;

  const std::vector<std::pair<int, SlotDescriptor*> >& materialized_slots()
    const { return materialized_slots_; }
  const std::vector<int>& column_to_slot_index() const { return column_idx_to_slot_idx_; }

  int GetNumPartitionKeys() { return num_partition_keys_; }

  // Scanners check the scan-node every row to see if the limit clause
  // has been satisfied. Therefore they need to update the total
  // number of rows seen directly in the scan node.
  void IncrNumRowsReturned(int num_rows = 1) { num_rows_returned_ += num_rows; }

  RuntimeProfile::Counter* parse_time_counter() const { return parse_time_counter_; }


 private:
  // Tuple id resolved in Prepare() to set tuple_desc_;
  int tuple_id_;

  // Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_;

  // Mem pool for tuple buffer data. Used by scanners for allocation,
  // but owned here.
  boost::scoped_ptr<MemPool> tuple_pool_;

  // Map from filename to list of scan ranges, to that we can iterate
  // over all scan ranges for a particular file efficiently
  typedef std::map<std::string, std::vector<HdfsScanRange> > ScanRangeMap;

  ScanRangeMap per_file_scan_ranges_;

  // Points at the (file, [scan ranges]) pair currently being processed
  ScanRangeMap::iterator current_file_scan_ranges_;

  // The index of the current scan range in the current file's scan range list
  int current_range_idx_;

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // The scanner in use for the current file / scan-range
  // combination.
  boost::scoped_ptr<HdfsScanner> current_scanner_;

  // The source of byte data for consumption by the scanner for the
  // current file / scan-range combination.
  boost::scoped_ptr<ByteStream> current_byte_stream_;

  // Tuple containing only materialized partition keys
  Tuple* template_tuple_;

  // Pool for allocating partition key tuple and string buffers
  boost::scoped_ptr<MemPool> partition_key_pool_;

  // Vector of positions in output slot list, indexed by table column
  // number. The first num_partition_keys_ entries are the partition
  // keys, followed by columns that are actually serialised in the
  // table itself.
  // If a column is not to be written to the output tuples, its entry
  // in this vector is SKIP_COLUMN.
  std::vector<int> column_idx_to_slot_idx_;

  // Total number of partition slot descriptors, including non-materialized ones.
  int num_partition_keys_;

  // Vector containing pairs of all
  // a) index into all non-partition 'columns' (including non-materialized ones)
  //    Scanners that maintain a sparse array of columns can use this
  //    index to select the correct column for an output slot.
  // b) the materialized, non-partition-key slot descriptor for that column
  //
  // The vector is indexed by the number of the output tuple slot, such that
  // materialized_slots_[j] corresponds to output slot j + num_partition_keys_
  // TODO: this could stand some simplification; it's rather confusing.
  std::vector<std::pair<int, SlotDescriptor*> > materialized_slots_;

  // Used to determine what kind of scanner to create. Will be
  // replaced by per-partition metadata.
  TPlanNodeType::type plan_node_type_;

  // Regular expressions to evaluate over file paths to extract partition key values
  boost::regex partition_key_regex_;

  // Mapping from partition key index to slot index
  // for materializing the virtual partition keys (if any) into Tuples.
  // pair.first refers to the number of the partition key index.
  // pair.second refers to the target slot index in the output Tuple.
  // Used for initialising template_tuple_ with partition key values.
  std::vector<std::pair<int, int> > key_idx_to_slot_idx_;

  // Hdfs specific counter
  RuntimeProfile::Counter* parse_time_counter_;       // time parsing files

  // Called once per scan-range to initialise (potentially) a new byte
  // stream and to call the same method on the current scanner.
  Status InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished);

  Status ExtractPartitionKeyValues(
      RuntimeState* state,
      const std::vector<std::pair<int, int> >& key_idx_to_slot_idx);

  Status InitRegex(ObjectPool* pool, const TPlanNode& tnode);
};

}

#endif
