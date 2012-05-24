// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>
#include <hdfs.h>

#include "exec/scan-node.h"
#include "exec/byte-stream.h"
#include "exec/hdfs-scanner.h"
#include "runtime/descriptors.h"
#include "runtime/string-buffer.h"
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

  int limit() const { return limit_; }

  bool compact_data() const { return compact_data_; }
  
  const static int SKIP_COLUMN = -1;

  const std::vector<SlotDescriptor*>& materialized_slots()
      const { return materialized_slots_; }

  int num_partition_keys() const { return num_partition_keys_; }

  int num_cols() const { return column_idx_to_materialized_slot_idx_.size(); }
  
  const TupleDescriptor* tuple_desc() { return tuple_desc_; }

  // Scanners check the scan-node every row to see if the limit clause
  // has been satisfied. Therefore they need to update the total
  // number of rows seen directly in the scan node.
  void IncrNumRowsReturned(int num_rows = 1) { num_rows_returned_ += num_rows; }

  RuntimeProfile::Counter* parse_time_counter() const { return parse_time_counter_; }
  RuntimeProfile::Counter* memory_used_counter() const { return memory_used_counter_; }

  // Returns index into materialized_slots with 'col_idx'.  Returns SKIP_COLUMN if
  // that column is not materialized.
  int GetMaterializedSlotIdx(int col_idx) const {
    return column_idx_to_materialized_slot_idx_[col_idx];
  }

 private:
  // Tuple id resolved in Prepare() to set tuple_desc_;
  int tuple_id_;

  // Copy strings to tuple memory pool if true.
  // We try to avoid the overhead copying strings if the data will just
  // stream to another node that will release the memory.
  bool compact_data_;

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

  // Map of HdfsScanner objects to file types.  Only one scanner object will be
  // created for each file type.  Objects stored in scanner_pool_.
  typedef std::map<TPlanNodeType::type, HdfsScanner*> ScannerMap;
  ScannerMap scanner_map_;

  // Pool for storing allocated scanner objects.  We don't want to use the 
  // runtime pool to ensure that the scanner objects are deleted before this
  // object is.
  boost::scoped_ptr<ObjectPool> scanner_pool_;

  // The scanner in use for the current file / scan-range
  // combination.
  HdfsScanner* current_scanner_;

  // The source of byte data for consumption by the scanner for the
  // current file / scan-range combination.
  boost::scoped_ptr<ByteStream> current_byte_stream_;

  // Tuple containing only materialized partition keys.  In the case where there
  // are only partition key slots, this tuple ptr is directly stored in tuple rows.
  // In the case where there are other slots, this template is copied into a new tuple
  // before the other slots are written.  In this case, the address of template_tuple_
  // can not change as the value is baked into the codegen'd functions.
  Tuple* template_tuple_;

  // Pool for allocating partition key tuple and string buffers
  boost::scoped_ptr<MemPool> partition_key_pool_;

  // Total number of partition slot descriptors, including non-materialized ones.
  int num_partition_keys_;

  // Vector containing indices into materialized_slots_.  The vector is indexed by
  // the slot_desc's col_pos.  Non-materialized slots and partition key slots will 
  // have SKIP_COLUMN as its entry.
  std::vector<int> column_idx_to_materialized_slot_idx_;
  
  // Vector containing slot descriptors for all materialized non-partition key
  // slots.  These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> materialized_slots_;

  // Vector containing slot descriptors for all materialized partition key slots  
  // These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> partition_key_slots_;

  // Used to determine what kind of scanner to create. Will be
  // replaced by per-partition metadata.
  TPlanNodeType::type plan_node_type_;

  // Regular expressions to evaluate over file paths to extract partition key values
  boost::regex partition_key_regex_;

  // Hdfs specific counter
  RuntimeProfile::Counter* parse_time_counter_;       // time parsing files

  // Account for peak memory used in the Hdfs scanner.
  RuntimeProfile::Counter* memory_used_counter_;

  // Called once per scan-range to initialise (potentially) a new byte
  // stream and to call the same method on the current scanner.
  Status InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished);

  Status ExtractPartitionKeyValues(RuntimeState* state);

  Status InitRegex(ObjectPool* pool, const TPlanNode& tnode);
};

}

#endif
