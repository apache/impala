// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SCAN_NODE_H_
#define IMPALA_EXEC_SCAN_NODE_H_

#include <string>
#include "exec/exec-node.h"
#include "util/runtime-profile.h"

namespace impala {

class TScanRange;

// Abstract base class of all scan nodes; introduces SetScanRange().
// Includes ScanNode common counters
class ScanNode : public ExecNode {
 public:
  ScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs) {}

  // Set up counters
  virtual Status Prepare(RuntimeState* state);

  // Convert scan_range into node-specific scan restrictions.  This should be 
  // called after Prepare()
  virtual Status SetScanRange(const TScanRange& scan_range) = 0;

  virtual bool IsScanNode() const { return true; }

  RuntimeProfile::Counter* bytes_read_counter() const { return bytes_read_counter_; }
  RuntimeProfile::Counter* read_timer() const { return read_timer_; }
  RuntimeProfile::Counter* throughput_counter() const { return throughput_counter_; }
  RuntimeProfile::Counter* materialize_tuple_timer() const { 
    return materialize_tuple_timer_; 
  }

  // names of ScanNode common counters
  static const std::string BYTES_READ_COUNTER;
  static const std::string READ_TIMER;
  static const std::string THROUGHPUT_COUNTER;
  static const std::string MATERIALIZE_TUPLE_TIMER;

 private:
  RuntimeProfile::Counter* bytes_read_counter_; // # bytes read from the scanner
  RuntimeProfile::Counter* read_timer_; // total read time 
  RuntimeProfile::Counter* throughput_counter_;
      // derived counter for read throughput [bytes/sec]
  RuntimeProfile::Counter* materialize_tuple_timer_;  // time writing tuple slots
};

}

#endif
