// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SCAN_NODE_H_
#define IMPALA_EXEC_SCAN_NODE_H_

#include <string>
#include "exec/exec-node.h"
#include "util/runtime-profile.h"
#include "gen-cpp/ImpalaInternalService_types.h"

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

  // Convert scan_ranges into node-specific scan restrictions.  This should be 
  // called after Prepare()
  virtual Status SetScanRanges(const std::vector<TScanRangeParams>& scan_ranges) = 0;

  virtual bool IsScanNode() const { return true; }

  RuntimeProfile::Counter* bytes_read_counter() const { return bytes_read_counter_; }
  RuntimeProfile::Counter* read_timer() const { return read_timer_; }
  RuntimeProfile::Counter* total_throughput_counter() const { 
    return total_throughput_counter_; 
  }
  RuntimeProfile::Counter* per_thread_throughput_counter() const {
    return per_thread_throughput_counter_;
  }
  RuntimeProfile::Counter* materialize_tuple_timer() const { 
    return materialize_tuple_timer_; 
  }
  RuntimeProfile::Counter* scan_ranges_complete_counter() const {
    return scan_ranges_complete_counter_;
  }

  // names of ScanNode common counters
  static const std::string BYTES_READ_COUNTER;
  static const std::string READ_TIMER;
  static const std::string TOTAL_THROUGHPUT_COUNTER;
  static const std::string PER_THREAD_THROUGHPUT_COUNTER;
  static const std::string MATERIALIZE_TUPLE_TIMER;
  static const std::string SCAN_RANGES_COMPLETE_COUNTER;

 private:
  RuntimeProfile::Counter* bytes_read_counter_; // # bytes read from the scanner
  RuntimeProfile::Counter* read_timer_; // total read time 
  // Wall based aggregate read throughput [bytes/sec]
  RuntimeProfile::Counter* total_throughput_counter_;
  // Per thread read throughput [bytes/sec]
  RuntimeProfile::Counter* per_thread_throughput_counter_;
  RuntimeProfile::Counter* materialize_tuple_timer_;  // time writing tuple slots
  RuntimeProfile::Counter* scan_ranges_complete_counter_;
};

}

#endif
