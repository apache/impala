// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SCAN_NODE_H_
#define IMPALA_EXEC_SCAN_NODE_H_

#include "exec/exec-node.h"

namespace impala {

class TScanRange;

// Abstract base class of all scan nodes; introduces SetScanRange().
// Includes ScanNode common counters
class ScanNode : public ExecNode {
 public:
  ScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs) {}

  virtual Status Prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::Prepare(state));
    scanner_timer_ =
        ADD_COUNTER(runtime_profile(), "ScannerTime", TCounterType::CPU_TICKS);
    bytes_read_counter_ =
        ADD_COUNTER(runtime_profile(), "BytesRead", TCounterType::BYTES);
    tuple_write_timer_ =
        ADD_COUNTER(runtime_profile(), "TupleWriteTime", TCounterType::CPU_TICKS);
    return Status::OK;
  }

  // Convert scan_range into node-specific scan restrictions.
  virtual Status SetScanRange(const TScanRange& scan_range) = 0;

  virtual bool IsScanNode() const { return true; }

  RuntimeProfile::Counter* scanner_timer() const { return scanner_timer_; }
  RuntimeProfile::Counter* bytes_read_counter() const { return bytes_read_counter_; }
  RuntimeProfile::Counter* tuple_write_timer() const { return tuple_write_timer_; }

 private:
  RuntimeProfile::Counter* scanner_timer_;      // time spent in underlying scanners
  RuntimeProfile::Counter* bytes_read_counter_; // bytes read from the scanner
  RuntimeProfile::Counter* tuple_write_timer_;  // time writing tuple slots
};

}

#endif
