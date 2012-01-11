// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SCAN_NODE_H_
#define IMPALA_EXEC_SCAN_NODE_H_

#include "exec/exec-node.h"

namespace impala {

class TScanRange;

// Abstract base class of all scan nodes; introduces SetScanRange().
class ScanNode : public ExecNode {
 public:
  ScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs) {}

  // Convert scan_range into node-specific scan restrictions.
  virtual void SetScanRange(const TScanRange& scan_range) = 0;

  virtual bool IsScanNode() const { return true; }
};

}

#endif
