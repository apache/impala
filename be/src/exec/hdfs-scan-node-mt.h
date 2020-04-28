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


#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_MT_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_MT_H_

#include <boost/scoped_ptr.hpp>

#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node-base.h"

namespace impala {

class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class RowBatch;
class ScannerContext;
class TPlanNode;

/// Scan node that materializes tuples, evaluates conjuncts and runtime filters
/// in the thread calling GetNext(). Uses the HdfsScanner::GetNext() interface.
class HdfsScanNodeMt : public HdfsScanNodeBase {
 public:
  HdfsScanNodeMt(
      ObjectPool* pool, const HdfsScanPlanNode& pnode, const DescriptorTbl& descs);
  ~HdfsScanNodeMt();

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos)
      override WARN_UNUSED_RESULT;
  virtual void Close(RuntimeState* state) override;

  virtual bool HasRowBatchQueue() const override { return false; }
  virtual ExecutionModel getExecutionModel() const override { return TASK_BASED; }

  /// Adds the range to a queue shared among all instances of this scan node.
  Status AddDiskIoRanges(const std::vector<io::ScanRange*>& ranges,
        EnqueueLocation enqueue_location = EnqueueLocation::TAIL) override;

 protected:
  /// Fetches the next range to read from a queue shared among all instances of this scan
  /// node. Also schedules it to be read by disk threads via the reader context.
  Status GetNextScanRangeToRead(io::ScanRange** scan_range, bool* needs_buffers) override;

 private:
  /// Create and open new scanner for this partition type.
  /// If the scanner is successfully created and opened, it is returned in 'scanner'.
  Status CreateAndOpenScanner(HdfsPartitionDescriptor* partition,
      ScannerContext* context, boost::scoped_ptr<HdfsScanner>* scanner);

  /// Current scan range and corresponding scanner.
  io::ScanRange* scan_range_;
  boost::scoped_ptr<ScannerContext> scanner_ctx_;
  boost::scoped_ptr<HdfsScanner> scanner_;
};

}

#endif
