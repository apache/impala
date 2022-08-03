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

#ifndef IMPALA_EXEC_KUDU_SCAN_NODE_MT_H_
#define IMPALA_EXEC_KUDU_SCAN_NODE_MT_H_

#include "exec/kudu/kudu-scan-node-base.h"

namespace impala {

class KuduScanner;

/// A scan node that scans a Kudu table.
///
/// This takes a set of serialized Kudu scan tokens which encode the information needed
/// for this scan. This materializes the tuples, evaluates conjuncts and runtimes filters
/// in the thread calling GetNext().
class KuduScanNodeMt : public KuduScanNodeBase {
 public:
  KuduScanNodeMt(ObjectPool* pool, const ScanPlanNode& pnode, const DescriptorTbl& descs);

  ~KuduScanNodeMt();

  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual void Close(RuntimeState* state) override;
  virtual ExecutionModel getExecutionModel() const override { return TASK_BASED; }

 private:
  /// Current scan token and corresponding scanner.
  /// TODO: The KuduScanner should be merged into this class when we get rid of the
  /// non-MT version of this class. It is not necessary for the scan node to
  /// be separate from the scanner in the single threaded model. This would remove a
  /// layer of indirection and clean up the code.
  const std::string* scan_token_;
  std::unique_ptr<KuduScanner> scanner_;
};

}

#endif
