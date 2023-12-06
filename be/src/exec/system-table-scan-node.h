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

#pragma once

#include "exec/scan-node.h"
#include "exec/system-table-scanner.h"

namespace impala {

class SystemTableScanNode : public ScanNode {
  /// A scan node that exposes Impala system state as a table.
  ///
  /// Different SystemTableScanner subclasses gather data from different Impala subsystems
  /// and materialize it into Tuples.
  /// e.g. QueryScanner materializes active queries into tuples.
 public:
  using ScanNode::ScanNode;

  /// Create schema and columns to slots mapping.
  Status Prepare(RuntimeState* state) override;

  /// Start scan.
  Status Open(RuntimeState* state) override;

  /// Fill the next row batch by fetching more data from the scanner.
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

  /// NYI
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;

  /// Close after scan is finished
  void Close(RuntimeState* state) override;

 private:
  // Used to scan gather data of system table from ExecEnv
  std::unique_ptr<SystemTableScanner> scanner_;

  /// Descriptor of tuples read from SystemTable
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Materializes the next row into tuple.
  Status MaterializeNextTuple(MemPool* mem_pool, Tuple* tuple);
};

}
