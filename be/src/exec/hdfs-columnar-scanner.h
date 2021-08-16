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

#include "exec/hdfs-scanner.h"

#include <boost/scoped_ptr.hpp>

namespace impala {

class HdfsScanNodeBase;
class HdfsScanPlanNode;
class RowBatch;
class RuntimeState;
struct ScratchTupleBatch;

/// Parent class for scanners that read values into a scratch batch before applying
/// conjuncts and runtime filters.
class HdfsColumnarScanner : public HdfsScanner {
 public:
  HdfsColumnarScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsColumnarScanner();

  /// Codegen ProcessScratchBatch(). Stores the resulting function in
  /// 'process_scratch_batch_fn' if codegen was successful or NULL otherwise.
  static Status Codegen(HdfsScanPlanNode* node, FragmentState* state,
      llvm::Function** process_scratch_batch_fn);

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 protected:
  /// Column readers will write slot values into this scratch batch for
  /// top-level tuples. See AssembleRows() in the derived classes.
  boost::scoped_ptr<ScratchTupleBatch> scratch_batch_;

  typedef int (*ProcessScratchBatchFn)(HdfsColumnarScanner*, RowBatch*);
  /// The codegen'd version of ProcessScratchBatch() if available, NULL otherwise.
  /// Function type: ProcessScratchBatchFn
  const CodegenFnPtrBase* codegend_process_scratch_batch_fn_ = nullptr;

  /// Filters out tuples from 'scratch_batch_' and adds the surviving tuples
  /// to the given batch. Finalizing transfer of batch is not done here.
  /// Returns the number of tuples that should be committed to the given batch.
  int FilterScratchBatch(RowBatch* row_batch);

  /// Evaluates runtime filters and conjuncts (if any) against the tuples in
  /// 'scratch_batch_', and adds the surviving tuples to the given batch.
  /// Transfers the ownership of tuple memory to the target batch when the
  /// scratch batch is exhausted.
  /// Returns the number of rows that should be committed to the given batch.
  int TransferScratchTuples(RowBatch* row_batch);

  /// Processes a single row batch for TransferScratchTuples, looping over scratch_batch_
  /// until it is exhausted or the output is full. Called for the case when there are
  /// materialized tuples. This is a separate function so it can be codegened.
  int ProcessScratchBatch(RowBatch* dst_batch);

 private:
  int ProcessScratchBatchCodegenOrInterpret(RowBatch* dst_batch);
};

}
