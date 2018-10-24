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

#ifndef IMPALA_EXEC_PARTIAL_SORT_NODE_H
#define IMPALA_EXEC_PARTIAL_SORT_NODE_H

#include "exec/exec-node.h"
#include "runtime/sorter.h"

namespace impala {

/// Node that implements a partial sort, where its input is divided up into runs, each
/// of which is sorted individually.
///
/// In GetNext(), PartialSortNode accepts rows up to its memory limit and sorts them,
/// creating a single sorted run. It then outputs as many rows as fit in the output batch.
/// Subsequent calls to GetNext() continue to ouptut rows from the sorted run until it is
/// exhausted, at which point the next call to GetNext() will again accept rows to create
/// another run. This means that PartialSortNode never spills to disk.
///
/// Uses Sorter and BufferedBlockMgr for the external sort implementation. The sorter
/// instance owns the sorted data.
///
/// Input rows to PartialSortNode may consist of several tuples. The Sorter materializes
/// them into a single tuple using the expressions specified in sort_tuple_exprs_. This
/// single tuple is then what the sort operates on.
///
/// PartialSortNode does not support limits or offsets.
class PartialSortNode : public ExecNode {
 public:
  PartialSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
  ~PartialSortNode();

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual void Codegen(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  /// Expressions and parameters used for tuple comparison.
  std::vector<ScalarExpr*> ordering_exprs_;

  /// Expressions used to materialize slots in the tuples to be sorted.
  /// One expr per slot in the materialized tuple.
  std::vector<ScalarExpr*> sort_tuple_exprs_;

  std::vector<bool> is_asc_order_;
  std::vector<bool> nulls_first_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Object used for external sorting.
  boost::scoped_ptr<Sorter> sorter_;

  /// The current batch of rows retrieved from the input (the output of child(0)). This
  /// allows us to store rows across calls to GetNext when the sorter run fills up.
  std::unique_ptr<RowBatch> input_batch_;

  /// The index in 'input_batch_' of the next row to be passed to the sorter.
  int input_batch_index_;

  /// True if the end of the input (the output of child(0)) has been reached.
  bool input_eos_;

  /// True if the current run in the sorter has been fully output. This node is done when
  /// both 'sorter_eos_' and 'input_eos_' are true.
  bool sorter_eos_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};
}

#endif
