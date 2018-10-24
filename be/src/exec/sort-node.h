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

#ifndef IMPALA_EXEC_SORT_NODE_H
#define IMPALA_EXEC_SORT_NODE_H

#include "exec/exec-node.h"
#include "runtime/sorter.h"

namespace impala {

/// Node that implements a full sort of its input with a fixed memory budget, spilling
/// to disk if the input is larger than available memory.
/// Uses Sorter for the external sort implementation.
/// Input rows to SortNode are materialized by the Sorter into a single tuple
/// using the expressions specified in sort_tuple_exprs_.
/// In GetNext(), SortNode passes in the output batch to the sorter instance created
/// in Open() to fill it with sorted rows.
/// If a merge phase was performed in the sort, sorted rows are deep copied into
/// the output batch. Otherwise, the sorter instance owns the sorted data.
class SortNode : public ExecNode {
 public:
  SortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
  ~SortNode();

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
  /// Fetch input rows and feed them to the sorter until the input is exhausted.
  Status SortInput(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Number of rows to skip.
  int64_t offset_;

  /// Expressions and parameters used for tuple comparison.
  std::vector<ScalarExpr*> ordering_exprs_;

  /// Expressions used to materialize slots in the tuples to be sorted.
  /// One expr per slot in the materialized tuple.
  std::vector<ScalarExpr*> sort_tuple_exprs_;

  std::vector<bool> is_asc_order_;
  std::vector<bool> nulls_first_;

  /// Whether the previous call to GetNext() returned a buffer attached to the RowBatch.
  /// Used to avoid unnecessary calls to ReleaseUnusedReservation().
  bool returned_buffer_ = false;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Object used for external sorting.
  boost::scoped_ptr<Sorter> sorter_;

  /// Keeps track of the number of rows skipped for handling offset_.
  int64_t num_rows_skipped_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};

}

#endif
