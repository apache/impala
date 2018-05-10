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

#ifndef IMPALA_EXEC_STREAMING_AGGREGATION_NODE_H
#define IMPALA_EXEC_STREAMING_AGGREGATION_NODE_H

#include <memory>

#include "exec/exec-node.h"
#include "exec/grouping-aggregator.h"

namespace impala {

class RowBatch;
class RuntimeState;

/// Node for doing streaming partitioned hash aggregation.
///
/// This node consumes the input from child(0) during GetNext() and then passes it to the
/// Aggregator, which does the actual work of aggregating. The aggregator will attempt to
/// aggregate the rows into its hash table, but if there is not enough memory available or
/// if the reduction from the aggregation is not very good, it will 'stream' the rows
/// through and return them without aggregating them instead of spilling. After all of the
/// input as been processed from child(0), subsequent calls to GetNext() will return any
/// rows that were aggregated in the Aggregator's hash table.
///
/// Since the rows returned by GetNext() may be only partially aggregated if there are
/// memory contraints, this is a preliminary aggregation step that functions as an
/// optimization and will always be followed in the plan by an AggregationNode that does
/// the final aggregation.
///
/// This node only supports grouping aggregations.
class StreamingAggregationNode : public ExecNode {
 public:
  StreamingAggregationNode(
      ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state) override;
  virtual Status Prepare(RuntimeState* state) override;
  virtual void Codegen(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual Status Reset(RuntimeState* state) override;
  virtual void Close(RuntimeState* state) override;

  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

 private:
  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Row batch used as argument to GetNext() for the child node preaggregations. Store
  /// in node to avoid reallocating for every GetNext() call when streaming.
  std::unique_ptr<RowBatch> child_batch_;

  /// True if no more rows to process from child.
  bool child_eos_;

  std::unique_ptr<GroupingAggregator> aggregator_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Get output rows from child for streaming pre-aggregation. Aggregates some rows with
  /// hash table and passes through other rows converted into the intermediate
  /// tuple format. Sets 'child_eos_' once all rows from child have been returned.
  Status GetRowsStreaming(RuntimeState* state, RowBatch* row_batch) WARN_UNUSED_RESULT;
};
} // namespace impala

#endif // IMPALA_EXEC_STREAMING_AGGREGATION_NODE_H
