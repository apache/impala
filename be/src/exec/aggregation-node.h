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

#ifndef IMPALA_EXEC_AGGREGATION_NODE_H
#define IMPALA_EXEC_AGGREGATION_NODE_H

#include <memory>

#include "exec/aggregation-node-base.h"

namespace impala {

class RowBatch;
class RuntimeState;

/// Node for doing partitioned hash aggregation.
/// This node consumes the input from child(0) during Open() and then passes it to the
/// Aggregator, which does the actual work of aggregating.
class AggregationNode : public AggregationNodeBase {
 public:
  AggregationNode(
      ObjectPool* pool, const AggregationPlanNode& pnode, const DescriptorTbl& descs);

  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual void Close(RuntimeState* state) override;

  virtual void DebugString(int indentation_level, std::stringstream* out) const override;
};
} // namespace impala

#endif // IMPALA_EXEC_AGGREGATION_NODE_H
