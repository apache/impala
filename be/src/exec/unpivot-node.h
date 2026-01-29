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

#include <memory>
#include <utility>
#include <vector>

#include "exec/exec-node.h"
#include "runtime/tuple.h"

namespace impala {

class TupleDescriptor;

class UnpivotPlanNode : public PlanNode {
 public:
  Status Init(const TPlanNode& tnode, FragmentState* state) override;
  void Close() override;
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;

  const TupleDescriptor* tuple_desc_ = nullptr;

  // Invalid SlotId to skip writing to the slot when materializing the tuples.
  static const SlotId INVALID_ID = -1;

  SlotId header_slot_id_ = INVALID_ID;
  SlotId data_slot_id_ = INVALID_ID;

  std::vector<ScalarExpr*> source_exprs_;

  std::vector<ScalarExpr*> data_exprs_;

  std::vector<ScalarExpr*> header_exprs_;

  int num_unpivot_columns_ = 0;

  ~UnpivotPlanNode(){}
};

class UnpivotNode : public ExecNode {
 public:
  UnpivotNode(ObjectPool* pool, const UnpivotPlanNode& pnode, const DescriptorTbl& descs);
  Status Prepare(RuntimeState* state) override;
  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;

 private:
  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// current row batch of child
  std::unique_ptr<RowBatch> child_row_batch_ = nullptr;

  /// index of current row in child_row_batch_
  int child_row_idx_ = 0;

  /// index of the current unpivot slot
  int unpivot_slot_idx_ = 0;

  /// true if last GetNext() call on child signalled eos
  bool child_eos_ = false;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  std::vector<ScalarExprEvaluator*> source_evals_;

  std::vector<ScalarExprEvaluator*> data_evals_;

  std::vector<ScalarExprEvaluator*> header_evals_;

  void MaterializeOutputTuple(Tuple* output_tuple, RowBatch* row_batch);
};

}
