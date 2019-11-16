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

#ifndef IMPALA_EXEC_SINGULAR_ROW_SRC_NODE_H_
#define IMPALA_EXEC_SINGULAR_ROW_SRC_NODE_H_

#include "exec/exec-node.h"

namespace impala {

class SingularRowSrcPlanNode : public PlanNode {
 public:
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~SingularRowSrcPlanNode(){}
};

/// Exec node that returns a single row from its containing Subplan node.
/// Does not have any children.

class SingularRowSrcNode : public ExecNode {
 public:
  SingularRowSrcNode(
      ObjectPool* pool, const SingularRowSrcPlanNode& pnode, const DescriptorTbl& descs);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
};

}

#endif
