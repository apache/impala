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

#include <string>

#include "exec/exec-node.h"

namespace impala {

class TupleCachePlanNode : public PlanNode {
 public:
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~TupleCachePlanNode(){}
};

/// Node that caches rows produced by a child node.
///
/// This is currently a stub implementation that simply returns the RowBatch produced
/// by its child node.
///
/// FUTURE:
/// This node looks up the subtree_hash_ in the tuple cache. If an entry exists, this
/// reads rows from the cache rather than executing its child node. If the entry does not
/// exist, this will read rows from its child, write them to the cache, and returns them.
class TupleCacheNode : public ExecNode {
 public:
  TupleCacheNode(ObjectPool* pool, const TupleCachePlanNode& pnode,
      const DescriptorTbl& descs);

  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  void DebugString(int indentation_level, std::stringstream* out) const override;
private:
  const std::string subtree_hash_;
};

}
