// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/empty-set-node.h"

using namespace std;

namespace impala {

EmptySetNode::EmptySetNode(ObjectPool* pool, const TPlanNode& tnode,
                     const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs) {
}

Status EmptySetNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  *eos = true;
  return Status::OK;
}

}
