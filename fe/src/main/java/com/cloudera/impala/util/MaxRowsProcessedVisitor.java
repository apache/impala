// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.util;

import com.cloudera.impala.planner.NestedLoopJoinNode;
import com.cloudera.impala.planner.HashJoinNode;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.ScanNode;

/**
 * Returns the maximum number of rows processed by any node in a given plan tree
 */
public class MaxRowsProcessedVisitor implements Visitor<PlanNode> {

  private boolean abort_ = false;
  private long result_ = -1l;

  @Override
  public void visit(PlanNode caller) {
    if (abort_) return;

    if (caller instanceof ScanNode) {
      long tmp = caller.getInputCardinality();
      ScanNode scan = (ScanNode) caller;
      if (scan.isTableMissingTableStats() && !scan.hasLimit()) {
        abort_ = true;
        return;
      }
      result_ = Math.max(result_, tmp);
    } else if (caller instanceof HashJoinNode || caller instanceof NestedLoopJoinNode) {
      // Revisit when multiple scan nodes can be executed in a single fragment, IMPALA-561
      abort_ = true;
      return;
    } else {
      long in = caller.getInputCardinality();
      long out = caller.getCardinality();
      if ((in == -1) || (out == -1)) {
        abort_ = true;
        return;
      }
      result_ = Math.max(result_, Math.max(in, out));
    }
  }

  public long get() {
    return abort_ ? -1 : result_;
  }
}
