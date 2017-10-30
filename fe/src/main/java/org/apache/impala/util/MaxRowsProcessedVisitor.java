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

package org.apache.impala.util;

import com.google.common.base.Preconditions;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.ScanNode;

/**
 * Returns the maximum number of rows processed by any node in a given plan tree
 */
public class MaxRowsProcessedVisitor implements Visitor<PlanNode> {

  // True if we should abort because we don't have valid estimates
  // for a plan node.
  private boolean valid_ = true;
  private boolean foundJoinNode_ = false;

  // Max number of rows processed across all instances of a plan node.
  private long maxRowsProcessed_ = 0;

  // Max number of rows processed per backend impala daemon for a plan node.
  private long maxRowsProcessedPerNode_ = 0;

  @Override
  public void visit(PlanNode caller) {
    if (!valid_) return;
    if (caller instanceof JoinNode) foundJoinNode_ = true;

    PlanFragment fragment = caller.getFragment();
    int numNodes = fragment == null ? 1 : fragment.getNumNodes();
    if (caller instanceof ScanNode) {
      long numRows = caller.getInputCardinality();
      ScanNode scan = (ScanNode) caller;
      boolean missingStats = scan.isTableMissingStats() || scan.hasCorruptTableStats();
      // In the absence of collection stats, treat scans on collections as if they
      // have no limit.
      if (scan.isAccessingCollectionType() ||
          (missingStats && !(scan.hasLimit() && !scan.hasScanConjuncts() &&
              !scan.hasStorageLayerConjuncts()))) {
        valid_ = false;
        return;
      }
      maxRowsProcessed_ = Math.max(maxRowsProcessed_, numRows);
      maxRowsProcessedPerNode_ = Math.max(maxRowsProcessedPerNode_,
          (long)Math.ceil(numRows / (double)numNodes));
    } else {
      long in = caller.getInputCardinality();
      long out = caller.getCardinality();
      if ((in == -1) || (out == -1)) {
        valid_ = false;
        return;
      }
      long numRows = Math.max(in, out);
      maxRowsProcessed_ = Math.max(maxRowsProcessed_, numRows);
      maxRowsProcessedPerNode_ = Math.max(maxRowsProcessedPerNode_,
          (long)Math.ceil(numRows / (double)numNodes));
    }
  }

  public boolean valid() { return valid_; }

  public long getMaxRowsProcessed() {
    Preconditions.checkState(valid_);
    return maxRowsProcessed_;
  }

  public long getMaxRowsProcessedPerNode() {
    Preconditions.checkState(valid_);
    return maxRowsProcessedPerNode_;
  }

  public boolean foundJoinNode() {
    Preconditions.checkState(valid_);
    return foundJoinNode_;
  }

}
