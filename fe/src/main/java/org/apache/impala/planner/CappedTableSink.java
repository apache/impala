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

package org.apache.impala.planner;

import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeTable;

/**
 * A TableSink whose fragment runs on at most 'maxTableSinks_' instances, e.g. to honour
 * the MAX_FS_WRITERS query option or a Kudu table's partition count.
 */
public abstract class CappedTableSink extends TableSink implements HasQuantityLimit {
  // Upper limit on the number of instances of the fragment containing this sink.
  // <= 0 means unbounded.
  private final int maxTableSinks_;

  protected CappedTableSink(FeTable targetTable, Op sinkOp, List<Expr> outputExprs,
      int maxTableSinks) {
    super(targetTable, sinkOp, outputExprs);
    maxTableSinks_ = maxTableSinks;
  }

  /** Returns true if the fragment's instance count is capped by 'maxTableSinks_'. */
  public boolean hasInstanceLimit() { return maxTableSinks_ > 0; }

  /**
   * Return an estimate of the number of nodes the fragment with this sink will run on.
   * Bounded above by the number of instances, if they are capped.
   */
  @Override
  public int getNumNodes() {
    int numNodes = getFragment().getPlanRoot().getNumNodes();
    // With fewer instances than nodes, the instances are spread one per node.
    if (hasInstanceLimit()) numNodes = Math.min(numNodes, getNumInstances());
    return numNodes;
  }

  /**
   * Return an estimate of the number of instances the fragment with this sink will run
   * on. Bounded above by 'maxTableSinks_', if set.
   */
  @Override
  public int getNumInstances() {
    int numInstances = getFragment().getPlanRoot().getNumInstances();
    if (hasInstanceLimit()) numInstances = Math.min(numInstances, maxTableSinks_);
    return numInstances;
  }
}
