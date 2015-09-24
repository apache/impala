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

package com.cloudera.impala.planner;

import java.util.ArrayList;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Preconditions;

/**
 * Node that returns an empty result set. Used for planning query blocks with a constant
 * predicate evaluating to false or a limit 0. The result set will have zero rows, but
 * the row descriptor must still include a materialized tuple so that the backend can
 * construct a valid row empty batch.
 */
public class EmptySetNode extends PlanNode {
  public EmptySetNode(PlanNodeId id, ArrayList<TupleId> tupleIds) {
    super(id, tupleIds, "EMPTYSET");
    Preconditions.checkArgument(tupleIds.size() > 0);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    avgRowSize_ = 0;
    cardinality_ = 0;
    perHostMemCost_ = 0;
    numNodes_ = 1;
  }

  @Override
  public void init(Analyzer analyzer) {
    // If the physical output tuple produced by an AnalyticEvalNode wasn't created
    // the logical output tuple is returned by getMaterializedTupleIds(). It needs
    // to be set as materialized (even though it isn't) to avoid failing precondition
    // checks generating the thrift for slot refs that may reference this tuple.
    for (TupleId id: tupleIds_) analyzer.getTupleDesc(id).setIsMaterialized(true);
    computeMemLayout(analyzer);
    computeStats(analyzer);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    return String.format("%s%s:%s\n", prefix, id_.toString(), displayName_);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.EMPTY_SET_NODE;
  }
}
