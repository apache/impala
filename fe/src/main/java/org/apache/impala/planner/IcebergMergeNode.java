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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MergeCase;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TIcebergMergeCase;
import org.apache.impala.thrift.TIcebergMergeNode;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

/**
 * Plan node for Iceberg merge operation. This node is responsible for holding the merge
 * cases, the descriptor of 'rowPresent_', the tuple descriptor for merge action tuple and
 * the target table's tuple id. The preceding node gives an upper limit for the resource
 * profile of this node.
 */
public class IcebergMergeNode extends PlanNode {
  private final List<MergeCase> cases_;
  private final Expr rowPresent_;
  private List<Expr> deleteMetaExprs_;
  private List<Expr> partitionMetaExprs_;
  private final TupleDescriptor mergeActionTuple_;
  private final TupleId targetTupleId_;

  public IcebergMergeNode(PlanNodeId id, PlanNode child, List<MergeCase> cases,
      Expr rowPresent, List<Expr> deleteMetaExprs, List<Expr> partitionMetaExprs,
      TupleDescriptor mergeActionTuple, TupleId targetTupleId) {
    super(id, "MERGE");
    this.cases_ = cases;
    this.rowPresent_ = rowPresent;
    this.deleteMetaExprs_ = deleteMetaExprs;
    this.partitionMetaExprs_ = partitionMetaExprs;
    this.mergeActionTuple_ = mergeActionTuple;
    this.targetTupleId_ = targetTupleId;
    Preconditions.checkState(child instanceof JoinNode);
    addChild(child);
    computeTupleIds();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    List<TIcebergMergeCase> mergeCases = new ArrayList<>();
    for (MergeCase mergeCase : cases_) {
      TIcebergMergeCase tMergeCase = new TIcebergMergeCase(
          Expr.treesToThrift(mergeCase.getResultExprs()), mergeCase.caseType(),
          mergeCase.matchType());
      if (!mergeCase.getFilterExprs().isEmpty()) {
        tMergeCase.setFilter_conjuncts(Expr.treesToThrift(mergeCase.getFilterExprs()));
      }
      mergeCases.add(tMergeCase);
    }
    TIcebergMergeNode mergeNode = new TIcebergMergeNode(mergeCases,
        rowPresent_.treeToThrift(), Expr.treesToThrift(deleteMetaExprs_),
        Expr.treesToThrift(partitionMetaExprs_), mergeActionTuple_.getId().asInt(),
        targetTupleId_.asInt());
    msg.setMerge_node(mergeNode);
    msg.setNode_type(TPlanNodeType.ICEBERG_MERGE_NODE);
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    mergeActionTuple_.computeMemLayout();
    for (MergeCase mergeCase : cases_) {
      mergeCase.substituteResultExprs(getOutputSmap(), analyzer);
    }
    partitionMetaExprs_ =
        Expr.substituteList(partitionMetaExprs_, getOutputSmap(), analyzer, true);
    deleteMetaExprs_ =
        Expr.substituteList(deleteMetaExprs_, getOutputSmap(), analyzer, true);
    rowPresent_.substitute(getOutputSmap(), analyzer, true);
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tblRefIds_.addAll(getChild(0).getTblRefIds());
    tupleIds_.addAll(getChild(0).getTupleIds());
    tupleIds_.add(mergeActionTuple_.getId());
    nullableTupleIds_.addAll(getChild(0).getNullableTupleIds());
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeDefaultProcessingCost();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (getChild(0).cardinality_ == -1) {
      cardinality_ = -1;
    } else {
      cardinality_ = applyConjunctsSelectivity(getChild(0).cardinality_);
      Preconditions.checkState(cardinality_ >= 0);
    }
  }

  @Override
  protected String getNodeExplainString(
      String rootPrefix, String detailPrefix, TExplainLevel detailLevel) {
    StringJoiner joiner = new StringJoiner("\n");
    joiner.add(String.format("%s%s", rootPrefix, getDisplayLabel()));
    for (int i = 0; i < cases_.size(); i++) {
      MergeCase mergeCase = cases_.get(i);
      joiner.add(String.format("%sCASE %d: %s", detailPrefix, i,
          cases_.get(i).matchTypeAsString()));
      for (String detail : mergeCase.getExplainStrings(detailLevel)) {
        joiner.add(String.format("%s%s%s", detailPrefix, detailPrefix, detail));
      }
    }
    return joiner + "\n";
  }
}
