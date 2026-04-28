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

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.UnpivotTableRef;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TUnpivotNode;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.MathUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public final class UnpivotNode extends PlanNode {
  final UnpivotTableRef unpivotTableRef_;

  // Maps the slot index to the Expr that materializes the slot.
  // The element is null if the slot is for the data column or the header column. in this
  // case, the NULL literal will be used as the place holder when serializing it to
  // Thrift.
  final List<Expr> sourceExprs_ = new ArrayList<>();

  final List<Expr> dataExprs_;
  final List<Expr> headerExprs_;

  UnpivotNode(PlanNodeId id, PlanNode child, List<Expr> conjuncts,
        UnpivotTableRef unpivotTableRef) {
    super(id, "UNPIVOT");
    addChild(child);
    conjuncts_ = conjuncts;
    unpivotTableRef_ = Preconditions.checkNotNull(unpivotTableRef);
    tupleIds_.add(unpivotTableRef_.getDesc().getId());
    tblRefIds_.add(unpivotTableRef_.getDesc().getId());
    dataExprs_ = Lists.newArrayListWithCapacity(
        unpivotTableRef_.getNumUnpivotColumns());
    headerExprs_ = Lists.newArrayListWithCapacity(
        unpivotTableRef_.getNumUnpivotColumns());
  }

  @Override
  public void init(Analyzer analyzer) {
    for (SlotDescriptor slot : unpivotTableRef_.getDesc().getMaterializedSlots()) {
      Expr e = unpivotTableRef_.getSourceExprMap().get(slot.getId());
      if (e == null) {
        sourceExprs_.add(null);
      } else {
        sourceExprs_.add(
            e.substitute(getChild(0).getOutputSmap(), analyzer, true));
      }
    }
    if (unpivotTableRef_.getUnpivotExprMap() != null) {
      for (Map.Entry<Expr, LiteralExpr> e :
            unpivotTableRef_.getUnpivotExprMap().entrySet()) {
        Expr result = e.getKey().substitute(getChild(0).getOutputSmap(), analyzer, true);
        dataExprs_.add(result);
        headerExprs_.add(e.getValue());
      }
    }

    conjuncts_ = orderConjunctsByCost(conjuncts_);
    computeMemLayout(analyzer);
    createDefaultSmap(analyzer);
    computeStats(analyzer);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    Preconditions.checkState(false, "Unexpected use of old toThrift() signature.");
  }

  @Override
  protected void toThrift(TPlanNode msg, ThriftSerializationCtx serialCtx) {
    msg.node_type = TPlanNodeType.UNPIVOT_NODE;
    TUnpivotNode unpivotNode = new TUnpivotNode();
    for (Expr e : sourceExprs_) {
      if (e == null) {
        // Use the NULL literal as the place holder.
        e = new NullLiteral();
      }
      unpivotNode.addToSource_exprs(e.treeToThrift(serialCtx));
    }
    unpivotNode.setNum_unpivot_columns(unpivotTableRef_.getNumUnpivotColumns());
    SlotDescriptor dataSlot = unpivotTableRef_.getDataSlotDescriptor();
    if (dataSlot != null && dataSlot.isMaterialized()) {
      unpivotNode.setData_slot_id(dataSlot.getId().asInt());
      for (Expr e : dataExprs_) {
        unpivotNode.addToData_exprs(e.treeToThrift(serialCtx));
      }
    }
    SlotDescriptor headerSlot = unpivotTableRef_.getHeaderSlotDescriptor();
    if (headerSlot != null && headerSlot.isMaterialized()) {
      unpivotNode.setHeader_slot_id(headerSlot.getId().asInt());
      for (Expr e : headerExprs_) {
        unpivotNode.addToHeader_exprs(e.treeToThrift(serialCtx));
      }
    }
    msg.setUnpivot_node(unpivotNode);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    output.append(detailPrefix + "source-exprs=" +
        Expr.getExplainString(sourceExprs_, detailLevel, true) + "\n");
    output.append(detailPrefix + "unpivot-data-exprs=" +
        Expr.getExplainString(dataExprs_, detailLevel, true) + "\n");
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix
            + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = MathUtil.multiplyCardinalities(
        children_.get(0).cardinality_, unpivotTableRef_.getNumUnpivotColumns());
    cardinality_ = capCardinalityAtLimit(cardinality_);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // TODO: Improve the estimation based on benchmarks.
    float exprsCost = ExprUtil.computeExprsTotalCost(sourceExprs_);
    if (!dataExprs_.isEmpty()) {
      exprsCost += ExprUtil.computeExprCost(dataExprs_.get(0))
          + ExprUtil.computeExprCost(headerExprs_.get(0));
    }
    processingCost_ = ProcessingCost.basicCost(
        getDisplayLabel(), cardinality_, exprsCost);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }
}
