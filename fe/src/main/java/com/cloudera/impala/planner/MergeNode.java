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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TMergeNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Node that merges the results of its child plans by materializing
 * the corresponding result exprs.
 * If no result exprs are specified for a child, it simply passes on the child's
 * results.
 */
public class MergeNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(MergeNode.class);

  // Expr lists corresponding to the input query stmts.
  // The ith resultExprList belongs to the ith child.
  // All exprs are resolved to base tables.
  protected List<List<Expr>> resultExprLists_ = Lists.newArrayList();

  // Expr lists that originate from constant select stmts.
  // We keep them separate from the regular expr lists to avoid null children.
  protected List<List<Expr>> constExprLists_ = Lists.newArrayList();

  protected final TupleId tupleId_;

  private final boolean isIntermediateMerge_;

  protected MergeNode(PlanNodeId id, TupleId tupleId) {
    super(id, Lists.newArrayList(tupleId), "MERGE");
    rowTupleIds_.add(tupleId);
    tupleId_ = tupleId;
    isIntermediateMerge_ = false;
  }

  /**
   * C'tor for intermediate MergeNode constructed during plan partitioning:
   * this node replicates the behavior of 'node' (same output tuple, same
   * result exprs) but only materializes child 'childIdx'.
   */
  protected MergeNode(PlanNodeId id, MergeNode node, int childIdx, PlanNode child) {
    super(id, node, "MERGE");
    tupleId_ = node.tupleId_;
    isIntermediateMerge_ = true;
    resultExprLists_.add(Expr.cloneList(node.getResultExprLists().get(childIdx), null));
    super.addChild(child);
  }

  private MergeNode(PlanNodeId id, MergeNode node) {
    super(id, node, "MERGE");
    tupleId_ = node.tupleId_;
    isIntermediateMerge_ = true;
  }

  static public MergeNode createConstIntermediateMerge(PlanNodeId id, MergeNode node) {
    MergeNode result = new MergeNode(id, node);
    result.constExprLists_.addAll(node.getConstExprLists());
    return result;
  }

  public List<List<Expr>> getResultExprLists() { return resultExprLists_; }
  public List<List<Expr>> getConstExprLists() { return constExprLists_; }

  public void addConstExprList(List<Expr> exprs) {
    constExprLists_.add(exprs);
  }

  /**
   * Add a child tree plus its corresponding resolved resultExprs.
   */
  public void addChild(PlanNode node, List<Expr> baseTblResultExprs) {
    super.addChild(node);
    resultExprLists_.add(baseTblResultExprs);
    if (baseTblResultExprs != null) {
      // if we're materializing output, we can only do that into a single
      // output tuple
      Preconditions.checkState(tupleIds_.size() == 1);
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = constExprLists_.size();
    for (PlanNode child: children_) {
      // ignore missing child cardinality info in the hope it won't matter enough
      // to change the planning outcome
      if (child.cardinality_ > 0) {
        cardinality_ += child.cardinality_;
      }
    }
    // The number of nodes of a merge node is -1 (invalid) if all the referenced tables
    // are inline views (e.g. select 1 FROM (VALUES(1 x, 1 y)) a FULL OUTER JOIN
    // (VALUES(1 x, 1 y)) b ON (a.x = b.y)). We need to set the correct value.
    if (numNodes_ == -1) numNodes_ = 1;

    LOG.debug("stats Merge: cardinality=" + Long.toString(cardinality_));
  }

  /**
   * This must be called *after* addChild()/addConstExprList() because it recomputes
   * both of them.
   * The MergeNode doesn't need an smap: like a ScanNode, it materializes an "original"
   * tuple id_
   */
  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);
    // All non-constant conjuncts should have been assigned to children.
    // This requirement is important to guarantee that conjuncts do not trigger
    // materialization of slots in this MergeNode's tuple.
    // TODO: It's possible to get constant conjuncts if a union operand
    // consists of a select stmt on a constant inline view. We should
    // drop the operand in such cases.
    for (Expr conjunct: conjuncts_) {
      Preconditions.checkState(conjunct.isConstant());
    }

    computeMemLayout(analyzer);
    computeStats(analyzer);
    Preconditions.checkState(resultExprLists_.size() == getChildren().size());

    if (isIntermediateMerge_) {
      // nothing left to do, but we want to check a few things
      Preconditions.checkState(resultExprLists_.size() == children_.size());
      Preconditions.checkState(resultExprLists_.isEmpty() || resultExprLists_.size() == 1);
      int numMaterializedSlots = 0;
      for (SlotDescriptor slot: analyzer.getDescTbl().getTupleDesc(tupleId_).getSlots()) {
        if (slot.isMaterialized()) ++numMaterializedSlots;
      }
      if (!resultExprLists_.isEmpty()) {
        Preconditions.checkState(resultExprLists_.get(0).size() == numMaterializedSlots);
      }
      for (List<Expr> l: constExprLists_) {
        Preconditions.checkState(l.size() == numMaterializedSlots);
      }
      return;
    }

    // drop resultExprs/constExprs that aren't getting materialized (= where the
    // corresponding output slot isn't being materialized)
    List<SlotDescriptor> slots = analyzer.getDescTbl().getTupleDesc(tupleId_).getSlots();
    List<List<Expr>> newResultExprLists = Lists.newArrayList();
    for (List<Expr> exprList: resultExprLists_) {
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      newResultExprLists.add(newExprList);
    }
    resultExprLists_ = newResultExprLists;
    Preconditions.checkState(resultExprLists_.size() == getChildren().size());

    List<List<Expr>> newConstExprLists = Lists.newArrayList();
    for (List<Expr> exprList: constExprLists_) {
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      newConstExprLists.add(newExprList);
    }
    constExprLists_ = newConstExprLists;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    List<List<TExpr>> texprLists = Lists.newArrayList();
    for (List<Expr> exprList : resultExprLists_) {
      if (exprList != null) texprLists.add(Expr.treesToThrift(exprList));
    }
    List<List<TExpr>> constTexprLists = Lists.newArrayList();
    for (List<Expr> constTexprList : constExprLists_) {
      constTexprLists.add(Expr.treesToThrift(constTexprList));
    }
    msg.merge_node = new TMergeNode(tupleId_.asInt(), texprLists, constTexprLists);
    msg.node_type = TPlanNodeType.MERGE_NODE;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    // A MergeNode may have predicates if a union is used inside an inline view,
    // and the enclosing select stmt has predicates referring to the inline view.
    if (!conjuncts_.isEmpty()) {
      output.append(detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
    }
    if (!constExprLists_.isEmpty()) {
      output.append(detailPrefix + "constant-selects=" + constExprLists_.size() + "\n");
    }
    return output.toString();
  }
}
