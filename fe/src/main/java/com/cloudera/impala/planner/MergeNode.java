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
  protected List<List<Expr>> resultExprLists = Lists.newArrayList();

  // Expr lists that originate from constant select stmts.
  // We keep them separate from the regular expr lists to avoid null children.
  protected List<List<Expr>> constExprLists = Lists.newArrayList();

  protected final TupleId tupleId;

  private final boolean isIntermediateMerge;

  protected MergeNode(PlanNodeId id, TupleId tupleId) {
    super(id, Lists.newArrayList(tupleId), "MERGE");
    this.rowTupleIds.add(tupleId);
    this.tupleId = tupleId;
    this.isIntermediateMerge = false;
  }

  /**
   * C'tor for intermediate MergeNode constructed during plan partitioning:
   * this node replicates the behavior of 'node' (same output tuple, same
   * result exprs) but only materializes child 'childIdx'.
   */
  protected MergeNode(PlanNodeId id, MergeNode node, int childIdx, PlanNode child) {
    super(id, node, "MERGE");
    this.tupleId = node.tupleId;
    this.isIntermediateMerge = true;
    resultExprLists.add(Expr.cloneList(node.getResultExprLists().get(childIdx), null));
    super.addChild(child);
  }

  private MergeNode(PlanNodeId id, MergeNode node) {
    super(id, node, "MERGE");
    this.tupleId = node.tupleId;
    this.isIntermediateMerge = true;
  }

  static public MergeNode createConstIntermediateMerge(PlanNodeId id, MergeNode node) {
    MergeNode result = new MergeNode(id, node);
    result.constExprLists.addAll(node.getConstExprLists());
    return result;
  }

  public List<List<Expr>> getResultExprLists() { return resultExprLists; }
  public List<List<Expr>> getConstExprLists() { return constExprLists; }

  public void addConstExprList(List<Expr> exprs) {
    constExprLists.add(exprs);
  }

  /**
   * Add a child tree plus its corresponding resolved resultExprs.
   */
  public void addChild(PlanNode node, List<Expr> baseTblResultExprs) {
    super.addChild(node);
    resultExprLists.add(baseTblResultExprs);
    if (baseTblResultExprs != null) {
      // if we're materializing output, we can only do that into a single
      // output tuple
      Preconditions.checkState(tupleIds.size() == 1);
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality = constExprLists.size();
    for (PlanNode child: children) {
      // ignore missing child cardinality info in the hope it won't matter enough
      // to change the planning outcome
      if (child.cardinality > 0) {
        cardinality += child.cardinality;
      }
    }
    LOG.debug("stats Merge: cardinality=" + Long.toString(cardinality));
  }

  /**
   * This must be called *after* addChild()/addConstExprList() because it recomputes
   * both of them.
   * The MergeNode doesn't need an smap: like a ScanNode, it materializes an "original"
   * tuple id
   */
  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);
    computeMemLayout(analyzer);
    computeStats(analyzer);
    Preconditions.checkState(resultExprLists.size() == getChildren().size());

    if (isIntermediateMerge) {
      // nothing left to do, but we want to check a few things
      Preconditions.checkState(resultExprLists.size() == children.size());
      Preconditions.checkState(resultExprLists.isEmpty() || resultExprLists.size() == 1);
      int numMaterializedSlots = 0;
      for (SlotDescriptor slot: analyzer.getDescTbl().getTupleDesc(tupleId).getSlots()) {
        if (slot.isMaterialized()) ++numMaterializedSlots;
      }
      if (!resultExprLists.isEmpty()) {
        Preconditions.checkState(resultExprLists.get(0).size() == numMaterializedSlots);
      }
      for (List<Expr> l: constExprLists) {
        Preconditions.checkState(l.size() == numMaterializedSlots);
       }
      return;
    }

    // drop resultExprs/constExprs that aren't getting materialized (= where the
    // corresponding output slot isn't being materialized)
    List<SlotDescriptor> slots = analyzer.getDescTbl().getTupleDesc(tupleId).getSlots();
    List<List<Expr>> newResultExprLists = Lists.newArrayList();
    for (List<Expr> exprList: resultExprLists) {
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      newResultExprLists.add(newExprList);
    }
    resultExprLists = newResultExprLists;
    Preconditions.checkState(resultExprLists.size() == getChildren().size());

    List<List<Expr>> newConstExprLists = Lists.newArrayList();
    for (List<Expr> exprList: constExprLists) {
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      newConstExprLists.add(newExprList);
    }
    constExprLists = newConstExprLists;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    List<List<TExpr>> texprLists = Lists.newArrayList();
    for (List<Expr> exprList : resultExprLists) {
      if (exprList != null) texprLists.add(Expr.treesToThrift(exprList));
    }
    List<List<TExpr>> constTexprLists = Lists.newArrayList();
    for (List<Expr> constTexprList : constExprLists) {
      constTexprLists.add(Expr.treesToThrift(constTexprList));
    }
    msg.merge_node = new TMergeNode(tupleId.asInt(), texprLists, constTexprLists);
    msg.node_type = TPlanNodeType.MERGE_NODE;
  }

  @Override
  protected String getNodeExplainString(String prefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    // A MergeNode may have predicates if a union is used inside an inline view,
    // and the enclosing select stmt has predicates referring to the inline view.
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
    }
    if (constExprLists.size() > 0) {
      output.append(prefix + "merging " + constExprLists.size() + " SELECT CONSTANT\n");
    }
    return output.toString();
  }
}
