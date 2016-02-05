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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TUnionNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Node that merges the results of its child plans by materializing
 * the corresponding result exprs into a new tuple.
 */
public class UnionNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(UnionNode.class);

  // Expr lists corresponding to the input query stmts.
  // The ith resultExprList belongs to the ith child.
  // All exprs are resolved to base tables.
  protected List<List<Expr>> resultExprLists_ = Lists.newArrayList();

  // Expr lists that originate from constant select stmts.
  // We keep them separate from the regular expr lists to avoid null children.
  protected List<List<Expr>> constExprLists_ = Lists.newArrayList();

  // Materialized result/const exprs corresponding to materialized slots.
  // Set in init() and substituted against the corresponding child's output smap.
  protected List<List<Expr>> materializedResultExprLists_ = Lists.newArrayList();
  protected List<List<Expr>> materializedConstExprLists_ = Lists.newArrayList();

  protected final TupleId tupleId_;

  protected UnionNode(PlanNodeId id, TupleId tupleId) {
    super(id, Lists.newArrayList(tupleId), "UNION");
    tupleId_ = tupleId;
  }

  public void addConstExprList(List<Expr> exprs) { constExprLists_.add(exprs); }

  /**
   * Add a child tree plus its corresponding resolved resultExprs.
   */
  public void addChild(PlanNode node, List<Expr> baseTblResultExprs) {
    super.addChild(node);
    resultExprLists_.add(baseTblResultExprs);
    if (baseTblResultExprs != null) {
      // if we're materializing output, we can only do that into a single
      // output tuple
      Preconditions.checkState(tupleIds_.size() == 1, tupleIds_.size());
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
        cardinality_ = addCardinalities(cardinality_, child.cardinality_);
      }
    }
    // The number of nodes of a union node is -1 (invalid) if all the referenced tables
    // are inline views (e.g. select 1 FROM (VALUES(1 x, 1 y)) a FULL OUTER JOIN
    // (VALUES(1 x, 1 y)) b ON (a.x = b.y)). We need to set the correct value.
    if (numNodes_ == -1) numNodes_ = 1;

    LOG.debug("stats Union: cardinality=" + Long.toString(cardinality_));
  }

  /**
   * Re-order the union's operands descending by their estimated per-host memory,
   * such that parent nodes can gauge the peak memory consumption of this MergeNode after
   * opening it during execution (a MergeNode opens its first operand in Open()).
   * Scan nodes are always ordered last because they can dynamically scale down their
   * memory usage, whereas many other nodes cannot (e.g., joins, aggregations).
   * One goal is to decrease the likelihood of a SortNode parent claiming too much
   * memory in its Open(), possibly causing the mem limit to be hit when subsequent
   * union operands are executed.
   * Can only be called on a fragmented plan because this function calls computeCosts()
   * on this node's children.
   * TODO: Come up with a good way of handing memory out to individual operators so that
   * they don't trip each other up. Then remove this function.
   */
  public void reorderOperands(Analyzer analyzer) {
    Preconditions.checkNotNull(fragment_,
        "Operands can only be reordered on the fragmented plan.");

    // List of estimated per-host memory consumption (first) by child index (second).
    List<Pair<Long, Integer>> memByChildIdx = Lists.newArrayList();
    for (int i = 0; i < children_.size(); ++i) {
      PlanNode child = children_.get(i);
      child.computeCosts(analyzer.getQueryCtx().request.getQuery_options());
      memByChildIdx.add(new Pair<Long, Integer>(child.getPerHostMemCost(), i));
    }

    Collections.sort(memByChildIdx,
        new Comparator<Pair<Long, Integer>>() {
      public int compare(Pair<Long, Integer> a, Pair<Long, Integer> b) {
        PlanNode aNode = children_.get(a.second);
        PlanNode bNode = children_.get(b.second);
        // Order scan nodes last because they can dynamically scale down their mem.
        if (bNode instanceof ScanNode && !(aNode instanceof ScanNode)) return -1;
        if (aNode instanceof ScanNode && !(bNode instanceof ScanNode)) return 1;
        long diff = b.first - a.first;
        return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
      }
    });

    List<List<Expr>> newResultExprLists = Lists.newArrayList();
    ArrayList<PlanNode> newChildren = Lists.newArrayList();
    for (Pair<Long, Integer> p: memByChildIdx) {
      newResultExprLists.add(resultExprLists_.get(p.second));
      newChildren.add(children_.get(p.second));
    }
    resultExprLists_ = newResultExprLists;
    children_ = newChildren;
  }

  /**
   * Must be called after addChild()/addConstExprList(). Computes the materialized
   * result/const expr lists based on the materialized slots of this UnionNode's
   * produced tuple. The UnionNode doesn't need an smap: like a ScanNode, it
   * materializes an original tuple.
   * There is no need to call assignConjuncts() because all non-constant conjuncts
   * have already been assigned to the union operands, and all constant conjuncts have
   * been evaluated during registration to set analyzer.hasEmptyResultSet_.
   */
  @Override
  public void init(Analyzer analyzer) {
    computeMemLayout(analyzer);
    computeStats(analyzer);

    // drop resultExprs/constExprs that aren't getting materialized (= where the
    // corresponding output slot isn't being materialized)
    materializedResultExprLists_.clear();
    Preconditions.checkState(resultExprLists_.size() == children_.size());
    List<SlotDescriptor> slots = analyzer.getDescTbl().getTupleDesc(tupleId_).getSlots();
    for (int i = 0; i < resultExprLists_.size(); ++i) {
      List<Expr> exprList = resultExprLists_.get(i);
      List<Expr> newExprList = Lists.newArrayList();
      Preconditions.checkState(exprList.size() == slots.size());
      for (int j = 0; j < exprList.size(); ++j) {
        if (slots.get(j).isMaterialized()) newExprList.add(exprList.get(j));
      }
      materializedResultExprLists_.add(
          Expr.substituteList(newExprList, getChild(i).getOutputSmap(), analyzer, true));
    }
    Preconditions.checkState(
        materializedResultExprLists_.size() == getChildren().size());

    materializedConstExprLists_.clear();
    for (List<Expr> exprList: constExprLists_) {
      Preconditions.checkState(exprList.size() == slots.size());
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      materializedConstExprLists_.add(newExprList);
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    Preconditions.checkState(materializedResultExprLists_.size() == children_.size());
    List<List<TExpr>> texprLists = Lists.newArrayList();
    for (List<Expr> exprList: materializedResultExprLists_) {
      texprLists.add(Expr.treesToThrift(exprList));
    }
    List<List<TExpr>> constTexprLists = Lists.newArrayList();
    for (List<Expr> constTexprList: materializedConstExprLists_) {
      constTexprLists.add(Expr.treesToThrift(constTexprList));
    }
    msg.union_node = new TUnionNode(tupleId_.asInt(), texprLists, constTexprLists);
    msg.node_type = TPlanNodeType.UNION_NODE;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    // A UnionNode may have predicates if a union is used inside an inline view,
    // and the enclosing select stmt has predicates referring to the inline view.
    if (!conjuncts_.isEmpty()) {
      output.append(detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
    }
    if (!constExprLists_.isEmpty()) {
      output.append(detailPrefix + "constant-operands=" + constExprLists_.size() + "\n");
    }
    return output.toString();
  }
}
