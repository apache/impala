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

  // Materialized result/const exprs corresponding to materialized slots. Set in init().
  protected List<List<Expr>> materializedResultExprLists_ = Lists.newArrayList();
  protected List<List<Expr>> materializedConstExprLists_ = Lists.newArrayList();

  protected final TupleId tupleId_;

  protected MergeNode(PlanNodeId id, TupleId tupleId) {
    super(id, Lists.newArrayList(tupleId), "MERGE");
    rowTupleIds_.add(tupleId);
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
   * Must be called after addChild()/addConstExprList(). Computes the materialized
   * result/const expr lists based on the materialized slots of this MergeNode's
   * produced tuple. The MergeNode doesn't need an smap: like a ScanNode, it
   * materializes an original tuple.
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

    // drop resultExprs/constExprs that aren't getting materialized (= where the
    // corresponding output slot isn't being materialized)
    materializedResultExprLists_.clear();
    Preconditions.checkState(resultExprLists_.size() == children_.size());
    List<SlotDescriptor> slots = analyzer.getDescTbl().getTupleDesc(tupleId_).getSlots();
    for (List<Expr> exprList: resultExprLists_) {
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      materializedResultExprLists_.add(newExprList);
    }
    Preconditions.checkState(
        materializedResultExprLists_.size() == getChildren().size());

    materializedConstExprLists_.clear();
    for (List<Expr> exprList: constExprLists_) {
      List<Expr> newExprList = Lists.newArrayList();
      for (int i = 0; i < exprList.size(); ++i) {
        if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
      }
      materializedConstExprLists_.add(newExprList);
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    List<List<TExpr>> texprLists = Lists.newArrayList();
    for (List<Expr> exprList: materializedResultExprLists_) {
      texprLists.add(Expr.treesToThrift(exprList));
    }
    List<List<TExpr>> constTexprLists = Lists.newArrayList();
    for (List<Expr> constTexprList: materializedConstExprLists_) {
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
