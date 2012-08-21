// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleDescriptor;
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
 */
public class MergeNode extends PlanNode {
  // Expr lists corresponding to the input query stmts.
  // The ith resultExprList belongs to the ith child.
  protected final List<List<Expr>> resultExprLists = Lists.newArrayList();

  // Expr lists that originate from constant select stmts.
  // We keep them separate from the regular expr lists to avoid null children.
  protected final List<List<Expr>> constExprLists = Lists.newArrayList();

  // Output tuple materialized by this node.
  protected final TupleDescriptor tupleDesc;

  protected MergeNode(PlanNodeId id, TupleDescriptor tupleDesc) {
    super(id, tupleDesc.getId().asList());
    this.tupleDesc = tupleDesc;
    this.rowTupleIds.clear();
    this.rowTupleIds.addAll(tupleDesc.getId().asList());
  }

  /**
   * C'tor for distributed merge operation: the child fragment corresponding to child i
   * of singleNodeMerge performs a "merge"/tuple materialization operation, but only
   * for child i of singleNodeMerge.
   */
  protected MergeNode(
      PlanNodeId id, MergeNode singleNodeMerge, int childIdx, PlanNode childPlan) {
    super(id, singleNodeMerge);
    Preconditions.checkState(singleNodeMerge.constExprLists.isEmpty());
    this.tupleDesc = singleNodeMerge.tupleDesc;
    addChild(childPlan);
    List<Expr> resultExprs =
        Expr.cloneList(singleNodeMerge.resultExprLists.get(childIdx), null);
    resultExprLists.add(resultExprs);
  }

  public void addConstExprList(List<Expr> exprs) {
    constExprLists.add(exprs);
  }

  public void addChild(PlanNode node, List<Expr> resultExprs) {
    addChild(node);
    resultExprLists.add(resultExprs);
  }

  public List<List<Expr>> getResultExprLists() {
    return resultExprLists;
  }

  public List<List<Expr>> getConstExprLists() {
    return constExprLists;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    List<List<TExpr>> texprLists = Lists.newArrayList();
    List<List<TExpr>> noFromTexprLists = Lists.newArrayList();
    for (List<Expr> exprList : resultExprLists) {
      texprLists.add(Expr.treesToThrift(exprList));
    }
    for (List<Expr> noFromTexprList : constExprLists) {
      noFromTexprLists.add(Expr.treesToThrift(noFromTexprList));
    }
    msg.merge_node = new TMergeNode(texprLists, noFromTexprLists);
    msg.node_type = TPlanNodeType.MERGE_NODE;
  }

  @Override
  protected String getExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "MERGE (" + id + ")\n");
    output.append(super.getExplainString(prefix + "  ", detailLevel));
    // A MergeNode may have predicates if a union is used inside an inline view,
    // and the enclosing select stmt has predicates referring to the inline view.
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    }
    for (int i = 0; i < constExprLists.size(); ++i) {
      output.append(prefix + "  SELECT CONSTANT\n");
    }
    for (PlanNode child : children) {
      output.append(child.getExplainString(prefix + "  ", detailLevel));
    }
    return output.toString();
  }

  @Override
  public void getMaterializedIds(List<SlotId> ids) {
    super.getMaterializedIds(ids);

    for (List<Expr> resultExprs: resultExprLists) {
      Expr.getIds(resultExprs, null, ids);
    }
  }
}
