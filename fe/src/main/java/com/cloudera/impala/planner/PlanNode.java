// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.thrift.TPlan;
import com.cloudera.impala.thrift.TPlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Each PlanNode represents a single relational operator
 * and encapsulates the information needed by the planner to
 * make optimization decisions.
 *
 * finalize(): Computes internal state, such as keys for scan nodes; gets called once on
 * the root of the plan tree before the call to toThrift(). Also finalizes the set
 * of conjuncts, such that each remaining one requires all of its referenced slots to
 * be materialized (ie, can be evaluated by calling GetValue(), rather than being
 * implicitly evaluated as part of a scan key).
 *
 * conjuncts: Each node has a list of conjuncts that can be executed in the context of
 * this node, ie, they only reference tuples materialized by this node or one of
 * its children (= are bound by tupleIds).
 */
abstract public class PlanNode extends TreeNode<PlanNode> {
  protected int id;  // unqiue w/in plan tree; assigned by planner
  protected long limit; // max. # of rows to be returned; 0: no limit
  protected ArrayList<TupleId> tupleIds;  // ids materialized by the tree rooted at this node

  // Composition of rows produced by this node. Possibly a superset of tupleIds
  // (the same RowBatch passes through multiple nodes; for instances, join nodes
  // form a left-deep chain and pass RowBatches on to their left children).
  // rowTupleIds[0] is the first tuple in the row, rowTupleIds[1] the second, etc.
  protected ArrayList<TupleId> rowTupleIds;

  protected List<Predicate> conjuncts;

  protected PlanNode(ArrayList<TupleId> tupleIds) {
    this.limit = -1;
    // make a copy, just to be on the safe side
    this.tupleIds = Lists.newArrayList(tupleIds);
    this.conjuncts = Lists.newArrayList();
    this.rowTupleIds = Lists.newArrayList();
  }

  protected PlanNode() {
    this.limit = -1;
    this.tupleIds = Lists.newArrayList();
    this.conjuncts = Lists.newArrayList();
    this.rowTupleIds = Lists.newArrayList();
  }

  public int getId() {
    return id;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public List<TupleId> getTupleIds() {
    Preconditions.checkState(tupleIds != null);
    return tupleIds;
  }

  public ArrayList<TupleId> getRowTupleIds() {
    Preconditions.checkState(rowTupleIds != null);
    return rowTupleIds;
  }

  public List<Predicate> getConjuncts() {
    return conjuncts;
  }

  public void setConjuncts(List<Predicate> conjuncts) {
    if (!conjuncts.isEmpty()) {
      this.conjuncts = conjuncts;
    }
  }

  public String getExplainString() {
    String explainString = getExplainString("");
    if (this.limit != -1) {
      explainString += "\nLIMIT: " + this.limit;
    }
    return explainString;
  }

  protected abstract String getExplainString(String prefix);

  // Convert this plan node, including all children, to its Thrift representation.
  public TPlan treeToThrift() {
    TPlan result = new TPlan();
    treeToThriftHelper(result);
    return result;
  }

  // Append a flattened version of this plan node, including all children, to 'container'.
  private void treeToThriftHelper(TPlan container) {
    TPlanNode msg = new TPlanNode();
    msg.node_id = id;
    msg.num_children = children.size();
    msg.limit = limit;
    for (TupleId tid: rowTupleIds) {
      msg.addToRow_tuples(tid.asInt());
    }
    for (Predicate p: conjuncts) {
      msg.addToConjuncts(p.treeToThrift());
    }
    toThrift(msg);
    container.addToNodes(msg);
    for (PlanNode child: children) {
      child.treeToThriftHelper(container);
    }
  }

  /**
   * Computes internal state. Call this once on the root of the plan tree before
   * calling toThrift(). The default implementation simply calls finalize()
   * on the children.
   */
  public void finalize(Analyzer analyzer) throws InternalException {
    for (PlanNode child: children) {
      child.finalize(analyzer);
    }
  }

  /**
   * Appends ids of slots that need to be materialized for this node.
   * By default, only slots referenced by conjuncts need to be materialized
   * (the rationale being that only conjuncts need to be evaluated explicitly;
   * exprs that are turned into scan predicates, etc., are evaluated implicitly).
   */
  public void getMaterializedIds(List<SlotId> ids) {
    Expr.getIds(getConjuncts(), null, ids);
  }

  // Convert this plan node into msg (excluding children), which requires setting
  // the node type and the node-specific field.
  protected abstract void toThrift(TPlanNode msg);

  protected String debugString() {
    // not using Objects.toStrHelper because
    // PlanNode.debugString() is embedded by debug strings of the subclasses
    StringBuilder output = new StringBuilder();
    output.append("preds=" + Expr.debugString(conjuncts));
    output.append(" limit=" + Long.toString(limit));
    return output.toString();
  }

  protected String getExplainString(List<? extends Expr> exprs) {
    if (exprs == null) {
      return "";
    }
    StringBuilder output = new StringBuilder();
    for (int i = 0; i < exprs.size(); ++i) {
      if (i > 0) {
        output.append(", ");
      }
      output.append(exprs.get(i).toSql());
    }
    return output.toString();
  }
}
