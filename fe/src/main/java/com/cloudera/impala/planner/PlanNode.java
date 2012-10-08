// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.thrift.TPlan;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TExplainLevel;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
  protected final PlanNodeId id;  // unique w/in plan tree; assigned by planner
  protected PlanFragmentId fragmentId;  // assigned by planner after fragmentation step
  protected long limit; // max. # of rows to be returned; 0: no limit

  // ids materialized by the tree rooted at this node
  protected ArrayList<TupleId> tupleIds;

  // Composition of rows produced by this node. Possibly a superset of tupleIds
  // (the same RowBatch passes through multiple nodes; for instances, join nodes
  // form a left-deep chain and pass RowBatches on to their left children).
  // rowTupleIds[0] is the first tuple in the row, rowTupleIds[1] the second, etc.
  protected ArrayList<TupleId> rowTupleIds = Lists.newArrayList();

  // A set of nullable TupleId produced by this node. It is a subset of tupleIds.
  // A tuple is nullable if it's the "nullable" side of an outer join, and it has nothing
  // to do with the schema.
  protected Set<TupleId> nullableTupleIds = Sets.newHashSet();

  protected List<Predicate> conjuncts = Lists.newArrayList();

  //  Node should compact data.
  protected boolean compactData;

  protected PlanNode(PlanNodeId id, ArrayList<TupleId> tupleIds) {
    this.id = id;
    this.limit = -1;
    // make a copy, just to be on the safe side
    this.tupleIds = Lists.newArrayList(tupleIds);
  }

  protected PlanNode(PlanNodeId id) {
    this.id = id;
    this.limit = -1;
    this.tupleIds = Lists.newArrayList();
  }

  /**
   * Copy c'tor. Also passes in new id.
   */
  protected PlanNode(PlanNodeId id, PlanNode node) {
    this.id = id;
    this.limit = node.limit;
    this.tupleIds = Lists.newArrayList(node.tupleIds);
    this.rowTupleIds = Lists.newArrayList(node.rowTupleIds);
    this.nullableTupleIds = Sets.newHashSet(node.nullableTupleIds);
    this.conjuncts = Expr.cloneList(node.conjuncts, null);
    this.compactData = node.compactData;
  }

  public PlanNodeId getId() {
    return id;
  }

  public void setFragmentId(PlanFragmentId id) {
    fragmentId = id;
  }

  public PlanFragmentId getFragmentId() {
    return fragmentId;
  }

  public long getLimit() {
    return limit;
  }

  /** Set the value of compactData in all children. */
  public void setCompactData(boolean on) {
    this.compactData = on;
    for (PlanNode child: this.getChildren()) {
      child.setCompactData(on);
    }
  }

  /**
   * Set the limit to the given limit only if the limit hasn't been set, or the new limit
   * is lower.
   * @param limit
   */
  public void setLimit(long limit) {
    if (this.limit == -1 || (limit != -1 && this.limit > limit)) {
      this.limit = limit;
    }
  }

  public void unsetLimit() {
    limit = -1;
  }

  public ArrayList<TupleId> getTupleIds() {
    Preconditions.checkState(tupleIds != null);
    return tupleIds;
  }

  public ArrayList<TupleId> getRowTupleIds() {
    Preconditions.checkState(rowTupleIds != null);
    return rowTupleIds;
  }

  public Set<TupleId> getNullableTupleIds() {
    Preconditions.checkState(nullableTupleIds != null);
    return nullableTupleIds;
  }

  public List<Predicate> getConjuncts() {
    return conjuncts;
  }

  public void setConjuncts(List<Predicate> conjuncts) {
    if (!conjuncts.isEmpty()) {
      this.conjuncts = conjuncts;
    }
  }

  public void transferConjuncts(PlanNode recipient) {
    recipient.conjuncts.addAll(conjuncts);
    conjuncts.clear();
  }

  public String getExplainString() {
    return getExplainString("", TExplainLevel.NORMAL);
  }

  protected String getExplainString(String prefix, TExplainLevel detailLevel) {
    String expStr = "";
    if (limit != -1) {
      expStr =  prefix + "LIMIT: " + limit + "\n";
    }

    // Output Tuple Ids only when explain plan level is set to verbose
    if (detailLevel.equals(TExplainLevel.VERBOSE)) {
      expStr += prefix + "TUPLE IDS: ";
      for (TupleId tupleId: tupleIds) {
        String nullIndicator = nullableTupleIds.contains(tupleId) ? "N" : "";
        expStr += tupleId.asInt() + nullIndicator + " ";
      }
      expStr += "\n";
    }

    return expStr;
  }

  // Convert this plan node, including all children, to its Thrift representation.
  public TPlan treeToThrift() {
    TPlan result = new TPlan();
    treeToThriftHelper(result);
    return result;
  }

  // Append a flattened version of this plan node, including all children, to 'container'.
  private void treeToThriftHelper(TPlan container) {
    TPlanNode msg = new TPlanNode();
    msg.node_id = id.asInt();
    msg.num_children = children.size();
    msg.limit = limit;
    for (TupleId tid: rowTupleIds) {
      msg.addToRow_tuples(tid.asInt());
      msg.addToNullable_tuples(nullableTupleIds.contains(tid));
    }
    for (Predicate p: conjuncts) {
      msg.addToConjuncts(p.treeToThrift());
    }
    msg.compact_data = compactData;
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
    if (this instanceof HashJoinNode && children.get(0) instanceof HdfsScanNode) {
      PlanNode leftChild = children.get(0);
      for (TupleId tid: rowTupleIds) {
        if (!leftChild.rowTupleIds.contains(tid)) {
          leftChild.rowTupleIds.add(tid);
        }
      }
    }
    for (PlanNode child: children) {
      child.finalize(analyzer);
    }
  }

  /**
   * Appends ids of slots that need to be materialized for this tree of nodes.
   * By default, only slots referenced by conjuncts need to be materialized
   * (the rationale being that only conjuncts need to be evaluated explicitly;
   * exprs that are turned into scan predicates, etc., are evaluated implicitly).
   */
  public void getMaterializedIds(List<SlotId> ids) {
    for (PlanNode childNode: children) {
      childNode.getMaterializedIds(ids);
    }
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
