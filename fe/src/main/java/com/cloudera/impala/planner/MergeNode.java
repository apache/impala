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
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleDescriptor;
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
  protected final List<List<Expr>> resultExprLists = Lists.newArrayList();

  // Expr lists that originate from constant select stmts.
  // We keep them separate from the regular expr lists to avoid null children.
  protected final List<List<Expr>> constExprLists = Lists.newArrayList();

  // Output tuple materialized by this node.
  protected final List<TupleDescriptor> tupleDescs = Lists.newArrayList();

  protected MergeNode(PlanNodeId id, TupleId tupleId) {
    super(id, Lists.newArrayList(tupleId));
    this.rowTupleIds.add(tupleId);
  }

  protected MergeNode(PlanNodeId id, MergeNode node) {
    super(id, node);
  }

  public void addConstExprList(List<Expr> exprs) {
    constExprLists.add(exprs);
  }

  public void addChild(PlanNode node, List<Expr> resultExprs) {
    addChild(node);
    resultExprLists.add(resultExprs);
    if (resultExprs != null) {
      // if we're materializing output, we can only do that into a single
      // output tuple
      Preconditions.checkState(tupleIds.size() == 1);
    }
  }

  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
    super.finalize(analyzer);
    cardinality = constExprLists.size();
    for (PlanNode child: children) {
      // ignore missing child cardinality info in the hope it won't matter enough
      // to change the planning outcome
      if (child.cardinality > 0) {
        cardinality += child.cardinality;
      }
    }
    LOG.info("finalize Merge: cardinality=" + Long.toString(cardinality));
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
    List<List<TExpr>> constTexprLists = Lists.newArrayList();
    for (List<Expr> exprList : resultExprLists) {
      if (exprList != null) {
        texprLists.add(Expr.treesToThrift(exprList));
      }
    }
    for (List<Expr> constTexprList : constExprLists) {
      constTexprLists.add(Expr.treesToThrift(constTexprList));
    }
    msg.merge_node = new TMergeNode(texprLists, constTexprLists);
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
  public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
    super.getMaterializedIds(analyzer, ids);

    for (List<Expr> resultExprs: resultExprLists) {
      Expr.getIds(resultExprs, null, ids);
    }

    // for now, also mark all of our output slots as materialized
    // TODO: fix this, it's not really necessary, but it breaks the logic
    // in MergeNode (c++)
    for (TupleId tupleId: tupleIds) {
      TupleDescriptor tupleDesc = analyzer.getTupleDesc(tupleId);
      for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
        ids.add(slotDesc.getId());
      }
    }
  }
}
