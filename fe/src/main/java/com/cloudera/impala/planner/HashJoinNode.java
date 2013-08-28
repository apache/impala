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
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TEqJoinCondition;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THashJoinNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 *
 */
public class HashJoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HashJoinNode.class);

  // Default per-host memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  private final static long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;

  private final TableRef innerRef;
  private final JoinOperator joinOp;

  enum DistributionMode {
    NONE("NONE"),
    BROADCAST("BROADCAST"),
    PARTITIONED("PARTITIONED");

    private final String description;

    private DistributionMode(String descr) {
      this.description = descr;
    }

    @Override
    public String toString() { return description; }
  }

  private DistributionMode distrMode;

  // conjuncts of the form "<lhs> = <rhs>", recorded as Pair(<lhs>, <rhs>)
  private List<Pair<Expr, Expr> > eqJoinConjuncts;

  // join conjuncts from the JOIN clause that aren't equi-join predicates
  private List<Expr> otherJoinConjuncts;

  public HashJoinNode(
      PlanNode outer, PlanNode inner, TableRef innerRef,
      List<Pair<Expr, Expr> > eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
    super("HASH JOIN");
    Preconditions.checkArgument(eqJoinConjuncts != null);
    Preconditions.checkArgument(otherJoinConjuncts != null);
    tupleIds.addAll(outer.getTupleIds());
    tupleIds.addAll(inner.getTupleIds());
    tblRefIds_.addAll(outer.getTblRefIds());
    tblRefIds_.addAll(inner.getTblRefIds());
    this.innerRef = innerRef;
    this.joinOp = innerRef.getJoinOp();
    this.distrMode = DistributionMode.NONE;
    this.eqJoinConjuncts = eqJoinConjuncts;
    this.otherJoinConjuncts = otherJoinConjuncts;
    children.add(outer);
    children.add(inner);

    // Inherits all the nullable tuple from the children
    // Mark tuples that form the "nullable" side of the outer join as nullable.
    nullableTupleIds.addAll(inner.getNullableTupleIds());
    nullableTupleIds.addAll(outer.getNullableTupleIds());
    if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
      nullableTupleIds.addAll(outer.getTupleIds());
      nullableTupleIds.addAll(inner.getTupleIds());
    } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
      nullableTupleIds.addAll(inner.getTupleIds());
    } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
      nullableTupleIds.addAll(outer.getTupleIds());
    }
  }

  public List<Pair<Expr, Expr>> getEqJoinConjuncts() { return eqJoinConjuncts; }
  public JoinOperator getJoinOp() { return joinOp; }
  public TableRef getInnerRef() { return innerRef; }
  public DistributionMode getDistributionMode() { return distrMode; }
  public void setDistributionMode(DistributionMode distrMode) {
    this.distrMode = distrMode;
  }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);

    // Set smap to the combined childrens' smaps and apply that to all conjuncts.
    createDefaultSmap();

    computeStats(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();

    Expr.SubstitutionMap combinedChildSmap = getCombinedChildSmap();
    //LOG.info("combinedChildSmap: " + combinedChildSmap.debugString());
    otherJoinConjuncts = Expr.cloneList(otherJoinConjuncts, combinedChildSmap);
    //LOG.info("HJ.finalized other conjuncts: " + Expr.toSql(otherJoinConjuncts));
    //LOG.info("HJ.finalized other conjuncts: " + Expr.debugString(otherJoinConjuncts));
    List<Pair<Expr, Expr>> newEqJoinConjuncts = Lists.newArrayList();
    for (Pair<Expr, Expr> c: eqJoinConjuncts) {
      Pair<Expr, Expr> p =
          new Pair(c.first.clone(combinedChildSmap), c.second.clone(combinedChildSmap));
      //LOG.info(p.first.toSql() + " " + p.second.toSql());
      //LOG.info(p.first.debugString() + " " + p.second.debugString());
      newEqJoinConjuncts.add(
          new Pair(c.first.clone(combinedChildSmap), c.second.clone(combinedChildSmap)));
    }
    eqJoinConjuncts = newEqJoinConjuncts;
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);

    // For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
    // (with L being from child(0) and R from child(1)) and use as the cardinality
    // estimate the maximum of
    //   child(0).cardinality * R.cardinality / # distinct values for R.d
    //     * child(1).cardinality / R.cardinality
    // across all suitable join conditions, which simplifies to
    //   child(0).cardinality * child(1).cardinality / # distinct values for R.d
    // The reasoning is that
    // - each row in child(0) joins with R.cardinality/#DV_R.d rows in R
    // - each row in R is 'present' in child(1).cardinality / R.cardinality rows in
    //   child(1)
    //
    // This handles the very frequent case of a fact table/dimension table join
    // (aka foreign key/primary key join) if the primary key is a single column, with
    // possible additional predicates against the dimension table. An example:
    // FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
    // - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
    //   and the output cardinality of the Customers scan would be 0.2 * # rows in
    //   Customers
    // - # rows in Customers == # of distinct values for Customers.id
    // - the output cardinality of the join would be F.cardinality * 0.2

    long maxNumDistinct = 0;
    for (Pair<Expr, Expr> eqJoinPredicate: eqJoinConjuncts) {
      if (eqJoinPredicate.first.unwrapSlotRef(false) == null) continue;
      SlotRef rhsSlotRef = eqJoinPredicate.second.unwrapSlotRef(false);
      if (rhsSlotRef == null) continue;
      SlotDescriptor slotDesc = rhsSlotRef.getDesc();
      if (slotDesc == null) continue;
      ColumnStats stats = slotDesc.getStats();
      if (!stats.hasNumDistinctValues()) continue;
      long numDistinct = stats.getNumDistinctValues();
      Table rhsTbl = slotDesc.getParent().getTable();
      if (rhsTbl != null && rhsTbl.getNumRows() != -1) {
        // we can't have more distinct values than rows in the table, even though
        // the metastore stats may think so
        LOG.debug("#distinct=" + numDistinct + " #rows="
            + Long.toString(rhsTbl.getNumRows()));
        numDistinct = Math.min(numDistinct, rhsTbl.getNumRows());
      }
      maxNumDistinct = Math.max(maxNumDistinct, numDistinct);
      LOG.debug("min slotref=" + rhsSlotRef.toSql() + " #distinct="
          + Long.toString(numDistinct));
    }

    if (maxNumDistinct == 0) {
      // if we didn't find any suitable join predicates or don't have stats
      // on the relevant columns, we very optimistically assume we're doing an
      // FK/PK join (which doesn't alter the cardinality of the left-hand side)
      cardinality = getChild(0).cardinality;
    } else if (getChild(0).cardinality != -1 && getChild(1).cardinality != -1) {
      cardinality = Math.round(
          (double) getChild(0).cardinality
            * (double) getChild(1).cardinality / (double) maxNumDistinct);
    }
    Preconditions.checkState(hasValidStats());
    LOG.debug("stats HashJoin: cardinality=" + Long.toString(cardinality));
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("eqJoinConjuncts", eqJoinConjunctsDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String eqJoinConjunctsDebugString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    for (Pair<Expr, Expr> entry: eqJoinConjuncts) {
      helper.add("lhs" , entry.first).add("rhs", entry.second);
    }
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.hash_join_node = new THashJoinNode();
    msg.hash_join_node.join_op = joinOp.toThrift();
    for (Pair<Expr, Expr> entry: eqJoinConjuncts) {
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(entry.first.treeToThrift(), entry.second.treeToThrift());
      msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
    }
    for (Expr e: otherJoinConjuncts) {
      msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
    }
  }

  @Override
  protected String getNodeExplainString(String detailPrefix,
      TExplainLevel detailLevel) {
    String distrModeStr = (distrMode != DistributionMode.NONE) ?
        (" (" + distrMode.toString() + ")") : "";
    StringBuilder output = new StringBuilder()
      .append(detailPrefix + "join op: " + joinOp.toString() + distrModeStr + "\n")
      .append(detailPrefix + "hash predicates:\n");
    for (Pair<Expr, Expr> entry: eqJoinConjuncts) {
      output.append(detailPrefix + "  " +
          entry.first.toSql() + " = " + entry.second.toSql() + "\n");
    }
    if (!otherJoinConjuncts.isEmpty()) {
      output.append(detailPrefix + "other join predicates: ")
          .append(getExplainString(otherJoinConjuncts) + "\n");
    }
    if (!conjuncts.isEmpty()) {
      output.append(detailPrefix + "other predicates: ")
          .append(getExplainString(conjuncts) + "\n");
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes == 0) {
      perHostMemCost = DEFAULT_PER_HOST_MEM;
      return;
    }
    perHostMemCost = (long) Math.ceil(getChild(1).cardinality * getChild(1).avgRowSize *
        Planner.HASH_TBL_SPACE_OVERHEAD);
    if (distrMode == DistributionMode.PARTITIONED) perHostMemCost /= numNodes;
  }
}
