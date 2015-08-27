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
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
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
 * Hash join between left child (outer) and right child (inner). One child must be the
 * plan generated for a table ref. Typically, that is the right child, but due to join
 * inversion (for outer/semi/cross joins) it could also be the left child.
 */
public class HashJoinNode extends JoinNode {
  private final static Logger LOG = LoggerFactory.getLogger(HashJoinNode.class);

  // If true, this node can add filters for the probe side that can be generated
  // after reading the build side. This can be very helpful if the join is selective and
  // there are few build rows.
  private boolean addProbeFilters_;

  public HashJoinNode(
      PlanNode outer, PlanNode inner, DistributionMode distrMode, JoinOperator joinOp,
      List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
    super(outer, inner, distrMode, joinOp, eqJoinConjuncts, otherJoinConjuncts,
        "HASH JOIN");
    Preconditions.checkNotNull(eqJoinConjuncts);
    Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
  }

  public List<BinaryPredicate> getEqJoinConjuncts() { return eqJoinConjuncts_; }
  public void setAddProbeFilters(boolean b) { addProbeFilters_ = true; }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    List<BinaryPredicate> newEqJoinConjuncts = Lists.newArrayList();
    ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
    for (Expr c: eqJoinConjuncts_) {
      BinaryPredicate eqPred =
          (BinaryPredicate) c.substitute(combinedChildSmap, analyzer, false);
      Type t0 = eqPred.getChild(0).getType();
      Type t1 = eqPred.getChild(1).getType();
      if (!t0.matchesType(t1)) {
        // With decimal and char types, the child types do not have to match because
        // the equality builtin handles it. However, they will not hash correctly so
        // insert a cast.
        boolean bothDecimal = t0.isDecimal() && t1.isDecimal();
        boolean bothString = t0.isStringType() && t1.isStringType();
        if (!bothDecimal && !bothString) {
          throw new InternalException("Cannot compare " +
              t0.toSql() + " to " + t1.toSql() + " in join predicate.");
        }
        Type compatibleType = Type.getAssignmentCompatibleType(t0, t1, false);
        Preconditions.checkState(compatibleType.isDecimal() ||
            compatibleType.isStringType());
        try {
          if (!t0.equals(compatibleType)) {
            eqPred.setChild(0, eqPred.getChild(0).castTo(compatibleType));
          }
          if (!t1.equals(compatibleType)) {
            eqPred.setChild(1, eqPred.getChild(1).castTo(compatibleType));
          }
        } catch (AnalysisException e) {
          throw new InternalException("Should not happen", e);
        }
      }
      Preconditions.checkState(
          eqPred.getChild(0).getType().matchesType(eqPred.getChild(1).getType()));
      newEqJoinConjuncts.add(new BinaryPredicate(eqPred.getOp(),
          eqPred.getChild(0), eqPred.getChild(1)));
    }
    eqJoinConjuncts_ = newEqJoinConjuncts;
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("eqJoinConjuncts_", eqJoinConjunctsDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String eqJoinConjunctsDebugString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    for (Expr entry: eqJoinConjuncts_) {
      helper.add("lhs" , entry.getChild(0)).add("rhs", entry.getChild(1));
    }
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.hash_join_node = new THashJoinNode();
    msg.hash_join_node.join_op = joinOp_.toThrift();
    for (Expr entry: eqJoinConjuncts_) {
      BinaryPredicate bp = (BinaryPredicate)entry;
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(bp.getChild(0).treeToThrift(),
              bp.getChild(1).treeToThrift(),
              bp.getOp() == BinaryPredicate.Operator.NOT_DISTINCT);
      msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
    }
    for (Expr e: otherJoinConjuncts_) {
      msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
    }
    msg.hash_join_node.setAdd_probe_filters(addProbeFilters_);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));

    if (detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(detailPrefix + "hash predicates: ");
      for (int i = 0; i < eqJoinConjuncts_.size(); ++i) {
        Expr eqConjunct = eqJoinConjuncts_.get(i);
        output.append(eqConjunct.toSql());
        if (i + 1 != eqJoinConjuncts_.size()) output.append(", ");
      }
      output.append("\n");
      if (!otherJoinConjuncts_.isEmpty()) {
        output.append(detailPrefix + "other join predicates: ")
        .append(getExplainString(otherJoinConjuncts_) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "other predicates: ")
        .append(getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes_ == 0) {
      perHostMemCost_ = DEFAULT_PER_HOST_MEM;
      return;
    }
    perHostMemCost_ =
        (long) Math.ceil(getChild(1).cardinality_ * getChild(1).avgRowSize_
          * PlannerContext.HASH_TBL_SPACE_OVERHEAD);
    if (distrMode_ == DistributionMode.PARTITIONED) perHostMemCost_ /= numNodes_;
  }
}
