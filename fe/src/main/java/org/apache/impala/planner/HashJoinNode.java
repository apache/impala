// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TEqJoinCondition;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THashJoinNode;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.BitUtil;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Hash join between left child (outer) and right child (inner). One child must be the
 * plan generated for a table ref. Typically, that is the right child, but due to join
 * inversion (for outer/semi/cross joins) it could also be the left child.
 */
public class HashJoinNode extends JoinNode {
  private final static Logger LOG = LoggerFactory.getLogger(HashJoinNode.class);

  // Coefficients for estimating hash join CPU processing cost.  Derived from
  // benchmarking. Probe side cost per input row consumed
  private static final double COST_COEFFICIENT_PROBE_INPUT = 0.2565;
  // Probe side cost per output row produced
  private static final double COST_COEFFICIENT_HASH_JOIN_OUTPUT = 0.1812;
  // Build side cost per input row consumed
  private static final double COST_COEFFICIENT_BUILD_INPUT = 1.0;

  public HashJoinNode(PlanNode outer, PlanNode inner, boolean isStraightJoin,
      DistributionMode distrMode, JoinOperator joinOp,
      List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
    super(outer, inner, isStraightJoin, distrMode, joinOp, eqJoinConjuncts,
        otherJoinConjuncts, "HASH JOIN");
    Preconditions.checkNotNull(eqJoinConjuncts);
    Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
    Preconditions.checkState(joinOp_ != JoinOperator.ICEBERG_DELETE_JOIN);
  }

  @Override
  public boolean isBlockingJoinNode() { return true; }

  @Override
  public List<BinaryPredicate> getEqJoinConjuncts() { return eqJoinConjuncts_; }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    List<BinaryPredicate> newEqJoinConjuncts = new ArrayList<>();
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
        Type compatibleType = Type.getAssignmentCompatibleType(
            t0, t1, analyzer.getRegularCompatibilityLevel());
        if (compatibleType.isInvalid()) {
          throw new InternalException(String.format(
              "Unable create a hash join with equi-join predicate %s " +
              "because the operands cannot be cast without loss of precision. " +
              "Operand types: %s = %s.", eqPred.toSql(), t0.toSql(), t1.toSql()));
        }
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
      BinaryPredicate newEqPred = new BinaryPredicate(eqPred.getOp(),
          eqPred.getChild(0), eqPred.getChild(1));
      newEqPred.analyze(analyzer);
      newEqJoinConjuncts.add(newEqPred);
    }
    eqJoinConjuncts_ = newEqJoinConjuncts;
    orderJoinConjunctsByCost();
    computeStats(analyzer);
  }

  @Override
  protected String debugString() {
    return MoreObjects.toStringHelper(this)
        .add("eqJoinConjuncts_", eqJoinConjunctsDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String eqJoinConjunctsDebugString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (Expr entry: eqJoinConjuncts_) {
      helper.add("lhs" , entry.getChild(0)).add("rhs", entry.getChild(1));
    }
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.join_node = joinNodeToThrift();
    msg.join_node.hash_join_node = new THashJoinNode();
    msg.join_node.hash_join_node.setEq_join_conjuncts(getThriftEquiJoinConjuncts());
    for (Expr e: otherJoinConjuncts_) {
      msg.join_node.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
    }
    msg.join_node.hash_join_node.setHash_seed(getFragment().getHashSeed());
  }

  /**
   * Helper to get the equi-join conjuncts in a thrift representation.
   */
  public List<TEqJoinCondition> getThriftEquiJoinConjuncts() {
    List<TEqJoinCondition> equiJoinConjuncts = new ArrayList<>(eqJoinConjuncts_.size());
    for (Expr entry : eqJoinConjuncts_) {
      BinaryPredicate bp = (BinaryPredicate)entry;
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(bp.getChild(0).treeToThrift(),
              bp.getChild(1).treeToThrift(),
              bp.getOp() == BinaryPredicate.Operator.NOT_DISTINCT);

      equiJoinConjuncts.add(eqJoinCondition);
    }
    return equiJoinConjuncts;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));

    if (detailLevel.ordinal() > TExplainLevel.STANDARD.ordinal()) {
      if (joinTableId_.isValid()) {
        output.append(
            detailPrefix + "hash-table-id=" + joinTableId_.toString() + "\n");
      }
    }
    if (detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      if (!isDeleteRowsJoin_ ||
          detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
        output.append(detailPrefix + "hash predicates: ");
        for (int i = 0; i < eqJoinConjuncts_.size(); ++i) {
          Expr eqConjunct = eqJoinConjuncts_.get(i);
          output.append(eqConjunct.toSql());
          if (i + 1 != eqJoinConjuncts_.size()) output.append(", ");
        }
        output.append("\n");
      }

      // Optionally print FK/PK equi-join conjuncts.
      if (joinOp_.isInnerJoin() || joinOp_.isOuterJoin()) {
        if (detailLevel.ordinal() > TExplainLevel.STANDARD.ordinal()) {
          output.append(detailPrefix + "fk/pk conjuncts: ");
          if (fkPkEqJoinConjuncts_ == null) {
            output.append("none");
          } else if (fkPkEqJoinConjuncts_.isEmpty()) {
            output.append("assumed fk/pk");
          } else {
            output.append(Joiner.on(", ").join(fkPkEqJoinConjuncts_));
          }
          output.append("\n");
        }
      }

      if (!otherJoinConjuncts_.isEmpty()) {
        output.append(detailPrefix + "other join predicates: ")
            .append(Expr.getExplainString(otherJoinConjuncts_, detailLevel) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "other predicates: ")
            .append(Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(getRuntimeFilterExplainString(true, detailLevel));
      }
    }
    return output.toString();
  }

  /**
   * Helper method to compute the resource requirements for the join that can be
   * called from the builder or the join node. Returns a pair of the probe
   * resource requirements and the build resource requirements.
   */
  @Override
  public Pair<ResourceProfile, ResourceProfile> computeJoinResourceProfile(
      TQueryOptions queryOptions) {
    long perBuildInstanceMemEstimate;
    long perBuildInstanceDataBytes;
    int numInstances = fragment_.getNumInstances();
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numInstances <= 0) {
      perBuildInstanceMemEstimate = DEFAULT_PER_INSTANCE_MEM;
      perBuildInstanceDataBytes = -1;
    } else {
      long rhsCard = getChild(1).getCardinality();
      long rhsNdv = 1;
      // Calculate the ndv of the right child, which is the multiplication of
      // the ndv of the right child column
      for (Expr eqJoinPredicate: eqJoinConjuncts_) {
        long rhsPdNdv = getNdv(eqJoinPredicate.getChild(1));
        rhsPdNdv = Math.min(rhsPdNdv, rhsCard);
        if (rhsPdNdv != -1) {
          rhsNdv = PlanNode.checkedMultiply(rhsNdv, rhsPdNdv);
        }
      }
      // The memory of the data stored in hash table and
      // the memory of the hash table‘s structure
      perBuildInstanceDataBytes = (long) Math.ceil(rhsCard * getChild(1).getAvgRowSize() +
          BitUtil.roundUpToPowerOf2((long) Math.ceil(3 * rhsCard / 2)) *
          PlannerContext.SIZE_OF_BUCKET);
      if (rhsNdv > 1 && rhsNdv < rhsCard) {
        perBuildInstanceDataBytes += (rhsCard - rhsNdv) *
            PlannerContext.SIZE_OF_DUPLICATENODE;
      }
      // Assume the rows are evenly divided among instances.
      if (distrMode_ == DistributionMode.PARTITIONED) {
        perBuildInstanceDataBytes /= numInstances;
      }
      perBuildInstanceMemEstimate = perBuildInstanceDataBytes;
    }

    // Must be kept in sync with PartitionedHashJoinBuilder::MinReservation() in be.
    final int PARTITION_FANOUT = 16;
    long minBuildBuffers = PARTITION_FANOUT + 1
        + (joinOp_ == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN ? 1 : 0);

    long bufferSize = queryOptions.getDefault_spillable_buffer_size();
    if (perBuildInstanceDataBytes != -1) {
      long bytesPerBuffer = perBuildInstanceDataBytes / PARTITION_FANOUT;
      // Scale down the buffer size if we think there will be excess free space with the
      // default buffer size, e.g. if the right side is a small dimension table.
      bufferSize = Math.min(bufferSize, Math.max(
          queryOptions.getMin_spillable_buffer_size(),
          BitUtil.roundUpToPowerOf2(bytesPerBuffer)));
    }

    // Two of the buffers need to be buffers large enough to hold the maximum-sized row
    // to serve as input and output buffers while repartitioning.
    long maxRowBufferSize =
        computeMaxSpillableBufferSize(bufferSize, queryOptions.getMax_row_size());
    long perInstanceBuildMinMemReservation =
        bufferSize * (minBuildBuffers - 2) + maxRowBufferSize * 2;
    if (Planner.useMTFragment(queryOptions) && canShareBuild()) {
      // Ensure we reserve enough memory to hand off to the PartitionedHashJoinNodes for
      // probe streams when spilling. numInstancePerHost is an upper bound on the number
      // of PartitionedHashJoinNodes per builder.
      // TODO: IMPALA-9416: be less conservative here
      // TODO: In case of compute_processing_cost=false, it is probably OK to stil use
      //     getNumInstancesPerHost(). Leave this for future work to avoid regression.
      int numInstancePerHost = queryOptions.compute_processing_cost ?
          fragment_.getNumInstancesPerHost(queryOptions) :
          queryOptions.getMt_dop();
      perInstanceBuildMinMemReservation *= numInstancePerHost;
    }
    // Most reservation for probe buffers is obtained from the join builder when
    // spilling. However, for NAAJ, two additional probe streams are needed that
    // are used exclusively by the probe side.
    long perInstanceProbeMinMemReservation = 0;
    if (joinOp_ == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
      // Only one of the NAAJ probe streams is written at a time, so it needs a max-sized
      // buffer. If the build is integrated, we already have a max sized buffer accounted
      // for in the build reservation.
      perInstanceProbeMinMemReservation =
          hasSeparateBuild() ? maxRowBufferSize + bufferSize : bufferSize * 2;
    }

    // Almost all resource consumption is in the build, or shared between the build and
    // the probe. These are accounted for in the build profile.
    ResourceProfile probeProfile = new ResourceProfileBuilder()
        .setMemEstimateBytes(0)
        .setMinMemReservationBytes(perInstanceProbeMinMemReservation)
        .setSpillableBufferBytes(bufferSize)
        .setMaxRowBufferBytes(maxRowBufferSize).build();
    ResourceProfile buildProfile = new ResourceProfileBuilder()
        .setMemEstimateBytes(perBuildInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceBuildMinMemReservation)
        .setSpillableBufferBytes(bufferSize)
        .setMaxRowBufferBytes(maxRowBufferSize).build();
    return Pair.create(probeProfile, buildProfile);
  }

  @Override
  public Pair<ProcessingCost, ProcessingCost> computeJoinProcessingCost() {
    // Compute the processing cost for lhs. Benchmarking suggests we can estimate the
    // probe cost as a linear function combining the probe input cardinality and the
    // estimated output cardinality.
    long outputCardinality = Math.max(0, getCardinality());
    double totalProbeCost =
        (getProbeCardinalityForCosting() * COST_COEFFICIENT_PROBE_INPUT)
        + (outputCardinality * COST_COEFFICIENT_HASH_JOIN_OUTPUT);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Probe CPU cost estimate: " + totalProbeCost + ", Input Card: "
          + getProbeCardinalityForCosting() + ", Output Card: " + outputCardinality);
    }
    ProcessingCost probeProcessingCost =
        ProcessingCost.basicCost(getDisplayLabel(), totalProbeCost);

    // Compute the processing cost for rhs.
    // TODO: For broadcast join builds we're underestimating cost here because we're using
    // the overall plan cardinality without accounting for the fact that the broadcast
    // effectively multiplies the cardinality by the number of hosts.
    // In the end this probably doesn't matter to the CPU resource allocation since the
    // build fragment count is fixed for broadcast(at num hosts) regardless of the cost
    // computed here. But we should clean up the costing here to avoid any future
    // confusion.
    long buildCardinality = Math.max(0, getChild(1).getFilteredCardinality());
    double totalBuildCost = buildCardinality * COST_COEFFICIENT_BUILD_INPUT;
    ProcessingCost buildProcessingCost =
        ProcessingCost.basicCost(getDisplayLabel() + " Build side", totalBuildCost);
    return Pair.create(probeProcessingCost, buildProcessingCost);
  }
}
