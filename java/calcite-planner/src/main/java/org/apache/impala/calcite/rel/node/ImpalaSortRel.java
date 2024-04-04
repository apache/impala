/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.calcite.rel.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.SortNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaSortRel
 *
 * IMPALA-13172: Optimizations for tryConvertToTopN needed. This should probably
 * be made rule based and done in the optimization phase, but keeping track
 * here because the Impala code was in the SortNode.
 */
public class ImpalaSortRel extends Sort
    implements ImpalaPlanRel {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaSortRel.class.getName());

  private final long limit_;
  private final long offset_;

  public ImpalaSortRel(Sort sort) {
    super(sort.getCluster(), sort.getTraitSet(), sort.getHints(), sort.getInput(),
        sort.getCollation(), sort.offset, sort.fetch);
    limit_ = this.fetch != null ?
        ((BigDecimal) RexLiteral.value(this.fetch)).longValue() : -1L;
    offset_ = this.offset != null ?
        ((BigDecimal) RexLiteral.value(this.offset)).longValue() : 0L;
  }

  private ImpalaSortRel(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, new ArrayList<>(), newInput, newCollation, offset, fetch);
    limit_ = this.fetch != null ?
        ((BigDecimal) RexLiteral.value(this.fetch)).longValue() : -1L;
    offset_ = this.offset != null ?
        ((BigDecimal) RexLiteral.value(this.offset)).longValue() : 0L;
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
      RexNode offset, RexNode fetch) {
    return new ImpalaSortRel(getCluster(), traitSet, newInput, newCollation,
        offset, fetch);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    if (limit_ == 0) {
      return NodeCreationUtils.createEmptySetPlanNode(
          context.ctx_.getNextNodeId(), context.ctx_.getRootAnalyzer(), getRowType());
    }

    List<RelFieldCollation> fieldCollations = getCollation().getFieldCollations();

    NodeWithExprs inputNodeWithExprs = getChildPlanNode(context);

    // If there's limit without order-by, we don't need to generate
    // a sort or top-n node..just set the limit on the child
    if (fieldCollations.size() == 0) {
      validateUnorderedLimit(context.filterCondition_, limit_, offset_);
      // Mutating an existing object here. Either we should pass in a context into
      // all PlanNodes containing the limit so the PlanNode constructor can set the
      // limit, or leave the code here to mutate.
      inputNodeWithExprs.planNode_.setLimit(limit_);
      return inputNodeWithExprs;
    }

    List<Expr> inputExprs = inputNodeWithExprs.outputExprs_;

    List<Boolean> isAscOrder = fieldCollations.stream()
        .map(t -> (t.direction == RelFieldCollation.Direction.ASCENDING))
        .collect(Collectors.toList());
    List<Boolean> nullsFirstParams = fieldCollations.stream()
        .map(t -> (t.nullDirection == RelFieldCollation.NullDirection.FIRST))
        .collect(Collectors.toList());
    List<Expr> sortExprs = fieldCollations.stream()
        .map(t -> getExpr(inputExprs, t.getFieldIndex()))
        .collect(Collectors.toList());

    SortInfo sortInfo = new SortInfo(sortExprs, isAscOrder, nullsFirstParams);
    sortInfo.createSortTupleInfo(inputNodeWithExprs.outputExprs_,
        context.ctx_.getRootAnalyzer());

    // createOutputExprs also makes the slot materialized, so call this before calling
    // materializeRequiredSlots because the latter checks for the isMaterialized flag
    List<Expr> outputExprs = createOutputExprs(sortInfo, inputNodeWithExprs.outputExprs_,
        context.ctx_.getRootAnalyzer());

    sortInfo.materializeRequiredSlots(context.ctx_.getRootAnalyzer(),
        new ExprSubstitutionMap());

    // Call a specific implementation of createSortNode(). In the future, we could
    // try to leverage Impala's SingleNodePlanner.createSortNode()
    SortNode sortNode = createSortNode(context.ctx_,
        inputNodeWithExprs.planNode_, sortInfo, limit_, offset_, limit_ != -1,
        context.ctx_.getRootAnalyzer());

    NodeWithExprs retNode = new NodeWithExprs(sortNode, outputExprs, inputNodeWithExprs);

    // If there is a filter condition, a SelectNode will get added on top
    // of the retNode.
    return NodeCreationUtils.wrapInSelectNodeIfNeeded(context, retNode,
        getCluster().getRexBuilder());
  }

  private NodeWithExprs getChildPlanNode(ParentPlanRelContext context
      ) throws ImpalaException {
    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput(0);
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    builder.setFilterCondition(null);
    return relInput.getPlanNode(builder.build());
  }

  private Expr getExpr(List<Expr> exprs, int index) {
    return exprs.get(index);
  }

  private void validateUnorderedLimit(RexNode filterCondition, long limit, long offset) {
    // The filter should have been pushed through by the optimizer. This is verified
    // here because there is no code to support pushing the filter condition if it
    // hits this code.
    Preconditions.checkArgument(filterCondition == null);
    Preconditions.checkArgument(limit_ > 0);
    Preconditions.checkArgument(offset_ == 0);
  }

  /**
   * Create the output expressions for the SortRel.
   * Impala can change the order in their SortInfo object. So the SlotDescriptors
   * do not necessarily line up with the indexes. So we need to walk through the
   * expressions of the input node and match them up with the corresponding SlotRef.
   */
  public List<Expr> createOutputExprs(SortInfo sortInfo,
      List<Expr> outputExprs, Analyzer analyzer) throws AnalysisException  {
    ImmutableList.Builder<Expr> builder = new ImmutableList.Builder();

    // project all columns coming from the child since Sort does not change
    // any projections but do the mapping based on its own substitution map
    for (Expr outputExpr : outputExprs) {
      builder.add(outputExpr.trySubstitute(sortInfo.getOutputSmap(),
          analyzer, true));
    }

    for (SlotDescriptor slotDesc : sortInfo.getSortTupleDescriptor().getSlots()) {
      slotDesc.setIsMaterialized(true);
    }

    return builder.build();
  }


  /**
   * Creates and initializes either a SortNode or a TopNNode depending on various
   * heuristics and configuration parameters.
   */

  public static SortNode createSortNode(PlannerContext planCtx, PlanNode root,
      SortInfo sortInfo, long limit, long offset, boolean hasLimit,
      Analyzer analyzer) throws ImpalaException {
    SortNode sortNode = createSortNode(planCtx, root, sortInfo, limit, offset, hasLimit);
    Preconditions.checkState(sortNode.hasValidStats());
    sortNode.setLimit(limit);
    sortNode.init(analyzer);
    return sortNode;
  }

  /**
   * Creates and initializes either a SortNode or a TopNNode depending on various
   * heuristics and configuration parameters.
   */
  public static SortNode createSortNode(PlannerContext planCtx, PlanNode root,
      SortInfo sortInfo, long limit, long offset, boolean hasLimit
      ) throws ImpalaException {

    if (!hasLimit) {
      return SortNode.createTotalSortNode(planCtx.getNextNodeId(), root, sortInfo,
          offset);
    }

    return SortNode.createTopNSortNode(planCtx.getQueryOptions(), planCtx.getNextNodeId(),
        root, sortInfo, offset, limit, false);
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.SORT;
  }
}
