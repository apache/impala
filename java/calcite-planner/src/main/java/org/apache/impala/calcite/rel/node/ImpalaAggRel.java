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
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.calcite.functions.AnalyzedFunctionCallExpr;
import org.apache.impala.calcite.functions.AnalyzedNullLiteral;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.AggregationNode;
import org.apache.impala.planner.CardinalityCheckNode;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaAggRel extends Aggregate
    implements ImpalaPlanRel {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaAggRel.class.getName());

  public ImpalaAggRel(Aggregate agg) {
    super(agg.getCluster(), agg.getTraitSet(), agg.getHints(), agg.getInput(),
        agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
  }

  public ImpalaAggRel(RelOptCluster cluster, RelTraitSet relTraitSet,
      List<RelHint> hints, RelNode input, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, relTraitSet, hints, input, groupSet, groupSets, aggCalls);
  }

  /**
   * Convert the Aggregation Rel Node into an Impala Plan Node.
   * Impala has its aggregate structure called MultiAggregateInfo. This structure
   * needs to be analyzed before converting it into the Impala aggregate node.
   * After analyzing, the final output expressions are retrieved through the
   * AggregateInfo structure.
   */
  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    Preconditions.checkState(getInputs().size() == 1);
    NodeWithExprs inputWithExprs = getChildPlanNode(context);
    PlanNode input = inputWithExprs.planNode_;
    Preconditions.checkState(
        context.ctx_.getRootAnalyzer() instanceof SimplifiedAnalyzer);
    SimplifiedAnalyzer simplifiedAnalyzer =
        (SimplifiedAnalyzer) context.ctx_.getRootAnalyzer();

    if (isCardinalityCheckRelNode()) {
      return getCardinalityCheckNode(inputWithExprs, context.ctx_);
    }

    List<Expr> groupingExprs = getGroupingExprs(inputWithExprs.outputExprs_);
    List<FunctionCallExpr> aggExprs = getAggregateExprs(context.ctx_,
        inputWithExprs.outputExprs_, simplifiedAnalyzer);
    List<List<Expr>> groupingSets =
        getGroupingSets(simplifiedAnalyzer, inputWithExprs.outputExprs_);

    // Impala's MultiAggregateInfo encapsulates functionality to represent
    // aggregation functions and grouping exprs belonging to multiple
    // aggregation classes - such as in the case of multiple DISTINCT
    // aggregates or multiple Grouping Sets.
    MultiAggregateInfo multiAggInfo =
        new MultiAggregateInfo(groupingExprs, aggExprs, groupingSets);
    multiAggInfo.analyze(simplifiedAnalyzer);

    // Impala pushes expressions up the stack, but Calcite has already done this.
    // So all expressions generated by Calcite are materialized and this method
    // takes care of that.
    multiAggInfo.materializeRequiredSlots(simplifiedAnalyzer, new ExprSubstitutionMap());

    AggregateInfo aggInfo = multiAggInfo.hasTransposePhase()
        ? multiAggInfo.getTransposeAggInfo()
        : multiAggInfo.getAggClasses().get(0);

    // TODO: IMPALA-12959 need to code Parquet CountStar Optimization

    PlanNode aggNode = getTopLevelAggNode(input, multiAggInfo, context.ctx_);

    List<Expr> outputExprs = createMappedOutputExprs(multiAggInfo, groupingExprs,
        aggExprs, aggInfo.getResultTupleDesc().getSlots());

    // This is the only way to shove in the "having" filter into the aggregate node.
    // In the init clause, the aggregate node calls into the analyzer to get all remaining
    // unassigned conjuncts. This should be refactored to remove analyzer dependence
    if (context.filterCondition_ != null) {
      ExprConjunctsConverter converter = new ExprConjunctsConverter(
          context.filterCondition_, outputExprs, getCluster().getRexBuilder(),
          simplifiedAnalyzer);
      simplifiedAnalyzer.setUnassignedConjuncts(converter.getImpalaConjuncts());
    }
    aggNode.init(simplifiedAnalyzer);
    simplifiedAnalyzer.clearUnassignedConjuncts();

    return new NodeWithExprs(aggNode, outputExprs, inputWithExprs);
  }

  private NodeWithExprs getChildPlanNode(ParentPlanRelContext context
      ) throws ImpalaException {
    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput(0);
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    // filter condition handled by agg node, so no need to pass it to the child.
    builder.setFilterCondition(null);
    return relInput.getPlanNode(builder.build());
  }

  private List<Expr> getGroupingExprs(List<Expr> inputExprs) {
    List<Expr> exprs = Lists.newArrayList();

    for (int groupIndex : super.getGroupSet()) {
      exprs.add(inputExprs.get(groupIndex));
    }
    return exprs;
  }

  /**
   * getGroupingSets retrieves a list of a list of expressions representing each group
   * in a grouping set.
   * An expression is generated for every group referenced, regardless of whether it is
   * in the grouping subset. If a group is not present in the grouping set, a NullLiteral
   * placeholder is needed for that group with the proper expression type.
   */
  private List<List<Expr>> getGroupingSets(Analyzer analyzer, List<Expr> inputExprs
      ) throws ImpalaException {
    if (Aggregate.isSimple(this)) {
      return null;
    }

    List<List<Expr>> allGroupSetExprs = Lists.newArrayList();

    ImmutableList<ImmutableBitSet> groupSets = super.getGroupSets();
    if (groupSets.size() == 0) {
      return allGroupSetExprs;
    }

    // gather all types for all groups. The types are needed for groups not used
    // in a grouping set as placeholders with a NullLiteral.
    ImmutableBitSet groupSet = super.getGroupSet();

    // create the map of the group field number to its type
    Map<Integer, Type> gbExprTypes = groupSet.asList().stream()
        .collect(Collectors.toMap(groupByField -> groupByField,
            groupByField -> inputExprs.get(groupByField).getType()));

    // Gather each group set
    for (int i = 0; i < groupSets.size(); i++) {
      ImmutableBitSet presentGbFields = groupSets.get(i);
      Map<Integer, Expr> oneGroupExprs = new LinkedHashMap<>();
      // each group set needs an Expr for all groups whether they are in the
      /// group set or not.
      for (int groupByField : groupSet) {
        if (presentGbFields.get(groupByField)) {
          oneGroupExprs.put(groupByField, inputExprs.get(groupByField));
        } else {
          // fill missing slots with null with the appropriate data type
          Type nullType = gbExprTypes.get(groupByField);
          AnalyzedNullLiteral nullLiteral = new AnalyzedNullLiteral(nullType);
          nullLiteral.analyze(analyzer);
          oneGroupExprs.put(groupByField, nullLiteral);
        }
      }
      allGroupSetExprs.add(new ArrayList<>(oneGroupExprs.values()));
    }
    return allGroupSetExprs;
  }

  /**
   * Get the top level agg node and create the needed agg nodes along the way.
   * Impala can break up the aggregation node up into multiple phases. This
   * code is similar to code that is found in SingleNodePlanner with the exception
   * that it creates ImpalaAggNode, a subclass of AggregationNode.
   */
  private PlanNode getTopLevelAggNode(PlanNode input, MultiAggregateInfo multiAggInfo,
      PlannerContext ctx) throws ImpalaException{
    Analyzer analyzer = ctx.getRootAnalyzer();

    AggregationNode firstPhaseAgg = new AggregationNode(ctx.getNextNodeId(), input,
        multiAggInfo, MultiAggregateInfo.AggPhase.FIRST);

    if (!multiAggInfo.hasSecondPhase() && !multiAggInfo.hasTransposePhase()) {
      // caller will call the "init" method
      return firstPhaseAgg;
    }

    firstPhaseAgg.init(analyzer);
    firstPhaseAgg.setIntermediateTuple();

    AggregationNode secondPhaseAgg = null;
    if (multiAggInfo.hasSecondPhase()) {
      firstPhaseAgg.unsetNeedsFinalize();
      // A second phase aggregation is needed when there is an aggregation on two
      // different groups but Calcite produces a single aggregation RelNode
      // (e.g. select count(distinct c1), min(c2) from tbl).
      secondPhaseAgg = new AggregationNode(ctx.getNextNodeId(), firstPhaseAgg,
          multiAggInfo, MultiAggregateInfo.AggPhase.SECOND);
      if (!multiAggInfo.hasTransposePhase()) {
        // caller will call the "init" method
        return secondPhaseAgg;
      }
      secondPhaseAgg.init(analyzer);
    }

    AggregationNode transposePhaseAgg = firstPhaseAgg;
    if (multiAggInfo.hasTransposePhase()) {
      AggregationNode inputAgg = secondPhaseAgg != null ? secondPhaseAgg : firstPhaseAgg;
      // A transpose aggregation is needed for grouping sets
      transposePhaseAgg = new AggregationNode(ctx.getNextNodeId(), inputAgg, multiAggInfo,
          MultiAggregateInfo.AggPhase.TRANSPOSE);
    }
    // caller will call the "init" method
    return transposePhaseAgg;
  }

  private List<FunctionCallExpr> getAggregateExprs(PlannerContext ctx,
      List<Expr> inputExprs, Analyzer analyzer) throws ImpalaException ,
      AnalysisException {
    List<FunctionCallExpr> exprs = Lists.newArrayList();
    ImpalaPlanRel input = (ImpalaPlanRel) getInput(0);
    for (AggregateCall aggCall : getAggCallList()) {
      List<Expr> operands = aggCall.getArgList().stream()
          .map(t -> inputExprs.get(t))
          .collect(Collectors.toList());

      Function fn = getFunction(ctx, aggCall);
      Preconditions.checkState(fn != null, "Could not find the Impala function for " +
          aggCall.getAggregation().getName());

      Type impalaRetType = ImpalaTypeConverter.createImpalaType(aggCall.getType());

      FunctionParams params = new FunctionParams(aggCall.isDistinct(), operands);

      FunctionCallExpr e = new AnalyzedFunctionCallExpr(fn, params, impalaRetType);
      e.analyze(analyzer);

      exprs.add(e);
    }
    return exprs;
  }

  private Function getFunction(PlannerContext ctx, AggregateCall aggCall)
      throws ImpalaException {
    RelDataType retType = aggCall.getType();
    SqlAggFunction aggFunction = aggCall.getAggregation();
    List<RelDataType> operandTypes = Lists.newArrayList();
    RelNode input = getInput(0);
    for (int i : aggCall.getArgList()) {
      RelDataType relDataType = input.getRowType().getFieldList().get(i).getType();
      operandTypes.add(relDataType);
    }
    return FunctionResolver.getExactFunction(aggFunction.getName(), aggFunction.getKind(),
        operandTypes);
  }

  private boolean isCardinalityCheckRelNode() {
    return getAggCallList().size() == 1 &&
        getAggCallList().get(0).getAggregation() instanceof SqlSingleValueAggFunction;
  }

  private NodeWithExprs getCardinalityCheckNode(NodeWithExprs inputNodeWithExprs,
      PlannerContext ctx) throws ImpalaException {
    // Not too thrilled with this hack.  We're mutating the input plan node. The way
    // to do this properly is to have getPlanNode() take this as an input and make
    // sure all PlanNode constructors can take some sort of context object. But this
    // is a bit too intrusive in the current iteration of this change.  A todo.
    inputNodeWithExprs.planNode_.setLimit(2);

    int inputRef = getAggCallList().get(0).getArgList().get(0);
    // output expressions do not change from the input
    List<Expr> outputExprs =
        ImmutableList.of(inputNodeWithExprs.outputExprs_.get(inputRef));

    CardinalityCheckNode cardinalityCheckNode = new CardinalityCheckNode(
        ctx.getNextNodeId(), inputNodeWithExprs.planNode_, "CARDINALITY CHECK");

    cardinalityCheckNode.init(ctx.getRootAnalyzer());

    return new NodeWithExprs(cardinalityCheckNode, outputExprs, inputNodeWithExprs);
  }

  public Aggregate copy(RelTraitSet relTraitSet, RelNode relNode,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new ImpalaAggRel(getCluster(), relTraitSet, new ArrayList<>(), relNode,
        groupSet, groupSets, aggCalls);
  }

  /**
   * Create the output expressions for the ImpalaAggregateRel.
   * Impala can change the order in their MultiAggInfo object. So the SlotDescriptors
   * do not necessarily line up with the indexes.   So we need to walk through the
   * grouping expressions and the aggExprs of the agg node and match them up with the
   * corresponding SlotRef. If there is a groupingId column, that slot will be at the end.
   */
  public List<Expr> createMappedOutputExprs(MultiAggregateInfo multiAggInfo,
      List<Expr> groupingExprs, List<FunctionCallExpr> aggExprs,
      List<SlotDescriptor> slotDescs) {
    ImmutableList.Builder<Expr> builder = new ImmutableList.Builder();
    int numSlots = groupingExprs.size() + aggExprs.size();

    int index = 0;

    for (Expr e : groupingExprs) {
      Expr slotRefExpr = multiAggInfo.getOutputSmap().get(e);
      Preconditions.checkNotNull(slotRefExpr);
      builder.add(slotRefExpr);
    }

    for (FunctionCallExpr e : aggExprs) {
      Expr slotRefExpr = multiAggInfo.getOutputSmap().get(e);
      Preconditions.checkNotNull(slotRefExpr);
      builder.add(slotRefExpr);
    }

    return builder.build();
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.AGGREGATE;
  }
}
