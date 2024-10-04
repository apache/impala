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

package org.apache.impala.calcite.rel.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticInfo;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.AnalyticWindow.Boundary;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.calcite.functions.AnalyzedAnalyticExpr;
import org.apache.impala.calcite.functions.AnalyzedFunctionCallExpr;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.functions.RexCallConverter;
import org.apache.impala.calcite.functions.RexLiteralConverter;
import org.apache.impala.calcite.rel.util.CreateExprVisitor;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.AnalyticPlanner;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.SelectNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class ImpalaAnalyticRel extends Project
    implements ImpalaPlanRel {

  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaAnalyticRel.class);

  public ImpalaAnalyticRel(Project project) {
    super(project.getCluster(), project.getTraitSet(), project.getHints(),
        project.getInput(0), project.getProjects(), project.getRowType());
  }

  private ImpalaAnalyticRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, new ArrayList<>(), input, projects, rowType);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
      RelDataType rowType) {
    return new ImpalaAnalyticRel(getCluster(), traitSet, input, projects, rowType);
  }

  /**
   * Convert the Project RelNode with analytic exprs into a Impala Plan Nodes.
   */
  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    List<RexNode> projects = getProjects();
    NodeWithExprs inputNodeWithExprs = getChildPlanNode(context, projects);
    ImpalaPlanRel inputRel = (ImpalaPlanRel) getInput(0);


    // retrieve list of all analytic expressions
    List<RexOver> overExprs = gatherRexOver(projects);

    // get the GroupedAnalyticExpr objects. A GroupedAnalyticExpr object will
    // contain a unique analytic expr and all the RexOver objects which are
    // equivalent to the unique analytic expr.
    List<GroupedAnalyticExpr> groupAnalyticExprs = getGroupedAnalyticExprs(
        overExprs, context.ctx_, inputRel, inputNodeWithExprs.outputExprs_);

    List<AnalyticExpr> analyticExprs = new ArrayList<>();
    for (GroupedAnalyticExpr g : groupAnalyticExprs) {
      analyticExprs.add(g.analyticExpr);
    }

    AnalyticInfo analyticInfo =
        AnalyticInfo.create(analyticExprs, context.ctx_.getRootAnalyzer());
    AnalyticPlanner analyticPlanner =
        new AnalyticPlanner(analyticInfo, context.ctx_.getRootAnalyzer(), context.ctx_);

    PlanNode planNode = analyticPlanner.createSingleNodePlan(
        inputNodeWithExprs.planNode_, Collections.emptyList(), new ArrayList<>());

    // Get a mapping of all expressions to its corresponding Impala Expr object. The
    // non-analytic expressions will have a RexInputRef type RexNode, while the
    // analytic expressions will have the RexOver type RexNode.
    Map<RexNode, Expr> mapping = createRexNodeExprMapping(inputRel, planNode, projects,
        inputNodeWithExprs.outputExprs_, groupAnalyticExprs, context.ctx_, analyticInfo);

    List<Expr> outputExprs =
        getOutputExprs(mapping, projects, context.ctx_.getRootAnalyzer());

    NodeWithExprs retNode = new NodeWithExprs(planNode, outputExprs, inputNodeWithExprs);

    // If there is a filter condition, a SelectNode will get added on top
    // of the retNode.
    return NodeCreationUtils.wrapInSelectNodeIfNeeded(context, retNode,
        getCluster().getRexBuilder());
  }

  private NodeWithExprs getChildPlanNode(ParentPlanRelContext context,
      List<RexNode> projects) throws ImpalaException {
    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput(0);
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    builder.setFilterCondition(null);
    builder.setInputRefs(RelOptUtil.InputFinder.bits(projects, null));
    return relInput.getPlanNode(builder.build());
  }

  /**
   * Generates the AnalyticExpr object from the RexOver and the input plan node
   * expressions.
   */
  private AnalyticExpr getAnalyticExpr(RexOver rexOver, PlannerContext ctx,
      ImpalaPlanRel inputRel, List<Expr> inputExprs) throws ImpalaException {
    final RexWindow rexWindow = rexOver.getWindow();
    // First parameter is the function call
    Function fn = getFunction(rexOver);
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(rexOver.getType());
    CreateExprVisitor visitor = new CreateExprVisitor(getCluster().getRexBuilder(),
        inputExprs, ctx.getRootAnalyzer());

    List<Expr> operands = CreateExprVisitor.getExprs(visitor, rexOver.operands);

    FunctionParams params = new FunctionParams(rexOver.isDistinct(),
        rexOver.ignoreNulls(), operands);
    FunctionCallExpr fnCall = new AnalyzedFunctionCallExpr(fn, params, impalaRetType);
    fnCall.analyze(ctx.getRootAnalyzer());
    fnCall.setIsAnalyticFnCall(true);

    // Second parameter contains the partition expressions
    List<Expr> partitionExprs = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(rexWindow.partitionKeys)) {
      partitionExprs = CreateExprVisitor.getExprs(visitor, rexWindow.partitionKeys);
    }

    // Third parameter contains the sort expressions
    List<OrderByElement> orderByElements = new ArrayList<>();
    if (rexWindow.orderKeys != null) {
      for (RexFieldCollation ok : rexWindow.orderKeys) {
        Expr orderByExpr = CreateExprVisitor.getExpr(visitor, ok.left);
        boolean nullsFirst = ok.getDirection() == RelFieldCollation.Direction.ASCENDING
            ? ok.right.contains(SqlKind.NULLS_FIRST)
            : !ok.right.contains(SqlKind.NULLS_FIRST);
        OrderByElement orderByElement = new OrderByElement(orderByExpr,
            ok.getDirection() == RelFieldCollation.Direction.ASCENDING,
            nullsFirst);
        orderByElements.add(orderByElement);
      }
    }

    // Fourth parameter is the window frame spec.
    // For offset functions like LEAD/LAG, we skip this and let Impala assign
    // the window frame as part of the AnalyticExpr's standardization.
    AnalyticWindow window = null;
    if ((!partitionExprs.isEmpty() || !orderByElements.isEmpty()) &&
        !skipWindowGeneration(fn)) {
      Boundary lBoundary = getWindowBoundary(rexWindow.getLowerBound(),
          ctx, inputRel, visitor);
      Boundary rBoundary = getWindowBoundary(rexWindow.getUpperBound(),
          ctx, inputRel, visitor);
      window = new AnalyticWindow(
          rexWindow.isRows() ? AnalyticWindow.Type.ROWS : AnalyticWindow.Type.RANGE,
          lBoundary, rBoundary);
    }

    AnalyzedAnalyticExpr retExpr = new AnalyzedAnalyticExpr(fnCall, partitionExprs,
        orderByElements, window);
    retExpr.analyze(ctx.getRootAnalyzer());
    return retExpr;
  }

  /**
   * Skip window generation on certain functions. These functions explicitly
   * set the window within AnalyticExpr.standardize
   */
  private boolean skipWindowGeneration(Function fn) {
    return fn.functionName().equals("lag") || fn.functionName().equals("lead") ||
        fn.functionName().equals("row_number");
  }

  private Boundary getWindowBoundary(RexWindowBound wb, PlannerContext ctx,
      ImpalaPlanRel inputRel, CreateExprVisitor visitor) throws ImpalaException {
    // At this stage, Calcite should have filled in the bound
    Preconditions.checkNotNull(wb);
    if (wb.isCurrentRow()) {
      return new Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null);
    } else {
      if (wb.isPreceding()) {
        if (wb.isUnbounded()) {
          return new Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null);
        }
        Expr operand = CreateExprVisitor.getExpr(visitor, wb.getOffset());
        return new Boundary(AnalyticWindow.BoundaryType.PRECEDING, operand,
            new BigDecimal(RexLiteral.intValue(wb.getOffset())));
      } else {
        if (wb.isUnbounded()) {
          return new Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING, null);
        }
        Expr operand = CreateExprVisitor.getExpr(visitor, wb.getOffset());
        return new Boundary(AnalyticWindow.BoundaryType.FOLLOWING, operand,
            new BigDecimal(RexLiteral.intValue(wb.getOffset())));
      }
    }
  }

  private List<Expr> getOutputExprs(Map<RexNode, Expr> mapping,
      List<RexNode> projects, Analyzer analyzer) throws ImpalaException {

    AnalyticRexVisitor visitor = new AnalyticRexVisitor(mapping,
        getCluster().getRexBuilder(), analyzer);

    Map<Integer, Expr> projectExprs = new LinkedHashMap<>();
    List<Expr> outputExprs = new ArrayList<>();
    // Walk through all the projects and grab the already created Expr object that exists
    // in the "mapping" variable.
    for (RexNode rexNode : projects) {
      Expr expr = rexNode.accept(visitor);
      expr.analyze(analyzer);
      outputExprs.add(expr);
    }
    return outputExprs;
  }

  public static List<RexOver> gatherRexOver(List<RexNode> exprs) {
    final List<RexOver> result = new ArrayList<>();
    RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
      public Void visitOver(RexOver over) {
        result.add(over);
        return super.visitOver(over);
      }
    };
    for (RexNode expr : exprs) {
      expr.accept(visitor);
    }
    return result;
  }

  private Function getFunction(RexOver exp) throws ImpalaException {
    RelDataType retType = exp.getType();
    SqlAggFunction aggFunction = exp.getAggOperator();
    List<RelDataType> operandTypes = Lists.newArrayList();
    for (RexNode operand : exp.operands) {
      operandTypes.add(operand.getType());
    }
    return FunctionResolver.getExactFunction(aggFunction.getName(),
       aggFunction.getKind(),  operandTypes);
  }

  /**
   * Get the analytic Expr objects from the RexOver objects in the Project.
   * Impala does not allow duplicate analytic expressions. So if two different
   * RexOvers create the same AnalyticExpr object, they get grouped together in
   * one GroupedAnalyticExpr object.
   */
  private List<GroupedAnalyticExpr> getGroupedAnalyticExprs(List<RexOver> overExprs,
      PlannerContext ctx, ImpalaPlanRel inputRel,
      List<Expr> inputExprs) throws ImpalaException {
    List<AnalyticExpr> analyticExprs = new ArrayList<>();
    List<List<RexOver>> overExprsList = new ArrayList<>();
    for (RexOver over : overExprs) {
      AnalyticExpr analyticExpr =
          getAnalyticExpr(over, ctx, inputRel, inputExprs);
      // check if we've seen this analytic expression before.
      int index = analyticExprs.indexOf(analyticExpr);
      if (index == -1) {
        analyticExprs.add(analyticExpr);
        overExprsList.add(Lists.newArrayList(over));
      } else {
        overExprsList.get(index).add(over);
      }
    }
    // The total number of unique analytic expressions should match the number
    // of RexOver expression lists created.
    Preconditions.checkState(analyticExprs.size() == overExprsList.size());

    // Create the GroupedAnalyticExprs from the corresponding lists
    List<GroupedAnalyticExpr> groupedAnalyticExprs = new ArrayList<>();
    for (int i = 0; i < analyticExprs.size(); ++i) {
      groupedAnalyticExprs.add(
          new GroupedAnalyticExpr(analyticExprs.get(i), overExprsList.get(i)));
    }
    return groupedAnalyticExprs;
  }

  private Map<RexNode, Expr> createRexNodeExprMapping(ImpalaPlanRel inputRel,
      PlanNode planNode, List<RexNode> projects, List<Expr> inputExprs,
      List<GroupedAnalyticExpr> groupAnalyticExprs,
      PlannerContext ctx, AnalyticInfo analyticInfo) {
    // Gather mappings from nodes created by analytic planner
    ExprSubstitutionMap outputExprMap = planNode.getOutputSmap();
    // We populate the outputs from the expressions
    Map<RexNode, Expr> mapping = new LinkedHashMap<>();

    // All the input references are going to get marked as a RexInputRef and
    // will be mapped to its given expression's position number
    for (int pos : getInputReferences(projects)) {
      // Get the Impala expr after substituting its operands based on the expression map
      Expr e = inputExprs.get(pos).substitute(outputExprMap, ctx.getRootAnalyzer(),
          /* preserveRootType = */true);
      mapping.put(RexInputRef.of(pos, getInput(0).getRowType().getFieldList()), e);
    }

    // Create a new SlotRef for analytic expressions.
    for (int i = 0; i < groupAnalyticExprs.size(); i++) {
      GroupedAnalyticExpr g = groupAnalyticExprs.get(i);
      SlotDescriptor slotDesc =
          analyticInfo.getOutputTupleDesc().getSlots().get(i);
      SlotRef logicalOutputSlot = new SlotRef(slotDesc);
      for (RexOver over : g.overExprsList) {
        mapping.put(over, outputExprMap.get(logicalOutputSlot));
      }
    }

    return mapping;
  }

  private Set<Integer> getInputReferences(List<RexNode> projects) {
    RelOptUtil.InputReferencedVisitor shuttle = new RelOptUtil.InputReferencedVisitor();
    shuttle.apply(projects);
    return shuttle.inputPosReferenced;
  }

  private static class GroupedAnalyticExpr {
    public final AnalyticExpr analyticExpr;
    public final List<RexOver> overExprsList;

    public GroupedAnalyticExpr(AnalyticExpr analyticExpr, List<RexOver> overExprsList) {
      this.analyticExpr = analyticExpr;
      this.overExprsList = overExprsList;
    }
  }

  /**
   * Visitor class that walks through the RexNode from the Project and
   * creates an expression. The Expr objects for the input PlanNode are
   * created through the AnalyticPlanner, so we use the mapping to help
   * extract these Expr objects.
   */
  private static class AnalyticRexVisitor extends RexVisitorImpl<Expr> {

    private final Map<RexNode, Expr> exprsMap_;

    private final RexBuilder rexBuilder_;

    private final Analyzer analyzer_;

    public AnalyticRexVisitor(Map<RexNode, Expr> exprsMap,
        RexBuilder rexBuilder, Analyzer analyzer) {
      super(false);
      this.exprsMap_ = exprsMap;
      this.rexBuilder_ = rexBuilder;
      this.analyzer_ = analyzer;
    }

    @Override
    public Expr visitCall(RexCall rexCall) {
      List<Expr> params = Lists.newArrayList();
      for (RexNode operand : rexCall.getOperands()) {
        params.add(operand.accept(this));
      }
      try {
        return RexCallConverter.getExpr(rexCall, params, rexBuilder_, analyzer_);
      } catch (ImpalaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Expr visitLiteral(RexLiteral rexLiteral) {
      return RexLiteralConverter.getExpr(rexLiteral);
    }

    @Override
    public Expr visitInputRef(RexInputRef rexInputRef) {
      return exprsMap_.get(rexInputRef);
    }

    @Override
    public Expr visitOver(RexOver over) {
      return exprsMap_.get(over);
    }

    @Override
    public Expr visitLocalRef(RexLocalRef localRef) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitCorrelVariable(RexCorrelVariable correlVariable) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitDynamicParam(RexDynamicParam dynamicParam) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitRangeRef(RexRangeRef rangeRef) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitFieldAccess(RexFieldAccess fieldAccess) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitSubQuery(RexSubQuery subQuery) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitTableInputRef(RexTableInputRef fieldRef) {
      throw new RuntimeException("Not supported");
    }

    @Override
    public Expr visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      throw new RuntimeException("Not supported");
    }
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.PROJECT;
  }
}
