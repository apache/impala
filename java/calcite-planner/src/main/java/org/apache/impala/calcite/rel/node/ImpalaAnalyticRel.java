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
import com.google.common.collect.ImmutableList;
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
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.calcite.functions.AnalyzedAnalyticExpr;
import org.apache.impala.calcite.functions.AnalyzedFunctionCallExpr;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.functions.RexCallConverter;
import org.apache.impala.calcite.functions.RexLiteralConverter;
import org.apache.impala.calcite.rel.util.CreateExprVisitor;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.AnalyticPlanner;
import org.apache.impala.planner.AnalyticPlanner.PartitionLimit;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.SelectNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ImpalaAnalyticRel. Calcite RelNode which maps to an AnalyticEval node and
 * which creates all the necessary Plan nodes associated with it.
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
   * Convert the Project RelNode with analytic exprs into Impala Plan Nodes.
   */
  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    List<RexNode> projects = getProjects();
    NodeWithExprs inputNodeWithExprs = getChildPlanNode(context, projects);
    ImpalaPlanRel inputRel = (ImpalaPlanRel) getInput(0);
    SimplifiedAnalyzer simplifiedAnalyzer =
        (SimplifiedAnalyzer) context.ctx_.getRootAnalyzer();

    // Get Info about all the Projects in the Calcite Project RelNode.
    AllProjectInfo allProjectInfo = createAllProjectInfo(getProjects(), context.ctx_,
        inputRel, inputNodeWithExprs.outputExprs_);
    // There should be one element in the projectInfos_ array for each project in
    // the original Project RelNode
    Preconditions.checkState(getProjects().size() == allProjectInfo.projectInfos_.size());

    // Create Impala Analytic planner objects.
    AnalyticInfo analyticInfo =
        AnalyticInfo.create(allProjectInfo.analyticExprs_, simplifiedAnalyzer);
    AnalyticPlanner analyticPlanner =
        new AnalyticPlanner(analyticInfo, simplifiedAnalyzer, context.ctx_);

    // Create a SlotRef for each analytic expression created by the AnalyticInfo.
    Map<AnalyticExpr, SlotRef> analyticExprSlotRefMap =
        createAnalyticExprSlotRefMap(allProjectInfo.analyticExprs_, analyticInfo);

    // Retrieve the "perPartitionLimits" used for a potential top-N optimization
    List<PartitionLimit> perPartitionLimits = getPerPartitionLimits(
        context.filterCondition_, allProjectInfo, analyticExprSlotRefMap,
        simplifiedAnalyzer);

    // Pass in all the expressions from the input node wrapped with a
    // TupleIsNullPredicate.
    // TODO: IMPALA-12961.  strip out null exprs which can exist when the tablescan does
    // not output all of its columns.
    List<Expr> nonNullExprs = inputNodeWithExprs.outputExprs_.stream()
        .filter(e -> e != null).collect(Collectors.toList());
    List<TupleIsNullPredicate> tupleIsNullPreds =
        TupleIsNullPredicate.getUniqueBoundTupleIsNullPredicates(nonNullExprs,
            inputNodeWithExprs.planNode_.getTupleIds());

    // Create the plan node
    // One difference with the original Impala planner.  The original Impala planner
    // calls a different outer AnalyticPlanner.createSingleNodePlan which also
    // returns the Select node on top of the Analytic Node. The Calcite planner plan
    // node here will be of type AnalyticEvalNode. If a SelectNode is needed because
    // there is a filter condition, it will be created
    PlanNode planNode = analyticPlanner.createSingleNodePlan(
        inputNodeWithExprs.planNode_, Collections.emptyList(), new ArrayList<>(),
        tupleIsNullPreds, perPartitionLimits);

    // Get a mapping of all project columns to its corresponding Impala Expr object. The
    // non-analytic expressions will have a RexInputRef type RexNode which are simply
    // pass-through projects. The analytic expressions in the project will be of type
    // RexOver.
    Map<RexNode, Expr> mapping = createRexNodeExprMapping(inputRel, planNode, projects,
        inputNodeWithExprs.outputExprs_, allProjectInfo.projectInfos_, context.ctx_,
        analyticExprSlotRefMap);

    List<Expr> outputExprs = getOutputExprs(mapping, projects, simplifiedAnalyzer);

    NodeWithExprs retNode =
        new NodeWithExprs(planNode, outputExprs, getRowType().getFieldNames());

    RexBuilder rexBuilder = getCluster().getRexBuilder();
    return context.filterCondition_ != null
       ? createSelectNode(context, retNode, rexBuilder, perPartitionLimits)
       : retNode;
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
   * Returns true if the given filter condition only contains input expressions
   * referencing a rank expression.
   */
  private boolean hasOnlyRankSlotRefInputExprs(RexNode filterCondition,
      Set<Integer> rankProjects) {
    Set<Integer> inputReferences =
        getInputReferences(ImmutableList.of(filterCondition));
    return inputReferences.size() == 1 &&
        rankProjects.containsAll(inputReferences);
  }

  /**
   * Generates the AnalyticExpr object from the RexOver and the input plan node
   * expressions.
   */
  private AnalyticExpr createAnalyticExpr(RexBuilder rexBuilder,
      RexOver rexOver, PlannerContext ctx,
      ImpalaPlanRel inputRel, List<Expr> inputExprs) throws ImpalaException {
    final RexWindow rexWindow = rexOver.getWindow();
    // First parameter is the function call
    Function fn = getFunction(rexOver);
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(rexOver.getType());
    CreateExprVisitor visitor = new CreateExprVisitor(rexBuilder,
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
        // Logic here:
        // If ascending, nulls first is true only when it explicitly contains nulls first.
        //
        // If descending, nulls first is true if it explicitly contains nulls first
        //   or it is blank (does not contain nulls last).
        boolean nullsFirst = ok.getDirection() == RelFieldCollation.Direction.ASCENDING
            ? ok.right.contains(SqlKind.NULLS_FIRST)
            : ok.right.contains(SqlKind.NULLS_FIRST)
                || !ok.right.contains(SqlKind.NULLS_LAST);
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

  /**
   * Generate a SlotRef for each AnalyticExpr and place in a hash map.
   */
  Map<AnalyticExpr, SlotRef> createAnalyticExprSlotRefMap(List<AnalyticExpr> exprs,
      AnalyticInfo analyticInfo) {
    Preconditions.checkState(
        exprs.size() == analyticInfo.getOutputTupleDesc().getSlots().size());
    Map<AnalyticExpr, SlotRef> result = new HashMap<>();
    for (int i = 0; i < exprs.size(); ++i) {
      AnalyticExpr expr = exprs.get(i);
      SlotDescriptor slotDesc =
          analyticInfo.getOutputTupleDesc().getSlots().get(i);
      SlotRef newSlotRef = new SlotRef(slotDesc);
      result.put(expr, newSlotRef);
    }
    return result;
  }

  private List<Expr> getOutputExprs(Map<RexNode, Expr> mapping,
      List<RexNode> projects, Analyzer analyzer) throws ImpalaException {

    AnalyticRexVisitor visitor = new AnalyticRexVisitor(mapping,
        getCluster().getRexBuilder(), analyzer);

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
   * Create a mapping from the Projects for this RelNode to the output Expr
   * objects that exist within the newly created Plan Node.
   */
  private Map<RexNode, Expr> createRexNodeExprMapping(ImpalaPlanRel inputRel,
      PlanNode planNode, List<RexNode> projects, List<Expr> inputExprs,
      List<ProjectInfo> projectInfos,
      PlannerContext ctx, Map<AnalyticExpr, SlotRef> analyticExprSlotRefMap) {
    Map<RexNode, Expr> mapping = new LinkedHashMap<>();

    // Gather mappings from nodes created by analytic planner
    ExprSubstitutionMap outputExprMap = planNode.getOutputSmap();

    // All the input references are going to get marked as a RexInputRef and
    // will be mapped to its given expression's position number
    for (int pos : getInputReferences(projects)) {
      // Get the Impala expr after substituting its operands based on the expression map
      Expr e = inputExprs.get(pos).substitute(outputExprMap, ctx.getRootAnalyzer(),
          /* preserveRootType = */true);
      mapping.put(RexInputRef.of(pos, getInput(0).getRowType().getFieldList()), e);
    }

    // Retrieve the new SlotRef for analytic expressions.
    for (ProjectInfo projectInfo : projectInfos) {
      for (int i = 0; i < projectInfo.rexOvers_.size(); ++i) {
        Expr analyticExpr = analyticExprSlotRefMap.get(projectInfo.analyticExprs_.get(i));
        mapping.put(projectInfo.rexOvers_.get(i), outputExprMap.get(analyticExpr));
      }
    }

    return mapping;
  }

  private Set<Integer> getInputReferences(List<RexNode> projects) {
    RelOptUtil.InputReferencedVisitor shuttle = new RelOptUtil.InputReferencedVisitor();
    shuttle.apply(projects);
    return shuttle.inputPosReferenced;
  }

  /**
   * Generate the AllProjectInfo structure (see internal class for details)
   */
  private AllProjectInfo createAllProjectInfo(List<RexNode> projects, PlannerContext ctx,
      ImpalaPlanRel inputRel, List<Expr> inputExprs) throws ImpalaException {

    RexBuilder rexBuilder = getCluster().getRexBuilder();
    List<ProjectInfo> projectInfos = new ArrayList<>();
    // AnalyticExpr must be unique within Impala's AnalyticPlanner. There can be two
    // RexOver projects that generate the same AnalyticExpr.
    LinkedHashSet<AnalyticExpr> uniqueAnalyticExprs = new LinkedHashSet<>();

    Set<Integer> rankProjects = new HashSet<>();

    // For loop generates a new ProjectInfo object for every Project RexNode column.
    for (int projectNum = 0; projectNum < projects.size(); ++projectNum) {
      RexNode project = projects.get(projectNum);
      List<RexOver> overs = gatherRexOver(ImmutableList.of(project));
      List<AnalyticExpr> projectAnalyticExprs = new ArrayList<>();
      for (RexOver over : overs) {
        AnalyticExpr analyticExpr =
            createAnalyticExpr(rexBuilder, over, ctx, inputRel, inputExprs);
        if (!uniqueAnalyticExprs.contains(analyticExpr)) {
          uniqueAnalyticExprs.add(analyticExpr);
        }
        projectAnalyticExprs.add(analyticExpr);
      }
      projectInfos.add(new ProjectInfo(project, overs, projectAnalyticExprs));
      if (project.getKind() == SqlKind.RANK ||
          project.getKind().equals(SqlKind.ROW_NUMBER)) {
        rankProjects.add(projectNum);
      }
    }
    return new AllProjectInfo(projectInfos, new ArrayList<>(uniqueAnalyticExprs),
        rankProjects);
  }

  /**
   * Retrieve the list of Partition Limits. This is specifically used for the top-n
   * optimization feature.
   */
  private List<PartitionLimit> getPerPartitionLimits(RexNode filterCondition,
        AllProjectInfo allProjectInfo,  Map<AnalyticExpr, SlotRef> analyticExprSlotRefMap,
        Analyzer analyzer) throws ImpalaException {
    // top-n optimization feature only works when there is a filter.
    if (filterCondition == null) {
      return ImmutableList.of();
    }

    List<Expr> rankConjuncts = new ArrayList<>();
    // Retrieve a list which contains an element for every project but
    // only contains a non-null Expr if the Project is of type RANK
    // or ROW_NUMBER
    List<Expr> rankSlotRefExprs = allProjectInfo.projectInfos_.stream()
        .map(e -> e.rankExpr_)
        .map(e -> analyticExprSlotRefMap.get(e))
        .collect(Collectors.toList());
    Preconditions.checkState(getProjects().size() == rankSlotRefExprs.size());

    // Break up the filter condition into its individual and elements
    List<RexNode> andConjuncts = ExprConjunctsConverter.getAndConjuncts(filterCondition);
    for (RexNode andConjunct : andConjuncts) {
      // We only want the filter conjuncts that contain a reference to the Rank project
      // e.g. 'rnk <= 5' where rnk is an alias to an analytic expr containing rank.
      if (hasOnlyRankSlotRefInputExprs(andConjunct, allProjectInfo.rankProjects_)) {
        // convert the RexNode filter condition into an Impala Expr that can be
        // understood by the Analytic Planner
        ExprConjunctsConverter converter = new ExprConjunctsConverter(
            andConjunct, rankSlotRefExprs, getCluster().getRexBuilder(),
            analyzer);
        rankConjuncts.addAll(converter.getImpalaConjuncts());
      }
    }
    return AnalyticPlanner.inferPartitionLimits(analyzer, rankConjuncts);
  }

  /**
   * Create a Select Node when there is a filter on top of this Project.
   */
  public NodeWithExprs createSelectNode(ParentPlanRelContext context,
      NodeWithExprs nodeWithExprs, RexBuilder rexBuilder,
      List<PartitionLimit> perPartitionLimits) throws ImpalaException {
    Analyzer analyzer = context.ctx_.getRootAnalyzer();
    PlanNodeId nodeId = context.ctx_.getNextNodeId();
    ExprConjunctsConverter converter = new ExprConjunctsConverter(
        context.filterCondition_, nodeWithExprs.outputExprs_, rexBuilder, analyzer);
    List<Expr> filterConjuncts = converter.getImpalaConjuncts();

    // For top-n optimization: If criteria is met, the filter conjunct gets its
    // selectivity overridden.
    filterConjuncts = overrideSelectivity(filterConjuncts, perPartitionLimits,
        nodeWithExprs.planNode_, analyzer);

    SelectNode selectNode =
        SelectNode.createFromCalcite(nodeId, nodeWithExprs.planNode_, filterConjuncts);
    selectNode.init(analyzer);
    return new NodeWithExprs(selectNode, nodeWithExprs);
  }

  /**
   * Override the selectivity on certain filtered conjuncts if needed.
   * The perPartitionLimits contain conjuncts that are used in top-n optimization
   * if it has been applied. This method returns the adjusted filtered conjuncts
   * with the potentially modified selectivity.
   */
  private List<Expr> overrideSelectivity(List<Expr> filterConjuncts,
      List<PartitionLimit> perPartitionLimits, PlanNode planNode, Analyzer analyzer) {

    ExprSubstitutionMap outputExprMap = planNode.getOutputSmap();
    // iterate through the partitions and retrieve the Expr conjuncts that were used
    // in the top-n optimization. The ExprSubstitutionMap is used since the
    // PartitionLimit contains the input Expr before the Expr node was created
    // and gets substituted with the Expr that is output from the AnalyticEval node.
    // is output from the AnalyticEval node.
    List<Expr> overrideSelectivityInputConjuncts = perPartitionLimits.stream()
        .filter(p -> p.shouldOverrideSelectivity())
        .map(p -> p.conjunct)
        .map(p -> p.substitute(outputExprMap, analyzer, false))
        .collect(Collectors.toList());

    // If there are no top-n optimizations applied, return the original unmodified
    // list.
    if (overrideSelectivityInputConjuncts.size() == 0) {
      return filterConjuncts;
    }

    // return the modified conjuncts (which are modified in the
    // overrideSelectivityIfNecessary method).
    return filterConjuncts.stream()
        .map(f -> overrideSelectivityIfNecessary(f, overrideSelectivityInputConjuncts))
        .collect(Collectors.toList());
  }

  /**
   * Given a filter conjunct and a list of conjuncts to override: If the filter conjunct
   * is contained in the list, return a modified conjunct. Otherwise return the original
   * conjunct.
   */
  public Expr overrideSelectivityIfNecessary(Expr filterConj,
      List<Expr> overrideConjuncts) {
    for (Expr overrideConjunct : overrideConjuncts) {
      if (overrideConjunct.equals(filterConj)) {
        return filterConj.cloneAndOverrideSelectivity(1.0);
      }
    }
    return filterConj;
  }

  public boolean isOverrideSelectivityConj(PartitionLimit limit) {
    return limit.isPushed() && limit.isLessThan && limit.conjunct != null;
  }

  /**
   * AllProjectInfo contains all the information about the Project RelNode, relating
   * newly created Impala objects to each Project RexNode.
   */
  private static class AllProjectInfo {
    // Array of Project RexNodes and its related information. There is one ProjectInfo
    // member for each RexNode in the Project RelNode.
    private final List<ProjectInfo> projectInfos_;
    // List of unique Impala AnalyticExpr objects to be passed into AnalyticInfo.
    private final List<AnalyticExpr> analyticExprs_;

    // list of all rank/row number expressions needed for TopN optimization.
    private final Set<Integer> rankProjects_;

    public AllProjectInfo(List<ProjectInfo> projectInfos,
        List<AnalyticExpr> analyticExprs, Set<Integer> rankProjects) {
      projectInfos_ = projectInfos;
      analyticExprs_ = analyticExprs;
      rankProjects_ = rankProjects;
    }
  }

  /**
   * ProjectInfo contains all Impala information related to a single Project RexNode
   * column in the Project RelNode
   */
  private static class ProjectInfo {
    // The Project RexNode.
    public final RexNode project_;
    // All the RexOver objects within the RexNode.
    public final List<RexOver> rexOvers_;
    // All the Impala Analytic expressions, 1:1 with the RexOver objects.
    public final List<AnalyticExpr> analyticExprs_;
    // Special member variable that is only non-null if the rexOvers_.get(0) is
    // specifically a SqlKind.RANK node or SqlKind.ROW_NUMBER.
    public final AnalyticExpr rankExpr_;

    public ProjectInfo(RexNode project, List<RexOver> rexOvers,
        List<AnalyticExpr> analyticExprs) {
      project_ = project;
      Preconditions.checkState(rexOvers.size() == analyticExprs.size());
      rexOvers_ = ImmutableList.copyOf(rexOvers);
      analyticExprs_ = ImmutableList.copyOf(analyticExprs);

      SqlKind kind = project.getKind();
      rankExpr_ = kind == SqlKind.RANK || kind == SqlKind.ROW_NUMBER
          ? analyticExprs.get(0)
          : null;
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
