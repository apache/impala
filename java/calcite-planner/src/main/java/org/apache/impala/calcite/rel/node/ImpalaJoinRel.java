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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.calcite.functions.AnalyzedFunctionCallExpr;
import org.apache.impala.calcite.functions.AnalyzedNullLiteral;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.rel.phys.ImpalaHashJoinNode;
import org.apache.impala.calcite.rel.phys.ImpalaNestedLoopJoinNode;
import org.apache.impala.calcite.rel.util.CreateExprVisitor;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.rel.util.RexInputRefCollector;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.PlanNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaJoinRel
 */
public class ImpalaJoinRel extends Join
    implements ImpalaPlanRel {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaJoinRel.class.getName());

  public ImpalaJoinRel(Join join) {
    super(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(),
        join.getCondition(), join.getJoinType(), new HashSet<>());
  }

  public ImpalaJoinRel(RelOptCluster cluster, RelTraitSet relTraitSet,
      RelNode leftInput, RelNode rightInput, RexNode condition, JoinRelType joinType) {
    super(cluster, relTraitSet, leftInput, rightInput, condition, joinType,
        new HashSet<>());
  }

  @Override
  public ImpalaJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new ImpalaJoinRel(getCluster(), traitSet, left, right, conditionExpr,
        joinType);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    NodeWithExprs leftInput = getChildPlanNode(getInput(0), context);
    NodeWithExprs rightInput = getChildPlanNode(getInput(1), context);

    Analyzer analyzer = context.ctx_.getRootAnalyzer();
    JoinOperator joinOp = getImpalaJoinOp();

    List<BinaryPredicate> equiJoinConjuncts = new ArrayList<>();
    List<Expr> otherJoinConjuncts = new ArrayList<>();

    // IMPALA-13176: TODO: Impala allows forcing hints for the distribution mode
    // - e.g force broadcast or hash partition join.  However, we are not
    // currently supporting hints from the new planner.
    JoinNode.DistributionMode distMode = JoinNode.DistributionMode.NONE;

    // get the Exprs from both sides. If it's an outer join, some of the
    // Exprs will be Null wrapped (see comment on method)
    List<Expr> outputExprs = new ArrayList<>();
    outputExprs.addAll(getOutputExprs(analyzer, leftInput, true, joinOp));
    outputExprs.addAll(getOutputExprs(analyzer, rightInput, false, joinOp));

    // Populate equijoin and non-equijoin conditions
    if (!getCondition().isAlwaysTrue()) {
      List<ConjunctInfo> conjunctInfos = getConditionConjuncts(getCondition(),
          leftInput, rightInput, analyzer);
      for (ConjunctInfo conjunctInfo : conjunctInfos) {
        if (conjunctInfo.isEquiJoin_) {
          equiJoinConjuncts.add((BinaryPredicate) conjunctInfo.conjunct_);
        } else {
          otherJoinConjuncts.add(conjunctInfo.conjunct_);
        }
      }
    }

    // Populate filter condition
    List<Expr> filterConjuncts = new ArrayList<>();
    if (context.filterCondition_ != null) {
      ExprConjunctsConverter converter = new ExprConjunctsConverter(
          context.filterCondition_, outputExprs, getCluster().getRexBuilder(), analyzer);
      filterConjuncts.addAll(converter.getImpalaConjuncts());
    }

    // Create Impala plan node
    PlanNode joinNode = equiJoinConjuncts.size() == 0
      ? new ImpalaNestedLoopJoinNode(context.ctx_.getNextNodeId(), leftInput.planNode_,
          rightInput.planNode_, false /* not a straight join */, distMode, joinOp,
          otherJoinConjuncts, filterConjuncts, analyzer)
      : new ImpalaHashJoinNode(context.ctx_.getNextNodeId(), leftInput.planNode_,
          rightInput.planNode_, false /* not a straight join */, distMode, joinOp,
          equiJoinConjuncts, otherJoinConjuncts, filterConjuncts, analyzer);

    if (equiJoinConjuncts.size() > 0) {
      // register the equi and non-equi join conjuncts with the analyzer such that
      // value transfer graph creation can consume it
      List<Expr> equiJoinExprs = new ArrayList<Expr>(equiJoinConjuncts);
      analyzer.registerConjuncts(getJoinConjunctListToRegister(equiJoinExprs));
    }
    analyzer.registerConjuncts(getJoinConjunctListToRegister(otherJoinConjuncts));

    return new NodeWithExprs(joinNode, outputExprs);
  }

  private NodeWithExprs getChildPlanNode(RelNode relInput,
      ParentPlanRelContext context) throws ImpalaException {
    ImpalaPlanRel inputPlanRel = (ImpalaPlanRel) relInput;
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    builder.setFilterCondition(null);
    return inputPlanRel.getPlanNode(builder.build());
  }


  /**
   * Checks and adds null wrapping for expressions fed into the null producing side of an
   * outer join (hence, both sides for a Full Outer Join).
   * The null wrapping checks if the tupleId is null and will produce a null row on that
   * side of the join.
   * In the case of anti joins and semi joins, no columns are created on one side of the
   * join.
   */
  private List<Expr> getOutputExprs(Analyzer analyzer, NodeWithExprs input,
      boolean isLeftInput, JoinOperator joinOp) throws ImpalaException {

    if (isLeftInput) {
      // No expressions needed on the right side on a semi or anti join.
      if (joinOp == JoinOperator.RIGHT_SEMI_JOIN ||
          joinOp == JoinOperator.RIGHT_ANTI_JOIN) {
        return new ArrayList<>();
      }
      // No null wrapping is needed if it's not an outer join.
      if ((joinOp != JoinOperator.RIGHT_OUTER_JOIN &&
          joinOp != JoinOperator.FULL_OUTER_JOIN)) {
        return input.outputExprs_;
      }
    } else {
      // No expressions needed on the left side on a semi or anti join.
      if (joinOp == JoinOperator.LEFT_SEMI_JOIN ||
          joinOp == JoinOperator.LEFT_ANTI_JOIN) {
        return new ArrayList<>();
      }
      // No null wrapping is needed if it's not an outer join.
      if ((joinOp != JoinOperator.LEFT_OUTER_JOIN &&
          joinOp != JoinOperator.FULL_OUTER_JOIN)) {
        return input.outputExprs_;
      }
    }

    // Apply null wrapping if necessary.
    List<Expr> wrappedExprs = new ArrayList<>();
    for (Expr expr : input.outputExprs_) {
      Expr exprToAdd = TupleIsNullPredicate.requiresNullWrapping(expr, analyzer)
          ? createIfTupleIsNullPredicate(analyzer, expr, input.planNode_.getTupleIds())
          : expr;

      wrappedExprs.add(exprToAdd);
    }
    return wrappedExprs;
  }


  /**
   * Returns a new conditional expr 'IF(TupleIsNull(tids), NULL, expr)' to
   * make an input expr nullable.  This is especially useful in cases where the Calcite
   * planner generates a literal TRUE and later does a IS_NULL($x) or IS_NOT_NULL($x)
   * check on this column - this happens for NOT IN, NOT EXISTS queries where the planner
   * generates a Left Outer Join and checks the nullability of the column being output
   * from the right side of the LOJ. Since the literal TRUE is a non-null value coming
   * into the join but after the join becomes nullable, we add this function to ensure
   * that happens. Without adding this function the direct translation would be
   * 'TRUE IS NULL' which is incorrect.
   */
  private static Expr createIfTupleIsNullPredicate(Analyzer analyzer, Expr expr,
      List<TupleId> tupleIds) throws ImpalaException {
    List<Expr> tmpArgs = new ArrayList<>();
    TupleIsNullPredicate tupleIsNullExpr = new TupleIsNullPredicate(tupleIds);
    tupleIsNullExpr.analyze(analyzer);
    tmpArgs.add(tupleIsNullExpr);
    // null type needs to be cast to appropriate target type before thrift serialization
    AnalyzedNullLiteral nullLiteral = new AnalyzedNullLiteral(expr.getType());
    nullLiteral.analyze(analyzer);
    tmpArgs.add(nullLiteral);
    tmpArgs.add(expr);
    List<Type> typeNames = ImmutableList.of(Type.BOOLEAN, expr.getType(), expr.getType());
    Function conditionalFunc = FunctionResolver.getExactFunction("if",
        ImpalaTypeConverter.getRelDataTypesForArgs(typeNames));
    Preconditions.checkNotNull(conditionalFunc,
        "Could not create IF function for arg types %s and return type %s",
        typeNames, expr.getType());
    Expr retExpr = new AnalyzedFunctionCallExpr(conditionalFunc, tmpArgs, expr.getType());
    retExpr.analyze(analyzer);
    return retExpr;
  }

  private JoinOperator getImpalaJoinOp() throws ImpalaException {
    switch (getJoinType()) {
    case INNER:
      return JoinOperator.INNER_JOIN;
    case FULL:
      return JoinOperator.FULL_OUTER_JOIN;
    case LEFT:
      return JoinOperator.LEFT_OUTER_JOIN;
    case RIGHT:
      return JoinOperator.RIGHT_OUTER_JOIN;
    case SEMI:
      // Calcite SEMI is always a LEFT_SEMI right now.
      return JoinOperator.LEFT_SEMI_JOIN;
    case ANTI:
      return JoinOperator.LEFT_ANTI_JOIN;
    }
    throw new AnalysisException("Unsupported join type: " + getJoinType());
  }

  /**
   * Get all the join conjuncts that need registering.
   */
  public List<Expr> getJoinConjunctListToRegister(List<Expr> joinConjuncts) {
    List<Expr> conjunctsToRegister = new ArrayList<>();
    for (Expr joinConjunct : joinConjuncts) {
      conjunctsToRegister.addAll(getJoinConjunctToRegister(joinConjunct));
    }
    return conjunctsToRegister;
  }

  /**
   * Given a join conjunct, derive the join conjunct that needs to be registered
   * with the analyzer.
   *
   * The expression produced here will get registered in the Impala value transfer
   * graph. This value transfer graph is then used to calculate the runtime filter
   * generators that can be created.
   *
   * There are two major differences between the runtime filter generators generated
   * within the original planner versus the ones generated within this Calcite planner.
   *
   * 1) The original planner only seems to generate one runtime filter generator for each
   * join conjunct. It traces down the SlotRef used on the probe side down to the
   * scan node.  This planner, however, makes liberal use of the value transfer
   * graph. Once it goes down the probe side, it can apply the runtime filter to
   * either side of join conjuncts below the current join node.
   *
   * 2) Because the original planner only generates one runtime filter generator off of
   * the given SlotRef, the data type always matches. There is never any casting
   * adjustment that needs to be done. However, if a lower conjunct within the Calcite
   * planner is comparing two expressions with different SlotRef types, there will need to
   * be a casting adjustment. This has not been coded into Impala so this will cause
   * impalad to crash.
   *
   * An example query where the casting adjustment is needed within the Impala test
   * framework is:
   *
   * select straight_join count(*) from functional.alltypessmall a
   * inner join
   *   (select straight_join t2.* from functional.alltypessmall t1
   *    inner join functional.alltypessmall t2 on t1.tinyint_col = t2.smallint_col) b
   *    on a.int_col = b.smallint_col
   *   inner join
   *   (select distinct tinyint_col from functional.alltypessmall) c
   * on a.int_col = c.tinyint_col
   *
   * Ultimately, it would be better to code the casting adjustment within Impala.
   * However, this has not been coded in Impala yet, making this a TODO:
   * Because it hasn't been coded yet within Impala, we do not allow a runtime filter
   * generator to be generated on SlotRefs that don't match the top level SlotRef.
   *
   * The value transfer graph is used to show expressions that are similar. So if we
   * detect a mismatch of SlotRef types, the full conjunct cannot be entered into
   * the value transfer graph which would show equivalentce of the SlotRefs. We do still
   * need to put in the left side of the conjunct into the value transfer graph to show
   * its existence.
   *
   * TL:DR; We only register the full conjunct if the SlotRef types match on both sides
   * of the join conjunct expression.
   */
  public List<Expr> getJoinConjunctToRegister(Expr joinConjunct) {
    Preconditions.checkState(joinConjunct.getChildren().size() == 2);
    List<SlotRef> leftSideSlotRefs = new ArrayList<>();
    List<SlotRef> rightSideSlotRefs = new ArrayList<>();
    joinConjunct.getChild(0).collect(SlotRef.class, leftSideSlotRefs);
    joinConjunct.getChild(1).collect(SlotRef.class, rightSideSlotRefs);

    if (leftSideSlotRefs.size() == 0) {
      return Lists.newArrayList();
    }

    if (rightSideSlotRefs.size() == 0) {
      return Lists.newArrayList(joinConjunct.getChild(0));
    }

    // If there is more than one slotRef on either side of the conjunct, we play it
    // safe and don't put in the SlotRef equivalency for the full conjunct. Break
    // the conjunct into a list of its left side and right side.
    if (leftSideSlotRefs.size() > 1 || rightSideSlotRefs.size() > 1) {
      return Lists.newArrayList(joinConjunct.getChild(0), joinConjunct.getChild(1));
    }

    if (leftSideSlotRefs.get(0).getType() != rightSideSlotRefs.get(0).getType()) {
      return Lists.newArrayList(joinConjunct.getChild(0), joinConjunct.getChild(1));
    }

    return Lists.newArrayList(joinConjunct);
  }

  private List<Expr> getAllInputExprs(NodeWithExprs leftInput, NodeWithExprs rightInput) {
    List<Expr> allExprs = new ArrayList<>();
    allExprs.addAll(leftInput.outputExprs_);
    allExprs.addAll(rightInput.outputExprs_);
    return allExprs;
  }

  /**
   * Canonicalize the equijoin condition such that it is
   * represented as =($M, $N) where M < N. The Impala backend
   * expects this. If the join condition has expressions
   * e.g CAST($5, INT) + $4 = CAST($3, INT) + $1
   * This method will traverse the left and right sides of the
   * condition and identify the minimum RexInputRef index
   * on each side.
   * If the left's min index is >= the number of expressions
   * on the left side, it will swap the two sides. In the case
   * that there is no input ref on the left side, it will check
   * the right side index to see if the expression needs swapping.
   */
  private RexNode getCanonical(RexNode conjunct, int numLHSExprs) {
    if (!conjunct.isA(SqlKind.EQUALS)) {
      return conjunct;
    }

    boolean swapExprs = false;
    MinIndexVisitor visitor = new MinIndexVisitor();
    RexNode lhsRexCall = ((RexCall) conjunct).getOperands().get(0);
    RexNode rhsRexCall = ((RexCall) conjunct).getOperands().get(1);
    // visit the left and right child
    lhsRexCall.accept(visitor);
    int minIndex = visitor.getMinIndex();
    if (minIndex != Integer.MAX_VALUE) {
      if (minIndex >= numLHSExprs) {
        swapExprs = true;
      }
    } else {
      visitor.reset();
      // didn't find an input ref on the left side, check the right side.
      rhsRexCall.accept(visitor);
      minIndex = visitor.getMinIndex();
      Preconditions.checkState(minIndex != Integer.MAX_VALUE);
      if (minIndex < numLHSExprs) {
        swapExprs = true;
      }
    }

    if (swapExprs) {
      // swap the left and right children
      conjunct = getCluster().getRexBuilder().
          makeCall(SqlStdOperatorTable.EQUALS, rhsRexCall, lhsRexCall);
    }
    return conjunct;
  }

  /**
   * Break up the given RexNode condition into its AND components and return a list
   * of the Expr conjuncts (and whether it is an EQUALS conjunct).
   */
  private List<ConjunctInfo> getConditionConjuncts(RexNode condition,
      NodeWithExprs leftInput, NodeWithExprs rightInput, Analyzer analyzer) {

    List<ConjunctInfo> conjunctInfos = new ArrayList<>();
    CreateExprVisitor visitor = new CreateExprVisitor(getCluster().getRexBuilder(),
        getAllInputExprs(leftInput, rightInput), analyzer);

    // break up the conjunctions into each AND component
    List<RexNode> conjuncts = RelOptUtil.conjunctions(getCondition());
    // convert the conjuncts to Impala Expr
    for (RexNode conj : conjuncts) {
      // get a canonicalized representation
      conj = getCanonical(conj, leftInput.outputExprs_.size());
      Expr impalaConjunct = conj.accept(visitor);
      conjunctInfos.add(
          new ConjunctInfo(impalaConjunct, isEquijoinConjunct(conj, leftInput)));
    }
    return conjunctInfos;
  }

  // Checks to see if this conjunct can be considered an "equijoin" conjunct.
  // An equijoin conjunct must have the following traits:
  // - should be an "EQUALS" RexCall at the top level.
  // - be in canonical form, that is, the input refs on the left side of the
  //   equals RexNode should point to the left input and the right side of the
  //   equals RexNode should point to the right input.
  // - should be at least one input ref on the left side and one input ref on
  //   the right side.
  private boolean isEquijoinConjunct(RexNode conjunct, NodeWithExprs leftInput) {
    if (!conjunct.isA(SqlKind.EQUALS)) {
      return false;
    }

    Preconditions.checkState(conjunct instanceof RexCall);
    RexCall call = (RexCall) conjunct;

    // left side expr may only contain inputrefs from left side.
    // right side expr may only contain inputrefs from right side.
    RexNode left = call.getOperands().get(0);
    Set<Integer> inputRefs = RexInputRefCollector.getInputRefs(left);
    if (inputRefs.size() == 0) {
      return false;
    }

    for (Integer inputRef : inputRefs) {
      if (inputRef >= leftInput.outputExprs_.size()) {
        return false;
      }
    }

    RexNode right = call.getOperands().get(1);
    inputRefs = RexInputRefCollector.getInputRefs(right);
    if (inputRefs.size() == 0) {
      return false;
    }
    for (Integer inputRef : inputRefs) {
      if (inputRef < leftInput.outputExprs_.size()) {
        return false;
      }
    }
    return true;

  }

  private static class ConjunctInfo {
    public final Expr conjunct_;
    public final boolean isEquiJoin_;

    public ConjunctInfo(Expr conjunct, boolean isEquiJoin) {
      this.conjunct_ = conjunct;
      this.isEquiJoin_ = isEquiJoin;
    }
  }

  private static class MinIndexVisitor extends RexVisitorImpl<Void> {
    private int minIndex = Integer.MAX_VALUE;
    public MinIndexVisitor() {
      super(true);
    }
    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      minIndex = Math.min(inputRef.getIndex(), minIndex);
      return null;
    }
    public int getMinIndex() {
      return minIndex;
    }
    public void reset() {
      minIndex = Integer.MAX_VALUE;
    }
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.JOIN;
  }
}
