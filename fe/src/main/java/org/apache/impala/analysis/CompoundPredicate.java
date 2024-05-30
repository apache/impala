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

package org.apache.impala.analysis;

import java.util.List;
import java.util.Objects;

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * &&, ||, ! predicates.
 *
 */
public class CompoundPredicate extends Predicate {
  public enum Operator {
    AND("AND"),
    OR("OR"),
    NOT("NOT");

    private final String description;

    private Operator(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }
  private final Operator op_;

  // Selectivity estimate from BetweenToCompoundRule.computeBetweenSelectivity().
  // Only set if this CompoundPredicate is a rewrite from BetweenPredicate.
  // Otherwise, set to -1.
  private final double betweenSelectivity_;

  public static void initBuiltins(Db db) {
    // AND and OR are implemented as custom exprs, so they do not have a function symbol.
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.AND.name(), "",
        Lists.<Type>newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.OR.name(), "",
        Lists.<Type>newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.NOT.name(), "impala::CompoundPredicate::Not",
        Lists.<Type>newArrayList(Type.BOOLEAN), Type.BOOLEAN));
  }

  public static CompoundPredicate createFromBetweenPredicate(Operator op,
      BinaryPredicate lower, BinaryPredicate upper, double betweenSelectivity) {
    Preconditions.checkArgument(betweenSelectivity >= 0.0);
    return new CompoundPredicate(op, lower, upper, betweenSelectivity);
  }

  private CompoundPredicate(Operator op, Expr e1, Expr e2, double betweenSelectivity) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkArgument(op == Operator.NOT && e2 == null
        || op != Operator.NOT && e2 != null);
    if (e2 != null) {
      children_.add(e2);
      if ((op == Operator.AND &&
          (Expr.IS_ALWAYS_TRUE_PREDICATE.apply(e1) &&
              Expr.IS_ALWAYS_TRUE_PREDICATE.apply(e2))) ||
          ((op == Operator.OR && (Expr.IS_ALWAYS_TRUE_PREDICATE.apply(e1) ||
              Expr.IS_ALWAYS_TRUE_PREDICATE.apply(e2))))) {
        setHasAlwaysTrueHint(true);
      }
    }
    betweenSelectivity_ = betweenSelectivity;
  }

  public CompoundPredicate(Operator op, Expr e1, Expr e2) { this(op, e1, e2, -1); }

  /**
   * Copy c'tor used in clone().
   */
  protected CompoundPredicate(CompoundPredicate other) {
    super(other);
    op_ = other.op_;
    betweenSelectivity_ = other.betweenSelectivity_;
  }

  public Operator getOp() { return op_; }

  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that) && ((CompoundPredicate) that).op_ == op_;
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), op_);
  }

  @Override
  public String debugString() {
    ToStringHelper helper = MoreObjects.toStringHelper(this)
        .add("op", op_)
        .addValue(super.debugString());
    if (betweenSelectivity_ != -1) helper.add("betweenSelectivity", betweenSelectivity_);
    return helper.toString();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (children_.size() == 1) {
      Preconditions.checkState(op_ == Operator.NOT);
      return "NOT " + getChild(0).toSql(options);
    } else {
      return getChild(0).toSql(options) + " " + op_.toString() + " "
          + getChild(1).toSql(options);
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.COMPOUND_PRED;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    // Check that children are predicates.
    for (Expr e: children_) {
      if (!e.getType().isBoolean() && !e.getType().isNull()) {
        throw new AnalysisException(String.format("Operand '%s' part of predicate " +
            "'%s' should return type 'BOOLEAN' but returns type '%s'.",
            e.toSql(), toSql(), e.getType().toSql()));
      }
    }

    fn_ = getBuiltinFunction(analyzer, op_.toString(), collectChildReturnTypes(),
        CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Preconditions.checkState(fn_ != null);
    Preconditions.checkState(fn_.getReturnType().isBoolean());
    castForFunctionCall(false, analyzer.getRegularCompatibilityLevel());

    computeSelectivity(analyzer);
  }

  protected void computeSelectivity(Analyzer analyzer) {
    if (hasValidSelectivityHint()) {
      // TODO: Support selectivity hint for 'AND' compound predicates.
      if (this.getOp() == Operator.AND) {
        // 'AND' compound predicates will be replaced by children in Expr#getConjuncts,
        // so selectivity hint will be missing, we add a warning here.
        analyzer.addWarning("Selectivity hints are ignored for 'AND' compound "
            + "predicates, either in the SQL query or internally generated.");
      } else {
        return;
      }
    }
    computeSelectivity();
  }

  @Deprecated
  protected void computeSelectivity() {
    if (betweenSelectivity_ != -1) {
      selectivity_ = betweenSelectivity_;
      return;
    } else if (!getChild(0).hasSelectivity()
        || (children_.size() == 2 && !getChild(1).hasSelectivity())) {
      // Give up if one of our children has an unknown selectivity.
      selectivity_ = -1;
      return;
    }

    switch (op_) {
      case AND:
        selectivity_ = getChild(0).selectivity_ * getChild(1).selectivity_;
        break;
      case OR:
        selectivity_ = getChild(0).selectivity_ + getChild(1).selectivity_
            - getChild(0).selectivity_ * getChild(1).selectivity_;
        break;
      case NOT:
        selectivity_ = 1.0 - getChild(0).selectivity_;
        break;
    }
    selectivity_ = Math.max(0.0, Math.min(1.0, selectivity_));
  }

  @Override
  protected float computeEvalCost() {
    return hasChildCosts() ? getChildCosts() + COMPOUND_PREDICATE_COST : UNKNOWN_COST;
  }

  /**
   * Negates a CompoundPredicate.
   */
  @Override
  public Expr negate() {
    if (op_ == Operator.NOT) return getChild(0);
    Expr negatedLeft = getChild(0).negate();
    Expr negatedRight = getChild(1).negate();
    Operator newOp = (op_ == Operator.OR) ? Operator.AND : Operator.OR;
    return new CompoundPredicate(newOp, negatedLeft, negatedRight);
  }

  /**
   * Creates a conjunctive predicate from a list of exprs.
   */
  public static Expr createConjunctivePredicate(List<Expr> conjuncts) {
    return createCompoundTree(conjuncts, Operator.AND);
  }

  /**
   * Creates a disjunctive predicate from a list of exprs.
   */
  public static Expr createDisjunctivePredicate(List<Expr> disjuncts) {
    return createCompoundTree(disjuncts, Operator.OR);
  }

  private static Expr createCompoundTree(List<Expr> exprs, Operator op) {
    Preconditions.checkState(op == Operator.AND || op == Operator.OR);
    Expr result = null;
    for (Expr expr: exprs) {
      if (result == null) {
        result = expr;
        continue;
      }
      result = new CompoundPredicate(op, result, expr);
    }
    return result;
  }

  @Override
  public Expr clone() { return new CompoundPredicate(this); }

  // Create an AND predicate between two exprs, 'lhs' and 'rhs'. If
  // 'rhs' is null, simply return 'lhs'.
  public static Expr createConjunction(Expr lhs, Expr rhs) {
    if (rhs == null) return lhs;
    return new CompoundPredicate(Operator.AND, rhs, lhs);
  }

  public boolean derivedFromBetween() { return betweenSelectivity_ != -1; }
}
