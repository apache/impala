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

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class representing a [NOT] IN predicate. It determines if a specified value
 * (first child) matches any value in a subquery (second child) or a list
 * of values (remaining children).
 */
public class InPredicate extends Predicate {
  private static final String IN_SET_LOOKUP = "in_set_lookup";
  private static final String NOT_IN_SET_LOOKUP = "not_in_set_lookup";
  private static final String IN_ITERATE = "in_iterate";
  private static final String NOT_IN_ITERATE = "not_in_iterate";
  private final boolean isNotIn_;

  public boolean isNotIn() { return isNotIn_; }

  public static void initBuiltins(Db db) {
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      // TODO we do not support codegen for CHAR and the In predicate must be codegened
      // because it has variable number of arguments. This will force CHARs to be
      // cast up to strings; meaning that "in" comparisons will not have CHAR comparison
      // semantics.
      if (t.getPrimitiveType() == PrimitiveType.CHAR) continue;

      String typeString = t.getPrimitiveType().toString().toLowerCase();
      if (t.isVarchar() || t.isBinary()) typeString = "string";

      db.addBuiltin(ScalarFunction.createBuiltin(IN_ITERATE,
          Lists.newArrayList(t, t), true, Type.BOOLEAN,
          "impala::InPredicate::InIterate", null, null,  false));
      db.addBuiltin(ScalarFunction.createBuiltin(NOT_IN_ITERATE,
          Lists.newArrayList(t, t), true, Type.BOOLEAN,
          "impala::InPredicate::NotInIterate", null, null, false));

      String prepareFn = "impala::InPredicate::SetLookupPrepare_" + typeString;
      String closeFn = "impala::InPredicate::SetLookupClose_" + typeString;

      db.addBuiltin(ScalarFunction.createBuiltin(IN_SET_LOOKUP,
          Lists.newArrayList(t, t), true, Type.BOOLEAN,
          "impala::InPredicate::InSetLookup", prepareFn, closeFn,  false));
      db.addBuiltin(ScalarFunction.createBuiltin(NOT_IN_SET_LOOKUP,
          Lists.newArrayList(t, t), true, Type.BOOLEAN,
          "impala::InPredicate::NotInSetLookup", prepareFn, closeFn, false));
    }
  }

  // First child is the comparison expr for which we
  // should check membership in the inList (the remaining children).
  public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
    children_.add(compareExpr);
    children_.addAll(inList);
    isNotIn_ = isNotIn;
  }

  // C'tor for initializing an [NOT] IN predicate with a subquery child.
  public InPredicate(Expr compareExpr, Expr subquery, boolean isNotIn) {
    Preconditions.checkNotNull(compareExpr);
    Preconditions.checkNotNull(subquery);
    children_.add(compareExpr);
    children_.add(subquery);
    isNotIn_ = isNotIn;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected InPredicate(InPredicate other) {
    super(other);
    isNotIn_ = other.isNotIn_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    if (contains(Subquery.class)) {
      // An [NOT] IN predicate with a subquery must contain two children, the second of
      // which is a Subquery.
      if (children_.size() != 2 || !(getChild(1) instanceof Subquery)) {
        throw new AnalysisException("Unsupported IN predicate with a subquery: " +
            toSqlImpl());
      }
      Subquery subquery = (Subquery)getChild(1);
      subquery.getStatement().setIsRuntimeScalar(false);
      if (!subquery.returnsScalarColumn()) {
        throw new AnalysisException("Subquery must return a single column: " +
            subquery.toSql());
      }

      // Ensure that the column in the lhs of the IN predicate and the result of
      // the subquery are type compatible. No need to perform any
      // casting at this point. Any casting needed will be performed when the
      // subquery is unnested.
      List<Expr> subqueryExprs = subquery.getStatement().getResultExprs();
      Expr compareExpr = children_.get(0);
      Expr subqueryExpr = subqueryExprs.get(0);
      analyzer.getCompatibleType(compareExpr.getType(), compareExpr, subqueryExpr);
    } else {
      Preconditions.checkState(getChildren().size() >= 2);
      analyzer.castAllToCompatibleType(children_);
      Type childType = children_.get(0).getType();

      if (childType.isNull()) {
        // Make sure the BE never sees TYPE_NULL by picking an arbitrary type
        for (int i = 0; i < children_.size(); ++i) {
          uncheckedCastChild(Type.BOOLEAN, i);
        }
      }

      // Choose SetLookup or Iterate strategy. SetLookup can be used if all the exprs in
      // the IN list are constant, and is faster than iterating if the IN list is big
      // enough.
      boolean allConstant = true;
      for (int i = 1; i < children_.size(); ++i) {
        if (!children_.get(i).isConstant()) {
          allConstant = false;
          break;
        }
      }
      boolean useSetLookup = allConstant;
      // Threshold based on InPredicateBenchmark results
      int setLookupThreshold = children_.get(0).getType().isStringType() ? 2 : 6;
      if (children_.size() - 1 < setLookupThreshold) useSetLookup = false;

      // Only lookup fn_ if all subqueries have been rewritten. If the second child is a
      // subquery, it will have type ArrayType, which cannot be resolved to a builtin
      // function and will fail analysis.
      Type[] argTypes = {getChild(0).type_, getChild(1).type_};
      if (useSetLookup) {
        fn_ = getBuiltinFunction(analyzer, isNotIn_ ? NOT_IN_SET_LOOKUP : IN_SET_LOOKUP,
            argTypes, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
      } else {
        fn_ = getBuiltinFunction(analyzer, isNotIn_ ? NOT_IN_ITERATE : IN_ITERATE,
            argTypes, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
      }
      Preconditions.checkNotNull(fn_);
      Preconditions.checkState(fn_.getReturnType().isBoolean());
      castForFunctionCall(false, analyzer.getRegularCompatibilityLevel());
    }
    computeSelectivity();
  }

  protected void computeSelectivity() {
    if (hasValidSelectivityHint()) {
      return;
    }
    // TODO: Fix selectivity_ for nested predicate
    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    Reference<Integer> idxRef = new Reference<Integer>();
    if (isSingleColumnPredicate(slotRefRef, idxRef) && idxRef.getRef() == 0
        && slotRefRef.getRef().getNumDistinctValues() > 0) {
      if (isNotIn()) {
        selectivity_ = 1.0 - ((double) (getChildren().size() - 1)
            / (double) slotRefRef.getRef().getNumDistinctValues());
      } else {
        selectivity_ = (double) (getChildren().size() - 1)
            / (double) slotRefRef.getRef().getNumDistinctValues();
      }
      selectivity_ = Math.max(0.0, Math.min(1.0, selectivity_));
    }
  }

  @Override
  protected float computeEvalCost() {
    if (!hasChildCosts()) return UNKNOWN_COST;
    // BINARY_PREDICATE_COST accounts for the cost of performing the comparison.
    return getChildCosts() + BINARY_PREDICATE_COST * (children_.size() - 1);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    // Can't serialize a predicate with a subquery
    Preconditions.checkState(!contains(Subquery.class));
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    String notStr = (isNotIn_) ? "NOT " : "";
    strBuilder.append(getChild(0).toSql(options) + " " + notStr + "IN ");
    boolean hasSubquery = contains(Subquery.class);
    if (!hasSubquery) strBuilder.append("(");
    for (int i = 1; i < children_.size(); ++i) {
      strBuilder.append(getChild(i).toSql(options));
      strBuilder.append((i+1 != children_.size()) ? ", " : "");
    }
    if (!hasSubquery) strBuilder.append(")");
    return strBuilder.toString();
  }

  /**
   * If predicate is of the form "<SlotRef> [NOT] IN", returns the
   * SlotRef.
   */
  @Override
  public SlotRef getBoundSlot() {
    return getChild(0).unwrapSlotRef(true);
  }

  /**
   * Negates an InPredicate.
   */
  @Override
  public Expr negate() {
    return new InPredicate(getChild(0), children_.subList(1, children_.size()),
        !isNotIn_);
  }

  @Override
  public Expr clone() { return new InPredicate(this); }
}
