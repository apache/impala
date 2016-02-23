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

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class IsNullPredicate extends Predicate {
  private final boolean isNotNull_;

  private static final String IS_NULL = "is_null_pred";
  private static final String IS_NOT_NULL = "is_not_null_pred";

  public IsNullPredicate(Expr e, boolean isNotNull) {
    super();
    this.isNotNull_ = isNotNull;
    Preconditions.checkNotNull(e);
    children_.add(e);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected IsNullPredicate(IsNullPredicate other) {
    super(other);
    isNotNull_ = other.isNotNull_;
  }

  public boolean isNotNull() { return isNotNull_; }

  public static void initBuiltins(Db db) {
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      String isNullSymbol;
      if (t.isBoolean()) {
        isNullSymbol = "_ZN6impala15IsNullPredicate6IsNullIN10impala_udf10BooleanValE" +
            "EES3_PNS2_15FunctionContextERKT_";
      } else {
        String udfType = Function.getUdfType(t);
        isNullSymbol = "_ZN6impala15IsNullPredicate6IsNullIN10impala_udf" +
            udfType.length() + udfType +
            "EEENS2_10BooleanValEPNS2_15FunctionContextERKT_";
      }
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          IS_NULL, isNullSymbol, Lists.newArrayList(t), Type.BOOLEAN));

      String isNotNullSymbol = isNullSymbol.replace("6IsNull", "9IsNotNull");
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          IS_NOT_NULL, isNotNullSymbol, Lists.newArrayList(t), Type.BOOLEAN));
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((IsNullPredicate) obj).isNotNull_ == isNotNull_;
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + (isNotNull_ ? " IS NOT NULL" : " IS NULL");
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("notNull", isNotNull_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);

    if (contains(Subquery.class)) {
      if (getChild(0) instanceof ExistsPredicate) {
        // Replace the EXISTS subquery with a BoolLiteral as it can never return
        // a null value.
        setChild(0, new BoolLiteral(true));
        getChild(0).analyze(analyzer);
      } else if (!getChild(0).contains(Expr.IS_SCALAR_SUBQUERY)) {
        // We only support scalar subqueries in an IS NULL predicate because
        // they can be rewritten into a join.
        // TODO: Add support for InPredicates and BinaryPredicates with
        // subqueries when we implement independent subquery evaluation.
        // TODO: Handle arbitrary UDA/Udfs
        throw new AnalysisException("Unsupported IS NULL predicate that contains " +
            "a subquery: " + toSqlImpl());
      }
    }

    // Make sure the BE never sees TYPE_NULL
    if (getChild(0).getType().isNull()) {
      uncheckedCastChild(ScalarType.BOOLEAN, 0);
    }

    if (getChild(0).getType().isComplexType()) {
      String errorMsg = (isNotNull_ ? "IS NOT NULL" : "IS NULL") +
         " predicate does not support complex types: ";
      throw new AnalysisException(errorMsg + toSqlImpl());
    }

    if (isNotNull_) {
      fn_ = getBuiltinFunction(
          analyzer, IS_NOT_NULL, collectChildReturnTypes(), CompareMode.IS_IDENTICAL);
    } else {
      fn_ = getBuiltinFunction(
          analyzer, IS_NULL, collectChildReturnTypes(), CompareMode.IS_IDENTICAL);
    }

    // determine selectivity
    // TODO: increase this to make sure we don't end up favoring broadcast joins
    // due to underestimated cardinalities?
    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    if (isSingleColumnPredicate(slotRefRef, null)) {
      SlotDescriptor slotDesc = slotRefRef.getRef().getDesc();
      if (!slotDesc.getStats().hasNulls()) return;
      Table table = slotDesc.getParent().getTable();
      if (table != null && table.getNumRows() > 0) {
        long numRows = table.getNumRows();
        if (isNotNull_) {
          selectivity_ =
              (double) (numRows - slotDesc.getStats().getNumNulls()) / (double) numRows;
        } else {
          selectivity_ = (double) slotDesc.getStats().getNumNulls() / (double) numRows;
        }
        selectivity_ = Math.max(0.0, Math.min(1.0, selectivity_));
      }
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  /*
   * If predicate is of the form "<SlotRef> IS [NOT] NULL", returns the
   * SlotRef.
   */
  @Override
  public SlotRef getBoundSlot() {
    return getChild(0).unwrapSlotRef(true);
  }

  /**
   * Negates an IsNullPredicate.
   */
  @Override
  public Expr negate() {
    return new IsNullPredicate(getChild(0), !isNotNull_);
  }

  @Override
  public Expr clone() { return new IsNullPredicate(this); }
}
