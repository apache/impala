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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Most predicates with two operands.
 *
 */
public class BinaryPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(BinaryPredicate.class);

  public enum Operator {
    EQ("=", "eq"),
    NE("!=", "ne"),
    LE("<=", "le"),
    GE(">=", "ge"),
    LT("<", "lt"),
    GT(">", "gt");

    private final String description;
    private final String name;

    private Operator(String description, String name) {
      this.description = description;
      this.name = name;
    }

    @Override
    public String toString() { return description; }
    public String getName() { return name; }
  }

  public static void initBuiltins(Db db) {
    for (ColumnType t: ColumnType.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.EQ.getName(), Lists.newArrayList(t, t), ColumnType.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.NE.getName(), Lists.newArrayList(t, t), ColumnType.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.LE.getName(), Lists.newArrayList(t, t), ColumnType.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.GE.getName(), Lists.newArrayList(t, t), ColumnType.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.LT.getName(), Lists.newArrayList(t, t), ColumnType.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.GT.getName(), Lists.newArrayList(t, t), ColumnType.BOOLEAN));
    }
  }

  private final Operator op_;

  public Operator getOp() { return op_; }

  public BinaryPredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkNotNull(e2);
    children_.add(e2);
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    Preconditions.checkState(children_.size() == 2);
    // This check is important because we often clone and/or evaluate predicates,
    // and it's easy to get the casting logic wrong, e.g., cloned predicates
    // with expr substitutions need to be re-analyzed *after* unsetIsAnalyzed().
    Preconditions.checkState(getChild(0).getType().matchesType(getChild(1).getType()));
    msg.node_type = TExprNodeType.BINARY_PRED;
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("op", op_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);

    fn_ = getBuiltinFunction(analyzer, op_.getName(), collectChildReturnTypes(),
        CompareMode.IS_SUPERTYPE_OF);
    if (fn_ == null) {
      throw new AnalysisException("operands of type " + getChild(0).getType() +
          " and " + getChild(1).getType()  + " are not comparable: " + toSql());
    }
    Preconditions.checkState(fn_.getReturnType().isBoolean());
    castForFunctionCall();

    // determine selectivity
    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    if (op_ == Operator.EQ
        && isSingleColumnPredicate(slotRefRef, null)
        && slotRefRef.getRef().getNumDistinctValues() > 0) {
      Preconditions.checkState(slotRefRef.getRef() != null);
      selectivity_ = 1.0 / slotRefRef.getRef().getNumDistinctValues();
      selectivity_ = Math.max(0, Math.min(1, selectivity_));
    } else {
      // TODO: improve using histograms, once they show up
      selectivity_ = Expr.DEFAULT_SELECTIVITY;
    }
  }

  /**
   * If predicate is of the form "<slotref> <op> <expr>", returns expr,
   * otherwise returns null. Slotref may be wrapped in a CastExpr.
   * TODO: revisit CAST handling at the caller
   */
  public Expr getSlotBinding(SlotId id) {
    // check left operand
    SlotRef slotRef = getChild(0).unwrapSlotRef(false);
    if (slotRef != null && slotRef.getSlotId() == id) return getChild(1);
    // check right operand
    slotRef = getChild(1).unwrapSlotRef(false);
    if (slotRef != null && slotRef.getSlotId() == id) return getChild(0);
    return null;
  }

  /**
   * If this is an equality predicate between two slots that only require implicit
   * casts, returns those two slots; otherwise returns null.
   */
  @Override
  public Pair<SlotId, SlotId> getEqSlots() {
    if (op_ != Operator.EQ) return null;
    SlotRef lhs = getChild(0).unwrapSlotRef(true);
    if (lhs == null) return null;
    SlotRef rhs = getChild(1).unwrapSlotRef(true);
    if (rhs == null) return null;
    return new Pair<SlotId, SlotId>(lhs.getSlotId(), rhs.getSlotId());
  }
}
