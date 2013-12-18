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
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Most predicates with two operands..
 *
 */
public class BinaryPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(BinaryPredicate.class);

  public enum Operator {
    EQ("=", FunctionOperator.EQ),
    NE("!=", FunctionOperator.NE),
    LE("<=", FunctionOperator.LE),
    GE(">=", FunctionOperator.GE),
    LT("<", FunctionOperator.LT),
    GT(">", FunctionOperator.GT);

    private final String description;
    private final FunctionOperator functionOp;

    private Operator(String description, FunctionOperator functionOp) {
      this.description = description;
      this.functionOp = functionOp;
    }

    @Override
    public String toString() {
      return description;
    }

    public FunctionOperator toFunctionOp() {
      return functionOp;
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
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((BinaryPredicate) obj).opcode_ == this.opcode_;
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.BINARY_PRED;
    msg.setOpcode(opcode_);
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

    PrimitiveType t1 = getChild(0).getType();
    PrimitiveType t2 = getChild(1).getType();
    PrimitiveType compatibleType = PrimitiveType.getAssignmentCompatibleType(t1, t2);

    if (!compatibleType.isValid()) {
      // there is no type to which both are assignment-compatible -> we can't compare them
      throw new AnalysisException("operands are not comparable: " + this.toSql());
    }

    // Ignore return value because type is always bool for predicates.
    castBinaryOp(compatibleType);

    OpcodeRegistry.BuiltinFunction match = OpcodeRegistry.instance().getFunctionInfo(
        op_.toFunctionOp(), true, compatibleType, compatibleType);
    Preconditions.checkState(match != null);
    Preconditions.checkState(match.getReturnType() == PrimitiveType.BOOLEAN);
    this.opcode_ = match.opcode;

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
    return new Pair(lhs.getSlotId(), rhs.getSlotId());
  }
}
