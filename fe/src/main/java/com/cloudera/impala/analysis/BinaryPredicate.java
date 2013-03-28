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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

/**
 * Most predicates with two operands..
 *
 */
public class BinaryPredicate extends Predicate {
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

  private final Operator op;

  public Operator getOp() {
    return op;
  }

  public BinaryPredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkNotNull(e2);
    children.add(e2);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((BinaryPredicate) obj).opcode == this.opcode;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.BINARY_PRED;
    msg.setOpcode(opcode);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
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

    OpcodeRegistry.Signature match = OpcodeRegistry.instance().getFunctionInfo(
        op.toFunctionOp(), true, compatibleType, compatibleType);
    Preconditions.checkState(match != null);
    Preconditions.checkState(match.returnType == PrimitiveType.BOOLEAN);
    this.opcode = match.opcode;

    // determine selectivity
    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    if (op == Operator.EQ
        && isSingleColumnPredicate(slotRefRef, null)
        && slotRefRef.getRef().getNumDistinctValues() > 0) {
      Preconditions.checkState(slotRefRef.getRef() != null);
      selectivity = 1.0 / slotRefRef.getRef().getNumDistinctValues();
      selectivity = Math.max(0, Math.min(1, selectivity));
    } else {
      // TODO: improve using histograms, once they show up
      selectivity = Expr.defaultSelectivity;
    }
  }

  /**
   * If predicate is of the form "<slotref> <op> <expr>", returns expr,
   * otherwise returns null. Slotref may be wrapped in a CastExpr.
   */
  public Expr getSlotBinding(SlotId id) {
    SlotRef slotRef = null;
    // check left operand
    if (getChild(0) instanceof SlotRef) {
      slotRef = (SlotRef) getChild(0);
    } else if (getChild(0) instanceof CastExpr
               && getChild(0).getChild(0) instanceof SlotRef) {
      slotRef = (SlotRef) getChild(0).getChild(0);
    }
    if (slotRef != null && slotRef.getSlotId() == id) {
      return getChild(1);
    }

    // check right operand
    if (getChild(1) instanceof SlotRef) {
      slotRef = (SlotRef) getChild(1);
    } else if (getChild(1) instanceof CastExpr
               && getChild(1).getChild(0) instanceof SlotRef) {
      slotRef = (SlotRef) getChild(1).getChild(0);
    }
    if (slotRef != null && slotRef.getSlotId() == id) {
      return getChild(0);
    }

    return null;
  }
}
