// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOperator;
import com.google.common.base.Preconditions;

/**
 * Most predicates with two operands..
 *
 */
public class BinaryPredicate extends Predicate {
  public enum Operator {
    EQ("=", TExprOperator.EQ),
    NE("!=", TExprOperator.NE),
    LE("<=", TExprOperator.LE),
    GE(">=", TExprOperator.GE),
    LT("<", TExprOperator.LT),
    GT(">", TExprOperator.GT);

    private final String description;
    private final TExprOperator thriftOp;

    private Operator(String description, TExprOperator thriftOp) {
      this.description = description;
      this.thriftOp = thriftOp;
    }

    @Override
    public String toString() {
      return description;
    }

    public TExprOperator toThrift() {
      return thriftOp;
    }
  };
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
    return ((BinaryPredicate) obj).op == op;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.BINARY_PRED;
    msg.op = op.toThrift();
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
    if (slotRef != null && slotRef.getId() == id) {
      return getChild(1);
    }

    // check right operand
    if (getChild(1) instanceof SlotRef) {
      slotRef = (SlotRef) getChild(1);
    } else if (getChild(1) instanceof CastExpr
               && getChild(1).getChild(0) instanceof SlotRef) {
      slotRef = (SlotRef) getChild(1).getChild(0);
    }
    if (slotRef != null && slotRef.getId() == id) {
      return getChild(0);
    }

    return null;
  }
}
