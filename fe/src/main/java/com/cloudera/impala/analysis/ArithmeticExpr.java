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
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY("*", FunctionOperator.MULTIPLY),
    DIVIDE("/", FunctionOperator.DIVIDE),
    MOD("%", FunctionOperator.MOD),
    INT_DIVIDE("DIV", FunctionOperator.INT_DIVIDE),
    ADD("+", FunctionOperator.ADD),
    SUBTRACT("-", FunctionOperator.SUBTRACT),
    BITAND("&", FunctionOperator.BITAND),
    BITOR("|", FunctionOperator.BITOR),
    BITXOR("^", FunctionOperator.BITXOR),
    BITNOT("~", FunctionOperator.BITNOT);

    private final String description;
    private final FunctionOperator functionOp;

    private Operator(String description, FunctionOperator thriftOp) {
      this.description = description;
      this.functionOp = thriftOp;
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

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkArgument(op == Operator.BITNOT && e2 == null
        || op != Operator.BITNOT && e2 != null);
    if (e2 != null) {
      children.add(e2);
    }
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("op", op)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public String toSql() {
    if (children.size() == 1) {
      return op.toString() + " " + getChild(0).toSql();
    } else {
      Preconditions.checkState(children.size() == 2);
      return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
    msg.setOpcode(opcode);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((ArithmeticExpr) obj).opcode == opcode;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    for (Expr child: children) {
      Expr operand = (Expr) child;
      if (!operand.type.isNumericType() && !operand.type.isNull()) {
        throw new AnalysisException("Arithmetic operation requires " +
            "numeric operands: " + toSql());
      }
    }

    // bitnot is the only unary op, deal with it here
    if (op == Operator.BITNOT) {
      type = getChild(0).getType();
      OpcodeRegistry.Signature match =
        OpcodeRegistry.instance().getFunctionInfo(op.functionOp, true, type);
      if (match == null) {
        throw new AnalysisException("Bitwise operations only allowed on fixed-point " +
            "types: " + toSql());
      }
      Preconditions.checkState(type == match.returnType || type.isNull());
      opcode = match.opcode;
      return;
    }

    PrimitiveType t1 = getChild(0).getType();
    PrimitiveType t2 = getChild(1).getType();  // only bitnot is unary

    switch (op) {
      case MULTIPLY:
      case ADD:
      case SUBTRACT:
        // numeric ops must be promoted to highest-resolution type
        // (otherwise we can't guarantee that a <op> b won't overflow/underflow)
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2).getMaxResolutionType();
        Preconditions.checkState(type.isValid());
        break;
      case MOD:
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        break;
      case DIVIDE:
        type = PrimitiveType.DOUBLE;
        break;
      case INT_DIVIDE:
      case BITAND:
      case BITOR:
      case BITXOR:
        if (t1.isFloatingPointType() || t2.isFloatingPointType()) {
          throw new AnalysisException(
              "Invalid floating point argument to operation " +
              op.toString() + ": " + this.toSql());
        }
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        // the result is always an integer or null
        Preconditions.checkState(type.isFixedPointType() || type.isNull());
        break;
      default:
        // the programmer forgot to deal with a case
        Preconditions.checkState(false,
            "Unknown arithmetic operation " + op.toString() + " in: " + this.toSql());
        break;
    }

    type = castBinaryOp(type);
    OpcodeRegistry.Signature match =
      OpcodeRegistry.instance().getFunctionInfo(op.toFunctionOp(), true, type, type);
    this.opcode = match.opcode;
  }
}
