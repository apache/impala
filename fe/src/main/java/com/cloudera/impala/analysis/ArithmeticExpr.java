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

import com.cloudera.impala.catalog.AuthorizationException;
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

  private final Operator op_;

  public Operator getOp() { return op_; }

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkArgument(op == Operator.BITNOT && e2 == null
        || op != Operator.BITNOT && e2 != null);
    if (e2 != null) children_.add(e2);
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("op", op_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public String toSqlImpl() {
    if (children_.size() == 1) {
      return op_.toString() + getChild(0).toSql();
    } else {
      Preconditions.checkState(children_.size() == 2);
      return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
    msg.setOpcode(opcode_);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((ArithmeticExpr) obj).opcode_ == opcode_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    for (Expr child: children_) {
      Expr operand = (Expr) child;
      if (!operand.type_.isNumericType() && !operand.type_.isNull()) {
        throw new AnalysisException("Arithmetic operation requires " +
            "numeric operands: " + toSql());
      }
    }

    // bitnot is the only unary op, deal with it here
    if (op_ == Operator.BITNOT) {
      type_ = getChild(0).getType();
      OpcodeRegistry.BuiltinFunction match =
        OpcodeRegistry.instance().getFunctionInfo(op_.functionOp, true, type_);
      if (match == null) {
        throw new AnalysisException("Bitwise operations only allowed on fixed-point " +
            "types: " + toSql());
      }
      Preconditions.checkState(type_ == match.getReturnType() || type_.isNull());
      opcode_ = match.opcode;
      return;
    }

    PrimitiveType t1 = getChild(0).getType();
    PrimitiveType t2 = getChild(1).getType();  // only bitnot is unary

    FunctionOperator funcOp = op_.toFunctionOp();
    switch (op_) {
      case MULTIPLY:
      case ADD:
      case SUBTRACT:
        // If one of the types is null, use the compatible type without promotion.
        // Otherwise, promote the compatible type to the next higher resolution type,
        // to ensure that that a <op> b won't overflow/underflow.
        type_ = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        if (!(t1.isNull() || t2.isNull())) {
          // Both operands are non-null. Use next higher resolution type.
          type_ = type_.getNextResolutionType();
        }
        Preconditions.checkState(type_.isValid());
        break;
      case MOD:
        type_ = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        // Use MATH_MOD function operator for floating-point modulo.
        if (type_.isFloatingPointType()) funcOp = FunctionOperator.MATH_FMOD;
        break;
      case DIVIDE:
        type_ = PrimitiveType.DOUBLE;
        break;
      case INT_DIVIDE:
      case BITAND:
      case BITOR:
      case BITXOR:
        if (t1.isFloatingPointType() || t2.isFloatingPointType()) {
          throw new AnalysisException(
              "Invalid floating point argument to operation " +
              op_.toString() + ": " + this.toSql());
        }
        type_ = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        // the result is always an integer or null
        Preconditions.checkState(type_.isFixedPointType() || type_.isNull());
        break;
      default:
        // the programmer forgot to deal with a case
        Preconditions.checkState(false,
            "Unknown arithmetic operation " + op_.toString() + " in: " + this.toSql());
        break;
    }

    type_ = castBinaryOp(type_);
    OpcodeRegistry.BuiltinFunction match =
        OpcodeRegistry.instance().getFunctionInfo(funcOp, true, type_, type_);
    if (match == null) {
      Preconditions.checkState(false, String.format("No match in function registry " +
          "for '%s' with operand types %s and %s", toSql(), type_, type_));
    }
    this.opcode_ = match.opcode;
  }
}
