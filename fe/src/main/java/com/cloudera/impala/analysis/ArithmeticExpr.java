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
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY("*", "multiply"),
    DIVIDE("/", "divide"),
    MOD("%", "mod"),
    INT_DIVIDE("DIV", "int_divide"),
    ADD("+", "add"),
    SUBTRACT("-", "subtract"),
    BITAND("&", "bitand"),
    BITOR("|", "bitor"),
    BITXOR("^", "bitxor"),
    BITNOT("~", "bitnot");

    private final String description_;
    private final String name_;

    private Operator(String description, String name) {
      this.description_ = description;
      this.name_ = name;
    }

    @Override
    public String toString() { return description_; }
    public String getName() { return name_; }
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

  /**
   * Copy c'tor used in clone().
   */
  protected ArithmeticExpr(ArithmeticExpr other) {
    super(other);
    op_ = other.op_;
  }

  public static void initBuiltins(Db db) {
    for (ColumnType t: ColumnType.getNumericTypes()) {
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.MULTIPLY.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.ADD.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.SUBTRACT.getName(), Lists.newArrayList(t, t), t));
    }
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.DIVIDE.getName(),
        Lists.newArrayList(ColumnType.DOUBLE, ColumnType.DOUBLE),
        ColumnType.DOUBLE));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.DIVIDE.getName(),
        Lists.newArrayList(ColumnType.DECIMAL, ColumnType.DECIMAL),
        ColumnType.DECIMAL));

    for (ColumnType t: ColumnType.getIntegerTypes()) {
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.MOD.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITAND.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITOR.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITXOR.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITNOT.getName(), Lists.newArrayList(t), t));
    }
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.MOD.getName(), Lists.newArrayList(
            ColumnType.DECIMAL, ColumnType.DECIMAL), ColumnType.DECIMAL));
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
  }

  /**
   * Inserts a cast from child[childIdx] to targetType if one is necessary.
   * Note this is different from Expr.castChild() since arithmetic for decimals
   * the cast is handled as part of the operator and in general, the return type
   * does not match the input types.
   */
  void castChild(int childIdx, ColumnType targetType) throws AnalysisException {
    ColumnType t = getChild(childIdx).getType();
    if (t.matchesType(targetType)) return;
    if (targetType.isDecimal() && !t.isNull()) targetType = t.getMinResolutionDecimal();
    castChild(targetType, childIdx);
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

    ColumnType t0 = getChild(0).getType();
    // bitnot is the only unary op, deal with it here
    if (op_ == Operator.BITNOT) {
      // Special case ~NULL to resolve to TYPE_INT.
      if (!t0.isNull() && !t0.isIntegerType()) {
        throw new AnalysisException("Bitwise operations only allowed on integer " +
            "types: " + toSql());
      }
      if (t0.isNull()) castChild(0, ColumnType.INT);
      fn_ = getBuiltinFunction(analyzer, op_.getName(), collectChildReturnTypes(),
          CompareMode.IS_SUPERTYPE_OF);
      Preconditions.checkNotNull(fn_);
      castForFunctionCall(false);
      type_ = fn_.getReturnType();
      return;
    }

    Preconditions.checkState(children_.size() == 2); // only bitnot is unary
    convertNumericLiteralsFromDecimal(analyzer);
    t0 = getChild(0).getType();
    ColumnType t1 = getChild(1).getType();

    String fnName = op_.getName();
    switch (op_) {
      case ADD:
      case SUBTRACT:
      case DIVIDE:
      case MULTIPLY:
      case MOD:
        type_ = TypesUtil.getArithmeticResultType(t0, t1, op_);
        // If both of the children are null, we'll default to the DOUBLE version of the
        // operator. This prevents the BE from seeing NULL_TYPE.
        if (type_.isNull()) type_ = ColumnType.DOUBLE;
        break;

      case INT_DIVIDE:
      case BITAND:
      case BITOR:
      case BITXOR:
        if ((!t0.isNull() & !t0.isIntegerType()) ||
            (!t1.isNull() && !t1.isIntegerType())) {
          throw new AnalysisException("Invalid non-integer argument to operation '" +
              op_.toString() + "': " + this.toSql());
        }
        type_ = ColumnType.getAssignmentCompatibleType(t0, t1);
        // If both of the children are null, we'll default to the INT version of the
        // operator. This prevents the BE from seeing NULL_TYPE.
        if (type_.isNull()) type_ = ColumnType.INT;
        Preconditions.checkState(type_.isIntegerType());
        break;

      default:
        // the programmer forgot to deal with a case
        Preconditions.checkState(false,
            "Unknown arithmetic operation " + op_.toString() + " in: " + this.toSql());
        break;
    }

    // Don't cast from decimal to decimal. The BE function can just handle this.
    if (!(type_.isDecimal() && t0.isDecimal())) castChild(0, type_);
    if (!(type_.isDecimal() && t1.isDecimal())) castChild(1, type_);
    t0 = getChild(0).getType();
    t1 = getChild(1).getType();

    // Use MATH_MOD function operator for floating-point modulo.
    // TODO remove this when we have operators implemented using the UDF interface
    // and we can resolve this just using function overloading.
    if ((t0.isFloatingPointType() || t1.isFloatingPointType()) &&
        op_ == ArithmeticExpr.Operator.MOD) {
      fnName = "fmod";
    }

    fn_ = getBuiltinFunction(analyzer, fnName, collectChildReturnTypes(),
        CompareMode.IS_IDENTICAL);
    if (fn_ == null) {
      Preconditions.checkState(false, String.format("No match " +
          "for '%s' with operand types %s and %s", toSql(), t0, t1));
    }
    Preconditions.checkState(type_.matchesType(fn_.getReturnType()));
  }

  @Override
  public Expr clone() { return new ArithmeticExpr(this); }
}
