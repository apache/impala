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

import java.util.Optional;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ArithmeticExpr extends Expr {
  enum OperatorPosition {
    BINARY_INFIX,
    UNARY_PREFIX,
    UNARY_POSTFIX,
  }

  public enum Operator {
    MULTIPLY("*", "multiply", OperatorPosition.BINARY_INFIX),
    DIVIDE("/", "divide", OperatorPosition.BINARY_INFIX),
    MOD("%", "mod", OperatorPosition.BINARY_INFIX),
    INT_DIVIDE("DIV", "int_divide", OperatorPosition.BINARY_INFIX),
    ADD("+", "add", OperatorPosition.BINARY_INFIX),
    SUBTRACT("-", "subtract", OperatorPosition.BINARY_INFIX),
    BITAND("&", "bitand", OperatorPosition.BINARY_INFIX),
    BITOR("|", "bitor", OperatorPosition.BINARY_INFIX),
    BITXOR("^", "bitxor", OperatorPosition.BINARY_INFIX),
    BITNOT("~", "bitnot", OperatorPosition.UNARY_PREFIX),
    FACTORIAL("!", "factorial", OperatorPosition.UNARY_POSTFIX);

    private final String description_;
    private final String name_;
    private final OperatorPosition pos_;

    private Operator(String description, String name, OperatorPosition pos) {
      this.description_ = description;
      this.name_ = name;
      this.pos_ = pos;
    }

    @Override
    public String toString() { return description_; }
    public String getName() { return name_; }
    public OperatorPosition getPos() { return pos_; }

    public boolean isUnary() {
      return pos_ == OperatorPosition.UNARY_PREFIX ||
             pos_ == OperatorPosition.UNARY_POSTFIX;
    }

    public boolean isBinary() {
      return pos_ == OperatorPosition.BINARY_INFIX;
    }
  }

  private final Operator op_;
  // cache prior shouldConvertToCNF checks to avoid repeat tree walking
  // omitted from clone in case cloner plans to mutate the expr
  protected Optional<Boolean> shouldConvertToCNF_ = Optional.empty();

  public Operator getOp() { return op_; }

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkArgument((op.isUnary() && e2 == null) ||
        (op.isBinary() && e2 != null));
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
    for (Type t: Type.getNumericTypes()) {
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.MULTIPLY.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.ADD.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.SUBTRACT.getName(), Lists.newArrayList(t, t), t));
    }
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.DIVIDE.getName(),
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE),
        Type.DOUBLE));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.DIVIDE.getName(),
        Lists.<Type>newArrayList(Type.DECIMAL, Type.DECIMAL),
        Type.DECIMAL));

    /*
     * MOD(), FACTORIAL(), BITAND(), BITOR(), BITXOR(), and BITNOT() are registered as
     * builtins, see impala_functions.py
     */
    for (Type t: Type.getIntegerTypes()) {
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t), t));
    }
  }

  @Override
  public String debugString() {
    return MoreObjects.toStringHelper(this)
        .add("op", op_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (children_.size() == 1) {
      if (op_.getPos() == OperatorPosition.UNARY_PREFIX) {
        return op_.toString() + getChild(0).toSql(options);
      } else {
        assert(op_.getPos() == OperatorPosition.UNARY_POSTFIX);
        return getChild(0).toSql(options) + op_.toString();
      }
    } else {
      Preconditions.checkState(children_.size() == 2);
      return getChild(0).toSql(options) + " " + op_.toString() + " "
          + getChild(1).toSql(options);
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  /**
   * Inserts a cast from child[childIdx] to targetType if one is necessary.
   * Note this is different from Expr.castChild() since arithmetic for decimals
   * the cast is handled as part of the operator and in general, the return type
   * does not match the input types.
   */
  void castChild(int childIdx, Type targetType) throws AnalysisException {
    Type t = getChild(childIdx).getType();
    if (t.matchesType(targetType)) return;
    if (targetType.isDecimal() && !t.isNull()) {
      Preconditions.checkState(t.isScalarType());
      targetType = ((ScalarType) t).getMinResolutionDecimal();
    }
    castChild(targetType, childIdx);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    for (Expr child: children_) {
      Expr operand = (Expr) child;
      if (!operand.type_.isNumericType() && !operand.type_.isNull()) {
        String errMsg = "Arithmetic operation requires numeric operands: " + toSql();
        if (operand instanceof Subquery && !operand.type_.isScalarType()) {
          errMsg = "Subquery must return a single row: " + operand.toSql();
        }
        throw new AnalysisException(errMsg);
      }
    }

    convertNumericLiteralsFromDecimal(analyzer);
    Type t0 = getChild(0).getType();
    Type t1 = null;
    if (op_.isUnary()) {
      Preconditions.checkState(children_.size() == 1);
    } else if (op_.isBinary()) {
      Preconditions.checkState(children_.size() == 2);
      t1 = getChild(1).getType();
    }

    String fnName = op_.getName();
    switch (op_) {
      case ADD:
      case SUBTRACT:
      case DIVIDE:
      case MULTIPLY:
      case MOD:
        type_ = TypesUtil.getArithmeticResultType(t0, t1, op_,
            analyzer.getQueryOptions().isDecimal_v2());
        // If both of the children are null, we'll default to the DOUBLE version of the
        // operator. This prevents the BE from seeing NULL_TYPE.
        if (type_.isNull()) type_ = Type.DOUBLE;
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
        type_ = Type.getAssignmentCompatibleType(t0, t1, TypeCompatibility.DEFAULT);
        // If both of the children are null, we'll default to the INT version of the
        // operator. This prevents the BE from seeing NULL_TYPE.
        if (type_.isNull()) type_ = Type.INT;
        Preconditions.checkState(type_.isIntegerType());
        break;
      case BITNOT:
      case FACTORIAL:
        if (!t0.isNull() && !t0.isIntegerType()) {
          throw new AnalysisException("'" + op_.toString() + "'" +
              " operation only allowed on integer types: " + toSql());
        }
        // Special-case NULL to resolve to the appropriate type.
        if (op_ == Operator.BITNOT) {
          if (t0.isNull()) castChild(0, Type.INT);
        } else {
          assert(op_ == Operator.FACTORIAL);
          if (t0.isNull()) castChild(0, Type.BIGINT);
        }
        fn_ = getBuiltinFunction(analyzer, op_.getName(), collectChildReturnTypes(),
            CompareMode.IS_SUPERTYPE_OF);
        Preconditions.checkNotNull(fn_);
        castForFunctionCall(false, analyzer.getRegularCompatibilityLevel());
        type_ = fn_.getReturnType();
        return;
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

    fn_ = getBuiltinFunction(analyzer, fnName, collectChildReturnTypes(),
        CompareMode.IS_IDENTICAL);
    if (fn_ == null) {
      Preconditions.checkState(false, String.format("No match " +
          "for '%s' with operand types %s and %s", toSql(), t0, t1));
    }
    Preconditions.checkState(type_.matchesType(fn_.getReturnType()));
  }

  @Override
  protected float computeEvalCost() {
    return hasChildCosts() ? getChildCosts() + ARITHMETIC_OP_COST : UNKNOWN_COST;
  }

  private boolean lookupShouldConvertToCNF() {
    for (int i = 0; i < children_.size(); ++i) {
      if (!getChild(i).shouldConvertToCNF()) return false;
    }
    return true;
  }

  /**
   * Return true if this expression's children should be converted to CNF.
   */
  @Override
  public boolean shouldConvertToCNF() {
    if (shouldConvertToCNF_.isPresent()) {
      return shouldConvertToCNF_.get();
    }
    boolean result = lookupShouldConvertToCNF();
    shouldConvertToCNF_ = Optional.of(result);
    return result;
  }

  @Override
  public Expr clone() { return new ArithmeticExpr(this); }
}
